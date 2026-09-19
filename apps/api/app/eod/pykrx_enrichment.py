from __future__ import annotations

from collections import defaultdict
import asyncio
from datetime import date, datetime
import hashlib
import logging
import math
import time
from typing import Any, Sequence
from zoneinfo import ZoneInfo

import pandas as pd
from sqlalchemy import func, select

from app.core.audit_logging import log_fetch_event
from app.core.settings import settings
from app.brokers.kis.provider import kis_real_client
from app.db import session_scope
from app.db_models import DailyPriceBar, DailyPykrxEnrichmentRow, Symbol
from app.eod.pykrx_import import import_pykrx_stock, pykrx_request_context
from app.market_data_store import (
    load_daily_market_relative_contexts,
    load_daily_pykrx_enrichment_rows,
    load_symbols,
    upsert_daily_pykrx_enrichment_rows,
)
from app.repositories.eod_run_repository import load_eod_run_persisted_symbols, load_latest_eod_collection_run

logger = logging.getLogger(__name__)


class _LazyPykrxStock:
    def __getattr__(self, name: str) -> Any:
        try:
            pykrx_stock = import_pykrx_stock()
        except Exception as exc:  # noqa: BLE001
            raise RuntimeError(f'PyKRX stock client is unavailable: {exc}') from exc
        return getattr(pykrx_stock, name)


stock = _LazyPykrxStock()


def _market_ticker_membership_frame(*, date: str, market: str) -> pd.DataFrame:
    tickers = stock.get_market_ticker_list(date=date, market=market)
    normalized = list(tickers or [])
    return pd.DataFrame({'symbol': normalized}, index=normalized)

DEFAULT_PYKRX_ENRICHMENT_CATEGORIES: tuple[str, ...] = (
    'sector_classification',
    'market_breadth',
    'investor_flow_market',
    'investor_flow_symbol',
    'short_selling',
    'valuation_snapshot',
    'foreign_ownership',
)
SYMBOL_ENRICHMENT_CATEGORIES = {
    'sector_classification',
    'investor_flow_symbol',
    'short_selling',
    'valuation_snapshot',
    'foreign_ownership',
}
MARKET_ENRICHMENT_CATEGORIES = {
    'market_breadth',
    'investor_flow_market',
}
FULL_MARKET_COVERAGE_CATEGORIES = {
    'sector_classification',
    'market_breadth',
    'investor_flow_market',
    'investor_flow_symbol',
    'short_selling',
}
CATEGORY_SCOPE_TYPE = {
    'sector_classification': 'symbol',
    'market_breadth': 'market',
    'investor_flow_market': 'market',
    'investor_flow_symbol': 'symbol',
    'short_selling': 'symbol',
    'valuation_snapshot': 'symbol',
    'foreign_ownership': 'symbol',
}
SECTOR_SUPPORTED_MARKETS = {'KOSPI', 'KOSDAQ'}
SHORTING_SUPPORTED_MARKETS = {'KOSPI', 'KOSDAQ', 'KONEX'}
CATEGORY_SUPPORTED_MARKETS = {
    'sector_classification': SECTOR_SUPPORTED_MARKETS,
    'investor_flow_symbol': SECTOR_SUPPORTED_MARKETS,
    'short_selling': SHORTING_SUPPORTED_MARKETS,
}
SYMBOL_INVESTOR_GROUPS: tuple[str, ...] = ('개인', '외국인', '기관합계')
INVESTOR_FLOW_METRIC_KEYS = (
    'sell_volume',
    'buy_volume',
    'net_volume',
    'sell_value',
    'buy_value',
    'net_value',
)
TIME_SENSITIVE_NO_CARRY_FORWARD_CATEGORIES = {
    'sector_classification',
    'investor_flow_market',
    'investor_flow_symbol',
    'short_selling',
}
SHORT_VOLUME_METRIC_KEYS = (
    'short_volume',
    'total_volume',
    'short_volume_ratio_pct',
)
SHORT_VALUE_METRIC_KEYS = (
    'short_value',
    'total_value',
    'short_value_ratio_pct',
)
SHORT_BALANCE_METRIC_KEYS = (
    'short_balance_qty',
    'listed_shares',
    'short_balance_amount',
    'market_cap',
    'short_balance_ratio_pct',
)
VALUATION_SNAPSHOT_REQUIRED_METRIC_KEYS = ('per', 'pbr', 'dividend_yield')
MAX_SQL_SYMBOL_FILTER_SIZE = 500
PYKRX_ENRICHMENT_READ_TIMEOUT_MS = 15_000


def _log_pykrx_enrichment_fetch(operation: str, status: str, **fields: Any) -> None:
    log_fetch_event(service='pykrx_enrichment', operation=operation, status=status, **fields)


def is_decision_usable_pykrx_enrichment_row(row: dict[str, Any]) -> bool:
    category = row.get('category')
    source = row.get('source')
    payload = dict(row.get('payload') or {})
    if category in TIME_SENSITIVE_NO_CARRY_FORWARD_CATEGORIES and source == 'pykrx_carry_forward':
        return False
    if category == 'sector_classification':
        return source == 'pykrx' and bool(str(payload.get('sector_name') or '').strip())
    if category == 'investor_flow_market':
        raw_groups = payload.get('investor_groups')
        if not isinstance(raw_groups, list):
            return False
        investor_groups = {
            str(item.get('investor_type') or '').strip(): item
            for item in raw_groups
            if isinstance(item, dict) and str(item.get('investor_type') or '').strip()
        }
        groups_complete = all(
            _metric_group_complete(
                dict(investor_groups.get(group_name) or {}),
                INVESTOR_FLOW_METRIC_KEYS,
            )
            for group_name in SYMBOL_INVESTOR_GROUPS
        )
        gross_activity = any(
            (_to_float((investor_groups.get(group_name) or {}).get(metric_key)) or 0.0) > 0
            for group_name in SYMBOL_INVESTOR_GROUPS
            for metric_key in ('sell_volume', 'buy_volume', 'sell_value', 'buy_value')
        )
        return groups_complete and gross_activity
    if category == 'investor_flow_symbol':
        investor_groups = dict(payload.get('investor_groups') or {})
        groups_complete = all(
            _metric_group_complete(
                dict(investor_groups.get(group_name) or {}),
                INVESTOR_FLOW_METRIC_KEYS,
            )
            for group_name in SYMBOL_INVESTOR_GROUPS
        )
        gross_activity = any(
            (_to_float((investor_groups.get(group_name) or {}).get(metric_key)) or 0.0) > 0
            for group_name in SYMBOL_INVESTOR_GROUPS
            for metric_key in ('sell_volume', 'buy_volume', 'sell_value', 'buy_value')
        )
        return groups_complete and gross_activity
    if category == 'short_selling':
        return _metric_group_complete(
            dict(payload.get('short_volume') or {}),
            SHORT_VOLUME_METRIC_KEYS,
        )
    return True


def _metric_group_complete(payload: dict[str, Any], keys: Sequence[str]) -> bool:
    return all(_to_float(payload.get(key)) is not None for key in keys)


def _decision_source_priority(row: dict[str, Any]) -> tuple[int, str]:
    source_priority = {
        'kis_open_api': 1,
        'pykrx': 2,
    }.get(str(row.get('source') or ''), 0)
    return source_priority, str(row.get('captured_at') or '')


def _normalize_iso_date(value: str | date) -> str:
    if isinstance(value, date):
        return value.isoformat()
    return date.fromisoformat(str(value)).isoformat()


def _normalize_yyyymmdd(value: str | date) -> str:
    return _normalize_iso_date(value).replace('-', '')


def _current_krx_calendar_date() -> date:
    return datetime.now(ZoneInfo('Asia/Seoul')).date()


def _normalize_symbols(symbols: Sequence[str] | None) -> list[str]:
    normalized: list[str] = []
    seen: set[str] = set()
    for raw_symbol in symbols or []:
        text = str(raw_symbol or '').strip().upper()
        if not text:
            continue
        symbol = text.zfill(6) if text.isdigit() else text
        if symbol in seen:
            continue
        seen.add(symbol)
        normalized.append(symbol)
    return normalized


def _normalize_markets(markets: Sequence[str] | None) -> list[str]:
    normalized: list[str] = []
    seen: set[str] = set()
    for raw_market in markets or []:
        market = str(raw_market or '').strip().upper()
        if not market or market in seen:
            continue
        seen.add(market)
        normalized.append(market)
    return normalized


def _normalize_categories(categories: Sequence[str] | None) -> list[str]:
    normalized: list[str] = []
    seen: set[str] = set()
    for raw_category in categories or []:
        category = str(raw_category or '').strip()
        if not category:
            continue
        if category not in CATEGORY_SCOPE_TYPE:
            raise ValueError(f'Unknown pykrx enrichment category: {category}')
        if category in seen:
            continue
        seen.add(category)
        normalized.append(category)
    return normalized or list(DEFAULT_PYKRX_ENRICHMENT_CATEGORIES)


def _prepare_rows_for_persistence(
    rows: Sequence[dict[str, Any]],
    *,
    point_in_time_recovery: bool,
) -> list[dict[str, Any]]:
    if not point_in_time_recovery:
        return list(rows)
    return [
        {
            **row,
            # Historical enrichment may reference a symbol that has since
            # moved market, changed name/source, or become inactive. The row
            # is date-scoped evidence; it must never refresh current Symbol.
            'skip_symbol_upsert': True,
        }
        for row in rows
    ]


def _to_int(value: Any) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value) if math.isfinite(value) else None
    text = str(value).strip().replace(',', '')
    if not text:
        return None
    try:
        return int(float(text))
    except (TypeError, ValueError, OverflowError):
        return None


def _to_float(value: Any, *, digits: int = 6) -> float | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return float(int(value))
    if isinstance(value, (int, float)):
        number = float(value)
        return round(number, digits) if math.isfinite(number) else None
    text = str(value).strip().replace(',', '')
    if not text:
        return None
    try:
        number = float(text)
        return round(number, digits) if math.isfinite(number) else None
    except (TypeError, ValueError, OverflowError):
        return None


def _parse_captured_at(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace('Z', '+00:00'))
    except ValueError:
        return None


def _coverage_status(*, present_rows: int, expected_rows: int) -> str:
    if expected_rows <= 0:
        return 'missing'
    if present_rows <= 0:
        return 'missing'
    if present_rows >= expected_rows:
        return 'complete'
    return 'partial'


def _row_market(row: dict[str, Any]) -> str:
    return str(row.get('market') or '').strip().upper()


def _has_valuation_snapshot_payload(rows: Sequence[dict[str, Any]]) -> bool:
    for row in rows:
        payload = dict(row.get('payload') or {})
        if any(payload.get(key) is not None for key in VALUATION_SNAPSHOT_REQUIRED_METRIC_KEYS):
            return True
    return False


def _normalize_symbol(value: Any) -> str:
    text = str(value or '').strip().upper()
    return text.zfill(6) if text.isdigit() else text


def _symbol_membership_sha256(symbols: Sequence[str] | set[str]) -> str:
    normalized = sorted({_normalize_symbol(symbol) for symbol in symbols if _normalize_symbol(symbol)})
    return hashlib.sha256('\n'.join(normalized).encode('utf-8')).hexdigest()


def _frame_value(frame: pd.DataFrame, symbol: str, column: Any) -> Any:
    if frame.empty or symbol not in frame.index:
        return None
    try:
        return frame.loc[symbol, column]
    except KeyError:
        return None


def _frame_value_candidates(frame: pd.DataFrame, symbol: str, *columns: Any) -> Any:
    for column in columns:
        value = _frame_value(frame, symbol, column)
        if value is None:
            continue
        try:
            if pd.isna(value):
                continue
        except TypeError:
            pass
        return value
    return None


def _frame_has_positive_metric(frame: pd.DataFrame, *columns: Any) -> bool:
    for ticker in frame.index.tolist():
        value = _to_float(_frame_value_candidates(frame, ticker, *columns))
        if value is not None and value > 0:
            return True
    return False


def _frame_has_any_positive_metric(frame: pd.DataFrame, *columns: Any) -> bool:
    for ticker in frame.index.tolist():
        for column in columns:
            value = _to_float(_frame_value(frame, ticker, column))
            if value is not None and value > 0:
                return True
    return False


def _first_present(*values: Any) -> Any:
    for value in values:
        if value is None:
            continue
        try:
            if pd.isna(value):
                continue
        except TypeError:
            pass
        return value
    return None


class PykrxEnrichmentService:
    def __init__(self) -> None:
        self._request_pause_seconds = max(
            0.0,
            float(getattr(settings, 'eod_pykrx_enrichment_pause_seconds', 0.05) or 0.0),
        )
        self._retry_count = max(
            0,
            int(getattr(settings, 'eod_pykrx_enrichment_retry_count', 2) or 0),
        )
        self._retry_backoff_seconds = max(
            0.0,
            float(getattr(settings, 'eod_pykrx_enrichment_retry_backoff_seconds', 0.5) or 0.0),
        )

    def collect_for_market_date(
        self,
        *,
        market_date: str | date,
        markets: Sequence[str],
        symbols: Sequence[str] | None = None,
        categories: Sequence[str] | None = None,
        allow_kis_candidate_fallback: bool = False,
        point_in_time_recovery: bool = False,
        use_full_market_coverage: bool = False,
    ) -> dict[str, Any]:
        market_date_iso = _normalize_iso_date(market_date)
        market_date_yyyymmdd = _normalize_yyyymmdd(market_date)
        point_in_time_recovery = bool(
            point_in_time_recovery
            or date.fromisoformat(market_date_iso)
            < _current_krx_calendar_date()
        )
        normalized_markets = _normalize_markets(markets) or ['KOSPI', 'KOSDAQ', 'KONEX']
        normalized_symbols = _normalize_symbols(symbols)
        resolved_categories = _normalize_categories(categories)
        price_universe_rows = (
            self._daily_price_rows_for_fallback(
                market_date_iso=market_date_iso,
                markets=normalized_markets,
                symbol_filter=set(),
            )
            if not normalized_symbols
            else []
        )
        active_symbol_rows = (
            load_symbols(
                is_active=True,
                markets=normalized_markets,
                symbols=None,
                limit=5000,
            )
            if use_full_market_coverage
            else
            [
                {
                    'symbol': item['symbol'],
                    'market': item['market'],
                    'name_ko': item.get('name_ko'),
                    'standard_code': item.get('standard_code'),
                }
                for item in price_universe_rows
            ]
            if price_universe_rows
            else load_symbols(
                is_active=True,
                markets=normalized_markets,
                symbols=normalized_symbols or None,
                limit=max(len(normalized_symbols), 5000) if normalized_symbols else 5000,
            )
        )
        symbol_meta = {item['symbol']: item for item in active_symbol_rows}
        symbol_filter = set(normalized_symbols) if normalized_symbols else {item['symbol'] for item in active_symbol_rows}
        cache: dict[tuple[Any, ...], pd.DataFrame] = {}
        category_results: dict[str, dict[str, Any]] = {}
        categories_with_errors: list[str] = []
        required_collection_symbol_filter = (
            None if use_full_market_coverage else symbol_filter
        )

        for category in resolved_categories:
            category_rows: list[dict[str, Any]]
            if category == 'sector_classification':
                category_rows, details = self._collect_sector_classification_rows(
                    market_date_iso=market_date_iso,
                    market_date_yyyymmdd=market_date_yyyymmdd,
                    markets=normalized_markets,
                    symbol_filter=required_collection_symbol_filter,
                    symbol_meta=symbol_meta,
                    cache=cache,
                )
            elif category == 'market_breadth':
                category_rows, details = self._collect_market_breadth_rows(
                    market_date_iso=market_date_iso,
                    markets=normalized_markets,
                    active_symbol_rows=active_symbol_rows,
                )
            elif category == 'investor_flow_market':
                category_rows, details = self._collect_market_investor_flow_rows(
                    market_date_iso=market_date_iso,
                    market_date_yyyymmdd=market_date_yyyymmdd,
                    markets=normalized_markets,
                    cache=cache,
                )
            elif category == 'investor_flow_symbol':
                category_rows, details = self._collect_symbol_investor_flow_rows(
                    market_date_iso=market_date_iso,
                    market_date_yyyymmdd=market_date_yyyymmdd,
                    markets=normalized_markets,
                    symbol_filter=required_collection_symbol_filter,
                    symbol_meta=symbol_meta,
                    cache=cache,
                )
            elif category == 'short_selling':
                category_rows, details = self._collect_short_selling_rows(
                    market_date_iso=market_date_iso,
                    market_date_yyyymmdd=market_date_yyyymmdd,
                    markets=normalized_markets,
                    symbol_filter=required_collection_symbol_filter,
                    symbol_meta=symbol_meta,
                    cache=cache,
                )
            elif category == 'valuation_snapshot':
                category_rows, details = self._collect_valuation_snapshot_rows(
                    market_date_iso=market_date_iso,
                    market_date_yyyymmdd=market_date_yyyymmdd,
                    markets=normalized_markets,
                    symbol_filter=symbol_filter,
                    symbol_meta=symbol_meta,
                    cache=cache,
                )
            elif category == 'foreign_ownership':
                category_rows, details = self._collect_foreign_ownership_rows(
                    market_date_iso=market_date_iso,
                    market_date_yyyymmdd=market_date_yyyymmdd,
                    markets=normalized_markets,
                    symbol_filter=symbol_filter,
                    symbol_meta=symbol_meta,
                    cache=cache,
                )
            else:  # pragma: no cover - guarded by validation
                raise ValueError(f'Unsupported pykrx enrichment category: {category}')

            if (
                allow_kis_candidate_fallback
                and not point_in_time_recovery
                and normalized_symbols
                and category in {'investor_flow_symbol', 'short_selling'}
            ):
                present_symbols = {
                    _normalize_symbol(row.get('symbol') or row.get('scope_key'))
                    for row in category_rows
                }
                missing_symbols = [symbol for symbol in normalized_symbols if symbol not in present_symbols]
                if missing_symbols:
                    kis_rows, kis_details = self._collect_kis_candidate_fallback_rows(
                        category=category,
                        market_date_iso=market_date_iso,
                        market_date_yyyymmdd=market_date_yyyymmdd,
                        symbols=missing_symbols,
                        symbol_meta=symbol_meta,
                    )
                    category_rows.extend(kis_rows)
                    details['kis_candidate_fallback'] = kis_details

            category_rows = _prepare_rows_for_persistence(
                category_rows,
                point_in_time_recovery=point_in_time_recovery,
            )
            if (
                use_full_market_coverage
                and category
                in {
                    'sector_classification',
                    'investor_flow_market',
                    'investor_flow_symbol',
                    'short_selling',
                }
            ):
                provider_rows = [
                    row for row in category_rows if row.get('source') == 'pykrx'
                ]
                non_provider_rows = [
                    row for row in category_rows if row.get('source') != 'pykrx'
                ]
                supported_replace_markets = CATEGORY_SUPPORTED_MARKETS.get(
                    category
                )
                replace_markets = [
                    market
                    for market in normalized_markets
                    if supported_replace_markets is None
                    or market in supported_replace_markets
                ]
                persisted_count = upsert_daily_pykrx_enrichment_rows(
                    rows=provider_rows,
                    replace_scopes=[
                        {
                            'market_date': market_date_iso,
                            'market': market,
                            'category': category,
                            'source': 'pykrx',
                        }
                        for market in replace_markets
                    ],
                )
                if non_provider_rows:
                    persisted_count += upsert_daily_pykrx_enrichment_rows(
                        rows=non_provider_rows
                    )
            else:
                persisted_count = upsert_daily_pykrx_enrichment_rows(
                    rows=category_rows
                )
            should_fallback = persisted_count <= 0 and (details.get('errors') or category in SYMBOL_ENRICHMENT_CATEGORIES)
            if (
                category == 'valuation_snapshot'
                and persisted_count > 0
                and details.get('errors')
                and not _has_valuation_snapshot_payload(category_rows)
            ):
                should_fallback = True
            fallback_result: dict[str, Any] | None = None
            if should_fallback:
                fallback_rows, fallback_result = self._collect_fallback_rows(
                    category=category,
                    market_date_iso=market_date_iso,
                    markets=normalized_markets,
                    symbol_filter=symbol_filter,
                    symbol_meta=symbol_meta,
                    point_in_time_recovery=point_in_time_recovery,
                )
                if fallback_rows:
                    fallback_rows = _prepare_rows_for_persistence(
                        fallback_rows,
                        point_in_time_recovery=point_in_time_recovery,
                    )
                    persisted_count = upsert_daily_pykrx_enrichment_rows(
                        rows=fallback_rows
                    )
                    details['fallback'] = {
                        **fallback_result,
                        'rows_persisted': persisted_count,
                    }
            details['rows_persisted'] = persisted_count
            details['status'] = (
                'recovered'
                if persisted_count > 0 and fallback_result is not None
                else 'completed'
                if persisted_count > 0 and not details.get('errors')
                else 'partial'
            )
            if details.get('errors') and persisted_count <= 0:
                categories_with_errors.append(category)
            category_results[category] = details

        full_market_coverage_categories = [
            category
            for category in resolved_categories
            if use_full_market_coverage
            and category in FULL_MARKET_COVERAGE_CATEGORIES
        ]
        requested_symbol_coverage_categories = [
            category
            for category in resolved_categories
            if category not in full_market_coverage_categories
        ]
        coverage_parts: list[dict[str, Any]] = []
        if full_market_coverage_categories:
            coverage_parts.append(
                self._build_coverage_summary(
                    market_date=market_date_iso,
                    markets=normalized_markets,
                    symbols=None,
                    categories=full_market_coverage_categories,
                )
            )
        if requested_symbol_coverage_categories:
            coverage_parts.append(
                self._build_coverage_summary(
                    market_date=market_date_iso,
                    markets=normalized_markets,
                    symbols=normalized_symbols,
                    categories=requested_symbol_coverage_categories,
                )
            )
        coverage = {
            'market_date': market_date_iso,
            'requested_symbol_count': max(
                (
                    int(part.get('requested_symbol_count') or 0)
                    for part in coverage_parts
                ),
                default=0,
            ),
            'requested_market_count': max(
                (
                    int(part.get('requested_market_count') or 0)
                    for part in coverage_parts
                ),
                default=0,
            ),
            'categories': {
                category: payload
                for part in coverage_parts
                for category, payload in dict(
                    part.get('categories') or {}
                ).items()
            },
        }
        recovered_categories = [
            category
            for category, details in category_results.items()
            if details.get('fallback') and int(details.get('rows_persisted') or 0) > 0
        ]
        status = 'partial' if categories_with_errors else ('recovered' if recovered_categories else 'completed')
        _log_pykrx_enrichment_fetch(
            'collect_for_market_date',
            status,
            market_date=market_date_iso,
            markets=normalized_markets,
            symbols_requested=normalized_symbols or None,
            categories_requested=resolved_categories,
            category_row_counts={
                category: int((details or {}).get('rows_persisted') or 0)
                for category, details in category_results.items()
            },
            failed_categories=categories_with_errors,
            recovered_categories=recovered_categories,
            requested_symbol_count=coverage.get('requested_symbol_count'),
            requested_market_count=coverage.get('requested_market_count'),
        )
        return {
            'market_date': market_date_iso,
            'point_in_time_recovery': point_in_time_recovery,
            'markets': normalized_markets,
            'symbols_requested': normalized_symbols or None,
            'coverage_scope': (
                'mixed'
                if full_market_coverage_categories
                and requested_symbol_coverage_categories
                and normalized_symbols
                else 'full_market'
                if full_market_coverage_categories
                else 'requested_symbols'
            ),
            'categories_requested': resolved_categories,
            'status': status,
            'categories': category_results,
            'coverage': coverage,
        }

    def _collect_kis_candidate_fallback_rows(
        self,
        *,
        category: str,
        market_date_iso: str,
        market_date_yyyymmdd: str,
        symbols: Sequence[str],
        symbol_meta: dict[str, dict[str, Any]],
    ) -> tuple[list[dict[str, Any]], dict[str, Any]]:
        async def collect() -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
            rows: list[dict[str, Any]] = []
            errors: list[dict[str, Any]] = []
            for symbol in symbols:
                try:
                    if category == 'investor_flow_symbol':
                        response = await kis_real_client.fetch_domestic_investor_trade_daily(
                            symbol,
                            trade_date=market_date_yyyymmdd,
                        )
                    else:
                        response = await kis_real_client.fetch_domestic_daily_short_sale(
                            symbol,
                            start_date=market_date_yyyymmdd,
                            end_date=market_date_yyyymmdd,
                        )
                    output_rows = response.get('output2') or response.get('output1') or []
                    if isinstance(output_rows, dict):
                        output_rows = [output_rows]
                    record = next(
                        (
                            item for item in output_rows
                            if isinstance(item, dict)
                            and str(item.get('stck_bsop_date') or '') == market_date_yyyymmdd
                        ),
                        None,
                    )
                    if record is None:
                        errors.append({'symbol': symbol, 'error': 'no_matching_trade_date'})
                        continue
                    symbol_info = symbol_meta.get(symbol) or {}
                    payload = (
                        self._build_kis_investor_flow_payload(record=record, market=symbol_info.get('market'))
                        if category == 'investor_flow_symbol'
                        else self._build_kis_short_selling_payload(record=record, market=symbol_info.get('market'))
                    )
                    rows.append(
                        {
                            'market_date': market_date_iso,
                            'market': symbol_info.get('market'),
                            'category': category,
                            'scope_type': 'symbol',
                            'scope_key': symbol,
                            'source': 'kis_open_api',
                            'symbol': symbol,
                            'skip_symbol_upsert': True,
                            'name_ko': symbol_info.get('name_ko') or symbol,
                            'standard_code': symbol_info.get('standard_code'),
                            'payload': payload,
                        }
                    )
                except Exception as exc:  # noqa: BLE001 - preserve PyKRX partial data
                    errors.append({'symbol': symbol, 'error': str(exc)})
            return rows, errors

        try:
            asyncio.get_running_loop()
        except RuntimeError:
            rows, errors = asyncio.run(collect())
        else:
            # This synchronous collector is also reached from async FastAPI
            # execution paths. Run its coroutine on a dedicated loop/thread
            # instead of nesting asyncio.run() in the request loop.
            from concurrent.futures import ThreadPoolExecutor

            with ThreadPoolExecutor(max_workers=1, thread_name_prefix='kis-enrichment') as executor:
                rows, errors = executor.submit(lambda: asyncio.run(collect())).result()
        return rows, {
            'provider': 'kis_open_api',
            'category': category,
            'symbols_requested': list(symbols),
            'rows_prepared': len(rows),
            'errors': errors,
        }

    @staticmethod
    def _build_kis_investor_flow_payload(*, record: dict[str, Any], market: str | None) -> dict[str, Any]:
        groups = {
            '개인': ('prsn',),
            '외국인': ('frgn',),
            '기관합계': ('orgn',),
        }
        investor_groups: dict[str, dict[str, int | None]] = {}
        for label, (prefix,) in groups.items():
            investor_groups[label] = {
                'sell_volume': _to_int(record.get(f'{prefix}_seln_vol')),
                'buy_volume': _to_int(record.get(f'{prefix}_shnu_vol')),
                'net_volume': _to_int(record.get(f'{prefix}_ntby_qty')),
                'sell_value': _to_int(record.get(f'{prefix}_seln_tr_pbmn')),
                'buy_value': _to_int(record.get(f'{prefix}_shnu_tr_pbmn')),
                'net_value': _to_int(record.get(f'{prefix}_ntby_tr_pbmn')),
            }
        return {
            'market': market,
            'window_start_date': str(record.get('stck_bsop_date') or ''),
            'window_end_date': str(record.get('stck_bsop_date') or ''),
            'investor_groups': investor_groups,
            'provider': 'kis_open_api',
        }

    @staticmethod
    def _build_kis_short_selling_payload(*, record: dict[str, Any], market: str | None) -> dict[str, Any]:
        return {
            'market': market,
            'short_volume': {
                'short_volume': _to_int(record.get('ssts_cntg_qty')),
                'total_volume': _to_int(record.get('stnd_vol_smtn') or record.get('acml_vol')),
                'short_volume_ratio_pct': _to_float(record.get('ssts_vol_rlim')),
                'short_value': _to_int(record.get('ssts_tr_pbmn')),
                'total_value': _to_int(record.get('stnd_tr_pbmn_smtn') or record.get('acml_tr_pbmn')),
                'short_value_ratio_pct': _to_float(record.get('ssts_tr_pbmn_rlim')),
            },
            'short_balance': {},
            'missing_fields': ['short_balance'],
            'provider': 'kis_open_api',
        }

    def inspect(
        self,
        *,
        market_date: str | date,
        categories: Sequence[str] | None = None,
        markets: Sequence[str] | None = None,
        symbols: Sequence[str] | None = None,
        limit: int = 200,
    ) -> dict[str, Any]:
        market_date_iso = _normalize_iso_date(market_date)
        normalized_markets = _normalize_markets(markets)
        normalized_symbols = _normalize_symbols(symbols)
        resolved_categories = _normalize_categories(categories)
        rows = load_daily_pykrx_enrichment_rows(
            market_date=market_date_iso,
            categories=resolved_categories,
            markets=normalized_markets or None,
            symbols=normalized_symbols or None,
            limit=limit,
        )
        coverage = self._build_coverage_summary(
            market_date=market_date_iso,
            markets=normalized_markets or None,
            symbols=normalized_symbols or None,
            categories=resolved_categories,
        )
        return {
            'market_date': market_date_iso,
            'categories_requested': resolved_categories,
            'markets_requested': normalized_markets or None,
            'symbols_requested': normalized_symbols or None,
            'summary': {
                'rows_returned': len(rows),
                'categories_returned': sorted({item['category'] for item in rows}),
            },
            'coverage': coverage,
            'items': rows,
        }

    def load_symbol_day_context_inputs(
        self,
        *,
        market_date: str | date,
        symbols: Sequence[str],
        markets: Sequence[str] | None = None,
    ) -> dict[str, Any]:
        market_date_iso = _normalize_iso_date(market_date)
        normalized_symbols = _normalize_symbols(symbols)
        resolved_markets = _normalize_markets(markets)
        symbol_rows = load_daily_pykrx_enrichment_rows(
            market_date=market_date_iso,
            categories=list(SYMBOL_ENRICHMENT_CATEGORIES),
            symbols=normalized_symbols,
            limit=None,
        )
        if not resolved_markets:
            symbol_meta = load_symbols(symbols=normalized_symbols, limit=max(len(normalized_symbols), 200))
            resolved_markets = sorted({str(item.get('market') or '').strip().upper() for item in symbol_meta if item.get('market')})
        market_rows = load_daily_pykrx_enrichment_rows(
            market_date=market_date_iso,
            categories=list(MARKET_ENRICHMENT_CATEGORIES),
            markets=resolved_markets or None,
            limit=None,
        )
        sector_rows = load_daily_pykrx_enrichment_rows(
            market_date=market_date_iso,
            categories=['sector_classification'],
            markets=resolved_markets or None,
            limit=20000,
        )

        by_symbol: dict[str, dict[str, dict[str, Any]]] = defaultdict(dict)
        by_market: dict[str, dict[str, dict[str, Any]]] = defaultdict(dict)
        sector_rows_by_market_and_symbol: dict[str, dict[str, dict[str, Any]]] = defaultdict(dict)

        for row in sorted(symbol_rows, key=_decision_source_priority):
            if not is_decision_usable_pykrx_enrichment_row(row):
                continue
            symbol = row.get('symbol')
            if symbol:
                by_symbol[symbol][row['category']] = row
        for row in sorted(market_rows, key=_decision_source_priority):
            if not is_decision_usable_pykrx_enrichment_row(row):
                continue
            market = str(row.get('market') or '').strip().upper()
            if market:
                by_market[market][row['category']] = row
        for row in sorted(sector_rows, key=_decision_source_priority):
            if not is_decision_usable_pykrx_enrichment_row(row):
                continue
            market = str(row.get('market') or '').strip().upper()
            symbol = str(row.get('symbol') or row.get('scope_key') or '').strip().upper()
            if market and symbol:
                sector_rows_by_market_and_symbol[market][symbol] = row

        return {
            'by_symbol': dict(by_symbol),
            'by_market': dict(by_market),
            'sector_rows_by_market': {
                market: list(rows_by_symbol.values())
                for market, rows_by_symbol in sector_rows_by_market_and_symbol.items()
            },
        }

    def _fetch_frame(
        self,
        cache: dict[tuple[Any, ...], pd.DataFrame],
        cache_key: tuple[Any, ...],
        operation: str,
        func: Any,
        *args: Any,
        require_non_empty: bool = False,
        frame_validator: Any | None = None,
        **kwargs: Any,
    ) -> pd.DataFrame:
        if cache_key in cache:
            cached_frame = cache[cache_key]
            if (
                (not require_non_empty or not cached_frame.empty)
                and (frame_validator is None or bool(frame_validator(cached_frame)))
            ):
                return cached_frame
            del cache[cache_key]

        last_error: Exception | None = None
        for attempt in range(self._retry_count + 1):
            started_at = time.perf_counter()
            try:
                with pykrx_request_context(
                    timeout_seconds=settings.eod_pykrx_request_timeout_seconds
                ):
                    frame = func(*args, **kwargs)
                if not isinstance(frame, pd.DataFrame):
                    raise TypeError(f'{operation} returned non-DataFrame payload')
                if require_non_empty and frame.empty:
                    raise RuntimeError(f'{operation} returned empty DataFrame')
                if frame_validator is not None and not bool(frame_validator(frame)):
                    raise RuntimeError(f'{operation} returned semantically invalid DataFrame')
                cache[cache_key] = frame
                _log_pykrx_enrichment_fetch(
                    operation,
                    'completed',
                    attempt=attempt + 1,
                    row_count=len(frame.index),
                    column_count=len(frame.columns),
                    duration_ms=round((time.perf_counter() - started_at) * 1000, 3),
                )
                if self._request_pause_seconds > 0:
                    time.sleep(self._request_pause_seconds)
                return frame
            except Exception as exc:  # pragma: no cover - defensive production guard
                last_error = exc
                logger.warning('PyKRX enrichment fetch failed for %s: %s', operation, exc)
                _log_pykrx_enrichment_fetch(
                    operation,
                    'failed',
                    attempt=attempt + 1,
                    duration_ms=round((time.perf_counter() - started_at) * 1000, 3),
                    error_type=type(exc).__name__,
                    error_message=str(exc),
                )
                if attempt >= self._retry_count:
                    raise
                if self._retry_backoff_seconds > 0:
                    time.sleep(self._retry_backoff_seconds * (attempt + 1))
        raise last_error or RuntimeError(f'PyKRX enrichment fetch failed for {operation}')

    def _active_symbol_filter(
        self,
        *,
        symbol_filter: set[str] | None,
        symbol_meta: dict[str, dict[str, Any]],
    ) -> set[str]:
        if symbol_filter is None:
            return set()
        return symbol_filter or set(symbol_meta)

    def _collect_fallback_rows(
        self,
        *,
        category: str,
        market_date_iso: str,
        markets: Sequence[str],
        symbol_filter: set[str],
        symbol_meta: dict[str, dict[str, Any]],
        point_in_time_recovery: bool = False,
    ) -> tuple[list[dict[str, Any]], dict[str, Any]]:
        if category in TIME_SENSITIVE_NO_CARRY_FORWARD_CATEGORIES:
            return [], {
                'category': category,
                'scope_type': CATEGORY_SCOPE_TYPE[category],
                'fallback_source': None,
                'rows_prepared': 0,
                'reason': 'time_sensitive_carry_forward_not_allowed',
            }
        if category == 'valuation_snapshot':
            latest_price_date = self._latest_price_bar_valuation_date(market_date_iso=market_date_iso, markets=markets)
            if latest_price_date is not None:
                return self._collect_valuation_snapshot_rows_from_daily_price_bars(
                    market_date_iso=market_date_iso,
                    markets=markets,
                    symbol_filter=symbol_filter,
                    symbol_meta=symbol_meta,
                    source_market_date_iso=latest_price_date.isoformat(),
                )
            return self._collect_valuation_snapshot_rows_from_daily_price_bars(
                market_date_iso=market_date_iso,
                markets=markets,
                symbol_filter=symbol_filter,
                symbol_meta=symbol_meta,
            )
        if category == 'sector_classification':
            if point_in_time_recovery:
                # Daily price bars carry no dated sector identity. Falling
                # back through today's Symbol metadata would stamp hindsight
                # into the requested date. Only previously persisted rows
                # strictly before the target are admissible.
                return self._collect_rows_from_latest_enrichment(
                    category=category,
                    market_date_iso=market_date_iso,
                    markets=markets,
                    symbol_filter=symbol_filter,
                    symbol_meta=symbol_meta,
                )
            return self._collect_sector_rows_from_symbols(
                market_date_iso=market_date_iso,
                markets=markets,
                symbol_filter=symbol_filter,
                symbol_meta=symbol_meta,
            )
        return self._collect_rows_from_latest_enrichment(
            category=category,
            market_date_iso=market_date_iso,
            markets=markets,
            symbol_filter=symbol_filter,
            symbol_meta=symbol_meta,
        )

    def _latest_price_bar_valuation_date(
        self,
        *,
        market_date_iso: str,
        markets: Sequence[str],
    ) -> date | None:
        with session_scope(
            statement_timeout_ms=PYKRX_ENRICHMENT_READ_TIMEOUT_MS
        ) as session:
            stmt = (
                select(DailyPriceBar.trade_date)
                .where(
                    DailyPriceBar.trade_date < date.fromisoformat(market_date_iso),
                    DailyPriceBar.per.isnot(None),
                    DailyPriceBar.pbr.isnot(None),
                )
                .order_by(DailyPriceBar.trade_date.desc())
                .limit(1)
            )
            return session.execute(stmt).scalar_one_or_none()

    def _daily_price_rows_for_fallback(
        self,
        *,
        market_date_iso: str,
        markets: Sequence[str],
        symbol_filter: set[str],
        source_market_date_iso: str | None = None,
    ) -> list[dict[str, Any]]:
        source_market_date_iso = source_market_date_iso or market_date_iso
        normalized_markets = set(_normalize_markets(markets))
        allowed_symbols = {_normalize_symbol(symbol) for symbol in symbol_filter if _normalize_symbol(symbol)}
        use_sql_symbol_filter = (
            bool(allowed_symbols)
            and len(allowed_symbols) <= MAX_SQL_SYMBOL_FILTER_SIZE
        )
        with session_scope(
            statement_timeout_ms=PYKRX_ENRICHMENT_READ_TIMEOUT_MS
        ) as session:
            stmt = (
                select(DailyPriceBar, Symbol)
                .join(Symbol, Symbol.id == DailyPriceBar.symbol_id)
                .where(DailyPriceBar.trade_date == date.fromisoformat(source_market_date_iso))
            )
            if normalized_markets:
                stmt = stmt.where(Symbol.market.in_(sorted(normalized_markets)))
            if use_sql_symbol_filter:
                stmt = stmt.where(Symbol.symbol.in_(sorted(allowed_symbols)))
            stmt = stmt.order_by(Symbol.market.asc(), Symbol.symbol.asc())
            rows = session.execute(stmt).all()
            return [
                {
                    'symbol': symbol.symbol,
                    'market': symbol.market,
                    'name_ko': symbol.name_ko,
                    'standard_code': symbol.standard_code,
                    'open_price': int(bar.open_price),
                    'high_price': int(bar.high_price),
                    'low_price': int(bar.low_price),
                    'close_price': int(bar.close_price),
                    'volume': int(bar.volume),
                    'turnover': int(bar.turnover),
                    'per': _to_float(bar.per),
                    'pbr': _to_float(bar.pbr),
                    'bps': _to_float(bar.bps),
                    'eps': _to_float(bar.eps),
                    'dividend_yield': _to_float(bar.dividend_yield),
                    'dps': _to_float(bar.dps),
                    'source': bar.source,
                }
                for bar, symbol in rows
                if not allowed_symbols or symbol.symbol in allowed_symbols
            ]

    def _collect_valuation_snapshot_rows_from_daily_price_bars(
        self,
        *,
        market_date_iso: str,
        markets: Sequence[str],
        symbol_filter: set[str],
        symbol_meta: dict[str, dict[str, Any]],
        source_market_date_iso: str | None = None,
    ) -> tuple[list[dict[str, Any]], dict[str, Any]]:
        source_market_date_iso = source_market_date_iso or market_date_iso
        price_rows = self._daily_price_rows_for_fallback(
            market_date_iso=market_date_iso,
            markets=markets,
            symbol_filter=symbol_filter,
            source_market_date_iso=source_market_date_iso,
        )
        rows: list[dict[str, Any]] = []
        for price_row in price_rows:
            symbol = _normalize_symbol(price_row.get('symbol'))
            symbol_info = symbol_meta.get(symbol) or {}
            rows.append(
                {
                    'market_date': market_date_iso,
                    'market': price_row.get('market'),
                    'category': 'valuation_snapshot',
                    'scope_type': 'symbol',
                    'scope_key': symbol,
                    'source': (
                        'daily_price_bars_carry_forward'
                        if source_market_date_iso != market_date_iso
                        else 'daily_price_bars_reconstructed'
                    ),
                    'symbol': symbol,
                    'skip_symbol_upsert': True,
                    'name_ko': price_row.get('name_ko') or symbol_info.get('name_ko') or symbol,
                    'standard_code': price_row.get('standard_code') or symbol_info.get('standard_code'),
                    'payload': {
                        'market': price_row.get('market'),
                        'close_price': price_row.get('close_price'),
                        'market_cap': None,
                        'volume': price_row.get('volume'),
                        'trading_value': price_row.get('turnover'),
                        'listed_shares': None,
                        'bps': price_row.get('bps'),
                        'per': price_row.get('per'),
                        'pbr': price_row.get('pbr'),
                        'eps': price_row.get('eps'),
                        'dividend_yield': price_row.get('dividend_yield'),
                        'dps': price_row.get('dps'),
                        'reconstructed_from': 'daily_price_bars',
                        'carried_forward_from_market_date': (
                            source_market_date_iso if source_market_date_iso != market_date_iso else None
                        ),
                        'price_source': price_row.get('source'),
                    },
                }
            )
        return rows, {
            'category': 'valuation_snapshot',
            'scope_type': 'symbol',
            'fallback_source': 'daily_price_bars',
            'source_market_date': source_market_date_iso,
            'rows_prepared': len(rows),
        }

    def _collect_sector_rows_from_symbols(
        self,
        *,
        market_date_iso: str,
        markets: Sequence[str],
        symbol_filter: set[str],
        symbol_meta: dict[str, dict[str, Any]],
    ) -> tuple[list[dict[str, Any]], dict[str, Any]]:
        price_rows = self._daily_price_rows_for_fallback(
            market_date_iso=market_date_iso,
            markets=[market for market in markets if market in SECTOR_SUPPORTED_MARKETS],
            symbol_filter=symbol_filter,
        )
        rows: list[dict[str, Any]] = []
        for price_row in price_rows:
            symbol = _normalize_symbol(price_row.get('symbol'))
            symbol_info = symbol_meta.get(symbol) or {}
            rows.append(
                {
                    'market_date': market_date_iso,
                    'market': price_row.get('market'),
                    'category': 'sector_classification',
                    'scope_type': 'symbol',
                    'scope_key': symbol,
                    'source': 'daily_price_bars_reconstructed',
                    'symbol': symbol,
                    'skip_symbol_upsert': True,
                    'name_ko': price_row.get('name_ko') or symbol_info.get('name_ko') or symbol,
                    'standard_code': price_row.get('standard_code') or symbol_info.get('standard_code'),
                    'payload': {
                        'market': price_row.get('market'),
                        'sector_name': symbol_info.get('sector_name'),
                        'close_price': price_row.get('close_price'),
                        'change': None,
                        'change_pct': None,
                        'market_cap': None,
                        'reconstructed_from': 'daily_price_bars',
                        'price_source': price_row.get('source'),
                    },
                }
            )
        return rows, {
            'category': 'sector_classification',
            'scope_type': 'symbol',
            'fallback_source': 'daily_price_bars',
            'rows_prepared': len(rows),
        }

    def _collect_rows_from_latest_enrichment(
        self,
        *,
        category: str,
        market_date_iso: str,
        markets: Sequence[str],
        symbol_filter: set[str],
        symbol_meta: dict[str, dict[str, Any]],
    ) -> tuple[list[dict[str, Any]], dict[str, Any]]:
        normalized_markets = set(_normalize_markets(markets))
        allowed_symbols = {_normalize_symbol(symbol) for symbol in symbol_filter if _normalize_symbol(symbol)}
        with session_scope(
            statement_timeout_ms=PYKRX_ENRICHMENT_READ_TIMEOUT_MS
        ) as session:
            candidate_dates = [
                item[0]
                for item in session.execute(
                    select(DailyPykrxEnrichmentRow.market_date)
                    .where(
                        DailyPykrxEnrichmentRow.market_date < date.fromisoformat(market_date_iso),
                        DailyPykrxEnrichmentRow.category == category,
                    )
                    .group_by(DailyPykrxEnrichmentRow.market_date)
                    .order_by(DailyPykrxEnrichmentRow.market_date.desc())
                    .limit(20)
                ).all()
            ]
            latest_date = candidate_dates[0] if candidate_dates else None
            if latest_date is None:
                return [], {
                    'category': category,
                    'scope_type': CATEGORY_SCOPE_TYPE[category],
                    'fallback_source': 'latest_pykrx_enrichment',
                    'rows_prepared': 0,
                    'reason': 'no_prior_enrichment_rows',
                }
            rows: list[dict[str, Any]] = []
            for candidate_date in candidate_dates:
                stmt = (
                    select(DailyPykrxEnrichmentRow, Symbol)
                    .outerjoin(Symbol, Symbol.id == DailyPykrxEnrichmentRow.symbol_id)
                    .where(
                        DailyPykrxEnrichmentRow.market_date == candidate_date,
                        DailyPykrxEnrichmentRow.category == category,
                    )
                )
                if normalized_markets:
                    stmt = stmt.where(DailyPykrxEnrichmentRow.market.in_(sorted(normalized_markets)))
                candidate_rows = [
                    {
                        'market': row.market,
                        'scope_type': row.scope_type,
                        'scope_key': row.scope_key,
                        'payload': dict(row.payload or {}),
                        'symbol': symbol.symbol if symbol is not None else None,
                        'name_ko': symbol.name_ko if symbol is not None else None,
                        'standard_code': symbol.standard_code if symbol is not None else None,
                    }
                    for row, symbol in session.execute(stmt).all()
                ]
                if category != 'valuation_snapshot' or _has_valuation_snapshot_payload(candidate_rows):
                    latest_date = candidate_date
                    rows = candidate_rows
                    break
            if not rows:
                return [], {
                    'category': category,
                    'scope_type': CATEGORY_SCOPE_TYPE[category],
                    'fallback_source': 'latest_pykrx_enrichment',
                    'rows_prepared': 0,
                    'reason': 'no_prior_enrichment_rows_with_payload',
                }

        prepared: list[dict[str, Any]] = []
        for row in rows:
            scope_type = str(row.get('scope_type') or CATEGORY_SCOPE_TYPE[category]).strip().lower()
            scope_key = str(row.get('scope_key') or '').strip()
            normalized_symbol = _normalize_symbol(row.get('symbol') or scope_key)
            if scope_type == 'symbol' and allowed_symbols and normalized_symbol not in allowed_symbols:
                continue
            symbol_info = symbol_meta.get(normalized_symbol) or {}
            payload = dict(row.get('payload') or {})
            payload['carried_forward_from_market_date'] = latest_date.isoformat()
            payload['carried_forward_reason'] = 'pykrx_upstream_unavailable'
            prepared.append(
                {
                    'market_date': market_date_iso,
                    'market': row.get('market'),
                    'category': category,
                    'scope_type': scope_type,
                    'scope_key': normalized_symbol if scope_type == 'symbol' else scope_key,
                    'source': 'pykrx_carry_forward',
                    'symbol': normalized_symbol if scope_type == 'symbol' else None,
                    'skip_symbol_upsert': True,
                    'name_ko': (
                        row.get('name_ko')
                        if row.get('name_ko')
                        else symbol_info.get('name_ko')
                        if symbol_info
                        else normalized_symbol
                    ),
                    'standard_code': (
                        row.get('standard_code')
                        if row.get('standard_code')
                        else symbol_info.get('standard_code')
                        if symbol_info
                        else None
                    ),
                    'payload': payload,
                }
            )
        return prepared, {
            'category': category,
            'scope_type': CATEGORY_SCOPE_TYPE[category],
            'fallback_source': 'latest_pykrx_enrichment',
            'source_market_date': latest_date.isoformat(),
            'rows_prepared': len(prepared),
        }

    def _collect_sector_classification_rows(
        self,
        *,
        market_date_iso: str,
        market_date_yyyymmdd: str,
        markets: Sequence[str],
        symbol_filter: set[str] | None,
        symbol_meta: dict[str, dict[str, Any]],
        cache: dict[tuple[Any, ...], pd.DataFrame],
    ) -> tuple[list[dict[str, Any]], dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        errors: list[dict[str, Any]] = []
        allowed_symbols = self._active_symbol_filter(symbol_filter=symbol_filter, symbol_meta=symbol_meta)
        for market in markets:
            if market not in SECTOR_SUPPORTED_MARKETS:
                continue
            reference_symbols: set[str] = set()
            try:
                membership_frame = self._fetch_frame(
                    cache=cache,
                    cache_key=('stock_universe_membership', market_date_yyyymmdd, market),
                    operation='stock_universe_membership',
                    func=_market_ticker_membership_frame,
                    require_non_empty=True,
                    date=market_date_yyyymmdd,
                    market=market,
                )
                reference_symbols = {
                    _normalize_symbol(ticker)
                    for ticker in membership_frame.index.tolist()
                    if _normalize_symbol(ticker)
                }
            except Exception as exc:
                errors.append(
                    {
                        'market': market,
                        'operation': 'stock_universe_membership',
                        'error': str(exc),
                    }
                )
            try:
                frame = self._fetch_frame(
                    cache=cache,
                    cache_key=('sector_classification', market_date_yyyymmdd, market),
                    operation='sector_classification',
                    func=stock.get_market_sector_classifications,
                    require_non_empty=True,
                    date=market_date_yyyymmdd,
                    market=market,
                )
            except Exception as exc:
                errors.append({'market': market, 'operation': 'sector_classification', 'error': str(exc)})
                continue

            frame_symbols = {
                _normalize_symbol(ticker)
                for ticker in frame.index.tolist()
                if _normalize_symbol(ticker)
            }
            reference_count = len(reference_symbols) if reference_symbols else None
            reference_sha256 = (
                _symbol_membership_sha256(reference_symbols)
                if reference_symbols
                else None
            )

            for ticker, record in frame.iterrows():
                symbol = str(ticker).strip().upper().zfill(6) if str(ticker).strip().isdigit() else str(ticker).strip().upper()
                if allowed_symbols and symbol not in allowed_symbols:
                    continue
                symbol_info = symbol_meta.get(symbol) or {}
                rows.append(
                    {
                        'market_date': market_date_iso,
                        'market': market,
                        'category': 'sector_classification',
                        'scope_type': 'symbol',
                        'scope_key': symbol,
                        'source': 'pykrx',
                        'symbol': symbol,
                        'name_ko': record.get('종목명') or symbol_info.get('name_ko') or symbol,
                        'standard_code': symbol_info.get('standard_code'),
                        'payload': {
                            'market': market,
                            'universe_market_complete': bool(
                                reference_symbols and frame_symbols == reference_symbols
                            ),
                            'universe_market_row_count': len(frame.index),
                            'universe_reference_source': 'pykrx_market_ticker_list',
                            'universe_reference_count': reference_count,
                            'universe_reference_sha256': reference_sha256,
                            'sector_name': record.get('업종명'),
                            'close_price': _to_int(record.get('종가')),
                            'change': _to_float(record.get('대비')),
                            'change_pct': _to_float(record.get('등락률')),
                            'market_cap': _to_int(record.get('시가총액')),
                        },
                    }
                )

        return rows, {
            'category': 'sector_classification',
            'scope_type': 'symbol',
            'rows_prepared': len(rows),
            'errors': errors,
        }

    def _collect_market_breadth_rows(
        self,
        *,
        market_date_iso: str,
        markets: Sequence[str],
        active_symbol_rows: Sequence[dict[str, Any]],
    ) -> tuple[list[dict[str, Any]], dict[str, Any]]:
        symbols = [
            item['symbol']
            for item in active_symbol_rows
            if str(item.get('market') or '').strip().upper() in markets
        ]
        breadth_rows = load_daily_market_relative_contexts(
            market_date=market_date_iso,
            symbols=symbols,
            markets=markets,
            statement_timeout_ms=PYKRX_ENRICHMENT_READ_TIMEOUT_MS,
        )
        payload_by_market: dict[str, dict[str, Any]] = {}
        for row in breadth_rows:
            market = str(row.get('market') or '').strip().upper()
            if market not in markets or market in payload_by_market:
                continue
            breadth = row.get('breadth') if isinstance(row.get('breadth'), dict) else None
            if not breadth:
                continue
            market_symbol_count = int(breadth.get('market_symbol_count') or 0)
            up_count = int(breadth.get('up_count') or 0)
            down_count = int(breadth.get('down_count') or 0)
            flat_count = int(breadth.get('flat_count') or 0)
            comparable = max(up_count + down_count + flat_count, 0)
            payload_by_market[market] = {
                'market': market,
                'basis': breadth.get('basis') or 'close_vs_previous_close',
                'market_symbol_count': market_symbol_count,
                'up_count': up_count,
                'down_count': down_count,
                'flat_count': flat_count,
                'missing_previous_close_count': int(breadth.get('missing_previous_close_count') or 0),
                'advance_decline_ratio': round(up_count / down_count, 6) if down_count > 0 else None,
                'up_ratio': round(up_count / comparable, 6) if comparable > 0 else None,
            }
        rows = [
            {
                'market_date': market_date_iso,
                'market': market,
                'category': 'market_breadth',
                'scope_type': 'market',
                'scope_key': market,
                'source': 'pykrx_derived',
                'payload': payload,
            }
            for market, payload in payload_by_market.items()
        ]
        return rows, {
            'category': 'market_breadth',
            'scope_type': 'market',
            'rows_prepared': len(rows),
            'errors': [],
        }

    def _collect_market_investor_flow_rows(
        self,
        *,
        market_date_iso: str,
        market_date_yyyymmdd: str,
        markets: Sequence[str],
        cache: dict[tuple[Any, ...], pd.DataFrame],
    ) -> tuple[list[dict[str, Any]], dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        errors: list[dict[str, Any]] = []
        for market in markets:
            try:
                value_frame = self._fetch_frame(
                    cache=cache,
                    cache_key=('investor_flow_market_value', market_date_yyyymmdd, market),
                    operation='investor_flow_market_value',
                    func=stock.get_market_trading_value_by_investor,
                    require_non_empty=True,
                    frame_validator=lambda frame: _frame_has_any_positive_metric(
                        frame,
                        '매도',
                        '매수',
                    ),
                    fromdate=market_date_yyyymmdd,
                    todate=market_date_yyyymmdd,
                    ticker=market,
                )
                volume_frame = self._fetch_frame(
                    cache=cache,
                    cache_key=('investor_flow_market_volume', market_date_yyyymmdd, market),
                    operation='investor_flow_market_volume',
                    func=stock.get_market_trading_volume_by_investor,
                    require_non_empty=True,
                    frame_validator=lambda frame: _frame_has_any_positive_metric(
                        frame,
                        '매도',
                        '매수',
                    ),
                    fromdate=market_date_yyyymmdd,
                    todate=market_date_yyyymmdd,
                    ticker=market,
                )
            except Exception as exc:
                errors.append({'market': market, 'error': str(exc)})
                continue

            common_investors = set(value_frame.index.tolist()) & set(volume_frame.index.tolist())
            if not set(SYMBOL_INVESTOR_GROUPS).issubset(common_investors):
                errors.append(
                    {
                        'market': market,
                        'error': 'required investor groups are incomplete across value/volume frames',
                    }
                )
                continue

            investor_groups = []
            for investor in sorted(common_investors):
                investor_groups.append(
                    {
                        'investor_type': str(investor),
                        'sell_value': _to_int(_frame_value(value_frame, investor, '매도')),
                        'buy_value': _to_int(_frame_value(value_frame, investor, '매수')),
                        'net_value': _to_int(_frame_value(value_frame, investor, '순매수')),
                        'sell_volume': _to_int(_frame_value(volume_frame, investor, '매도')),
                        'buy_volume': _to_int(_frame_value(volume_frame, investor, '매수')),
                        'net_volume': _to_int(_frame_value(volume_frame, investor, '순매수')),
                    }
                )
            if not investor_groups:
                continue
            rows.append(
                {
                    'market_date': market_date_iso,
                    'market': market,
                    'category': 'investor_flow_market',
                    'scope_type': 'market',
                    'scope_key': market,
                    'source': 'pykrx',
                    'payload': {
                        'market': market,
                        'window_start_date': market_date_iso,
                        'window_end_date': market_date_iso,
                        'investor_groups': investor_groups,
                    },
                }
            )

        return rows, {
            'category': 'investor_flow_market',
            'scope_type': 'market',
            'rows_prepared': len(rows),
            'errors': errors,
        }

    def _collect_symbol_investor_flow_rows(
        self,
        *,
        market_date_iso: str,
        market_date_yyyymmdd: str,
        markets: Sequence[str],
        symbol_filter: set[str] | None,
        symbol_meta: dict[str, dict[str, Any]],
        cache: dict[tuple[Any, ...], pd.DataFrame],
    ) -> tuple[list[dict[str, Any]], dict[str, Any]]:
        rows_by_symbol: dict[str, dict[str, Any]] = {}
        errors: list[dict[str, Any]] = []
        allowed_symbols = self._active_symbol_filter(symbol_filter=symbol_filter, symbol_meta=symbol_meta)
        for market in markets:
            frames_by_investor: dict[str, pd.DataFrame] = {}
            for investor in SYMBOL_INVESTOR_GROUPS:
                try:
                    frames_by_investor[investor] = self._fetch_frame(
                        cache=cache,
                        cache_key=('investor_flow_symbol', market_date_yyyymmdd, market, investor),
                        operation='investor_flow_symbol',
                        func=stock.get_market_net_purchases_of_equities_by_ticker,
                        require_non_empty=True,
                        frame_validator=lambda frame: _frame_has_any_positive_metric(
                            frame,
                            '매도거래량',
                            '매수거래량',
                            '매도거래대금',
                            '매수거래대금',
                        ),
                        fromdate=market_date_yyyymmdd,
                        todate=market_date_yyyymmdd,
                        market=market,
                        investor=investor,
                    )
                except Exception as exc:
                    errors.append({'market': market, 'investor': investor, 'error': str(exc)})
            if len(frames_by_investor) != len(SYMBOL_INVESTOR_GROUPS):
                continue

            common_tickers = set.intersection(
                *(set(frame.index.tolist()) for frame in frames_by_investor.values())
            )
            for investor, frame in frames_by_investor.items():
                for ticker in sorted(common_tickers):
                    record = frame.loc[ticker]
                    symbol = str(ticker).strip().upper().zfill(6) if str(ticker).strip().isdigit() else str(ticker).strip().upper()
                    if allowed_symbols and symbol not in allowed_symbols:
                        continue
                    symbol_info = symbol_meta.get(symbol) or {}
                    entry = rows_by_symbol.setdefault(
                        symbol,
                        {
                            'market_date': market_date_iso,
                            'market': market,
                            'category': 'investor_flow_symbol',
                            'scope_type': 'symbol',
                            'scope_key': symbol,
                            'source': 'pykrx',
                            'symbol': symbol,
                            'name_ko': record.get('종목명') or symbol_info.get('name_ko') or symbol,
                            'standard_code': symbol_info.get('standard_code'),
                            'payload': {
                                'market': market,
                                'window_start_date': market_date_iso,
                                'window_end_date': market_date_iso,
                                'investor_groups': {},
                            },
                        },
                    )
                    entry['payload']['investor_groups'][investor] = {
                        'sell_volume': _to_int(record.get('매도거래량')),
                        'buy_volume': _to_int(record.get('매수거래량')),
                        'net_volume': _to_int(record.get('순매수거래량')),
                        'sell_value': _to_int(record.get('매도거래대금')),
                        'buy_value': _to_int(record.get('매수거래대금')),
                        'net_value': _to_int(record.get('순매수거래대금')),
                    }

        rows = list(rows_by_symbol.values())
        return rows, {
            'category': 'investor_flow_symbol',
            'scope_type': 'symbol',
            'rows_prepared': len(rows),
            'errors': errors,
        }

    def _collect_short_selling_rows(
        self,
        *,
        market_date_iso: str,
        market_date_yyyymmdd: str,
        markets: Sequence[str],
        symbol_filter: set[str] | None,
        symbol_meta: dict[str, dict[str, Any]],
        cache: dict[tuple[Any, ...], pd.DataFrame],
    ) -> tuple[list[dict[str, Any]], dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        errors: list[dict[str, Any]] = []
        allowed_symbols = self._active_symbol_filter(symbol_filter=symbol_filter, symbol_meta=symbol_meta)
        for market in markets:
            if market not in SHORTING_SUPPORTED_MARKETS:
                continue
            reference_symbols: set[str] = set()
            if symbol_filter is None:
                try:
                    membership_frame = self._fetch_frame(
                        cache=cache,
                        cache_key=('stock_universe_membership', market_date_yyyymmdd, market),
                        operation='stock_universe_membership',
                        func=_market_ticker_membership_frame,
                        require_non_empty=True,
                        date=market_date_yyyymmdd,
                        market=market,
                    )
                    reference_symbols = {
                        _normalize_symbol(ticker)
                        for ticker in membership_frame.index.tolist()
                        if _normalize_symbol(ticker)
                    }
                except Exception as exc:
                    errors.append(
                        {
                            'market': market,
                            'operation': 'stock_universe_membership',
                            'error': str(exc),
                        }
                    )
            try:
                volume_frame = self._fetch_frame(
                    cache=cache,
                    cache_key=('short_selling_volume', market_date_yyyymmdd, market),
                    operation='short_selling_volume',
                    func=stock.get_shorting_volume_by_ticker,
                    require_non_empty=True,
                    frame_validator=lambda frame: _frame_has_positive_metric(
                        frame,
                        ('거래량', '매수'),
                        '매수',
                    ),
                    date=market_date_yyyymmdd,
                    market=market,
                    include=['주식'],
                )
            except Exception as exc:
                errors.append({'market': market, 'operation': 'short_selling_volume', 'error': str(exc)})
                continue

            try:
                value_frame = self._fetch_frame(
                    cache=cache,
                    cache_key=('short_selling_value', market_date_yyyymmdd, market),
                    operation='short_selling_value',
                    func=stock.get_shorting_value_by_ticker,
                    require_non_empty=True,
                    frame_validator=lambda frame: _frame_has_positive_metric(
                        frame,
                        ('거래대금', '매수'),
                        '매수',
                    ),
                    date=market_date_yyyymmdd,
                    market=market,
                    include=['주식'],
                )
            except Exception as exc:
                errors.append({'market': market, 'operation': 'short_selling_value', 'error': str(exc)})
                value_frame = pd.DataFrame()

            try:
                balance_frame = self._fetch_frame(
                    cache=cache,
                    cache_key=('short_selling_balance', market_date_yyyymmdd, market),
                    operation='short_selling_balance',
                    func=stock.get_shorting_balance_by_ticker,
                    require_non_empty=True,
                    date=market_date_yyyymmdd,
                    market=market,
                )
            except Exception as exc:
                errors.append({'market': market, 'operation': 'short_selling_balance', 'error': str(exc)})
                balance_frame = pd.DataFrame()

            symbols_for_market = {
                str(item).strip().upper().zfill(6) if str(item).strip().isdigit() else str(item).strip().upper()
                for item in volume_frame.index.tolist()
            }
            reference_count = len(reference_symbols) if reference_symbols else None
            reference_sha256 = (
                _symbol_membership_sha256(reference_symbols)
                if reference_symbols
                else None
            )
            for symbol in sorted(symbols_for_market):
                if allowed_symbols and symbol not in allowed_symbols:
                    continue
                symbol_info = symbol_meta.get(symbol) or {}
                payload = {
                    'market': market,
                    'universe_market_complete': bool(
                        reference_symbols and symbols_for_market == reference_symbols
                    ),
                    'universe_market_row_count': len(volume_frame.index),
                    'universe_reference_source': 'pykrx_market_ticker_list',
                    'universe_reference_count': reference_count,
                    'universe_reference_sha256': reference_sha256,
                    'short_volume': {
                        'short_volume': _to_int(
                            _frame_value_candidates(volume_frame, symbol, ('거래량', '공매도'), '공매도')
                        ),
                        'total_volume': _to_int(
                            _frame_value_candidates(volume_frame, symbol, ('거래량', '매수'), '매수')
                        ),
                        'short_volume_ratio_pct': _to_float(
                            _frame_value_candidates(volume_frame, symbol, ('거래량', '비중'), '비중')
                        ),
                        'short_value': _to_int(
                            _first_present(
                                _frame_value_candidates(value_frame, symbol, ('거래대금', '공매도'), '공매도'),
                                _frame_value_candidates(volume_frame, symbol, ('거래대금', '공매도')),
                            )
                        ),
                        'total_value': _to_int(
                            _first_present(
                                _frame_value_candidates(value_frame, symbol, ('거래대금', '매수'), '매수'),
                                _frame_value_candidates(volume_frame, symbol, ('거래대금', '매수')),
                            )
                        ),
                        'short_value_ratio_pct': _to_float(
                            _first_present(
                                _frame_value_candidates(value_frame, symbol, ('거래대금', '비중'), '비중'),
                                _frame_value_candidates(volume_frame, symbol, ('거래대금', '비중')),
                            )
                        ),
                    },
                    'short_balance': {
                        'short_balance_qty': _to_int(_frame_value(balance_frame, symbol, '공매도잔고')),
                        'listed_shares': _to_int(_frame_value(balance_frame, symbol, '상장주식수')),
                        'short_balance_amount': _to_int(_frame_value(balance_frame, symbol, '공매도금액')),
                        'market_cap': _to_int(_frame_value(balance_frame, symbol, '시가총액')),
                        'short_balance_ratio_pct': _to_float(_frame_value(balance_frame, symbol, '비중')),
                    },
                }
                missing_fields: list[str] = []
                if not _metric_group_complete(payload['short_volume'], SHORT_VOLUME_METRIC_KEYS):
                    missing_fields.append('short_volume')
                if not _metric_group_complete(payload['short_volume'], SHORT_VALUE_METRIC_KEYS):
                    missing_fields.append('short_value')
                if not _metric_group_complete(payload['short_balance'], SHORT_BALANCE_METRIC_KEYS):
                    missing_fields.append('short_balance')
                payload['missing_fields'] = missing_fields
                rows.append(
                    {
                        'market_date': market_date_iso,
                        'market': market,
                        'category': 'short_selling',
                        'scope_type': 'symbol',
                        'scope_key': symbol,
                        'source': 'pykrx',
                        'symbol': symbol,
                        'name_ko': symbol_info.get('name_ko') or symbol,
                        'standard_code': symbol_info.get('standard_code'),
                        'payload': payload,
                    }
                )

        return rows, {
            'category': 'short_selling',
            'scope_type': 'symbol',
            'rows_prepared': len(rows),
            'errors': errors,
        }

    def _collect_valuation_snapshot_rows(
        self,
        *,
        market_date_iso: str,
        market_date_yyyymmdd: str,
        markets: Sequence[str],
        symbol_filter: set[str],
        symbol_meta: dict[str, dict[str, Any]],
        cache: dict[tuple[Any, ...], pd.DataFrame],
    ) -> tuple[list[dict[str, Any]], dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        errors: list[dict[str, Any]] = []
        allowed_symbols = self._active_symbol_filter(symbol_filter=symbol_filter, symbol_meta=symbol_meta)
        for market in markets:
            try:
                market_cap_frame = self._fetch_frame(
                    cache=cache,
                    cache_key=('valuation_market_cap', market_date_yyyymmdd, market),
                    operation='valuation_market_cap',
                    func=stock.get_market_cap_by_ticker,
                    date=market_date_yyyymmdd,
                    market=market,
                )
            except Exception as exc:
                errors.append({'market': market, 'operation': 'valuation_market_cap', 'error': str(exc)})
                continue

            try:
                fundamental_frame = self._fetch_frame(
                    cache=cache,
                    cache_key=('valuation_fundamentals', market_date_yyyymmdd, market),
                    operation='valuation_fundamentals',
                    func=stock.get_market_fundamental_by_ticker,
                    date=market_date_yyyymmdd,
                    market=market,
                )
            except Exception as exc:
                errors.append({'market': market, 'operation': 'valuation_fundamentals', 'error': str(exc)})
                fundamental_frame = pd.DataFrame()

            symbols_for_market = {
                str(item).strip().upper().zfill(6) if str(item).strip().isdigit() else str(item).strip().upper()
                for item in set(market_cap_frame.index.tolist()) | set(fundamental_frame.index.tolist())
            }
            for symbol in sorted(symbols_for_market):
                if allowed_symbols and symbol not in allowed_symbols:
                    continue
                symbol_info = symbol_meta.get(symbol) or {}
                rows.append(
                    {
                        'market_date': market_date_iso,
                        'market': market,
                        'category': 'valuation_snapshot',
                        'scope_type': 'symbol',
                        'scope_key': symbol,
                        'source': 'pykrx',
                        'symbol': symbol,
                        'name_ko': symbol_info.get('name_ko') or symbol,
                        'standard_code': symbol_info.get('standard_code'),
                        'payload': {
                            'market': market,
                            'close_price': _to_int(_frame_value(market_cap_frame, symbol, '종가')),
                            'market_cap': _to_int(_frame_value(market_cap_frame, symbol, '시가총액')),
                            'volume': _to_int(_frame_value(market_cap_frame, symbol, '거래량')),
                            'trading_value': _to_int(_frame_value(market_cap_frame, symbol, '거래대금')),
                            'listed_shares': _to_int(_frame_value(market_cap_frame, symbol, '상장주식수')),
                            'bps': _to_float(_frame_value(fundamental_frame, symbol, 'BPS')),
                            'per': _to_float(_frame_value(fundamental_frame, symbol, 'PER')),
                            'pbr': _to_float(_frame_value(fundamental_frame, symbol, 'PBR')),
                            'eps': _to_float(_frame_value(fundamental_frame, symbol, 'EPS')),
                            'dividend_yield': _to_float(_frame_value(fundamental_frame, symbol, 'DIV')),
                            'dps': _to_float(_frame_value(fundamental_frame, symbol, 'DPS')),
                        },
                    }
                )
        return rows, {
            'category': 'valuation_snapshot',
            'scope_type': 'symbol',
            'rows_prepared': len(rows),
            'errors': errors,
        }

    def _collect_foreign_ownership_rows(
        self,
        *,
        market_date_iso: str,
        market_date_yyyymmdd: str,
        markets: Sequence[str],
        symbol_filter: set[str],
        symbol_meta: dict[str, dict[str, Any]],
        cache: dict[tuple[Any, ...], pd.DataFrame],
    ) -> tuple[list[dict[str, Any]], dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        errors: list[dict[str, Any]] = []
        allowed_symbols = self._active_symbol_filter(symbol_filter=symbol_filter, symbol_meta=symbol_meta)
        for market in markets:
            try:
                frame = self._fetch_frame(
                    cache=cache,
                    cache_key=('foreign_ownership', market_date_yyyymmdd, market),
                    operation='foreign_ownership',
                    func=stock.get_exhaustion_rates_of_foreign_investment_by_ticker,
                    date=market_date_yyyymmdd,
                    market=market,
                )
            except Exception as exc:
                errors.append({'market': market, 'error': str(exc)})
                continue

            for ticker, record in frame.iterrows():
                symbol = str(ticker).strip().upper().zfill(6) if str(ticker).strip().isdigit() else str(ticker).strip().upper()
                if allowed_symbols and symbol not in allowed_symbols:
                    continue
                symbol_info = symbol_meta.get(symbol) or {}
                rows.append(
                    {
                        'market_date': market_date_iso,
                        'market': market,
                        'category': 'foreign_ownership',
                        'scope_type': 'symbol',
                        'scope_key': symbol,
                        'source': 'pykrx',
                        'symbol': symbol,
                        'name_ko': symbol_info.get('name_ko') or symbol,
                        'standard_code': symbol_info.get('standard_code'),
                        'payload': {
                            'market': market,
                            'listed_shares': _to_int(record.get('상장주식수')),
                            'foreign_holding_qty': _to_int(record.get('보유수량')),
                            'foreign_ownership_ratio_pct': _to_float(record.get('지분율')),
                            'foreign_limit_qty': _to_int(record.get('한도수량')),
                            'foreign_limit_exhaustion_rate_pct': _to_float(record.get('한도소진률')),
                        },
                    }
                )

        return rows, {
            'category': 'foreign_ownership',
            'scope_type': 'symbol',
            'rows_prepared': len(rows),
            'errors': errors,
        }

    def _build_coverage_summary(
        self,
        *,
        market_date: str,
        markets: Sequence[str] | None,
        symbols: Sequence[str] | None,
        categories: Sequence[str],
    ) -> dict[str, Any]:
        normalized_markets = _normalize_markets(markets)
        normalized_symbols = _normalize_symbols(symbols)
        active_symbol_rows = self._load_coverage_symbol_rows(
            market_date=market_date,
            markets=normalized_markets,
            symbols=normalized_symbols,
        )
        expected_symbol_count = len(normalized_symbols) if normalized_symbols else len(active_symbol_rows)
        requested_markets = normalized_markets or sorted(
            {_row_market(row) for row in active_symbol_rows if row.get('market')}
        )
        expected_market_count = len(requested_markets)
        normalized_symbol_set = set(normalized_symbols)
        symbol_categories = [category for category in categories if CATEGORY_SCOPE_TYPE[category] == 'symbol']
        market_categories = [category for category in categories if CATEGORY_SCOPE_TYPE[category] == 'market']
        time_sensitive_categories = set(symbol_categories) & TIME_SENSITIVE_NO_CARRY_FORWARD_CATEGORIES
        coverage_symbol_categories = list(symbol_categories)
        if time_sensitive_categories and not normalized_symbols and 'sector_classification' not in coverage_symbol_categories:
            coverage_symbol_categories.append('sector_classification')
        if (
            not normalized_symbols
            and ('sector_classification' in coverage_symbol_categories or time_sensitive_categories)
            and 'short_selling' not in coverage_symbol_categories
        ):
            coverage_symbol_categories.append('short_selling')
        rows: list[dict[str, Any]] = []
        if coverage_symbol_categories:
            rows.extend(
                load_daily_pykrx_enrichment_rows(
                    market_date=market_date,
                    categories=coverage_symbol_categories,
                    markets=normalized_markets or None,
                    symbols=normalized_symbols or None,
                    limit=None,
                    statement_timeout_ms=PYKRX_ENRICHMENT_READ_TIMEOUT_MS,
                    order_results=False,
                    include_symbol_metadata=False,
                )
            )
        if market_categories:
            rows.extend(
                load_daily_pykrx_enrichment_rows(
                    market_date=market_date,
                    categories=market_categories,
                    markets=normalized_markets or None,
                    limit=None,
                    statement_timeout_ms=PYKRX_ENRICHMENT_READ_TIMEOUT_MS,
                    order_results=False,
                    include_symbol_metadata=False,
                )
            )

        fresh_sector_rows = [
            item
            for item in rows
            if item.get('category') == 'sector_classification'
            and item.get('source') == 'pykrx'
            and (item.get('symbol') or item.get('scope_key'))
            and is_decision_usable_pykrx_enrichment_row(item)
        ]
        fresh_short_rows = [
            item
            for item in rows
            if item.get('category') == 'short_selling'
            and item.get('source') == 'pykrx'
            and (item.get('symbol') or item.get('scope_key'))
            and _metric_group_complete(
                dict((item.get('payload') or {}).get('short_volume') or {}),
                SHORT_VOLUME_METRIC_KEYS,
            )
        ]
        requested_stock_markets = set(requested_markets) & SECTOR_SUPPORTED_MARKETS
        complete_fresh_sector_markets: set[str] = set()
        for market in requested_stock_markets:
            market_rows = [item for item in fresh_sector_rows if _row_market(item) == market]
            market_short_rows = [item for item in fresh_short_rows if _row_market(item) == market]
            market_symbols = {
                str(item.get('symbol') or item.get('scope_key') or '').strip().upper()
                for item in market_rows
                if str(item.get('symbol') or item.get('scope_key') or '').strip()
            }
            short_symbols = {
                str(item.get('symbol') or item.get('scope_key') or '').strip().upper()
                for item in fresh_short_rows
                if _row_market(item) == market
                and str(item.get('symbol') or item.get('scope_key') or '').strip()
            }
            reference_counts = {
                int((item.get('payload') or {}).get('universe_reference_count') or 0)
                for item in market_rows
            }
            reference_hashes = {
                str((item.get('payload') or {}).get('universe_reference_sha256') or '').strip()
                for item in market_rows
            }
            reference_sources = {
                str((item.get('payload') or {}).get('universe_reference_source') or '').strip()
                for item in market_rows
            }
            reference_matches_sector = bool(
                market_symbols
                and reference_counts == {len(market_symbols)}
                and reference_hashes == {_symbol_membership_sha256(market_symbols)}
                and reference_sources == {'pykrx_market_ticker_list'}
            )
            short_market_has_activity = any(
                (_to_int(((item.get('payload') or {}).get('short_volume') or {}).get('total_volume')) or 0) > 0
                for item in market_short_rows
            )
            if (
                reference_matches_sector
                and market_symbols == short_symbols
                and short_market_has_activity
            ):
                complete_fresh_sector_markets.add(market)
        fresh_stock_universe_available = bool(requested_stock_markets) and requested_stock_markets.issubset(
            complete_fresh_sector_markets
        )
        fresh_stock_symbols = {
            str(item.get('symbol') or item.get('scope_key') or '').strip().upper()
            for item in fresh_sector_rows
            if _row_market(item) in requested_stock_markets
            if str(item.get('symbol') or item.get('scope_key') or '').strip()
        }
        requested_short_markets = set(requested_markets) & SHORTING_SUPPORTED_MARKETS
        requested_non_sector_short_markets = (
            requested_short_markets - SECTOR_SUPPORTED_MARKETS
        )
        complete_fresh_non_sector_short_markets: set[str] = set()
        fresh_non_sector_short_symbols: set[str] = set()
        for market in requested_non_sector_short_markets:
            market_short_rows = [
                item for item in fresh_short_rows if _row_market(item) == market
            ]
            market_short_symbols = {
                str(item.get('symbol') or item.get('scope_key') or '').strip().upper()
                for item in market_short_rows
                if str(item.get('symbol') or item.get('scope_key') or '').strip()
            }
            reference_counts = {
                int((item.get('payload') or {}).get('universe_reference_count') or 0)
                for item in market_short_rows
            }
            reference_hashes = {
                str((item.get('payload') or {}).get('universe_reference_sha256') or '').strip()
                for item in market_short_rows
            }
            reference_sources = {
                str((item.get('payload') or {}).get('universe_reference_source') or '').strip()
                for item in market_short_rows
            }
            reported_market_row_counts = {
                int((item.get('payload') or {}).get('universe_market_row_count') or 0)
                for item in market_short_rows
            }
            completion_flags = {
                bool((item.get('payload') or {}).get('universe_market_complete'))
                for item in market_short_rows
            }
            reference_matches_short = bool(
                market_short_symbols
                and reference_counts == {len(market_short_symbols)}
                and reference_hashes
                == {_symbol_membership_sha256(market_short_symbols)}
                and reference_sources == {'pykrx_market_ticker_list'}
                and reported_market_row_counts == {len(market_short_symbols)}
                and completion_flags == {True}
            )
            short_market_has_activity = any(
                (_to_int(((item.get('payload') or {}).get('short_volume') or {}).get('total_volume')) or 0) > 0
                for item in market_short_rows
            )
            if reference_matches_short and short_market_has_activity:
                complete_fresh_non_sector_short_markets.add(market)
                fresh_non_sector_short_symbols.update(market_short_symbols)
        fresh_short_universe_available = (
            (not requested_stock_markets or fresh_stock_universe_available)
            and requested_non_sector_short_markets.issubset(
                complete_fresh_non_sector_short_markets
            )
        )
        fresh_short_symbols = fresh_stock_symbols | fresh_non_sector_short_symbols
        positive_volume_stock_symbols: set[str] = set()
        positive_volume_universe_available = False
        if (
            'investor_flow_symbol' in time_sensitive_categories
            and fresh_stock_universe_available
        ):
            price_rows = self._daily_price_rows_for_fallback(
                market_date_iso=market_date,
                markets=normalized_markets,
                symbol_filter=fresh_stock_symbols,
            )
            price_row_symbols = {
                str(item.get('symbol') or '').strip().upper()
                for item in price_rows
                if str(item.get('symbol') or '').strip()
            }
            positive_volume_stock_symbols = {
                str(item.get('symbol') or '').strip().upper()
                for item in price_rows
                if str(item.get('symbol') or '').strip()
                and int(item.get('volume') or 0) > 0
            } & fresh_stock_symbols
            positive_volume_universe_available = bool(positive_volume_stock_symbols) and fresh_stock_symbols.issubset(
                price_row_symbols
            )

        category_summaries: dict[str, dict[str, Any]] = {}
        for category in categories:
            supported_markets = CATEGORY_SUPPORTED_MARKETS.get(category)
            category_rows = [
                item
                for item in rows
                if item['category'] == category
                and (supported_markets is None or _row_market(item) in supported_markets)
                and is_decision_usable_pykrx_enrichment_row(item)
            ]
            present_scope_keys = {
                item.get('symbol') or item['scope_key']
                for item in category_rows
                if (item.get('symbol') or item['scope_key'])
            }
            expected_scope_keys: set[str] | None = None
            expectation_status = 'available'
            expected_universe_source = 'active_collection_universe'
            if CATEGORY_SCOPE_TYPE[category] == 'symbol':
                expected_rows = expected_symbol_count
                if supported_markets is not None:
                    if normalized_symbols:
                        unsupported_known_symbols = {
                            str(item.get('symbol') or '').strip().upper()
                            for item in active_symbol_rows
                            if str(item.get('symbol') or '').strip().upper() in normalized_symbol_set
                            and _row_market(item)
                            and _row_market(item) not in supported_markets
                        }
                        expected_rows = max(len(normalized_symbols) - len(unsupported_known_symbols), 0)
                    else:
                        expected_rows = sum(1 for item in active_symbol_rows if _row_market(item) in supported_markets)
                if normalized_symbols:
                    expected_scope_keys = set(normalized_symbols)
                    if supported_markets is not None:
                        unsupported_known_symbols = {
                            str(item.get('symbol') or '').strip().upper()
                            for item in active_symbol_rows
                            if str(item.get('symbol') or '').strip().upper() in normalized_symbol_set
                            and _row_market(item)
                            and _row_market(item) not in supported_markets
                        }
                        expected_scope_keys -= unsupported_known_symbols
                    expected_rows = len(expected_scope_keys)
                    if (
                        supported_markets is not None
                        and unsupported_known_symbols == normalized_symbol_set
                    ):
                        expectation_status = 'not_applicable'
                        expected_universe_source = (
                            'explicit_requested_symbols_unsupported_markets'
                        )
                    else:
                        expected_universe_source = 'explicit_requested_symbols'
                elif (
                    supported_markets is not None
                    and requested_markets
                    and not (set(requested_markets) & supported_markets)
                ):
                    expected_scope_keys = set()
                    expected_rows = 0
                    expectation_status = 'not_applicable'
                    expected_universe_source = 'requested_markets'
                elif category == 'sector_classification':
                    if not fresh_stock_universe_available:
                        expected_rows = None
                        expectation_status = 'unavailable'
                        expected_universe_source = 'fresh_pykrx_stock_universe_unavailable'
                    else:
                        expected_scope_keys = fresh_stock_symbols
                        expected_rows = len(expected_scope_keys)
                        expected_universe_source = 'fresh_pykrx_sector_classification'
                elif category == 'short_selling':
                    if not fresh_short_universe_available:
                        expected_rows = None
                        expectation_status = 'unavailable'
                        expected_universe_source = (
                            'fresh_pykrx_short_selling_universe_unavailable'
                        )
                    else:
                        expected_scope_keys = fresh_short_symbols
                        expected_rows = len(expected_scope_keys)
                        expected_universe_source = (
                            'fresh_pykrx_short_selling_universe'
                            if requested_non_sector_short_markets
                            else 'fresh_pykrx_sector_classification'
                        )
                elif category in TIME_SENSITIVE_NO_CARRY_FORWARD_CATEGORIES:
                    if not fresh_stock_universe_available:
                        expected_rows = None
                        expectation_status = 'unavailable'
                        expected_universe_source = 'fresh_pykrx_stock_universe_unavailable'
                    elif (
                        category == 'investor_flow_symbol'
                        and not positive_volume_universe_available
                    ):
                        expected_rows = None
                        expectation_status = 'unavailable'
                        expected_universe_source = 'same_day_positive_volume_unavailable'
                    elif category == 'investor_flow_symbol':
                        expected_scope_keys = positive_volume_stock_symbols
                        expected_rows = len(expected_scope_keys)
                        expected_universe_source = 'fresh_pykrx_sector_positive_volume'
                    else:
                        expected_scope_keys = fresh_stock_symbols
                        expected_rows = len(expected_scope_keys)
                        expected_universe_source = 'fresh_pykrx_sector_classification'
            else:
                expected_rows = expected_market_count
                if supported_markets is not None:
                    expected_rows = sum(1 for market in requested_markets if market in supported_markets)
                expected_universe_source = 'requested_markets'
            eligible_present_scope_keys = (
                present_scope_keys
                if expected_scope_keys is None
                else present_scope_keys & expected_scope_keys
            )
            present_rows = len(eligible_present_scope_keys)
            latest_captured_at = None
            latest_parsed = None
            for item in category_rows:
                parsed = _parse_captured_at(item.get('captured_at'))
                if parsed is not None and (latest_parsed is None or parsed > latest_parsed):
                    latest_parsed = parsed
                    latest_captured_at = item.get('captured_at')
            category_summaries[category] = {
                'scope_type': CATEGORY_SCOPE_TYPE[category],
                'present_rows': present_rows,
                'raw_present_rows': len(present_scope_keys),
                'expected_rows': expected_rows,
                'coverage_ratio': (
                    round(present_rows / expected_rows, 6)
                    if expected_rows is not None and expected_rows > 0
                    else None
                ),
                'coverage_status': (
                    'unavailable'
                    if expectation_status == 'unavailable'
                    else 'not_applicable'
                    if expectation_status == 'not_applicable'
                    else _coverage_status(
                        present_rows=present_rows,
                        expected_rows=expected_rows,
                    )
                ),
                'expectation_status': expectation_status,
                'expected_universe_source': expected_universe_source,
                'sources': sorted({item['source'] for item in category_rows if item.get('source')}),
                'markets': sorted({item['market'] for item in category_rows if item.get('market')}),
                'latest_captured_at': latest_captured_at,
            }

        return {
            'market_date': market_date,
            'requested_symbol_count': expected_symbol_count,
            'requested_market_count': expected_market_count,
            'categories': category_summaries,
        }

    def _load_coverage_symbol_rows(
        self,
        *,
        market_date: str,
        markets: Sequence[str],
        symbols: Sequence[str],
    ) -> list[dict[str, Any]]:
        if symbols:
            return load_symbols(
                is_active=True,
                markets=list(markets) or None,
                symbols=list(symbols),
                limit=max(len(symbols), 1),
            )
        latest_run = load_latest_eod_collection_run(
            market='KRX',
            trade_date=market_date,
            source='pykrx',
            statuses=['completed'],
            statement_timeout_ms=PYKRX_ENRICHMENT_READ_TIMEOUT_MS,
        )
        if latest_run is not None:
            persisted_symbols = load_eod_run_persisted_symbols(
                int(latest_run['id']),
                statement_timeout_ms=PYKRX_ENRICHMENT_READ_TIMEOUT_MS,
            )
            if persisted_symbols:
                return load_symbols(
                    markets=list(markets) or None,
                    symbols=persisted_symbols,
                    limit=max(len(persisted_symbols), 1),
                )
        price_rows = self._daily_price_rows_for_fallback(
            market_date_iso=market_date,
            markets=markets,
            symbol_filter=set(),
        )
        if price_rows:
            return [
                {
                    'symbol': row['symbol'],
                    'market': row['market'],
                    'name_ko': row.get('name_ko'),
                    'standard_code': row.get('standard_code'),
                }
                for row in price_rows
            ]
        return load_symbols(
            is_active=True,
            markets=list(markets) or None,
            limit=5000,
        )


pykrx_enrichment_service = PykrxEnrichmentService()
