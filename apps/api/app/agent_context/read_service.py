from __future__ import annotations

from calendar import monthrange
from datetime import date
from typing import Any, Sequence

from app.dart.fundamentals_store import DartFundamentalsRepository, dart_fundamentals_repository
from app.dart.service import FS_DIV_TO_STATEMENT_TYPE, REPORT_CODE_METADATA
from app.dart.store import DartSyncRepository, dart_sync_repository
from app.market_data_store import (
    load_daily_action_summaries,
    load_daily_signal_features,
    load_daily_technical_snapshots,
    load_symbols,
)


def _normalize_symbols(symbols: Sequence[str]) -> list[str]:
    normalized: list[str] = []
    seen: set[str] = set()
    for raw_symbol in symbols:
        for piece in (raw_symbol or '').split(','):
            symbol = piece.strip()
            if not symbol or symbol in seen:
                continue
            seen.add(symbol)
            normalized.append(symbol)
    if not normalized:
        raise ValueError('At least one symbol is required')
    return normalized


def _normalize_fs_div(fs_div: str | None) -> str | None:
    if fs_div is None:
        return None
    normalized = fs_div.strip().upper()
    if not normalized:
        return None
    if normalized not in FS_DIV_TO_STATEMENT_TYPE:
        raise ValueError('fs_div must be one of CFS or OFS')
    return normalized


def _build_period_end_date(*, business_year: int, period_end_month: int | None) -> date | None:
    if period_end_month is None:
        return None
    last_day = monthrange(business_year, period_end_month)[1]
    return date(business_year, period_end_month, last_day)


def _summarize_highlights(rows: list[dict[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    ratios: dict[str, str | None] = {}
    for row in rows:
        metric_key = row['metric_key']
        if row['metric_type'] == 'ratio':
            ratios[metric_key] = row.get('ratio_value')
            continue
        result[metric_key] = {
            'statement_type': row.get('statement_type'),
            'statement_name': row.get('statement_name'),
            'account_id': row.get('account_id'),
            'account_name': row.get('account_name'),
            'current_amount': row.get('current_amount'),
            'current_cumulative_amount': row.get('current_cumulative_amount'),
            'prior_amount': row.get('prior_amount'),
            'prior_cumulative_amount': row.get('prior_cumulative_amount'),
            'pre_prior_amount': row.get('pre_prior_amount'),
            'currency': row.get('currency'),
        }
    if ratios:
        result['ratios'] = ratios
    return result


def _build_fundamentals_snapshot(item: dict[str, Any], *, period_end_date: date | None) -> dict[str, Any]:
    report_meta = REPORT_CODE_METADATA.get(item['report_code'], {})
    statement_rows = item.get('statement_rows') or []
    statement_row_count = item.get('statement_row_count')
    return {
        'filing': {
            'rcept_no': item['rcept_no'],
            'report_name': item['disclosure'].get('report_nm'),
            'filing_category': item['filing_category'],
            'receipt_date': item['disclosure'].get('receipt_date'),
            'captured_at': item.get('captured_at'),
        },
        'period': {
            'business_year': item['business_year'],
            'report_code': item['report_code'],
            'report_label': report_meta.get('label'),
            'period_type': item['period_type'],
            'period_scope': report_meta.get('scope'),
            'quarter': report_meta.get('quarter'),
            'period_end_month': item.get('period_end_month'),
            'period_end_date': period_end_date.isoformat() if period_end_date else None,
        },
        'statement': {
            'fs_div': item['fs_div'],
            'fs_label': FS_DIV_TO_STATEMENT_TYPE.get(item['fs_div']),
            'statement_type': item['statement_type'],
            'currency': item.get('currency'),
            'source_status': item.get('source_status'),
            'source_message': item.get('source_message'),
            'row_count': statement_row_count if statement_row_count is not None else len(statement_rows),
        },
        'highlights': _summarize_highlights(item.get('highlights') or []),
    }


def _build_fundamentals_summary(items: list[dict[str, Any]], *, market_date: date) -> dict[str, Any]:
    eligible_items: list[tuple[dict[str, Any], date | None]] = []
    for item in items:
        receipt_date_raw = item['disclosure'].get('receipt_date')
        receipt_date = date.fromisoformat(receipt_date_raw) if receipt_date_raw else None
        if receipt_date is not None and receipt_date > market_date:
            continue
        period_end_date = _build_period_end_date(
            business_year=item['business_year'],
            period_end_month=item.get('period_end_month'),
        )
        eligible_items.append((item, period_end_date))

    if not eligible_items:
        return {
            'latest_snapshot': None,
            'history_summary': {
                'entries': 0,
                'latest_receipt_date': None,
                'latest_period_end_date': None,
                'available_business_years': [],
                'recent_periods': [],
            },
        }

    latest_item, latest_period_end_date = eligible_items[0]
    recent_periods: list[dict[str, Any]] = []
    business_years: list[int] = []
    seen_period_keys: set[tuple[int, str, str | None]] = set()
    for item, period_end_date in eligible_items:
        business_years.append(item['business_year'])
        period_key = (
            item['business_year'],
            item['report_code'],
            period_end_date.isoformat() if period_end_date else None,
        )
        if period_key in seen_period_keys:
            continue
        seen_period_keys.add(period_key)
        recent_periods.append(
            {
                'business_year': item['business_year'],
                'report_code': item['report_code'],
                'report_label': REPORT_CODE_METADATA.get(item['report_code'], {}).get('label'),
                'period_type': item['period_type'],
                'period_end_date': period_end_date.isoformat() if period_end_date else None,
                'receipt_date': item['disclosure'].get('receipt_date'),
                'rcept_no': item['rcept_no'],
            }
        )
        if len(recent_periods) >= 5:
            break

    latest_receipt_date = latest_item['disclosure'].get('receipt_date')
    return {
        'latest_snapshot': _build_fundamentals_snapshot(latest_item, period_end_date=latest_period_end_date),
        'history_summary': {
            'entries': len(eligible_items),
            'latest_receipt_date': latest_receipt_date,
            'latest_period_end_date': latest_period_end_date.isoformat() if latest_period_end_date else None,
            'available_business_years': sorted(set(business_years), reverse=True),
            'recent_periods': recent_periods,
        },
    }


def _serialize_disclosure(item: dict[str, Any]) -> dict[str, Any]:
    return {
        'rcept_no': item['rcept_no'],
        'corp_code': item['corp_code'],
        'corp_name': item['corp_name'],
        'report_nm': item['report_nm'],
        'filing_category': item['filing_category'],
        'is_financial_report': item['is_financial_report'],
        'receipt_date': item['receipt_date'],
        'remark': item['remark'],
        'fundamentals_capture_status': item['fundamentals_capture_status'],
    }


class AgentContextReadService:
    def __init__(
        self,
        *,
        fundamentals_repository: DartFundamentalsRepository | None = None,
        disclosure_repository: DartSyncRepository | None = None,
    ) -> None:
        self.fundamentals_repository = fundamentals_repository or dart_fundamentals_repository
        self.disclosure_repository = disclosure_repository or dart_sync_repository

    def get_symbol_day_context(
        self,
        *,
        market_date: str | date,
        symbols: Sequence[str],
        fs_div: str | None = None,
        disclosures_limit: int = 5,
    ) -> dict[str, Any]:
        normalized_symbols = _normalize_symbols(symbols)
        normalized_market_date = market_date if isinstance(market_date, date) else date.fromisoformat(market_date)
        normalized_fs_div = _normalize_fs_div(fs_div)

        symbol_rows = load_symbols(symbols=normalized_symbols, limit=max(len(normalized_symbols), 1000))
        technical_rows = load_daily_technical_snapshots(
            market_date=normalized_market_date,
            symbols=normalized_symbols,
            limit=max(len(normalized_symbols), 1000),
            fallback_window_days=3,
        )
        action_rows = load_daily_action_summaries(
            market_date=normalized_market_date,
            symbols=normalized_symbols,
            limit=max(len(normalized_symbols), 1000),
        )
        signal_feature_rows = load_daily_signal_features(
            market_date=normalized_market_date,
            symbols=normalized_symbols,
            limit=max(len(normalized_symbols), 1000),
        )
        fundamentals_rows = self.fundamentals_repository.list_stored_fundamentals_summary(
            stock_codes=normalized_symbols,
            fs_div=normalized_fs_div,
        )
        disclosures_by_symbol = self.disclosure_repository.list_recent_events_by_stock_codes(
            stock_codes=normalized_symbols,
            receipt_date_to=normalized_market_date,
            limit_per_symbol=disclosures_limit,
        )

        symbol_map = {item['symbol']: item for item in symbol_rows}
        technical_map = {item['symbol']: item for item in technical_rows}
        action_map = {item['symbol']: item for item in action_rows}
        signal_feature_map = {item['symbol']: item for item in signal_feature_rows}
        fundamentals_map: dict[str, list[dict[str, Any]]] = {symbol: [] for symbol in normalized_symbols}
        for item in fundamentals_rows:
            stock_code = item.get('stock_code')
            if stock_code in fundamentals_map:
                fundamentals_map[stock_code].append(item)

        items: list[dict[str, Any]] = []
        matched_symbols: list[str] = []
        missing_symbols: list[str] = []

        for symbol in normalized_symbols:
            technical_snapshot = technical_map.get(symbol)
            action_summary = action_map.get(symbol)
            signal_features = signal_feature_map.get(symbol)
            fundamentals = _build_fundamentals_summary(
                fundamentals_map.get(symbol, []),
                market_date=normalized_market_date,
            )
            recent_disclosures = [
                _serialize_disclosure(item) for item in disclosures_by_symbol.get(symbol, [])
            ]

            company_row = symbol_map.get(symbol)
            fundamentals_entries = fundamentals_map.get(symbol) or []
            latest_disclosure = recent_disclosures[0] if recent_disclosures else None
            company = {
                'symbol': symbol,
                'name_ko': company_row.get('name_ko') if company_row else None,
                'market': company_row.get('market') if company_row else None,
                'instrument_type': company_row.get('instrument_type') if company_row else None,
                'standard_code': company_row.get('standard_code') if company_row else None,
                'source': company_row.get('source') if company_row else None,
                'is_active': company_row.get('is_active') if company_row else None,
                'last_refreshed_at': (
                    company_row['last_refreshed_at'].isoformat()
                    if company_row and company_row.get('last_refreshed_at')
                    else None
                ),
                'dart': {
                    'corp_code': (
                        latest_disclosure.get('corp_code')
                        if latest_disclosure
                        else (fundamentals_entries[0].get('corp_code') if fundamentals_entries else None)
                    ),
                    'corp_name': (
                        latest_disclosure.get('corp_name')
                        if latest_disclosure
                        else (fundamentals_entries[0].get('corp_name') if fundamentals_entries else None)
                    ),
                },
            }

            availability = {
                'company': company_row is not None,
                'technical_snapshot': technical_snapshot is not None,
                'action_summary': action_summary is not None,
                'signal_features': signal_features is not None,
                'fundamentals': fundamentals['latest_snapshot'] is not None,
                'recent_disclosures': bool(recent_disclosures),
            }
            if any(availability.values()):
                matched_symbols.append(symbol)
            else:
                missing_symbols.append(symbol)

            items.append(
                {
                    'symbol': symbol,
                    'market_date': normalized_market_date.isoformat(),
                    'company': company,
                    'technical_snapshot': technical_snapshot,
                    'action_summary': action_summary,
                    'signal_features': signal_features,
                    'fundamentals': fundamentals,
                    'recent_disclosures': {
                        'count': len(recent_disclosures),
                        'items': recent_disclosures,
                    },
                    'availability': availability,
                }
            )

        return {
            'request': {
                'market_date': normalized_market_date.isoformat(),
                'symbols': normalized_symbols,
                'fs_div': normalized_fs_div,
                'fs_label': FS_DIV_TO_STATEMENT_TYPE.get(normalized_fs_div) if normalized_fs_div else None,
                'disclosures_limit': disclosures_limit,
            },
            'summary': {
                'requested_symbol_count': len(normalized_symbols),
                'matched_symbol_count': len(matched_symbols),
                'missing_symbols': missing_symbols,
                'technical_snapshot_count': sum(1 for item in items if item['technical_snapshot'] is not None),
                'action_summary_count': sum(1 for item in items if item['action_summary'] is not None),
                'signal_feature_count': sum(1 for item in items if item['signal_features'] is not None),
                'fundamentals_count': sum(
                    1 for item in items if item['fundamentals']['latest_snapshot'] is not None
                ),
                'recent_disclosure_symbol_count': sum(
                    1 for item in items if item['recent_disclosures']['count'] > 0
                ),
            },
            'items': items,
        }


agent_context_read_service = AgentContextReadService()
