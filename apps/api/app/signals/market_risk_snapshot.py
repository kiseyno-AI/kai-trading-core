from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import date
from typing import Any, Callable, Sequence

from app.brokers.kis.provider import kis_client
from app.market_data_store import (
    list_symbols_with_daily_bar_on_date,
    load_daily_market_risk_snapshots,
    load_latest_available_daily_market_risk_snapshot_date,
    load_symbols,
    upsert_daily_market_risk_snapshot,
)

MARKET_RISK_SNAPSHOT_SOURCE = 'kis_quote2_snapshot_v1'
DEFAULT_MARKET_CODE = 'J'
DEFAULT_FETCH_CONCURRENCY = 1
DEFAULT_FETCH_THROTTLE_SECONDS = 0.25
RATE_LIMIT_RETRY_DELAYS_SECONDS = (1.0, 2.0, 4.0)
LATEST_FALLBACK_POLICY = 'latest_on_or_before_requested_market_date'
INACTIVE_VI_CODES = frozenset({'', '0', '00'})


def normalize_market_date(value: str | date) -> date:
    if isinstance(value, date):
        return value
    normalized = str(value).strip()
    if not normalized:
        raise ValueError('market_date must not be blank')
    return date.fromisoformat(normalized)


def normalize_optional_market_date(value: str | date | None) -> date | None:
    if value is None:
        return None
    return normalize_market_date(value)


def normalize_symbol(symbol: str) -> str:
    normalized = symbol.strip().upper()
    if not normalized:
        raise ValueError('symbol must not be blank')
    if normalized.isdigit():
        normalized = normalized.zfill(6)
    return normalized


def normalize_symbols(symbols: Sequence[str] | None) -> list[str] | None:
    if symbols is None:
        return None
    return list(dict.fromkeys(normalize_symbol(symbol) for symbol in symbols))


def normalize_market_code(value: str) -> str:
    normalized = str(value or '').strip().upper()
    if not normalized:
        raise ValueError('market_code must not be blank')
    return normalized


def _normalize_code(value: Any) -> str | None:
    if value is None:
        return None
    normalized = str(value).strip()
    return normalized or None


def _normalize_warning_code(value: Any) -> str | None:
    normalized = _normalize_code(value)
    if normalized is None:
        return None
    if normalized.isdigit() and len(normalized) < 2:
        return normalized.zfill(2)
    return normalized


def _normalize_flag(value: Any) -> bool:
    normalized = str(value or '').strip().upper()
    return normalized in {'Y', '1', 'TRUE', 'T'}


def _extract_quote_payload(response: dict[str, Any]) -> dict[str, Any]:
    for key in ('output', 'output1'):
        payload = response.get(key)
        if isinstance(payload, dict):
            return payload
    return response


@dataclass(slots=True)
class MarketRiskSymbolInput:
    symbol: str
    name_ko: str | None
    market: str | None


def build_market_risk_snapshot(
    *,
    symbol: str,
    market_date: str | date,
    quote_response: dict[str, Any],
    source: str = MARKET_RISK_SNAPSHOT_SOURCE,
    market: str | None = None,
    name_ko: str | None = None,
    market_code: str = DEFAULT_MARKET_CODE,
    eod_run_id: int | None = None,
) -> dict[str, Any]:
    normalized_market_date = normalize_market_date(market_date).isoformat()
    normalized_symbol = normalize_symbol(symbol)
    normalized_market_code = normalize_market_code(market_code)
    payload = _extract_quote_payload(quote_response)

    market_warning_code = _normalize_warning_code(payload.get('mrkt_warn_cls_code'))
    market_warning_name = _normalize_code(payload.get('mrkt_warn_cls_name'))
    is_management_item = _normalize_flag(payload.get('mang_issu_yn'))
    is_investment_caution = _normalize_flag(payload.get('invt_caful_yn'))
    is_short_overheated = _normalize_flag(payload.get('short_over_yn'))
    short_overheat_class_code = _normalize_code(payload.get('short_over_cls_code'))
    is_short_sale_overheated = _normalize_flag(payload.get('ssts_hot_yn'))
    vi_class_code = _normalize_code(payload.get('vi_cls_code'))
    is_vi_triggered = (vi_class_code or '') not in INACTIVE_VI_CODES
    is_dishonest_disclosure = _normalize_flag(payload.get('insn_pbnt_yn'))
    is_trade_halted = _normalize_flag(payload.get('trht_yn'))

    return {
        'symbol': normalized_symbol,
        'name_ko': name_ko,
        'market': market or 'KRX',
        'eod_run_id': eod_run_id,
        'market_date': normalized_market_date,
        'source': source,
        'feature_version': source,
        'market_code': normalized_market_code,
        'source_endpoint': 'inquire-price-2',
        'market_warning_code': market_warning_code,
        'market_warning_name': market_warning_name,
        'is_management_item': is_management_item,
        'is_investment_caution': is_investment_caution,
        'is_short_overheated': is_short_overheated,
        'short_overheat_class_code': short_overheat_class_code,
        'is_short_sale_overheated': is_short_sale_overheated,
        'vi_class_code': vi_class_code,
        'is_vi_triggered': is_vi_triggered,
        'is_dishonest_disclosure': is_dishonest_disclosure,
        'is_trade_halted': is_trade_halted,
        'snapshot_payload': {
            'feature_version': source,
            'market_code': normalized_market_code,
            'source_endpoint': 'inquire-price-2',
            'raw': payload,
        },
    }


class DailyMarketRiskSnapshotService:
    def __init__(self, *, client=None, fetch_concurrency: int = DEFAULT_FETCH_CONCURRENCY) -> None:
        self.client = client or kis_client
        self.fetch_concurrency = max(int(fetch_concurrency or DEFAULT_FETCH_CONCURRENCY), 1)

    def _load_symbol_rows(
        self,
        *,
        market_date: date,
        symbols: Sequence[str] | None,
    ) -> tuple[list[MarketRiskSymbolInput], list[str], list[str]]:
        if symbols is None:
            matched_symbols = list_symbols_with_daily_bar_on_date(market_date.isoformat())
            if not matched_symbols:
                return [], [], []
            missing_symbols: list[str] = []
        else:
            requested_symbols = list(symbols)
            symbol_rows = load_symbols(symbols=requested_symbols)
            rows_by_symbol = {row['symbol']: row for row in symbol_rows}
            matched_symbols = [symbol for symbol in requested_symbols if symbol in rows_by_symbol]
            missing_symbols = [symbol for symbol in requested_symbols if symbol not in rows_by_symbol]
            if not matched_symbols:
                return [], [], missing_symbols

        row_map = {row['symbol']: row for row in load_symbols(symbols=matched_symbols)}
        inputs = [
            MarketRiskSymbolInput(
                symbol=symbol,
                name_ko=(row_map.get(symbol) or {}).get('name_ko'),
                market=(row_map.get(symbol) or {}).get('market'),
            )
            for symbol in matched_symbols
            if symbol in row_map
        ]
        return inputs, matched_symbols, missing_symbols

    async def collect_for_market_date(
        self,
        *,
        market_date: str | date,
        symbols: Sequence[str] | None = None,
        source: str = MARKET_RISK_SNAPSHOT_SOURCE,
        market_code: str = DEFAULT_MARKET_CODE,
        run_id: int | None = None,
        fetch_concurrency: int | None = None,
        progress_callback: Callable[..., Any] | None = None,
    ) -> dict[str, Any]:
        normalized_market_date = normalize_market_date(market_date)
        normalized_symbols = normalize_symbols(symbols)
        normalized_market_code = normalize_market_code(market_code)
        requested_symbols = list(normalized_symbols or [])
        symbol_rows, matched_symbols, missing_symbols = self._load_symbol_rows(
            market_date=normalized_market_date,
            symbols=normalized_symbols,
        )

        if not symbol_rows:
            return {
                'market_date': normalized_market_date.isoformat(),
                'source': source,
                'feature_version': source,
                'requested_symbols': requested_symbols,
                'matched_symbols': matched_symbols,
                'missing_symbols': missing_symbols,
                'summary': {
                    'matched_symbol_count': 0,
                    'collected_count': 0,
                    'management_item_count': 0,
                    'warning_count': 0,
                    'trade_halted_count': 0,
                },
                'snapshots_persisted': 0,
                'snapshots_failed': 0,
                'failed_symbols': [],
                'failed_symbol_errors': [],
                'items': [],
            }

        semaphore = asyncio.Semaphore(max(int(fetch_concurrency or self.fetch_concurrency), 1))
        throttle_lock = asyncio.Lock()
        progress_lock = asyncio.Lock()
        last_request_at = 0.0
        processed_count = 0
        succeeded_count = 0
        failed_count = 0
        total_count = len(symbol_rows)

        async def _emit_progress(**payload: Any) -> None:
            if progress_callback is None:
                return
            try:
                progress_callback(**payload)
            except Exception:
                return

        await _emit_progress(
            status='running',
            current_step='fetching_kis_market_risk_snapshots',
            current_activity=(
                f'Fetching KIS market-risk snapshot for {symbol_rows[0].symbol}'
                if symbol_rows
                else 'Fetching KIS market-risk snapshots'
            ),
            processed=0,
            total=total_count,
            succeeded=0,
            failed=0,
            current_symbol=symbol_rows[0].symbol if symbol_rows else None,
        )

        async def _wait_for_request_slot() -> None:
            nonlocal last_request_at
            async with throttle_lock:
                now = asyncio.get_running_loop().time()
                wait_seconds = DEFAULT_FETCH_THROTTLE_SECONDS - (now - last_request_at)
                if wait_seconds > 0:
                    await asyncio.sleep(wait_seconds)
                    now = asyncio.get_running_loop().time()
                last_request_at = now

        def _is_rate_limit_error(exc: Exception) -> bool:
            message = str(exc)
            return '초당 거래건수' in message or 'rate limit' in message.lower()

        async def _fetch_symbol(item: MarketRiskSymbolInput) -> dict[str, Any]:
            nonlocal processed_count, succeeded_count, failed_count
            async with semaphore:
                attempts = len(RATE_LIMIT_RETRY_DELAYS_SECONDS) + 1
                last_error: Exception | None = None
                for attempt_index in range(attempts):
                    try:
                        await _wait_for_request_slot()
                        response = await self.client.fetch_domestic_quote_risk_status(
                            item.symbol,
                            market_code=normalized_market_code,
                        )
                        payload = {
                            'symbol': item.symbol,
                            'item': build_market_risk_snapshot(
                                symbol=item.symbol,
                                name_ko=item.name_ko,
                                market=item.market,
                                market_date=normalized_market_date,
                                quote_response=response,
                                source=source,
                                market_code=normalized_market_code,
                                eod_run_id=run_id,
                            ),
                        }
                        async with progress_lock:
                            processed_count += 1
                            succeeded_count += 1
                            await _emit_progress(
                                status='running',
                                current_step='fetching_kis_market_risk_snapshots',
                                current_activity=f'Fetched KIS market-risk snapshot for {item.symbol}',
                                processed=processed_count,
                                total=total_count,
                                succeeded=succeeded_count,
                                failed=failed_count,
                                current_symbol=item.symbol,
                            )
                        return payload
                    except Exception as exc:
                        last_error = exc
                        if not _is_rate_limit_error(exc) or attempt_index >= len(RATE_LIMIT_RETRY_DELAYS_SECONDS):
                            break
                        await asyncio.sleep(RATE_LIMIT_RETRY_DELAYS_SECONDS[attempt_index])
                payload = {
                    'symbol': item.symbol,
                    'error': str(last_error) if last_error is not None else 'unknown KIS market-risk fetch failure',
                }
                async with progress_lock:
                    processed_count += 1
                    failed_count += 1
                    await _emit_progress(
                        status='running',
                        current_step='fetching_kis_market_risk_snapshots',
                        current_activity=f'KIS market-risk snapshot failed for {item.symbol}',
                        processed=processed_count,
                        total=total_count,
                        succeeded=succeeded_count,
                        failed=failed_count,
                        current_symbol=item.symbol,
                    )
                return payload

        fetch_results = await asyncio.gather(*[_fetch_symbol(item) for item in symbol_rows])

        items: list[dict[str, Any]] = []
        failed_symbols: list[str] = []
        failed_symbol_errors: list[dict[str, str]] = []
        for result in fetch_results:
            item = result.get('item')
            if item is not None:
                items.append(item)
                continue
            failed_symbols.append(result['symbol'])
            failed_symbol_errors.append({'symbol': result['symbol'], 'error': result['error']})

        items.sort(
            key=lambda item: (
                not item['is_trade_halted'],
                not item['is_management_item'],
                item['market_warning_code'] or '99',
                item['symbol'],
            )
        )

        persisted_items: list[dict[str, Any]] = []
        for item in items:
            try:
                upsert_daily_market_risk_snapshot(item)
                persisted_items.append(item)
            except Exception as exc:
                failed_symbols.append(item['symbol'])
                failed_symbol_errors.append({'symbol': item['symbol'], 'error': str(exc)})

        return {
            'market_date': normalized_market_date.isoformat(),
            'source': source,
            'feature_version': source,
            'requested_symbols': requested_symbols,
            'matched_symbols': matched_symbols,
            'missing_symbols': missing_symbols,
            'summary': {
                'matched_symbol_count': len(matched_symbols),
                'collected_count': len(items),
                'management_item_count': sum(1 for item in items if item.get('is_management_item')),
                'warning_count': sum(1 for item in items if item.get('market_warning_code') not in (None, '00')),
                'trade_halted_count': sum(1 for item in items if item.get('is_trade_halted')),
            },
            'snapshots_persisted': len(persisted_items),
            'snapshots_failed': len(failed_symbols),
            'failed_symbols': failed_symbols,
            'failed_symbol_errors': failed_symbol_errors,
            'items': persisted_items,
        }

    def inspect(
        self,
        *,
        market_date: str | date | None = None,
        symbol: str | None = None,
        limit: int | None = None,
    ) -> dict[str, Any]:
        normalized_requested_market_date = normalize_optional_market_date(market_date)
        normalized_symbol = normalize_symbol(symbol) if symbol is not None else None
        requested_symbols = [normalized_symbol] if normalized_symbol else []
        resolved_market_date = load_latest_available_daily_market_risk_snapshot_date(
            market_date=normalized_requested_market_date,
            symbols=requested_symbols or None,
        )
        items = (
            load_daily_market_risk_snapshots(
                market_date=resolved_market_date,
                symbols=requested_symbols or None,
                limit=(1 if normalized_symbol else limit),
            )
            if resolved_market_date is not None
            else []
        )
        matched_symbols = [item['symbol'] for item in items]
        matched_symbol_set = set(matched_symbols)
        requested_market_date = (
            normalized_requested_market_date.isoformat() if normalized_requested_market_date is not None else None
        )
        is_latest_fallback = (
            requested_market_date is not None
            and resolved_market_date is not None
            and resolved_market_date != requested_market_date
        )
        summary = {
            'entries': len(items),
            'requested_symbol': normalized_symbol,
            'matched_symbols': matched_symbols,
            'missing_symbols': [value for value in requested_symbols if value not in matched_symbol_set],
            'management_item_count': sum(1 for item in items if item.get('is_management_item')),
            'warning_count': sum(1 for item in items if item.get('market_warning_code') not in (None, '00')),
            'trade_halted_count': sum(1 for item in items if item.get('is_trade_halted')),
        }
        payload: dict[str, Any] = {
            'requested_market_date': requested_market_date,
            'resolved_market_date': resolved_market_date,
            'market_date': resolved_market_date,
            'latest_mode': normalized_requested_market_date is None,
            'is_latest_fallback': is_latest_fallback,
            'fallback_policy': LATEST_FALLBACK_POLICY,
            'feature_version': (items[0].get('feature_version') if items else MARKET_RISK_SNAPSHOT_SOURCE),
            'summary': summary,
        }
        if normalized_symbol:
            payload['item'] = items[0] if items else None
        else:
            payload['items'] = items
        return payload


daily_market_risk_snapshot_service = DailyMarketRiskSnapshotService()
