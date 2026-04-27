from __future__ import annotations

import argparse
import asyncio
from dataclasses import dataclass
from datetime import date
from typing import Any, Iterable, Sequence

from app.brokers.kis.provider import kis_client
from app.market_data_store import (
    list_symbols_with_daily_bar_on_date,
    load_daily_market_risk_filter_results,
    load_latest_available_daily_market_risk_filter_result_date,
    load_symbols,
    upsert_daily_market_risk_filter_result,
)

MARKET_RISK_FILTER_SOURCE = 'kis_market_risk_v1'
DEFAULT_MARKET_CODE = 'J'
DEFAULT_FETCH_CONCURRENCY = 8
LATEST_FALLBACK_POLICY = 'latest_on_or_before_requested_market_date'
BLOCKING_MARKET_WARNING_CODES = frozenset({'02', '03'})
WATCHLIST_MARKET_WARNING_CODES = frozenset({'01'})
INACTIVE_VI_CODES = frozenset({'', '0', '00'})


def _normalize_market_date(value: str | date) -> date:
    if isinstance(value, date):
        return value
    normalized = str(value).strip()
    if not normalized:
        raise ValueError('market_date must not be blank')
    return date.fromisoformat(normalized)


def _normalize_optional_market_date(value: str | date | None) -> date | None:
    if value is None:
        return None
    return _normalize_market_date(value)


def _normalize_symbol(symbol: str) -> str:
    normalized = symbol.strip().upper()
    if not normalized:
        raise ValueError('symbol must not be blank')
    if normalized.isdigit():
        normalized = normalized.zfill(6)
    return normalized


def _normalize_symbols(symbols: Sequence[str] | None) -> list[str] | None:
    if symbols is None:
        return None
    return list(dict.fromkeys(_normalize_symbol(symbol) for symbol in symbols))


def _normalize_market_code(value: str) -> str:
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


def _risk_state(*, passed_filter: bool, watchlist_flagged: bool) -> str:
    if not passed_filter:
        return 'fail'
    if watchlist_flagged:
        return 'watchlist'
    return 'pass'


def _append_reason(target: list[str], reason: str, *, when: bool) -> None:
    if when and reason not in target:
        target.append(reason)


@dataclass(slots=True)
class MarketRiskSymbolInput:
    symbol: str
    name_ko: str | None
    market: str | None


def build_market_risk_filter_result(
    *,
    symbol: str,
    market_date: str | date,
    quote_response: dict[str, Any],
    source: str = MARKET_RISK_FILTER_SOURCE,
    market: str | None = None,
    name_ko: str | None = None,
    market_code: str = DEFAULT_MARKET_CODE,
    eod_run_id: int | None = None,
) -> dict[str, Any]:
    normalized_market_date = _normalize_market_date(market_date).isoformat()
    normalized_symbol = _normalize_symbol(symbol)
    normalized_market_code = _normalize_market_code(market_code)
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

    blocking_reasons: list[str] = []
    watchlist_reasons: list[str] = []
    _append_reason(blocking_reasons, 'management_item', when=is_management_item)
    _append_reason(
        blocking_reasons,
        f'market_warning_{market_warning_code}',
        when=market_warning_code in BLOCKING_MARKET_WARNING_CODES,
    )
    _append_reason(
        watchlist_reasons,
        f'market_warning_{market_warning_code}',
        when=market_warning_code in WATCHLIST_MARKET_WARNING_CODES,
    )
    _append_reason(watchlist_reasons, 'investment_caution', when=is_investment_caution)
    _append_reason(watchlist_reasons, 'short_overheated', when=is_short_overheated)
    _append_reason(watchlist_reasons, 'short_sale_overheated', when=is_short_sale_overheated)
    _append_reason(watchlist_reasons, 'volatility_interruption', when=is_vi_triggered)
    _append_reason(blocking_reasons, 'dishonest_disclosure', when=is_dishonest_disclosure)
    _append_reason(blocking_reasons, 'trade_halted', when=is_trade_halted)

    passed_filter = not blocking_reasons
    watchlist_flagged = bool(watchlist_reasons)

    return {
        'symbol': normalized_symbol,
        'name_ko': name_ko,
        'market': market or 'KRX',
        'eod_run_id': eod_run_id,
        'market_date': normalized_market_date,
        'source': source,
        'feature_version': source,
        'risk_state': _risk_state(
            passed_filter=passed_filter,
            watchlist_flagged=watchlist_flagged,
        ),
        'passed_filter': passed_filter,
        'watchlist_flagged': watchlist_flagged,
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
        'filter_payload': {
            'feature_version': source,
            'market_code': normalized_market_code,
            'source_endpoint': 'inquire-price-2',
            'blocking_reasons': blocking_reasons,
            'watchlist_reasons': watchlist_reasons,
            'raw': payload,
        },
    }


class DailyMarketRiskFilterService:
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

    async def rebuild_for_market_date(
        self,
        *,
        market_date: str | date,
        symbols: Sequence[str] | None = None,
        source: str = MARKET_RISK_FILTER_SOURCE,
        market_code: str = DEFAULT_MARKET_CODE,
        run_id: int | None = None,
        fetch_concurrency: int | None = None,
    ) -> dict[str, Any]:
        normalized_market_date = _normalize_market_date(market_date)
        normalized_symbols = _normalize_symbols(symbols)
        normalized_market_code = _normalize_market_code(market_code)
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
                    'evaluated_count': 0,
                    'passed_count': 0,
                    'watchlist_count': 0,
                    'failed_count': 0,
                },
                'results_persisted': 0,
                'results_failed': 0,
                'failed_symbols': [],
                'failed_symbol_errors': [],
                'items': [],
            }

        semaphore = asyncio.Semaphore(max(int(fetch_concurrency or self.fetch_concurrency), 1))

        async def _fetch_symbol(item: MarketRiskSymbolInput) -> dict[str, Any]:
            async with semaphore:
                try:
                    response = await self.client.fetch_domestic_quote_risk_status(
                        item.symbol,
                        market_code=normalized_market_code,
                    )
                    return {
                        'symbol': item.symbol,
                        'item': build_market_risk_filter_result(
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
                except Exception as exc:
                    return {
                        'symbol': item.symbol,
                        'error': str(exc),
                    }

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
                item['passed_filter'],
                not item['watchlist_flagged'],
                item['symbol'],
            )
        )

        persisted_count = 0
        for item in items:
            try:
                upsert_daily_market_risk_filter_result(item)
                persisted_count += 1
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
                'evaluated_count': len(items),
                'passed_count': sum(1 for item in items if item.get('passed_filter')),
                'watchlist_count': sum(1 for item in items if item.get('watchlist_flagged')),
                'failed_count': sum(1 for item in items if not item.get('passed_filter')),
            },
            'results_persisted': persisted_count,
            'results_failed': len(failed_symbols),
            'failed_symbols': failed_symbols,
            'failed_symbol_errors': failed_symbol_errors,
            'items': items,
        }

    def inspect(
        self,
        *,
        market_date: str | date | None = None,
        symbol: str | None = None,
        limit: int | None = None,
    ) -> dict[str, Any]:
        normalized_requested_market_date = _normalize_optional_market_date(market_date)
        normalized_symbol = _normalize_symbol(symbol) if symbol is not None else None
        requested_symbols = [normalized_symbol] if normalized_symbol else []
        resolved_market_date = load_latest_available_daily_market_risk_filter_result_date(
            market_date=normalized_requested_market_date,
            symbols=requested_symbols or None,
        )
        items = (
            load_daily_market_risk_filter_results(
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
            'missing_symbols': [symbol for symbol in requested_symbols if symbol not in matched_symbol_set],
            'passed_count': sum(1 for item in items if item.get('passed_filter')),
            'watchlist_count': sum(1 for item in items if item.get('watchlist_flagged')),
            'failed_count': sum(1 for item in items if not item.get('passed_filter')),
        }
        payload: dict[str, Any] = {
            'requested_market_date': requested_market_date,
            'resolved_market_date': resolved_market_date,
            'market_date': resolved_market_date,
            'latest_mode': normalized_requested_market_date is None,
            'is_latest_fallback': is_latest_fallback,
            'fallback_policy': LATEST_FALLBACK_POLICY,
            'feature_version': (items[0].get('feature_version') if items else MARKET_RISK_FILTER_SOURCE),
            'summary': summary,
        }
        if normalized_symbol:
            payload['item'] = items[0] if items else None
        else:
            payload['items'] = items
        return payload


daily_market_risk_filter_service = DailyMarketRiskFilterService()


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description='Compute daily KIS market risk filter rows')
    parser.add_argument('--market-date', required=True, help='Market date in YYYY-MM-DD format')
    parser.add_argument('--symbols')
    parser.add_argument('--source', default=MARKET_RISK_FILTER_SOURCE)
    parser.add_argument('--market-code', default=DEFAULT_MARKET_CODE)
    parser.add_argument('--fetch-concurrency', type=int, default=DEFAULT_FETCH_CONCURRENCY)
    return parser


async def _run_from_args(args: argparse.Namespace) -> dict[str, Any]:
    parsed_symbols = [item.strip() for item in str(args.symbols or '').split(',') if item.strip()] or None
    return await daily_market_risk_filter_service.rebuild_for_market_date(
        market_date=args.market_date,
        symbols=parsed_symbols,
        source=args.source,
        market_code=args.market_code,
        fetch_concurrency=args.fetch_concurrency,
    )


def main(argv: Iterable[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(list(argv) if argv is not None else None)
    result = asyncio.run(_run_from_args(args))
    print(result)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
