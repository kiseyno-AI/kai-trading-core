from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Any, Sequence

from app.market_data_store import (
    list_symbols_with_daily_bar_on_date,
    load_daily_value_factor_results,
    load_recent_price_bars_by_symbol,
    load_symbols,
    upsert_daily_value_factor_result,
)

VALUE_FACTOR_VERSION = 'value_factor_v1'
VALUE_FACTOR_SOURCE = VALUE_FACTOR_VERSION
VALUE_STATUS_SCORED = 'scored'
VALUE_STATUS_INVALID_PER = 'invalid_per'
VALUE_STATUS_MISSING_METRIC = 'missing_metric'
VALUE_STATUS_MISSING_MARKET_DATE_BAR = 'missing_market_date_bar'
METRIC_STATUS_AVAILABLE = 'available'
METRIC_STATUS_MISSING = 'missing'
METRIC_STATUS_INVALID_NON_POSITIVE = 'invalid_non_positive'
METRIC_STATUS_INVALID_NEGATIVE = 'invalid_negative'
VALUE_SCORE_COMPONENT_KEYS = (
    'per_percentile',
    'pbr_percentile',
    'dividend_yield_percentile',
)
EMPTY_RANKING_UNIVERSE_COUNTS = {
    'per': 0,
    'pbr': 0,
    'dividend_yield': 0,
    'composite_score': 0,
}


def _round(value: float | None, digits: int = 6) -> float | None:
    if value is None:
        return None
    return round(float(value), digits)


def _normalize_market_date(value: str | date) -> date:
    if isinstance(value, date):
        return value
    return date.fromisoformat(value)


def _normalize_symbol(symbol: str) -> str:
    normalized = str(symbol or '').strip().upper()
    if not normalized:
        raise ValueError('symbol must not be blank')
    if normalized.isdigit():
        normalized = normalized.zfill(6)
    return normalized


def _normalize_symbols(symbols: Sequence[str] | None) -> list[str] | None:
    if symbols is None:
        return None
    return list(dict.fromkeys(_normalize_symbol(symbol) for symbol in symbols))


def _percentile_from_rank(rank: int, total: int) -> float:
    if total <= 1:
        return 1.0
    return _round((total - rank) / (total - 1), 6) or 0.0


def _positive_only_metric_status(value: float | None) -> str:
    if value is None:
        return METRIC_STATUS_MISSING
    if float(value) <= 0:
        return METRIC_STATUS_INVALID_NON_POSITIVE
    return METRIC_STATUS_AVAILABLE


def _non_negative_metric_status(value: float | None) -> str:
    if value is None:
        return METRIC_STATUS_MISSING
    if float(value) < 0:
        return METRIC_STATUS_INVALID_NEGATIVE
    return METRIC_STATUS_AVAILABLE


def _sort_items(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return sorted(
        items,
        key=lambda item: (
            item.get('value_status') != VALUE_STATUS_SCORED,
            item.get('value_rank') is None,
            item.get('value_rank') if item.get('value_rank') is not None else 999999,
            item['symbol'],
        ),
    )


@dataclass(slots=True)
class ValueFactorInput:
    symbol: str
    name_ko: str | None
    market: str | None
    bars: list[dict[str, Any]]


class DailyValueFactorService:
    def rebuild_for_market_date(
        self,
        *,
        market_date: str | date,
        symbols: Sequence[str] | None = None,
        source: str = VALUE_FACTOR_SOURCE,
    ) -> dict[str, Any]:
        normalized_market_date = _normalize_market_date(market_date)
        normalized_symbols = _normalize_symbols(symbols)
        requested_symbols = list(normalized_symbols or [])
        if normalized_symbols == []:
            return {
                'market_date': normalized_market_date.isoformat(),
                'source': source,
                'feature_version': source,
                'requested_symbols': requested_symbols,
                'matched_symbols': [],
                'missing_symbols': [],
                'summary': self._build_summary(
                    [],
                    requested_symbols=requested_symbols,
                    matched_symbols=[],
                    missing_symbols=[],
                    universe_counts=EMPTY_RANKING_UNIVERSE_COUNTS,
                ),
                'results_persisted': 0,
                'results_failed': 0,
                'failed_symbols': [],
                'failed_symbol_errors': [],
                'items': [],
            }
        inputs, matched_symbols, missing_symbols = self._load_inputs(
            market_date=normalized_market_date,
            symbols=normalized_symbols,
        )
        ranking_inputs = inputs
        if normalized_symbols is not None:
            ranking_inputs, _, _ = self._load_inputs(
                market_date=normalized_market_date,
                symbols=None,
            )

        ranking_items = [
            self._build_symbol_result(
                input_row=item,
                market_date=normalized_market_date,
                source=source,
            )
            for item in ranking_inputs
        ]
        universe_counts = self._assign_metric_rankings(ranking_items)
        self._assign_scores_and_rankings(ranking_items)

        if normalized_symbols is None:
            items = _sort_items(ranking_items)
        else:
            ranked_items_by_symbol = {item['symbol']: item for item in ranking_items}
            items = []
            for input_row in inputs:
                item = ranked_items_by_symbol.get(input_row.symbol)
                if item is None:
                    item = self._build_symbol_result(
                        input_row=input_row,
                        market_date=normalized_market_date,
                        source=source,
                    )
                    self._assign_scores_and_rankings([item])
                items.append(item)

        for item in items:
            item['factor_payload'] = self._build_factor_payload(
                item=item,
                source=source,
                universe_counts=universe_counts,
            )
        summary = self._build_summary(
            items,
            requested_symbols=requested_symbols,
            matched_symbols=matched_symbols,
            missing_symbols=missing_symbols,
            universe_counts=universe_counts,
        )
        for item in items:
            self._strip_internal_fields(item)

        persisted_count = 0
        failed_symbols: list[str] = []
        failed_symbol_errors: list[dict[str, str]] = []
        for item in items:
            try:
                upsert_daily_value_factor_result(item)
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
            'summary': summary,
            'results_persisted': persisted_count,
            'results_failed': len(failed_symbols),
            'failed_symbols': failed_symbols,
            'failed_symbol_errors': failed_symbol_errors,
            'items': _sort_items(items),
        }

    def inspect(
        self,
        *,
        market_date: str | date,
        symbol: str | None = None,
        limit: int | None = None,
    ) -> dict[str, Any]:
        normalized_market_date = _normalize_market_date(market_date)
        normalized_symbol = _normalize_symbol(symbol) if symbol is not None else None
        requested_symbols = [normalized_symbol] if normalized_symbol else []
        items = load_daily_value_factor_results(
            market_date=normalized_market_date,
            symbols=requested_symbols or None,
            limit=(1 if normalized_symbol else limit),
        )
        matched_symbols = [item['symbol'] for item in items]
        matched_symbol_set = set(matched_symbols)
        summary = {
            'entries': len(items),
            'requested_symbol': normalized_symbol,
            'matched_symbols': matched_symbols,
            'missing_symbols': [candidate for candidate in requested_symbols if candidate not in matched_symbol_set],
            'scored_count': sum(1 for item in items if item.get('value_status') == VALUE_STATUS_SCORED),
            'invalid_per_count': sum(1 for item in items if item.get('value_status') == VALUE_STATUS_INVALID_PER),
            'missing_metric_count': sum(1 for item in items if item.get('value_status') == VALUE_STATUS_MISSING_METRIC),
        }
        payload: dict[str, Any] = {
            'market_date': normalized_market_date.isoformat(),
            'feature_version': VALUE_FACTOR_VERSION,
            'summary': summary,
        }
        if normalized_symbol:
            payload['item'] = items[0] if items else None
        else:
            payload['items'] = items
        return payload

    def evaluate_symbol(
        self,
        *,
        symbol: str,
        market_date: str | date,
    ) -> dict[str, Any]:
        inspection = self.inspect(market_date=market_date, symbol=symbol)
        return {
            'market_date': inspection['market_date'],
            'feature_version': inspection['feature_version'],
            'symbol': _normalize_symbol(symbol),
            'summary': inspection['summary'],
            'item': inspection.get('item'),
        }

    def _load_inputs(
        self,
        *,
        market_date: date,
        symbols: list[str] | None,
    ) -> tuple[list[ValueFactorInput], list[str], list[str]]:
        if symbols == []:
            return [], [], []
        if symbols is None:
            matched_symbols = list_symbols_with_daily_bar_on_date(market_date.isoformat())
            if not matched_symbols:
                return [], [], []
            missing_symbols: list[str] = []
        else:
            symbol_rows = load_symbols(symbols=symbols)
            matched_symbols = [item['symbol'] for item in symbol_rows]
            matched_symbol_set = set(matched_symbols)
            missing_symbols = [symbol for symbol in symbols if symbol not in matched_symbol_set]
            if not matched_symbols:
                return [], [], missing_symbols

        symbol_rows = load_symbols(symbols=matched_symbols)
        bars_by_symbol = load_recent_price_bars_by_symbol(
            market_date=market_date,
            symbols=matched_symbols,
            history_days=1,
        )
        inputs = [
            ValueFactorInput(
                symbol=row['symbol'],
                name_ko=row.get('name_ko'),
                market=row.get('market'),
                bars=bars_by_symbol.get(row['symbol'], []),
            )
            for row in symbol_rows
        ]
        return inputs, matched_symbols, missing_symbols

    def _build_symbol_result(
        self,
        *,
        input_row: ValueFactorInput,
        market_date: date,
        source: str,
    ) -> dict[str, Any]:
        market_date_iso = market_date.isoformat()
        current_bar = input_row.bars[0] if input_row.bars and input_row.bars[0].get('trade_date') == market_date_iso else None
        per = _round(float(current_bar['per']), 6) if current_bar and current_bar.get('per') is not None else None
        pbr = _round(float(current_bar['pbr']), 6) if current_bar and current_bar.get('pbr') is not None else None
        dividend_yield = (
            _round(float(current_bar['dividend_yield']), 6)
            if current_bar and current_bar.get('dividend_yield') is not None
            else None
        )
        per_status = _positive_only_metric_status(per) if current_bar is not None else METRIC_STATUS_MISSING
        pbr_status = _positive_only_metric_status(pbr) if current_bar is not None else METRIC_STATUS_MISSING
        dividend_yield_status = _non_negative_metric_status(dividend_yield) if current_bar is not None else METRIC_STATUS_MISSING

        return {
            'symbol': input_row.symbol,
            'name_ko': input_row.name_ko,
            'market': input_row.market,
            'market_date': market_date_iso,
            'source': source,
            'feature_version': source,
            'per': per,
            'pbr': pbr,
            'dividend_yield': dividend_yield,
            'per_status': per_status,
            'per_rank': None,
            'pbr_rank': None,
            'dividend_yield_rank': None,
            'per_percentile': None,
            'pbr_percentile': None,
            'dividend_yield_percentile': None,
            'value_score': None,
            'value_rank': None,
            'value_percentile': None,
            'value_status': VALUE_STATUS_MISSING_METRIC,
            'factor_payload': {},
            '_current_bar_available': current_bar is not None,
            '_bar_source': current_bar.get('source') if current_bar is not None else None,
            '_bar_captured_at': current_bar.get('captured_at') if current_bar is not None else None,
            '_pbr_status': pbr_status,
            '_dividend_yield_status': dividend_yield_status,
        }

    def _assign_metric_rankings(self, items: list[dict[str, Any]]) -> dict[str, int]:
        return {
            'per': self._assign_single_metric_rankings(
                items,
                value_key='per',
                status_key='per_status',
                rank_key='per_rank',
                percentile_key='per_percentile',
                best='lower',
            ),
            'pbr': self._assign_single_metric_rankings(
                items,
                value_key='pbr',
                status_key='_pbr_status',
                rank_key='pbr_rank',
                percentile_key='pbr_percentile',
                best='lower',
            ),
            'dividend_yield': self._assign_single_metric_rankings(
                items,
                value_key='dividend_yield',
                status_key='_dividend_yield_status',
                rank_key='dividend_yield_rank',
                percentile_key='dividend_yield_percentile',
                best='higher',
            ),
            'composite_score': sum(
                1
                for item in items
                if all(item.get(key) is not None for key in VALUE_SCORE_COMPONENT_KEYS)
            ),
        }

    def _assign_single_metric_rankings(
        self,
        items: list[dict[str, Any]],
        *,
        value_key: str,
        status_key: str,
        rank_key: str,
        percentile_key: str,
        best: str,
    ) -> int:
        candidates = [
            item
            for item in items
            if item.get('_current_bar_available')
            and item.get(status_key) == METRIC_STATUS_AVAILABLE
            and item.get(value_key) is not None
        ]
        if best == 'lower':
            ordered = sorted(candidates, key=lambda item: (float(item[value_key]), item['symbol']))
        elif best == 'higher':
            ordered = sorted(candidates, key=lambda item: (-float(item[value_key]), item['symbol']))
        else:
            raise ValueError(f'Unsupported ranking direction: {best}')

        total = len(ordered)
        for rank, item in enumerate(ordered, start=1):
            item[rank_key] = rank
            item[percentile_key] = _percentile_from_rank(rank, total)
        return total

    def _assign_scores_and_rankings(self, items: list[dict[str, Any]]) -> None:
        for item in items:
            if not item.get('_current_bar_available'):
                item['value_status'] = VALUE_STATUS_MISSING_MARKET_DATE_BAR
                continue
            if item.get('per_status') == METRIC_STATUS_INVALID_NON_POSITIVE:
                item['value_status'] = VALUE_STATUS_INVALID_PER
                continue
            if item.get('per_status') != METRIC_STATUS_AVAILABLE:
                item['value_status'] = VALUE_STATUS_MISSING_METRIC
                continue
            if item.get('_pbr_status') != METRIC_STATUS_AVAILABLE:
                item['value_status'] = VALUE_STATUS_MISSING_METRIC
                continue
            if item.get('_dividend_yield_status') != METRIC_STATUS_AVAILABLE:
                item['value_status'] = VALUE_STATUS_MISSING_METRIC
                continue
            if any(item.get(key) is None for key in VALUE_SCORE_COMPONENT_KEYS):
                item['value_status'] = VALUE_STATUS_MISSING_METRIC
                continue

            item['value_score'] = _round(
                sum(float(item[key]) for key in VALUE_SCORE_COMPONENT_KEYS) / float(len(VALUE_SCORE_COMPONENT_KEYS)),
                6,
            )
            item['value_status'] = VALUE_STATUS_SCORED

        scored_items = sorted(
            [item for item in items if item.get('value_status') == VALUE_STATUS_SCORED],
            key=lambda item: (-float(item['value_score']), item['symbol']),
        )
        total = len(scored_items)
        for rank, item in enumerate(scored_items, start=1):
            item['value_rank'] = rank
            item['value_percentile'] = _percentile_from_rank(rank, total)

    def _build_factor_payload(
        self,
        *,
        item: dict[str, Any],
        source: str,
        universe_counts: dict[str, int],
    ) -> dict[str, Any]:
        return {
            'feature_version': source,
            'methodology': {
                'input_table': 'daily_price_bars',
                'score_method': 'equal_weight_mean_of_component_percentiles',
                'ranking_directions': {
                    'per': 'lower_better_positive_only',
                    'pbr': 'lower_better_positive_only',
                    'dividend_yield': 'higher_better_non_negative',
                    'value_score': 'higher_better',
                },
                'tie_breaker': 'symbol_asc',
            },
            'metric_statuses': {
                'per': item['per_status'],
                'pbr': item['_pbr_status'],
                'dividend_yield': item['_dividend_yield_status'],
            },
            'ranking': {
                'per_rank': item['per_rank'],
                'pbr_rank': item['pbr_rank'],
                'dividend_yield_rank': item['dividend_yield_rank'],
                'value_rank': item['value_rank'],
                'per_percentile': item['per_percentile'],
                'pbr_percentile': item['pbr_percentile'],
                'dividend_yield_percentile': item['dividend_yield_percentile'],
                'value_percentile': item['value_percentile'],
                'ranking_universe_counts': universe_counts,
            },
            'audit': {
                'trade_date': item['market_date'],
                'name_ko': item.get('name_ko'),
                'market': item.get('market'),
                'bar_source': item.get('_bar_source'),
                'bar_captured_at': item.get('_bar_captured_at'),
            },
        }

    def _build_summary(
        self,
        items: list[dict[str, Any]],
        *,
        requested_symbols: list[str],
        matched_symbols: list[str],
        missing_symbols: list[str],
        universe_counts: dict[str, int],
    ) -> dict[str, Any]:
        return {
            'requested_symbol_count': len(requested_symbols) if requested_symbols else len(matched_symbols),
            'matched_symbol_count': len(matched_symbols),
            'missing_symbol_count': len(missing_symbols),
            'symbols_with_market_date_bar': sum(1 for item in items if item.get('_current_bar_available')),
            'missing_market_date_bar_count': sum(
                1 for item in items if item.get('value_status') == VALUE_STATUS_MISSING_MARKET_DATE_BAR
            ),
            'invalid_per_count': sum(1 for item in items if item.get('value_status') == VALUE_STATUS_INVALID_PER),
            'missing_metric_count': sum(1 for item in items if item.get('value_status') == VALUE_STATUS_MISSING_METRIC),
            'scored_count': sum(1 for item in items if item.get('value_status') == VALUE_STATUS_SCORED),
            'ranking_universe_counts': universe_counts,
        }

    @staticmethod
    def _strip_internal_fields(item: dict[str, Any]) -> None:
        for key in (
            '_current_bar_available',
            '_bar_source',
            '_bar_captured_at',
            '_pbr_status',
            '_dividend_yield_status',
        ):
            item.pop(key, None)


daily_value_factor_service = DailyValueFactorService()
