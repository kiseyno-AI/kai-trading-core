from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import date
from typing import Any, Iterable, Sequence

from app.market_data_store import (
    list_symbols_with_daily_bar_on_date,
    load_daily_momentum_factor_results,
    load_recent_price_bars_by_symbol,
    upsert_daily_momentum_factor_result,
)

MOMENTUM_FACTOR_VERSION = 'price_momentum_v1'
MOMENTUM_FACTOR_SOURCE = 'daily_price_bars'
MOMENTUM_SKIP_WINDOW_DAYS = 21
MOMENTUM_LOOKBACK_DAYS = 252
MOMENTUM_HISTORY_WINDOW = MOMENTUM_LOOKBACK_DAYS + 1
MA_60_WINDOW = 60
MA_120_WINDOW = 120
OVERHEAT_LOOKBACK_DAYS = 5


def _normalize_market_date(value: str | date) -> str:
    if isinstance(value, date):
        return value.isoformat()
    return date.fromisoformat(value).isoformat()


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

    normalized: list[str] = []
    seen: set[str] = set()
    for item in symbols:
        normalized_symbol = _normalize_symbol(item)
        if normalized_symbol in seen:
            continue
        seen.add(normalized_symbol)
        normalized.append(normalized_symbol)
    return normalized


def _round(value: float | None, digits: int = 6) -> float | None:
    if value is None:
        return None
    return round(float(value), digits)


def _moving_average(values: Sequence[float], window: int) -> float | None:
    if len(values) < window:
        return None
    return sum(values[-window:]) / window


def _return_ratio(values: Sequence[float], window: int) -> float | None:
    if len(values) <= window:
        return None
    baseline = values[-(window + 1)]
    if baseline == 0:
        return None
    return (values[-1] / baseline) - 1.0


def _momentum_12_1(values: Sequence[float]) -> float | None:
    if len(values) < MOMENTUM_HISTORY_WINDOW:
        return None
    end_value = values[-(MOMENTUM_SKIP_WINDOW_DAYS + 1)]
    start_value = values[-(MOMENTUM_LOOKBACK_DAYS + 1)]
    if start_value == 0:
        return None
    return (end_value / start_value) - 1.0


def _ordered_bars(bars: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
    return sorted(bars, key=lambda item: str(item['trade_date']))


def _sort_rows(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return sorted(
        items,
        key=lambda item: (
            not bool(item.get('is_momentum_eligible')),
            item.get('momentum_rank') is None,
            item.get('momentum_rank') if item.get('momentum_rank') is not None else 999999,
            item['symbol'],
        ),
    )


@dataclass(frozen=True, slots=True)
class MomentumOverheatRule:
    lookback_days: int = OVERHEAT_LOOKBACK_DAYS
    return_threshold: float = 0.15
    rule_type: str = 'absolute_return_threshold'
    comparison: str = '>='

    def classify(self, return_value: float | None) -> bool:
        if return_value is None:
            return False
        return return_value >= self.return_threshold

    def to_payload(self) -> dict[str, Any]:
        return {
            'rule_type': self.rule_type,
            'comparison': self.comparison,
            'lookback_days': self.lookback_days,
            'threshold': self.return_threshold,
            'threshold_units': 'decimal_return',
        }


def build_daily_momentum_factor_rows(
    *,
    market_date: str | date,
    price_histories: dict[str, Sequence[dict[str, Any]]],
    overheat_rule: MomentumOverheatRule | None = None,
) -> list[dict[str, Any]]:
    normalized_market_date = _normalize_market_date(market_date)
    selected_overheat_rule = overheat_rule or MomentumOverheatRule()
    rows: list[dict[str, Any]] = []

    for symbol in sorted(price_histories):
        bars = _ordered_bars(price_histories[symbol])
        if not bars:
            raise ValueError(f'No daily bars available for {symbol}.')

        latest = bars[-1]
        latest_trade_date = str(latest['trade_date'])
        if latest_trade_date != normalized_market_date:
            raise ValueError(f'Latest stored bar for {symbol} is {latest_trade_date}, not {normalized_market_date}.')

        closes = [float(item['close_price']) for item in bars]
        close_price = int(latest['close_price'])
        ma_60 = _round(_moving_average(closes, MA_60_WINDOW))
        ma_120 = _round(_moving_average(closes, MA_120_WINDOW))
        return_5d = _round(_return_ratio(closes, selected_overheat_rule.lookback_days))
        momentum_12_1 = _round(_momentum_12_1(closes))
        has_sufficient_history = momentum_12_1 is not None
        is_price_above_ma120 = bool(ma_120 is not None and close_price > ma_120)
        is_ma60_above_ma120 = bool(ma_60 is not None and ma_120 is not None and ma_60 > ma_120)
        is_recent_overheated = selected_overheat_rule.classify(return_5d)

        skip_index = len(bars) - (MOMENTUM_SKIP_WINDOW_DAYS + 1)
        lookback_index = len(bars) - (MOMENTUM_LOOKBACK_DAYS + 1)
        price_sources = sorted({str(item.get('source') or MOMENTUM_FACTOR_SOURCE) for item in bars})

        row = {
            'symbol': symbol,
            'market_date': normalized_market_date,
            'source': MOMENTUM_FACTOR_SOURCE,
            'close_price': close_price,
            'ma_60': ma_60,
            'ma_120': ma_120,
            'momentum_12_1': momentum_12_1,
            'return_5d': return_5d,
            'has_sufficient_history': has_sufficient_history,
            'is_price_above_ma120': is_price_above_ma120,
            'is_ma60_above_ma120': is_ma60_above_ma120,
            'is_recent_overheated': is_recent_overheated,
            'momentum_rank': None,
            'momentum_percentile': None,
            'is_momentum_eligible': False,
            'factor_payload': {
                'feature_version': MOMENTUM_FACTOR_VERSION,
                'symbol': symbol,
                'market_date': normalized_market_date,
                'price_basis': {
                    'table': 'daily_price_bars',
                    'field': 'close_price',
                    'stored_sources': price_sources,
                    'series_note': 'Uses stored daily_price_bars close series as-is.',
                },
                'history_requirements': {
                    'bars_available': len(bars),
                    'minimum_bars_for_momentum_12_1': MOMENTUM_HISTORY_WINDOW,
                    'minimum_bars_for_ma_120': MA_120_WINDOW,
                },
                'inputs': {
                    'close': close_price,
                    'reference_close_t_minus_21': (
                        int(bars[skip_index]['close_price']) if has_sufficient_history else None
                    ),
                    'reference_trade_date_t_minus_21': (
                        str(bars[skip_index]['trade_date']) if has_sufficient_history else None
                    ),
                    'reference_close_t_minus_252': (
                        int(bars[lookback_index]['close_price']) if has_sufficient_history else None
                    ),
                    'reference_trade_date_t_minus_252': (
                        str(bars[lookback_index]['trade_date']) if has_sufficient_history else None
                    ),
                },
                'moving_averages': {
                    'ma_60': ma_60,
                    'ma_120': ma_120,
                    'is_price_above_ma120': is_price_above_ma120,
                    'is_ma60_above_ma120': is_ma60_above_ma120,
                },
                'returns': {
                    'momentum_12_1': momentum_12_1,
                    'return_5d': return_5d,
                    'units': 'decimal_return',
                },
                'overheat_rule': selected_overheat_rule.to_payload(),
                'overheat': {
                    'is_recent_overheated': is_recent_overheated,
                },
                'eligibility_rule': {
                    'rule_name': 'trend_confirmed_and_not_recently_overheated',
                    'requires': [
                        'has_sufficient_history',
                        'is_price_above_ma120',
                        'is_ma60_above_ma120',
                        'not is_recent_overheated',
                    ],
                },
                'insufficient_history_reason': (
                    None
                    if has_sufficient_history
                    else f'requires_at_least_{MOMENTUM_HISTORY_WINDOW}_stored_trading_bars'
                ),
                'ranking': {
                    'ranked_universe_size': 0,
                    'total_universe_size': 0,
                    'method': 'momentum_12_1_desc_symbol_asc_tiebreak',
                    'momentum_rank': None,
                    'momentum_percentile': None,
                },
            },
        }
        rows.append(row)

    ranked_rows = sorted(
        [row for row in rows if row['momentum_12_1'] is not None],
        key=lambda item: (-float(item['momentum_12_1']), item['symbol']),
    )
    ranked_universe_size = len(ranked_rows)
    total_universe_size = len(rows)

    for index, row in enumerate(ranked_rows, start=1):
        percentile = 1.0 if ranked_universe_size <= 1 else 1.0 - ((index - 1) / (ranked_universe_size - 1))
        row['momentum_rank'] = index
        row['momentum_percentile'] = _round(percentile)
        row['is_momentum_eligible'] = bool(
            row['has_sufficient_history']
            and row['is_price_above_ma120']
            and row['is_ma60_above_ma120']
            and not row['is_recent_overheated']
        )
        row['factor_payload']['ranking'].update(
            {
                'ranked_universe_size': ranked_universe_size,
                'total_universe_size': total_universe_size,
                'momentum_rank': row['momentum_rank'],
                'momentum_percentile': row['momentum_percentile'],
            }
        )

    for row in rows:
        if row['momentum_rank'] is None:
            row['factor_payload']['ranking'].update(
                {
                    'ranked_universe_size': ranked_universe_size,
                    'total_universe_size': total_universe_size,
                }
            )
        row['factor_payload']['eligibility'] = {
            'is_momentum_eligible': row['is_momentum_eligible'],
            'has_sufficient_history': row['has_sufficient_history'],
            'is_price_above_ma120': row['is_price_above_ma120'],
            'is_ma60_above_ma120': row['is_ma60_above_ma120'],
            'is_recent_overheated': row['is_recent_overheated'],
        }

    return _sort_rows(rows)


class DailyMomentumFactorService:
    def __init__(self, *, overheat_rule: MomentumOverheatRule | None = None) -> None:
        self.overheat_rule = overheat_rule or MomentumOverheatRule()
        self.history_window = MOMENTUM_HISTORY_WINDOW

    def evaluate_symbol(
        self,
        *,
        symbol: str,
        market_date: str | date,
        universe_symbols: Sequence[str] | None = None,
        persist: bool = False,
    ) -> dict[str, Any]:
        normalized_market_date = _normalize_market_date(market_date)
        normalized_symbol = _normalize_symbol(symbol)
        selected_symbols = _normalize_symbols(universe_symbols)

        if selected_symbols is None:
            target_symbols = list_symbols_with_daily_bar_on_date(normalized_market_date)
        else:
            target_symbols = list(selected_symbols)

        if normalized_symbol not in target_symbols:
            target_symbols.append(normalized_symbol)

        price_histories = load_recent_price_bars_by_symbol(
            market_date=normalized_market_date,
            symbols=target_symbols,
            history_days=self.history_window,
        )
        valid_histories = self._extract_valid_histories(
            market_date=normalized_market_date,
            target_symbols=target_symbols,
            price_histories=price_histories,
        )
        if normalized_symbol not in valid_histories:
            raise ValueError(f'No stored daily bar available for {normalized_symbol} on {normalized_market_date}.')

        rows = build_daily_momentum_factor_rows(
            market_date=normalized_market_date,
            price_histories=valid_histories,
            overheat_rule=self.overheat_rule,
        )
        row = next(item for item in rows if item['symbol'] == normalized_symbol)
        if persist:
            upsert_daily_momentum_factor_result(row)
        return row

    def rebuild_for_market_date(
        self,
        *,
        market_date: str | date,
        symbols: Sequence[str] | None = None,
    ) -> dict[str, Any]:
        normalized_market_date = _normalize_market_date(market_date)
        target_symbols = _normalize_symbols(symbols) or list_symbols_with_daily_bar_on_date(normalized_market_date)
        if not target_symbols:
            return {
                'market_date': normalized_market_date,
                'source': MOMENTUM_FACTOR_SOURCE,
                'feature_version': MOMENTUM_FACTOR_VERSION,
                'requested_symbols': 0,
                'symbols_evaluated': 0,
                'ranked_symbols': 0,
                'eligible_symbols': 0,
                'results_persisted': 0,
                'results_failed': 0,
                'failed_symbols': [],
                'items': [],
            }

        price_histories = load_recent_price_bars_by_symbol(
            market_date=normalized_market_date,
            symbols=target_symbols,
            history_days=self.history_window,
        )
        valid_histories = self._extract_valid_histories(
            market_date=normalized_market_date,
            target_symbols=target_symbols,
            price_histories=price_histories,
        )
        failed_symbols = [symbol for symbol in target_symbols if symbol not in valid_histories]

        rows = build_daily_momentum_factor_rows(
            market_date=normalized_market_date,
            price_histories=valid_histories,
            overheat_rule=self.overheat_rule,
        )

        persisted_items: list[dict[str, Any]] = []
        write_failed_symbols: list[str] = []
        for row in rows:
            try:
                upsert_daily_momentum_factor_result(row)
                persisted_items.append(row)
            except Exception:
                write_failed_symbols.append(row['symbol'])

        combined_failed_symbols = list(dict.fromkeys([*failed_symbols, *write_failed_symbols]))
        return {
            'market_date': normalized_market_date,
            'source': MOMENTUM_FACTOR_SOURCE,
            'feature_version': MOMENTUM_FACTOR_VERSION,
            'requested_symbols': len(target_symbols),
            'symbols_evaluated': len(rows),
            'ranked_symbols': sum(1 for row in rows if row.get('momentum_rank') is not None),
            'eligible_symbols': sum(1 for row in rows if row.get('is_momentum_eligible')),
            'results_persisted': len(persisted_items),
            'results_failed': len(combined_failed_symbols),
            'failed_symbols': combined_failed_symbols[:50],
            'items': persisted_items,
        }

    def inspect(
        self,
        *,
        market_date: str | date,
        symbols: Sequence[str] | None = None,
        limit: int | None = 100,
    ) -> dict[str, Any]:
        normalized_market_date = date.fromisoformat(_normalize_market_date(market_date))
        requested_symbols = _normalize_symbols(symbols)
        items = load_daily_momentum_factor_results(
            market_date=normalized_market_date,
            symbols=requested_symbols,
            limit=limit,
        )
        matched_symbols = [item['symbol'] for item in items]
        matched_symbol_set = set(matched_symbols)
        return {
            'market_date': normalized_market_date.isoformat(),
            'feature_version': MOMENTUM_FACTOR_VERSION,
            'summary': {
                'entries': len(items),
                'requested_symbols': requested_symbols or [],
                'matched_symbols': matched_symbols,
                'missing_symbols': [symbol for symbol in (requested_symbols or []) if symbol not in matched_symbol_set],
                'ranked_symbols': sum(1 for item in items if item.get('momentum_rank') is not None),
                'eligible_symbols': sum(1 for item in items if item.get('is_momentum_eligible')),
            },
            'items': items,
        }

    @staticmethod
    def _extract_valid_histories(
        *,
        market_date: str,
        target_symbols: Sequence[str],
        price_histories: dict[str, Sequence[dict[str, Any]]],
    ) -> dict[str, list[dict[str, Any]]]:
        valid_histories: dict[str, list[dict[str, Any]]] = {}
        for symbol in target_symbols:
            bars = _ordered_bars(price_histories.get(symbol) or [])
            if not bars:
                continue
            if str(bars[-1]['trade_date']) != market_date:
                continue
            valid_histories[symbol] = bars
        return valid_histories


daily_momentum_factor_service = DailyMomentumFactorService()


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description='Compute daily price-momentum factor rows from stored daily bars')
    parser.add_argument('--market-date', required=True, help='Market date in YYYY-MM-DD format')
    parser.add_argument('--symbol', action='append', dest='symbols')
    return parser


def main(argv: Iterable[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(list(argv) if argv is not None else None)
    result = daily_momentum_factor_service.rebuild_for_market_date(
        market_date=args.market_date,
        symbols=args.symbols,
    )
    print(result)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
