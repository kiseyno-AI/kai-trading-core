from __future__ import annotations

import argparse
import math
from dataclasses import dataclass
from datetime import date
from statistics import stdev
from typing import Any, Iterable, Mapping, Sequence

from app.market_data_store import (
    list_symbols_with_daily_bar_on_date,
    load_daily_momentum_factor_results,
    load_recent_price_bars_by_symbol,
    replace_daily_derived_results_exact_scope,
    upsert_daily_momentum_factor_result,
    upsert_daily_momentum_factor_results,
)
from app.repositories.eod_run_repository import (
    daily_price_bar_writer_admission,
    daily_price_bar_writer_admission_lock,
)
from app.signals.factor_coverage import (
    SYMBOL_LOCAL_COMPUTATION_ERRORS,
    attach_ranking_dependency_contract,
    build_factor_coverage_contract,
    build_symbol_results,
    factor_stage_result_contract,
    merge_symbol_failures,
    partition_symbol_failures,
    persist_symbol_items_with_isolation,
    run_symbol_computation,
)

MOMENTUM_FACTOR_VERSION = 'price_momentum_v1'
MOMENTUM_FACTOR_SOURCE = 'daily_price_bars'
MOMENTUM_SKIP_WINDOW_DAYS = 21
MOMENTUM_LOOKBACK_DAYS = 252
MOMENTUM_HISTORY_WINDOW = MOMENTUM_LOOKBACK_DAYS + 1
MA_60_WINDOW = 60
MA_120_WINDOW = 120
OVERHEAT_LOOKBACK_DAYS = 5
OVERHEAT_VOLATILITY_WINDOW_DAYS = 20


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


def _percentile_from_rank(rank: int, total: int) -> float:
    if total <= 1:
        return 1.0
    return _round(1.0 - ((rank - 1) / (total - 1))) or 0.0


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


def _realized_return_volatility(values: Sequence[float], *, window: int) -> float | None:
    if len(values) < window + 1:
        return None

    observed_returns: list[float] = []
    close_window = list(values[-(window + 1) :])
    for previous_close, current_close in zip(close_window, close_window[1:]):
        if previous_close <= 0:
            return None
        observed_returns.append((current_close / previous_close) - 1.0)
    if len(observed_returns) < 2:
        return None
    return _round(stdev(observed_returns))


def _volatility_adjusted_return(
    return_value: float | None,
    realized_volatility: float | None,
    *,
    lookback_days: int,
) -> float | None:
    if return_value is None or realized_volatility is None or realized_volatility <= 0:
        return None
    expected_scale = realized_volatility * math.sqrt(float(lookback_days))
    if expected_scale <= 0:
        return None
    return _round(return_value / expected_scale)


def _ordered_bars(bars: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
    return sorted(bars, key=lambda item: str(item['trade_date']))


def _sort_rows(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return sorted(
        items,
        key=lambda item: (
            not bool(item.get('is_momentum_eligible')),
            not bool(item.get('passes_momentum_top_percentile')),
            item.get('momentum_rank') is None,
            item.get('momentum_rank') if item.get('momentum_rank') is not None else 999999,
            item['symbol'],
        ),
    )


MOMENTUM_ELIGIBILITY_STATUS_ELIGIBLE = 'eligible'
MOMENTUM_ELIGIBILITY_STATUS_INSUFFICIENT_HISTORY = 'insufficient_history'
MOMENTUM_ELIGIBILITY_STATUS_TREND_NOT_CONFIRMED = 'trend_not_confirmed'
MOMENTUM_ELIGIBILITY_STATUS_BELOW_TOP_PERCENTILE_CUTOFF = 'below_top_percentile_cutoff'
MOMENTUM_ELIGIBILITY_STATUS_RECENTLY_OVERHEATED = 'recently_overheated'


def _momentum_eligibility_failed_requirements(
    *,
    has_sufficient_history: bool,
    is_price_above_ma120: bool,
    is_ma60_above_ma120: bool,
    passes_momentum_top_percentile: bool,
    is_recent_overheated: bool,
) -> list[str]:
    failed_requirements: list[str] = []
    if not has_sufficient_history:
        failed_requirements.append('has_sufficient_history')
    if not is_price_above_ma120:
        failed_requirements.append('is_price_above_ma120')
    if not is_ma60_above_ma120:
        failed_requirements.append('is_ma60_above_ma120')
    if not passes_momentum_top_percentile:
        failed_requirements.append('passes_momentum_top_percentile')
    if is_recent_overheated:
        failed_requirements.append('not_recently_overheated')
    return failed_requirements


def _momentum_eligibility_status(
    *,
    has_sufficient_history: bool,
    is_price_above_ma120: bool,
    is_ma60_above_ma120: bool,
    passes_momentum_top_percentile: bool,
    is_recent_overheated: bool,
) -> str:
    if not has_sufficient_history:
        return MOMENTUM_ELIGIBILITY_STATUS_INSUFFICIENT_HISTORY
    if not is_price_above_ma120 or not is_ma60_above_ma120:
        return MOMENTUM_ELIGIBILITY_STATUS_TREND_NOT_CONFIRMED
    if not passes_momentum_top_percentile:
        return MOMENTUM_ELIGIBILITY_STATUS_BELOW_TOP_PERCENTILE_CUTOFF
    if is_recent_overheated:
        return MOMENTUM_ELIGIBILITY_STATUS_RECENTLY_OVERHEATED
    return MOMENTUM_ELIGIBILITY_STATUS_ELIGIBLE


@dataclass(frozen=True, slots=True)
class MomentumEligibilityRule:
    top_percentile_threshold: float = 0.80
    rule_version: str = 'momentum_top_percentile_v1'
    rule_name: str = 'trend_confirmed_top_percentile_and_not_recently_overheated'
    comparison: str = '>='

    def passes_momentum_top_percentile(self, momentum_percentile: float | None) -> bool:
        if momentum_percentile is None:
            return False
        return momentum_percentile >= self.top_percentile_threshold

    def evaluate(
        self,
        *,
        has_sufficient_history: bool,
        is_price_above_ma120: bool,
        is_ma60_above_ma120: bool,
        momentum_percentile: float | None,
        is_recent_overheated: bool,
    ) -> dict[str, Any]:
        passes_top_percentile = self.passes_momentum_top_percentile(momentum_percentile)
        failed_requirements = _momentum_eligibility_failed_requirements(
            has_sufficient_history=has_sufficient_history,
            is_price_above_ma120=is_price_above_ma120,
            is_ma60_above_ma120=is_ma60_above_ma120,
            passes_momentum_top_percentile=passes_top_percentile,
            is_recent_overheated=is_recent_overheated,
        )
        status = _momentum_eligibility_status(
            has_sufficient_history=has_sufficient_history,
            is_price_above_ma120=is_price_above_ma120,
            is_ma60_above_ma120=is_ma60_above_ma120,
            passes_momentum_top_percentile=passes_top_percentile,
            is_recent_overheated=is_recent_overheated,
        )
        return {
            'passes_trend_confirmation': bool(is_price_above_ma120 and is_ma60_above_ma120),
            'passes_momentum_top_percentile': passes_top_percentile,
            'momentum_percentile_threshold': _round(self.top_percentile_threshold),
            'momentum_eligibility_status': status,
            'failed_requirements': failed_requirements,
            'is_momentum_eligible': status == MOMENTUM_ELIGIBILITY_STATUS_ELIGIBLE,
        }

    def to_payload(self) -> dict[str, Any]:
        return {
            'rule_version': self.rule_version,
            'rule_name': self.rule_name,
            'requires': [
                'has_sufficient_history',
                'is_price_above_ma120',
                'is_ma60_above_ma120',
                'passes_momentum_top_percentile',
                'not is_recent_overheated',
            ],
            'percentile_gate': {
                'field': 'momentum_percentile',
                'comparison': self.comparison,
                'threshold': _round(self.top_percentile_threshold),
                'threshold_units': '0_to_1_percentile',
                'higher_percentile_is_better': True,
                'top_fraction': _round(1.0 - float(self.top_percentile_threshold)),
            },
        }


@dataclass(frozen=True, slots=True)
class MomentumOverheatRule:
    lookback_days: int = OVERHEAT_LOOKBACK_DAYS
    volatility_window_days: int = OVERHEAT_VOLATILITY_WINDOW_DAYS
    return_percentile_threshold: float = 0.98
    volatility_adjusted_threshold: float = 2.25
    minimum_absolute_return: float = 0.07
    minimum_ranked_universe_size: int = 2
    rule_version: str = 'momentum_overheat_v2'
    rule_type: str = 'return_percentile_and_volatility_adjusted_acceleration'
    comparison: str = '>='

    def has_sufficient_cross_sectional_context(self, ranked_universe_size: int) -> bool:
        return ranked_universe_size >= self.minimum_ranked_universe_size

    def is_return_5d_extreme(
        self,
        return_percentile: float | None,
        *,
        ranked_universe_size: int,
    ) -> bool:
        if return_percentile is None or not self.has_sufficient_cross_sectional_context(ranked_universe_size):
            return False
        return return_percentile >= self.return_percentile_threshold

    def is_volatility_adjusted_overheated(self, volatility_adjusted_return: float | None) -> bool:
        if volatility_adjusted_return is None:
            return False
        return volatility_adjusted_return >= self.volatility_adjusted_threshold

    def passes_minimum_absolute_return(self, return_value: float | None) -> bool:
        if return_value is None:
            return False
        return return_value >= self.minimum_absolute_return

    def classify(
        self,
        *,
        return_value: float | None,
        return_percentile: float | None,
        volatility_adjusted_return: float | None,
        ranked_universe_size: int,
    ) -> dict[str, bool]:
        has_sufficient_cross_sectional_context = self.has_sufficient_cross_sectional_context(ranked_universe_size)
        return_5d_extreme = self.is_return_5d_extreme(
            return_percentile,
            ranked_universe_size=ranked_universe_size,
        )
        volatility_adjusted_overheated = self.is_volatility_adjusted_overheated(volatility_adjusted_return)
        minimum_absolute_return_met = self.passes_minimum_absolute_return(return_value)
        return {
            'has_sufficient_cross_sectional_context': has_sufficient_cross_sectional_context,
            'is_return_5d_extreme': return_5d_extreme,
            'is_volatility_adjusted_overheated': volatility_adjusted_overheated,
            'passes_minimum_absolute_return': minimum_absolute_return_met,
            'is_recent_overheated': bool(
                return_5d_extreme
                and volatility_adjusted_overheated
                and minimum_absolute_return_met
            ),
        }

    def to_payload(self) -> dict[str, Any]:
        return {
            'rule_version': self.rule_version,
            'rule_type': self.rule_type,
            'lookback_days': self.lookback_days,
            'combine_operator': 'all_of',
            'cross_sectional_percentile': {
                'field': 'return_5d_percentile',
                'comparison': self.comparison,
                'threshold': self.return_percentile_threshold,
                'threshold_units': '0_to_1_percentile',
                'extreme_direction': 'higher_is_more_overheated',
                'minimum_ranked_universe_size': self.minimum_ranked_universe_size,
            },
            'volatility_adjusted_return': {
                'field': 'return_5d_vol_adjusted',
                'comparison': self.comparison,
                'threshold': self.volatility_adjusted_threshold,
                'threshold_units': '5d_shock_multiple',
                'formula': (
                    'return_5d / (stdev(simple_daily_returns_over_20d) * sqrt(lookback_days))'
                ),
                'volatility_window_days': self.volatility_window_days,
            },
            'minimum_absolute_return': {
                'field': 'return_5d',
                'comparison': self.comparison,
                'threshold': self.minimum_absolute_return,
                'threshold_units': 'decimal_return',
            },
        }


def resolve_momentum_eligibility_status(item: dict[str, Any] | None) -> str | None:
    if item is None:
        return None

    payload = item.get('factor_payload') or {}
    eligibility = payload.get('eligibility') or {}
    explicit_status = item.get('momentum_eligibility_status') or eligibility.get('momentum_eligibility_status')
    if explicit_status:
        return explicit_status

    threshold = item.get('momentum_percentile_threshold')
    if threshold is None:
        ranking = payload.get('ranking') or {}
        threshold = ranking.get('momentum_percentile_threshold')
    if threshold is None:
        eligibility_rule = payload.get('eligibility_rule') or {}
        percentile_gate = eligibility_rule.get('percentile_gate') or {}
        threshold = percentile_gate.get('threshold')
    if threshold is None:
        threshold = MomentumEligibilityRule().top_percentile_threshold

    passes_top_percentile = item.get('passes_momentum_top_percentile')
    if passes_top_percentile is None and 'passes_momentum_top_percentile' in eligibility:
        passes_top_percentile = eligibility.get('passes_momentum_top_percentile')
    if passes_top_percentile is None:
        percentile = item.get('momentum_percentile')
        passes_top_percentile = bool(percentile is not None and percentile >= float(threshold))

    return _momentum_eligibility_status(
        has_sufficient_history=bool(item.get('has_sufficient_history')),
        is_price_above_ma120=bool(item.get('is_price_above_ma120')),
        is_ma60_above_ma120=bool(item.get('is_ma60_above_ma120')),
        passes_momentum_top_percentile=bool(passes_top_percentile),
        is_recent_overheated=bool(item.get('is_recent_overheated')),
    )


def enrich_momentum_audit_fields(item: dict[str, Any]) -> dict[str, Any]:
    payload = item.get('factor_payload') or {}
    ranking = payload.get('ranking') or {}
    eligibility = payload.get('eligibility') or {}
    eligibility_rule = payload.get('eligibility_rule') or {}
    percentile_gate = eligibility_rule.get('percentile_gate') or {}
    returns = payload.get('returns') or {}
    volatility = payload.get('volatility') or {}
    overheat = payload.get('overheat') or {}
    overheat_rule = payload.get('overheat_rule') or {}

    enriched = dict(item)
    enriched.setdefault(
        'momentum_percentile_threshold',
        ranking.get('momentum_percentile_threshold')
        if 'momentum_percentile_threshold' in ranking
        else percentile_gate.get('threshold'),
    )
    enriched.setdefault(
        'passes_momentum_top_percentile',
        eligibility.get('passes_momentum_top_percentile')
        if 'passes_momentum_top_percentile' in eligibility
        else ranking.get('passes_momentum_top_percentile'),
    )
    enriched.setdefault('return_5d_rank', returns.get('return_5d_rank'))
    enriched.setdefault('return_5d_percentile', returns.get('return_5d_percentile'))
    enriched.setdefault('return_5d_ranked_universe_size', returns.get('return_5d_ranked_universe_size'))
    enriched.setdefault(
        'return_5d_realized_volatility_20d',
        volatility.get('return_5d_realized_volatility_20d'),
    )
    enriched.setdefault('return_5d_vol_adjusted', volatility.get('return_5d_vol_adjusted'))
    enriched.setdefault(
        'has_sufficient_cross_sectional_context',
        overheat.get('has_sufficient_cross_sectional_context'),
    )
    enriched.setdefault('is_return_5d_extreme', overheat.get('is_return_5d_extreme'))
    enriched.setdefault(
        'is_volatility_adjusted_overheated',
        overheat.get('is_volatility_adjusted_overheated'),
    )
    enriched.setdefault(
        'passes_minimum_absolute_return',
        overheat.get('passes_minimum_absolute_return'),
    )
    enriched.setdefault('is_recent_overheated', overheat.get('is_recent_overheated'))
    enriched.setdefault('overheat_rule_version', overheat_rule.get('rule_version'))
    enriched.setdefault('passes_trend_confirmation', eligibility.get('passes_trend_confirmation'))
    enriched.setdefault('eligibility_failed_requirements', eligibility.get('failed_requirements'))
    enriched.setdefault('momentum_eligibility_status', resolve_momentum_eligibility_status(enriched))
    return enriched


def build_daily_momentum_factor_rows(
    *,
    market_date: str | date,
    price_histories: dict[str, Sequence[dict[str, Any]]],
    overheat_rule: MomentumOverheatRule | None = None,
    eligibility_rule: MomentumEligibilityRule | None = None,
) -> list[dict[str, Any]]:
    normalized_market_date = _normalize_market_date(market_date)
    selected_overheat_rule = overheat_rule or MomentumOverheatRule()
    selected_eligibility_rule = eligibility_rule or MomentumEligibilityRule()
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
        realized_volatility_20d = _realized_return_volatility(
            closes,
            window=selected_overheat_rule.volatility_window_days,
        )
        return_5d_vol_adjusted = _volatility_adjusted_return(
            return_5d,
            realized_volatility_20d,
            lookback_days=selected_overheat_rule.lookback_days,
        )
        momentum_12_1 = _round(_momentum_12_1(closes))
        has_sufficient_history = momentum_12_1 is not None
        is_price_above_ma120 = bool(ma_120 is not None and close_price > ma_120)
        is_ma60_above_ma120 = bool(ma_60 is not None and ma_120 is not None and ma_60 > ma_120)

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
            'return_5d_rank': None,
            'return_5d_percentile': None,
            'return_5d_ranked_universe_size': 0,
            'return_5d_realized_volatility_20d': realized_volatility_20d,
            'return_5d_vol_adjusted': return_5d_vol_adjusted,
            'has_sufficient_history': has_sufficient_history,
            'is_price_above_ma120': is_price_above_ma120,
            'is_ma60_above_ma120': is_ma60_above_ma120,
            'has_sufficient_cross_sectional_context': False,
            'is_return_5d_extreme': False,
            'is_volatility_adjusted_overheated': False,
            'passes_minimum_absolute_return': False,
            'overheat_rule_version': selected_overheat_rule.rule_version,
            'is_recent_overheated': False,
            'momentum_rank': None,
            'momentum_percentile': None,
            'momentum_percentile_threshold': _round(selected_eligibility_rule.top_percentile_threshold),
            'passes_momentum_top_percentile': False,
            'is_momentum_eligible': False,
            'momentum_eligibility_status': MOMENTUM_ELIGIBILITY_STATUS_INSUFFICIENT_HISTORY,
            'eligibility_failed_requirements': [],
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
                    'return_5d_rank': None,
                    'return_5d_percentile': None,
                    'return_5d_ranked_universe_size': 0,
                    'units': 'decimal_return',
                },
                'volatility': {
                    'return_5d_realized_volatility_20d': realized_volatility_20d,
                    'return_5d_vol_adjusted': return_5d_vol_adjusted,
                    'daily_return_type': 'simple_close_to_close',
                    'volatility_window_days': selected_overheat_rule.volatility_window_days,
                    'return_5d_vol_adjusted_units': '5d_shock_multiple',
                },
                'overheat_rule': selected_overheat_rule.to_payload(),
                'overheat': {
                    'has_sufficient_cross_sectional_context': False,
                    'is_return_5d_extreme': False,
                    'is_volatility_adjusted_overheated': False,
                    'passes_minimum_absolute_return': False,
                    'is_recent_overheated': False,
                },
                'eligibility_rule': selected_eligibility_rule.to_payload(),
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
                    'momentum_percentile_threshold': _round(selected_eligibility_rule.top_percentile_threshold),
                    'passes_momentum_top_percentile': False,
                },
            },
        }
        rows.append(row)

    return_ranked_rows = sorted(
        [row for row in rows if row['return_5d'] is not None],
        key=lambda item: (-float(item['return_5d']), item['symbol']),
    )
    return_ranked_universe_size = len(return_ranked_rows)
    for index, row in enumerate(return_ranked_rows, start=1):
        row['return_5d_rank'] = index
        row['return_5d_percentile'] = _percentile_from_rank(index, return_ranked_universe_size)
        row['return_5d_ranked_universe_size'] = return_ranked_universe_size
        row['factor_payload']['returns'].update(
            {
                'return_5d_rank': row['return_5d_rank'],
                'return_5d_percentile': row['return_5d_percentile'],
                'return_5d_ranked_universe_size': return_ranked_universe_size,
            }
        )

    for row in rows:
        row['return_5d_ranked_universe_size'] = return_ranked_universe_size
        if row['return_5d_rank'] is None:
            row['factor_payload']['returns']['return_5d_ranked_universe_size'] = return_ranked_universe_size
        overheat_classification = selected_overheat_rule.classify(
            return_value=row['return_5d'],
            return_percentile=row['return_5d_percentile'],
            volatility_adjusted_return=row['return_5d_vol_adjusted'],
            ranked_universe_size=return_ranked_universe_size,
        )
        row['has_sufficient_cross_sectional_context'] = overheat_classification[
            'has_sufficient_cross_sectional_context'
        ]
        row['is_return_5d_extreme'] = overheat_classification['is_return_5d_extreme']
        row['is_volatility_adjusted_overheated'] = overheat_classification['is_volatility_adjusted_overheated']
        row['passes_minimum_absolute_return'] = overheat_classification['passes_minimum_absolute_return']
        row['is_recent_overheated'] = overheat_classification['is_recent_overheated']
        row['factor_payload']['overheat'].update(overheat_classification)

    ranked_rows = sorted(
        [row for row in rows if row['momentum_12_1'] is not None],
        key=lambda item: (-float(item['momentum_12_1']), item['symbol']),
    )
    ranked_universe_size = len(ranked_rows)
    total_universe_size = len(rows)

    for index, row in enumerate(ranked_rows, start=1):
        row['momentum_rank'] = index
        row['momentum_percentile'] = _percentile_from_rank(index, ranked_universe_size)
        row['factor_payload']['ranking'].update(
            {
                'ranked_universe_size': ranked_universe_size,
                'total_universe_size': total_universe_size,
                'momentum_rank': row['momentum_rank'],
                'momentum_percentile': row['momentum_percentile'],
                'momentum_percentile_threshold': _round(selected_eligibility_rule.top_percentile_threshold),
            }
        )

    for row in rows:
        if row['momentum_rank'] is None:
            row['factor_payload']['ranking'].update(
                {
                    'ranked_universe_size': ranked_universe_size,
                    'total_universe_size': total_universe_size,
                    'momentum_percentile_threshold': _round(selected_eligibility_rule.top_percentile_threshold),
                }
            )
        eligibility = selected_eligibility_rule.evaluate(
            has_sufficient_history=row['has_sufficient_history'],
            is_price_above_ma120=row['is_price_above_ma120'],
            is_ma60_above_ma120=row['is_ma60_above_ma120'],
            momentum_percentile=row['momentum_percentile'],
            is_recent_overheated=row['is_recent_overheated'],
        )
        row['passes_momentum_top_percentile'] = eligibility['passes_momentum_top_percentile']
        row['is_momentum_eligible'] = eligibility['is_momentum_eligible']
        row['momentum_eligibility_status'] = eligibility['momentum_eligibility_status']
        row['eligibility_failed_requirements'] = list(eligibility['failed_requirements'])
        row['factor_payload']['ranking']['passes_momentum_top_percentile'] = row['passes_momentum_top_percentile']
        row['factor_payload']['eligibility'] = {
            'is_momentum_eligible': row['is_momentum_eligible'],
            'momentum_eligibility_status': row['momentum_eligibility_status'],
            'has_sufficient_history': row['has_sufficient_history'],
            'passes_trend_confirmation': eligibility['passes_trend_confirmation'],
            'is_price_above_ma120': row['is_price_above_ma120'],
            'is_ma60_above_ma120': row['is_ma60_above_ma120'],
            'momentum_percentile_threshold': eligibility['momentum_percentile_threshold'],
            'passes_momentum_top_percentile': row['passes_momentum_top_percentile'],
            'has_sufficient_cross_sectional_context': row['has_sufficient_cross_sectional_context'],
            'is_return_5d_extreme': row['is_return_5d_extreme'],
            'is_volatility_adjusted_overheated': row['is_volatility_adjusted_overheated'],
            'passes_minimum_absolute_return': row['passes_minimum_absolute_return'],
            'is_recent_overheated': row['is_recent_overheated'],
            'failed_requirements': row['eligibility_failed_requirements'],
        }

    return _sort_rows(rows)


class DailyMomentumFactorService:
    def __init__(
        self,
        *,
        overheat_rule: MomentumOverheatRule | None = None,
        eligibility_rule: MomentumEligibilityRule | None = None,
    ) -> None:
        self.overheat_rule = overheat_rule or MomentumOverheatRule()
        self.eligibility_rule = eligibility_rule or MomentumEligibilityRule()
        self.history_window = MOMENTUM_HISTORY_WINDOW

    def evaluate_symbol(
        self,
        *,
        symbol: str,
        market_date: str | date,
        universe_symbols: Sequence[str] | None = None,
        persist: bool = False,
    ) -> dict[str, Any]:
        if persist:
            with daily_price_bar_writer_admission_lock(
                market_date=market_date,
            ):
                return self._evaluate_symbol(
                    symbol=symbol,
                    market_date=market_date,
                    universe_symbols=universe_symbols,
                    persist=True,
                )
        return self._evaluate_symbol(
            symbol=symbol,
            market_date=market_date,
            universe_symbols=universe_symbols,
            persist=False,
        )

    def _evaluate_symbol(
        self,
        *,
        symbol: str,
        market_date: str | date,
        universe_symbols: Sequence[str] | None,
        persist: bool,
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
            eligibility_rule=self.eligibility_rule,
        )
        row = next(item for item in rows if item['symbol'] == normalized_symbol)
        if persist:
            upsert_daily_momentum_factor_result(row)
        return enrich_momentum_audit_fields(row)

    @daily_price_bar_writer_admission
    def rebuild_for_market_date(
        self,
        *,
        market_date: str | date,
        symbols: Sequence[str] | None = None,
        ranking_symbols: Sequence[str] | None = None,
        exact_scope_symbols: Sequence[str] | None = None,
        authoritative_market_scope: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        normalized_market_date = _normalize_market_date(market_date)
        requested_symbols = _normalize_symbols(symbols)
        normalized_ranking_symbols = _normalize_symbols(ranking_symbols)
        if ranking_symbols is None:
            ranking_population = list_symbols_with_daily_bar_on_date(
                normalized_market_date
            )
        else:
            ranking_population = normalized_ranking_symbols or []
        target_symbols = (
            requested_symbols
            if requested_symbols is not None
            else ranking_population
        )
        explicit_empty_scope = (
            (symbols is not None and requested_symbols == [])
            or (ranking_symbols is not None and normalized_ranking_symbols == [])
        )
        if ranking_symbols is not None:
            outside_ranking_scope = sorted(
                set(target_symbols).difference(ranking_population)
            )
            if outside_ranking_scope:
                raise ValueError(
                    'requested symbols must be contained in ranking_symbols: '
                    + ','.join(outside_ranking_scope[:20])
                )
        if not target_symbols:
            summary = self._build_coverage_contract(
                items=[],
                persisted_items=[],
                missing_symbols=[],
                failed_symbols=[],
                failed_symbol_errors=[],
            )
            stage_contract = (
                {
                    'status': 'completed',
                    'reason': 'explicit_empty_scope_noop',
                    'quality_status': summary['quality_status'],
                    'quality_warning': summary['quality_warning'],
                    'gate_status': 'ready',
                    'blocking_status': 'non_blocking',
                    'remaining_retryable_events': 0,
                }
                if explicit_empty_scope
                else factor_stage_result_contract(summary)
            )
            result = {
                **stage_contract,
                'market_date': normalized_market_date,
                'source': MOMENTUM_FACTOR_SOURCE,
                'feature_version': MOMENTUM_FACTOR_VERSION,
                'eligibility_rule_version': self.eligibility_rule.rule_version,
                'momentum_percentile_threshold': _round(self.eligibility_rule.top_percentile_threshold),
                'requested_symbols': 0,
                'requested_symbol_count': 0,
                'symbols_evaluated': 0,
                'ready_count': 0,
                'unready_count': 0,
                'persisted_ready_count': 0,
                'persisted_unready_count': 0,
                'insufficient_history_count': 0,
                'missing_market_date_bar_count': 0,
                'ranked_symbols': 0,
                'top_percentile_symbols': 0,
                'eligible_symbols': 0,
                'results_persisted': 0,
                'results_failed': 0,
                'failed_symbols': [],
                'summary': summary,
                'items': [],
            }
            if exact_scope_symbols is not None:
                result['scope_finalization'] = (
                    replace_daily_derived_results_exact_scope(
                        market_date=normalized_market_date,
                        stage_id='momentum_factors',
                        results=[],
                        exact_symbols=exact_scope_symbols,
                        authoritative_market_scope=(
                            authoritative_market_scope
                        ),
                    )['scope_finalization']
                )
            return result

        price_histories = load_recent_price_bars_by_symbol(
            market_date=normalized_market_date,
            symbols=ranking_population,
            history_days=self.history_window,
        )
        (
            history_rows,
            computation_failed_symbols,
            computation_failed_symbol_errors,
        ) = build_symbol_results(
            ranking_population,
            symbol_of=lambda symbol: symbol,
            build=lambda symbol: run_symbol_computation(
                lambda: {
                    'symbol': symbol,
                    'bars': self._validated_history_or_none(
                        market_date=normalized_market_date,
                        bars=price_histories.get(symbol) or [],
                    ),
                }
            ),
        )
        valid_histories = {
            row['symbol']: row['bars']
            for row in history_rows
            if row['bars'] is not None
        }
        history_computation_failed_set = set(computation_failed_symbols)
        ranking_missing_history_symbols = [
            symbol for symbol in ranking_population
            if symbol not in valid_histories
            and symbol not in history_computation_failed_set
        ]
        try:
            ranking_rows = build_daily_momentum_factor_rows(
                market_date=normalized_market_date,
                price_histories=valid_histories,
                overheat_rule=self.overheat_rule,
                eligibility_rule=self.eligibility_rule,
            )
        except SYMBOL_LOCAL_COMPUTATION_ERRORS:
            _, isolated_failed_symbols, isolated_failed_errors = (
                build_symbol_results(
                    list(valid_histories.items()),
                    symbol_of=lambda item: item[0],
                    build=lambda item: run_symbol_computation(
                        lambda: build_daily_momentum_factor_rows(
                            market_date=normalized_market_date,
                            price_histories={item[0]: item[1]},
                            overheat_rule=self.overheat_rule,
                            eligibility_rule=self.eligibility_rule,
                        )[0]
                    ),
                )
            )
            if (
                not isolated_failed_symbols
                or len(isolated_failed_symbols) == len(valid_histories)
            ):
                raise
            failed_set = set(isolated_failed_symbols)
            ranking_rows = build_daily_momentum_factor_rows(
                market_date=normalized_market_date,
                price_histories={
                    symbol: history
                    for symbol, history in valid_histories.items()
                    if symbol not in failed_set
                },
                overheat_rule=self.overheat_rule,
                eligibility_rule=self.eligibility_rule,
            )
            computation_failed_symbols, computation_failed_symbol_errors = (
                merge_symbol_failures(
                    (
                        computation_failed_symbols,
                        computation_failed_symbol_errors,
                    ),
                    (isolated_failed_symbols, isolated_failed_errors),
                )
            )
        (
            computation_failed_symbols,
            computation_failed_symbol_errors,
            ranking_dependency_failed_symbols,
            ranking_dependency_failed_symbol_errors,
        ) = partition_symbol_failures(
            failed_symbols=computation_failed_symbols,
            failed_symbol_errors=computation_failed_symbol_errors,
            target_symbols=target_symbols,
        )
        (
            missing_history_symbols,
            _,
            ranking_dependency_missing_symbols,
            ranking_dependency_missing_errors,
        ) = partition_symbol_failures(
            failed_symbols=ranking_missing_history_symbols,
            failed_symbol_errors=[
                {
                    'symbol': symbol,
                    'error': 'missing_market_date_bar',
                }
                for symbol in ranking_missing_history_symbols
            ],
            target_symbols=target_symbols,
        )
        (
            ranking_dependency_failed_symbols,
            ranking_dependency_failed_symbol_errors,
        ) = merge_symbol_failures(
            (
                ranking_dependency_failed_symbols,
                ranking_dependency_failed_symbol_errors,
            ),
            (
                ranking_dependency_missing_symbols,
                ranking_dependency_missing_errors,
            ),
        )
        failed_symbols = list(
            dict.fromkeys([
                *missing_history_symbols,
                *computation_failed_symbols,
            ])
        )
        if requested_symbols:
            target_symbol_set = set(target_symbols)
            rows = [row for row in ranking_rows if row['symbol'] in target_symbol_set]
        else:
            rows = ranking_rows

        persisted_items: list[dict[str, Any]] = []
        write_failed_symbols: list[str] = []
        write_failed_symbol_errors: list[dict[str, str]] = []
        scope_finalization: dict[str, Any] | None = None
        if exact_scope_symbols is not None:
            publication = replace_daily_derived_results_exact_scope(
                market_date=normalized_market_date,
                stage_id='momentum_factors',
                results=rows,
                exact_symbols=exact_scope_symbols,
                authoritative_market_scope=authoritative_market_scope,
            )
            successful_symbol_set = set(publication['successful_symbols'])
            persisted_items = [
                row for row in rows
                if row['symbol'] in successful_symbol_set
            ]
            write_failed_symbols = list(publication['failed_symbols'])
            write_failed_symbol_errors = list(
                publication['failed_symbol_errors']
            )
            scope_finalization = publication['scope_finalization']
        else:
            (
                persisted_items,
                write_failed_symbols,
                write_failed_symbol_errors,
            ) = persist_symbol_items_with_isolation(
                rows,
                bulk_write=upsert_daily_momentum_factor_results,
                item_write=upsert_daily_momentum_factor_result,
            )

        combined_failed_symbols, combined_failed_symbol_errors = (
            merge_symbol_failures(
                (
                    missing_history_symbols,
                    [
                        {
                            'symbol': symbol,
                            'error': 'missing_market_date_bar',
                        }
                        for symbol in missing_history_symbols
                    ],
                ),
                (
                    computation_failed_symbols,
                    computation_failed_symbol_errors,
                ),
                (write_failed_symbols, write_failed_symbol_errors),
            )
        )
        persisted_ready_count = sum(
            1 for row in persisted_items if row.get('momentum_percentile') is not None
        )
        coverage_failed_symbols = list(
            dict.fromkeys([
                *computation_failed_symbols,
                *write_failed_symbols,
            ])
        )
        summary = self._build_coverage_contract(
            items=rows,
            persisted_items=persisted_items,
            missing_symbols=missing_history_symbols,
            failed_symbols=coverage_failed_symbols,
            failed_symbol_errors=[
                *computation_failed_symbol_errors,
                *write_failed_symbol_errors,
            ],
        )
        attach_ranking_dependency_contract(
            summary,
            failed_symbols=ranking_dependency_failed_symbols,
            failed_symbol_errors=ranking_dependency_failed_symbol_errors,
        )
        result = {
            **factor_stage_result_contract(summary),
            'market_date': normalized_market_date,
            'source': MOMENTUM_FACTOR_SOURCE,
            'feature_version': MOMENTUM_FACTOR_VERSION,
            'eligibility_rule_version': self.eligibility_rule.rule_version,
            'momentum_percentile_threshold': _round(self.eligibility_rule.top_percentile_threshold),
            'requested_symbols': len(target_symbols),
            'requested_symbol_count': len(target_symbols),
            'symbols_evaluated': len(rows),
            'ready_count': sum(1 for row in rows if row.get('momentum_percentile') is not None),
            'unready_count': (
                len(failed_symbols)
                + sum(1 for row in rows if row.get('momentum_percentile') is None)
            ),
            'persisted_ready_count': persisted_ready_count,
            'persisted_unready_count': len(persisted_items) - persisted_ready_count,
            'insufficient_history_count': sum(
                1 for row in rows if not row.get('has_sufficient_history')
            ),
            'missing_market_date_bar_count': len(missing_history_symbols),
            'ranked_symbols': sum(1 for row in rows if row.get('momentum_rank') is not None),
            'top_percentile_symbols': sum(1 for row in rows if row.get('passes_momentum_top_percentile')),
            'eligible_symbols': sum(1 for row in rows if row.get('is_momentum_eligible')),
            'results_persisted': len(persisted_items),
            'results_failed': len(combined_failed_symbols),
            'failed_symbols': combined_failed_symbols[:50],
            'failed_symbol_errors': combined_failed_symbol_errors[:50],
            'summary': summary,
            'items': [enrich_momentum_audit_fields(item) for item in persisted_items],
        }
        if scope_finalization is not None:
            result['scope_finalization'] = scope_finalization
        return result

    def inspect(
        self,
        *,
        market_date: str | date,
        symbols: Sequence[str] | None = None,
        limit: int | None = 100,
    ) -> dict[str, Any]:
        normalized_market_date = date.fromisoformat(_normalize_market_date(market_date))
        requested_symbols = _normalize_symbols(symbols)
        items = [
            enrich_momentum_audit_fields(item)
            for item in load_daily_momentum_factor_results(
                market_date=normalized_market_date,
                symbols=requested_symbols,
                limit=limit,
            )
        ]
        matched_symbols = [item['symbol'] for item in items]
        matched_symbol_set = set(matched_symbols)
        return {
            'market_date': normalized_market_date.isoformat(),
            'feature_version': MOMENTUM_FACTOR_VERSION,
            'eligibility_rule_version': self.eligibility_rule.rule_version,
            'momentum_percentile_threshold': _round(self.eligibility_rule.top_percentile_threshold),
            'summary': {
                'entries': len(items),
                'requested_symbols': requested_symbols or [],
                'matched_symbols': matched_symbols,
                'missing_symbols': [symbol for symbol in (requested_symbols or []) if symbol not in matched_symbol_set],
                'ranked_symbols': sum(1 for item in items if item.get('momentum_rank') is not None),
                'top_percentile_symbols': sum(1 for item in items if item.get('passes_momentum_top_percentile')),
                'eligible_symbols': sum(1 for item in items if item.get('is_momentum_eligible')),
            },
            'items': items,
        }

    @staticmethod
    def _validated_history_or_none(
        *,
        market_date: str,
        bars: Sequence[dict[str, Any]],
    ) -> list[dict[str, Any]] | None:
        ordered_bars = _ordered_bars(bars)
        if not ordered_bars:
            return None
        if str(ordered_bars[-1]['trade_date']) != market_date:
            return None
        return ordered_bars

    @staticmethod
    def _extract_valid_histories(
        *,
        market_date: str,
        target_symbols: Sequence[str],
        price_histories: dict[str, Sequence[dict[str, Any]]],
    ) -> dict[str, list[dict[str, Any]]]:
        valid_histories: dict[str, list[dict[str, Any]]] = {}
        for symbol in target_symbols:
            bars = DailyMomentumFactorService._validated_history_or_none(
                market_date=market_date,
                bars=price_histories.get(symbol) or [],
            )
            if bars is None:
                continue
            valid_histories[symbol] = bars
        return valid_histories

    @staticmethod
    def _build_coverage_contract(
        *,
        items: list[dict[str, Any]],
        persisted_items: list[dict[str, Any]],
        missing_symbols: list[str],
        failed_symbols: list[str],
        failed_symbol_errors: list[dict[str, str]] | None = None,
    ) -> dict[str, Any]:
        def momentum_available(item: dict[str, Any]) -> bool:
            return item.get('momentum_12_1') is not None

        def trend_available(item: dict[str, Any]) -> bool:
            return item.get('ma_60') is not None and item.get('ma_120') is not None

        def overheat_available(item: dict[str, Any]) -> bool:
            return all(
                item.get(key) is not None
                for key in (
                    'return_5d',
                    'return_5d_realized_volatility_20d',
                    'return_5d_vol_adjusted',
                )
            )

        return build_factor_coverage_contract(
            items=items,
            persisted_items=persisted_items,
            missing_symbols=missing_symbols,
            failed_symbols=failed_symbols,
            failed_symbol_errors=failed_symbol_errors or [],
            is_ready=lambda item: item.get('momentum_percentile') is not None,
            is_complete=lambda item: (
                momentum_available(item)
                and trend_available(item)
                and overheat_available(item)
            ),
            unusable_reason=lambda item: item.get('momentum_eligibility_status')
            or (
                MOMENTUM_ELIGIBILITY_STATUS_INSUFFICIENT_HISTORY
                if not item.get('has_sufficient_history')
                else 'unusable_momentum_row'
            ),
            input_families={
                'momentum_12_1': (
                    momentum_available,
                    lambda item: (
                        None
                        if momentum_available(item)
                        else MOMENTUM_ELIGIBILITY_STATUS_INSUFFICIENT_HISTORY
                    ),
                ),
                'trend_confirmation': (
                    trend_available,
                    lambda item: (
                        None
                        if trend_available(item)
                        else 'trend_input_unavailable'
                    ),
                ),
                'overheat_evidence': (
                    overheat_available,
                    lambda item: (
                        None
                        if overheat_available(item)
                        else 'overheat_evidence_unavailable'
                    ),
                ),
            },
        )


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
