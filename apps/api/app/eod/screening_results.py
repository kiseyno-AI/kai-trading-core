from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from typing import Any, Callable

from sqlalchemy import case, func, select

from app.db import session_scope
from app.db_models import (
    DailyAdvancedValueFactorResult,
    DailyBetaFactorResult,
    DailyCompositeFactorResult,
    DailyLowRiskFactorResult,
    DailyMarketRiskFilterResult,
    DailyMomentumFactorResult,
    DailyPiotroskiFilterResult,
    DailyUnifiedValueFactorResult,
    DailyValueFactorResult,
    Symbol,
)
from app.signals.advanced_value_factor import ADVANCED_VALUE_FACTOR_VERSION
from app.signals.beta_factor import BETA_FACTOR_VERSION
from app.signals.composite_factor import COMPOSITE_FACTOR_VERSION
from app.signals.low_risk_factor import LOW_RISK_FEATURE_VERSION
from app.signals.market_risk_filter import MARKET_RISK_FILTER_SOURCE
from app.signals.momentum_factor import MOMENTUM_FACTOR_VERSION
from app.signals.piotroski_filter import FILTER_SOURCE as PIOTROSKI_FILTER_SOURCE
from app.signals.unified_value_factor import UNIFIED_VALUE_FACTOR_VERSION
from app.signals.value_factor import VALUE_FACTOR_VERSION


def _normalize_optional_market_date(value: str | date | None) -> date | None:
    if value is None:
        return None
    if isinstance(value, date):
        return value
    normalized = str(value).strip()
    if not normalized:
        raise ValueError('market_date must not be blank')
    return date.fromisoformat(normalized)


def _round_float(value: Any, digits: int = 6) -> float | None:
    if value is None:
        return None
    try:
        return round(float(value), digits)
    except (TypeError, ValueError):
        return None


def _as_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _as_iso_datetime(value: Any) -> str | None:
    if not isinstance(value, datetime):
        return None
    return value.isoformat()


def _identity(value: Any) -> Any:
    return value


def _count_when(condition: Any) -> Any:
    return func.coalesce(func.sum(case((condition, 1), else_=0)), 0)


@dataclass(frozen=True)
class ScreeningField:
    key: str
    column: Any
    transform: Callable[[Any], Any] = _identity


@dataclass(frozen=True)
class ScreeningMetric:
    key: str
    expression: Any


@dataclass(frozen=True)
class ScreeningFamilyDefinition:
    family: str
    feature_version: str
    detail_route: str
    model: Any
    item_fields: tuple[ScreeningField, ...]
    order_by: tuple[Any, ...]
    summary_fields: tuple[ScreeningMetric, ...]


COMPOSITE_CLASSIFICATION_ORDER = case(
    (DailyCompositeFactorResult.composite_classification == 'eligible', 0),
    (DailyCompositeFactorResult.composite_classification == 'watchlist', 1),
    (DailyCompositeFactorResult.composite_classification == 'ineligible', 2),
    (DailyCompositeFactorResult.composite_classification == 'hard_excluded', 3),
    else_=4,
)
MOMENTUM_ELIGIBILITY_STATUS = case(
    (DailyMomentumFactorResult.has_sufficient_history.is_(False), 'insufficient_history'),
    (
        DailyMomentumFactorResult.is_price_above_ma120.is_(False)
        | DailyMomentumFactorResult.is_ma60_above_ma120.is_(False),
        'trend_not_confirmed',
    ),
    (DailyMomentumFactorResult.passes_momentum_top_percentile.is_(False), 'below_top_percentile_cutoff'),
    (DailyMomentumFactorResult.is_recent_overheated.is_(True), 'recently_overheated'),
    (DailyMomentumFactorResult.is_momentum_eligible.is_(True), 'eligible'),
    else_='ineligible',
)

SCREENING_FAMILY_DEFINITIONS: dict[str, ScreeningFamilyDefinition] = {
    'market_risk_filter': ScreeningFamilyDefinition(
        family='market_risk_filter',
        feature_version=MARKET_RISK_FILTER_SOURCE,
        detail_route='/eod/market-risk-filters',
        model=DailyMarketRiskFilterResult,
        item_fields=(
            ScreeningField('risk_state', DailyMarketRiskFilterResult.risk_state),
            ScreeningField('passed_filter', DailyMarketRiskFilterResult.passed_filter),
            ScreeningField('watchlist_flagged', DailyMarketRiskFilterResult.watchlist_flagged),
            ScreeningField('market_warning_code', DailyMarketRiskFilterResult.market_warning_code),
            ScreeningField('market_warning_name', DailyMarketRiskFilterResult.market_warning_name),
            ScreeningField('captured_at', DailyMarketRiskFilterResult.captured_at, _as_iso_datetime),
        ),
        order_by=(
            DailyMarketRiskFilterResult.passed_filter.asc(),
            DailyMarketRiskFilterResult.watchlist_flagged.desc(),
            Symbol.symbol.asc(),
        ),
        summary_fields=(
            ScreeningMetric('entries', func.count(DailyMarketRiskFilterResult.id)),
            ScreeningMetric('passed_count', _count_when(DailyMarketRiskFilterResult.passed_filter.is_(True))),
            ScreeningMetric('watchlist_count', _count_when(DailyMarketRiskFilterResult.watchlist_flagged.is_(True))),
            ScreeningMetric('failed_count', _count_when(DailyMarketRiskFilterResult.passed_filter.is_(False))),
        ),
    ),
    'piotroski_filter': ScreeningFamilyDefinition(
        family='piotroski_filter',
        feature_version=PIOTROSKI_FILTER_SOURCE,
        detail_route='/eod/piotroski-filter-results',
        model=DailyPiotroskiFilterResult,
        item_fields=(
            ScreeningField('pbr', DailyPiotroskiFilterResult.pbr, _round_float),
            ScreeningField('valuation_available', DailyPiotroskiFilterResult.valuation_available),
            ScreeningField('pbr_percentile_rank', DailyPiotroskiFilterResult.pbr_percentile_rank, _round_float),
            ScreeningField('is_low_pbr', DailyPiotroskiFilterResult.is_low_pbr),
            ScreeningField('piotroski_score', DailyPiotroskiFilterResult.piotroski_score, _as_int),
            ScreeningField('passes_piotroski_score', DailyPiotroskiFilterResult.passes_piotroski_score),
            ScreeningField('momentum_percentile_rank', DailyPiotroskiFilterResult.momentum_percentile_rank, _round_float),
            ScreeningField('is_bottom_momentum', DailyPiotroskiFilterResult.is_bottom_momentum),
            ScreeningField('passed_filter', DailyPiotroskiFilterResult.passed_filter),
            ScreeningField('captured_at', DailyPiotroskiFilterResult.captured_at, _as_iso_datetime),
        ),
        order_by=(
            DailyPiotroskiFilterResult.passed_filter.desc(),
            DailyPiotroskiFilterResult.pbr_percentile_rank.asc().nullslast(),
            Symbol.symbol.asc(),
        ),
        summary_fields=(
            ScreeningMetric('entries', func.count(DailyPiotroskiFilterResult.id)),
            ScreeningMetric('passed_count', _count_when(DailyPiotroskiFilterResult.passed_filter.is_(True))),
            ScreeningMetric('not_passed_count', _count_when(DailyPiotroskiFilterResult.passed_filter.is_(False))),
            ScreeningMetric('missing_valuation_count', _count_when(DailyPiotroskiFilterResult.valuation_available.is_(False))),
            ScreeningMetric('bottom_momentum_count', _count_when(DailyPiotroskiFilterResult.is_bottom_momentum.is_(True))),
        ),
    ),
    'momentum': ScreeningFamilyDefinition(
        family='momentum',
        feature_version=MOMENTUM_FACTOR_VERSION,
        detail_route='/eod/momentum-factors',
        model=DailyMomentumFactorResult,
        item_fields=(
            ScreeningField('close_price', DailyMomentumFactorResult.close_price, _as_int),
            ScreeningField('momentum_rank', DailyMomentumFactorResult.momentum_rank, _as_int),
            ScreeningField('momentum_percentile', DailyMomentumFactorResult.momentum_percentile, _round_float),
            ScreeningField(
                'momentum_percentile_threshold',
                DailyMomentumFactorResult.momentum_percentile_threshold,
                _round_float,
            ),
            ScreeningField(
                'passes_momentum_top_percentile',
                DailyMomentumFactorResult.passes_momentum_top_percentile,
            ),
            ScreeningField('momentum_eligibility_status', MOMENTUM_ELIGIBILITY_STATUS),
            ScreeningField('return_5d', DailyMomentumFactorResult.return_5d, _round_float),
            ScreeningField('has_sufficient_history', DailyMomentumFactorResult.has_sufficient_history),
            ScreeningField('is_price_above_ma120', DailyMomentumFactorResult.is_price_above_ma120),
            ScreeningField('is_ma60_above_ma120', DailyMomentumFactorResult.is_ma60_above_ma120),
            ScreeningField('is_recent_overheated', DailyMomentumFactorResult.is_recent_overheated),
            ScreeningField('is_momentum_eligible', DailyMomentumFactorResult.is_momentum_eligible),
            ScreeningField('captured_at', DailyMomentumFactorResult.captured_at, _as_iso_datetime),
        ),
        order_by=(
            DailyMomentumFactorResult.is_momentum_eligible.desc(),
            DailyMomentumFactorResult.passes_momentum_top_percentile.desc(),
            DailyMomentumFactorResult.momentum_rank.asc().nullslast(),
            Symbol.symbol.asc(),
        ),
        summary_fields=(
            ScreeningMetric('entries', func.count(DailyMomentumFactorResult.id)),
            ScreeningMetric('ranked_symbols', _count_when(DailyMomentumFactorResult.momentum_rank.is_not(None))),
            ScreeningMetric(
                'top_percentile_symbols',
                _count_when(DailyMomentumFactorResult.passes_momentum_top_percentile.is_(True)),
            ),
            ScreeningMetric('eligible_symbols', _count_when(DailyMomentumFactorResult.is_momentum_eligible.is_(True))),
            ScreeningMetric('overheated_count', _count_when(DailyMomentumFactorResult.is_recent_overheated.is_(True))),
            ScreeningMetric(
                'insufficient_history_count',
                _count_when(DailyMomentumFactorResult.has_sufficient_history.is_(False)),
            ),
        ),
    ),
    'value': ScreeningFamilyDefinition(
        family='value',
        feature_version=VALUE_FACTOR_VERSION,
        detail_route='/eod/value-factors',
        model=DailyValueFactorResult,
        item_fields=(
            ScreeningField('per', DailyValueFactorResult.per, _round_float),
            ScreeningField('pbr', DailyValueFactorResult.pbr, _round_float),
            ScreeningField('dividend_yield', DailyValueFactorResult.dividend_yield, _round_float),
            ScreeningField('value_rank', DailyValueFactorResult.value_rank, _as_int),
            ScreeningField('value_score', DailyValueFactorResult.value_score, _round_float),
            ScreeningField('value_percentile', DailyValueFactorResult.value_percentile, _round_float),
            ScreeningField('value_status', DailyValueFactorResult.value_status),
            ScreeningField('captured_at', DailyValueFactorResult.captured_at, _as_iso_datetime),
        ),
        order_by=(
            DailyValueFactorResult.value_rank.asc().nullslast(),
            DailyValueFactorResult.value_score.desc().nullslast(),
            Symbol.symbol.asc(),
        ),
        summary_fields=(
            ScreeningMetric('entries', func.count(DailyValueFactorResult.id)),
            ScreeningMetric('scored_count', _count_when(DailyValueFactorResult.value_status == 'scored')),
            ScreeningMetric('invalid_per_count', _count_when(DailyValueFactorResult.value_status == 'invalid_per')),
            ScreeningMetric(
                'missing_metric_count',
                _count_when(DailyValueFactorResult.value_status == 'missing_metric'),
            ),
        ),
    ),
    'low_risk': ScreeningFamilyDefinition(
        family='low_risk',
        feature_version=LOW_RISK_FEATURE_VERSION,
        detail_route='/eod/low-risk-factors',
        model=DailyLowRiskFactorResult,
        item_fields=(
            ScreeningField('low_risk_score', DailyLowRiskFactorResult.low_risk_score, _round_float),
            ScreeningField('low_risk_classification', DailyLowRiskFactorResult.low_risk_classification),
            ScreeningField('is_low_risk_eligible', DailyLowRiskFactorResult.is_low_risk_eligible),
            ScreeningField('liquidity_percentile', DailyLowRiskFactorResult.liquidity_percentile, _round_float),
            ScreeningField('passes_liquidity_floor', DailyLowRiskFactorResult.passes_liquidity_floor),
            ScreeningField('history_days_available', DailyLowRiskFactorResult.history_days_available, _as_int),
            ScreeningField('captured_at', DailyLowRiskFactorResult.captured_at, _as_iso_datetime),
        ),
        order_by=(
            DailyLowRiskFactorResult.is_low_risk_eligible.desc(),
            DailyLowRiskFactorResult.low_risk_score.desc().nullslast(),
            DailyLowRiskFactorResult.liquidity_percentile.desc().nullslast(),
            Symbol.symbol.asc(),
        ),
        summary_fields=(
            ScreeningMetric('entries', func.count(DailyLowRiskFactorResult.id)),
            ScreeningMetric('eligible_count', _count_when(DailyLowRiskFactorResult.is_low_risk_eligible.is_(True))),
            ScreeningMetric('watchlist_count', _count_when(DailyLowRiskFactorResult.low_risk_classification == 'watchlist')),
            ScreeningMetric('ineligible_count', _count_when(DailyLowRiskFactorResult.low_risk_classification == 'ineligible')),
            ScreeningMetric(
                'liquidity_excluded_count',
                _count_when(DailyLowRiskFactorResult.low_risk_classification == 'liquidity_excluded'),
            ),
            ScreeningMetric(
                'insufficient_history_count',
                _count_when(DailyLowRiskFactorResult.low_risk_classification == 'insufficient_history'),
            ),
            ScreeningMetric(
                'missing_market_date_bar_count',
                _count_when(DailyLowRiskFactorResult.low_risk_classification == 'missing_market_date_bar'),
            ),
        ),
    ),
    'beta': ScreeningFamilyDefinition(
        family='beta',
        feature_version=BETA_FACTOR_VERSION,
        detail_route='/eod/beta-factors',
        model=DailyBetaFactorResult,
        item_fields=(
            ScreeningField('benchmark_id', DailyBetaFactorResult.benchmark_id),
            ScreeningField('beta_120d', DailyBetaFactorResult.beta_120d, _round_float),
            ScreeningField('low_beta_score', DailyBetaFactorResult.low_beta_score, _round_float),
            ScreeningField('low_beta_classification', DailyBetaFactorResult.low_beta_classification),
            ScreeningField('is_low_beta_eligible', DailyBetaFactorResult.is_low_beta_eligible),
            ScreeningField('history_days_available', DailyBetaFactorResult.history_days_available, _as_int),
            ScreeningField('captured_at', DailyBetaFactorResult.captured_at, _as_iso_datetime),
        ),
        order_by=(
            DailyBetaFactorResult.is_low_beta_eligible.desc(),
            DailyBetaFactorResult.low_beta_score.desc().nullslast(),
            DailyBetaFactorResult.beta_120d.asc().nullslast(),
            Symbol.symbol.asc(),
        ),
        summary_fields=(
            ScreeningMetric('entries', func.count(DailyBetaFactorResult.id)),
            ScreeningMetric('eligible_count', _count_when(DailyBetaFactorResult.is_low_beta_eligible.is_(True))),
            ScreeningMetric('watchlist_count', _count_when(DailyBetaFactorResult.low_beta_classification == 'watchlist')),
            ScreeningMetric('ineligible_count', _count_when(DailyBetaFactorResult.low_beta_classification == 'ineligible')),
            ScreeningMetric(
                'insufficient_history_count',
                _count_when(DailyBetaFactorResult.low_beta_classification == 'insufficient_history'),
            ),
        ),
    ),
    'advanced_value': ScreeningFamilyDefinition(
        family='advanced_value',
        feature_version=ADVANCED_VALUE_FACTOR_VERSION,
        detail_route='/eod/advanced-value-factors',
        model=DailyAdvancedValueFactorResult,
        item_fields=(
            ScreeningField('advanced_value_rank', DailyAdvancedValueFactorResult.advanced_value_rank, _as_int),
            ScreeningField('advanced_value_score', DailyAdvancedValueFactorResult.advanced_value_score, _round_float),
            ScreeningField(
                'advanced_value_percentile',
                DailyAdvancedValueFactorResult.advanced_value_percentile,
                _round_float,
            ),
            ScreeningField(
                'advanced_value_classification',
                DailyAdvancedValueFactorResult.advanced_value_classification,
            ),
            ScreeningField(
                'is_advanced_value_eligible',
                DailyAdvancedValueFactorResult.is_advanced_value_eligible,
            ),
            ScreeningField(
                'advanced_value_components_used',
                DailyAdvancedValueFactorResult.advanced_value_components_used,
                _as_int,
            ),
            ScreeningField('fcf_yield', DailyAdvancedValueFactorResult.fcf_yield, _round_float),
            ScreeningField('ev_to_ebitda', DailyAdvancedValueFactorResult.ev_to_ebitda, _round_float),
            ScreeningField('captured_at', DailyAdvancedValueFactorResult.captured_at, _as_iso_datetime),
        ),
        order_by=(
            DailyAdvancedValueFactorResult.advanced_value_rank.asc().nullslast(),
            DailyAdvancedValueFactorResult.advanced_value_score.desc().nullslast(),
            Symbol.symbol.asc(),
        ),
        summary_fields=(
            ScreeningMetric('entries', func.count(DailyAdvancedValueFactorResult.id)),
            ScreeningMetric(
                'eligible_count',
                _count_when(DailyAdvancedValueFactorResult.is_advanced_value_eligible.is_(True)),
            ),
            ScreeningMetric(
                'watchlist_count',
                _count_when(DailyAdvancedValueFactorResult.advanced_value_classification == 'watchlist'),
            ),
            ScreeningMetric(
                'ineligible_count',
                _count_when(DailyAdvancedValueFactorResult.advanced_value_classification == 'ineligible'),
            ),
            ScreeningMetric(
                'insufficient_components_count',
                _count_when(
                    DailyAdvancedValueFactorResult.advanced_value_classification == 'insufficient_components'
                ),
            ),
        ),
    ),
    'unified_value': ScreeningFamilyDefinition(
        family='unified_value',
        feature_version=UNIFIED_VALUE_FACTOR_VERSION,
        detail_route='/eod/unified-value-factors',
        model=DailyUnifiedValueFactorResult,
        item_fields=(
            ScreeningField('per', DailyUnifiedValueFactorResult.per, _round_float),
            ScreeningField('pbr', DailyUnifiedValueFactorResult.pbr, _round_float),
            ScreeningField('dividend_yield', DailyUnifiedValueFactorResult.dividend_yield, _round_float),
            ScreeningField('fcf_yield', DailyUnifiedValueFactorResult.fcf_yield, _round_float),
            ScreeningField('ev_to_ebitda', DailyUnifiedValueFactorResult.ev_to_ebitda, _round_float),
            ScreeningField('components_used', DailyUnifiedValueFactorResult.components_used, _as_int),
            ScreeningField('unified_value_rank', DailyUnifiedValueFactorResult.unified_value_rank, _as_int),
            ScreeningField('unified_value_score', DailyUnifiedValueFactorResult.unified_value_score, _round_float),
            ScreeningField(
                'unified_value_percentile',
                DailyUnifiedValueFactorResult.unified_value_percentile,
                _round_float,
            ),
            ScreeningField('unified_value_status', DailyUnifiedValueFactorResult.unified_value_status),
            ScreeningField('captured_at', DailyUnifiedValueFactorResult.captured_at, _as_iso_datetime),
        ),
        order_by=(
            DailyUnifiedValueFactorResult.unified_value_rank.asc().nullslast(),
            DailyUnifiedValueFactorResult.unified_value_score.desc().nullslast(),
            Symbol.symbol.asc(),
        ),
        summary_fields=(
            ScreeningMetric('entries', func.count(DailyUnifiedValueFactorResult.id)),
            ScreeningMetric(
                'scored_count',
                _count_when(DailyUnifiedValueFactorResult.unified_value_status == 'scored'),
            ),
            ScreeningMetric(
                'invalid_per_count',
                _count_when(DailyUnifiedValueFactorResult.unified_value_status == 'invalid_per'),
            ),
            ScreeningMetric(
                'missing_metric_count',
                _count_when(DailyUnifiedValueFactorResult.unified_value_status == 'missing_metric'),
            ),
            ScreeningMetric(
                'insufficient_components_count',
                _count_when(DailyUnifiedValueFactorResult.unified_value_status == 'insufficient_components'),
            ),
            ScreeningMetric(
                'insufficient_fundamentals_count',
                _count_when(DailyUnifiedValueFactorResult.unified_value_status == 'insufficient_fundamentals'),
            ),
            ScreeningMetric(
                'missing_market_date_bar_count',
                _count_when(DailyUnifiedValueFactorResult.unified_value_status == 'missing_market_date_bar'),
            ),
        ),
    ),
    'composite': ScreeningFamilyDefinition(
        family='composite',
        feature_version=COMPOSITE_FACTOR_VERSION,
        detail_route='/eod/composite-factors',
        model=DailyCompositeFactorResult,
        item_fields=(
            ScreeningField('composite_rank', DailyCompositeFactorResult.composite_rank, _as_int),
            ScreeningField('composite_score', DailyCompositeFactorResult.composite_score, _round_float),
            ScreeningField('composite_percentile', DailyCompositeFactorResult.composite_percentile, _round_float),
            ScreeningField('composite_classification', DailyCompositeFactorResult.composite_classification),
            ScreeningField('is_composite_eligible', DailyCompositeFactorResult.is_composite_eligible),
            ScreeningField('valid_factor_count', DailyCompositeFactorResult.valid_factor_count, _as_int),
            ScreeningField('available_base_weight', DailyCompositeFactorResult.available_base_weight, _round_float),
            ScreeningField('captured_at', DailyCompositeFactorResult.captured_at, _as_iso_datetime),
        ),
        order_by=(
            COMPOSITE_CLASSIFICATION_ORDER.asc(),
            DailyCompositeFactorResult.composite_rank.asc().nullslast(),
            Symbol.symbol.asc(),
        ),
        summary_fields=(
            ScreeningMetric('entries', func.count(DailyCompositeFactorResult.id)),
            ScreeningMetric('scored_count', _count_when(DailyCompositeFactorResult.composite_score.is_not(None))),
            ScreeningMetric('eligible_count', _count_when(DailyCompositeFactorResult.is_composite_eligible.is_(True))),
            ScreeningMetric(
                'watchlist_count',
                _count_when(DailyCompositeFactorResult.composite_classification == 'watchlist'),
            ),
            ScreeningMetric(
                'ineligible_count',
                _count_when(DailyCompositeFactorResult.composite_classification == 'ineligible'),
            ),
            ScreeningMetric(
                'hard_excluded_count',
                _count_when(DailyCompositeFactorResult.composite_classification == 'hard_excluded'),
            ),
            ScreeningMetric(
                'insufficient_inputs_count',
                _count_when(DailyCompositeFactorResult.composite_classification == 'insufficient_inputs'),
            ),
        ),
    ),
}


class ScreeningResultReadService:
    def read(
        self,
        *,
        family: str,
        market_date: str | date | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> dict[str, Any]:
        definition = SCREENING_FAMILY_DEFINITIONS.get(family)
        if definition is None:
            supported = ', '.join(sorted(SCREENING_FAMILY_DEFINITIONS))
            raise ValueError(f'family must be one of: {supported}')

        normalized_requested_market_date = _normalize_optional_market_date(market_date)
        requested_market_date = (
            normalized_requested_market_date.isoformat()
            if normalized_requested_market_date is not None
            else None
        )
        resolved_market_date = self._load_resolved_market_date(
            definition=definition,
            requested_market_date=normalized_requested_market_date,
        )
        is_latest_fallback = (
            requested_market_date is not None
            and resolved_market_date is not None
            and resolved_market_date != requested_market_date
        )
        summary = self._empty_summary(definition)
        items: list[dict[str, Any]] = []
        total_entries = 0
        if resolved_market_date is not None:
            summary = self._load_summary(definition=definition, market_date=date.fromisoformat(resolved_market_date))
            total_entries = int(summary.get('entries') or 0)
            items = self._load_items(
                definition=definition,
                market_date=date.fromisoformat(resolved_market_date),
                limit=limit,
                offset=offset,
            )
        pagination = {
            'limit': limit,
            'offset': offset,
            'returned': len(items),
            'total': total_entries,
            'has_more': offset + len(items) < total_entries,
        }
        if pagination['has_more']:
            pagination['next_offset'] = offset + len(items)
        return {
            'family': definition.family,
            'detail_route': definition.detail_route,
            'read_mode': 'screening_compact',
            'requested_market_date': requested_market_date,
            'resolved_market_date': resolved_market_date,
            'market_date': resolved_market_date,
            'latest_mode': normalized_requested_market_date is None,
            'is_latest_fallback': is_latest_fallback,
            'feature_version': definition.feature_version,
            'summary': summary,
            'pagination': pagination,
            'items': items,
        }

    @staticmethod
    def _load_resolved_market_date(
        *,
        definition: ScreeningFamilyDefinition,
        requested_market_date: date | None,
    ) -> str | None:
        with session_scope() as session:
            stmt = select(func.max(definition.model.market_date)).select_from(definition.model)
            if requested_market_date is not None:
                stmt = stmt.where(definition.model.market_date <= requested_market_date)
            resolved = session.execute(stmt).scalar_one()
        return resolved.isoformat() if isinstance(resolved, date) else None

    @staticmethod
    def _empty_summary(definition: ScreeningFamilyDefinition) -> dict[str, int]:
        return {field.key: 0 for field in definition.summary_fields}

    @staticmethod
    def _load_summary(
        *,
        definition: ScreeningFamilyDefinition,
        market_date: date,
    ) -> dict[str, int]:
        with session_scope() as session:
            stmt = (
                select(*[field.expression.label(field.key) for field in definition.summary_fields])
                .select_from(definition.model)
                .where(definition.model.market_date == market_date)
            )
            row = session.execute(stmt).mappings().one()
        return {field.key: int(row[field.key] or 0) for field in definition.summary_fields}

    @staticmethod
    def _load_items(
        *,
        definition: ScreeningFamilyDefinition,
        market_date: date,
        limit: int,
        offset: int,
    ) -> list[dict[str, Any]]:
        with session_scope() as session:
            stmt = (
                select(
                    Symbol.symbol.label('symbol'),
                    Symbol.name_ko.label('name_ko'),
                    Symbol.market.label('market'),
                    definition.model.market_date.label('market_date'),
                    definition.model.source.label('source'),
                    *[field.column.label(field.key) for field in definition.item_fields],
                )
                .join(Symbol, Symbol.id == definition.model.symbol_id)
                .where(definition.model.market_date == market_date)
                .order_by(*definition.order_by)
                .limit(limit)
                .offset(offset)
            )
            rows = session.execute(stmt).mappings().all()
        return [ScreeningResultReadService._build_item(definition=definition, row=row) for row in rows]

    @staticmethod
    def _build_item(
        *,
        definition: ScreeningFamilyDefinition,
        row: dict[str, Any],
    ) -> dict[str, Any]:
        item = {
            'symbol': row['symbol'],
            'name_ko': row['name_ko'],
            'market': row['market'],
            'market_date': row['market_date'].isoformat() if isinstance(row['market_date'], date) else None,
            'source': row['source'],
        }
        for field in definition.item_fields:
            item[field.key] = field.transform(row[field.key])
        return item


screening_result_read_service = ScreeningResultReadService()
