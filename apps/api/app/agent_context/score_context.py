from __future__ import annotations

from datetime import date
from decimal import Decimal
from typing import Any

from app.signals.momentum_factor import (
    MOMENTUM_ELIGIBILITY_STATUS_BELOW_TOP_PERCENTILE_CUTOFF,
    MOMENTUM_ELIGIBILITY_STATUS_ELIGIBLE,
    MOMENTUM_ELIGIBILITY_STATUS_INSUFFICIENT_HISTORY,
    MOMENTUM_ELIGIBILITY_STATUS_RECENTLY_OVERHEATED,
    MOMENTUM_ELIGIBILITY_STATUS_TREND_NOT_CONFIRMED,
    MOMENTUM_FACTOR_VERSION,
    MOMENTUM_HISTORY_WINDOW,
    enrich_momentum_audit_fields,
)
from app.signals.quality_factor import (
    QUALITY_COMPONENT_COUNT,
    QUALITY_FACTOR_VERSION,
    QUALITY_STATUS_INSUFFICIENT_COMPONENTS,
    QUALITY_STATUS_INSUFFICIENT_FUNDAMENTALS,
    QUALITY_STATUS_INVALID_ASSETS,
    QUALITY_STATUS_INVALID_EQUITY,
    QUALITY_STATUS_INVALID_NET_INCOME,
    QUALITY_STATUS_MISSING_MARKET_DATE_BAR,
    QUALITY_STATUS_MISSING_METRIC,
    QUALITY_STATUS_SCORED_COMPLETE,
    QUALITY_STATUS_SCORED_PARTIAL,
)
from app.signals.unified_value_factor import (
    UNIFIED_VALUE_FACTOR_VERSION,
    UNIFIED_VALUE_STATUS_INSUFFICIENT_COMPONENTS,
    UNIFIED_VALUE_STATUS_INSUFFICIENT_FUNDAMENTALS,
    UNIFIED_VALUE_STATUS_INVALID_PER,
    UNIFIED_VALUE_STATUS_MISSING_MARKET_DATE_BAR,
    UNIFIED_VALUE_STATUS_MISSING_METRIC,
    UNIFIED_VALUE_STATUS_SCORED,
)

SCORE_FAMILY_KEYS: tuple[str, ...] = ('unified_value', 'quality', 'momentum')

_UNIFIED_VALUE_COMPONENT_KEYS: tuple[str, ...] = (
    'per',
    'pbr',
    'dividend_yield',
    'fcf_yield',
    'ev_to_ebitda',
)
_QUALITY_COMPONENT_KEYS: tuple[str, ...] = (
    'roe',
    'roa',
    'gross_profit_to_assets',
    'operating_cash_flow_to_net_income',
    'debt_ratio',
    'accruals_ratio',
)
_MOMENTUM_COMPONENT_KEYS: tuple[str, ...] = (
    'momentum_12_1',
    'trend_confirmation',
    'recent_overheat_guard',
)


def _to_float(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, (int, float)):
        return float(value)
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _to_int(value: Any) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _normalize_market_date(value: str | date | None) -> str | None:
    if value is None:
        return None
    if isinstance(value, date):
        return value.isoformat()
    try:
        return date.fromisoformat(str(value)).isoformat()
    except ValueError:
        return str(value)


def _maybe_dict(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    return {}


def _string_list(value: Any) -> list[str]:
    if isinstance(value, (list, tuple)):
        result: list[str] = []
        for item in value:
            text = str(item or '').strip()
            if text:
                result.append(text)
        return result
    return []


def _append_note(notes: list[str], note: str | None) -> None:
    text = str(note or '').strip()
    if text and text not in notes:
        notes.append(text)


def _format_component_list(keys: list[str]) -> str:
    return ', '.join(keys)


def _build_unified_value_score_context(
    *,
    row: dict[str, Any] | None,
    market_date: str | date,
) -> dict[str, Any]:
    factor_payload = _maybe_dict(_maybe_dict(row).get('factor_payload'))
    availability = _maybe_dict(factor_payload.get('availability'))
    component_statuses = _maybe_dict(factor_payload.get('component_statuses'))
    methodology = _maybe_dict(factor_payload.get('methodology'))
    used_keys = _string_list(availability.get('valid_component_keys'))
    missing_keys = _string_list(availability.get('missing_component_keys'))
    components_used = _to_int(_maybe_dict(row).get('components_used')) or len(used_keys)
    status_code = str(_maybe_dict(row).get('unified_value_status') or '').strip()
    has_partial_inputs = bool(row) and (components_used > 0 or bool(component_statuses))
    availability_status = (
        'available'
        if _to_float(_maybe_dict(row).get('unified_value_score')) is not None
        else 'partial'
        if has_partial_inputs
        else 'unavailable'
    )

    coverage_notes: list[str] = []
    if row is None:
        _append_note(coverage_notes, 'No stored unified value factor row was available for this market date.')
    elif status_code == UNIFIED_VALUE_STATUS_MISSING_MARKET_DATE_BAR:
        _append_note(coverage_notes, 'No stored market-date bar was available for unified value scoring.')
    elif status_code == UNIFIED_VALUE_STATUS_INSUFFICIENT_FUNDAMENTALS:
        _append_note(coverage_notes, 'The required annual fundamentals snapshot was unavailable.')
    elif status_code == UNIFIED_VALUE_STATUS_INVALID_PER:
        _append_note(coverage_notes, 'PER was non-positive, so the unified value score was not computed.')
    elif status_code == UNIFIED_VALUE_STATUS_MISSING_METRIC:
        _append_note(coverage_notes, 'One or more required value inputs were missing.')
    elif status_code == UNIFIED_VALUE_STATUS_INSUFFICIENT_COMPONENTS:
        _append_note(coverage_notes, 'The unified value score requires all five components.')
    if missing_keys:
        _append_note(
            coverage_notes,
            f"Missing value components: {_format_component_list(missing_keys)}.",
        )

    return {
        'status': availability_status,
        'score': _to_float(_maybe_dict(row).get('unified_value_score')),
        'score_scale': '0_to_1_percentile_mean',
        'rank': _to_int(_maybe_dict(row).get('unified_value_rank')),
        'percentile': _to_float(_maybe_dict(row).get('unified_value_percentile')),
        'higher_is_better': True,
        'meaning': 'Composite value attractiveness score across stored cross-sectional value percentiles.',
        'method': (
            'Equal-weight mean of PER, PBR, dividend yield, FCF yield, and EV/EBITDA percentiles; '
            'requires all five components, with lower PER/PBR/EV-EBITDA and higher dividend yield/FCF yield ranking better.'
        ),
        'feature_version': str(
            _maybe_dict(row).get('feature_version')
            or _maybe_dict(row).get('source')
            or factor_payload.get('feature_version')
            or UNIFIED_VALUE_FACTOR_VERSION
        ),
        'source': _maybe_dict(row).get('source'),
        'components_used': components_used,
        'components_possible': len(_UNIFIED_VALUE_COMPONENT_KEYS),
        'component_keys_used': used_keys,
        'component_keys_possible': list(_UNIFIED_VALUE_COMPONENT_KEYS),
        'component_statuses': component_statuses,
        'coverage_notes': coverage_notes,
        'as_of_market_date': _normalize_market_date(_maybe_dict(row).get('market_date') or market_date),
        'methodology': {
            'score_method': methodology.get('score_method'),
            'score_requirement': methodology.get('score_requirement'),
            'ranking_directions': methodology.get('ranking_directions'),
        },
    }


def _build_quality_score_context(
    *,
    row: dict[str, Any] | None,
    market_date: str | date,
) -> dict[str, Any]:
    factor_payload = _maybe_dict(_maybe_dict(row).get('factor_payload'))
    availability = _maybe_dict(factor_payload.get('availability'))
    component_statuses = _maybe_dict(factor_payload.get('component_statuses'))
    methodology = _maybe_dict(factor_payload.get('methodology'))
    used_keys = _string_list(availability.get('valid_component_keys'))
    missing_keys = _string_list(availability.get('missing_component_keys'))
    components_used = _to_int(_maybe_dict(row).get('components_used')) or len(used_keys)
    components_possible = _to_int(_maybe_dict(row).get('components_possible')) or QUALITY_COMPONENT_COUNT
    quality_status = str(_maybe_dict(row).get('quality_status') or '').strip()
    quality_score = _to_float(_maybe_dict(row).get('quality_score'))
    availability_status = (
        'available'
        if quality_status == QUALITY_STATUS_SCORED_COMPLETE
        else 'partial'
        if quality_score is not None or components_used > 0 or bool(component_statuses)
        else 'unavailable'
    )

    coverage_notes: list[str] = []
    if row is None:
        _append_note(coverage_notes, 'No stored quality factor row was available for this market date.')
    elif quality_status == QUALITY_STATUS_SCORED_PARTIAL:
        _append_note(coverage_notes, 'The quality score was computed from the available components only.')
    elif quality_status == QUALITY_STATUS_MISSING_MARKET_DATE_BAR:
        _append_note(coverage_notes, 'No stored market-date bar was available for quality scoring.')
    elif quality_status == QUALITY_STATUS_INSUFFICIENT_FUNDAMENTALS:
        _append_note(coverage_notes, 'The required annual fundamentals snapshot was unavailable.')
    elif quality_status == QUALITY_STATUS_INVALID_ASSETS:
        _append_note(coverage_notes, 'One or more quality formulas required positive total assets.')
    elif quality_status == QUALITY_STATUS_INVALID_EQUITY:
        _append_note(coverage_notes, 'One or more quality formulas required positive total equity.')
    elif quality_status == QUALITY_STATUS_INVALID_NET_INCOME:
        _append_note(
            coverage_notes,
            'The operating-cash-flow-versus-net-income component required positive net income.',
        )
    elif quality_status == QUALITY_STATUS_MISSING_METRIC:
        _append_note(coverage_notes, 'One or more required quality inputs were missing.')
    elif quality_status == QUALITY_STATUS_INSUFFICIENT_COMPONENTS:
        _append_note(coverage_notes, 'No quality subcomponent could be scored from the stored inputs.')
    if missing_keys:
        _append_note(
            coverage_notes,
            f"Missing quality components: {_format_component_list(missing_keys)}.",
        )

    return {
        'status': availability_status,
        'score': quality_score,
        'score_scale': '0_to_1_percentile_mean',
        'rank': _to_int(_maybe_dict(row).get('quality_rank')),
        'percentile': _to_float(_maybe_dict(row).get('quality_percentile')),
        'higher_is_better': True,
        'meaning': 'Business quality score across profitability, cash-flow quality, and balance-sheet quality inputs.',
        'method': (
            'Equal-weight mean of the available ROE, ROA, gross-profit-to-assets, operating-cash-flow-to-net-income, '
            'debt-ratio, and accruals-ratio percentiles; a complete score uses all six components.'
        ),
        'feature_version': str(
            _maybe_dict(row).get('feature_version')
            or _maybe_dict(row).get('source')
            or factor_payload.get('feature_version')
            or QUALITY_FACTOR_VERSION
        ),
        'source': _maybe_dict(row).get('source'),
        'components_used': components_used,
        'components_possible': components_possible,
        'component_keys_used': used_keys,
        'component_keys_possible': list(_QUALITY_COMPONENT_KEYS),
        'component_statuses': component_statuses,
        'coverage_notes': coverage_notes,
        'as_of_market_date': _normalize_market_date(_maybe_dict(row).get('market_date') or market_date),
        'methodology': {
            'score_method': methodology.get('score_method'),
            'score_requirement': methodology.get('score_requirement'),
            'complete_requirement': methodology.get('complete_requirement'),
            'ranking_directions': methodology.get('ranking_directions'),
        },
    }


def _build_momentum_score_context(
    *,
    row: dict[str, Any] | None,
    market_date: str | date,
) -> dict[str, Any]:
    enriched_row = enrich_momentum_audit_fields(dict(row)) if row is not None else {}
    factor_payload = _maybe_dict(enriched_row.get('factor_payload'))
    ranking = _maybe_dict(factor_payload.get('ranking'))
    eligibility_rule = _maybe_dict(factor_payload.get('eligibility_rule'))
    overheat_rule = _maybe_dict(factor_payload.get('overheat_rule'))
    momentum_percentile = _to_float(enriched_row.get('momentum_percentile'))

    used_keys: list[str] = []
    if _to_float(enriched_row.get('momentum_12_1')) is not None:
        used_keys.append('momentum_12_1')
    if _to_float(enriched_row.get('ma_60')) is not None and _to_float(enriched_row.get('ma_120')) is not None:
        used_keys.append('trend_confirmation')
    if (
        _to_float(enriched_row.get('return_5d')) is not None
        and _to_float(enriched_row.get('return_5d_vol_adjusted')) is not None
        and enriched_row.get('has_sufficient_cross_sectional_context') is True
    ):
        used_keys.append('recent_overheat_guard')

    missing_keys = [key for key in _MOMENTUM_COMPONENT_KEYS if key not in set(used_keys)]
    eligibility_status = str(enriched_row.get('momentum_eligibility_status') or '').strip()
    availability_status = (
        'available'
        if momentum_percentile is not None
        else 'partial'
        if row is not None
        else 'unavailable'
    )

    coverage_notes: list[str] = []
    if row is None:
        _append_note(coverage_notes, 'No stored momentum factor row was available for this market date.')
    elif eligibility_status == MOMENTUM_ELIGIBILITY_STATUS_INSUFFICIENT_HISTORY:
        _append_note(
            coverage_notes,
            f'At least {MOMENTUM_HISTORY_WINDOW} stored trading bars were required to compute 12-1 momentum.',
        )
    elif eligibility_status == MOMENTUM_ELIGIBILITY_STATUS_TREND_NOT_CONFIRMED:
        _append_note(
            coverage_notes,
            'Trend confirmation failed because price was not above MA120 and/or MA60 was not above MA120.',
        )
    elif eligibility_status == MOMENTUM_ELIGIBILITY_STATUS_BELOW_TOP_PERCENTILE_CUTOFF:
        _append_note(coverage_notes, 'Momentum percentile did not clear the configured top-percentile gate.')
    elif eligibility_status == MOMENTUM_ELIGIBILITY_STATUS_RECENTLY_OVERHEATED:
        _append_note(coverage_notes, 'The recent-overheat guard blocked momentum eligibility.')
    if enriched_row.get('has_sufficient_cross_sectional_context') is False:
        _append_note(coverage_notes, 'Cross-sectional context was too small to fully evaluate the overheat rule.')
    if missing_keys:
        _append_note(
            coverage_notes,
            f"Missing momentum components: {_format_component_list(missing_keys)}.",
        )

    return {
        'status': availability_status,
        'score': momentum_percentile,
        'score_scale': '0_to_1_percentile',
        'rank': _to_int(enriched_row.get('momentum_rank')),
        'percentile': momentum_percentile,
        'higher_is_better': True,
        'meaning': 'Medium-term price-strength score plus trend and overheat gating context from stored daily bars.',
        'method': (
            'Ranks 12-1 momentum from stored daily closes, then records trend confirmation, a top-percentile eligibility gate, '
            'and the configured recent-overheat guard.'
        ),
        'feature_version': str(
            enriched_row.get('feature_version')
            or enriched_row.get('source')
            or factor_payload.get('feature_version')
            or MOMENTUM_FACTOR_VERSION
        ),
        'source': enriched_row.get('source'),
        'components_used': len(used_keys),
        'components_possible': len(_MOMENTUM_COMPONENT_KEYS),
        'component_keys_used': used_keys,
        'component_keys_possible': list(_MOMENTUM_COMPONENT_KEYS),
        'component_statuses': {
            'momentum_12_1': 'available' if _to_float(enriched_row.get('momentum_12_1')) is not None else 'missing',
            'trend_confirmation': (
                'confirmed'
                if eligibility_status == MOMENTUM_ELIGIBILITY_STATUS_ELIGIBLE
                or enriched_row.get('passes_trend_confirmation') is True
                else 'not_confirmed'
                if _to_float(enriched_row.get('ma_120')) is not None
                else 'missing'
            ),
            'recent_overheat_guard': (
                'blocked'
                if eligibility_status == MOMENTUM_ELIGIBILITY_STATUS_RECENTLY_OVERHEATED
                else 'passed'
                if enriched_row.get('has_sufficient_cross_sectional_context') is True
                else 'missing'
            ),
        },
        'coverage_notes': coverage_notes,
        'as_of_market_date': _normalize_market_date(enriched_row.get('market_date') or market_date),
        'methodology': {
            'eligibility_rule_version': eligibility_rule.get('rule_version'),
            'percentile_gate': eligibility_rule.get('percentile_gate'),
            'overheat_rule_version': overheat_rule.get('rule_version'),
        },
        'raw_score_inputs': {
            'momentum_12_1': _to_float(enriched_row.get('momentum_12_1')),
            'return_5d': _to_float(enriched_row.get('return_5d')),
            'return_5d_vol_adjusted': _to_float(enriched_row.get('return_5d_vol_adjusted')),
            'momentum_eligibility_status': eligibility_status or MOMENTUM_ELIGIBILITY_STATUS_ELIGIBLE,
            'is_momentum_eligible': bool(enriched_row.get('is_momentum_eligible')) if row is not None else None,
            'passes_momentum_top_percentile': enriched_row.get('passes_momentum_top_percentile'),
            'momentum_percentile_threshold': _to_float(
                enriched_row.get('momentum_percentile_threshold')
                or ranking.get('momentum_percentile_threshold')
            ),
            'is_recent_overheated': enriched_row.get('is_recent_overheated'),
        },
    }


def build_signal_decider_scores(
    *,
    market_date: str | date,
    unified_value_row: dict[str, Any] | None = None,
    quality_row: dict[str, Any] | None = None,
    momentum_row: dict[str, Any] | None = None,
) -> dict[str, dict[str, Any]]:
    return {
        'unified_value': _build_unified_value_score_context(
            row=unified_value_row,
            market_date=market_date,
        ),
        'quality': _build_quality_score_context(
            row=quality_row,
            market_date=market_date,
        ),
        'momentum': _build_momentum_score_context(
            row=momentum_row,
            market_date=market_date,
        ),
    }
