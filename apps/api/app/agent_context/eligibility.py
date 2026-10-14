from __future__ import annotations

from collections import defaultdict
from datetime import date, datetime, timezone
from math import ceil
from typing import Any, Sequence

from sqlalchemy import select

from app.agent_context.roles import DECISION_SUPPORT_AGENT_NAMES, SUMMARIZER_AGENT_NAMES
from app.agent_context.run_modes import (
    FEEDBACK_SCOPE_MODE_BROAD_FEEDBACK_UNIVERSE,
    FEEDBACK_SCOPE_MODE_DECIDER_SCOPE_FAST_PATH,
    normalize_feedback_scope_mode,
)
from app.agent_context.signal_decider_snapshot_validity import (
    is_decision_snapshot_valid_for_source_date,
)
from app.core.settings import settings
from app.db import session_scope
from app.db_models import (
    AgentContextFeedback,
    BrokerOrder,
    DartDisclosureEvent,
    OrderRequest,
    SignalDeciderDecisionSnapshot,
    SignalDeciderEvaluation,
    SignalDeciderExecutionPlanItem,
)
from app.market_data_store import (
    load_agent_feedback_candidate_score_rows,
    load_daily_market_risk_filter_results,
    load_daily_piotroski_filter_results,
)
from app.orders.domain import BrokerOrderStatus, OrderRequestSubmissionStatus

AGENT_FEEDBACK_ELIGIBILITY_FILTERS: tuple[str, ...] = (
    'market_risk_filter',
    'piotroski_filter',
)
AGENT_FEEDBACK_ELIGIBILITY_RULE = 'strategy_aware_prefilter_scoring_v1'
AGENT_FEEDBACK_CANDIDATE_CAP_RULE = 'strategy_aware_prefilter_broad_top_percentile_cap_v3'
AGENT_FEEDBACK_CANDIDATE_SCORE_VERSION = 'decider_role_gated_candidate_score_v3'
AGENT_FEEDBACK_CANDIDATE_TIE_BREAK_VERSION = (
    'score_then_evidence_completeness_freshness_risk_v2'
)
AGENT_FEEDBACK_ROLE_CAP_RULE = 'agent_feedback_role_scope_cap_v1'
AGENT_FEEDBACK_FUNNEL_SCHEMA_VERSION = 'agent_feedback_funnel_v1'
AGENT_FEEDBACK_VALUE_LONG_ONLY_ELIGIBILITY_RULE = 'market_risk_filter_passed_and_piotroski_passed_v1'
AGENT_FEEDBACK_STRATEGY_SCOPE_SHORT_TERM_MOMENTUM = 'short_term_momentum_breakout'
AGENT_FEEDBACK_STRATEGY_SCOPE_VALUE_LONG_ONLY = 'value_long_only'
AGENT_FEEDBACK_DEFAULT_STRATEGY_SCOPE = AGENT_FEEDBACK_STRATEGY_SCOPE_SHORT_TERM_MOMENTUM
AGENT_FEEDBACK_SCOPE_ALL = 'active_strategy_aware_prefilter_scoring'
AGENT_FEEDBACK_SCOPE_REQUESTED = 'requested_symbols_scored_by_strategy_aware_prefilter'
AGENT_FEEDBACK_ROLE_CAP_BYPASS_REASON_REQUESTED = 'requested_symbol_scope_cap_bypass'

_BROAD_CAP_DEFAULTS = {
    'percentile': 0.10,
    'min_symbols': 150,
    'max_symbols': 200,
}
_LEGACY_BROAD_CAP_DEFAULTS = {
    'percentile': 0.10,
    'min_symbols': 150,
    'max_symbols': 300,
}
_ROLE_CAP_SETTING_BY_ROLE = {
    'eod-summarizer': 'openclaw_feedback_role_cap_eod_summarizer',
    'technical-summarizer': 'openclaw_feedback_role_cap_technical_summarizer',
    'fundamental-summarizer': 'openclaw_feedback_role_cap_fundamental_summarizer',
    'short-term-decider': 'openclaw_feedback_role_cap_short_term_decider',
    'signal-decider': 'openclaw_feedback_role_cap_signal_decider',
}
_ROLE_CAP_DEFAULT_BY_ROLE = {
    'eod-summarizer': 200,
    'technical-summarizer': 150,
    'fundamental-summarizer': 120,
    'short-term-decider': 80,
    'signal-decider': 80,
}
_POSITIVE_DECIDER_ACTIONS = {
    'buy',
    'watch',
    'watch_only',
    'watch_for_trigger',
    'scalp_buy_candidate',
    'advance_to_decider',
}
_PRIOR_CONTEXT_SYMBOL_CHUNK_SIZE = 50


def _chunk_symbols(symbols: Sequence[str], size: int = _PRIOR_CONTEXT_SYMBOL_CHUNK_SIZE) -> list[list[str]]:
    normalized = [str(symbol).strip().upper() for symbol in symbols if str(symbol or '').strip()]
    return [normalized[index:index + size] for index in range(0, len(normalized), size)]
_ACTIVE_ORDER_REQUEST_STATUSES = {
    OrderRequestSubmissionStatus.PENDING_SUBMIT.value,
    OrderRequestSubmissionStatus.SUBMITTING.value,
    OrderRequestSubmissionStatus.SUBMITTED.value,
}
_ACTIVE_BROKER_ORDER_STATUSES = {
    BrokerOrderStatus.PENDING_SUBMIT.value,
    BrokerOrderStatus.SUBMITTED.value,
    BrokerOrderStatus.ACCEPTED.value,
    BrokerOrderStatus.CANCEL_PENDING.value,
    BrokerOrderStatus.REPLACE_PENDING.value,
    BrokerOrderStatus.PARTIAL_FILL.value,
    BrokerOrderStatus.RECONCILE_ONLY.value,
}
_ACTIVE_EXEC_WATCH_STATUSES = {
    'planned',
    'awaiting_trigger',
    'dispatch_pending_remote_ack',
    'dry_run',
    'submitted',
    'delegated_for_exec_agent_sizing',
}
_ACTIVE_EXEC_WATCH_ELIGIBILITY_STATUSES = {
    'eligible',
    'monitor_candidate',
}


def _normalize_symbol(value: Any) -> str:
    return str(value or '').strip().upper()


def _normalize_symbol_scope(symbols: Sequence[str] | None) -> list[str] | None:
    if symbols is None:
        return None
    normalized: list[str] = []
    seen: set[str] = set()
    for symbol in symbols:
        value = _normalize_symbol(symbol)
        if not value or value in seen:
            continue
        seen.add(value)
        normalized.append(value)
    return normalized


def _rows_by_symbol(rows: Sequence[dict]) -> dict[str, dict]:
    mapped: dict[str, dict] = {}
    for row in rows:
        symbol = _normalize_symbol(row.get('symbol'))
        if symbol and symbol not in mapped:
            mapped[symbol] = dict(row)
    return mapped


def _as_dict(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, dict) else {}


def _as_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _as_int(value: Any, default: int = 0) -> int:
    try:
        if value is None:
            return default
        return int(value)
    except (TypeError, ValueError):
        return default


def _evidence_timestamp_sort_value(value: Any) -> float:
    if value is None:
        return float('-inf')
    try:
        parsed = (
            value
            if isinstance(value, datetime)
            else datetime.fromisoformat(str(value).strip().replace('Z', '+00:00'))
        )
    except (TypeError, ValueError):
        return float('-inf')
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc).timestamp()


def _normalized_risk_tier(value: Any) -> str:
    normalized = str(value or '').strip().lower()
    return normalized if normalized in {'low', 'medium', 'high'} else 'unknown'


def _risk_tier_sort_priority(value: Any) -> int:
    return {
        'low': 0,
        'medium': 1,
        'high': 2,
        'unknown': 3,
    }[_normalized_risk_tier(value)]


def _normalize_action(value: Any) -> str:
    return str(value or '').strip().lower().replace('-', '_')


def _setting_with_legacy(
    *,
    current_name: str,
    legacy_name: str,
    current_default: Any,
    legacy_default: Any,
) -> Any:
    value = getattr(settings, current_name, current_default)
    legacy_value = getattr(settings, legacy_name, legacy_default)
    if value == current_default and legacy_value != legacy_default:
        return legacy_value
    return value


def _broad_cap_settings() -> dict[str, int | float]:
    return {
        'percentile': float(
            _setting_with_legacy(
                current_name='openclaw_feedback_broad_candidate_cap_percentile',
                legacy_name='openclaw_feedback_candidate_cap_percentile',
                current_default=_BROAD_CAP_DEFAULTS['percentile'],
                legacy_default=_LEGACY_BROAD_CAP_DEFAULTS['percentile'],
            )
        ),
        'min_symbols': int(
            _setting_with_legacy(
                current_name='openclaw_feedback_broad_candidate_cap_min_symbols',
                legacy_name='openclaw_feedback_candidate_cap_min_symbols',
                current_default=_BROAD_CAP_DEFAULTS['min_symbols'],
                legacy_default=_LEGACY_BROAD_CAP_DEFAULTS['min_symbols'],
            )
        ),
        'max_symbols': int(
            _setting_with_legacy(
                current_name='openclaw_feedback_broad_candidate_cap_max_symbols',
                legacy_name='openclaw_feedback_candidate_cap_max_symbols',
                current_default=_BROAD_CAP_DEFAULTS['max_symbols'],
                legacy_default=_LEGACY_BROAD_CAP_DEFAULTS['max_symbols'],
            )
        ),
    }


def _role_cap_limit(role: str, total: int) -> int:
    setting_name = _ROLE_CAP_SETTING_BY_ROLE.get(role)
    configured = getattr(
        settings,
        setting_name,
        _ROLE_CAP_DEFAULT_BY_ROLE.get(role, total),
    ) if setting_name else total
    cap = max(0, _as_int(configured, default=0))
    if total <= 0 or cap <= 0:
        return total
    return min(total, cap)


def _requested_scope_bypasses_caps(requested_symbols: Sequence[str] | None) -> bool:
    if requested_symbols is None:
        return False
    bypass_limit = max(
        0,
        _as_int(
            getattr(settings, 'openclaw_feedback_requested_scope_cap_bypass_max_symbols', 50),
            default=50,
        ),
    )
    return len(_normalize_symbol_scope(requested_symbols) or []) <= bypass_limit


def _append_forced_reason(
    reasons_by_symbol: dict[str, list[dict[str, Any]]],
    *,
    symbol: str,
    code: str,
    source: str,
    message: str,
    details: dict[str, Any] | None = None,
) -> None:
    normalized_symbol = _normalize_symbol(symbol)
    if not normalized_symbol:
        return
    reason: dict[str, Any] = {
        'code': code,
        'source': source,
        'message': message,
    }
    if details:
        reason['details'] = details
    existing_codes = {
        str(item.get('code') or '')
        for item in reasons_by_symbol.get(normalized_symbol, [])
        if isinstance(item, dict)
    }
    if code in existing_codes:
        return
    reasons_by_symbol.setdefault(normalized_symbol, []).append(reason)


def _reason(
    *,
    code: str,
    source: str,
    message: str,
    details: dict[str, Any] | None = None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        'code': code,
        'source': source,
        'severity': 'hard_block',
        'message': message,
    }
    if details:
        payload['details'] = details
    return payload


def _penalty(
    *,
    code: str,
    source: str,
    message: str,
    details: dict[str, Any] | None = None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        'code': code,
        'source': source,
        'severity': 'soft_penalty',
        'message': message,
    }
    if details:
        payload['details'] = details
    return payload


def _market_risk_filter_signals(row: dict | None) -> dict[str, Any]:
    if row is None:
        return {'available': False}
    filter_payload = _as_dict(row.get('filter_payload'))
    return {
        'available': True,
        'source': row.get('source'),
        'feature_version': row.get('feature_version') or row.get('source'),
        'market_date': row.get('market_date'),
        'risk_state': row.get('risk_state'),
        'passed_filter': row.get('passed_filter'),
        'watchlist_flagged': row.get('watchlist_flagged'),
        'blocking_reasons': list(filter_payload.get('blocking_reasons') or []),
        'watchlist_reasons': list(filter_payload.get('watchlist_reasons') or []),
        'captured_at': row.get('captured_at'),
    }


def _piotroski_filter_signals(row: dict | None) -> dict[str, Any]:
    if row is None:
        return {'available': False}
    filter_payload = _as_dict(row.get('filter_payload'))
    score_summary = _as_dict(filter_payload.get('score_summary'))
    thresholds = _as_dict(filter_payload.get('thresholds'))
    valuation = _as_dict(filter_payload.get('valuation'))
    provenance = _as_dict(filter_payload.get('provenance'))
    evidence_status = _as_dict(filter_payload.get('evidence_status'))
    required_signal_count = _as_int(
        score_summary.get('required_financial_signal_count'),
        default=0,
    )
    available_required_signals = _as_int(
        score_summary.get('required_financial_signals_available'),
        default=0,
    )
    factor_coverage_status = evidence_status.get('financial_signal_coverage')
    if not factor_coverage_status and required_signal_count > 0:
        factor_coverage_status = (
            'complete'
            if available_required_signals >= required_signal_count
            else 'unavailable'
        )
    audit_evidence_status = provenance.get('audit_evidence_status')
    profitability_evidence_status = evidence_status.get('profitability')
    capital_evidence_status = evidence_status.get('capital')
    return {
        'available': True,
        'source': row.get('source'),
        'feature_version': row.get('feature_version') or row.get('source'),
        'market_date': row.get('market_date'),
        'passed_filter': row.get('passed_filter'),
        'pbr': row.get('pbr'),
        'pbr_percentile_rank': row.get('pbr_percentile_rank'),
        'is_low_pbr': row.get('is_low_pbr'),
        'low_pbr_percentile_threshold': thresholds.get('low_pbr_percentile'),
        'has_profitability_issue': (
            None
            if profitability_evidence_status == 'unavailable'
            else row.get('has_profitability_issue')
        ),
        'profitability_evidence_status': profitability_evidence_status,
        'has_capital_impairment': (
            None
            if capital_evidence_status == 'unavailable'
            else row.get('has_capital_impairment')
        ),
        'capital_evidence_status': capital_evidence_status,
        'has_audit_issue': (
            None
            if audit_evidence_status == 'unknown'
            else row.get('has_audit_issue')
        ),
        'audit_evidence_status': audit_evidence_status,
        'piotroski_score': row.get('piotroski_score'),
        'passes_piotroski_score': row.get('passes_piotroski_score'),
        'piotroski_min_score': thresholds.get('piotroski_min_score'),
        'score_ratio': score_summary.get('score_ratio'),
        'required_financial_signal_count': required_signal_count or None,
        'required_financial_signals_available': (
            available_required_signals
            if required_signal_count > 0
            else None
        ),
        'factor_coverage_status': factor_coverage_status or 'unknown',
        'momentum_percentile_rank': row.get('momentum_percentile_rank'),
        'is_bottom_momentum': row.get('is_bottom_momentum'),
        'valuation_available': valuation.get('available'),
        'valuation_source_status': valuation.get('source_status'),
        'captured_at': row.get('captured_at'),
    }


def _market_risk_hard_blocks(row: dict | None) -> list[dict[str, Any]]:
    if row is None:
        return [
            _reason(
                code='missing_market_risk_filter',
                source='market_risk_filter',
                message='Market-risk filter row is missing, so the symbol cannot be dispatched safely.',
            )
        ]
    if bool(row.get('passed_filter')):
        return []
    filter_payload = _as_dict(row.get('filter_payload'))
    blocking_reasons = list(filter_payload.get('blocking_reasons') or [])
    if not blocking_reasons:
        blocking_reasons = ['market_risk_filter_failed']
    return [
        _reason(
            code=f'market_risk_{reason}',
            source='market_risk_filter',
            message=f'Market-risk filter hard blocker: {reason}.',
            details={
                'risk_state': row.get('risk_state'),
                'passed_filter': row.get('passed_filter'),
            },
        )
        for reason in blocking_reasons
    ]


def _market_risk_soft_penalties(row: dict | None) -> list[dict[str, Any]]:
    if row is None or not bool(row.get('passed_filter')) or not bool(row.get('watchlist_flagged')):
        return []
    filter_payload = _as_dict(row.get('filter_payload'))
    watchlist_reasons = list(filter_payload.get('watchlist_reasons') or [])
    if not watchlist_reasons:
        watchlist_reasons = ['market_risk_watchlist']
    return [
        _penalty(
            code=f'market_risk_watchlist_{reason}',
            source='market_risk_filter',
            message=f'Market-risk watchlist signal carried as a soft penalty: {reason}.',
            details={
                'risk_state': row.get('risk_state'),
                'watchlist_flagged': row.get('watchlist_flagged'),
            },
        )
        for reason in watchlist_reasons
    ]


def _piotroski_hard_blocks_for_value_scope(row: dict | None) -> list[dict[str, Any]]:
    if row is None:
        return [
            _reason(
                code='missing_piotroski_filter_for_value_scope',
                source='piotroski_filter',
                message='Piotroski/value filter row is missing for the strict value/long-only scope.',
            )
        ]
    if bool(row.get('passed_filter')):
        return []
    return [
        _reason(
            code='piotroski_filter_failed_for_value_scope',
            source='piotroski_filter',
            message='Strict value/long-only scope preserves the legacy Piotroski/value hard gate.',
            details=_piotroski_filter_signals(row),
        )
    ]


def _piotroski_soft_penalties(row: dict | None) -> list[dict[str, Any]]:
    if row is None:
        return [
            _penalty(
                code='missing_piotroski_filter',
                source='piotroski_filter',
                message='Piotroski/value filter row is missing; dispatch can continue with a quality-data penalty.',
            )
        ]
    if bool(row.get('passed_filter')):
        return []

    signals = _piotroski_filter_signals(row)
    penalties: list[dict[str, Any]] = []
    filter_payload = _as_dict(row.get('filter_payload'))
    thresholds = _as_dict(filter_payload.get('thresholds'))
    provenance = _as_dict(filter_payload.get('provenance'))
    audit_evidence_status = provenance.get('audit_evidence_status')
    factor_coverage_unavailable = (
        signals.get('factor_coverage_status') == 'unavailable'
    )
    valuation_unavailable = signals.get('valuation_available') is False
    if factor_coverage_unavailable:
        penalties.append(
            _penalty(
                code='piotroski_factor_coverage_unavailable',
                source='piotroski_filter',
                message=(
                    'Required Piotroski financial inputs are unavailable; '
                    'this is data-quality uncertainty, not a failed factor.'
                ),
                details={
                    'required_financial_signal_count': signals.get(
                        'required_financial_signal_count'
                    ),
                    'required_financial_signals_available': signals.get(
                        'required_financial_signals_available'
                    ),
                },
            )
        )
    if audit_evidence_status == 'unknown':
        penalties.append(
            _penalty(
                code='audit_evidence_unknown',
                source='piotroski_filter',
                message='Audit evidence is unavailable; this is treated as data-quality uncertainty, not a proven issue.',
            )
        )
    if valuation_unavailable:
        penalties.append(
            _penalty(
                code='piotroski_valuation_unavailable',
                source='piotroski_filter',
                message=(
                    'PBR valuation evidence is unavailable; this is '
                    'data-quality uncertainty, not a failed value signal.'
                ),
            )
        )
    if not (
        factor_coverage_unavailable
        or audit_evidence_status == 'unknown'
        or valuation_unavailable
    ):
        penalties.append(
            _penalty(
                code='piotroski_filter_failed',
                source='piotroski_filter',
                message='Piotroski/value filter did not pass; short-term momentum dispatch keeps the symbol with this penalty.',
                details=signals,
            )
        )
    if not valuation_unavailable and row.get('is_low_pbr') is False:
        penalties.append(
            _penalty(
                code='not_low_pbr',
                source='piotroski_filter',
                message='Symbol is not in the low-PBR value bucket.',
                details={
                    'pbr': row.get('pbr'),
                    'pbr_percentile_rank': row.get('pbr_percentile_rank'),
                    'low_pbr_percentile_threshold': thresholds.get('low_pbr_percentile'),
                },
            )
        )
    if (
        not factor_coverage_unavailable
        and row.get('passes_piotroski_score') is False
    ):
        penalties.append(
            _penalty(
                code='low_piotroski_score',
                source='piotroski_filter',
                message='Piotroski score does not meet the value filter threshold.',
                details={
                    'piotroski_score': row.get('piotroski_score'),
                    'piotroski_min_score': thresholds.get('piotroski_min_score'),
                },
            )
        )
    for field, message in (
        ('has_profitability_issue', 'Profitability issue flagged by the Piotroski/value filter.'),
        ('has_capital_impairment', 'Capital impairment risk flagged by the Piotroski/value filter.'),
        ('has_audit_issue', 'Audit issue flagged by the Piotroski/value filter.'),
        ('is_bottom_momentum', 'Bottom-momentum signal flagged by the Piotroski/value filter.'),
    ):
        if field == 'has_audit_issue' and audit_evidence_status == 'unknown':
            continue
        if signals.get(field) is True:
            penalties.append(
                _penalty(
                    code=field,
                    source='piotroski_filter',
                    message=message,
                )
            )
    return penalties


def _record_order(
    *,
    requested_symbols: Sequence[str] | None,
    market_risk_rows: Sequence[dict],
    piotroski_rows: Sequence[dict],
) -> list[str]:
    if requested_symbols is not None:
        return list(requested_symbols)
    ordered: list[str] = []
    seen: set[str] = set()
    for row in [*market_risk_rows, *piotroski_rows]:
        symbol = _normalize_symbol(row.get('symbol'))
        if symbol and symbol not in seen:
            seen.add(symbol)
            ordered.append(symbol)
    return ordered


def _score_rows_by_symbol(rows: Sequence[dict]) -> dict[str, dict]:
    mapped: dict[str, dict] = {}
    for row in rows:
        symbol = _normalize_symbol(row.get('symbol'))
        if symbol:
            mapped[symbol] = dict(row)
    return mapped


def _rank_bonus(rank: Any, *, max_rank: int = 30, weight: float = 2.0) -> float:
    normalized_rank = _as_int(rank, default=max_rank + 1)
    if normalized_rank <= 0 or normalized_rank > max_rank:
        return 0.0
    return float(max_rank - normalized_rank + 1) * weight


def _soft_penalty_score(soft_penalties: Sequence[dict[str, Any]]) -> float:
    score = 0.0
    uncertainty_penalized = False
    uncertainty_codes = {
        'missing_piotroski_filter',
        'audit_evidence_unknown',
        'piotroski_factor_coverage_unavailable',
        'piotroski_valuation_unavailable',
    }
    penalty_codes = {
        str(penalty.get('code') or '') for penalty in soft_penalties
    }
    proven_piotroski_codes = {
        'not_low_pbr',
        'low_piotroski_score',
        'has_profitability_issue',
        'has_capital_impairment',
        'has_audit_issue',
        'is_bottom_momentum',
    }
    has_specific_proven_piotroski_penalty = bool(
        penalty_codes.intersection(proven_piotroski_codes)
    )
    for penalty in soft_penalties:
        code = str(penalty.get('code') or '')
        if code in uncertainty_codes:
            if not uncertainty_penalized:
                score -= 12.0
                uncertainty_penalized = True
            continue
        if (
            code == 'piotroski_filter_failed'
            and has_specific_proven_piotroski_penalty
        ):
            continue
        if code.startswith('market_risk_watchlist'):
            score -= 10.0
        elif code in {'has_audit_issue', 'has_capital_impairment'}:
            score -= 20.0
        elif code in {'piotroski_filter_failed', 'low_piotroski_score'}:
            score -= 12.0
        else:
            score -= 6.0
    return score


def _piotroski_evidence_known_count(record: dict[str, Any]) -> int:
    signals = _as_dict(
        _as_dict(record.get('filter_signals')).get('piotroski_filter')
    )
    if signals.get('available') is not True:
        return 0

    count = 0
    if signals.get('valuation_available') is True:
        count += 1
    if signals.get('factor_coverage_status') == 'complete':
        count += 1
    for status_key, issue_key, unavailable_status in (
        ('profitability_evidence_status', 'has_profitability_issue', 'unavailable'),
        ('capital_evidence_status', 'has_capital_impairment', 'unavailable'),
        ('audit_evidence_status', 'has_audit_issue', 'unknown'),
    ):
        status = signals.get(status_key)
        if status in {'clean', 'issue'}:
            count += 1
        elif status != unavailable_status and isinstance(
            signals.get(issue_key), bool
        ):
            # Legacy v1 rows predate nested evidence_status. Their existing
            # top-level booleans remain the compatibility source of truth.
            count += 1
    return count


def _candidate_score_payload(
    *,
    record: dict[str, Any],
    score_row: dict | None,
) -> dict[str, Any]:
    row = score_row or {}
    signal_score = _as_float(row.get('signal_score'))
    direct_signal_score = (
        _as_float(row.get('signal_count')) * 25.0
        + _rank_bonus(row.get('volume_power_rank'), weight=2.0)
        + _rank_bonus(row.get('overtime_volume_rank'), weight=1.5)
        + _rank_bonus(row.get('overtime_fluctuation_rank'), weight=1.5)
    )
    momentum_score = _as_float(row.get('momentum_percentile')) * 80.0
    composite_score = _as_float(row.get('composite_percentile')) * 60.0
    momentum_eligible = (
        row.get('is_momentum_eligible') is True
        and row.get('passes_momentum_top_percentile') is True
    )
    eligible_momentum_score = momentum_score if momentum_eligible else 0.0
    eligible_composite_score = composite_score if row.get('is_composite_eligible') is True else 0.0
    quality_adjustment = _soft_penalty_score(list(record.get('soft_penalties') or []))
    # Direct rank signals and the percentile factor are both momentum-shaped evidence.
    # Taking the stronger of them prevents one market move from being counted twice.
    short_term_score = (
        signal_score
        + max(direct_signal_score, eligible_momentum_score)
        + eligible_composite_score * 0.20
        + quality_adjustment
    )
    signal_decider_score = (
        signal_score
        + direct_signal_score * 0.25
        + eligible_momentum_score * 0.25
        + eligible_composite_score
        + quality_adjustment
    )
    role_scores = {
        'short-term-decider': round(short_term_score, 6),
        'signal-decider': round(signal_decider_score, 6),
    }
    total_score = max(role_scores.values())
    evidence_source_presence = {
        'action_summary': bool(
            row.get('action_captured_at')
            or row.get('signal_score') is not None
            or row.get('action_label') is not None
        ),
        'direct_signal': bool(
            row.get('signal_captured_at')
            or any(
                row.get(key) is not None
                for key in (
                    'signal_count',
                    'volume_power_rank',
                    'overtime_volume_rank',
                    'overtime_fluctuation_rank',
                )
            )
        ),
        'momentum_factor': bool(
            row.get('momentum_captured_at')
            or row.get('momentum_percentile') is not None
        ),
        'composite_factor': bool(
            row.get('composite_captured_at')
            or row.get('composite_percentile') is not None
        ),
    }
    captured_at_values = [
        str(row[key])
        for key in (
            'action_captured_at',
            'signal_captured_at',
            'momentum_captured_at',
            'composite_captured_at',
        )
        if row.get(key)
    ]
    risk_adjusted_components = [
        _as_float(row.get(key))
        for key in ('composite_low_risk_input', 'composite_low_beta_input')
        if row.get(key) is not None
    ]
    risk_adjusted_component_score = (
        sum(risk_adjusted_components) / len(risk_adjusted_components)
        if risk_adjusted_components
        else None
    )
    tie_breakers = {
        'version': AGENT_FEEDBACK_CANDIDATE_TIE_BREAK_VERSION,
        'evidence_source_count': sum(evidence_source_presence.values()),
        'evidence_source_presence': evidence_source_presence,
        'fresh_evidence_source_count': len(captured_at_values),
        'piotroski_evidence_known_count': (
            _piotroski_evidence_known_count(record)
        ),
        'oldest_evidence_captured_at': (
            min(captured_at_values, key=_evidence_timestamp_sort_value)
            if captured_at_values
            else None
        ),
        'latest_evidence_captured_at': (
            max(captured_at_values, key=_evidence_timestamp_sort_value)
            if captured_at_values
            else None
        ),
        'risk_adjusted_component_score': (
            round(risk_adjusted_component_score, 6)
            if risk_adjusted_component_score is not None
            else None
        ),
        'composite_valid_factor_count': row.get(
            'composite_valid_factor_count'
        ),
        'composite_available_base_weight': row.get(
            'composite_available_base_weight'
        ),
        'risk_tier': _normalized_risk_tier(row.get('risk_tier')),
        'risk_tier_priority': _risk_tier_sort_priority(row.get('risk_tier')),
    }
    return {
        'scoring_version': AGENT_FEEDBACK_CANDIDATE_SCORE_VERSION,
        'score': round(total_score, 6),
        'role_scores': role_scores,
        'tie_breakers': tie_breakers,
        'components': {
            'action_signal_score': signal_score,
            'direct_signal_score': round(direct_signal_score, 6),
            'momentum_score': round(momentum_score, 6),
            'composite_score': round(composite_score, 6),
            'eligible_momentum_score': round(eligible_momentum_score, 6),
            'eligible_composite_score': round(eligible_composite_score, 6),
            'soft_penalty_adjustment': round(quality_adjustment, 6),
        },
        'source_values': {
            'action_label': row.get('action_label'),
            'confidence_tier': row.get('confidence_tier'),
            'signal_score': row.get('signal_score'),
            'signal_count': row.get('signal_count'),
            'volume_power_rank': row.get('volume_power_rank'),
            'overtime_volume_rank': row.get('overtime_volume_rank'),
            'overtime_fluctuation_rank': row.get('overtime_fluctuation_rank'),
            'momentum_percentile': row.get('momentum_percentile'),
            'is_momentum_eligible': row.get('is_momentum_eligible'),
            'passes_momentum_top_percentile': row.get('passes_momentum_top_percentile'),
            'composite_percentile': row.get('composite_percentile'),
            'is_composite_eligible': row.get('is_composite_eligible'),
            'risk_tier': row.get('risk_tier'),
            'composite_low_risk_input': row.get(
                'composite_low_risk_input'
            ),
            'composite_low_beta_input': row.get(
                'composite_low_beta_input'
            ),
        },
    }


def _feedback_payload_positive_action(payload: dict[str, Any]) -> str | None:
    for key in (
        'candidate_action',
        'preferred_action',
        'next_day_open_action',
        'tactical_action',
        'action',
    ):
        action = _normalize_action(payload.get(key))
        if action in _POSITIVE_DECIDER_ACTIONS:
            return action
    return None


def _load_prior_positive_decision_reasons(
    *,
    market_date: date,
    symbols: Sequence[str],
    reasons_by_symbol: dict[str, list[dict[str, Any]]],
) -> dict[str, Any]:
    if not bool(getattr(settings, 'openclaw_feedback_force_include_prior_decisions_enabled', True)):
        return {'status': 'disabled'}
    if not symbols:
        return {'status': 'empty_scope'}
    try:
        with session_scope() as session:
            rows = []
            for symbol in symbols:
                normalized_symbol = _normalize_symbol(symbol)
                if not normalized_symbol:
                    continue
                for agent_name in DECISION_SUPPORT_AGENT_NAMES:
                    row = session.execute(
                        select(
                            AgentContextFeedback.symbol,
                            AgentContextFeedback.agent_name,
                            AgentContextFeedback.market_date,
                            AgentContextFeedback.feedback_payload,
                            AgentContextFeedback.normalized_feedback_payload,
                            AgentContextFeedback.received_at,
                        )
                        .where(AgentContextFeedback.market_date < market_date)
                        .where(AgentContextFeedback.symbol == normalized_symbol)
                        .where(AgentContextFeedback.agent_name == agent_name)
                        .order_by(
                            AgentContextFeedback.market_date.desc(),
                            AgentContextFeedback.received_at.desc(),
                            AgentContextFeedback.id.desc(),
                        )
                        .limit(1)
                    ).first()
                    if row is not None:
                        rows.append(row)
    except Exception as exc:  # noqa: BLE001
        return {'status': 'source_error', 'error_type': type(exc).__name__, 'message': str(exc)}

    seen: set[tuple[str, str]] = set()
    matched = 0
    for row in rows:
        if not is_decision_snapshot_valid_for_source_date(
            {
                'market_date': row.market_date,
                'feedback_received_at': row.received_at,
                'created_at': None,
            }
        ):
            continue
        symbol = _normalize_symbol(row.symbol)
        agent_name = str(row.agent_name or '')
        key = (symbol, agent_name)
        if key in seen:
            continue
        seen.add(key)
        payload = (
            row.normalized_feedback_payload
            if isinstance(row.normalized_feedback_payload, dict)
            else row.feedback_payload
        )
        if not isinstance(payload, dict):
            continue
        action = _feedback_payload_positive_action(payload)
        if action is None:
            continue
        matched += 1
        _append_forced_reason(
            reasons_by_symbol,
            symbol=symbol,
            code='prior_positive_decider_action',
            source='agent_context_feedback',
            message='Latest prior decider feedback for this symbol carried a positive buy/watch action.',
            details={
                'agent_name': agent_name,
                'source_market_date': row.market_date.isoformat() if row.market_date else None,
                'action': action,
                'received_at': row.received_at.isoformat() if row.received_at else None,
            },
        )
    return {'status': 'loaded', 'matched_symbols': matched}


def _load_prior_missed_opportunity_reasons(
    *,
    market_date: date,
    symbols: Sequence[str],
    reasons_by_symbol: dict[str, list[dict[str, Any]]],
) -> dict[str, Any]:
    if not bool(getattr(settings, 'openclaw_feedback_force_include_prior_missed_opportunities_enabled', True)):
        return {'status': 'disabled'}
    if not symbols:
        return {'status': 'empty_scope'}
    try:
        with session_scope() as session:
            rows = []
            for symbol_chunk in _chunk_symbols(symbols):
                rows.extend(
                    session.execute(
                        select(
                            SignalDeciderEvaluation.symbol,
                            SignalDeciderEvaluation.market_date,
                            SignalDeciderEvaluation.decision_action,
                            SignalDeciderEvaluation.horizon_days,
                            SignalDeciderEvaluation.grade_label,
                            SignalDeciderEvaluation.grade_score,
                            SignalDeciderEvaluation.evaluation_version,
                            SignalDeciderEvaluation.evaluated_at,
                            SignalDeciderDecisionSnapshot.feedback_received_at.label(
                                'feedback_received_at'
                            ),
                            SignalDeciderDecisionSnapshot.created_at.label('created_at'),
                        )
                        .join(
                            SignalDeciderDecisionSnapshot,
                            SignalDeciderDecisionSnapshot.id
                            == SignalDeciderEvaluation.decision_snapshot_id,
                        )
                        .where(SignalDeciderEvaluation.market_date < market_date)
                        .where(SignalDeciderEvaluation.symbol.in_(symbol_chunk))
                        .where(SignalDeciderEvaluation.evaluation_status == 'graded')
                        .where(SignalDeciderEvaluation.missed_opportunity.is_(True))
                        .where(
                            SignalDeciderEvaluation.grade_label.in_(
                                ['missed_opportunity_after_wait', 'missed_opportunity_after_reject']
                            )
                        )
                        .order_by(
                            SignalDeciderEvaluation.symbol.asc(),
                            SignalDeciderEvaluation.market_date.desc(),
                            SignalDeciderEvaluation.horizon_days.asc(),
                            SignalDeciderEvaluation.evaluated_at.desc(),
                            SignalDeciderEvaluation.id.desc(),
                        )
                    ).all()
                )
    except Exception as exc:  # noqa: BLE001
        return {'status': 'source_error', 'error_type': type(exc).__name__, 'message': str(exc)}

    seen: set[str] = set()
    matched = 0
    for row in rows:
        symbol = _normalize_symbol(row.symbol)
        if not symbol or symbol in seen:
            continue
        seen.add(symbol)
        if not is_decision_snapshot_valid_for_source_date(row):
            continue
        matched += 1
        _append_forced_reason(
            reasons_by_symbol,
            symbol=symbol,
            code='prior_missed_opportunity_after_wait_or_reject',
            source='signal_decider_evaluations',
            message='Recent signal-decider evaluation marked this symbol as a missed opportunity after waiting/rejecting.',
            details={
                'source_market_date': row.market_date.isoformat() if row.market_date else None,
                'decision_action': row.decision_action,
                'horizon_days': row.horizon_days,
                'grade_label': row.grade_label,
                'grade_score': _as_float(row.grade_score, default=0.0),
                'evaluation_version': row.evaluation_version,
                'evaluated_at': row.evaluated_at.isoformat() if row.evaluated_at else None,
            },
        )
    return {'status': 'loaded', 'matched_symbols': matched}


def _load_event_symbol_reasons(
    *,
    market_date: date,
    symbols: Sequence[str],
    reasons_by_symbol: dict[str, list[dict[str, Any]]],
) -> dict[str, Any]:
    if not bool(getattr(settings, 'openclaw_feedback_force_include_event_symbols_enabled', True)):
        return {'status': 'disabled'}
    if not symbols:
        return {'status': 'empty_scope'}
    try:
        with session_scope() as session:
            rows = session.execute(
                select(
                    DartDisclosureEvent.stock_code,
                    DartDisclosureEvent.rcept_no,
                    DartDisclosureEvent.report_nm,
                    DartDisclosureEvent.receipt_date,
                )
                .where(DartDisclosureEvent.receipt_date == market_date)
                .where(DartDisclosureEvent.stock_code.in_(list(symbols)))
                .order_by(DartDisclosureEvent.stock_code.asc(), DartDisclosureEvent.id.desc())
            ).all()
    except Exception as exc:  # noqa: BLE001
        return {'status': 'source_error', 'error_type': type(exc).__name__, 'message': str(exc)}

    matched = 0
    for row in rows:
        matched += 1
        _append_forced_reason(
            reasons_by_symbol,
            symbol=row.stock_code,
            code='same_day_disclosure_event',
            source='dart_disclosure_events',
            message='Same-day DART disclosure event exists for this symbol.',
            details={
                'rcept_no': row.rcept_no,
                'report_nm': row.report_nm,
                'receipt_date': row.receipt_date.isoformat() if row.receipt_date else None,
            },
        )
    return {'status': 'loaded', 'matched_events': matched}


def _load_active_order_reasons(
    *,
    symbols: Sequence[str],
    reasons_by_symbol: dict[str, list[dict[str, Any]]],
) -> dict[str, Any]:
    if not symbols:
        return {'status': 'empty_scope'}
    try:
        with session_scope() as session:
            request_rows = session.execute(
                select(OrderRequest.symbol, OrderRequest.client_order_id, OrderRequest.submission_status)
                .where(OrderRequest.symbol.in_(list(symbols)))
                .where(OrderRequest.submission_status.in_(list(_ACTIVE_ORDER_REQUEST_STATUSES)))
                .order_by(OrderRequest.symbol.asc(), OrderRequest.updated_at.desc(), OrderRequest.id.desc())
            ).all()
            broker_rows = session.execute(
                select(BrokerOrder.symbol, BrokerOrder.client_order_id, BrokerOrder.current_status)
                .where(BrokerOrder.symbol.in_(list(symbols)))
                .where(BrokerOrder.current_status.in_(list(_ACTIVE_BROKER_ORDER_STATUSES)))
                .order_by(BrokerOrder.symbol.asc(), BrokerOrder.updated_at.desc(), BrokerOrder.id.desc())
            ).all()
    except Exception as exc:  # noqa: BLE001
        return {'status': 'source_error', 'error_type': type(exc).__name__, 'message': str(exc)}

    matched = 0
    for row in request_rows:
        matched += 1
        _append_forced_reason(
            reasons_by_symbol,
            symbol=row.symbol,
            code='active_order_request',
            source='order_requests',
            message='A local order request for this symbol is still active.',
            details={
                'client_order_id': row.client_order_id,
                'submission_status': row.submission_status,
            },
        )
    for row in broker_rows:
        matched += 1
        _append_forced_reason(
            reasons_by_symbol,
            symbol=row.symbol,
            code='active_broker_order',
            source='broker_orders',
            message='A broker order row for this symbol is still active.',
            details={
                'client_order_id': row.client_order_id,
                'current_status': row.current_status,
            },
        )
    return {'status': 'loaded', 'matched_rows': matched}


def _load_active_watch_reasons(
    *,
    market_date: date,
    symbols: Sequence[str],
    reasons_by_symbol: dict[str, list[dict[str, Any]]],
) -> dict[str, Any]:
    if not bool(getattr(settings, 'openclaw_feedback_force_include_active_watch_enabled', True)):
        return {'status': 'disabled'}
    if not symbols:
        return {'status': 'empty_scope'}
    try:
        with session_scope() as session:
            rows = session.execute(
                select(
                    SignalDeciderExecutionPlanItem.symbol,
                    SignalDeciderExecutionPlanItem.decision_agent_name,
                    SignalDeciderExecutionPlanItem.execution_window,
                    SignalDeciderExecutionPlanItem.target_trade_date,
                    SignalDeciderExecutionPlanItem.source_market_date,
                    SignalDeciderExecutionPlanItem.eligibility_status,
                    SignalDeciderExecutionPlanItem.execution_status,
                )
                .where(SignalDeciderExecutionPlanItem.symbol.in_(list(symbols)))
                .where(SignalDeciderExecutionPlanItem.target_trade_date >= market_date)
                .where(SignalDeciderExecutionPlanItem.source_market_date < market_date)
                .where(
                    SignalDeciderExecutionPlanItem.eligibility_status.in_(
                        list(_ACTIVE_EXEC_WATCH_ELIGIBILITY_STATUSES)
                    )
                )
                .where(SignalDeciderExecutionPlanItem.execution_status.in_(list(_ACTIVE_EXEC_WATCH_STATUSES)))
                .order_by(
                    SignalDeciderExecutionPlanItem.symbol.asc(),
                    SignalDeciderExecutionPlanItem.target_trade_date.desc(),
                    SignalDeciderExecutionPlanItem.id.desc(),
                )
            ).all()
    except Exception as exc:  # noqa: BLE001
        return {'status': 'source_error', 'error_type': type(exc).__name__, 'message': str(exc)}

    matched = 0
    for row in rows:
        matched += 1
        _append_forced_reason(
            reasons_by_symbol,
            symbol=row.symbol,
            code='active_exec_watch_item',
            source='signal_decider_execution_plan_items',
            message='A local exec/monitoring plan row is still active for this symbol.',
            details={
                'decision_agent_name': row.decision_agent_name,
                'execution_window': row.execution_window,
                'target_trade_date': row.target_trade_date.isoformat() if row.target_trade_date else None,
                'source_market_date': row.source_market_date.isoformat() if row.source_market_date else None,
                'eligibility_status': row.eligibility_status,
                'execution_status': row.execution_status,
            },
        )
    return {'status': 'loaded', 'matched_rows': matched}


def _add_score_signal_forced_reasons(
    *,
    score_by_symbol: dict[str, dict],
    reasons_by_symbol: dict[str, list[dict[str, Any]]],
) -> dict[str, Any]:
    matched = 0
    for symbol, row in score_by_symbol.items():
        signal_count = _as_int(row.get('signal_count'), default=0)
        rank_fields = {
            'volume_power_rank': _as_int(row.get('volume_power_rank'), default=10**9),
            'overtime_volume_rank': _as_int(row.get('overtime_volume_rank'), default=10**9),
            'overtime_fluctuation_rank': _as_int(row.get('overtime_fluctuation_rank'), default=10**9),
        }
        top_rank_fields = {
            field: rank
            for field, rank in rank_fields.items()
            if 0 < rank <= 30
        }
        if signal_count <= 0 and not top_rank_fields:
            continue
        matched += 1
        _append_forced_reason(
            reasons_by_symbol,
            symbol=symbol,
            code='abnormal_volume_or_price_signal',
            source='agent_feedback_candidate_score_rows',
            message='KIS ranking or direct signal score indicates abnormal volume/price movement.',
            details={
                'signal_count': signal_count,
                'top_rank_fields': top_rank_fields,
            },
        )
    return {'status': 'loaded', 'matched_symbols': matched}


def _load_forced_inclusion_reasons(
    *,
    market_date: date,
    symbols: Sequence[str],
    requested_symbols: Sequence[str] | None,
    score_by_symbol: dict[str, dict] | None = None,
) -> tuple[dict[str, list[dict[str, Any]]], dict[str, Any]]:
    normalized_symbols = _normalize_symbol_scope(symbols) or []
    reasons_by_symbol: dict[str, list[dict[str, Any]]] = {}
    source_diagnostics: dict[str, Any] = {
        'schema_version': AGENT_FEEDBACK_FUNNEL_SCHEMA_VERSION,
        'holdings': {
            'status': (
                'source_unavailable'
                if bool(getattr(settings, 'openclaw_feedback_force_include_holdings_enabled', True))
                else 'disabled'
            ),
            'message': (
                'No persisted holdings snapshot table is available in core; '
                'live portfolio APIs are not called during feedback eligibility.'
            ),
        },
    }
    if requested_symbols is not None:
        requested_cap_bypass = _requested_scope_bypasses_caps(requested_symbols)
        for symbol in _normalize_symbol_scope(requested_symbols) or []:
            _append_forced_reason(
                reasons_by_symbol,
                symbol=symbol,
                code='manual_requested_symbol',
                source='request',
                message=(
                    'Symbol was explicitly requested by the caller; small requested scopes bypass caps, '
                    'while large requested scopes are still ranked and capped.'
                ),
            )
        source_diagnostics['manual_requested_symbols'] = {
            'status': 'loaded',
            'matched_symbols': len(reasons_by_symbol),
            'cap_bypass_enabled': requested_cap_bypass,
            'cap_bypass_max_symbols': max(
                0,
                _as_int(
                    getattr(settings, 'openclaw_feedback_requested_scope_cap_bypass_max_symbols', 50),
                    default=50,
                ),
            ),
        }

    prior_decisions_broad_scope_enabled = bool(
        getattr(settings, 'openclaw_feedback_force_include_prior_decisions_broad_scope_enabled', False)
    )
    prior_decisions_allowed = prior_decisions_broad_scope_enabled or _requested_scope_bypasses_caps(requested_symbols)
    if prior_decisions_allowed:
        source_diagnostics['prior_decisions'] = _load_prior_positive_decision_reasons(
            market_date=market_date,
            symbols=normalized_symbols,
            reasons_by_symbol=reasons_by_symbol,
        )
    else:
        source_diagnostics['prior_decisions'] = {
            'status': 'skipped_broad_scope',
            'message': (
                'Prior positive decider enrichment is skipped for broad daily scopes to avoid '
                'blocking the core eligibility pipeline on historical feedback scans.'
            ),
        }
    prior_missed_broad_scope_enabled = bool(
        getattr(settings, 'openclaw_feedback_force_include_prior_missed_opportunities_broad_scope_enabled', False)
    )
    prior_missed_allowed = prior_missed_broad_scope_enabled or _requested_scope_bypasses_caps(requested_symbols)
    if prior_missed_allowed:
        source_diagnostics['prior_missed_opportunities'] = _load_prior_missed_opportunity_reasons(
            market_date=market_date,
            symbols=normalized_symbols,
            reasons_by_symbol=reasons_by_symbol,
        )
    else:
        source_diagnostics['prior_missed_opportunities'] = {
            'status': 'skipped_broad_scope',
            'message': (
                'Prior missed opportunities are not force-included for broad feedback scopes because '
                'that source can cover most of the universe and defeat candidate caps.'
            ),
        }
    source_diagnostics['event_symbols'] = _load_event_symbol_reasons(
        market_date=market_date,
        symbols=normalized_symbols,
        reasons_by_symbol=reasons_by_symbol,
    )
    source_diagnostics['open_orders'] = _load_active_order_reasons(
        symbols=normalized_symbols,
        reasons_by_symbol=reasons_by_symbol,
    )
    source_diagnostics['active_watch'] = _load_active_watch_reasons(
        market_date=market_date,
        symbols=normalized_symbols,
        reasons_by_symbol=reasons_by_symbol,
    )
    abnormal_movement_broad_scope_enabled = bool(
        getattr(settings, 'openclaw_feedback_force_include_abnormal_movement_broad_scope_enabled', False)
    )
    abnormal_movement_allowed = abnormal_movement_broad_scope_enabled or _requested_scope_bypasses_caps(requested_symbols)
    if abnormal_movement_allowed:
        source_diagnostics['abnormal_movement'] = _add_score_signal_forced_reasons(
            score_by_symbol=score_by_symbol or {},
            reasons_by_symbol=reasons_by_symbol,
        )
    else:
        source_diagnostics['abnormal_movement'] = {
            'status': 'skipped_broad_scope',
            'message': (
                'Abnormal movement signals are ranked through candidate scores but do not force-include '
                'broad daily scopes over role caps.'
            ),
        }
    return reasons_by_symbol, source_diagnostics


def _candidate_cap_limit(total: int) -> int:
    cap_settings = _broad_cap_settings()
    percentile = max(0.0, min(1.0, float(cap_settings['percentile'])))
    min_symbols = max(0, int(cap_settings['min_symbols']))
    max_symbols = max(0, int(cap_settings['max_symbols']))
    if total <= 0 or percentile <= 0:
        return total
    target = max(min_symbols, int(ceil(total * percentile)))
    if max_symbols > 0:
        target = min(target, max_symbols)
    return min(total, max(1, target))


def _cap_metadata(
    *,
    total: int,
    limit: int,
    cap_applied: bool,
    forced_included_count: int = 0,
    forced_source_diagnostics: dict[str, Any] | None = None,
) -> dict[str, Any]:
    cap_settings = _broad_cap_settings()
    return {
        'rule_version': AGENT_FEEDBACK_CANDIDATE_CAP_RULE,
        'cap_scope': 'broad_feedback_universe',
        'applied': cap_applied,
        'pre_cap_eligible_count': total,
        'post_cap_dispatched_count': limit if cap_applied else total,
        'percentile': float(cap_settings['percentile']),
        'min_symbols': int(cap_settings['min_symbols']),
        'max_symbols': int(cap_settings['max_symbols']),
        'forced_included_count': forced_included_count,
        'forced_source_diagnostics': forced_source_diagnostics or {},
    }


def _rank_scored_candidates(dispatchable: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
    def tie_break_key(record: dict[str, Any]) -> tuple[Any, ...]:
        score_payload = _as_dict(record.get('candidate_score_payload'))
        tie_breakers = _as_dict(score_payload.get('tie_breakers'))
        components = _as_dict(score_payload.get('components'))
        return (
            -_as_int(tie_breakers.get('evidence_source_count')),
            -_as_int(tie_breakers.get('fresh_evidence_source_count')),
            -_as_int(tie_breakers.get('piotroski_evidence_known_count')),
            -_as_int(tie_breakers.get('composite_valid_factor_count')),
            -_as_float(
                tie_breakers.get('composite_available_base_weight')
            ),
            -_evidence_timestamp_sort_value(
                tie_breakers.get('oldest_evidence_captured_at')
            ),
            -_evidence_timestamp_sort_value(
                tie_breakers.get('latest_evidence_captured_at')
            ),
            _risk_tier_sort_priority(tie_breakers.get('risk_tier')),
            -_as_float(
                tie_breakers.get('risk_adjusted_component_score'),
                default=-1.0,
            ),
            -_as_float(components.get('eligible_composite_score')),
            -_as_float(components.get('eligible_momentum_score')),
            -_as_float(components.get('direct_signal_score')),
            str(record.get('symbol') or ''),
        )

    ranked = sorted(
        dispatchable,
        key=lambda record: (
            -_as_float(record.get('candidate_score')),
            *tie_break_key(record),
        ),
    )
    for index, record in enumerate(ranked, start=1):
        record['candidate_rank'] = index
        record['candidate_global_rank'] = index
        record['candidate_role_ranks'] = {}
    for role in DECISION_SUPPORT_AGENT_NAMES:
        role_ranked = sorted(
            dispatchable,
            key=lambda record: (
                -_as_float(
                    _as_dict(_as_dict(record.get('candidate_score_payload')).get('role_scores')).get(role),
                    default=_as_float(record.get('candidate_score')),
                ),
                *tie_break_key(record),
            ),
        )
        for index, record in enumerate(role_ranked, start=1):
            record['candidate_role_ranks'][role] = index
    return ranked


def _selection_boundary(
    *,
    ranked: Sequence[dict[str, Any]],
    selected_symbols: set[str],
) -> dict[str, Any]:
    def summary(record: dict[str, Any] | None) -> dict[str, Any] | None:
        if record is None:
            return None
        score_payload = _as_dict(record.get('candidate_score_payload'))
        return {
            'symbol': record.get('symbol'),
            'candidate_rank': record.get('candidate_rank'),
            'candidate_score': record.get('candidate_score'),
            'tie_breakers': _as_dict(score_payload.get('tie_breakers')),
        }

    selected = [
        record
        for record in ranked
        if str(record.get('symbol')) in selected_symbols
    ]
    excluded = [
        record
        for record in ranked
        if str(record.get('symbol')) not in selected_symbols
    ]
    return {
        'selection_basis': AGENT_FEEDBACK_CANDIDATE_TIE_BREAK_VERSION,
        'last_selected': summary(selected[-1] if selected else None),
        'first_excluded': summary(excluded[0] if excluded else None),
        'excluded_alternatives_preview': [
            summary(record) for record in excluded[:5]
        ],
    }


def _apply_candidate_cap(
    *,
    records: list[dict[str, Any]],
    requested_symbols: Sequence[str] | None,
) -> list[dict[str, Any]]:
    dispatchable = [record for record in records if not bool(record.get('hard_blocked'))]
    requested_cap_bypass = _requested_scope_bypasses_caps(requested_symbols)
    if not dispatchable:
        metadata = _cap_metadata(total=0, limit=0, cap_applied=False)
        for record in records:
            record.setdefault('candidate_cap', dict(metadata))
            record.setdefault('cap_excluded', False)
            record.setdefault('forced_inclusion_reasons', [])
            record.setdefault('forced_included', False)
            record.setdefault('forced_inclusion_reason', None)
        return records

    market_date = date.fromisoformat(str(records[0]['market_date']))
    score_rows = load_agent_feedback_candidate_score_rows(
        market_date=market_date,
        symbols=[str(record['symbol']) for record in dispatchable],
    )
    score_by_symbol = _score_rows_by_symbol(score_rows)
    for record in dispatchable:
        score_payload = _candidate_score_payload(
            record=record,
            score_row=score_by_symbol.get(str(record.get('symbol'))),
        )
        record['candidate_score'] = score_payload['score']
        record['candidate_score_payload'] = score_payload
    ranked = _rank_scored_candidates(dispatchable)

    if requested_cap_bypass:
        forced_reasons_by_symbol: dict[str, list[dict[str, Any]]] = {}
        forced_source_diagnostics: dict[str, Any] = {}
        forced_reasons_by_symbol, forced_source_diagnostics = _load_forced_inclusion_reasons(
            market_date=market_date,
            symbols=[str(record['symbol']) for record in dispatchable],
            requested_symbols=requested_symbols,
            score_by_symbol=score_by_symbol,
        )
        metadata = _cap_metadata(
            total=len(dispatchable),
            limit=len(dispatchable),
            cap_applied=False,
            forced_included_count=len(forced_reasons_by_symbol),
            forced_source_diagnostics=forced_source_diagnostics,
        )
        for record in records:
            record.setdefault('candidate_cap', dict(metadata))
            record.setdefault('cap_excluded', False)
            reasons = forced_reasons_by_symbol.get(str(record.get('symbol') or '').strip().upper(), [])
            record['forced_inclusion_reasons'] = list(reasons)
            record['forced_included'] = bool(reasons)
            record['forced_inclusion_reason'] = str(reasons[0].get('code')) if reasons else None
        for record in dispatchable:
            record['candidate_cap'] = dict(metadata)
            record['candidate_selection_reason'] = AGENT_FEEDBACK_ROLE_CAP_BYPASS_REASON_REQUESTED
        return records

    limit = _candidate_cap_limit(len(dispatchable))
    forced_reasons_by_symbol, forced_source_diagnostics = _load_forced_inclusion_reasons(
        market_date=market_date,
        symbols=[str(record['symbol']) for record in dispatchable],
        requested_symbols=requested_symbols if requested_cap_bypass else None,
        score_by_symbol=score_by_symbol,
    )
    forced_symbols = {
        symbol
        for symbol, reasons in forced_reasons_by_symbol.items()
        if reasons
    }
    selected_symbols: set[str] = set()
    for record in ranked:
        if len(selected_symbols) >= limit and limit > 0:
            break
        selected_symbols.add(str(record.get('symbol')))
    selected_symbols.update(forced_symbols)
    if limit <= 0:
        selected_symbols = {str(record.get('symbol')) for record in ranked}
    cap_applied = len(selected_symbols) < len(dispatchable)
    metadata = _cap_metadata(
        total=len(dispatchable),
        limit=len(selected_symbols),
        cap_applied=cap_applied,
        forced_included_count=len(forced_symbols),
        forced_source_diagnostics=forced_source_diagnostics,
    )
    metadata['selection_boundary'] = _selection_boundary(
        ranked=ranked,
        selected_symbols=selected_symbols,
    )
    for index, record in enumerate(ranked, start=1):
        symbol = str(record.get('symbol'))
        forced_reasons = forced_reasons_by_symbol.get(symbol, [])
        record['candidate_rank'] = index
        record['candidate_cap'] = dict(metadata)
        record['forced_inclusion_reasons'] = list(forced_reasons)
        record['forced_included'] = bool(forced_reasons)
        record['forced_inclusion_reason'] = str(forced_reasons[0].get('code')) if forced_reasons else None
        if cap_applied and symbol not in selected_symbols:
            record['eligible'] = False
            record['cap_excluded'] = True
            record['status'] = 'candidate_cap_excluded'
            record['candidate_selection_reason'] = 'broad_rank_cap_excluded'
        else:
            record['cap_excluded'] = False
            record['candidate_selection_reason'] = (
                'forced_included_over_broad_rank_cap'
                if forced_reasons and index > limit
                else 'broad_rank_within_cap'
            )
            if forced_reasons and record['status'] == 'dispatched':
                record['status'] = 'forced_included'
            elif forced_reasons and record['status'] == 'soft_penalized_dispatched':
                record['status'] = 'soft_penalized_forced_included'
    for record in records:
        record.setdefault('candidate_cap', dict(metadata))
        record.setdefault('cap_excluded', False)
        record.setdefault('forced_inclusion_reasons', [])
        record.setdefault('forced_included', False)
        record.setdefault('forced_inclusion_reason', None)
    return records


def _build_eligibility_record(
    *,
    symbol: str,
    market_date: date,
    strategy_scope: str,
    market_risk_row: dict | None,
    piotroski_row: dict | None,
) -> dict[str, Any]:
    hard_block_reasons = _market_risk_hard_blocks(market_risk_row)
    soft_penalties = _market_risk_soft_penalties(market_risk_row)
    if strategy_scope == AGENT_FEEDBACK_STRATEGY_SCOPE_VALUE_LONG_ONLY:
        hard_block_reasons.extend(_piotroski_hard_blocks_for_value_scope(piotroski_row))
        rule_version = AGENT_FEEDBACK_VALUE_LONG_ONLY_ELIGIBILITY_RULE
        candidate_path = AGENT_FEEDBACK_STRATEGY_SCOPE_VALUE_LONG_ONLY
    else:
        soft_penalties.extend(_piotroski_soft_penalties(piotroski_row))
        rule_version = AGENT_FEEDBACK_ELIGIBILITY_RULE
        candidate_path = AGENT_FEEDBACK_STRATEGY_SCOPE_SHORT_TERM_MOMENTUM

    hard_blocked = bool(hard_block_reasons)
    status = (
        'hard_blocked'
        if hard_blocked
        else 'soft_penalized_dispatched'
        if soft_penalties
        else 'dispatched'
    )
    return {
        'symbol': symbol,
        'market_date': market_date.isoformat(),
        'strategy_scope': strategy_scope,
        'candidate_path': candidate_path,
        'rule_version': rule_version,
        'eligible': not hard_blocked,
        'hard_blocked': hard_blocked,
        'status': status,
        'hard_block_reasons': hard_block_reasons,
        'soft_penalties': soft_penalties,
        'filter_signals': {
            'market_risk_filter': _market_risk_filter_signals(market_risk_row),
            'piotroski_filter': _piotroski_filter_signals(piotroski_row),
        },
    }


def load_agent_feedback_eligibility_records(
    *,
    market_date: date,
    requested_symbols: Sequence[str] | None = None,
    strategy_scope: str = AGENT_FEEDBACK_DEFAULT_STRATEGY_SCOPE,
) -> list[dict[str, Any]]:
    symbol_scope = _normalize_symbol_scope(requested_symbols)
    if requested_symbols is not None and not symbol_scope:
        return []
    piotroski_rows = load_daily_piotroski_filter_results(
        market_date=market_date,
        symbols=symbol_scope,
        passed_filter=None,
        limit=None,
    )
    market_risk_rows = load_daily_market_risk_filter_results(
        market_date=market_date,
        symbols=symbol_scope,
        passed_filter=None,
        limit=None,
    )
    piotroski_by_symbol = _rows_by_symbol(piotroski_rows)
    market_risk_by_symbol = _rows_by_symbol(market_risk_rows)
    records = [
        _build_eligibility_record(
            symbol=symbol,
            market_date=market_date,
            strategy_scope=strategy_scope,
            market_risk_row=market_risk_by_symbol.get(symbol),
            piotroski_row=piotroski_by_symbol.get(symbol),
        )
        for symbol in _record_order(
            requested_symbols=symbol_scope,
            market_risk_rows=market_risk_rows,
            piotroski_rows=piotroski_rows,
        )
    ]
    return _apply_candidate_cap(
        records=records,
        requested_symbols=symbol_scope if requested_symbols is not None else None,
    )


def _ordered_dispatchable_records(
    *,
    records: Sequence[dict[str, Any]],
    candidate_symbols: Sequence[str] | None = None,
) -> list[dict[str, Any]]:
    normalized_candidate_symbols = _normalize_symbol_scope(candidate_symbols) or []
    candidate_order = {
        symbol: index
        for index, symbol in enumerate(normalized_candidate_symbols)
    }
    if not records and normalized_candidate_symbols:
        return [
            {
                'symbol': symbol,
                'eligible': True,
                'hard_blocked': False,
                'cap_excluded': False,
                'candidate_rank': index,
                'candidate_score': 0.0,
                'forced_inclusion_reasons': [],
                'forced_included': False,
                'forced_inclusion_reason': None,
            }
            for index, symbol in enumerate(normalized_candidate_symbols, start=1)
        ]

    def order_key(record: dict[str, Any]) -> tuple[int, int, int, str]:
        symbol = str(record.get('symbol') or '').strip().upper()
        if symbol in candidate_order:
            return (0, candidate_order[symbol], _as_int(record.get('candidate_rank'), default=10**9), symbol)
        return (1, _as_int(record.get('candidate_rank'), default=10**9), 0, symbol)

    ordered = sorted(
        [
            dict(record)
            for record in records
            if not bool(record.get('hard_blocked')) and not bool(record.get('cap_excluded'))
        ],
        key=order_key,
    )
    for index, record in enumerate(ordered, start=1):
        if record.get('candidate_rank') is None:
            record['candidate_rank'] = index
    return ordered


def _summarizer_gate_payload(value: Any) -> dict[str, Any]:
    if not isinstance(value, dict):
        return {}
    details = value.get('details') if isinstance(value.get('details'), dict) else {}
    nested_gate = details.get('summarizer_gate') if isinstance(details.get('summarizer_gate'), dict) else {}
    payload = {**nested_gate}
    for key in (
        'priority_score',
        'candidate_action',
        'gate_reason',
        'data_quality_blockers',
        'positive_catalysts',
        'negative_risks',
        'time_horizon_fit',
    ):
        if key in value:
            payload[key] = value[key]
    return payload


def _summarizer_gate_score(gate: dict[str, Any]) -> float:
    score = _as_float(gate.get('priority_score'), default=0.0) * 100.0
    action = _normalize_action(gate.get('candidate_action'))
    if action == 'advance_to_decider':
        score += 10_000.0
    elif action in {'watch_only', 'watch'}:
        score += 1_000.0
    elif action == 'reject':
        score -= 10_000.0
    return score


def _merge_forced_reasons(
    *,
    record: dict[str, Any],
    forced_inclusion_reasons_by_symbol: dict[str, list[dict[str, Any]]] | None,
) -> list[dict[str, Any]]:
    symbol = str(record.get('symbol') or '').strip().upper()
    merged: list[dict[str, Any]] = []
    seen_codes: set[str] = set()
    for source in (
        record.get('forced_inclusion_reasons'),
        (forced_inclusion_reasons_by_symbol or {}).get(symbol),
    ):
        if not isinstance(source, list):
            continue
        for item in source:
            if not isinstance(item, dict):
                continue
            code = str(item.get('code') or '')
            if code in seen_codes:
                continue
            seen_codes.add(code)
            merged.append(dict(item))
    return merged


def _role_ranked_records(
    *,
    role: str,
    records: Sequence[dict[str, Any]],
    summarizer_gate_by_symbol: dict[str, Any] | None,
) -> list[dict[str, Any]]:
    gate_by_symbol = summarizer_gate_by_symbol or {}

    def role_score(record: dict[str, Any]) -> tuple[float, int, str]:
        symbol = str(record.get('symbol') or '').strip().upper()
        score = _as_float(record.get('candidate_score'), default=0.0)
        if role in DECISION_SUPPORT_AGENT_NAMES:
            score_payload = _as_dict(record.get('candidate_score_payload'))
            role_scores = _as_dict(score_payload.get('role_scores'))
            score = _as_float(role_scores.get(role), default=score)
            gate = _summarizer_gate_payload(gate_by_symbol.get(symbol))
            score += _summarizer_gate_score(gate)
        elif role == 'fundamental-summarizer':
            score_payload = (
                record.get('candidate_score_payload')
                if isinstance(record.get('candidate_score_payload'), dict)
                else {}
            )
            source_values = (
                score_payload.get('source_values')
                if isinstance(score_payload.get('source_values'), dict)
                else {}
            )
            score += _as_float(source_values.get('composite_percentile'), default=0.0) * 25.0
        return (
            -score,
            _as_int(record.get('candidate_rank'), default=10**9),
            symbol,
        )

    return sorted([dict(record) for record in records], key=role_score)


def _role_cap_metadata(
    *,
    role: str,
    total: int,
    selected_count: int,
    cap_limit: int,
    cap_applied: bool,
    requested_scope_bypass: bool,
    forced_included_count: int,
) -> dict[str, Any]:
    return {
        'rule_version': AGENT_FEEDBACK_ROLE_CAP_RULE,
        'role': role,
        'configured_cap': cap_limit,
        'applied': cap_applied,
        'requested_scope_bypass': requested_scope_bypass,
        'bypass_reason': AGENT_FEEDBACK_ROLE_CAP_BYPASS_REASON_REQUESTED if requested_scope_bypass else None,
        'pre_cap_symbol_count': total,
        'post_cap_symbol_count': selected_count,
        'forced_included_count': forced_included_count,
    }


def _build_role_scope_selection(
    *,
    role: str,
    records: Sequence[dict[str, Any]],
    requested_scope_bypass: bool,
    summarizer_gate_by_symbol: dict[str, Any] | None,
    forced_inclusion_reasons_by_symbol: dict[str, list[dict[str, Any]]] | None,
    feedback_scope_mode: str,
    decider_scope_symbols: Sequence[str] | None = None,
) -> tuple[list[str], dict[str, Any]]:
    ranked = _role_ranked_records(
        role=role,
        records=records,
        summarizer_gate_by_symbol=summarizer_gate_by_symbol,
    )
    forced_reasons_by_symbol: dict[str, list[dict[str, Any]]] = {}
    for record in ranked:
        symbol = str(record.get('symbol') or '').strip().upper()
        forced_reasons = _merge_forced_reasons(
            record=record,
            forced_inclusion_reasons_by_symbol=forced_inclusion_reasons_by_symbol,
        )
        if forced_reasons:
            forced_reasons_by_symbol[symbol] = forced_reasons

    if requested_scope_bypass:
        selected_symbols = {str(record.get('symbol') or '').strip().upper() for record in ranked}
        cap_limit = len(selected_symbols)
    else:
        cap_limit = _role_cap_limit(role, len(ranked))
        selected_symbols = set()
        for record in ranked:
            if len(selected_symbols) >= cap_limit and cap_limit > 0:
                break
            selected_symbols.add(str(record.get('symbol') or '').strip().upper())
        force_over_cap_enabled = bool(
            getattr(settings, 'openclaw_feedback_force_include_over_role_cap_broad_scope_enabled', False)
        )
        if force_over_cap_enabled:
            selected_symbols.update(forced_reasons_by_symbol)
        if cap_limit <= 0:
            selected_symbols = {str(record.get('symbol') or '').strip().upper() for record in ranked}

    decider_scope_applied = decider_scope_symbols is not None
    normalized_decider_scope = _normalize_symbol_scope(decider_scope_symbols) or []
    role_cap_selected_symbols = set(selected_symbols)
    ranked_symbol_set = {
        str(record.get('symbol') or '').strip().upper()
        for record in ranked
    }
    if decider_scope_applied:
        # The fast path is already bounded by the smallest requested upstream
        # role cap. Summarizers cover the combined prerequisite scope, while
        # each decider keeps only its own role-ranked candidates.
        decider_scope_set = set(normalized_decider_scope).intersection(ranked_symbol_set)
        if role in DECISION_SUPPORT_AGENT_NAMES:
            selected_symbols.intersection_update(decider_scope_set)
        else:
            selected_symbols = decider_scope_set

    if decider_scope_applied:
        if role in DECISION_SUPPORT_AGENT_NAMES:
            ordered_selected = [
                str(record.get('symbol') or '').strip().upper()
                for record in ranked
                if str(record.get('symbol') or '').strip().upper() in selected_symbols
                and str(record.get('symbol') or '').strip().upper() in normalized_decider_scope
            ]
        else:
            ordered_selected = [
                symbol
                for symbol in normalized_decider_scope
                if symbol in ranked_symbol_set and symbol in selected_symbols
            ]
    else:
        ordered_selected = [
            str(record.get('symbol') or '').strip().upper()
            for record in ranked
            if str(record.get('symbol') or '').strip().upper() in selected_symbols
        ]
    cap_excluded = [
        str(record.get('symbol') or '').strip().upper()
        for record in ranked
        if str(record.get('symbol') or '').strip().upper() not in selected_symbols
    ]
    cap_applied = len(ordered_selected) < len(ranked)
    selection_reasons_by_symbol: dict[str, list[str]] = defaultdict(list)
    rank_by_symbol: dict[str, int] = {}
    for index, record in enumerate(ranked, start=1):
        symbol = str(record.get('symbol') or '').strip().upper()
        rank_by_symbol[symbol] = index
        if symbol not in selected_symbols:
            continue
        if requested_scope_bypass and symbol in role_cap_selected_symbols:
            selection_reasons_by_symbol[symbol].append(AGENT_FEEDBACK_ROLE_CAP_BYPASS_REASON_REQUESTED)
        elif symbol in role_cap_selected_symbols and index <= cap_limit:
            selection_reasons_by_symbol[symbol].append('rank_within_role_cap')
        gate = _summarizer_gate_payload((summarizer_gate_by_symbol or {}).get(symbol))
        if _normalize_action(gate.get('candidate_action')) == 'advance_to_decider':
            selection_reasons_by_symbol[symbol].append('summarizer_gate_advance_to_decider')
        if decider_scope_applied:
            selection_reasons_by_symbol[symbol].append(FEEDBACK_SCOPE_MODE_DECIDER_SCOPE_FAST_PATH)
            if symbol not in role_cap_selected_symbols:
                selection_reasons_by_symbol[symbol].append('decider_scope_prerequisite')
        for reason in forced_reasons_by_symbol.get(symbol, []):
            selection_reasons_by_symbol[symbol].append(f"forced:{reason.get('code')}")

    diagnostics = {
        'role': role,
        'symbols': ordered_selected,
        'feedback_scope_mode': feedback_scope_mode,
        'role_cap': _role_cap_metadata(
            role=role,
            total=len(ranked),
            selected_count=len(ordered_selected),
            cap_limit=cap_limit,
            cap_applied=cap_applied,
            requested_scope_bypass=requested_scope_bypass,
            forced_included_count=len(forced_reasons_by_symbol),
        ),
        'scope_alignment': {
            'mode': feedback_scope_mode,
            'applied': decider_scope_applied,
            'decider_scope_symbol_count': len(ordered_selected) if decider_scope_applied else None,
        },
        'cap_excluded_symbols': cap_excluded[:200],
        'cap_excluded_symbol_count': len(cap_excluded),
        'forced_included_symbols': sorted(forced_reasons_by_symbol),
        'forced_inclusion_reasons_by_symbol': forced_reasons_by_symbol,
        'selection_reasons_by_symbol': {
            symbol: reasons
            for symbol, reasons in selection_reasons_by_symbol.items()
        },
        'role_rank_by_symbol': rank_by_symbol,
    }
    return ordered_selected, diagnostics


def _decider_scope_roles(roles: Sequence[str]) -> list[str]:
    requested_decider_roles = [
        role
        for role in roles
        if role in DECISION_SUPPORT_AGENT_NAMES
    ]
    return requested_decider_roles or list(DECISION_SUPPORT_AGENT_NAMES)


def _build_decider_scope_fast_path(
    *,
    roles: Sequence[str],
    records: Sequence[dict[str, Any]],
    requested_scope_bypass: bool,
    summarizer_gate_by_symbol: dict[str, Any] | None,
    forced_inclusion_reasons_by_symbol: dict[str, list[dict[str, Any]]] | None,
) -> dict[str, Any]:
    source_roles = _decider_scope_roles(roles)
    candidate_scopes: list[tuple[str, list[str], dict[str, Any]]] = []
    for role in source_roles:
        symbols, diagnostics = _build_role_scope_selection(
            role=role,
            records=records,
            requested_scope_bypass=requested_scope_bypass,
            summarizer_gate_by_symbol=summarizer_gate_by_symbol,
            forced_inclusion_reasons_by_symbol=forced_inclusion_reasons_by_symbol,
            feedback_scope_mode=FEEDBACK_SCOPE_MODE_DECIDER_SCOPE_FAST_PATH,
        )
        candidate_scopes.append((role, symbols, diagnostics))

    if not candidate_scopes:
        return {
            'mode': FEEDBACK_SCOPE_MODE_DECIDER_SCOPE_FAST_PATH,
            'applied': True,
            'source_roles': [],
            'reference_role': None,
            'symbol_count': 0,
            'symbols': [],
            'candidate_role_symbol_counts': {},
            'prerequisite_role_cap_limits': {},
        }

    reference_role = 'decider_role_union'
    reference_symbols: list[str] = []
    seen_symbols: set[str] = set()
    max_scope_size = max(len(symbols) for _, symbols, _ in candidate_scopes)
    for index in range(max_scope_size):
        for _, symbols, _ in candidate_scopes:
            if index >= len(symbols):
                continue
            symbol = symbols[index]
            if symbol in seen_symbols:
                continue
            seen_symbols.add(symbol)
            reference_symbols.append(symbol)
    prerequisite_role_cap_limits = {
        role: _role_cap_limit(role, len(records))
        for role in roles
        if role in SUMMARIZER_AGENT_NAMES
    }
    if prerequisite_role_cap_limits and not requested_scope_bypass:
        scope_limit = min(prerequisite_role_cap_limits.values())
        reference_symbols = reference_symbols[:scope_limit]
    return {
        'mode': FEEDBACK_SCOPE_MODE_DECIDER_SCOPE_FAST_PATH,
        'applied': True,
        'source_roles': source_roles,
        'reference_role': reference_role,
        'symbol_count': len(reference_symbols),
        'symbols': list(reference_symbols),
        'candidate_role_symbol_counts': {
            role: len(symbols)
            for role, symbols, _ in candidate_scopes
        },
        'prerequisite_role_cap_limits': prerequisite_role_cap_limits,
    }


def build_agent_feedback_role_funnel(
    *,
    records: Sequence[dict[str, Any]],
    roles: Sequence[str],
    requested_symbols: Sequence[str] | None = None,
    candidate_symbols: Sequence[str] | None = None,
    summarizer_gate_by_symbol: dict[str, Any] | None = None,
    forced_inclusion_reasons_by_symbol: dict[str, list[dict[str, Any]]] | None = None,
    feedback_scope_mode: str = FEEDBACK_SCOPE_MODE_BROAD_FEEDBACK_UNIVERSE,
) -> dict[str, Any]:
    normalized_feedback_scope_mode = normalize_feedback_scope_mode(feedback_scope_mode)
    requested_scope_bypass = _requested_scope_bypasses_caps(requested_symbols)
    dispatchable_records = _ordered_dispatchable_records(
        records=records,
        candidate_symbols=candidate_symbols,
    )
    broad_symbols = [str(record.get('symbol') or '').strip().upper() for record in dispatchable_records]
    role_symbols: dict[str, list[str]] = {}
    role_diagnostics: dict[str, Any] = {}
    seen_roles: list[str] = []
    decider_scope = (
        _build_decider_scope_fast_path(
            roles=roles,
            records=dispatchable_records,
            requested_scope_bypass=requested_scope_bypass,
            summarizer_gate_by_symbol=summarizer_gate_by_symbol,
            forced_inclusion_reasons_by_symbol=forced_inclusion_reasons_by_symbol,
        )
        if normalized_feedback_scope_mode == FEEDBACK_SCOPE_MODE_DECIDER_SCOPE_FAST_PATH
        else {
            'mode': FEEDBACK_SCOPE_MODE_BROAD_FEEDBACK_UNIVERSE,
            'applied': False,
            'source_roles': [],
            'reference_role': None,
            'symbol_count': None,
            'symbols': None,
            'candidate_role_symbol_counts': {},
        }
    )
    for raw_role in roles:
        role = str(raw_role or '').strip()
        if not role or role in seen_roles:
            continue
        seen_roles.append(role)
        fast_path_scope_symbols = (
            decider_scope.get('symbols')
            if normalized_feedback_scope_mode == FEEDBACK_SCOPE_MODE_DECIDER_SCOPE_FAST_PATH
            and role in (*SUMMARIZER_AGENT_NAMES, *DECISION_SUPPORT_AGENT_NAMES)
            else None
        )
        selected, diagnostics = _build_role_scope_selection(
            role=role,
            records=dispatchable_records,
            requested_scope_bypass=requested_scope_bypass,
            summarizer_gate_by_symbol=summarizer_gate_by_symbol,
            forced_inclusion_reasons_by_symbol=forced_inclusion_reasons_by_symbol,
            feedback_scope_mode=normalized_feedback_scope_mode,
            decider_scope_symbols=fast_path_scope_symbols,
        )
        role_symbols[role] = selected
        role_diagnostics[role] = diagnostics
    return {
        'schema_version': AGENT_FEEDBACK_FUNNEL_SCHEMA_VERSION,
        'feedback_scope_mode': normalized_feedback_scope_mode,
        'broad_symbol_count': len(broad_symbols),
        'broad_symbols': broad_symbols,
        'decider_scope': decider_scope,
        'requested_symbol_count': len(_normalize_symbol_scope(requested_symbols) or []),
        'requested_scope_bypass': requested_scope_bypass,
        'role_symbols': role_symbols,
        'role_symbol_counts': {role: len(symbols) for role, symbols in role_symbols.items()},
        'role_diagnostics': role_diagnostics,
    }


def build_agent_feedback_eligibility_diagnostics(
    *,
    records: Sequence[dict[str, Any]],
    requested_symbols: Sequence[str] | None = None,
) -> dict[str, Any]:
    normalized_requested = _normalize_symbol_scope(requested_symbols)
    hard_blocked = [record for record in records if bool(record.get('hard_blocked'))]
    cap_excluded = [record for record in records if bool(record.get('cap_excluded'))]
    forced_included = [record for record in records if bool(record.get('forced_included'))]
    dispatched = [
        record
        for record in records
        if not bool(record.get('hard_blocked')) and not bool(record.get('cap_excluded'))
    ]
    soft_penalized = [record for record in dispatched if record.get('soft_penalties')]
    rule_version = records[0].get('rule_version') if records else AGENT_FEEDBACK_ELIGIBILITY_RULE
    strategy_scope = records[0].get('strategy_scope') if records else AGENT_FEEDBACK_DEFAULT_STRATEGY_SCOPE
    return {
        'rule_version': rule_version,
        'strategy_scope': strategy_scope,
        'requested_symbol_count': len(normalized_requested or []),
        'evaluated_symbol_count': len(records),
        'dispatched_symbol_count': len(dispatched),
        'hard_blocked_symbol_count': len(hard_blocked),
        'candidate_cap_excluded_symbol_count': len(cap_excluded),
        'forced_included_symbol_count': len(forced_included),
        'soft_penalized_dispatched_symbol_count': len(soft_penalized),
        'hard_blocked_symbols': [str(record.get('symbol')) for record in hard_blocked],
        'candidate_cap_excluded_symbols': [str(record.get('symbol')) for record in cap_excluded[:100]],
        'forced_included_symbols': [str(record.get('symbol')) for record in forced_included],
        'soft_penalized_symbols': [str(record.get('symbol')) for record in soft_penalized],
        'candidate_cap': (
            records[0].get('candidate_cap')
            if records and isinstance(records[0].get('candidate_cap'), dict)
            else _cap_metadata(total=len(dispatched), limit=len(dispatched), cap_applied=False)
        ),
        'hard_block_reasons_by_symbol': {
            str(record.get('symbol')): list(record.get('hard_block_reasons') or [])
            for record in hard_blocked
        },
        'soft_penalties_by_symbol': {
            str(record.get('symbol')): list(record.get('soft_penalties') or [])
            for record in soft_penalized
        },
        'forced_inclusion_reasons_by_symbol': {
            str(record.get('symbol')): list(record.get('forced_inclusion_reasons') or [])
            for record in forced_included
        },
    }


def resolve_agent_feedback_scope(*, requested_symbols: Sequence[str] | None) -> str:
    return AGENT_FEEDBACK_SCOPE_REQUESTED if requested_symbols is not None else AGENT_FEEDBACK_SCOPE_ALL


def load_agent_feedback_eligible_symbols(
    *,
    market_date: date,
    requested_symbols: Sequence[str] | None = None,
    strategy_scope: str = AGENT_FEEDBACK_DEFAULT_STRATEGY_SCOPE,
) -> list[str]:
    records = load_agent_feedback_eligibility_records(
        market_date=market_date,
        requested_symbols=requested_symbols,
        strategy_scope=strategy_scope,
    )
    ordered_records = (
        records
        if requested_symbols is not None
        else sorted(
            records,
            key=lambda record: (
                _as_int(record.get('candidate_rank'), default=10**9),
                str(record.get('symbol')),
            ),
        )
    )
    return [
        str(record['symbol'])
        for record in ordered_records
        if not bool(record.get('hard_blocked')) and not bool(record.get('cap_excluded'))
    ]


def split_requested_symbols_by_agent_feedback_eligibility(
    *,
    market_date: date,
    requested_symbols: Sequence[str],
    strategy_scope: str = AGENT_FEEDBACK_DEFAULT_STRATEGY_SCOPE,
) -> tuple[list[str], list[str]]:
    eligible_symbols, skipped_symbols, _ = split_requested_symbols_by_agent_feedback_eligibility_with_diagnostics(
        market_date=market_date,
        requested_symbols=requested_symbols,
        strategy_scope=strategy_scope,
    )
    return eligible_symbols, skipped_symbols


def split_requested_symbols_by_agent_feedback_eligibility_with_diagnostics(
    *,
    market_date: date,
    requested_symbols: Sequence[str],
    strategy_scope: str = AGENT_FEEDBACK_DEFAULT_STRATEGY_SCOPE,
) -> tuple[list[str], list[str], dict[str, Any]]:
    normalized_requested = _normalize_symbol_scope(requested_symbols) or []
    records = load_agent_feedback_eligibility_records(
        market_date=market_date,
        requested_symbols=normalized_requested,
        strategy_scope=strategy_scope,
    )
    records_by_symbol = {str(record.get('symbol')): record for record in records}
    eligible_symbols = [
        symbol
        for symbol in normalized_requested
        if (
            symbol in records_by_symbol
            and not bool(records_by_symbol[symbol].get('hard_blocked'))
            and not bool(records_by_symbol[symbol].get('cap_excluded'))
        )
    ]
    eligible_symbol_set = set(eligible_symbols)
    skipped_symbols = [symbol for symbol in normalized_requested if symbol not in eligible_symbol_set]
    diagnostics = build_agent_feedback_eligibility_diagnostics(
        records=records,
        requested_symbols=normalized_requested,
    )
    diagnostics['records_by_symbol'] = records_by_symbol
    return eligible_symbols, skipped_symbols, diagnostics
