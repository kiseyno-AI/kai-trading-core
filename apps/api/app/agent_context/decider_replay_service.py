from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import date, datetime, timedelta
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
import json
import math
from collections import Counter
from typing import Any, Sequence
from zoneinfo import ZoneInfo

from sqlalchemy import select

from app.agent_context.roles import (
    SHORT_TERM_DECIDER_AGENT_NAME,
    SIGNAL_DECIDER_AGENT_NAME,
    canonical_evaluation_action,
)
from app.agent_context.model_identity import (
    expected_decider_model,
    select_model_identity_cohort_rows,
)
from app.core.settings import settings
from app.db import session_scope
from app.db_models import (
    DeciderPostDecisionEvaluation,
    SignalDeciderActualExecutionOutcome,
    SignalDeciderDecisionSnapshot,
    SignalDeciderExecutionPlanItem,
    SignalDeciderRealizedOutcome,
)
from app.orders.signal_decider_alignment_gate import (
    ALIGNMENT_GATE_ACTION_BLOCK,
    ALIGNMENT_GATE_ACTION_DOWNSIZE_PROBE,
    ALIGNMENT_GATE_ACTION_REPORT_ONLY_BLOCK,
    SIGNAL_DECIDER_ALIGNMENT_WAIT_CONFLICT_BLOCK,
    SIGNAL_DECIDER_ALIGNMENT_WAIT_CONFLICT_OVERRIDE,
    apply_alignment_quantity_enforcement,
    build_alignment_override_evidence,
    build_signal_decider_alignment_context,
    build_signal_decider_alignment_gate,
    normalize_alignment_gate_mode,
)


DECIDER_REPLAY_SCHEMA_VERSION = 'decider_replay_v1'
WAIT_CONFLICT_GATE_REPLAY_SCHEMA_VERSION = 'short_term_wait_conflict_gate_replay_v1'
DECIDER_REPLAY_RULE_CONFIG_SCHEMA_VERSION = 'decider_replay_rule_config_v1'
SUPPORTED_REPLAY_AGENT_NAMES: tuple[str, ...] = (
    SIGNAL_DECIDER_AGENT_NAME,
    SHORT_TERM_DECIDER_AGENT_NAME,
)
DEFAULT_REPLAY_HORIZON_DAYS = 5
DEFAULT_REPLAY_NOTIONAL_KRW = Decimal('1000000')
MIN_PROMOTION_FEE_BPS = Decimal('10')
MIN_PROMOTION_SLIPPAGE_BPS = Decimal('5')
DEFAULT_TOP_EXAMPLES_LIMIT = 10
DEFAULT_WAIT_CONFLICT_COMPLETED_WEEKS = 4
MIN_WAIT_CONFLICT_PROBE_QUANTITY = 1
MAX_WAIT_CONFLICT_PROBE_QUANTITY = 100


@dataclass(frozen=True)
class DeciderReplayCostAssumptions:
    fee_bps: Decimal = Decimal('0')
    slippage_bps: Decimal = Decimal('0')
    notional_krw: Decimal = DEFAULT_REPLAY_NOTIONAL_KRW


@dataclass(frozen=True)
class DeciderReplayTrade:
    replay_trade_id: str
    snapshot_id: int
    agent_name: str
    symbol: str
    market_date: date
    policy_version: str
    baseline_gross_pnl_krw: Decimal
    baseline_return_pct: float | None
    baseline_notional_krw: Decimal
    outcome_source: str
    point_in_time: dict[str, Any]
    decision_timestamp: datetime | None = None
    alignment_gate: dict[str, Any] | None = None
    outcome_resolved: bool = True
    lifecycle_status: str | None = None
    entry_filled_quantity: Decimal | None = None
    closed_quantity: Decimal | None = None
    open_quantity: Decimal | None = None


@dataclass(frozen=True)
class DeciderReplayDataset:
    trades: tuple[DeciderReplayTrade, ...]
    source_decision_count: int
    excluded_reason_counts: dict[str, int]
    cohort_basis: str
    diagnostics: dict[str, Any]

    def __iter__(self):
        return iter(self.trades)

    def __len__(self) -> int:
        return len(self.trades)

    def __getitem__(self, index: int) -> DeciderReplayTrade:
        return self.trades[index]


@dataclass(frozen=True)
class PredicateResult:
    matched: bool
    reason: str | None = None


@dataclass(frozen=True)
class RuleEvaluationResult:
    skipped: bool
    reasons: tuple[str, ...]


def _replay_profitability_metadata(
    cost_assumptions: DeciderReplayCostAssumptions,
    *,
    cohort_basis: str,
) -> dict[str, Any]:
    below_minimum_cost_inputs = [
        name
        for name, value, minimum in (
            ('fee_bps', cost_assumptions.fee_bps, MIN_PROMOTION_FEE_BPS),
            ('slippage_bps', cost_assumptions.slippage_bps, MIN_PROMOTION_SLIPPAGE_BPS),
        )
        if value < minimum
    ]
    promotion_eligible = not below_minimum_cost_inputs
    missing_cost_reason = None
    if cost_assumptions.fee_bps == 0 and cost_assumptions.slippage_bps == 0:
        missing_cost_reason = 'zero_fee_and_slippage_assumptions'
    elif len(below_minimum_cost_inputs) == 2:
        missing_cost_reason = 'below_minimum_fee_and_slippage_assumptions'
    elif below_minimum_cost_inputs:
        missing_cost_reason = f'below_minimum_{below_minimum_cost_inputs[0]}_assumption'
    return {
        'profitability_basis': (
            'gross_only'
            if cost_assumptions.fee_bps == 0 and cost_assumptions.slippage_bps == 0
            else 'estimated_net'
        ),
        'promotion_metrics_basis': 'estimated_net',
        'promotion_cohort_basis': cohort_basis,
        'promotion_cost_gate': {
            'eligible': promotion_eligible,
            'status': 'passed' if promotion_eligible else 'ineligible',
            'reason': missing_cost_reason,
            'minimum_assumptions': {
                'fee_bps': _to_json_number(MIN_PROMOTION_FEE_BPS, places='0.000001'),
                'slippage_bps': _to_json_number(
                    MIN_PROMOTION_SLIPPAGE_BPS,
                    places='0.000001',
                ),
            },
            'effective_assumptions': {
                'fee_bps': _to_json_number(cost_assumptions.fee_bps, places='0.000001'),
                'slippage_bps': _to_json_number(
                    cost_assumptions.slippage_bps,
                    places='0.000001',
                ),
            },
        },
    }


def _normalize_market_date(value: str | date) -> date:
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value))


def _completed_week_windows(
    *,
    as_of_date_kst: str | date | None,
    completed_weeks: int,
) -> list[tuple[date, date]]:
    as_of = (
        _normalize_market_date(as_of_date_kst)
        if as_of_date_kst is not None
        else datetime.now(ZoneInfo('Asia/Seoul')).date()
    )
    week_count = max(DEFAULT_WAIT_CONFLICT_COMPLETED_WEEKS, int(completed_weeks))
    current_monday = as_of - timedelta(days=as_of.weekday())
    latest_completed_friday = (
        current_monday + timedelta(days=4)
        if as_of.weekday() >= 5
        else current_monday - timedelta(days=3)
    )
    windows: list[tuple[date, date]] = []
    for offset in reversed(range(week_count)):
        week_end = latest_completed_friday - timedelta(weeks=offset)
        windows.append((week_end - timedelta(days=4), week_end))
    return windows


def _explicit_week_windows(market_date_from: date, market_date_to: date) -> list[tuple[date, date]]:
    cursor = market_date_from - timedelta(days=market_date_from.weekday())
    windows: list[tuple[date, date]] = []
    while cursor <= market_date_to:
        start = max(cursor, market_date_from)
        end = min(cursor + timedelta(days=4), market_date_to)
        if start <= end:
            windows.append((start, end))
        cursor += timedelta(weeks=1)
    return windows


def _candidate_action_for_mode(mode: str) -> str:
    resolved_mode = normalize_alignment_gate_mode(mode)
    if resolved_mode == 'hard_block':
        return ALIGNMENT_GATE_ACTION_BLOCK
    if resolved_mode == 'downsize_probe':
        return ALIGNMENT_GATE_ACTION_DOWNSIZE_PROBE
    return ALIGNMENT_GATE_ACTION_REPORT_ONLY_BLOCK


def _replay_alignment_gate(
    persisted_gate: dict[str, Any] | None,
    *,
    gate_mode: str,
) -> dict[str, Any]:
    if not isinstance(persisted_gate, dict) or not persisted_gate:
        return {
            'schema_version': 'signal_decider_alignment_gate_v1',
            'enforcement_mode': normalize_alignment_gate_mode(gate_mode),
            'signal_decider_alignment': 'missing_alignment_evidence',
            'alignment_gate_action': _candidate_action_for_mode(gate_mode),
            'alignment_gate_reason': 'Persisted point-in-time alignment evidence is missing.',
            'override': {'allowed': False, 'reason': 'missing_alignment_evidence'},
            'evidence_status': 'missing',
        }
    gate = dict(persisted_gate)
    gate['enforcement_mode'] = normalize_alignment_gate_mode(gate_mode)
    gate['evidence_status'] = 'persisted'
    alignment = str(gate.get('signal_decider_alignment') or '').strip().lower()
    override = gate.get('override') if isinstance(gate.get('override'), dict) else {}
    if alignment == SIGNAL_DECIDER_ALIGNMENT_WAIT_CONFLICT_OVERRIDE:
        override = build_alignment_override_evidence(
            override.get('metrics') if isinstance(override.get('metrics'), dict) else None
        )
        gate['override'] = override
        if not override['allowed']:
            gate['persisted_signal_decider_alignment'] = alignment
            alignment = SIGNAL_DECIDER_ALIGNMENT_WAIT_CONFLICT_BLOCK
            gate['signal_decider_alignment'] = alignment
            gate['alignment_gate_reason'] = (
                'Persisted WAIT-conflict override evidence does not satisfy the current stronger override threshold.'
            )
    valid_override = alignment == SIGNAL_DECIDER_ALIGNMENT_WAIT_CONFLICT_OVERRIDE and override.get('allowed') is True
    if valid_override:
        gate['alignment_gate_action'] = 'allow_by_override'
    elif alignment != 'aligned':
        gate['alignment_gate_action'] = _candidate_action_for_mode(gate_mode)
    return gate


def _normalize_agent_name(value: str) -> str:
    normalized = str(value or '').strip().lower()
    if normalized not in SUPPORTED_REPLAY_AGENT_NAMES:
        raise ValueError(
            f'agent_name must be one of: {", ".join(SUPPORTED_REPLAY_AGENT_NAMES)}'
        )
    return normalized


def _normalize_symbols(symbols: Sequence[str] | str | None) -> list[str]:
    if symbols is None:
        return []
    raw_values = [symbols] if isinstance(symbols, str) else list(symbols)
    normalized: list[str] = []
    seen: set[str] = set()
    for raw_value in raw_values:
        for piece in str(raw_value or '').split(','):
            symbol = piece.strip().upper()
            if not symbol or symbol in seen:
                continue
            seen.add(symbol)
            normalized.append(symbol)
    return normalized


def _normalize_policy_versions(
    policy_versions: Sequence[str] | str | None,
) -> list[str]:
    if policy_versions is None:
        return []
    raw_values = [policy_versions] if isinstance(policy_versions, str) else list(policy_versions)
    normalized: list[str] = []
    seen: set[str] = set()
    for raw_value in raw_values:
        for piece in str(raw_value or '').split(','):
            policy_version = piece.strip()
            if not policy_version or policy_version in seen:
                continue
            seen.add(policy_version)
            normalized.append(policy_version)
    return normalized


def _safe_decimal(value: Any) -> Decimal | None:
    if value is None or value == '' or isinstance(value, bool):
        return None
    if isinstance(value, Decimal):
        return value if value.is_finite() else None
    try:
        resolved = Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None
    return resolved if resolved.is_finite() else None


def _safe_decimal_or_zero(value: Any) -> Decimal:
    return _safe_decimal(value) or Decimal('0')


def _normalize_non_negative_decimal(value: Any, *, name: str) -> Decimal:
    resolved = _safe_decimal(value)
    if resolved is None:
        raise ValueError(f'{name} must be numeric')
    if resolved < 0:
        raise ValueError(f'{name} must be >= 0')
    return resolved


def _normalize_probe_quantity(value: Any) -> int:
    resolved = _safe_decimal(value)
    if (
        resolved is None
        or not resolved.is_finite()
        or resolved != resolved.to_integral_value()
        or resolved < MIN_WAIT_CONFLICT_PROBE_QUANTITY
        or resolved > MAX_WAIT_CONFLICT_PROBE_QUANTITY
    ):
        raise ValueError('probe_quantity must be an integer from 1 through 100')
    return int(resolved)


def _to_float(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, Decimal):
        return float(value) if value.is_finite() else None
    if isinstance(value, (int, float)):
        resolved = float(value)
        return resolved if math.isfinite(resolved) else None
    try:
        resolved = float(str(value).strip())
    except (TypeError, ValueError):
        return None
    return resolved if math.isfinite(resolved) else None


def _to_json_number(value: Decimal | float | int | None, *, places: str = '0.01') -> int | float | None:
    if value is None:
        return None
    decimal_value = value if isinstance(value, Decimal) else Decimal(str(value))
    quantized = decimal_value.quantize(Decimal(places), rounding=ROUND_HALF_UP)
    if quantized == quantized.to_integral():
        return int(quantized)
    return float(quantized)


def _to_ratio_number(value: float | Decimal | None) -> float | None:
    resolved = _to_float(value)
    if resolved is None:
        return None
    return round(resolved, 6)


def _maybe_dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _maybe_list(value: Any) -> list[Any]:
    if value is None:
        return []
    if isinstance(value, list):
        return value
    return [value]


def _normalize_text(value: Any) -> str | None:
    if value is None:
        return None
    normalized = str(value).strip()
    return normalized or None


def _dedupe_text(items: Sequence[Any]) -> list[str]:
    deduped: list[str] = []
    seen: set[str] = set()
    for item in items:
        text = _normalize_text(item)
        if text is None or text in seen:
            continue
        seen.add(text)
        deduped.append(text)
    return deduped


def _get_path(payload: Any, path: str | None) -> Any:
    if not path:
        return None
    current = payload
    for piece in str(path).split('.'):
        if isinstance(current, dict):
            current = current.get(piece)
            continue
        if isinstance(current, list):
            try:
                current = current[int(piece)]
            except (ValueError, IndexError):
                return None
            continue
        return None
    return current


def _first_path_value(payload: dict[str, Any], paths: Sequence[str]) -> tuple[Any, str | None]:
    for path in paths:
        value = _get_path(payload, path)
        if value is not None:
            return value, path
    return None, None


def _first_number(payload: dict[str, Any], paths: Sequence[str]) -> tuple[float | None, str | None]:
    value, path = _first_path_value(payload, paths)
    return _to_float(value), path


def _factor_score_bucket(score: float | None) -> str | None:
    if score is None:
        return None
    if score >= 0.80:
        return 'high'
    if score >= 0.55:
        return 'medium'
    return 'low'


def _liquidity_bucket(liquidity_context: dict[str, Any]) -> str | None:
    ranking = _maybe_dict(liquidity_context.get('ranking'))
    percentile = _to_float(ranking.get('liquidity_percentile'))
    passes_liquidity_floor = ranking.get('passes_liquidity_floor')
    status = _normalize_text(liquidity_context.get('status')) or _normalize_text(ranking.get('status'))
    if passes_liquidity_floor is False:
        return 'fails_liquidity_floor'
    if percentile is not None:
        if percentile >= 0.80:
            return 'high_liquidity'
        if percentile >= 0.50:
            return 'medium_liquidity'
        return 'lower_liquidity'
    if status:
        return f'{status}_context'
    return None


def _extract_risk_flags(*, response_payload: dict[str, Any], traceability_payload: dict[str, Any], warnings: Any, risks: Any) -> list[str]:
    flags: list[Any] = []
    flags.extend(_maybe_list(response_payload.get('risk_flags')))
    flags.extend(_maybe_list(traceability_payload.get('risk_flags')))
    tactical_context = _maybe_dict(traceability_payload.get('tactical_context'))
    flags.extend(_maybe_list(tactical_context.get('risk_flags')))
    flags.extend(_maybe_list(response_payload.get('data_quality_warnings')))
    flags.extend(_maybe_list(warnings))
    flags.extend(_maybe_list(risks))
    return _dedupe_text(flags)


def _build_payload_context(row: SignalDeciderDecisionSnapshot) -> dict[str, Any]:
    request_payload = _maybe_dict(row.request_payload)
    response_payload = _maybe_dict(row.response_payload)
    traceability_payload = _maybe_dict(row.traceability_payload)
    input_feature_snapshot_payload = _maybe_dict(row.input_feature_snapshot_payload)
    symbol_day_context = _maybe_dict(request_payload.get('symbol_day_context'))
    return {
        'request_payload': request_payload,
        'response_payload': response_payload,
        'traceability_payload': traceability_payload,
        'input_feature_snapshot_payload': input_feature_snapshot_payload,
        'symbol_day_context': symbol_day_context,
    }


def _build_point_in_time_context(row: SignalDeciderDecisionSnapshot) -> dict[str, Any]:
    payload_context = _build_payload_context(row)
    request_payload = payload_context['request_payload']
    response_payload = payload_context['response_payload']
    traceability_payload = payload_context['traceability_payload']
    input_feature_snapshot_payload = payload_context['input_feature_snapshot_payload']
    symbol_day_context = payload_context['symbol_day_context']
    tactical_context = _maybe_dict(traceability_payload.get('tactical_context'))

    trigger_price, trigger_source = _first_number(
        {
            **payload_context,
            'tactical_context': tactical_context,
        },
        (
            'response_payload.trigger_price',
            'traceability_payload.trigger_price',
            'tactical_context.trigger_price',
            'request_payload.trigger_price',
        ),
    )
    chase_limit_price, chase_limit_source = _first_number(
        {
            **payload_context,
            'tactical_context': tactical_context,
        },
        (
            'response_payload.chase_limit_price',
            'traceability_payload.chase_limit_price',
            'tactical_context.chase_limit_price',
            'request_payload.chase_limit_price',
        ),
    )
    reference_price, reference_price_source = _first_number(
        payload_context,
        (
            'response_payload.current_price',
            'response_payload.reference_price',
            'request_payload.current_price',
            'request_payload.reference_price',
            'request_payload.pre_decider_enrichment_context.quote_orderbook_snapshot.current_price',
            'request_payload.pre_decider_enrichment_context.quote_orderbook_snapshot.last_price',
            'symbol_day_context.price_context.current_price',
            'symbol_day_context.price_context.close_price',
            'symbol_day_context.quote_context.current_price',
            'symbol_day_context.quote_context.price',
            'input_feature_snapshot_payload.price_context.current_price',
            'input_feature_snapshot_payload.price_context.close_price',
            'input_feature_snapshot_payload.technical.close_price',
        ),
    )
    trigger_price_drift_pct = None
    if trigger_price is not None and trigger_price > 0 and reference_price is not None:
        trigger_price_drift_pct = round((reference_price - trigger_price) / trigger_price, 6)

    factor_summary = _maybe_dict(request_payload.get('factor_summary')) or _maybe_dict(
        symbol_day_context.get('factor_summary')
    )
    factor_scores = _maybe_dict(factor_summary.get('factor_scores')) or _maybe_dict(
        request_payload.get('scores')
    ) or _maybe_dict(input_feature_snapshot_payload.get('scores'))
    factor_score_buckets: dict[str, str] = {}
    for factor_name, raw_value in factor_scores.items():
        factor_payload = _maybe_dict(raw_value)
        score = _to_float(factor_payload.get('score')) if factor_payload else _to_float(raw_value)
        status = _normalize_text(factor_payload.get('status')) if factor_payload else None
        bucket = _factor_score_bucket(score)
        if status and bucket:
            factor_score_buckets[str(factor_name)] = f'{status}:{bucket}'
        elif bucket:
            factor_score_buckets[str(factor_name)] = bucket
        elif status:
            factor_score_buckets[str(factor_name)] = status

    liquidity_context = _maybe_dict(request_payload.get('liquidity_context')) or _maybe_dict(
        symbol_day_context.get('liquidity_context')
    ) or _maybe_dict(input_feature_snapshot_payload.get('liquidity_context'))
    risk_flags = _extract_risk_flags(
        response_payload=response_payload,
        traceability_payload=traceability_payload,
        warnings=row.data_quality_warnings_payload,
        risks=row.decision_risks_payload,
    )
    return {
        'snapshot_id': row.id,
        'agent_name': row.agent_name,
        'symbol': row.symbol,
        'market_date': row.market_date.isoformat(),
        'decision_action': row.decision_action,
        'policy_version': row.policy_version,
        'confidence_score_10': _to_float(row.confidence_score_10),
        'conviction_score_10': _to_float(row.conviction_score_10),
        'trigger_price': trigger_price,
        'trigger_price_source': trigger_source,
        'chase_limit_price': chase_limit_price,
        'chase_limit_price_source': chase_limit_source,
        'reference_price': reference_price,
        'reference_price_source': reference_price_source,
        'trigger_price_drift_pct': trigger_price_drift_pct,
        'risk_flags': risk_flags,
        'data_quality_warnings': _dedupe_text(_maybe_list(row.data_quality_warnings_payload)),
        'liquidity_bucket': _liquidity_bucket(liquidity_context),
        'factor_score_buckets': factor_score_buckets,
        'payload_context': payload_context,
    }


def _decimal_values_match(
    left: Decimal,
    right: Decimal,
    *,
    relative_tolerance: Decimal = Decimal('0.001'),
    absolute_tolerance: Decimal = Decimal('0.00000001'),
) -> bool:
    tolerance = max(absolute_tolerance, max(abs(left), abs(right)) * relative_tolerance)
    return abs(left - right) <= tolerance


def _actual_outcome_exclusion_reason(
    outcome: SignalDeciderActualExecutionOutcome,
) -> str | None:
    if str(outcome.lifecycle_status or '').strip().lower() != 'closed':
        return 'actual_lifecycle_not_closed'

    open_quantity = _safe_decimal(outcome.open_quantity)
    if open_quantity is None:
        return 'actual_open_quantity_missing_or_non_finite'
    if open_quantity != 0:
        return 'actual_position_still_open'

    entry_quantity = _safe_decimal(outcome.entry_filled_quantity)
    closed_quantity = _safe_decimal(outcome.closed_quantity)
    exit_quantity = _safe_decimal(outcome.exit_filled_quantity)
    if entry_quantity is None or entry_quantity <= 0:
        return 'actual_entry_quantity_missing_or_non_finite'
    if closed_quantity is None or closed_quantity <= 0:
        return 'actual_closed_quantity_missing_or_non_finite'
    if not _decimal_values_match(entry_quantity, closed_quantity):
        return 'actual_closed_quantity_mismatch'
    if exit_quantity is None or not _decimal_values_match(exit_quantity, closed_quantity):
        return 'actual_exit_quantity_mismatch'

    entry_notional = _safe_decimal(outcome.entry_notional_krw)
    exit_notional = _safe_decimal(outcome.exit_notional_krw)
    entry_price = _safe_decimal(outcome.entry_average_fill_price)
    exit_price = _safe_decimal(outcome.exit_average_fill_price)
    if entry_notional is None or entry_notional <= 0:
        return 'actual_entry_notional_missing_or_non_finite'
    if exit_notional is None or exit_notional <= 0:
        return 'actual_exit_notional_missing_or_non_finite'
    if entry_price is None or entry_price <= 0:
        return 'actual_entry_price_missing_or_non_finite'
    if exit_price is None or exit_price <= 0:
        return 'actual_exit_price_missing_or_non_finite'
    if not _decimal_values_match(entry_notional, entry_quantity * entry_price):
        return 'actual_entry_notional_mismatch'
    if not _decimal_values_match(exit_notional, closed_quantity * exit_price):
        return 'actual_exit_notional_mismatch'

    gross_pnl = _safe_decimal(outcome.realized_pnl_krw)
    realized_return = _safe_decimal(outcome.realized_return_pct)
    if gross_pnl is None and realized_return is None:
        return 'actual_economic_result_missing_or_non_finite'
    return None


def _actual_outcome_to_trade(
    *,
    snapshot: SignalDeciderDecisionSnapshot,
    outcome: SignalDeciderActualExecutionOutcome,
    cost_assumptions: DeciderReplayCostAssumptions,
) -> DeciderReplayTrade:
    exclusion_reason = _actual_outcome_exclusion_reason(outcome)
    if exclusion_reason is not None:
        raise ValueError(f'actual outcome is not eligible for economic replay: {exclusion_reason}')
    entry_notional = _safe_decimal(outcome.entry_notional_krw)
    assert entry_notional is not None
    notional = entry_notional
    gross_pnl = _safe_decimal(outcome.realized_pnl_krw)
    realized_return = _to_float(outcome.realized_return_pct)
    if gross_pnl is None and realized_return is not None:
        gross_pnl = Decimal(str(realized_return)) * notional
    assert gross_pnl is not None
    return DeciderReplayTrade(
        replay_trade_id=f'actual:{outcome.id}',
        snapshot_id=int(snapshot.id),
        agent_name=snapshot.agent_name,
        symbol=snapshot.symbol,
        market_date=snapshot.market_date,
        policy_version=snapshot.policy_version,
        baseline_gross_pnl_krw=gross_pnl,
        baseline_return_pct=realized_return,
        baseline_notional_krw=notional,
        outcome_source='actual_execution_outcome',
        point_in_time=_build_point_in_time_context(snapshot),
        decision_timestamp=snapshot.feedback_received_at or snapshot.created_at,
    )


def _actual_short_term_outcome_to_wait_conflict_trade(
    *,
    snapshot: SignalDeciderDecisionSnapshot,
    plan_row: SignalDeciderExecutionPlanItem,
    outcome: SignalDeciderActualExecutionOutcome,
    signal_decider_context: dict[str, Any] | None,
    cost_assumptions: DeciderReplayCostAssumptions,
    gate_mode: str,
    probe_quantity: int,
    alignment_context_source: str,
) -> DeciderReplayTrade:
    exclusion_reason = _actual_outcome_exclusion_reason(outcome)
    validation = plan_row.validation_payload if isinstance(plan_row.validation_payload, dict) else {}
    traceability = plan_row.traceability_payload if isinstance(plan_row.traceability_payload, dict) else {}
    persisted_gate = _plan_alignment_gate(plan_row)
    if persisted_gate:
        alignment_gate = _replay_alignment_gate(persisted_gate, gate_mode=gate_mode)
    elif signal_decider_context is not None:
        short_term = validation.get('short_term') if isinstance(validation.get('short_term'), dict) else {}
        response_payload = snapshot.response_payload if isinstance(snapshot.response_payload, dict) else {}
        tactical_action = (
            traceability.get('tactical_action')
            or short_term.get('tactical_action')
            or response_payload.get('tactical_action')
        )
        rebuilt_gate = build_signal_decider_alignment_gate(
            signal_decider_context=signal_decider_context,
            short_term_entry_quality=(
                validation.get('short_term_entry_quality')
                if isinstance(validation.get('short_term_entry_quality'), dict)
                else None
            ),
            tactical_action=(
                str(tactical_action).strip().lower()
                if tactical_action is not None
                else None
            ),
            mode=gate_mode,
        )
        alignment_gate = _replay_alignment_gate(rebuilt_gate, gate_mode=gate_mode)
        alignment_gate['evidence_status'] = 'as_of_plan'
    else:
        alignment_gate = _replay_alignment_gate(None, gate_mode=gate_mode)
    entry_notional = _safe_decimal(outcome.entry_notional_krw)
    if entry_notional is None or entry_notional <= 0:
        entry_quantity = _safe_decimal(outcome.entry_filled_quantity)
        entry_price = _safe_decimal(outcome.entry_average_fill_price)
        entry_notional = (
            entry_quantity * entry_price
            if entry_quantity is not None
            and entry_quantity > 0
            and entry_price is not None
            and entry_price > 0
            else None
        )
    notional = (
        entry_notional
        if entry_notional is not None and entry_notional > 0
        else cost_assumptions.notional_krw
    )
    gross_pnl = _safe_decimal(outcome.realized_pnl_krw)
    realized_return = _to_float(outcome.realized_return_pct)
    if gross_pnl is None and realized_return is not None:
        gross_pnl = Decimal(str(realized_return)) * notional
    entry_filled_quantity = _safe_decimal(outcome.entry_filled_quantity)
    alignment_gate = apply_alignment_quantity_enforcement(
        alignment_gate,
        requested_quantity=entry_filled_quantity,
        probe_quantity=probe_quantity,
    )
    closed_quantity = _safe_decimal(outcome.closed_quantity)
    open_quantity = _safe_decimal(outcome.open_quantity)
    outcome_resolved = exclusion_reason is None
    point_in_time = _build_point_in_time_context(snapshot)
    point_in_time.update(
        {
            'market_date': plan_row.source_market_date.isoformat(),
            'source_market_date': plan_row.source_market_date.isoformat(),
            'target_trade_date': outcome.target_trade_date.isoformat(),
            'entry_plan_item_id': plan_row.id,
            'actual_execution_outcome_id': outcome.id,
            'lifecycle_status': outcome.lifecycle_status,
            'entry_filled_quantity': _to_json_number(entry_filled_quantity),
            'closed_quantity': _to_json_number(closed_quantity),
            'open_quantity': _to_json_number(open_quantity),
            'realized_outcome_available': outcome_resolved,
            'economic_replay_exclusion_reason': exclusion_reason,
            'signal_decider_alignment': alignment_gate.get('signal_decider_alignment'),
            'alignment_gate_action': alignment_gate.get('alignment_gate_action'),
            'alignment_gate_reason': alignment_gate.get('alignment_gate_reason'),
            'alignment_signal_decider_decision_snapshot_id': alignment_gate.get(
                'signal_decider_decision_snapshot_id'
            ),
            'alignment_gate': alignment_gate,
            'alignment_context_source': alignment_context_source,
        }
    )
    return DeciderReplayTrade(
        replay_trade_id=f'actual_short_term:{outcome.id}',
        snapshot_id=int(snapshot.id),
        agent_name=snapshot.agent_name,
        symbol=snapshot.symbol,
        market_date=plan_row.source_market_date,
        policy_version=snapshot.policy_version,
        baseline_gross_pnl_krw=gross_pnl if gross_pnl is not None else Decimal('0'),
        baseline_return_pct=realized_return,
        baseline_notional_krw=notional,
        outcome_source='actual_short_term_execution_outcome',
        point_in_time=point_in_time,
        decision_timestamp=snapshot.feedback_received_at or snapshot.created_at,
        alignment_gate=alignment_gate,
        outcome_resolved=outcome_resolved,
        lifecycle_status=outcome.lifecycle_status,
        entry_filled_quantity=entry_filled_quantity,
        closed_quantity=closed_quantity,
        open_quantity=open_quantity,
    )


def _plan_alignment_gate(plan_row: SignalDeciderExecutionPlanItem) -> dict[str, Any]:
    validation = plan_row.validation_payload if isinstance(plan_row.validation_payload, dict) else {}
    traceability = plan_row.traceability_payload if isinstance(plan_row.traceability_payload, dict) else {}
    short_term = validation.get('short_term') if isinstance(validation.get('short_term'), dict) else {}
    for candidate in (
        traceability.get('alignment_gate'),
        validation.get('alignment_gate'),
        short_term.get('alignment_gate'),
    ):
        if isinstance(candidate, dict) and candidate:
            return candidate
    return {}


def _plan_alignment_snapshot_id(plan_row: SignalDeciderExecutionPlanItem) -> int | None:
    validation = plan_row.validation_payload if isinstance(plan_row.validation_payload, dict) else {}
    traceability = plan_row.traceability_payload if isinstance(plan_row.traceability_payload, dict) else {}
    persisted_gate = _plan_alignment_gate(plan_row)
    for value in (
        persisted_gate.get('signal_decider_decision_snapshot_id'),
        persisted_gate.get('decision_snapshot_id'),
        traceability.get('alignment_signal_decider_decision_snapshot_id'),
        validation.get('alignment_signal_decider_decision_snapshot_id'),
    ):
        resolved = _safe_decimal(value)
        if resolved is not None and resolved == resolved.to_integral() and resolved > 0:
            return int(resolved)
    return None


def _snapshot_timestamp(snapshot: SignalDeciderDecisionSnapshot) -> datetime | None:
    return snapshot.feedback_received_at or snapshot.created_at


def _timestamp_not_after(value: datetime | None, cutoff: datetime | None) -> bool:
    if value is None or cutoff is None:
        return False
    if value.tzinfo is None and cutoff.tzinfo is not None:
        cutoff = cutoff.replace(tzinfo=None)
    elif value.tzinfo is not None and cutoff.tzinfo is None:
        value = value.replace(tzinfo=None)
    return value <= cutoff


def _realized_outcome_to_trade(
    *,
    snapshot: SignalDeciderDecisionSnapshot,
    outcome: SignalDeciderRealizedOutcome,
    cost_assumptions: DeciderReplayCostAssumptions,
) -> DeciderReplayTrade | None:
    if str(outcome.outcome_status or '').strip().lower() != 'complete':
        return None
    return_pct = _to_float(outcome.close_return_pct)
    if return_pct is None:
        return None
    gross_pnl = Decimal(str(return_pct)) * cost_assumptions.notional_krw
    return DeciderReplayTrade(
        replay_trade_id=f'realized:{outcome.id}',
        snapshot_id=int(snapshot.id),
        agent_name=snapshot.agent_name,
        symbol=snapshot.symbol,
        market_date=snapshot.market_date,
        policy_version=snapshot.policy_version,
        baseline_gross_pnl_krw=gross_pnl,
        baseline_return_pct=return_pct,
        baseline_notional_krw=cost_assumptions.notional_krw,
        outcome_source='signal_decider_realized_outcome',
        point_in_time=_build_point_in_time_context(snapshot),
        decision_timestamp=snapshot.feedback_received_at or snapshot.created_at,
    )


def _post_decision_eval_to_trade(
    *,
    snapshot: SignalDeciderDecisionSnapshot,
    evaluation: DeciderPostDecisionEvaluation,
    cost_assumptions: DeciderReplayCostAssumptions,
) -> DeciderReplayTrade | None:
    entry_price = _to_float(evaluation.entry_price)
    return_pct = None
    if entry_price is not None and entry_price > 0:
        target_price = _to_float(evaluation.target_price)
        stop_loss_price = _to_float(evaluation.stop_loss_price)
        if evaluation.outcome_label == 'target_hit_before_stop' and target_price is not None:
            return_pct = (target_price - entry_price) / entry_price
        elif evaluation.outcome_label == 'stop_hit_before_target' and stop_loss_price is not None:
            return_pct = (stop_loss_price - entry_price) / entry_price
        elif evaluation.outcome_label == 'neither_hit_within_5d':
            metadata = _maybe_dict(evaluation.metadata_payload)
            observed_price_path = _maybe_list(metadata.get('observed_price_path'))
            final_close = next(
                (
                    _to_float(_maybe_dict(item).get('close_price'))
                    for item in reversed(observed_price_path)
                    if _to_float(_maybe_dict(item).get('close_price')) is not None
                ),
                None,
            )
            if final_close is not None and final_close > 0:
                return_pct = (final_close - entry_price) / entry_price
    if return_pct is None or not math.isfinite(return_pct):
        return None
    gross_pnl = Decimal(str(return_pct)) * cost_assumptions.notional_krw
    return DeciderReplayTrade(
        replay_trade_id=f'post_decision_eval:{evaluation.id}',
        snapshot_id=int(snapshot.id),
        agent_name=snapshot.agent_name,
        symbol=snapshot.symbol,
        market_date=snapshot.market_date,
        policy_version=snapshot.policy_version,
        baseline_gross_pnl_krw=gross_pnl,
        baseline_return_pct=_to_ratio_number(return_pct),
        baseline_notional_krw=cost_assumptions.notional_krw,
        outcome_source='decider_post_decision_evaluation',
        point_in_time=_build_point_in_time_context(snapshot),
        decision_timestamp=snapshot.feedback_received_at or snapshot.created_at,
    )


class DeciderReplayRepository:
    def list_replay_trades(
        self,
        *,
        market_date_from: date,
        market_date_to: date,
        agent_name: str,
        symbols: Sequence[str] | None,
        policy_versions: Sequence[str] | None = None,
        horizon_days: int,
        cost_assumptions: DeciderReplayCostAssumptions,
    ) -> DeciderReplayDataset:
        with session_scope() as session:
            allowed_policy_versions = set(policy_versions or ())
            stmt = select(SignalDeciderDecisionSnapshot).where(
                SignalDeciderDecisionSnapshot.market_date >= market_date_from,
                SignalDeciderDecisionSnapshot.market_date <= market_date_to,
                SignalDeciderDecisionSnapshot.agent_name == agent_name,
            )
            if symbols:
                stmt = stmt.where(SignalDeciderDecisionSnapshot.symbol.in_(list(symbols)))
            snapshot_rows = list(
                session.execute(
                    stmt.order_by(
                        SignalDeciderDecisionSnapshot.market_date.asc(),
                        SignalDeciderDecisionSnapshot.created_at.asc(),
                        SignalDeciderDecisionSnapshot.id.asc(),
                    )
                ).scalars().all()
            )
            latest_snapshot_by_feedback: dict[int, SignalDeciderDecisionSnapshot] = {}
            for row in snapshot_rows:
                if row.feedback_id is None:
                    continue
                feedback_id = int(row.feedback_id)
                current = latest_snapshot_by_feedback.get(feedback_id)
                if current is None or (row.created_at, int(row.id)) > (
                    current.created_at,
                    int(current.id),
                ):
                    latest_snapshot_by_feedback[feedback_id] = row
            latest_snapshot_ids = {
                int(row.id) for row in latest_snapshot_by_feedback.values()
            }
            candidate_snapshot_rows = [
                row
                for row in snapshot_rows
                if (
                    row.feedback_id is None
                    or int(row.id) in latest_snapshot_ids
                )
                and (
                    not allowed_policy_versions
                    or row.policy_version in allowed_policy_versions
                )
                and row.decision_action == 'buy'
            ]
            source_decision_count = len(candidate_snapshot_rows)
            (
                candidate_snapshot_rows,
                model_identity_exclusions,
                model_identity_cohort,
            ) = select_model_identity_cohort_rows(
                candidate_snapshot_rows,
                role=agent_name,
                expected_model=expected_decider_model(agent_name, settings_obj=settings),
            )
            canonical_action_exclusions: Counter[str] = Counter()
            snapshot_rows = []
            for row in candidate_snapshot_rows:
                canonical_action = canonical_evaluation_action(
                    agent_name=row.agent_name,
                    persisted_action=row.decision_action,
                    response_payload=row.response_payload,
                )
                if canonical_action == 'buy':
                    snapshot_rows.append(row)
                elif canonical_action is None and row.agent_name == SHORT_TERM_DECIDER_AGENT_NAME:
                    canonical_action_exclusions['invalid_short_term_tactical_action'] += 1
                else:
                    canonical_action_exclusions['short_term_tactical_action_not_buy'] += 1
            snapshot_ids = [int(row.id) for row in snapshot_rows]
            source_snapshot_ids = [int(row.id) for row in candidate_snapshot_rows]
            actual_rows = list(
                session.execute(
                    select(SignalDeciderActualExecutionOutcome)
                    .where(
                        SignalDeciderActualExecutionOutcome.decision_snapshot_id.in_(
                            source_snapshot_ids
                        )
                    )
                    .order_by(
                        SignalDeciderActualExecutionOutcome.market_date.asc(),
                        SignalDeciderActualExecutionOutcome.target_trade_date.asc(),
                        SignalDeciderActualExecutionOutcome.id.asc(),
                    )
                ).scalars().all()
            )
            realized_rows = list(
                session.execute(
                    select(SignalDeciderRealizedOutcome)
                    .where(
                        SignalDeciderRealizedOutcome.decision_snapshot_id.in_(snapshot_ids),
                        SignalDeciderRealizedOutcome.horizon_days == int(horizon_days),
                    )
                    .order_by(SignalDeciderRealizedOutcome.id.asc())
                ).scalars().all()
            )
            post_decision_rows = list(
                session.execute(
                    select(DeciderPostDecisionEvaluation)
                    .where(
                        DeciderPostDecisionEvaluation.decision_snapshot_id.in_(snapshot_ids),
                        DeciderPostDecisionEvaluation.window_days == int(horizon_days),
                    )
                    .order_by(DeciderPostDecisionEvaluation.id.asc())
                ).scalars().all()
            )

            actual_excluded_reason_counts: Counter[str] = Counter()
            actual_eligible_count = 0
            for row in actual_rows:
                exclusion_reason = _actual_outcome_exclusion_reason(row)
                if exclusion_reason is None:
                    actual_eligible_count += 1
                else:
                    actual_excluded_reason_counts[exclusion_reason] += 1
            realized_by_snapshot = {
                int(row.decision_snapshot_id): row
                for row in realized_rows
            }
            post_decision_by_snapshot = {
                int(row.decision_snapshot_id): row
                for row in post_decision_rows
                if row.decision_snapshot_id is not None
            }

            trades: list[DeciderReplayTrade] = []
            excluded_reason_counts: Counter[str] = Counter(canonical_action_exclusions)
            excluded_reason_counts.update(
                str(item['status_reason']) for item in model_identity_exclusions
            )
            for snapshot in snapshot_rows:
                snapshot_id = int(snapshot.id)
                realized = realized_by_snapshot.get(snapshot_id)
                if realized is not None:
                    realized_trade = _realized_outcome_to_trade(
                        snapshot=snapshot,
                        outcome=realized,
                        cost_assumptions=cost_assumptions,
                    )
                    if realized_trade is not None:
                        trades.append(realized_trade)
                        continue
                post_decision = post_decision_by_snapshot.get(snapshot_id)
                if post_decision is not None:
                    post_decision_trade = _post_decision_eval_to_trade(
                        snapshot=snapshot,
                        evaluation=post_decision,
                        cost_assumptions=cost_assumptions,
                    )
                    if post_decision_trade is not None:
                        trades.append(post_decision_trade)
                        continue
                    excluded_reason_counts['fixed_horizon_evaluation_not_economic'] += 1
                    continue
                if realized is not None:
                    excluded_reason_counts['fixed_horizon_outcome_incomplete_or_non_finite'] += 1
                else:
                    excluded_reason_counts['missing_fixed_horizon_outcome'] += 1
            sorted_trades = tuple(
                sorted(trades, key=lambda item: (item.market_date, item.symbol, item.replay_trade_id))
            )
            return DeciderReplayDataset(
                trades=sorted_trades,
                source_decision_count=source_decision_count,
                excluded_reason_counts=dict(sorted(excluded_reason_counts.items())),
                cohort_basis='fixed_horizon_counterfactual',
                diagnostics={
                    'requested_horizon_days': int(horizon_days),
                    'actual_lifecycle_outcomes_used_for_promotion': False,
                    'actual_lifecycle_outcome_count': len(actual_rows),
                    'actual_lifecycle_eligible_count': actual_eligible_count,
                    'actual_lifecycle_excluded_count': len(actual_rows) - actual_eligible_count,
                    'actual_lifecycle_excluded_reason_counts': dict(
                        sorted(actual_excluded_reason_counts.items())
                    ),
                    'model_identity_cohort': model_identity_cohort,
                },
            )

    def list_wait_conflict_gate_trades(
        self,
        *,
        market_date_from: date,
        market_date_to: date,
        symbols: Sequence[str] | None,
        cost_assumptions: DeciderReplayCostAssumptions,
        gate_mode: str,
        probe_quantity: int,
    ) -> DeciderReplayDataset:
        with session_scope() as session:
            stmt = (
                select(
                    SignalDeciderActualExecutionOutcome,
                    SignalDeciderExecutionPlanItem,
                    SignalDeciderDecisionSnapshot,
                )
                .join(
                    SignalDeciderExecutionPlanItem,
                    SignalDeciderActualExecutionOutcome.entry_plan_item_id == SignalDeciderExecutionPlanItem.id,
                )
                .join(
                    SignalDeciderDecisionSnapshot,
                    SignalDeciderActualExecutionOutcome.decision_snapshot_id == SignalDeciderDecisionSnapshot.id,
                )
                .where(
                    SignalDeciderExecutionPlanItem.execution_window == 'next_open',
                    SignalDeciderExecutionPlanItem.decision_agent_name == SHORT_TERM_DECIDER_AGENT_NAME,
                    SignalDeciderExecutionPlanItem.source_market_date >= market_date_from,
                    SignalDeciderExecutionPlanItem.source_market_date <= market_date_to,
                    SignalDeciderActualExecutionOutcome.entry_filled_quantity > 0,
                )
                .order_by(
                    SignalDeciderExecutionPlanItem.source_market_date.asc(),
                    SignalDeciderActualExecutionOutcome.target_trade_date.asc(),
                    SignalDeciderActualExecutionOutcome.symbol.asc(),
                    SignalDeciderActualExecutionOutcome.id.asc(),
                )
            )
            if symbols:
                stmt = stmt.where(SignalDeciderActualExecutionOutcome.symbol.in_(list(symbols)))
            rows = list(session.execute(stmt).all())
            if not rows:
                return DeciderReplayDataset(
                    trades=(),
                    source_decision_count=0,
                    excluded_reason_counts={},
                    cohort_basis='complete_closed_actual_execution',
                    diagnostics={'actual_lifecycle_outcome_count': 0},
                )

            source_decision_count = len(rows)
            selected_snapshots, model_identity_exclusions, model_identity_cohort = (
                select_model_identity_cohort_rows(
                    [row[2] for row in rows],
                    role=SHORT_TERM_DECIDER_AGENT_NAME,
                    expected_model=expected_decider_model(
                        SHORT_TERM_DECIDER_AGENT_NAME,
                        settings_obj=settings,
                    ),
                )
            )
            selected_snapshot_ids = {int(row.id) for row in selected_snapshots}
            rows = [row for row in rows if int(row[2].id) in selected_snapshot_ids]
            excluded_reason_counts: Counter[str] = Counter(
                str(item['status_reason']) for item in model_identity_exclusions
            )
            for row in rows:
                exclusion_reason = _actual_outcome_exclusion_reason(row[0])
                if exclusion_reason is not None:
                    excluded_reason_counts[exclusion_reason] += 1

            rows_requiring_alignment_fallback = [
                row for row in rows if not _plan_alignment_gate(row[1])
            ]
            source_dates = sorted(
                {row[1].source_market_date for row in rows_requiring_alignment_fallback}
            )
            signal_symbols = sorted(
                {row[0].symbol for row in rows_requiring_alignment_fallback}
            )
            signal_stmt = (
                select(SignalDeciderDecisionSnapshot)
                .where(
                    SignalDeciderDecisionSnapshot.agent_name == SIGNAL_DECIDER_AGENT_NAME,
                    SignalDeciderDecisionSnapshot.market_date.in_(source_dates),
                    SignalDeciderDecisionSnapshot.symbol.in_(signal_symbols),
                )
                .order_by(
                    SignalDeciderDecisionSnapshot.market_date.asc(),
                    SignalDeciderDecisionSnapshot.symbol.asc(),
                    SignalDeciderDecisionSnapshot.feedback_received_at.desc(),
                    SignalDeciderDecisionSnapshot.created_at.desc(),
                    SignalDeciderDecisionSnapshot.id.desc(),
                )
            )
            signal_rows = (
                list(session.execute(signal_stmt).scalars().all())
                if rows_requiring_alignment_fallback
                else []
            )
            signal_by_id = {int(signal_row.id): signal_row for signal_row in signal_rows}
            signal_by_context: dict[tuple[date, str], list[SignalDeciderDecisionSnapshot]] = {}
            for signal_row in signal_rows:
                key = (signal_row.market_date, signal_row.symbol)
                signal_by_context.setdefault(key, []).append(signal_row)

            trades: list[DeciderReplayTrade] = []
            alignment_context_source_counts: Counter[str] = Counter()
            for outcome, plan_row, snapshot in rows:
                persisted_gate = _plan_alignment_gate(plan_row)
                signal_context = None
                if persisted_gate:
                    alignment_context_source = 'persisted_plan_alignment_gate'
                else:
                    alignment_snapshot_id = _plan_alignment_snapshot_id(plan_row)
                    signal_row = (
                        signal_by_id.get(alignment_snapshot_id)
                        if alignment_snapshot_id is not None
                        else None
                    )
                    if signal_row is not None:
                        signal_context = build_signal_decider_alignment_context(signal_row)
                        alignment_context_source = 'persisted_plan_alignment_snapshot_id'
                    else:
                        plan_cutoff = plan_row.created_at
                        eligible_signal_rows = [
                            row
                            for row in signal_by_context.get(
                                (plan_row.source_market_date, outcome.symbol),
                                [],
                            )
                            if _timestamp_not_after(_snapshot_timestamp(row), plan_cutoff)
                        ]
                        fallback_row = eligible_signal_rows[0] if eligible_signal_rows else None
                        signal_context = build_signal_decider_alignment_context(fallback_row)
                        alignment_context_source = (
                            'legacy_as_of_plan_created_at'
                            if fallback_row is not None
                            else 'no_point_in_time_alignment_context'
                        )
                alignment_context_source_counts[alignment_context_source] += 1
                trades.append(
                    _actual_short_term_outcome_to_wait_conflict_trade(
                        snapshot=snapshot,
                        plan_row=plan_row,
                        outcome=outcome,
                        signal_decider_context=signal_context,
                        cost_assumptions=cost_assumptions,
                        gate_mode=gate_mode,
                        probe_quantity=probe_quantity,
                        alignment_context_source=alignment_context_source,
                    )
                )
            return DeciderReplayDataset(
                trades=tuple(
                    sorted(trades, key=lambda item: (item.market_date, item.symbol, item.replay_trade_id))
                ),
                source_decision_count=source_decision_count,
                excluded_reason_counts=dict(sorted(excluded_reason_counts.items())),
                cohort_basis='complete_closed_actual_execution',
                diagnostics={
                    'actual_lifecycle_outcome_count': source_decision_count,
                    'actual_lifecycle_eligible_count': (
                        source_decision_count - sum(excluded_reason_counts.values())
                    ),
                    'model_identity_cohort': model_identity_cohort,
                    'alignment_context_source_counts': dict(
                        sorted(alignment_context_source_counts.items())
                    ),
                },
            )


def _normalize_rule_config(rule_config: dict[str, Any] | str | None) -> dict[str, Any]:
    if rule_config is None:
        return {'schema_version': DECIDER_REPLAY_RULE_CONFIG_SCHEMA_VERSION, 'rules': []}
    if isinstance(rule_config, str):
        try:
            parsed = json.loads(rule_config)
        except json.JSONDecodeError as exc:
            raise ValueError(f'rule_config must be valid JSON: {exc.msg}') from exc
        if not isinstance(parsed, dict):
            raise ValueError('rule_config JSON must be an object')
        rule_config = parsed
    if not isinstance(rule_config, dict):
        raise ValueError('rule_config must be a dict or JSON object string')
    unexpected_config_keys = set(rule_config) - {
        'schema_version',
        'candidate_policy_version',
        'match',
        'name',
        'rules',
        'predicates',
    }
    if unexpected_config_keys:
        raise ValueError(
            'rule_config contains unsupported fields: '
            + ', '.join(sorted(str(key) for key in unexpected_config_keys))
        )
    schema_version = rule_config.get('schema_version')
    if schema_version not in {None, DECIDER_REPLAY_RULE_CONFIG_SCHEMA_VERSION}:
        raise ValueError(
            f'rule_config.schema_version must be {DECIDER_REPLAY_RULE_CONFIG_SCHEMA_VERSION}'
        )
    if 'rules' in rule_config and 'predicates' in rule_config:
        raise ValueError('rule_config must define either rules or predicates, not both')
    if 'rules' in rule_config and not isinstance(rule_config.get('rules'), list):
        raise ValueError('rule_config.rules must be a list')
    if 'predicates' in rule_config and not isinstance(rule_config.get('predicates'), list):
        raise ValueError('rule_config.predicates must be a list')

    for rule_index, rule in enumerate(_as_rule_list(rule_config), start=1):
        if not isinstance(rule, dict):
            raise ValueError(f'rule_config rule {rule_index} must be an object')
        unexpected_rule_keys = set(rule) - {'name', 'match', 'predicates', 'all', 'any'}
        if unexpected_rule_keys:
            raise ValueError(
                f'rule_config rule {rule_index} contains unsupported fields: '
                + ', '.join(sorted(str(key) for key in unexpected_rule_keys))
            )
        predicate_keys = [key for key in ('predicates', 'all', 'any') if key in rule]
        if len(predicate_keys) != 1:
            raise ValueError(
                f'rule_config rule {rule_index} must define exactly one of predicates, all, or any'
            )
        match_mode = str(rule.get('match') or ('any' if 'any' in rule else 'all')).strip().lower()
        if match_mode not in {'all', 'any'}:
            raise ValueError(f'rule_config rule {rule_index} match must be all or any')
        predicates = rule.get(predicate_keys[0])
        if not isinstance(predicates, list) or not predicates:
            raise ValueError(f'rule_config rule {rule_index} predicates must be a non-empty list')
        for predicate_index, predicate in enumerate(predicates, start=1):
            _validate_predicate_config(
                predicate,
                location=f'rule_config rule {rule_index} predicate {predicate_index}',
            )
    return rule_config


SUPPORTED_PREDICATE_TYPES = {
    'trigger_price_drift',
    'confidence',
    'risk_flag',
    'liquidity_bucket',
    'factor_score_bucket',
    'source_availability',
    'field',
}
SUPPORTED_PREDICATE_OPERATORS = {
    '==',
    '!=',
    'equals',
    'not_equals',
    '<',
    '<=',
    '>',
    '>=',
    'exists',
    'present',
    'missing',
    'not_exists',
    'contains',
    'contains_any',
    'contains_all',
    'in',
    'not_in',
}


def _validate_predicate_config(predicate: Any, *, location: str) -> None:
    if not isinstance(predicate, dict):
        raise ValueError(f'{location} must be an object')
    unexpected_predicate_keys = set(predicate) - {
        'type',
        'field',
        'path',
        'factor',
        'op',
        'value',
        'values',
        'match',
        'max_below_trigger_pct',
        'min_score_10',
    }
    if unexpected_predicate_keys:
        raise ValueError(
            f'{location} contains unsupported fields: '
            + ', '.join(sorted(str(key) for key in unexpected_predicate_keys))
        )
    predicate_type = str(predicate.get('type') or '').strip().lower()
    if not predicate_type and predicate.get('field'):
        predicate_type = 'field'
    if predicate_type not in SUPPORTED_PREDICATE_TYPES:
        raise ValueError(f'{location} has unsupported predicate type {predicate_type or "missing"}')

    operator = str(predicate.get('op') or '').strip().lower()
    if 'op' in predicate and operator not in SUPPORTED_PREDICATE_OPERATORS:
        raise ValueError(f'{location} has unsupported operator {operator or "missing"}')

    numeric_operators = {'==', '!=', 'equals', 'not_equals', '<', '<=', '>', '>='}
    if predicate_type in {'trigger_price_drift', 'confidence'} and operator and operator not in numeric_operators:
        raise ValueError(f'{location} operator {operator} is invalid for a numeric predicate')
    if predicate_type in {'liquidity_bucket', 'factor_score_bucket'} and operator and operator not in {'in', 'not_in'}:
        raise ValueError(f'{location} operator {operator} is invalid for a bucket predicate')
    if predicate_type == 'risk_flag' and operator and operator not in {
        'contains',
        'contains_any',
        'contains_all',
    }:
        raise ValueError(f'{location} operator {operator} is invalid for a risk-flag predicate')
    if 'match' in predicate and str(predicate.get('match') or '').strip().lower() not in {'all', 'any'}:
        raise ValueError(f'{location} match must be all or any')

    if predicate_type in {'field', 'source_availability'} and not _normalize_text(
        predicate.get('field') or predicate.get('path')
    ):
        raise ValueError(f'{location} requires field or path')
    if predicate_type == 'factor_score_bucket' and not _normalize_text(predicate.get('factor')):
        raise ValueError(f'{location} requires factor')
    if predicate_type in {'risk_flag', 'liquidity_bucket', 'factor_score_bucket'} and not _values_from_predicate(
        predicate
    ):
        raise ValueError(f'{location} requires value or values')

    if predicate_type == 'trigger_price_drift':
        if 'value' in predicate and 'max_below_trigger_pct' in predicate:
            raise ValueError(f'{location} must define only one trigger threshold')
        threshold = predicate.get('value', predicate.get('max_below_trigger_pct'))
        if _to_float(threshold) is None:
            raise ValueError(f'{location} requires a finite numeric threshold')
    if predicate_type == 'confidence':
        if 'value' in predicate and 'min_score_10' in predicate:
            raise ValueError(f'{location} must define only one confidence threshold')
        threshold = predicate.get('value', predicate.get('min_score_10'))
        if _to_float(threshold) is None:
            raise ValueError(f'{location} requires a finite numeric threshold')
    if predicate_type in {'field', 'source_availability'}:
        effective_operator = operator or (
            'in'
            if isinstance(predicate.get('values'), list)
            else '=='
            if 'value' in predicate
            else 'exists'
        )
        if effective_operator in {'<', '<=', '>', '>='}:
            if _to_float(predicate.get('value')) is None:
                raise ValueError(f'{location} numeric comparison requires a finite value')
        elif effective_operator in {'in', 'not_in', 'contains', 'contains_any', 'contains_all'}:
            if not _values_from_predicate(predicate):
                raise ValueError(f'{location} operator {effective_operator} requires value or values')
        elif effective_operator not in {'exists', 'present', 'missing', 'not_exists'} and 'value' not in predicate:
            raise ValueError(f'{location} operator {effective_operator} requires value')


def _as_rule_list(rule_config: dict[str, Any]) -> list[dict[str, Any]]:
    rules = rule_config.get('rules')
    if isinstance(rules, list):
        return list(rules)
    predicates = rule_config.get('predicates')
    if isinstance(predicates, list):
        return [
            {
                'name': rule_config.get('name') or 'candidate_rule',
                'match': rule_config.get('match') or 'all',
                'predicates': predicates,
            }
        ]
    return []


def _values_from_predicate(predicate: dict[str, Any]) -> list[Any]:
    if 'values' in predicate:
        return _maybe_list(predicate.get('values'))
    if 'value' in predicate:
        return [predicate.get('value')]
    return []


def _compare_values(left: Any, *, op: str, right: Any = None, values: Sequence[Any] | None = None) -> bool:
    normalized_op = str(op or '==').strip().lower()
    if normalized_op in {'exists', 'present'}:
        return left is not None
    if normalized_op in {'missing', 'not_exists'}:
        return left is None
    if normalized_op in {'contains', 'contains_any', 'contains_all'}:
        left_values = {str(item) for item in _maybe_list(left)}
        expected_values = {str(item) for item in (values or ([] if right is None else [right]))}
        if normalized_op == 'contains_all':
            return expected_values.issubset(left_values)
        return bool(left_values.intersection(expected_values))
    if normalized_op in {'in', 'not_in'}:
        expected_values = {str(item) for item in (values or [])}
        result = str(left) in expected_values
        return not result if normalized_op == 'not_in' else result
    if normalized_op in {'<', '<=', '>', '>='}:
        left_float = _to_float(left)
        right_float = _to_float(right)
        if left_float is None or right_float is None:
            return False
        if normalized_op == '<':
            return left_float < right_float
        if normalized_op == '<=':
            return left_float <= right_float
        if normalized_op == '>':
            return left_float > right_float
        return left_float >= right_float
    result = str(left) == str(right)
    return not result if normalized_op in {'!=', 'not_equals'} else result


def _resolve_generic_field(point_in_time: dict[str, Any], field: str | None) -> Any:
    if not field:
        return None
    if field in point_in_time:
        return point_in_time[field]
    if field.startswith('payload_context.'):
        return _get_path(point_in_time, field)
    payload_context = _maybe_dict(point_in_time.get('payload_context'))
    for prefix in ('request_payload', 'response_payload', 'traceability_payload', 'input_feature_snapshot_payload', 'symbol_day_context'):
        value = _get_path(payload_context, f'{prefix}.{field}')
        if value is not None:
            return value
    return _get_path(point_in_time, field)


def _evaluate_predicate(predicate: dict[str, Any], point_in_time: dict[str, Any]) -> PredicateResult:
    predicate_type = str(predicate.get('type') or '').strip().lower()
    if not predicate_type and predicate.get('field'):
        predicate_type = 'field'

    if predicate_type == 'trigger_price_drift':
        drift = point_in_time.get('trigger_price_drift_pct')
        threshold = predicate.get('value')
        if threshold is None and predicate.get('max_below_trigger_pct') is not None:
            threshold = -abs(_to_float(predicate.get('max_below_trigger_pct')) or 0)
        matched = _compare_values(drift, op=str(predicate.get('op') or '<='), right=threshold)
        return PredicateResult(
            matched=matched,
            reason=f'trigger_price_drift_pct {predicate.get("op") or "<="} {threshold}',
        )

    if predicate_type == 'confidence':
        value = point_in_time.get('confidence_score_10')
        op = str(predicate.get('op') or '<')
        threshold = predicate.get('value', predicate.get('min_score_10'))
        return PredicateResult(
            matched=_compare_values(value, op=op, right=threshold),
            reason=f'confidence_score_10 {op} {threshold}',
        )

    if predicate_type == 'risk_flag':
        expected = [str(item) for item in _values_from_predicate(predicate)]
        match_mode = str(predicate.get('match') or 'any').lower()
        op = str(
            predicate.get('op')
            or ('contains_all' if match_mode == 'all' else 'contains_any')
        )
        matched = _compare_values(
            point_in_time.get('risk_flags') or [],
            op=op,
            values=expected,
        )
        return PredicateResult(matched=matched, reason=f'risk_flags contains {",".join(expected)}')

    if predicate_type == 'liquidity_bucket':
        values = [str(item) for item in _values_from_predicate(predicate)]
        op = str(predicate.get('op') or 'in')
        matched = _compare_values(point_in_time.get('liquidity_bucket'), op=op, values=values)
        return PredicateResult(matched=matched, reason=f'liquidity_bucket {op} {",".join(values)}')

    if predicate_type == 'factor_score_bucket':
        factor = str(predicate.get('factor') or '').strip()
        bucket = _maybe_dict(point_in_time.get('factor_score_buckets')).get(factor)
        values = [str(item) for item in _values_from_predicate(predicate)]
        op = str(predicate.get('op') or 'in')
        matched = _compare_values(bucket, op=op, values=values)
        return PredicateResult(matched=matched, reason=f'factor_score_bucket[{factor}] {op} {",".join(values)}')

    if predicate_type == 'source_availability':
        path = _normalize_text(predicate.get('path') or predicate.get('field'))
        value = _resolve_generic_field(point_in_time, path)
        values = _values_from_predicate(predicate)
        op = str(predicate.get('op') or ('in' if values else 'exists'))
        matched = _compare_values(value, op=op, right=predicate.get('value'), values=values)
        return PredicateResult(matched=matched, reason=f'{path} {op} {",".join(str(item) for item in values)}')

    if predicate_type == 'field':
        field = _normalize_text(predicate.get('field'))
        value = _resolve_generic_field(point_in_time, field)
        values = _values_from_predicate(predicate)
        op = str(predicate.get('op') or ('in' if len(values) > 1 else '=='))
        matched = _compare_values(value, op=op, right=predicate.get('value'), values=values)
        return PredicateResult(matched=matched, reason=f'{field} {op} {predicate.get("value", values)}')

    return PredicateResult(matched=False, reason=f'unsupported predicate type {predicate_type or "missing"}')


def _evaluate_rule_config(rule_config: dict[str, Any], trade: DeciderReplayTrade) -> RuleEvaluationResult:
    reasons: list[str] = []
    for index, rule in enumerate(_as_rule_list(rule_config), start=1):
        predicates = rule.get('predicates') or rule.get('all') or rule.get('any') or []
        if not isinstance(predicates, list) or not predicates:
            continue
        match_mode = str(rule.get('match') or ('any' if rule.get('any') else 'all')).lower()
        results = [
            _evaluate_predicate(predicate, trade.point_in_time)
            for predicate in predicates
            if isinstance(predicate, dict)
        ]
        if not results:
            continue
        rule_matched = any(result.matched for result in results) if match_mode == 'any' else all(result.matched for result in results)
        if not rule_matched:
            continue
        rule_name = _normalize_text(rule.get('name')) or f'rule_{index}'
        matched_reasons = [result.reason for result in results if result.matched and result.reason]
        reasons.append(f'{rule_name}: {"; ".join(matched_reasons)}')
    return RuleEvaluationResult(skipped=bool(reasons), reasons=tuple(reasons))


def _round_money(value: Decimal) -> Decimal:
    return value.quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)


def _estimated_round_trip_cost(trade: DeciderReplayTrade, cost_assumptions: DeciderReplayCostAssumptions) -> Decimal:
    total_bps = cost_assumptions.fee_bps + cost_assumptions.slippage_bps
    return trade.baseline_notional_krw * total_bps * Decimal('2') / Decimal('10000')


def _trade_net_pnl(trade: DeciderReplayTrade, cost_assumptions: DeciderReplayCostAssumptions) -> Decimal:
    return trade.baseline_gross_pnl_krw - _estimated_round_trip_cost(trade, cost_assumptions)


def _win_rate(
    trades: Sequence[DeciderReplayTrade],
    *,
    cost_assumptions: DeciderReplayCostAssumptions | None = None,
) -> float | None:
    if not trades:
        return None
    winners = sum(
        1
        for trade in trades
        if (
            _trade_net_pnl(trade, cost_assumptions)
            if cost_assumptions is not None
            else trade.baseline_gross_pnl_krw
        )
        > 0
    )
    return round(winners / len(trades), 6)


def _average_return(trades: Sequence[DeciderReplayTrade]) -> float | None:
    values = [
        float(trade.baseline_return_pct)
        for trade in trades
        if trade.baseline_return_pct is not None
    ]
    if not values:
        return None
    return round(sum(values) / len(values), 6)


def _estimated_net_average_return(
    trades: Sequence[DeciderReplayTrade],
    *,
    cost_assumptions: DeciderReplayCostAssumptions,
) -> float | None:
    values = [
        float(_trade_net_pnl(trade, cost_assumptions) / trade.baseline_notional_krw)
        for trade in trades
        if trade.baseline_notional_krw > 0
    ]
    if not values:
        return None
    return round(sum(values) / len(values), 6)


def _max_drawdown_proxy(
    trades: Sequence[DeciderReplayTrade],
    *,
    cost_assumptions: DeciderReplayCostAssumptions,
) -> Decimal | None:
    if len(trades) < 2:
        return None
    equity = Decimal('0')
    peak = Decimal('0')
    max_drawdown = Decimal('0')
    for trade in sorted(trades, key=lambda item: (item.market_date, item.symbol, item.replay_trade_id)):
        equity += _trade_net_pnl(trade, cost_assumptions)
        if equity > peak:
            peak = equity
        drawdown = equity - peak
        if drawdown < max_drawdown:
            max_drawdown = drawdown
    return max_drawdown


def _metric_block(
    trades: Sequence[DeciderReplayTrade],
    *,
    cost_assumptions: DeciderReplayCostAssumptions,
) -> dict[str, Any]:
    gross_pnl = sum((trade.baseline_gross_pnl_krw for trade in trades), Decimal('0'))
    net_pnl = sum((_trade_net_pnl(trade, cost_assumptions) for trade in trades), Decimal('0'))
    max_drawdown = _max_drawdown_proxy(trades, cost_assumptions=cost_assumptions)
    return {
        'trade_count': len(trades),
        'gross_pnl_krw': _to_json_number(_round_money(gross_pnl)),
        'estimated_net_pnl_krw': _to_json_number(_round_money(net_pnl)),
        'win_rate': _win_rate(trades),
        'gross_win_rate': _win_rate(trades),
        'estimated_net_win_rate': _win_rate(
            trades,
            cost_assumptions=cost_assumptions,
        ),
        'average_return_pct': _average_return(trades),
        'gross_average_return_pct': _average_return(trades),
        'estimated_net_average_return_pct': _estimated_net_average_return(
            trades,
            cost_assumptions=cost_assumptions,
        ),
        'max_drawdown_proxy_krw': _to_json_number(_round_money(max_drawdown)) if max_drawdown is not None else None,
    }


def _delta(left: int | float | None, right: int | float | None) -> int | float | None:
    if left is None or right is None:
        return None
    return round(float(left) - float(right), 6)


def _example_from_trade(
    *,
    trade: DeciderReplayTrade,
    reasons: Sequence[str],
    cost_assumptions: DeciderReplayCostAssumptions,
) -> dict[str, Any]:
    example = {
        'snapshot_id': trade.snapshot_id,
        'replay_trade_id': trade.replay_trade_id,
        'symbol': trade.symbol,
        'market_date': trade.market_date.isoformat(),
        'agent_name': trade.agent_name,
        'policy_version': trade.policy_version,
        'reason': '; '.join(reasons),
        'reasons': list(reasons),
        'baseline_gross_pnl_krw': _to_json_number(_round_money(trade.baseline_gross_pnl_krw)),
        'baseline_estimated_net_pnl_krw': _to_json_number(_round_money(_trade_net_pnl(trade, cost_assumptions))),
        'baseline_return_pct': _to_ratio_number(trade.baseline_return_pct),
        'outcome_source': trade.outcome_source,
    }
    source_market_date = _normalize_text(trade.point_in_time.get('source_market_date'))
    target_trade_date = _normalize_text(trade.point_in_time.get('target_trade_date'))
    if source_market_date is not None:
        example['source_market_date'] = source_market_date
    if target_trade_date is not None:
        example['target_trade_date'] = target_trade_date
    return example


def _sort_examples(examples: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
    return sorted(
        examples,
        key=lambda item: (
            0 if _to_float(item.get('baseline_gross_pnl_krw')) is not None and _to_float(item.get('baseline_gross_pnl_krw')) < 0 else 1,
            _to_float(item.get('baseline_gross_pnl_krw')) or 0,
            item.get('market_date') or '',
            item.get('symbol') or '',
        ),
    )


def _profitability_markdown_lines(
    *,
    request: dict[str, Any],
    metadata: dict[str, Any],
) -> list[str]:
    costs = _maybe_dict(request.get('cost_assumptions'))
    gate = _maybe_dict(metadata.get('promotion_cost_gate'))
    gate_status = _normalize_text(gate.get('status')) or 'unknown'
    gate_reason = _normalize_text(gate.get('reason'))
    gate_text = f'- Promotion cost gate: {gate_status}'
    if gate_reason:
        gate_text = f'{gate_text} ({gate_reason})'
    return [
        f'- Profitability basis: {metadata.get("profitability_basis") or "unknown"}',
        (
            '- Per-side cost assumptions: '
            f'fee_bps={costs.get("fee_bps")}, '
            f'slippage_bps={costs.get("slippage_bps")}'
        ),
        f'- Fallback notional assumption KRW: {costs.get("notional_krw")}',
        gate_text,
    ]


def _build_markdown_summary(
    *,
    request: dict[str, Any],
    metadata: dict[str, Any],
    summary: dict[str, Any],
    examples: Sequence[dict[str, Any]],
) -> str:
    lines = [
        '# Decider replay summary',
        '',
        f'- Window: {request["market_date_from"]}..{request["market_date_to"]}',
        f'- Agent: {request["agent_name"]}',
        *_profitability_markdown_lines(request=request, metadata=metadata),
        f'- Baseline trades: {summary["baseline_trade_count"]}',
        f'- Candidate trades: {summary["candidate_trade_count"]}',
        f'- Skipped buys: {summary["skipped_buys"]}',
        f'- Avoided losers: {summary["avoided_losers"]}',
        f'- Skipped winners: {summary["skipped_winners"]}',
        f'- Gross P/L delta KRW: {summary["gross_pnl_delta_krw"]}',
        f'- Estimated net P/L delta KRW: {summary["estimated_net_pnl_delta_krw"]}',
        f'- Win-rate delta: {summary["win_rate_delta"]}',
        f'- Average-return delta: {summary["average_return_delta"]}',
    ]
    outcome_sources = _maybe_dict(summary.get('outcome_sources'))
    if outcome_sources:
        source_text = ', '.join(f'{key}={value}' for key, value in sorted(outcome_sources.items()))
        lines.append(f'- Outcome sources: {source_text}')
    if summary.get('max_drawdown_proxy_delta_krw') is not None:
        lines.append(f'- Max drawdown proxy delta KRW: {summary["max_drawdown_proxy_delta_krw"]}')
    lines.extend(['', '## Top rule-triggered examples'])
    if not examples:
        lines.append('No rule-triggered examples.')
        return '\n'.join(lines)
    lines.append('| date | symbol | reason | gross pnl KRW | return |')
    lines.append('| --- | --- | --- | ---: | ---: |')
    for example in examples:
        lines.append(
            '| {date} | {symbol} | {reason} | {pnl} | {ret} |'.format(
                date=example.get('market_date'),
                symbol=example.get('symbol'),
                reason=str(example.get('reason') or '').replace('|', '/'),
                pnl=example.get('baseline_gross_pnl_krw'),
                ret=example.get('baseline_return_pct'),
            )
        )
    return '\n'.join(lines)


def _trade_alignment_gate(trade: DeciderReplayTrade) -> dict[str, Any]:
    if isinstance(trade.alignment_gate, dict):
        return trade.alignment_gate
    gate = _maybe_dict(trade.point_in_time.get('alignment_gate'))
    return gate


def _wait_conflict_reason(trade: DeciderReplayTrade) -> list[str]:
    gate = _trade_alignment_gate(trade)
    reason = _normalize_text(gate.get('alignment_gate_reason')) or _normalize_text(
        trade.point_in_time.get('alignment_gate_reason')
    )
    return [reason] if reason else ['signal_decider_wait_conflict_gate']


def _unresolved_wait_conflict_example(trade: DeciderReplayTrade) -> dict[str, Any]:
    gate = _trade_alignment_gate(trade)
    return {
        'snapshot_id': trade.snapshot_id,
        'replay_trade_id': trade.replay_trade_id,
        'entry_plan_item_id': trade.point_in_time.get('entry_plan_item_id'),
        'actual_execution_outcome_id': trade.point_in_time.get('actual_execution_outcome_id'),
        'source_market_date': trade.point_in_time.get('source_market_date') or trade.market_date.isoformat(),
        'target_trade_date': trade.point_in_time.get('target_trade_date'),
        'symbol': trade.symbol,
        'lifecycle_status': trade.lifecycle_status,
        'entry_filled_quantity': _to_json_number(trade.entry_filled_quantity),
        'closed_quantity': _to_json_number(trade.closed_quantity),
        'open_quantity': _to_json_number(trade.open_quantity),
        'realized_pnl_krw': (
            _to_json_number(_round_money(trade.baseline_gross_pnl_krw))
            if trade.outcome_resolved
            else None
        ),
        'signal_decider_alignment': gate.get('signal_decider_alignment'),
        'alignment_gate_action': gate.get('alignment_gate_action'),
        'alignment_evidence_status': gate.get('evidence_status'),
        'reason': gate.get('alignment_gate_reason'),
    }


def _build_wait_conflict_markdown_summary(
    *,
    request: dict[str, Any],
    metadata: dict[str, Any],
    summary: dict[str, Any],
    examples: Sequence[dict[str, Any]],
    weeks: Sequence[dict[str, Any]],
    unresolved_open_candidates: Sequence[dict[str, Any]],
    promotion_readiness: dict[str, Any],
) -> str:
    lines = [
        '# Short-term WAIT-conflict gate replay',
        '',
        f'- Source decision window: {request["market_date_from"]}..{request["market_date_to"]}',
        f'- Date window basis: {request["date_window_basis"]}',
        '- Baseline source: actual short-term execution outcomes only',
        f'- Candidate gate mode: {request["candidate_gate_mode"]}',
        f'- Replay probe quantity: {request["probe_quantity"]}',
        *_profitability_markdown_lines(request=request, metadata=metadata),
        f'- Baseline trades: {summary["baseline_trade_count"]}',
        f'- Candidate trades: {summary["candidate_trade_count"]}',
        f'- Blocked candidates: {summary["blocked_candidate_count"]}',
        f'- Allowed by override: {summary["allowed_by_override_count"]}',
        f'- Skipped losses: {summary["skipped_losses"]}',
        f'- Skipped winners: {summary["skipped_winners"]}',
        f'- Gross P/L delta KRW: {summary["gross_pnl_delta_krw"]}',
        f'- Estimated net P/L delta KRW: {summary["estimated_net_pnl_delta_krw"]}',
        f'- Unresolved/open candidates: {summary["unresolved_open_candidate_count"]}',
        f'- Missing alignment evidence: {summary["missing_alignment_evidence_count"]}',
        '- Manual approval may now be requested: '
        f'{str(bool(promotion_readiness.get("ready_for_manual_approval"))).lower()}',
        f'- Final activation ready: {str(bool(promotion_readiness.get("ready"))).lower()}',
        '',
        '## Completed-week evidence',
        '',
        '| week | resolved | unresolved/open | skipped losses | skipped winners | estimated net delta KRW |',
        '| --- | ---: | ---: | ---: | ---: | ---: |',
    ]
    for week in weeks:
        week_summary = week['summary']
        lines.append(
            '| {start}..{end} | {resolved} | {unresolved} | {losses} | {winners} | {delta} |'.format(
                start=week['week_start'],
                end=week['week_end'],
                resolved=week_summary['baseline_trade_count'],
                unresolved=week_summary['unresolved_open_candidate_count'],
                losses=week_summary['skipped_losses'],
                winners=week_summary['skipped_winners'],
                delta=week_summary['estimated_net_pnl_delta_krw'],
            )
        )
    if summary.get('empty_data_reason'):
        lines.append(f'- Empty-data reason: {summary["empty_data_reason"]}')
    lines.extend(['', '## Unresolved/open candidates'])
    if not unresolved_open_candidates:
        lines.append('No unresolved or open filled candidates.')
    else:
        lines.append('| source date | execution date | symbol | lifecycle | closed qty | open qty | alignment |')
        lines.append('| --- | --- | --- | --- | ---: | ---: | --- |')
        for candidate in unresolved_open_candidates:
            lines.append(
                '| {source} | {target} | {symbol} | {lifecycle} | {closed} | {open_qty} | {alignment} |'.format(
                    source=candidate.get('source_market_date'),
                    target=candidate.get('target_trade_date') or '',
                    symbol=candidate.get('symbol'),
                    lifecycle=candidate.get('lifecycle_status') or 'unresolved',
                    closed=candidate.get('closed_quantity'),
                    open_qty=candidate.get('open_quantity'),
                    alignment=candidate.get('signal_decider_alignment') or 'missing_alignment_evidence',
                )
            )
    lines.extend(['', '## Blocked candidate examples'])
    if not examples:
        lines.append('No blocked candidate examples.')
        return '\n'.join(lines)
    lines.append('| source date | execution date | symbol | reason | gross pnl KRW | return |')
    lines.append('| --- | --- | --- | --- | ---: | ---: |')
    for example in examples:
        lines.append(
            '| {source_date} | {execution_date} | {symbol} | {reason} | {pnl} | {ret} |'.format(
                source_date=example.get('source_market_date') or example.get('market_date'),
                execution_date=example.get('target_trade_date') or '',
                symbol=example.get('symbol'),
                reason=str(example.get('reason') or '').replace('|', '/'),
                pnl=example.get('baseline_gross_pnl_krw'),
                ret=example.get('baseline_return_pct'),
            )
        )
    return '\n'.join(lines)


def _coerce_replay_dataset(
    value: DeciderReplayDataset | Sequence[DeciderReplayTrade],
    *,
    cohort_basis: str,
) -> DeciderReplayDataset:
    if isinstance(value, DeciderReplayDataset):
        return value
    trades = tuple(value)
    return DeciderReplayDataset(
        trades=trades,
        source_decision_count=len(trades),
        excluded_reason_counts={},
        cohort_basis=cohort_basis,
        diagnostics={},
    )


def _dataset_coverage(dataset: DeciderReplayDataset) -> dict[str, Any]:
    excluded_count = sum(dataset.excluded_reason_counts.values())
    denominator = dataset.source_decision_count
    economic_count = sum(1 for trade in dataset.trades if trade.outcome_resolved)
    return {
        'source_decision_count': denominator,
        'economic_cohort_count': economic_count,
        'censored_or_excluded_count': excluded_count,
        'economic_coverage_ratio': (
            round(economic_count / denominator, 6)
            if denominator > 0
            else None
        ),
        'excluded_reason_counts': dict(dataset.excluded_reason_counts),
        'cohort_basis': dataset.cohort_basis,
        **dataset.diagnostics,
    }


class DeciderReplayService:
    def __init__(self, *, repository: DeciderReplayRepository | None = None) -> None:
        self.repository = repository or DeciderReplayRepository()

    def replay(
        self,
        *,
        market_date_from: str | date,
        market_date_to: str | date,
        agent_name: str,
        rule_config: dict[str, Any] | str | None,
        symbols: Sequence[str] | str | None = None,
        policy_versions: Sequence[str] | str | None = None,
        fee_bps: Decimal | float | int | str = Decimal('0'),
        slippage_bps: Decimal | float | int | str = Decimal('0'),
        notional_krw: Decimal | float | int | str = DEFAULT_REPLAY_NOTIONAL_KRW,
        horizon_days: int = DEFAULT_REPLAY_HORIZON_DAYS,
        top_examples_limit: int = DEFAULT_TOP_EXAMPLES_LIMIT,
    ) -> dict[str, Any]:
        normalized_from = _normalize_market_date(market_date_from)
        normalized_to = _normalize_market_date(market_date_to)
        if normalized_from > normalized_to:
            raise ValueError('market_date_from must be on or before market_date_to')
        normalized_agent = _normalize_agent_name(agent_name)
        normalized_symbols = _normalize_symbols(symbols)
        normalized_policy_versions = _normalize_policy_versions(policy_versions)
        resolved_costs = DeciderReplayCostAssumptions(
            fee_bps=_normalize_non_negative_decimal(fee_bps, name='fee_bps'),
            slippage_bps=_normalize_non_negative_decimal(slippage_bps, name='slippage_bps'),
            notional_krw=_normalize_non_negative_decimal(notional_krw, name='notional_krw'),
        )
        if resolved_costs.notional_krw <= 0:
            raise ValueError('notional_krw must be > 0')
        resolved_horizon_days = max(1, int(horizon_days))
        resolved_top_examples_limit = max(0, int(top_examples_limit))
        resolved_rule_config = _normalize_rule_config(rule_config)
        dataset = _coerce_replay_dataset(
            self.repository.list_replay_trades(
                market_date_from=normalized_from,
                market_date_to=normalized_to,
                agent_name=normalized_agent,
                symbols=normalized_symbols or None,
                policy_versions=normalized_policy_versions or None,
                horizon_days=resolved_horizon_days,
                cost_assumptions=resolved_costs,
            ),
            cohort_basis='fixed_horizon_counterfactual',
        )
        trades = list(dataset.trades)

        skipped: list[tuple[DeciderReplayTrade, RuleEvaluationResult]] = []
        kept: list[DeciderReplayTrade] = []
        for trade in trades:
            evaluation = _evaluate_rule_config(resolved_rule_config, trade)
            if evaluation.skipped:
                skipped.append((trade, evaluation))
            else:
                kept.append(trade)

        baseline = _metric_block(trades, cost_assumptions=resolved_costs)
        candidate = _metric_block(kept, cost_assumptions=resolved_costs)
        examples = _sort_examples(
            [
                _example_from_trade(
                    trade=trade,
                    reasons=evaluation.reasons,
                    cost_assumptions=resolved_costs,
                )
                for trade, evaluation in skipped
            ]
        )[:resolved_top_examples_limit]
        gross_delta = _safe_decimal_or_zero(candidate['gross_pnl_krw']) - _safe_decimal_or_zero(baseline['gross_pnl_krw'])
        net_delta = _safe_decimal_or_zero(candidate['estimated_net_pnl_krw']) - _safe_decimal_or_zero(baseline['estimated_net_pnl_krw'])
        gross_win_rate_delta = _delta(candidate.get('gross_win_rate'), baseline.get('gross_win_rate'))
        net_win_rate_delta = _delta(
            candidate.get('estimated_net_win_rate'),
            baseline.get('estimated_net_win_rate'),
        )
        gross_average_return_delta = _delta(
            candidate.get('gross_average_return_pct'),
            baseline.get('gross_average_return_pct'),
        )
        net_average_return_delta = _delta(
            candidate.get('estimated_net_average_return_pct'),
            baseline.get('estimated_net_average_return_pct'),
        )
        drawdown_delta = _delta(candidate.get('max_drawdown_proxy_krw'), baseline.get('max_drawdown_proxy_krw'))
        request = {
            'market_date_from': normalized_from.isoformat(),
            'market_date_to': normalized_to.isoformat(),
            'agent_name': normalized_agent,
            'symbols': normalized_symbols,
            'policy_versions': normalized_policy_versions,
            'horizon_days': resolved_horizon_days,
            'rule_config': resolved_rule_config,
            'cost_assumptions': {
                'fee_bps': _to_json_number(resolved_costs.fee_bps, places='0.000001'),
                'slippage_bps': _to_json_number(resolved_costs.slippage_bps, places='0.000001'),
                'notional_krw': _to_json_number(resolved_costs.notional_krw),
            },
            'top_examples_limit': resolved_top_examples_limit,
            'model_identity_cohort': dataset.diagnostics.get('model_identity_cohort'),
        }
        summary = {
            'baseline_trade_count': baseline['trade_count'],
            'candidate_trade_count': candidate['trade_count'],
            'skipped_buys': len(skipped),
            'skipped_winners': sum(1 for trade, _ in skipped if trade.baseline_gross_pnl_krw > 0),
            'avoided_losers': sum(1 for trade, _ in skipped if trade.baseline_gross_pnl_krw < 0),
            'skipped_net_winners': sum(
                1
                for trade, _ in skipped
                if _trade_net_pnl(trade, resolved_costs) > 0
            ),
            'avoided_net_losers': sum(
                1
                for trade, _ in skipped
                if _trade_net_pnl(trade, resolved_costs) < 0
            ),
            'gross_pnl_delta_krw': _to_json_number(_round_money(gross_delta)),
            'estimated_net_pnl_delta_krw': _to_json_number(_round_money(net_delta)),
            'win_rate_delta': gross_win_rate_delta,
            'average_return_delta': gross_average_return_delta,
            'gross_win_rate_delta': gross_win_rate_delta,
            'estimated_net_win_rate_delta': net_win_rate_delta,
            'gross_average_return_delta': gross_average_return_delta,
            'estimated_net_average_return_delta': net_average_return_delta,
            'max_drawdown_proxy_delta_krw': drawdown_delta,
            'baseline': baseline,
            'candidate': candidate,
            'outcome_sources': _count_by_key(trades, key='outcome_source'),
            'coverage': _dataset_coverage(dataset),
            'promotion_metrics': {
                'basis': 'estimated_net',
                'estimated_net_pnl_delta_krw': _to_json_number(_round_money(net_delta)),
                'estimated_net_win_rate_delta': net_win_rate_delta,
                'estimated_net_average_return_delta': net_average_return_delta,
            },
        }
        profitability_metadata = _replay_profitability_metadata(
            resolved_costs,
            cohort_basis=dataset.cohort_basis,
        )
        markdown_summary = _build_markdown_summary(
            request=request,
            metadata=profitability_metadata,
            summary=summary,
            examples=examples,
        )
        return {
            'schema_version': DECIDER_REPLAY_SCHEMA_VERSION,
            'metadata': profitability_metadata,
            'request': request,
            'summary': summary,
            'examples': examples,
            'markdown_summary': markdown_summary,
        }

    def replay_wait_conflict_gate(
        self,
        *,
        market_date_from: str | date | None = None,
        market_date_to: str | date | None = None,
        completed_weeks: int = DEFAULT_WAIT_CONFLICT_COMPLETED_WEEKS,
        as_of_date_kst: str | date | None = None,
        symbols: Sequence[str] | str | None = None,
        fee_bps: Decimal | float | int | str = Decimal('0'),
        slippage_bps: Decimal | float | int | str = Decimal('0'),
        notional_krw: Decimal | float | int | str = DEFAULT_REPLAY_NOTIONAL_KRW,
        gate_mode: str = 'hard_block',
        probe_quantity: Decimal | float | int | str = MIN_WAIT_CONFLICT_PROBE_QUANTITY,
        top_examples_limit: int = DEFAULT_TOP_EXAMPLES_LIMIT,
    ) -> dict[str, Any]:
        if (market_date_from is None) != (market_date_to is None):
            raise ValueError('market_date_from and market_date_to must be provided together')
        if market_date_from is None:
            week_windows = _completed_week_windows(
                as_of_date_kst=as_of_date_kst,
                completed_weeks=completed_weeks,
            )
            normalized_from, normalized_to = week_windows[0][0], week_windows[-1][1]
            window_mode = 'rolling_completed_weeks'
        else:
            normalized_from = _normalize_market_date(market_date_from)
            normalized_to = _normalize_market_date(market_date_to)
            week_windows = _explicit_week_windows(normalized_from, normalized_to)
            window_mode = 'explicit_source_date_range'
        if normalized_from > normalized_to:
            raise ValueError('market_date_from must be on or before market_date_to')
        resolved_gate_mode = normalize_alignment_gate_mode(gate_mode)
        resolved_probe_quantity = _normalize_probe_quantity(probe_quantity)
        normalized_symbols = _normalize_symbols(symbols)
        resolved_costs = DeciderReplayCostAssumptions(
            fee_bps=_normalize_non_negative_decimal(fee_bps, name='fee_bps'),
            slippage_bps=_normalize_non_negative_decimal(slippage_bps, name='slippage_bps'),
            notional_krw=_normalize_non_negative_decimal(notional_krw, name='notional_krw'),
        )
        if resolved_costs.notional_krw <= 0:
            raise ValueError('notional_krw must be > 0')
        resolved_top_examples_limit = max(0, int(top_examples_limit))
        dataset = _coerce_replay_dataset(
            self.repository.list_wait_conflict_gate_trades(
                market_date_from=normalized_from,
                market_date_to=normalized_to,
                symbols=normalized_symbols or None,
                cost_assumptions=resolved_costs,
                gate_mode=resolved_gate_mode,
                probe_quantity=resolved_probe_quantity,
            ),
            cohort_basis='complete_closed_actual_execution',
        )
        trades = list(dataset.trades)

        def summarize(scope: Sequence[DeciderReplayTrade]) -> dict[str, Any]:
            resolved = [trade for trade in scope if trade.outcome_resolved]
            blocked = [
                trade
                for trade in scope
                if _trade_alignment_gate(trade).get('alignment_gate_action') == ALIGNMENT_GATE_ACTION_BLOCK
            ]
            downsized = [
                trade
                for trade in scope
                if _trade_alignment_gate(trade).get('alignment_gate_action')
                == ALIGNMENT_GATE_ACTION_DOWNSIZE_PROBE
            ]
            allowed_by_override = [
                trade
                for trade in scope
                if _trade_alignment_gate(trade).get('signal_decider_alignment')
                == SIGNAL_DECIDER_ALIGNMENT_WAIT_CONFLICT_OVERRIDE
                and (_trade_alignment_gate(trade).get('override') or {}).get('allowed') is True
            ]
            def retained_quantity_ratio(trade: DeciderReplayTrade) -> Decimal:
                gate = _trade_alignment_gate(trade)
                action = gate.get('alignment_gate_action')
                if action == ALIGNMENT_GATE_ACTION_BLOCK:
                    return Decimal('0')
                if action != ALIGNMENT_GATE_ACTION_DOWNSIZE_PROBE:
                    return Decimal('1')
                requested = _safe_decimal(gate.get('requested_quantity'))
                final = _safe_decimal(gate.get('final_quantity'))
                if requested is None or requested <= 0 or final is None:
                    return Decimal('1')
                return min(max(final / requested, Decimal('0')), Decimal('1'))

            candidate_resolved: list[DeciderReplayTrade] = []
            for trade in resolved:
                retained_ratio = retained_quantity_ratio(trade)
                if retained_ratio == 0:
                    continue
                if retained_ratio < 1:
                    candidate_resolved.append(
                        replace(
                            trade,
                            baseline_gross_pnl_krw=trade.baseline_gross_pnl_krw * retained_ratio,
                            baseline_notional_krw=trade.baseline_notional_krw * retained_ratio,
                        )
                    )
                else:
                    candidate_resolved.append(trade)
            unresolved = [
                trade
                for trade in scope
                if not trade.outcome_resolved or (trade.open_quantity or Decimal('0')) > Decimal('0')
            ]
            baseline = _metric_block(resolved, cost_assumptions=resolved_costs)
            candidate = _metric_block(candidate_resolved, cost_assumptions=resolved_costs)
            gross_delta = _safe_decimal_or_zero(candidate['gross_pnl_krw']) - _safe_decimal_or_zero(
                baseline['gross_pnl_krw']
            )
            net_delta = _safe_decimal_or_zero(candidate['estimated_net_pnl_krw']) - _safe_decimal_or_zero(
                baseline['estimated_net_pnl_krw']
            )
            removed_pnl: list[Decimal] = []
            for trade in resolved:
                removed_ratio = Decimal('1') - retained_quantity_ratio(trade)
                if removed_ratio > 0:
                    removed_pnl.append(trade.baseline_gross_pnl_krw * removed_ratio)
            skipped_loss_pnl = sum((pnl for pnl in removed_pnl if pnl < 0), Decimal('0'))
            skipped_winner_pnl = sum((pnl for pnl in removed_pnl if pnl > 0), Decimal('0'))
            missing_alignment = [
                trade
                for trade in scope
                if _trade_alignment_gate(trade).get('evidence_status') == 'missing'
                or _trade_alignment_gate(trade).get('signal_decider_alignment') == 'missing_alignment_evidence'
            ]
            return {
                'actual_filled_candidate_count': len(scope),
                'baseline_trade_count': baseline['trade_count'],
                'candidate_trade_count': candidate['trade_count'],
                'blocked_candidate_count': len(blocked),
                'downsized_candidate_count': len(downsized),
                'allowed_by_override_count': len(allowed_by_override),
                'allowed_non_conflict_count': max(
                    len(scope) - len(blocked) - len(downsized) - len(allowed_by_override),
                    0,
                ),
                'unresolved_open_candidate_count': len(unresolved),
                'missing_alignment_evidence_count': len(missing_alignment),
                'skipped_losses': sum(1 for pnl in removed_pnl if pnl < 0),
                'skipped_winners': sum(1 for pnl in removed_pnl if pnl > 0),
                'skipped_losses_gross_pnl_krw': _to_json_number(_round_money(skipped_loss_pnl)),
                'skipped_losses_avoided_krw': _to_json_number(_round_money(-skipped_loss_pnl)),
                'skipped_winners_gross_pnl_krw': _to_json_number(_round_money(skipped_winner_pnl)),
                'gross_pnl_delta_krw': _to_json_number(_round_money(gross_delta)),
                'estimated_net_pnl_delta_krw': _to_json_number(_round_money(net_delta)),
                'baseline_realized_pnl_krw': baseline['gross_pnl_krw'],
                'counterfactual_realized_pnl_krw': candidate['gross_pnl_krw'],
                'net_pnl_delta_krw': _to_json_number(_round_money(net_delta)),
                'baseline': baseline,
                'candidate': candidate,
                'outcome_sources': _count_by_key(scope, key='outcome_source'),
                'alignment_counts': _count_alignment(scope),
                '_blocked': blocked,
                '_unresolved': unresolved,
            }

        summary = summarize(trades)
        blocked = summary.pop('_blocked')
        unresolved = summary.pop('_unresolved')
        examples = _sort_examples(
            [
                _example_from_trade(
                    trade=trade,
                    reasons=_wait_conflict_reason(trade),
                    cost_assumptions=resolved_costs,
                )
                for trade in blocked
                if trade.outcome_resolved
            ]
        )[:resolved_top_examples_limit]
        unresolved_open_candidates = [
            _unresolved_wait_conflict_example(trade)
            for trade in sorted(unresolved, key=lambda item: (item.market_date, item.symbol, item.replay_trade_id))
        ]
        weeks: list[dict[str, Any]] = []
        for week_start, week_end in week_windows:
            week_summary = summarize(
                [trade for trade in trades if week_start <= trade.market_date <= week_end]
            )
            week_summary.pop('_blocked')
            week_summary.pop('_unresolved')
            weeks.append(
                {
                    'week_start': week_start.isoformat(),
                    'week_end': week_end.isoformat(),
                    'summary': week_summary,
                }
            )
        completed_week_count = sum(
            1
            for (start, end), week in zip(week_windows, weeks)
            if start.weekday() == 0
            and end.weekday() == 4
            and (end - start).days == 4
            and week['summary']['baseline_trade_count'] > 0
        )
        wait_conflict_trades = [
            trade
            for trade in trades
            if _trade_alignment_gate(trade).get('signal_decider_alignment')
            == SIGNAL_DECIDER_ALIGNMENT_WAIT_CONFLICT_BLOCK
        ]
        resolved_wait_conflict_count = sum(1 for trade in wait_conflict_trades if trade.outcome_resolved)
        unresolved_wait_conflict_count = sum(
            1
            for trade in wait_conflict_trades
            if not trade.outcome_resolved or (trade.open_quantity or Decimal('0')) > Decimal('0')
        )
        wait_conflict_total = len(wait_conflict_trades)
        unresolved_ratio = (
            Decimal(unresolved_wait_conflict_count) / Decimal(wait_conflict_total)
            if wait_conflict_total > 0
            else None
        )
        profitability_metadata = _replay_profitability_metadata(
            resolved_costs,
            cohort_basis=dataset.cohort_basis,
        )
        promotion_cost_gate = _maybe_dict(
            profitability_metadata.get('promotion_cost_gate')
        )
        promotion_thresholds = {
            'minimum_completed_weeks': DEFAULT_WAIT_CONFLICT_COMPLETED_WEEKS,
            'minimum_resolved_wait_conflicts': 20,
            'minimum_fee_bps': _to_json_number(
                MIN_PROMOTION_FEE_BPS,
                places='0.000001',
            ),
            'minimum_slippage_bps': _to_json_number(
                MIN_PROMOTION_SLIPPAGE_BPS,
                places='0.000001',
            ),
            'require_positive_net_pnl_delta': True,
            'require_avoided_losses_exceed_skipped_winners': True,
            'require_minimum_cost_assumptions': True,
            'maximum_unresolved_ratio': 0.25,
            'manual_approval_required': True,
        }
        promotion_checks = {
            'completed_weeks_met': completed_week_count >= promotion_thresholds['minimum_completed_weeks'],
            'resolved_wait_conflicts_met': (
                resolved_wait_conflict_count >= promotion_thresholds['minimum_resolved_wait_conflicts']
            ),
            'positive_net_pnl_delta': Decimal(str(summary['estimated_net_pnl_delta_krw'])) > 0,
            'avoided_losses_exceed_skipped_winners': Decimal(
                str(summary['skipped_losses_avoided_krw'])
            ) > Decimal(str(summary['skipped_winners_gross_pnl_krw'])),
            'unresolved_ratio_within_limit': (
                unresolved_ratio is not None
                and unresolved_ratio <= Decimal(str(promotion_thresholds['maximum_unresolved_ratio']))
            ),
            'minimum_cost_assumptions_met': promotion_cost_gate.get('eligible') is True,
        }
        evidence_threshold_met = all(promotion_checks.values())
        promotion_readiness = {
            'ready': False,
            'ready_for_manual_approval': evidence_threshold_met,
            'manual_approval_recorded': False,
            'automatic_activation': False,
            'thresholds': promotion_thresholds,
            'checks': promotion_checks,
            'unresolved_ratio': _to_ratio_number(float(unresolved_ratio)) if unresolved_ratio is not None else None,
        }
        summary['completed_week_count'] = completed_week_count
        summary['resolved_wait_conflict_count'] = resolved_wait_conflict_count
        summary['unresolved_wait_conflict_count'] = unresolved_wait_conflict_count
        summary['coverage'] = _dataset_coverage(dataset)
        summary['promotion_metrics'] = {
            'basis': 'estimated_net',
            'estimated_net_pnl_delta_krw': summary['estimated_net_pnl_delta_krw'],
            'estimated_net_win_rate_delta': _delta(
                summary['candidate'].get('estimated_net_win_rate'),
                summary['baseline'].get('estimated_net_win_rate'),
            ),
            'estimated_net_average_return_delta': _delta(
                summary['candidate'].get('estimated_net_average_return_pct'),
                summary['baseline'].get('estimated_net_average_return_pct'),
            ),
        }
        summary['empty_data_reason'] = (
            'no_actual_short_term_entry_fills_in_range'
            if dataset.source_decision_count == 0
            else 'no_complete_closed_actual_execution_outcomes_in_range'
            if summary['baseline_trade_count'] == 0
            else None
        )
        request = {
            'market_date_from': normalized_from.isoformat(),
            'market_date_to': normalized_to.isoformat(),
            'agent_name': SHORT_TERM_DECIDER_AGENT_NAME,
            'symbols': normalized_symbols,
            'candidate_gate_mode': resolved_gate_mode,
            'probe_quantity': resolved_probe_quantity,
            'window_mode': window_mode,
            'as_of_date_kst': (
                _normalize_market_date(as_of_date_kst).isoformat()
                if as_of_date_kst is not None
                else None
            ),
            'completed_weeks': len(week_windows),
            'baseline_source': 'actual_short_term_execution_outcomes',
            'actual_outcomes_only': True,
            'date_window_basis': 'source_market_date',
            'cost_assumptions': {
                'fee_bps': _to_json_number(resolved_costs.fee_bps, places='0.000001'),
                'slippage_bps': _to_json_number(resolved_costs.slippage_bps, places='0.000001'),
                'notional_krw': _to_json_number(resolved_costs.notional_krw),
            },
            'top_examples_limit': resolved_top_examples_limit,
        }
        evidence = {
            'actual_entry_fill_required': True,
            'synthetic_outcomes_used': False,
            'realized_candidate_count': summary['baseline_trade_count'],
            'unresolved_open_candidate_count': summary['unresolved_open_candidate_count'],
            'missing_alignment_evidence_count': summary['missing_alignment_evidence_count'],
            'economic_cohort_basis': dataset.cohort_basis,
            'excluded_reason_counts': dict(dataset.excluded_reason_counts),
        }
        markdown_summary = _build_wait_conflict_markdown_summary(
            request=request,
            metadata=profitability_metadata,
            summary=summary,
            examples=examples,
            weeks=weeks,
            unresolved_open_candidates=unresolved_open_candidates,
            promotion_readiness=promotion_readiness,
        )
        return {
            'schema_version': WAIT_CONFLICT_GATE_REPLAY_SCHEMA_VERSION,
            'metadata': profitability_metadata,
            'request': request,
            'summary': summary,
            'evidence': evidence,
            'weeks': weeks,
            'unresolved_open_candidates': unresolved_open_candidates,
            'promotion_readiness': promotion_readiness,
            'examples': examples,
            'markdown_summary': markdown_summary,
        }


def _count_by_key(trades: Sequence[DeciderReplayTrade], *, key: str) -> dict[str, int]:
    counts: dict[str, int] = {}
    for trade in trades:
        value = getattr(trade, key)
        text = str(value)
        counts[text] = counts.get(text, 0) + 1
    return dict(sorted(counts.items()))


def _count_alignment(trades: Sequence[DeciderReplayTrade]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for trade in trades:
        alignment = _normalize_text(_trade_alignment_gate(trade).get('signal_decider_alignment')) or 'unknown'
        counts[alignment] = counts.get(alignment, 0) + 1
    return dict(sorted(counts.items()))


decider_replay_service = DeciderReplayService()
