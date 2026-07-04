from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
import json
from typing import Any, Sequence

from sqlalchemy import select

from app.agent_context.roles import SHORT_TERM_DECIDER_AGENT_NAME, SIGNAL_DECIDER_AGENT_NAME
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
    SIGNAL_DECIDER_ALIGNMENT_WAIT_CONFLICT_BLOCK,
    SIGNAL_DECIDER_ALIGNMENT_WAIT_CONFLICT_OVERRIDE,
    build_signal_decider_alignment_context,
    build_signal_decider_alignment_gate,
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
DEFAULT_TOP_EXAMPLES_LIMIT = 10
DEFAULT_WAIT_CONFLICT_REPLAY_MARKET_DATE_FROM = date(2026, 6, 29)
DEFAULT_WAIT_CONFLICT_REPLAY_MARKET_DATE_TO = date(2026, 7, 3)


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


@dataclass(frozen=True)
class PredicateResult:
    matched: bool
    reason: str | None = None


@dataclass(frozen=True)
class RuleEvaluationResult:
    skipped: bool
    reasons: tuple[str, ...]


def _normalize_market_date(value: str | date) -> date:
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value))


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


def _safe_decimal(value: Any) -> Decimal | None:
    if value is None or value == '' or isinstance(value, bool):
        return None
    if isinstance(value, Decimal):
        return value
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None


def _safe_decimal_or_zero(value: Any) -> Decimal:
    return _safe_decimal(value) or Decimal('0')


def _normalize_non_negative_decimal(value: Any, *, name: str) -> Decimal:
    resolved = _safe_decimal(value)
    if resolved is None:
        raise ValueError(f'{name} must be numeric')
    if resolved < 0:
        raise ValueError(f'{name} must be >= 0')
    return resolved


def _to_float(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, (int, float)):
        return float(value)
    try:
        return float(str(value).strip())
    except (TypeError, ValueError):
        return None


def _to_json_number(value: Decimal | float | int | None, *, places: str = '0.01') -> int | float | None:
    if value is None:
        return None
    decimal_value = value if isinstance(value, Decimal) else Decimal(str(value))
    quantized = decimal_value.quantize(Decimal(places), rounding=ROUND_HALF_UP)
    if quantized == quantized.to_integral():
        return int(quantized)
    return float(quantized)


def _to_ratio_number(value: float | Decimal | None) -> float | None:
    if value is None:
        return None
    return round(float(value), 6)


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


def _actual_outcome_to_trade(
    *,
    snapshot: SignalDeciderDecisionSnapshot,
    outcome: SignalDeciderActualExecutionOutcome,
    cost_assumptions: DeciderReplayCostAssumptions,
) -> DeciderReplayTrade:
    entry_notional = _safe_decimal(outcome.entry_notional_krw)
    if entry_notional is None:
        quantity = _safe_decimal(outcome.entry_filled_quantity)
        price = _safe_decimal(outcome.entry_average_fill_price)
        entry_notional = quantity * price if quantity is not None and price is not None else None
    notional = entry_notional if entry_notional is not None and entry_notional > 0 else cost_assumptions.notional_krw
    gross_pnl = _safe_decimal(outcome.realized_pnl_krw)
    realized_return = _to_float(outcome.realized_return_pct)
    if gross_pnl is None and realized_return is not None:
        gross_pnl = Decimal(str(realized_return)) * notional
    return DeciderReplayTrade(
        replay_trade_id=f'actual:{outcome.id}',
        snapshot_id=int(snapshot.id),
        agent_name=snapshot.agent_name,
        symbol=snapshot.symbol,
        market_date=snapshot.market_date,
        policy_version=snapshot.policy_version,
        baseline_gross_pnl_krw=gross_pnl or Decimal('0'),
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
) -> DeciderReplayTrade:
    validation = plan_row.validation_payload if isinstance(plan_row.validation_payload, dict) else {}
    traceability = plan_row.traceability_payload if isinstance(plan_row.traceability_payload, dict) else {}
    short_term = validation.get('short_term') if isinstance(validation.get('short_term'), dict) else {}
    response_payload = snapshot.response_payload if isinstance(snapshot.response_payload, dict) else {}
    tactical_action = (
        traceability.get('tactical_action')
        or short_term.get('tactical_action')
        or response_payload.get('tactical_action')
    )
    alignment_gate = build_signal_decider_alignment_gate(
        signal_decider_context=signal_decider_context,
        short_term_entry_quality=validation.get('short_term_entry_quality')
        if isinstance(validation.get('short_term_entry_quality'), dict)
        else None,
        tactical_action=str(tactical_action).strip().lower() if tactical_action is not None else None,
        mode=gate_mode,
    )
    entry_notional = _safe_decimal(outcome.entry_notional_krw)
    if entry_notional is None:
        quantity = _safe_decimal(outcome.entry_filled_quantity)
        price = _safe_decimal(outcome.entry_average_fill_price)
        entry_notional = quantity * price if quantity is not None and price is not None else None
    notional = entry_notional if entry_notional is not None and entry_notional > 0 else cost_assumptions.notional_krw
    gross_pnl = _safe_decimal(outcome.realized_pnl_krw)
    realized_return = _to_float(outcome.realized_return_pct)
    if gross_pnl is None and realized_return is not None:
        gross_pnl = Decimal(str(realized_return)) * notional
    point_in_time = _build_point_in_time_context(snapshot)
    point_in_time.update(
        {
            'market_date': outcome.target_trade_date.isoformat(),
            'source_market_date': plan_row.source_market_date.isoformat(),
            'target_trade_date': outcome.target_trade_date.isoformat(),
            'entry_plan_item_id': plan_row.id,
            'actual_execution_outcome_id': outcome.id,
            'signal_decider_alignment': alignment_gate.get('signal_decider_alignment'),
            'alignment_gate_action': alignment_gate.get('alignment_gate_action'),
            'alignment_gate_reason': alignment_gate.get('alignment_gate_reason'),
            'alignment_signal_decider_decision_snapshot_id': alignment_gate.get(
                'signal_decider_decision_snapshot_id'
            ),
            'alignment_gate': alignment_gate,
        }
    )
    return DeciderReplayTrade(
        replay_trade_id=f'actual_short_term:{outcome.id}',
        snapshot_id=int(snapshot.id),
        agent_name=snapshot.agent_name,
        symbol=snapshot.symbol,
        market_date=outcome.target_trade_date,
        policy_version=snapshot.policy_version,
        baseline_gross_pnl_krw=gross_pnl or Decimal('0'),
        baseline_return_pct=realized_return,
        baseline_notional_krw=notional,
        outcome_source='actual_short_term_execution_outcome',
        point_in_time=point_in_time,
        decision_timestamp=snapshot.feedback_received_at or snapshot.created_at,
        alignment_gate=alignment_gate,
    )


def _realized_outcome_to_trade(
    *,
    snapshot: SignalDeciderDecisionSnapshot,
    outcome: SignalDeciderRealizedOutcome,
    cost_assumptions: DeciderReplayCostAssumptions,
) -> DeciderReplayTrade:
    return_pct = _to_float(outcome.close_return_pct)
    gross_pnl = Decimal(str(return_pct or 0)) * cost_assumptions.notional_krw
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
) -> DeciderReplayTrade:
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
            return_pct = 0.0
    gross_pnl = Decimal(str(return_pct or 0)) * cost_assumptions.notional_krw
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
        horizon_days: int,
        cost_assumptions: DeciderReplayCostAssumptions,
    ) -> list[DeciderReplayTrade]:
        with session_scope() as session:
            stmt = select(SignalDeciderDecisionSnapshot).where(
                SignalDeciderDecisionSnapshot.market_date >= market_date_from,
                SignalDeciderDecisionSnapshot.market_date <= market_date_to,
                SignalDeciderDecisionSnapshot.agent_name == agent_name,
                SignalDeciderDecisionSnapshot.decision_action == 'buy',
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
            snapshot_ids = [int(row.id) for row in snapshot_rows]
            if not snapshot_ids:
                return []

            actual_rows = list(
                session.execute(
                    select(SignalDeciderActualExecutionOutcome)
                    .where(SignalDeciderActualExecutionOutcome.decision_snapshot_id.in_(snapshot_ids))
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

            actual_by_snapshot: dict[int, list[SignalDeciderActualExecutionOutcome]] = {}
            for row in actual_rows:
                if row.realized_pnl_krw is None and row.realized_return_pct is None:
                    continue
                actual_by_snapshot.setdefault(int(row.decision_snapshot_id), []).append(row)
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
            for snapshot in snapshot_rows:
                snapshot_id = int(snapshot.id)
                for actual in actual_by_snapshot.get(snapshot_id, []):
                    trades.append(
                        _actual_outcome_to_trade(
                            snapshot=snapshot,
                            outcome=actual,
                            cost_assumptions=cost_assumptions,
                        )
                    )
                if snapshot_id in actual_by_snapshot:
                    continue
                realized = realized_by_snapshot.get(snapshot_id)
                if realized is not None:
                    trades.append(
                        _realized_outcome_to_trade(
                            snapshot=snapshot,
                            outcome=realized,
                            cost_assumptions=cost_assumptions,
                        )
                    )
                    continue
                post_decision = post_decision_by_snapshot.get(snapshot_id)
                if post_decision is not None:
                    trades.append(
                        _post_decision_eval_to_trade(
                            snapshot=snapshot,
                            evaluation=post_decision,
                            cost_assumptions=cost_assumptions,
                        )
                    )
                    continue
                trades.append(
                    DeciderReplayTrade(
                        replay_trade_id=f'snapshot:{snapshot_id}',
                        snapshot_id=snapshot_id,
                        agent_name=snapshot.agent_name,
                        symbol=snapshot.symbol,
                        market_date=snapshot.market_date,
                        policy_version=snapshot.policy_version,
                        baseline_gross_pnl_krw=Decimal('0'),
                        baseline_return_pct=None,
                        baseline_notional_krw=cost_assumptions.notional_krw,
                        outcome_source='decision_snapshot_no_outcome',
                        point_in_time=_build_point_in_time_context(snapshot),
                        decision_timestamp=snapshot.feedback_received_at or snapshot.created_at,
                    )
                )
            return sorted(trades, key=lambda item: (item.market_date, item.symbol, item.replay_trade_id))

    def list_wait_conflict_gate_trades(
        self,
        *,
        market_date_from: date,
        market_date_to: date,
        symbols: Sequence[str] | None,
        cost_assumptions: DeciderReplayCostAssumptions,
        gate_mode: str,
    ) -> list[DeciderReplayTrade]:
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
                    SignalDeciderActualExecutionOutcome.target_trade_date >= market_date_from,
                    SignalDeciderActualExecutionOutcome.target_trade_date <= market_date_to,
                )
                .order_by(
                    SignalDeciderActualExecutionOutcome.target_trade_date.asc(),
                    SignalDeciderActualExecutionOutcome.symbol.asc(),
                    SignalDeciderActualExecutionOutcome.id.asc(),
                )
            )
            if symbols:
                stmt = stmt.where(SignalDeciderActualExecutionOutcome.symbol.in_(list(symbols)))
            rows = [
                row
                for row in session.execute(stmt).all()
                if row[0].realized_pnl_krw is not None or row[0].realized_return_pct is not None
            ]
            if not rows:
                return []

            source_dates = sorted({row[1].source_market_date for row in rows})
            signal_symbols = sorted({row[0].symbol for row in rows})
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
            signal_by_context: dict[tuple[date, str], dict[str, Any]] = {}
            for signal_row in session.execute(signal_stmt).scalars().all():
                key = (signal_row.market_date, signal_row.symbol)
                if key in signal_by_context:
                    continue
                signal_by_context[key] = build_signal_decider_alignment_context(signal_row) or {}

            trades: list[DeciderReplayTrade] = []
            for outcome, plan_row, snapshot in rows:
                signal_context = signal_by_context.get((plan_row.source_market_date, outcome.symbol))
                trades.append(
                    _actual_short_term_outcome_to_wait_conflict_trade(
                        snapshot=snapshot,
                        plan_row=plan_row,
                        outcome=outcome,
                        signal_decider_context=signal_context,
                        cost_assumptions=cost_assumptions,
                        gate_mode=gate_mode,
                    )
                )
            return sorted(trades, key=lambda item: (item.market_date, item.symbol, item.replay_trade_id))


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
        return parsed
    if not isinstance(rule_config, dict):
        raise ValueError('rule_config must be a dict or JSON object string')
    return rule_config


def _as_rule_list(rule_config: dict[str, Any]) -> list[dict[str, Any]]:
    rules = rule_config.get('rules')
    if isinstance(rules, list):
        return [rule for rule in rules if isinstance(rule, dict)]
    predicates = rule_config.get('predicates')
    if isinstance(predicates, list):
        return [{'name': rule_config.get('name') or 'candidate_rule', 'predicates': predicates}]
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
        matched = _compare_values(
            point_in_time.get('risk_flags') or [],
            op='contains_all' if match_mode == 'all' else 'contains_any',
            values=expected,
        )
        return PredicateResult(matched=matched, reason=f'risk_flags contains {",".join(expected)}')

    if predicate_type == 'liquidity_bucket':
        values = [str(item) for item in _values_from_predicate(predicate)]
        matched = _compare_values(point_in_time.get('liquidity_bucket'), op='in', values=values)
        return PredicateResult(matched=matched, reason=f'liquidity_bucket in {",".join(values)}')

    if predicate_type == 'factor_score_bucket':
        factor = str(predicate.get('factor') or '').strip()
        bucket = _maybe_dict(point_in_time.get('factor_score_buckets')).get(factor)
        values = [str(item) for item in _values_from_predicate(predicate)]
        matched = _compare_values(bucket, op='in', values=values)
        return PredicateResult(matched=matched, reason=f'factor_score_bucket[{factor}] in {",".join(values)}')

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


def _win_rate(trades: Sequence[DeciderReplayTrade]) -> float | None:
    if not trades:
        return None
    winners = sum(1 for trade in trades if trade.baseline_gross_pnl_krw > 0)
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
        'average_return_pct': _average_return(trades),
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
    return {
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


def _build_markdown_summary(
    *,
    request: dict[str, Any],
    summary: dict[str, Any],
    examples: Sequence[dict[str, Any]],
) -> str:
    lines = [
        '# Decider replay summary',
        '',
        f'- Window: {request["market_date_from"]}..{request["market_date_to"]}',
        f'- Agent: {request["agent_name"]}',
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


def _build_wait_conflict_markdown_summary(
    *,
    request: dict[str, Any],
    summary: dict[str, Any],
    examples: Sequence[dict[str, Any]],
) -> str:
    lines = [
        '# Short-term WAIT-conflict gate replay',
        '',
        f'- Execution window: {request["market_date_from"]}..{request["market_date_to"]}',
        '- Baseline source: actual short-term execution outcomes only',
        f'- Baseline trades: {summary["baseline_trade_count"]}',
        f'- Candidate trades: {summary["candidate_trade_count"]}',
        f'- Blocked candidates: {summary["blocked_candidate_count"]}',
        f'- Allowed by override: {summary["allowed_by_override_count"]}',
        f'- Skipped losses: {summary["skipped_losses"]}',
        f'- Skipped winners: {summary["skipped_winners"]}',
        f'- Gross P/L delta KRW: {summary["gross_pnl_delta_krw"]}',
        f'- Estimated net P/L delta KRW: {summary["estimated_net_pnl_delta_krw"]}',
    ]
    if summary.get('empty_data_reason'):
        lines.append(f'- Empty-data reason: {summary["empty_data_reason"]}')
    lines.extend(['', '## Blocked examples'])
    if not examples:
        lines.append('No blocked WAIT-conflict examples.')
        return '\n'.join(lines)
    lines.append('| execution date | symbol | reason | gross pnl KRW | return |')
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
        trades = self.repository.list_replay_trades(
            market_date_from=normalized_from,
            market_date_to=normalized_to,
            agent_name=normalized_agent,
            symbols=normalized_symbols or None,
            horizon_days=resolved_horizon_days,
            cost_assumptions=resolved_costs,
        )

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
        drawdown_delta = _delta(candidate.get('max_drawdown_proxy_krw'), baseline.get('max_drawdown_proxy_krw'))
        request = {
            'market_date_from': normalized_from.isoformat(),
            'market_date_to': normalized_to.isoformat(),
            'agent_name': normalized_agent,
            'symbols': normalized_symbols,
            'horizon_days': resolved_horizon_days,
            'rule_config': resolved_rule_config,
            'cost_assumptions': {
                'fee_bps': _to_json_number(resolved_costs.fee_bps, places='0.000001'),
                'slippage_bps': _to_json_number(resolved_costs.slippage_bps, places='0.000001'),
                'notional_krw': _to_json_number(resolved_costs.notional_krw),
            },
            'top_examples_limit': resolved_top_examples_limit,
        }
        summary = {
            'baseline_trade_count': baseline['trade_count'],
            'candidate_trade_count': candidate['trade_count'],
            'skipped_buys': len(skipped),
            'skipped_winners': sum(1 for trade, _ in skipped if trade.baseline_gross_pnl_krw > 0),
            'avoided_losers': sum(1 for trade, _ in skipped if trade.baseline_gross_pnl_krw < 0),
            'gross_pnl_delta_krw': _to_json_number(_round_money(gross_delta)),
            'estimated_net_pnl_delta_krw': _to_json_number(_round_money(net_delta)),
            'win_rate_delta': _delta(candidate.get('win_rate'), baseline.get('win_rate')),
            'average_return_delta': _delta(candidate.get('average_return_pct'), baseline.get('average_return_pct')),
            'max_drawdown_proxy_delta_krw': drawdown_delta,
            'baseline': baseline,
            'candidate': candidate,
            'outcome_sources': _count_by_key(trades, key='outcome_source'),
        }
        markdown_summary = _build_markdown_summary(
            request=request,
            summary=summary,
            examples=examples,
        )
        return {
            'schema_version': DECIDER_REPLAY_SCHEMA_VERSION,
            'request': request,
            'summary': summary,
            'examples': examples,
            'markdown_summary': markdown_summary,
        }

    def replay_wait_conflict_gate(
        self,
        *,
        market_date_from: str | date = DEFAULT_WAIT_CONFLICT_REPLAY_MARKET_DATE_FROM,
        market_date_to: str | date = DEFAULT_WAIT_CONFLICT_REPLAY_MARKET_DATE_TO,
        symbols: Sequence[str] | str | None = None,
        fee_bps: Decimal | float | int | str = Decimal('0'),
        slippage_bps: Decimal | float | int | str = Decimal('0'),
        notional_krw: Decimal | float | int | str = DEFAULT_REPLAY_NOTIONAL_KRW,
        gate_mode: str = 'hard_block',
        top_examples_limit: int = DEFAULT_TOP_EXAMPLES_LIMIT,
    ) -> dict[str, Any]:
        normalized_from = _normalize_market_date(market_date_from)
        normalized_to = _normalize_market_date(market_date_to)
        if normalized_from > normalized_to:
            raise ValueError('market_date_from must be on or before market_date_to')
        normalized_symbols = _normalize_symbols(symbols)
        resolved_costs = DeciderReplayCostAssumptions(
            fee_bps=_normalize_non_negative_decimal(fee_bps, name='fee_bps'),
            slippage_bps=_normalize_non_negative_decimal(slippage_bps, name='slippage_bps'),
            notional_krw=_normalize_non_negative_decimal(notional_krw, name='notional_krw'),
        )
        if resolved_costs.notional_krw <= 0:
            raise ValueError('notional_krw must be > 0')
        resolved_top_examples_limit = max(0, int(top_examples_limit))
        trades = self.repository.list_wait_conflict_gate_trades(
            market_date_from=normalized_from,
            market_date_to=normalized_to,
            symbols=normalized_symbols or None,
            cost_assumptions=resolved_costs,
            gate_mode=gate_mode,
        )

        blocked = [
            trade
            for trade in trades
            if _trade_alignment_gate(trade).get('signal_decider_alignment')
            == SIGNAL_DECIDER_ALIGNMENT_WAIT_CONFLICT_BLOCK
            and _trade_alignment_gate(trade).get('alignment_gate_action') == ALIGNMENT_GATE_ACTION_BLOCK
        ]
        allowed_by_override = [
            trade
            for trade in trades
            if _trade_alignment_gate(trade).get('signal_decider_alignment')
            == SIGNAL_DECIDER_ALIGNMENT_WAIT_CONFLICT_OVERRIDE
        ]
        kept = [trade for trade in trades if trade not in blocked]
        baseline = _metric_block(trades, cost_assumptions=resolved_costs)
        candidate = _metric_block(kept, cost_assumptions=resolved_costs)
        gross_delta = _safe_decimal_or_zero(candidate['gross_pnl_krw']) - _safe_decimal_or_zero(baseline['gross_pnl_krw'])
        net_delta = _safe_decimal_or_zero(candidate['estimated_net_pnl_krw']) - _safe_decimal_or_zero(
            baseline['estimated_net_pnl_krw']
        )
        examples = _sort_examples(
            [
                _example_from_trade(
                    trade=trade,
                    reasons=_wait_conflict_reason(trade),
                    cost_assumptions=resolved_costs,
                )
                for trade in blocked
            ]
        )[:resolved_top_examples_limit]
        request = {
            'market_date_from': normalized_from.isoformat(),
            'market_date_to': normalized_to.isoformat(),
            'agent_name': SHORT_TERM_DECIDER_AGENT_NAME,
            'symbols': normalized_symbols,
            'candidate_gate_mode': gate_mode,
            'baseline_source': 'actual_short_term_execution_outcomes',
            'cost_assumptions': {
                'fee_bps': _to_json_number(resolved_costs.fee_bps, places='0.000001'),
                'slippage_bps': _to_json_number(resolved_costs.slippage_bps, places='0.000001'),
                'notional_krw': _to_json_number(resolved_costs.notional_krw),
            },
            'top_examples_limit': resolved_top_examples_limit,
        }
        summary = {
            'baseline_trade_count': baseline['trade_count'],
            'candidate_trade_count': candidate['trade_count'],
            'blocked_candidate_count': len(blocked),
            'allowed_by_override_count': len(allowed_by_override),
            'allowed_non_conflict_count': max(len(kept) - len(allowed_by_override), 0),
            'skipped_losses': sum(1 for trade in blocked if trade.baseline_gross_pnl_krw < 0),
            'skipped_winners': sum(1 for trade in blocked if trade.baseline_gross_pnl_krw > 0),
            'skipped_losses_gross_pnl_krw': _to_json_number(
                _round_money(sum((trade.baseline_gross_pnl_krw for trade in blocked if trade.baseline_gross_pnl_krw < 0), Decimal('0')))
            ),
            'skipped_winners_gross_pnl_krw': _to_json_number(
                _round_money(sum((trade.baseline_gross_pnl_krw for trade in blocked if trade.baseline_gross_pnl_krw > 0), Decimal('0')))
            ),
            'gross_pnl_delta_krw': _to_json_number(_round_money(gross_delta)),
            'estimated_net_pnl_delta_krw': _to_json_number(_round_money(net_delta)),
            'baseline': baseline,
            'candidate': candidate,
            'outcome_sources': _count_by_key(trades, key='outcome_source'),
            'alignment_counts': _count_alignment(trades),
            'empty_data_reason': (
                'no_actual_short_term_execution_outcomes_in_range'
                if not trades
                else None
            ),
        }
        markdown_summary = _build_wait_conflict_markdown_summary(
            request=request,
            summary=summary,
            examples=examples,
        )
        return {
            'schema_version': WAIT_CONFLICT_GATE_REPLAY_SCHEMA_VERSION,
            'request': request,
            'summary': summary,
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
