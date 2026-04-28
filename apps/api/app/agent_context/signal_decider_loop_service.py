from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from decimal import Decimal
import hashlib
from typing import Any, Sequence

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert

from app.agent_context.roles import SIGNAL_DECIDER_AGENT_NAME
from app.db import session_scope
from app.db_models import (
    AgentContextFeedback,
    SignalDeciderDecisionSnapshot,
    SignalDeciderEvaluation,
    SignalDeciderPolicyCandidate,
    SignalDeciderRealizedOutcome,
)
from app.market_data_store import load_daily_bars_for_symbol_range

DEFAULT_SIGNAL_DECIDER_POLICY_VERSION = 'signal_decider_policy_v1'
DEFAULT_SIGNAL_DECIDER_EVALUATION_VERSION = 'signal_decider_eval_v1'
DEFAULT_POLICY_CANDIDATE_STATUS = 'generated'
DEFAULT_SIGNAL_DECIDER_EVALUATION_LOOKBACK_DAYS = 30
SIGNAL_DECIDER_EVALUATION_HORIZONS: tuple[int, ...] = (1, 3, 5, 10)
SIGNAL_DECIDER_ACTIONS: tuple[str, ...] = ('buy', 'wait', 'reject')
SIGNAL_DECIDER_GRADE_LABELS: tuple[str, ...] = (
    'good_buy',
    'bad_buy',
    'missed_opportunity_after_wait',
    'correct_wait',
    'missed_opportunity_after_reject',
    'correct_reject',
    'pending_horizon',
)
NEGATIVE_SIGNAL_DECIDER_GRADE_LABELS: tuple[str, ...] = (
    'bad_buy',
    'missed_opportunity_after_wait',
    'missed_opportunity_after_reject',
)


@dataclass(frozen=True)
class HorizonThreshold:
    good_return_pct: float
    opportunity_return_pct: float
    loss_return_pct: float


@dataclass(frozen=True)
class SignalDeciderEvaluationProfile:
    requested_version: str
    rule_set_version: str
    buy_severe_drawdown_multiplier: float = 1.5


HORIZON_THRESHOLDS: dict[int, HorizonThreshold] = {
    1: HorizonThreshold(good_return_pct=0.010, opportunity_return_pct=0.015, loss_return_pct=0.010),
    3: HorizonThreshold(good_return_pct=0.020, opportunity_return_pct=0.025, loss_return_pct=0.020),
    5: HorizonThreshold(good_return_pct=0.030, opportunity_return_pct=0.035, loss_return_pct=0.025),
    10: HorizonThreshold(good_return_pct=0.050, opportunity_return_pct=0.060, loss_return_pct=0.035),
}


def _normalize_market_date(value: str | date) -> date:
    if isinstance(value, date):
        return value
    return date.fromisoformat(value)


def _normalize_optional_symbol(value: str | None) -> str | None:
    if value is None:
        return None
    normalized = value.strip().upper()
    return normalized or None


def _normalize_symbols(symbols: Sequence[str] | None) -> list[str]:
    if not symbols:
        return []
    normalized: list[str] = []
    seen: set[str] = set()
    for raw_symbol in symbols:
        for piece in (raw_symbol or '').split(','):
            symbol = piece.strip().upper()
            if not symbol or symbol in seen:
                continue
            seen.add(symbol)
            normalized.append(symbol)
    return normalized


def _normalize_optional_action_filter(value: str | None) -> str | None:
    if value is None:
        return None
    normalized = str(value).strip().lower()
    if not normalized:
        return None
    if normalized not in SIGNAL_DECIDER_ACTIONS:
        raise ValueError(f'action must be one of {list(SIGNAL_DECIDER_ACTIONS)}')
    return normalized


def _normalize_optional_verdict(value: str | None) -> str | None:
    if value is None:
        return None
    normalized = str(value).strip().lower()
    if not normalized:
        return None
    if normalized not in SIGNAL_DECIDER_GRADE_LABELS:
        raise ValueError(f'verdict must be one of {list(SIGNAL_DECIDER_GRADE_LABELS)}')
    return normalized


def _normalize_limit(limit: int | None, *, default: int) -> int:
    if limit is None:
        return default
    return max(1, limit)


def _normalize_horizons(horizons: Sequence[int] | None) -> list[int]:
    if not horizons:
        return list(SIGNAL_DECIDER_EVALUATION_HORIZONS)
    normalized = sorted({int(value) for value in horizons})
    invalid = [value for value in normalized if value not in SIGNAL_DECIDER_EVALUATION_HORIZONS]
    if invalid:
        raise ValueError(
            f'horizons must be drawn from {list(SIGNAL_DECIDER_EVALUATION_HORIZONS)}; got {invalid}'
        )
    return normalized


def _resolve_market_date_window(
    *,
    market_date: str | date | None = None,
    market_date_from: str | date | None = None,
    market_date_to: str | date | None = None,
) -> tuple[date, date]:
    if market_date is not None:
        resolved = _normalize_market_date(market_date)
        return resolved, resolved
    if market_date_from is None and market_date_to is None:
        raise ValueError('market_date or market_date_from/market_date_to is required')
    resolved_from = _normalize_market_date(market_date_from or market_date_to)
    resolved_to = _normalize_market_date(market_date_to or market_date_from)
    if resolved_from > resolved_to:
        raise ValueError('market_date_from must be on or before market_date_to')
    return resolved_from, resolved_to


def _normalize_optional_text(value: str | None, *, default: str | None = None) -> str | None:
    normalized = str(value or '').strip()
    if normalized:
        return normalized
    return default


def _normalize_optional_datetime(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        normalized = value.strip()
        if not normalized:
            return None
        if normalized.endswith('Z'):
            normalized = normalized[:-1] + '+00:00'
        return datetime.fromisoformat(normalized)
    return None


def _normalize_action(value: Any) -> str:
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {'buy', 'wait', 'reject'}:
            return normalized
    return 'wait'


def _to_float(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value.strip())
        except ValueError:
            return None
    return None


def _to_int(value: Any) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, Decimal):
        return int(value)
    if isinstance(value, float):
        return int(round(value))
    if isinstance(value, str):
        try:
            return int(float(value.strip()))
        except ValueError:
            return None
    return None


def _normalize_text_list(value: Any) -> list[str]:
    if isinstance(value, str):
        normalized = value.strip()
        return [normalized] if normalized else []
    if not isinstance(value, list):
        return []
    items: list[str] = []
    seen: set[str] = set()
    for item in value:
        if item is None:
            continue
        normalized = str(item).strip()
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        items.append(normalized)
    return items


def _maybe_dict(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    return {}


def _signal_score_bucket(score: float | None) -> str | None:
    if score is None:
        return None
    if score >= 70:
        return 'high'
    if score >= 40:
        return 'medium'
    if score <= -70:
        return 'strong_negative'
    if score <= -40:
        return 'negative'
    return 'low'


def _signal_count_bucket(count: int | None) -> str | None:
    if count is None:
        return None
    if count >= 3:
        return '3plus'
    if count >= 2:
        return '2'
    if count >= 1:
        return '1'
    return '0'


def build_signal_decider_feature_snapshot(
    *,
    symbol_day_context: dict[str, Any] | None,
    feedback_payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    context = _maybe_dict(symbol_day_context)
    feedback = _maybe_dict(feedback_payload)
    technical = _maybe_dict(context.get('technical_snapshot'))
    action_summary = _maybe_dict(context.get('action_summary'))
    signal_features = _maybe_dict(context.get('signal_features'))
    price_context = _maybe_dict(context.get('price_context'))
    decision_support = _maybe_dict(_maybe_dict(feedback.get('details')).get('decision_support'))
    signal_score = _to_float(action_summary.get('signal_score'))
    signal_count = _to_int(signal_features.get('signal_count'))
    supporting_evidence = _normalize_text_list(
        feedback.get('supporting_evidence') or decision_support.get('supporting_evidence')
    )
    decision_risks = _normalize_text_list(
        feedback.get('decision_risks') or decision_support.get('decision_risks')
    )
    data_warnings = _normalize_text_list(
        feedback.get('data_quality_warnings') or decision_support.get('data_quality_warnings')
    )
    disagreement = _normalize_text_list(
        feedback.get('specialist_disagreement') or decision_support.get('specialist_disagreement')
    )
    return {
        'technical': {
            'trend_state': technical.get('trend_state'),
            'ma_alignment': technical.get('ma_alignment'),
            'breakout_20d': bool(technical.get('breakout_20d')) if technical.get('breakout_20d') is not None else None,
            'breakdown_20d': bool(technical.get('breakdown_20d')) if technical.get('breakdown_20d') is not None else None,
            'return_5d': _to_float(technical.get('return_5d')),
            'return_20d': _to_float(technical.get('return_20d')),
            'volume_ratio_20d': _to_float(technical.get('volume_ratio_20d')),
            'atr_pct_14': _to_float(technical.get('atr_pct_14')),
        },
        'action_summary': {
            'action_label': action_summary.get('action_label'),
            'bias_direction': action_summary.get('bias_direction'),
            'signal_score': signal_score,
            'signal_score_bucket': _signal_score_bucket(signal_score),
            'confidence_tier': action_summary.get('confidence_tier'),
            'risk_tier': action_summary.get('risk_tier'),
        },
        'signal_features': {
            'signal_count': signal_count,
            'signal_count_bucket': _signal_count_bucket(signal_count),
            'volume_power_rank': _to_int(signal_features.get('volume_power_rank')),
            'overtime_volume_rank': _to_int(signal_features.get('overtime_volume_rank')),
            'overtime_fluctuation_rank': _to_int(signal_features.get('overtime_fluctuation_rank')),
        },
        'price_context': {
            'close_location_ratio': _to_float(price_context.get('close_location_ratio')),
            'gap_vs_prev_close_pct': _to_float(price_context.get('gap_vs_prev_close_pct')),
            'range_pct': _to_float(price_context.get('range_pct')),
        },
        'meta': {
            'supporting_evidence_count': len(supporting_evidence),
            'decision_risk_count': len(decision_risks),
            'data_warning_count': len(data_warnings),
            'disagreement_count': len(disagreement),
            'has_data_warnings': bool(data_warnings),
            'has_disagreement': bool(disagreement),
        },
    }


def _build_feature_flags(feature_snapshot: dict[str, Any]) -> list[tuple[str, str]]:
    technical = _maybe_dict(feature_snapshot.get('technical'))
    action_summary = _maybe_dict(feature_snapshot.get('action_summary'))
    signal_features = _maybe_dict(feature_snapshot.get('signal_features'))
    meta = _maybe_dict(feature_snapshot.get('meta'))
    flags: list[tuple[str, str]] = []
    for key in ('trend_state', 'ma_alignment'):
        value = _normalize_optional_text(technical.get(key))
        if value:
            flags.append((f'technical.{key}', value))
    if technical.get('breakout_20d') is True:
        flags.append(('technical.breakout_20d', 'true'))
    if technical.get('breakdown_20d') is True:
        flags.append(('technical.breakdown_20d', 'true'))
    for key in ('action_label', 'confidence_tier', 'risk_tier', 'signal_score_bucket'):
        value = _normalize_optional_text(action_summary.get(key))
        if value:
            flags.append((f'action_summary.{key}', value))
    signal_count_bucket = _normalize_optional_text(signal_features.get('signal_count_bucket'))
    if signal_count_bucket:
        flags.append(('signal_features.signal_count_bucket', signal_count_bucket))
    if meta.get('has_data_warnings') is True:
        flags.append(('meta.has_data_warnings', 'true'))
    if meta.get('has_disagreement') is True:
        flags.append(('meta.has_disagreement', 'true'))
    return flags


def _policy_version_from_payloads(
    *,
    summary_payload: dict[str, Any],
    feedback_payload: dict[str, Any],
    context_payload: dict[str, Any] | None,
) -> str:
    context = _maybe_dict(context_payload)
    policy_info = _maybe_dict(context.get('signal_decider_policy'))
    trace_info = _maybe_dict(context.get('signal_decider_trace'))
    return (
        _normalize_optional_text(feedback_payload.get('policy_version'))
        or _normalize_optional_text(summary_payload.get('policy_version'))
        or _normalize_optional_text(policy_info.get('current_version'))
        or _normalize_optional_text(trace_info.get('policy_version'))
        or DEFAULT_SIGNAL_DECIDER_POLICY_VERSION
    )


def build_signal_decider_traceability_snapshot(
    *,
    agent_name: str,
    agent_version: str | None,
    feedback_id: int | None,
    decision_run_id: str | None,
    market_date: date,
    symbol: str,
    policy_version: str,
    decision_action: str,
    thesis_summary: str,
    summary_payload: dict[str, Any],
    feedback_payload: dict[str, Any],
    context_payload: dict[str, Any] | None,
    feature_snapshot: dict[str, Any],
) -> dict[str, Any]:
    context = _maybe_dict(context_payload)
    raw_traceability = _maybe_dict(feedback_payload.get('traceability'))
    decision_support = _maybe_dict(_maybe_dict(feedback_payload.get('details')).get('decision_support'))
    policy_payload = _maybe_dict(context.get('signal_decider_policy'))
    orchestration = _maybe_dict(context.get('orchestration'))
    decision_support_constraints = _maybe_dict(context.get('decision_support_constraints'))
    upstream_traceability = _maybe_dict(context.get('upstream_traceability'))
    upstream_summaries = _maybe_dict(context.get('upstream_summaries'))
    supporting_evidence = _normalize_text_list(
        feedback_payload.get('supporting_evidence') or decision_support.get('supporting_evidence')
    )
    decision_risks = _normalize_text_list(
        feedback_payload.get('decision_risks') or decision_support.get('decision_risks')
    )
    invalidation_conditions = _normalize_text_list(
        feedback_payload.get('invalidation_conditions') or decision_support.get('invalidation_conditions')
    )
    data_quality_warnings = _normalize_text_list(
        feedback_payload.get('data_quality_warnings') or decision_support.get('data_quality_warnings')
    )
    specialist_disagreement = _normalize_text_list(
        feedback_payload.get('specialist_disagreement') or decision_support.get('specialist_disagreement')
    )
    required_upstream_roles = [
        str(role).strip()
        for role in orchestration.get('required_upstream_roles', [])
        if str(role).strip()
    ]
    if not required_upstream_roles:
        required_upstream_roles = sorted(
            {
                *[
                    str(role).strip()
                    for role in upstream_traceability.keys()
                    if str(role).strip()
                ],
                *[
                    str(role).strip()
                    for role in upstream_summaries.keys()
                    if str(role).strip()
                ],
            }
        )
    available_upstream_roles = sorted(
        {
            *[
                str(role).strip()
                for role in upstream_traceability.keys()
                if str(role).strip()
            ],
            *[
                str(role).strip()
                for role in upstream_summaries.keys()
                if str(role).strip()
            ],
        }
    )
    factor_flags = [{'key': key, 'value': value} for key, value in _build_feature_flags(feature_snapshot)]
    traceability_symbol = _normalize_optional_text(
        str(raw_traceability.get('symbol')) if raw_traceability.get('symbol') is not None else None
    )
    traceability_market_date = _normalize_optional_text(
        str(raw_traceability.get('market_date')) if raw_traceability.get('market_date') is not None else None
    )
    traceability_agent_name = _normalize_optional_text(
        str(raw_traceability.get('agent_name')) if raw_traceability.get('agent_name') is not None else None
    )
    upstream_references: dict[str, dict[str, Any]] = {}
    for role in available_upstream_roles:
        trace_info = _maybe_dict(upstream_traceability.get(role))
        upstream_references[role] = {
            'feedback_id': trace_info.get('feedback_id'),
            'run_id': trace_info.get('run_id'),
            'market_date': trace_info.get('market_date'),
            'received_at': trace_info.get('received_at'),
            'source': trace_info.get('source'),
        }
    return {
        'schema_version': 'signal_decider_trace_v2',
        'agent': {
            'agent_name': agent_name,
            'agent_version': agent_version,
            'model': summary_payload.get('model'),
            'feedback_id': feedback_id,
            'decision_run_id': decision_run_id,
        },
        'policy': {
            'version': policy_version,
            'activation_mode': policy_payload.get('activation_mode', 'manual_only'),
            'candidate_activation_required': bool(policy_payload.get('candidate_activation_required', True)),
        },
        'orchestration': orchestration,
        'upstream_references': upstream_references,
        'gate_checks': {
            'required_upstream_roles': required_upstream_roles,
            'available_upstream_roles': available_upstream_roles,
            'missing_upstream_roles': [
                role for role in required_upstream_roles if role not in set(available_upstream_roles)
            ],
            'decision_support_only': bool(decision_support_constraints.get('decision_support_only')),
            'allow_execution_logic': bool(decision_support_constraints.get('allow_execution_logic', False)),
            'has_data_warnings': bool(data_quality_warnings),
            'has_specialist_disagreement': bool(specialist_disagreement),
            'traceability_symbol_matches': traceability_symbol in {None, symbol},
            'traceability_market_date_matches': traceability_market_date in {None, market_date.isoformat()},
            'traceability_agent_name_matches': traceability_agent_name in {None, agent_name},
        },
        'factor_contributions': {
            'supporting_evidence': supporting_evidence,
            'decision_risks': decision_risks,
            'invalidation_conditions': invalidation_conditions,
            'data_quality_warnings': data_quality_warnings,
            'specialist_disagreement': specialist_disagreement,
            'feature_flags': factor_flags,
            'feature_snapshot_meta': _maybe_dict(feature_snapshot.get('meta')),
        },
        'final_action_rationale': {
            'decision_action': decision_action,
            'summary': thesis_summary,
            'confidence_score_10': _to_float(
                feedback_payload.get('confidence_score_10') or decision_support.get('confidence_score_10')
            ),
            'conviction_score_10': _to_float(
                feedback_payload.get('conviction_score_10') or decision_support.get('conviction_score_10')
            ),
            'expected_return_pct': _to_float(
                feedback_payload.get('expected_return_pct') or decision_support.get('expected_return_pct')
            ),
            'target_price': _to_int(feedback_payload.get('target_price') or decision_support.get('target_price')),
            'stop_loss_price': _to_int(
                feedback_payload.get('stop_loss_price') or decision_support.get('stop_loss_price')
            ),
        },
        'raw_traceability': raw_traceability,
    }


def _serialize_outcome_row(row: SignalDeciderRealizedOutcome) -> dict[str, Any]:
    diagnostics = _maybe_dict(row.diagnostics_payload)
    return {
        'id': row.id,
        'decision_snapshot_id': row.decision_snapshot_id,
        'feedback_id': row.feedback_id,
        'market_date': row.market_date.isoformat(),
        'symbol': row.symbol,
        'policy_version': row.policy_version,
        'decision_action': row.decision_action,
        'horizon_days': row.horizon_days,
        'outcome_status': row.outcome_status,
        'entry_trade_date': row.entry_trade_date.isoformat() if row.entry_trade_date else None,
        'exit_trade_date': row.exit_trade_date.isoformat() if row.exit_trade_date else None,
        'bars_observed': row.bars_observed,
        'entry_price': row.entry_price,
        'exit_price': row.exit_price,
        'max_high_price': row.max_high_price,
        'min_low_price': row.min_low_price,
        'close_return_pct': _to_float(row.close_return_pct),
        'max_upside_return_pct': _to_float(row.max_upside_return_pct),
        'max_drawdown_return_pct': _to_float(row.max_drawdown_return_pct),
        'target_price': row.target_price,
        'stop_loss_price': row.stop_loss_price,
        'expected_return_pct': _to_float(row.expected_return_pct),
        'target_hit': bool(row.target_hit),
        'stop_loss_hit': bool(row.stop_loss_hit),
        'target_stop_collision': bool(row.target_stop_collision),
        'best_exit_trade_date': diagnostics.get('best_exit_trade_date'),
        'worst_drawdown_trade_date': diagnostics.get('worst_drawdown_trade_date'),
        'first_target_hit_trade_date': diagnostics.get('first_target_hit_trade_date'),
        'first_stop_loss_hit_trade_date': diagnostics.get('first_stop_loss_hit_trade_date'),
        'price_path': row.price_path_payload,
        'diagnostics': diagnostics,
        'computed_at': row.computed_at.isoformat() if row.computed_at else None,
    }


def _serialize_evaluation_row(row: SignalDeciderEvaluation) -> dict[str, Any]:
    return {
        'id': row.id,
        'decision_snapshot_id': row.decision_snapshot_id,
        'feedback_id': row.feedback_id,
        'realized_outcome_id': row.realized_outcome_id,
        'market_date': row.market_date.isoformat(),
        'symbol': row.symbol,
        'policy_version': row.policy_version,
        'decision_action': row.decision_action,
        'horizon_days': row.horizon_days,
        'evaluation_version': row.evaluation_version,
        'evaluation_status': row.evaluation_status,
        'grade_label': row.grade_label,
        'grade_score': _to_float(row.grade_score),
        'missed_opportunity': row.missed_opportunity,
        'counterfactual_action': row.counterfactual_action,
        'rationale': row.rationale_payload,
        'diagnostics': row.diagnostics_payload,
        'evaluated_at': row.evaluated_at.isoformat() if row.evaluated_at else None,
    }


def _serialize_policy_candidate_row(row: SignalDeciderPolicyCandidate) -> dict[str, Any]:
    return {
        'id': row.id,
        'candidate_version': row.candidate_version,
        'base_policy_version': row.base_policy_version,
        'evaluation_version': row.evaluation_version,
        'market_date_from': row.market_date_from.isoformat(),
        'market_date_to': row.market_date_to.isoformat(),
        'horizon_days': row.horizon_days,
        'focus_action': row.focus_action,
        'candidate_kind': row.candidate_kind,
        'feature_key': row.feature_key,
        'feature_value': row.feature_value,
        'affected_decisions_count': row.affected_decisions_count,
        'support_count': row.support_count,
        'contradiction_count': row.contradiction_count,
        'support_ratio': _to_float(row.support_ratio),
        'status': row.status,
        'activation_mode': row.activation_mode,
        'summary': row.summary,
        'hypothesis': row.hypothesis,
        'evidence': row.evidence_payload,
        'recommendation': row.recommendation_payload,
        'generated_at': row.generated_at.isoformat() if row.generated_at else None,
        'activated_at': row.activated_at.isoformat() if row.activated_at else None,
    }


class SignalDeciderLoopRepository:
    def insert_decision_snapshots(
        self,
        *,
        session,
        items: Sequence[dict[str, Any]],
    ) -> list[SignalDeciderDecisionSnapshot]:
        if not items:
            return []
        rows = [SignalDeciderDecisionSnapshot(**item) for item in items]
        session.add_all(rows)
        session.flush()
        return rows

    def list_decision_snapshots(
        self,
        *,
        market_date: date | None = None,
        market_date_from: date | None = None,
        market_date_to: date | None = None,
        symbols: Sequence[str] | None = None,
        decision_actions: Sequence[str] | None = None,
        policy_versions: Sequence[str] | None = None,
        limit: int | None = None,
    ) -> list[SignalDeciderDecisionSnapshot]:
        normalized_symbols = _normalize_symbols(symbols)
        normalized_actions = [_normalize_optional_action_filter(value) for value in decision_actions or []]
        normalized_actions = [value for value in normalized_actions if value]
        normalized_policy_versions = [
            value
            for value in (
                _normalize_optional_text(str(raw_value)) if raw_value is not None else None
                for raw_value in (policy_versions or [])
            )
            if value
        ]
        with session_scope() as session:
            stmt = select(SignalDeciderDecisionSnapshot).where(
                SignalDeciderDecisionSnapshot.agent_name == SIGNAL_DECIDER_AGENT_NAME
            )
            if market_date is not None:
                stmt = stmt.where(SignalDeciderDecisionSnapshot.market_date == market_date)
            if market_date_from is not None:
                stmt = stmt.where(SignalDeciderDecisionSnapshot.market_date >= market_date_from)
            if market_date_to is not None:
                stmt = stmt.where(SignalDeciderDecisionSnapshot.market_date <= market_date_to)
            if normalized_symbols:
                stmt = stmt.where(SignalDeciderDecisionSnapshot.symbol.in_(normalized_symbols))
            if normalized_actions:
                stmt = stmt.where(SignalDeciderDecisionSnapshot.decision_action.in_(normalized_actions))
            if normalized_policy_versions:
                stmt = stmt.where(SignalDeciderDecisionSnapshot.policy_version.in_(normalized_policy_versions))
            stmt = stmt.order_by(
                SignalDeciderDecisionSnapshot.market_date.desc(),
                SignalDeciderDecisionSnapshot.created_at.desc(),
                SignalDeciderDecisionSnapshot.id.desc(),
            )
            if limit is not None:
                stmt = stmt.limit(limit)
            return session.execute(stmt).scalars().all()

    def get_decision_snapshot(self, *, snapshot_id: int) -> SignalDeciderDecisionSnapshot | None:
        with session_scope() as session:
            stmt = select(SignalDeciderDecisionSnapshot).where(
                SignalDeciderDecisionSnapshot.id == int(snapshot_id),
                SignalDeciderDecisionSnapshot.agent_name == SIGNAL_DECIDER_AGENT_NAME,
            )
            return session.execute(stmt).scalar_one_or_none()

    def list_realized_outcomes(
        self,
        *,
        decision_snapshot_ids: Sequence[int],
        horizons: Sequence[int] | None = None,
    ) -> list[dict[str, Any]]:
        if not decision_snapshot_ids:
            return []
        normalized_horizons = _normalize_horizons(horizons)
        with session_scope() as session:
            stmt = select(SignalDeciderRealizedOutcome).where(
                SignalDeciderRealizedOutcome.decision_snapshot_id.in_(decision_snapshot_ids)
            )
            if normalized_horizons:
                stmt = stmt.where(SignalDeciderRealizedOutcome.horizon_days.in_(normalized_horizons))
            rows = session.execute(stmt.order_by(SignalDeciderRealizedOutcome.horizon_days.asc())).scalars().all()
            return [_serialize_outcome_row(row) for row in rows]

    def list_evaluations(
        self,
        *,
        decision_snapshot_ids: Sequence[int],
        horizons: Sequence[int] | None = None,
        evaluation_version: str | None = None,
    ) -> list[dict[str, Any]]:
        if not decision_snapshot_ids:
            return []
        normalized_horizons = _normalize_horizons(horizons)
        with session_scope() as session:
            stmt = select(SignalDeciderEvaluation).where(
                SignalDeciderEvaluation.decision_snapshot_id.in_(decision_snapshot_ids)
            )
            if normalized_horizons:
                stmt = stmt.where(SignalDeciderEvaluation.horizon_days.in_(normalized_horizons))
            if evaluation_version:
                stmt = stmt.where(SignalDeciderEvaluation.evaluation_version == evaluation_version)
            rows = session.execute(stmt.order_by(SignalDeciderEvaluation.horizon_days.asc())).scalars().all()
            return [_serialize_evaluation_row(row) for row in rows]

    def list_evaluations_by_range(
        self,
        *,
        market_date_from: date,
        market_date_to: date,
        symbols: Sequence[str] | None = None,
        horizons: Sequence[int] | None = None,
        evaluation_version: str | None = None,
    ) -> list[dict[str, Any]]:
        normalized_symbols = _normalize_symbols(symbols)
        normalized_horizons = _normalize_horizons(horizons)
        with session_scope() as session:
            stmt = select(SignalDeciderEvaluation).where(
                SignalDeciderEvaluation.market_date >= market_date_from,
                SignalDeciderEvaluation.market_date <= market_date_to,
            )
            if normalized_symbols:
                stmt = stmt.where(SignalDeciderEvaluation.symbol.in_(normalized_symbols))
            if normalized_horizons:
                stmt = stmt.where(SignalDeciderEvaluation.horizon_days.in_(normalized_horizons))
            if evaluation_version:
                stmt = stmt.where(SignalDeciderEvaluation.evaluation_version == evaluation_version)
            rows = session.execute(
                stmt.order_by(
                    SignalDeciderEvaluation.market_date.desc(),
                    SignalDeciderEvaluation.symbol.asc(),
                    SignalDeciderEvaluation.horizon_days.asc(),
                )
            ).scalars().all()
            return [_serialize_evaluation_row(row) for row in rows]

    def list_policy_candidates(
        self,
        *,
        status: str | None = None,
        evaluation_version: str | None = None,
        base_policy_version: str | None = None,
        limit: int = 50,
    ) -> list[dict[str, Any]]:
        with session_scope() as session:
            stmt = select(SignalDeciderPolicyCandidate)
            if status:
                stmt = stmt.where(SignalDeciderPolicyCandidate.status == status)
            if evaluation_version:
                stmt = stmt.where(SignalDeciderPolicyCandidate.evaluation_version == evaluation_version)
            if base_policy_version:
                stmt = stmt.where(SignalDeciderPolicyCandidate.base_policy_version == base_policy_version)
            rows = session.execute(
                stmt.order_by(
                    SignalDeciderPolicyCandidate.generated_at.desc(),
                    SignalDeciderPolicyCandidate.id.desc(),
                ).limit(limit)
            ).scalars().all()
            return [_serialize_policy_candidate_row(row) for row in rows]

    def bulk_upsert_realized_outcomes(self, *, items: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
        if not items:
            return []
        with session_scope() as session:
            stmt = insert(SignalDeciderRealizedOutcome).values(list(items))
            excluded = stmt.excluded
            stmt = stmt.on_conflict_do_update(
                constraint='uq_signal_decider_realized_outcomes_snapshot_horizon',
                set_={
                    'feedback_id': excluded.feedback_id,
                    'market_date': excluded.market_date,
                    'symbol': excluded.symbol,
                    'policy_version': excluded.policy_version,
                    'decision_action': excluded.decision_action,
                    'outcome_status': excluded.outcome_status,
                    'entry_trade_date': excluded.entry_trade_date,
                    'exit_trade_date': excluded.exit_trade_date,
                    'bars_observed': excluded.bars_observed,
                    'entry_price': excluded.entry_price,
                    'exit_price': excluded.exit_price,
                    'max_high_price': excluded.max_high_price,
                    'min_low_price': excluded.min_low_price,
                    'close_return_pct': excluded.close_return_pct,
                    'max_upside_return_pct': excluded.max_upside_return_pct,
                    'max_drawdown_return_pct': excluded.max_drawdown_return_pct,
                    'target_price': excluded.target_price,
                    'stop_loss_price': excluded.stop_loss_price,
                    'expected_return_pct': excluded.expected_return_pct,
                    'target_hit': excluded.target_hit,
                    'stop_loss_hit': excluded.stop_loss_hit,
                    'target_stop_collision': excluded.target_stop_collision,
                    'price_path_payload': excluded.price_path_payload,
                    'diagnostics_payload': excluded.diagnostics_payload,
                    'computed_at': excluded.computed_at,
                    'updated_at': excluded.updated_at,
                },
            ).returning(SignalDeciderRealizedOutcome.id)
            ids = session.execute(stmt).scalars().all()
            rows = session.execute(
                select(SignalDeciderRealizedOutcome)
                .where(SignalDeciderRealizedOutcome.id.in_(ids))
                .order_by(SignalDeciderRealizedOutcome.id.asc())
            ).scalars().all()
            by_key = {
                (row.decision_snapshot_id, row.horizon_days): _serialize_outcome_row(row)
                for row in rows
            }
            return [by_key[(item['decision_snapshot_id'], item['horizon_days'])] for item in items]

    def bulk_upsert_evaluations(self, *, items: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
        if not items:
            return []
        with session_scope() as session:
            stmt = insert(SignalDeciderEvaluation).values(list(items))
            excluded = stmt.excluded
            stmt = stmt.on_conflict_do_update(
                constraint='uq_signal_decider_evaluations_snapshot_horizon_version',
                set_={
                    'realized_outcome_id': excluded.realized_outcome_id,
                    'feedback_id': excluded.feedback_id,
                    'market_date': excluded.market_date,
                    'symbol': excluded.symbol,
                    'policy_version': excluded.policy_version,
                    'decision_action': excluded.decision_action,
                    'evaluation_status': excluded.evaluation_status,
                    'grade_label': excluded.grade_label,
                    'grade_score': excluded.grade_score,
                    'missed_opportunity': excluded.missed_opportunity,
                    'counterfactual_action': excluded.counterfactual_action,
                    'rationale_payload': excluded.rationale_payload,
                    'diagnostics_payload': excluded.diagnostics_payload,
                    'evaluated_at': excluded.evaluated_at,
                    'updated_at': excluded.updated_at,
                },
            ).returning(SignalDeciderEvaluation.id)
            ids = session.execute(stmt).scalars().all()
            rows = session.execute(
                select(SignalDeciderEvaluation)
                .where(SignalDeciderEvaluation.id.in_(ids))
                .order_by(SignalDeciderEvaluation.id.asc())
            ).scalars().all()
            by_key = {
                (row.decision_snapshot_id, row.horizon_days, row.evaluation_version): _serialize_evaluation_row(row)
                for row in rows
            }
            return [
                by_key[(item['decision_snapshot_id'], item['horizon_days'], item['evaluation_version'])]
                for item in items
            ]

    def bulk_upsert_policy_candidates(self, *, items: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
        if not items:
            return []
        with session_scope() as session:
            stmt = insert(SignalDeciderPolicyCandidate).values(list(items))
            excluded = stmt.excluded
            stmt = stmt.on_conflict_do_update(
                constraint='uq_signal_decider_policy_candidates_version',
                set_={
                    'base_policy_version': excluded.base_policy_version,
                    'evaluation_version': excluded.evaluation_version,
                    'market_date_from': excluded.market_date_from,
                    'market_date_to': excluded.market_date_to,
                    'horizon_days': excluded.horizon_days,
                    'focus_action': excluded.focus_action,
                    'candidate_kind': excluded.candidate_kind,
                    'feature_key': excluded.feature_key,
                    'feature_value': excluded.feature_value,
                    'affected_decisions_count': excluded.affected_decisions_count,
                    'support_count': excluded.support_count,
                    'contradiction_count': excluded.contradiction_count,
                    'support_ratio': excluded.support_ratio,
                    'status': excluded.status,
                    'activation_mode': excluded.activation_mode,
                    'summary': excluded.summary,
                    'hypothesis': excluded.hypothesis,
                    'evidence_payload': excluded.evidence_payload,
                    'recommendation_payload': excluded.recommendation_payload,
                    'generated_at': excluded.generated_at,
                    'updated_at': excluded.updated_at,
                },
            ).returning(SignalDeciderPolicyCandidate.id)
            ids = session.execute(stmt).scalars().all()
            rows = session.execute(
                select(SignalDeciderPolicyCandidate)
                .where(SignalDeciderPolicyCandidate.id.in_(ids))
                .order_by(SignalDeciderPolicyCandidate.id.asc())
            ).scalars().all()
            by_key = {row.candidate_version: _serialize_policy_candidate_row(row) for row in rows}
            return [by_key[item['candidate_version']] for item in items]


class SignalDeciderLoopService:
    def __init__(self, *, repository: SignalDeciderLoopRepository | None = None) -> None:
        self.repository = repository or SignalDeciderLoopRepository()

    def prepare_feedback_persistence(
        self,
        *,
        agent_name: str,
        summary_payload: dict[str, Any],
        feedback_payload: dict[str, Any],
        context_payload: dict[str, Any] | None,
    ) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any] | None]:
        if agent_name != SIGNAL_DECIDER_AGENT_NAME:
            return summary_payload, feedback_payload, context_payload

        context = dict(context_payload or {})
        policy_version = _policy_version_from_payloads(
            summary_payload=summary_payload,
            feedback_payload=feedback_payload,
            context_payload=context,
        )
        feature_snapshot = build_signal_decider_feature_snapshot(
            symbol_day_context=_maybe_dict(context.get('symbol_day_context')),
            feedback_payload=feedback_payload,
        )
        enriched_summary = dict(summary_payload)
        enriched_feedback = dict(feedback_payload)
        enriched_summary.setdefault('policy_version', policy_version)
        enriched_feedback.setdefault('policy_version', policy_version)
        context['signal_decider_policy'] = {
            'current_version': policy_version,
            'activation_mode': 'manual_only',
            'candidate_activation_required': True,
        }
        context['signal_decider_trace'] = {
            'policy_version': policy_version,
            'decision_action': _normalize_action(
                feedback_payload.get('next_day_open_action') or feedback_payload.get('preferred_action')
            ),
            'feature_snapshot': feature_snapshot,
            'traceability': _maybe_dict(feedback_payload.get('traceability')),
        }
        return enriched_summary, enriched_feedback, context

    def prepare_snapshot_persistence_item(
        self,
        *,
        feedback_id: int | None,
        market_date: str | date,
        symbol: str,
        agent_name: str,
        agent_version: str | None,
        run_id: str | None,
        summary_payload: dict[str, Any],
        feedback_payload: dict[str, Any],
        normalized_feedback_payload: dict[str, Any] | None = None,
        context_payload: dict[str, Any] | None,
        received_at: str | datetime | None,
    ) -> dict[str, Any] | None:
        if agent_name != SIGNAL_DECIDER_AGENT_NAME:
            return None

        normalized_market_date = _normalize_market_date(market_date)
        response_payload = _maybe_dict(normalized_feedback_payload) or _maybe_dict(feedback_payload)
        request_payload = _maybe_dict(context_payload)
        policy_version = _policy_version_from_payloads(
            summary_payload=summary_payload,
            feedback_payload=response_payload,
            context_payload=request_payload,
        )
        feature_snapshot = _maybe_dict(
            _maybe_dict(request_payload.get('signal_decider_trace')).get('feature_snapshot')
        )
        if not feature_snapshot:
            feature_snapshot = build_signal_decider_feature_snapshot(
                symbol_day_context=_maybe_dict(request_payload.get('symbol_day_context')),
                feedback_payload=response_payload,
            )
        decision_action = _normalize_action(
            response_payload.get('next_day_open_action') or response_payload.get('preferred_action')
        )
        thesis_summary = str(
            response_payload.get('thesis_summary')
            or response_payload.get('summary')
            or summary_payload.get('summary')
            or ''
        ).strip()
        traceability_payload = _maybe_dict(request_payload.get('signal_decider_trace')).get('traceability')
        if not isinstance(traceability_payload, dict) or not traceability_payload.get('schema_version'):
            traceability_payload = build_signal_decider_traceability_snapshot(
                agent_name=agent_name,
                agent_version=agent_version,
                feedback_id=feedback_id,
                decision_run_id=run_id,
                market_date=normalized_market_date,
                symbol=symbol,
                policy_version=policy_version,
                decision_action=decision_action,
                thesis_summary=thesis_summary,
                summary_payload=summary_payload,
                feedback_payload=response_payload,
                context_payload=request_payload,
                feature_snapshot=feature_snapshot,
            )
        return {
            'feedback_id': feedback_id,
            'market_date': normalized_market_date,
            'symbol': symbol,
            'agent_name': agent_name,
            'agent_version': agent_version,
            'decision_run_id': run_id,
            'feedback_received_at': _normalize_optional_datetime(received_at),
            'policy_version': policy_version,
            'decision_action': decision_action,
            'preferred_action': _normalize_action(response_payload.get('preferred_action')),
            'next_day_open_action': _normalize_action(response_payload.get('next_day_open_action')),
            'entry_price_basis': _normalize_optional_text(
                str(response_payload.get('entry_price_basis')) if response_payload.get('entry_price_basis') is not None else None,
                default='next_open',
            )
            or 'next_open',
            'target_price': _to_int(response_payload.get('target_price')),
            'expected_return_pct': _to_float(response_payload.get('expected_return_pct')),
            'stop_loss_price': _to_int(response_payload.get('stop_loss_price')),
            'suggested_holding_period': _normalize_optional_text(
                str(response_payload.get('suggested_holding_period'))
                if response_payload.get('suggested_holding_period') is not None
                else None
            ),
            'confidence_score_10': _to_float(response_payload.get('confidence_score_10')),
            'conviction_score_10': _to_float(response_payload.get('conviction_score_10')),
            'thesis_summary': thesis_summary,
            'supporting_evidence_payload': _normalize_text_list(response_payload.get('supporting_evidence')),
            'decision_risks_payload': _normalize_text_list(response_payload.get('decision_risks')),
            'invalidation_conditions_payload': _normalize_text_list(
                response_payload.get('invalidation_conditions')
            ),
            'data_quality_warnings_payload': _normalize_text_list(
                response_payload.get('data_quality_warnings')
            ),
            'specialist_disagreement_payload': _normalize_text_list(
                response_payload.get('specialist_disagreement')
            ),
            'traceability_payload': traceability_payload,
            'input_feature_snapshot_payload': feature_snapshot,
            'request_payload': request_payload,
            'response_payload': response_payload,
            'response_summary_payload': _maybe_dict(summary_payload),
        }

    def list_decisions(
        self,
        *,
        market_date: str | date | None = None,
        market_date_from: str | date | None = None,
        market_date_to: str | date | None = None,
        symbol: str | None = None,
        action: str | None = None,
        policy_version: str | None = None,
        verdict: str | None = None,
        has_missed_opportunity: bool | None = None,
        horizons: Sequence[int] | None = None,
        evaluation_version: str | None = None,
        limit: int = 50,
    ) -> dict[str, Any]:
        normalized_from, normalized_to = _resolve_market_date_window(
            market_date=market_date,
            market_date_from=market_date_from,
            market_date_to=market_date_to,
        )
        normalized_symbol = _normalize_optional_symbol(symbol)
        normalized_action = _normalize_optional_action_filter(action)
        normalized_policy_version = _normalize_optional_text(policy_version)
        normalized_verdict = _normalize_optional_verdict(verdict)
        normalized_horizons = _normalize_horizons(horizons)
        resolved_limit = _normalize_limit(limit, default=50)
        resolved_evaluation_version = (
            _normalize_optional_text(evaluation_version, default=DEFAULT_SIGNAL_DECIDER_EVALUATION_VERSION)
            or DEFAULT_SIGNAL_DECIDER_EVALUATION_VERSION
        )
        rows = self.repository.list_decision_snapshots(
            market_date_from=normalized_from,
            market_date_to=normalized_to,
            symbols=[normalized_symbol] if normalized_symbol else None,
            decision_actions=[normalized_action] if normalized_action else None,
            policy_versions=[normalized_policy_version] if normalized_policy_version else None,
        )
        snapshot_ids = [row.id for row in rows]
        evaluations_by_snapshot = self._group_by_snapshot_id(
            self.repository.list_evaluations(
                decision_snapshot_ids=snapshot_ids,
                horizons=normalized_horizons,
                evaluation_version=resolved_evaluation_version,
            )
        )
        action_counts: Counter[str] = Counter()
        verdict_counts: Counter[str] = Counter()
        items: list[dict[str, Any]] = []
        for row in rows:
            evaluations = evaluations_by_snapshot.get(row.id, [])
            if not self._decision_matches_evaluation_filters(
                evaluations=evaluations,
                verdict=normalized_verdict,
                has_missed_opportunity=has_missed_opportunity,
            ):
                continue
            decision = self._build_decision_snapshot(row)
            evaluation_summary = self._build_evaluation_summary(
                evaluations=evaluations,
                evaluation_version=resolved_evaluation_version,
            )
            item = {
                **decision,
                'explanation': self._build_decision_explanation(
                    decision=decision,
                    evaluations=evaluations,
                ),
                'evaluation_summary': evaluation_summary,
            }
            items.append(item)
            action_counts[decision['decision']['action']] += 1
            verdict_counts.update(evaluation_summary.get('verdict_counts') or {})
            if len(items) >= resolved_limit:
                break
        return {
            'request': {
                'market_date': normalized_from.isoformat() if normalized_from == normalized_to else None,
                'market_date_from': normalized_from.isoformat(),
                'market_date_to': normalized_to.isoformat(),
                'symbol': normalized_symbol,
                'action': normalized_action,
                'policy_version': normalized_policy_version,
                'verdict': normalized_verdict,
                'has_missed_opportunity': has_missed_opportunity,
                'horizons': normalized_horizons,
                'evaluation_version': resolved_evaluation_version,
                'limit': limit,
            },
            'summary': {
                'count': len(items),
                'evaluated_count': sum(
                    1 for item in items if _maybe_dict(item.get('evaluation_summary')).get('evaluated')
                ),
                'decision_actions': dict(sorted(action_counts.items())),
                'policy_versions': sorted({item['policy_version'] for item in items}),
                'verdict_counts': dict(sorted(verdict_counts.items())),
            },
            'items': items,
        }

    def get_decision_detail(
        self,
        *,
        snapshot_id: int,
        horizons: Sequence[int] | None = None,
        evaluation_version: str | None = None,
    ) -> dict[str, Any]:
        normalized_snapshot_id = int(snapshot_id)
        normalized_horizons = _normalize_horizons(horizons)
        resolved_evaluation_version = (
            _normalize_optional_text(evaluation_version, default=DEFAULT_SIGNAL_DECIDER_EVALUATION_VERSION)
            or DEFAULT_SIGNAL_DECIDER_EVALUATION_VERSION
        )
        row = self.repository.get_decision_snapshot(snapshot_id=normalized_snapshot_id)
        if row is None:
            raise ValueError(f'signal-decider decision snapshot {normalized_snapshot_id} was not found')
        detail_item = self._build_decision_detail_item(
            row=row,
            horizons=normalized_horizons,
            evaluation_version=resolved_evaluation_version,
        )
        return {
            'request': {
                'snapshot_id': normalized_snapshot_id,
                'horizons': normalized_horizons,
                'evaluation_version': resolved_evaluation_version,
            },
            'summary': {
                'evaluated_count': len(detail_item['evaluations']),
                'outcome_count': len(detail_item['outcomes']),
            },
            'item': detail_item,
        }

    def get_diagnostics(
        self,
        *,
        market_date: str | date,
        symbol: str | None = None,
        evaluation_version: str | None = None,
        limit: int = 20,
    ) -> dict[str, Any]:
        normalized_market_date = _normalize_market_date(market_date)
        normalized_symbol = _normalize_optional_symbol(symbol)
        resolved_evaluation_version = (
            _normalize_optional_text(evaluation_version, default=DEFAULT_SIGNAL_DECIDER_EVALUATION_VERSION)
            or DEFAULT_SIGNAL_DECIDER_EVALUATION_VERSION
        )
        rows = self.repository.list_decision_snapshots(
            market_date=normalized_market_date,
            symbols=[normalized_symbol] if normalized_symbol else None,
            limit=_normalize_limit(limit, default=20),
        )
        items = [
            self._build_decision_detail_item(
                row=row,
                horizons=None,
                evaluation_version=resolved_evaluation_version,
            )
            for row in rows
        ]
        return {
            'request': {
                'market_date': normalized_market_date.isoformat(),
                'symbol': normalized_symbol,
                'evaluation_version': resolved_evaluation_version,
                'limit': limit,
            },
            'summary': {
                'count': len(items),
                'evaluated_count': sum(1 for item in items if item['evaluations']),
            },
            'items': items,
        }

    def get_aggregate_diagnostics(
        self,
        *,
        market_date: str | date | None = None,
        market_date_from: str | date | None = None,
        market_date_to: str | date | None = None,
        symbols: Sequence[str] | None = None,
        action: str | None = None,
        policy_version: str | None = None,
        horizons: Sequence[int] | None = None,
        evaluation_version: str | None = None,
        contributor_limit: int = 20,
    ) -> dict[str, Any]:
        normalized_from, normalized_to = _resolve_market_date_window(
            market_date=market_date,
            market_date_from=market_date_from,
            market_date_to=market_date_to,
        )
        normalized_symbols = _normalize_symbols(symbols)
        normalized_action = _normalize_optional_action_filter(action)
        normalized_policy_version = _normalize_optional_text(policy_version)
        normalized_horizons = _normalize_horizons(horizons)
        resolved_evaluation_version = (
            _normalize_optional_text(evaluation_version, default=DEFAULT_SIGNAL_DECIDER_EVALUATION_VERSION)
            or DEFAULT_SIGNAL_DECIDER_EVALUATION_VERSION
        )
        rows = self.repository.list_decision_snapshots(
            market_date_from=normalized_from,
            market_date_to=normalized_to,
            symbols=normalized_symbols or None,
            decision_actions=[normalized_action] if normalized_action else None,
            policy_versions=[normalized_policy_version] if normalized_policy_version else None,
        )
        snapshot_ids = [row.id for row in rows]
        evaluations = self.repository.list_evaluations(
            decision_snapshot_ids=snapshot_ids,
            horizons=normalized_horizons,
            evaluation_version=resolved_evaluation_version,
        )
        evaluations_by_snapshot = self._group_by_snapshot_id(evaluations)
        return {
            'request': {
                'market_date': normalized_from.isoformat() if normalized_from == normalized_to else None,
                'market_date_from': normalized_from.isoformat(),
                'market_date_to': normalized_to.isoformat(),
                'symbols': normalized_symbols,
                'action': normalized_action,
                'policy_version': normalized_policy_version,
                'horizons': normalized_horizons,
                'evaluation_version': resolved_evaluation_version,
                'contributor_limit': _normalize_limit(contributor_limit, default=20),
            },
            'summary': self._build_aggregate_summary(rows=rows, evaluations=evaluations),
            'aggregates': {
                'by_horizon': self._build_aggregate_groups(
                    evaluations=evaluations,
                    group_key='horizon_days',
                ),
                'by_policy_version': self._build_aggregate_groups(
                    evaluations=evaluations,
                    group_key='policy_version',
                ),
            },
            'contributors': self._build_contributor_report(
                rows=rows,
                evaluations_by_snapshot=evaluations_by_snapshot,
                limit=_normalize_limit(contributor_limit, default=20),
            ),
        }

    def compute_evaluations(
        self,
        *,
        market_date_from: str | date,
        market_date_to: str | date,
        symbols: Sequence[str] | None = None,
        horizons: Sequence[int] | None = None,
        evaluation_version: str | None = None,
        force_recompute: bool = False,
    ) -> dict[str, Any]:
        normalized_from = _normalize_market_date(market_date_from)
        normalized_to = _normalize_market_date(market_date_to)
        if normalized_from > normalized_to:
            raise ValueError('market_date_from must be on or before market_date_to')
        normalized_symbols = _normalize_symbols(symbols)
        normalized_horizons = _normalize_horizons(horizons)
        resolved_version = (
            _normalize_optional_text(evaluation_version, default=DEFAULT_SIGNAL_DECIDER_EVALUATION_VERSION)
            or DEFAULT_SIGNAL_DECIDER_EVALUATION_VERSION
        )
        evaluation_profile = self._resolve_evaluation_profile(resolved_version)
        rows = self.repository.list_decision_snapshots(
            market_date_from=normalized_from,
            market_date_to=normalized_to,
            symbols=normalized_symbols or None,
        )
        if not rows:
            return {
                'request': {
                    'market_date_from': normalized_from.isoformat(),
                    'market_date_to': normalized_to.isoformat(),
                    'symbols': normalized_symbols,
                    'horizons': normalized_horizons,
                    'evaluation_version': resolved_version,
                    'evaluation_rule_set_version': evaluation_profile.rule_set_version,
                    'force_recompute': force_recompute,
                },
                'summary': {
                    'decisions_considered': 0,
                    'outcomes_upserted': 0,
                    'evaluations_upserted': 0,
                    'graded_count': 0,
                    'pending_count': 0,
                },
                'items': [],
            }

        max_horizon = max(normalized_horizons)
        outcome_items: list[dict[str, Any]] = []
        decision_snapshots: dict[int, dict[str, Any]] = {}
        for row in rows:
            decision = self._build_decision_snapshot(row)
            decision_snapshots[row.id] = decision
            future_bars = self._load_future_price_bars(
                symbol=row.symbol,
                market_date=row.market_date,
                max_horizon=max_horizon,
            )
            for horizon_days in normalized_horizons:
                outcome_items.append(
                    self._build_realized_outcome_item(
                        snapshot_row=row,
                        decision=decision,
                        future_bars=future_bars,
                        horizon_days=horizon_days,
                    )
                )

        persisted_outcomes = self.repository.bulk_upsert_realized_outcomes(items=outcome_items)
        outcomes_by_key = {
            (item['decision_snapshot_id'], item['horizon_days']): item
            for item in persisted_outcomes
        }
        evaluation_items: list[dict[str, Any]] = []
        for row in rows:
            decision = decision_snapshots[row.id]
            for horizon_days in normalized_horizons:
                outcome = outcomes_by_key[(row.id, horizon_days)]
                evaluation_items.append(
                    self._build_evaluation_item(
                        snapshot_row=row,
                        decision=decision,
                        outcome=outcome,
                        horizon_days=horizon_days,
                        evaluation_version=resolved_version,
                        evaluation_profile=evaluation_profile,
                    )
                )
        persisted_evaluations = self.repository.bulk_upsert_evaluations(items=evaluation_items)
        outcomes_by_snapshot = self._group_by_snapshot_id(persisted_outcomes)
        evaluations_by_snapshot = self._group_by_snapshot_id(persisted_evaluations)
        items = [
            {
                **decision_snapshots[row.id],
                'outcomes': outcomes_by_snapshot.get(row.id, []),
                'evaluations': evaluations_by_snapshot.get(row.id, []),
            }
            for row in rows
        ]
        graded_count = sum(
            1 for item in persisted_evaluations if item.get('evaluation_status') == 'graded'
        )
        pending_count = sum(
            1 for item in persisted_evaluations if item.get('evaluation_status') != 'graded'
        )
        return {
            'request': {
                'market_date_from': normalized_from.isoformat(),
                'market_date_to': normalized_to.isoformat(),
                'symbols': normalized_symbols,
                'horizons': normalized_horizons,
                'evaluation_version': resolved_version,
                'evaluation_rule_set_version': evaluation_profile.rule_set_version,
                'force_recompute': force_recompute,
            },
            'summary': {
                'decisions_considered': len(rows),
                'outcomes_upserted': len(persisted_outcomes),
                'evaluations_upserted': len(persisted_evaluations),
                'graded_count': graded_count,
                'pending_count': pending_count,
            },
            'items': items,
        }

    def compute_recent_evaluations(
        self,
        *,
        as_of_market_date: str | date,
        lookback_days: int = DEFAULT_SIGNAL_DECIDER_EVALUATION_LOOKBACK_DAYS,
        symbols: Sequence[str] | None = None,
        horizons: Sequence[int] | None = None,
        evaluation_version: str | None = None,
        force_recompute: bool = False,
    ) -> dict[str, Any]:
        normalized_market_date = _normalize_market_date(as_of_market_date)
        resolved_lookback_days = max(1, int(lookback_days))
        market_date_from = normalized_market_date - timedelta(days=resolved_lookback_days)
        result = self.compute_evaluations(
            market_date_from=market_date_from,
            market_date_to=normalized_market_date,
            symbols=symbols,
            horizons=horizons,
            evaluation_version=evaluation_version,
            force_recompute=force_recompute,
        )
        request_payload = dict(result.get('request') or {})
        request_payload['as_of_market_date'] = normalized_market_date.isoformat()
        request_payload['lookback_days'] = resolved_lookback_days
        result['request'] = request_payload
        return result

    def list_evaluations(
        self,
        *,
        market_date: str | date,
        symbol: str | None = None,
        horizons: Sequence[int] | None = None,
        evaluation_version: str | None = None,
        limit: int = 50,
    ) -> dict[str, Any]:
        normalized_market_date = _normalize_market_date(market_date)
        normalized_symbol = _normalize_optional_symbol(symbol)
        normalized_horizons = _normalize_horizons(horizons)
        resolved_version = (
            _normalize_optional_text(evaluation_version, default=DEFAULT_SIGNAL_DECIDER_EVALUATION_VERSION)
            or DEFAULT_SIGNAL_DECIDER_EVALUATION_VERSION
        )
        rows = self.repository.list_decision_snapshots(
            market_date=normalized_market_date,
            symbols=[normalized_symbol] if normalized_symbol else None,
            limit=_normalize_limit(limit, default=50),
        )
        snapshot_ids = [row.id for row in rows]
        outcomes_by_snapshot = self._group_by_snapshot_id(
            self.repository.list_realized_outcomes(
                decision_snapshot_ids=snapshot_ids,
                horizons=normalized_horizons,
            )
        )
        evaluations_by_snapshot = self._group_by_snapshot_id(
            self.repository.list_evaluations(
                decision_snapshot_ids=snapshot_ids,
                horizons=normalized_horizons,
                evaluation_version=resolved_version,
            )
        )
        items = [
            {
                **self._build_decision_snapshot(row),
                'outcomes': outcomes_by_snapshot.get(row.id, []),
                'evaluations': evaluations_by_snapshot.get(row.id, []),
            }
            for row in rows
        ]
        return {
            'request': {
                'market_date': normalized_market_date.isoformat(),
                'symbol': normalized_symbol,
                'horizons': normalized_horizons,
                'evaluation_version': resolved_version,
                'limit': limit,
            },
            'summary': {
                'count': len(items),
                'evaluated_count': sum(1 for item in items if item['evaluations']),
            },
            'items': items,
        }

    def generate_policy_candidates(
        self,
        *,
        market_date_from: str | date,
        market_date_to: str | date,
        symbols: Sequence[str] | None = None,
        horizons: Sequence[int] | None = None,
        primary_horizon_days: int = 5,
        evaluation_version: str | None = None,
        min_sample_size: int = 3,
        force_recompute: bool = False,
    ) -> dict[str, Any]:
        normalized_from = _normalize_market_date(market_date_from)
        normalized_to = _normalize_market_date(market_date_to)
        normalized_symbols = _normalize_symbols(symbols)
        normalized_horizons = _normalize_horizons(horizons)
        if primary_horizon_days not in normalized_horizons:
            raise ValueError('primary_horizon_days must be included in horizons')
        resolved_version = (
            _normalize_optional_text(evaluation_version, default=DEFAULT_SIGNAL_DECIDER_EVALUATION_VERSION)
            or DEFAULT_SIGNAL_DECIDER_EVALUATION_VERSION
        )
        self.compute_evaluations(
            market_date_from=normalized_from,
            market_date_to=normalized_to,
            symbols=normalized_symbols,
            horizons=normalized_horizons,
            evaluation_version=resolved_version,
            force_recompute=force_recompute,
        )
        snapshot_rows = self.repository.list_decision_snapshots(
            market_date_from=normalized_from,
            market_date_to=normalized_to,
            symbols=normalized_symbols or None,
        )
        snapshots_by_id = {row.id: row for row in snapshot_rows}
        evaluations = self.repository.list_evaluations_by_range(
            market_date_from=normalized_from,
            market_date_to=normalized_to,
            symbols=normalized_symbols or None,
            horizons=[primary_horizon_days],
            evaluation_version=resolved_version,
        )
        candidate_groups: dict[tuple[str, str, str, str, str], dict[str, Any]] = {}
        for evaluation in evaluations:
            if evaluation.get('evaluation_status') != 'graded':
                continue
            snapshot_id = int(evaluation['decision_snapshot_id'])
            snapshot_row = snapshots_by_id.get(snapshot_id)
            if snapshot_row is None:
                continue
            decision = self._build_decision_snapshot(snapshot_row)
            feature_flags = self._build_feature_flags(decision['feature_snapshot'])
            if not feature_flags:
                continue
            decision_action = str(evaluation.get('decision_action') or decision['decision']['action'])
            grade_label = str(evaluation.get('grade_label') or '')
            support_flag = self._candidate_support_flag(
                decision_action=decision_action,
                grade_label=grade_label,
            )
            if support_flag is None:
                continue
            for feature_key, feature_value in feature_flags:
                base_policy_version = decision['policy_version']
                group_key = (
                    base_policy_version,
                    decision_action,
                    support_flag['candidate_kind'],
                    feature_key,
                    feature_value,
                )
                group = candidate_groups.setdefault(
                    group_key,
                    {
                        'base_policy_version': base_policy_version,
                        'decision_action': decision_action,
                        'candidate_kind': support_flag['candidate_kind'],
                        'feature_key': feature_key,
                        'feature_value': feature_value,
                        'total_count': 0,
                        'support_count': 0,
                        'counterexamples': 0,
                        'samples': [],
                    },
                )
                group['total_count'] += 1
                if support_flag['supports_candidate']:
                    group['support_count'] += 1
                    if len(group['samples']) < 10:
                        group['samples'].append(
                            {
                                'snapshot_id': snapshot_id,
                                'feedback_id': evaluation.get('feedback_id'),
                                'symbol': decision['symbol'],
                                'market_date': decision['market_date'],
                                'grade_label': grade_label,
                                'summary': decision['summary'],
                                'close_return_pct': evaluation.get('diagnostics', {}).get('close_return_pct'),
                                'max_upside_return_pct': evaluation.get('diagnostics', {}).get('max_upside_return_pct'),
                            }
                        )

        candidate_items: list[dict[str, Any]] = []
        for group in candidate_groups.values():
            support_count = int(group['support_count'])
            total_count = int(group['total_count'])
            if total_count < min_sample_size or support_count < min_sample_size:
                continue
            support_ratio = support_count / total_count if total_count else 0.0
            if support_ratio < 0.60:
                continue
            contradiction_count = total_count - support_count
            candidate_version = self._candidate_version(
                base_policy_version=group['base_policy_version'],
                evaluation_version=resolved_version,
                horizon_days=primary_horizon_days,
                candidate_kind=group['candidate_kind'],
                feature_key=group['feature_key'],
                feature_value=group['feature_value'],
                market_date_from=normalized_from,
                market_date_to=normalized_to,
            )
            summary = self._candidate_summary(
                decision_action=group['decision_action'],
                candidate_kind=group['candidate_kind'],
                feature_key=group['feature_key'],
                feature_value=group['feature_value'],
                support_ratio=support_ratio,
                support_count=support_count,
                total_count=total_count,
                horizon_days=primary_horizon_days,
            )
            hypothesis = self._candidate_hypothesis(
                decision_action=group['decision_action'],
                candidate_kind=group['candidate_kind'],
                feature_key=group['feature_key'],
                feature_value=group['feature_value'],
                horizon_days=primary_horizon_days,
            )
            candidate_items.append(
                {
                    'candidate_version': candidate_version,
                    'base_policy_version': group['base_policy_version'],
                    'evaluation_version': resolved_version,
                    'market_date_from': normalized_from,
                    'market_date_to': normalized_to,
                    'horizon_days': primary_horizon_days,
                    'focus_action': group['decision_action'],
                    'candidate_kind': group['candidate_kind'],
                    'feature_key': group['feature_key'],
                    'feature_value': group['feature_value'],
                    'affected_decisions_count': total_count,
                    'support_count': support_count,
                    'contradiction_count': contradiction_count,
                    'support_ratio': round(support_ratio, 6),
                    'status': DEFAULT_POLICY_CANDIDATE_STATUS,
                    'activation_mode': 'manual_only',
                    'summary': summary,
                    'hypothesis': hypothesis,
                    'evidence_payload': {
                        'support_ratio': round(support_ratio, 6),
                        'support_count': support_count,
                        'contradiction_count': contradiction_count,
                        'sample_decisions': group['samples'],
                        'market_date_from': normalized_from.isoformat(),
                        'market_date_to': normalized_to.isoformat(),
                    },
                    'recommendation_payload': {
                        'proposed_policy_version': candidate_version,
                        'base_policy_version': group['base_policy_version'],
                        'activation_mode': 'manual_only',
                        'candidate_kind': group['candidate_kind'],
                        'feature_gate': {
                            'key': group['feature_key'],
                            'value': group['feature_value'],
                        },
                        'review_note': 'Candidate generation is automatic; activation must remain manual.',
                    },
                    'generated_at': self._utcnow(),
                    'created_at': self._utcnow(),
                    'updated_at': self._utcnow(),
                    'activated_at': None,
                }
            )

        persisted_candidates = self.repository.bulk_upsert_policy_candidates(items=candidate_items)
        return {
            'request': {
                'market_date_from': normalized_from.isoformat(),
                'market_date_to': normalized_to.isoformat(),
                'symbols': normalized_symbols,
                'horizons': normalized_horizons,
                'primary_horizon_days': primary_horizon_days,
                'evaluation_version': resolved_version,
                'min_sample_size': min_sample_size,
                'force_recompute': force_recompute,
            },
            'summary': {
                'evaluations_considered': len(evaluations),
                'candidates_generated': len(persisted_candidates),
                'manual_activation_required': True,
            },
            'items': persisted_candidates,
        }

    def list_policy_candidates(
        self,
        *,
        status: str | None = None,
        evaluation_version: str | None = None,
        base_policy_version: str | None = None,
        limit: int = 50,
    ) -> dict[str, Any]:
        resolved_version = _normalize_optional_text(evaluation_version)
        resolved_status = _normalize_optional_text(status)
        resolved_policy_version = _normalize_optional_text(base_policy_version)
        items = self.repository.list_policy_candidates(
            status=resolved_status,
            evaluation_version=resolved_version,
            base_policy_version=resolved_policy_version,
            limit=_normalize_limit(limit, default=50),
        )
        return {
            'request': {
                'status': resolved_status,
                'evaluation_version': resolved_version,
                'base_policy_version': resolved_policy_version,
                'limit': limit,
            },
            'summary': {
                'count': len(items),
                'manual_activation_required': True,
            },
            'items': items,
        }

    def _build_decision_detail_item(
        self,
        *,
        row: SignalDeciderDecisionSnapshot,
        horizons: Sequence[int] | None,
        evaluation_version: str,
    ) -> dict[str, Any]:
        outcomes = self.repository.list_realized_outcomes(
            decision_snapshot_ids=[row.id],
            horizons=horizons,
        )
        evaluations = self.repository.list_evaluations(
            decision_snapshot_ids=[row.id],
            horizons=horizons,
            evaluation_version=evaluation_version,
        )
        decision = self._build_decision_snapshot(row)
        request_payload = _maybe_dict(row.request_payload)
        return {
            **decision,
            'explanation': self._build_decision_explanation(
                decision=decision,
                evaluations=evaluations,
            ),
            'evaluation_summary': self._build_evaluation_summary(
                evaluations=evaluations,
                evaluation_version=evaluation_version,
            ),
            'request_payload': request_payload,
            'response_payload': _maybe_dict(row.response_payload),
            'response_summary': _maybe_dict(row.response_summary_payload),
            'upstream_summaries': request_payload.get('upstream_summaries'),
            'upstream_traceability': request_payload.get('upstream_traceability'),
            'outcomes': outcomes,
            'evaluations': evaluations,
        }

    def _decision_matches_evaluation_filters(
        self,
        *,
        evaluations: Sequence[dict[str, Any]],
        verdict: str | None,
        has_missed_opportunity: bool | None,
    ) -> bool:
        if verdict is not None and not any(item.get('grade_label') == verdict for item in evaluations):
            return False
        if has_missed_opportunity is not None:
            resolved_has_missed = any(item.get('missed_opportunity') is True for item in evaluations)
            if resolved_has_missed != has_missed_opportunity:
                return False
        return True

    def _build_decision_explanation(
        self,
        *,
        decision: dict[str, Any],
        evaluations: Sequence[dict[str, Any]],
    ) -> dict[str, Any]:
        traceability = _maybe_dict(decision.get('traceability'))
        factor_contributions = _maybe_dict(traceability.get('factor_contributions'))
        gate_checks = _maybe_dict(traceability.get('gate_checks'))
        evaluation_summary = self._build_evaluation_summary(
            evaluations=evaluations,
            evaluation_version=None,
        )
        return {
            'decision_action': _maybe_dict(decision.get('decision')).get('action'),
            'summary': decision.get('summary'),
            'supporting_evidence': list(decision.get('supporting_evidence') or []),
            'decision_risks': list(decision.get('decision_risks') or []),
            'invalidation_conditions': list(decision.get('invalidation_conditions') or []),
            'feature_flags': list(factor_contributions.get('feature_flags') or []),
            'data_quality_warnings': list(
                factor_contributions.get('data_quality_warnings') or decision.get('data_quality_warnings') or []
            ),
            'specialist_disagreement': list(
                factor_contributions.get('specialist_disagreement') or decision.get('specialist_disagreement') or []
            ),
            'gate_checks': {
                'required_upstream_roles': list(gate_checks.get('required_upstream_roles') or []),
                'available_upstream_roles': list(gate_checks.get('available_upstream_roles') or []),
                'missing_upstream_roles': list(gate_checks.get('missing_upstream_roles') or []),
                'decision_support_only': bool(gate_checks.get('decision_support_only')),
                'allow_execution_logic': bool(gate_checks.get('allow_execution_logic')),
                'has_data_warnings': bool(gate_checks.get('has_data_warnings')),
                'has_specialist_disagreement': bool(gate_checks.get('has_specialist_disagreement')),
                'traceability_symbol_matches': bool(gate_checks.get('traceability_symbol_matches', True)),
                'traceability_market_date_matches': bool(gate_checks.get('traceability_market_date_matches', True)),
                'traceability_agent_name_matches': bool(gate_checks.get('traceability_agent_name_matches', True)),
            },
            'upstream_references': _maybe_dict(traceability.get('upstream_references')),
            'latest_evaluation': evaluation_summary.get('latest_graded'),
        }

    def _build_evaluation_summary(
        self,
        *,
        evaluations: Sequence[dict[str, Any]],
        evaluation_version: str | None,
    ) -> dict[str, Any]:
        sorted_evaluations = sorted(evaluations, key=lambda item: int(item.get('horizon_days') or 0))
        verdict_counts: Counter[str] = Counter()
        graded_count = 0
        pending_count = 0
        latest_graded: dict[str, Any] | None = None
        horizons: list[dict[str, Any]] = []
        for evaluation in sorted_evaluations:
            evaluation_status = str(evaluation.get('evaluation_status') or '')
            grade_label = str(evaluation.get('grade_label') or '')
            horizon_days = int(evaluation.get('horizon_days') or 0)
            projection = {
                'horizon_days': horizon_days,
                'evaluation_status': evaluation_status,
                'grade_label': grade_label,
                'missed_opportunity': evaluation.get('missed_opportunity'),
                'grade_score': _to_float(evaluation.get('grade_score')),
            }
            horizons.append(projection)
            if evaluation_status == 'graded':
                graded_count += 1
                if grade_label:
                    verdict_counts[grade_label] += 1
                latest_graded = projection
            else:
                pending_count += 1
        return {
            'evaluation_version': evaluation_version,
            'evaluated': bool(sorted_evaluations),
            'graded_horizon_count': graded_count,
            'pending_horizon_count': pending_count,
            'has_missed_opportunity': any(item.get('missed_opportunity') is True for item in sorted_evaluations),
            'verdict_counts': dict(sorted(verdict_counts.items())),
            'latest_graded': latest_graded,
            'horizons': horizons,
        }

    def _build_aggregate_summary(
        self,
        *,
        rows: Sequence[SignalDeciderDecisionSnapshot],
        evaluations: Sequence[dict[str, Any]],
    ) -> dict[str, Any]:
        verdict_counts: Counter[str] = Counter(
            str(item.get('grade_label'))
            for item in evaluations
            if item.get('evaluation_status') == 'graded' and item.get('grade_label')
        )
        return {
            'decision_count': len(rows),
            'symbol_count': len({row.symbol for row in rows}),
            'policy_versions': sorted({row.policy_version for row in rows if row.policy_version}),
            'decision_actions': dict(
                sorted(Counter(row.decision_action for row in rows if row.decision_action).items())
            ),
            'evaluation_count': len(evaluations),
            'graded_count': sum(1 for item in evaluations if item.get('evaluation_status') == 'graded'),
            'pending_count': sum(1 for item in evaluations if item.get('evaluation_status') != 'graded'),
            'verdict_counts': dict(sorted(verdict_counts.items())),
        }

    def _build_aggregate_groups(
        self,
        *,
        evaluations: Sequence[dict[str, Any]],
        group_key: str,
    ) -> list[dict[str, Any]]:
        grouped: dict[Any, list[dict[str, Any]]] = {}
        for evaluation in evaluations:
            grouped.setdefault(evaluation.get(group_key), []).append(evaluation)
        items: list[dict[str, Any]] = []
        for group_value, group_evaluations in grouped.items():
            payload = self._aggregate_metric_payload(group_evaluations)
            payload[group_key] = group_value
            items.append(payload)
        if group_key == 'horizon_days':
            items.sort(key=lambda item: int(item.get('horizon_days') or 0))
        else:
            items.sort(key=lambda item: str(item.get(group_key) or ''))
        return items

    def _aggregate_metric_payload(self, evaluations: Sequence[dict[str, Any]]) -> dict[str, Any]:
        graded = [item for item in evaluations if item.get('evaluation_status') == 'graded']
        verdict_counts: Counter[str] = Counter(
            str(item.get('grade_label')) for item in graded if item.get('grade_label')
        )
        action_counts: Counter[str] = Counter(
            str(item.get('decision_action')) for item in evaluations if item.get('decision_action')
        )
        buy_items = [item for item in graded if item.get('decision_action') == 'buy']
        wait_items = [item for item in graded if item.get('decision_action') == 'wait']
        reject_items = [item for item in graded if item.get('decision_action') == 'reject']
        close_returns = self._collect_diagnostic_metric(graded, 'close_return_pct')
        max_upside_returns = self._collect_diagnostic_metric(graded, 'max_upside_return_pct')
        max_drawdown_returns = self._collect_diagnostic_metric(graded, 'max_drawdown_return_pct')
        grade_scores = [_to_float(item.get('grade_score')) for item in graded if _to_float(item.get('grade_score')) is not None]
        return {
            'evaluation_count': len(evaluations),
            'graded_count': len(graded),
            'pending_count': len(evaluations) - len(graded),
            'decision_action_counts': dict(sorted(action_counts.items())),
            'verdict_counts': dict(sorted(verdict_counts.items())),
            'buy_precision': self._rate(
                numerator=sum(1 for item in buy_items if item.get('grade_label') == 'good_buy'),
                denominator=len(buy_items),
            ),
            'avg_close_return_pct': self._average(close_returns),
            'avg_max_upside_return_pct': self._average(max_upside_returns),
            'avg_max_drawdown_return_pct': self._average(max_drawdown_returns),
            'avg_grade_score': self._average(grade_scores),
            'stop_hit_rate': self._diagnostic_bool_rate(buy_items, 'stop_hit'),
            'target_hit_rate': self._diagnostic_bool_rate(buy_items, 'target_hit'),
            'severe_drawdown_before_target_rate': self._diagnostic_bool_rate(
                buy_items,
                'severe_drawdown_before_target',
            ),
            'wait_missed_opportunity_rate': self._rate(
                numerator=sum(1 for item in wait_items if item.get('missed_opportunity') is True),
                denominator=len(wait_items),
            ),
            'reject_missed_opportunity_rate': self._rate(
                numerator=sum(1 for item in reject_items if item.get('missed_opportunity') is True),
                denominator=len(reject_items),
            ),
        }

    def _build_contributor_report(
        self,
        *,
        rows: Sequence[SignalDeciderDecisionSnapshot],
        evaluations_by_snapshot: dict[int, list[dict[str, Any]]],
        limit: int,
    ) -> dict[str, Any]:
        factor_buckets: dict[tuple[str, str], dict[str, Any]] = {}
        gate_buckets: dict[tuple[str, str], dict[str, Any]] = {}
        for row in rows:
            evaluations = [
                item
                for item in evaluations_by_snapshot.get(row.id, [])
                if item.get('evaluation_status') == 'graded'
            ]
            if not evaluations:
                continue
            decision = self._build_decision_snapshot(row)
            factor_tokens, gate_tokens = self._build_contributor_tokens(
                _maybe_dict(decision.get('traceability'))
            )
            for evaluation in evaluations:
                self._update_contributor_buckets(
                    buckets=factor_buckets,
                    tokens=factor_tokens,
                    row=row,
                    evaluation=evaluation,
                )
                self._update_contributor_buckets(
                    buckets=gate_buckets,
                    tokens=gate_tokens,
                    row=row,
                    evaluation=evaluation,
                )
        return {
            'factors': self._finalize_contributor_buckets(factor_buckets, limit=limit),
            'gates': self._finalize_contributor_buckets(gate_buckets, limit=limit),
        }

    def _build_contributor_tokens(
        self,
        traceability: dict[str, Any],
    ) -> tuple[list[tuple[str, str]], list[tuple[str, str]]]:
        factor_tokens: list[tuple[str, str]] = []
        gate_tokens: list[tuple[str, str]] = []
        factor_contributions = _maybe_dict(traceability.get('factor_contributions'))
        gate_checks = _maybe_dict(traceability.get('gate_checks'))
        seen_factors: set[tuple[str, str]] = set()
        for raw_item in factor_contributions.get('feature_flags') or []:
            item = _maybe_dict(raw_item)
            key = _normalize_optional_text(
                str(item.get('key')) if item.get('key') is not None else None
            )
            value = _normalize_optional_text(
                str(item.get('value')) if item.get('value') is not None else None
            )
            if not key or not value:
                continue
            token = (key, value)
            if token in seen_factors:
                continue
            seen_factors.add(token)
            factor_tokens.append(token)
        seen_gates: set[tuple[str, str]] = set()
        for gate_key in (
            'decision_support_only',
            'allow_execution_logic',
            'has_data_warnings',
            'has_specialist_disagreement',
        ):
            if gate_checks.get(gate_key) is True:
                token = (gate_key, 'true')
                if token not in seen_gates:
                    seen_gates.add(token)
                    gate_tokens.append(token)
        for gate_key in (
            'traceability_symbol_matches',
            'traceability_market_date_matches',
            'traceability_agent_name_matches',
        ):
            if gate_checks.get(gate_key) is False:
                token = (gate_key, 'false')
                if token not in seen_gates:
                    seen_gates.add(token)
                    gate_tokens.append(token)
        for raw_role in gate_checks.get('missing_upstream_roles') or []:
            role = _normalize_optional_text(str(raw_role) if raw_role is not None else None)
            if not role:
                continue
            token = ('missing_upstream_role', role)
            if token in seen_gates:
                continue
            seen_gates.add(token)
            gate_tokens.append(token)
        return factor_tokens, gate_tokens

    def _update_contributor_buckets(
        self,
        *,
        buckets: dict[tuple[str, str], dict[str, Any]],
        tokens: Sequence[tuple[str, str]],
        row: SignalDeciderDecisionSnapshot,
        evaluation: dict[str, Any],
    ) -> None:
        for key, value in tokens:
            bucket = buckets.setdefault(
                (key, value),
                {
                    'key': key,
                    'value': value,
                    'sample_count': 0,
                    'decision_action_counts': Counter(),
                    'verdict_counts': Counter(),
                    'negative_outcome_count': 0,
                    'buy_sample_count': 0,
                    'buy_bad_count': 0,
                    'wait_reject_sample_count': 0,
                    'missed_opportunity_count': 0,
                    'close_return_sum': 0.0,
                    'close_return_count': 0,
                    'max_upside_sum': 0.0,
                    'max_upside_count': 0,
                    'grade_score_sum': 0.0,
                    'grade_score_count': 0,
                    'examples': [],
                },
            )
            decision_action = str(evaluation.get('decision_action') or '')
            grade_label = str(evaluation.get('grade_label') or '')
            bucket['sample_count'] += 1
            if decision_action:
                bucket['decision_action_counts'][decision_action] += 1
            if grade_label:
                bucket['verdict_counts'][grade_label] += 1
            if grade_label in NEGATIVE_SIGNAL_DECIDER_GRADE_LABELS:
                bucket['negative_outcome_count'] += 1
            if decision_action == 'buy':
                bucket['buy_sample_count'] += 1
                if grade_label == 'bad_buy':
                    bucket['buy_bad_count'] += 1
            if decision_action in {'wait', 'reject'}:
                bucket['wait_reject_sample_count'] += 1
                if evaluation.get('missed_opportunity') is True:
                    bucket['missed_opportunity_count'] += 1
            diagnostics = _maybe_dict(evaluation.get('diagnostics'))
            close_return_pct = _to_float(diagnostics.get('close_return_pct'))
            if close_return_pct is not None:
                bucket['close_return_sum'] += close_return_pct
                bucket['close_return_count'] += 1
            max_upside_return_pct = _to_float(diagnostics.get('max_upside_return_pct'))
            if max_upside_return_pct is not None:
                bucket['max_upside_sum'] += max_upside_return_pct
                bucket['max_upside_count'] += 1
            grade_score = _to_float(evaluation.get('grade_score'))
            if grade_score is not None:
                bucket['grade_score_sum'] += grade_score
                bucket['grade_score_count'] += 1
            examples: list[dict[str, Any]] = bucket['examples']
            if len(examples) < 3:
                examples.append(
                    {
                        'snapshot_id': row.id,
                        'symbol': row.symbol,
                        'market_date': row.market_date.isoformat(),
                        'decision_action': decision_action,
                        'horizon_days': evaluation.get('horizon_days'),
                        'grade_label': grade_label,
                    }
                )

    def _finalize_contributor_buckets(
        self,
        buckets: dict[tuple[str, str], dict[str, Any]],
        *,
        limit: int,
    ) -> list[dict[str, Any]]:
        items: list[dict[str, Any]] = []
        for bucket in buckets.values():
            items.append(
                {
                    'key': bucket['key'],
                    'value': bucket['value'],
                    'sample_count': bucket['sample_count'],
                    'decision_action_counts': dict(sorted(bucket['decision_action_counts'].items())),
                    'verdict_counts': dict(sorted(bucket['verdict_counts'].items())),
                    'negative_outcome_count': bucket['negative_outcome_count'],
                    'negative_outcome_rate': self._rate(
                        numerator=bucket['negative_outcome_count'],
                        denominator=bucket['sample_count'],
                    ),
                    'bad_buy_rate': self._rate(
                        numerator=bucket['buy_bad_count'],
                        denominator=bucket['buy_sample_count'],
                    ),
                    'missed_opportunity_rate': self._rate(
                        numerator=bucket['missed_opportunity_count'],
                        denominator=bucket['wait_reject_sample_count'],
                    ),
                    'avg_close_return_pct': self._ratio_from_sums(
                        total=bucket['close_return_sum'],
                        count=bucket['close_return_count'],
                    ),
                    'avg_max_upside_return_pct': self._ratio_from_sums(
                        total=bucket['max_upside_sum'],
                        count=bucket['max_upside_count'],
                    ),
                    'avg_grade_score': self._ratio_from_sums(
                        total=bucket['grade_score_sum'],
                        count=bucket['grade_score_count'],
                    ),
                    'examples': list(bucket['examples']),
                }
            )
        items.sort(
            key=lambda item: (
                -int(item.get('negative_outcome_count') or 0),
                -int(item.get('sample_count') or 0),
                str(item.get('key') or ''),
                str(item.get('value') or ''),
            )
        )
        return items[:limit]

    def _collect_diagnostic_metric(
        self,
        evaluations: Sequence[dict[str, Any]],
        key: str,
    ) -> list[float]:
        values: list[float] = []
        for evaluation in evaluations:
            metric = _to_float(_maybe_dict(evaluation.get('diagnostics')).get(key))
            if metric is not None:
                values.append(metric)
        return values

    def _diagnostic_bool_rate(
        self,
        evaluations: Sequence[dict[str, Any]],
        key: str,
    ) -> float | None:
        values = [
            bool(_maybe_dict(item.get('diagnostics')).get(key))
            for item in evaluations
            if _maybe_dict(item.get('diagnostics')).get(key) is not None
        ]
        if not values:
            return None
        return round(sum(1 for value in values if value) / len(values), 6)

    def _average(self, values: Sequence[float]) -> float | None:
        if not values:
            return None
        return round(sum(values) / len(values), 6)

    def _rate(self, *, numerator: int, denominator: int) -> float | None:
        if denominator <= 0:
            return None
        return round(numerator / denominator, 6)

    def _ratio_from_sums(self, *, total: float, count: int) -> float | None:
        if count <= 0:
            return None
        return round(total / count, 6)

    def _build_decision_snapshot(self, row: SignalDeciderDecisionSnapshot) -> dict[str, Any]:
        summary_payload = _maybe_dict(row.response_summary_payload)
        feedback_payload = _maybe_dict(row.response_payload)
        request_payload = _maybe_dict(row.request_payload)
        feature_snapshot = _maybe_dict(row.input_feature_snapshot_payload)
        if not feature_snapshot:
            feature_snapshot = build_signal_decider_feature_snapshot(
                symbol_day_context=_maybe_dict(request_payload.get('symbol_day_context')),
                feedback_payload=feedback_payload,
            )
        policy_version = _normalize_optional_text(row.policy_version, default=DEFAULT_SIGNAL_DECIDER_POLICY_VERSION)
        decision_action = _normalize_action(row.decision_action or feedback_payload.get('next_day_open_action'))
        thesis_summary = str(
            row.thesis_summary
            or feedback_payload.get('thesis_summary')
            or feedback_payload.get('summary')
            or summary_payload.get('summary')
            or ''
        ).strip()
        traceability = _maybe_dict(row.traceability_payload)
        if not traceability.get('schema_version'):
            traceability = build_signal_decider_traceability_snapshot(
                agent_name=row.agent_name,
                agent_version=row.agent_version,
                feedback_id=row.feedback_id,
                decision_run_id=row.decision_run_id,
                market_date=row.market_date,
                symbol=row.symbol,
                policy_version=policy_version or DEFAULT_SIGNAL_DECIDER_POLICY_VERSION,
                decision_action=decision_action,
                thesis_summary=thesis_summary,
                summary_payload=summary_payload,
                feedback_payload=feedback_payload,
                context_payload=request_payload,
                feature_snapshot=feature_snapshot,
            )
        return {
            'snapshot_id': row.id,
            'feedback_id': row.feedback_id,
            'decision_run_id': row.decision_run_id,
            'market_date': row.market_date.isoformat(),
            'symbol': row.symbol,
            'policy_version': policy_version,
            'decision': {
                'action': decision_action,
                'preferred_action': _normalize_action(row.preferred_action or feedback_payload.get('preferred_action')),
                'next_day_open_action': _normalize_action(
                    row.next_day_open_action or feedback_payload.get('next_day_open_action')
                ),
                'entry_price_basis': row.entry_price_basis,
                'target_price': _to_int(row.target_price),
                'expected_return_pct': _to_float(row.expected_return_pct),
                'stop_loss_price': _to_int(row.stop_loss_price),
                'suggested_holding_period': row.suggested_holding_period,
                'confidence_score_10': _to_float(row.confidence_score_10),
                'conviction_score_10': _to_float(row.conviction_score_10),
            },
            'summary': thesis_summary,
            'supporting_evidence': _normalize_text_list(row.supporting_evidence_payload),
            'decision_risks': _normalize_text_list(row.decision_risks_payload),
            'invalidation_conditions': _normalize_text_list(row.invalidation_conditions_payload),
            'data_quality_warnings': _normalize_text_list(row.data_quality_warnings_payload),
            'specialist_disagreement': _normalize_text_list(row.specialist_disagreement_payload),
            'traceability': traceability,
            'feature_snapshot': feature_snapshot,
            'received_at': row.feedback_received_at.isoformat() if row.feedback_received_at else None,
            'created_at': row.created_at.isoformat() if row.created_at else None,
        }

    def _load_future_price_bars(
        self,
        *,
        symbol: str,
        market_date: date,
        max_horizon: int,
    ) -> list[dict[str, Any]]:
        bars = load_daily_bars_for_symbol_range(
            symbol,
            start_date=market_date,
            limit=max_horizon + 2,
            prefer_recent_limit=False,
        )
        return [
            bar
            for bar in bars
            if str(bar.get('trade_date') or '') > market_date.isoformat()
        ][:max_horizon]

    def _build_realized_outcome_item(
        self,
        *,
        snapshot_row: SignalDeciderDecisionSnapshot,
        decision: dict[str, Any],
        future_bars: list[dict[str, Any]],
        horizon_days: int,
    ) -> dict[str, Any]:
        now = self._utcnow()
        decision_fields = _maybe_dict(decision.get('decision'))
        entry_bar = future_bars[0] if future_bars else None
        observed_bars = future_bars[:horizon_days]
        outcome_status = 'complete' if len(observed_bars) >= horizon_days and entry_bar is not None else 'pending'
        if entry_bar is None:
            diagnostics = {
                'reason': 'missing_future_price_bars',
                'required_horizon_days': horizon_days,
            }
            return {
                'decision_snapshot_id': snapshot_row.id,
                'feedback_id': snapshot_row.feedback_id,
                'market_date': snapshot_row.market_date,
                'symbol': snapshot_row.symbol,
                'policy_version': decision['policy_version'],
                'decision_action': decision_fields.get('action') or 'wait',
                'horizon_days': horizon_days,
                'outcome_status': 'pending',
                'entry_trade_date': None,
                'exit_trade_date': None,
                'bars_observed': 0,
                'entry_price': None,
                'exit_price': None,
                'max_high_price': None,
                'min_low_price': None,
                'close_return_pct': None,
                'max_upside_return_pct': None,
                'max_drawdown_return_pct': None,
                'target_price': decision_fields.get('target_price'),
                'stop_loss_price': decision_fields.get('stop_loss_price'),
                'expected_return_pct': decision_fields.get('expected_return_pct'),
                'target_hit': False,
                'stop_loss_hit': False,
                'target_stop_collision': False,
                'price_path_payload': [],
                'diagnostics_payload': diagnostics,
                'computed_at': now,
                'created_at': now,
                'updated_at': now,
            }

        entry_price = _to_int(entry_bar.get('open_price'))
        exit_bar = observed_bars[-1] if observed_bars else entry_bar
        exit_price = _to_int(exit_bar.get('close_price'))
        max_high_price = max((_to_int(bar.get('high_price')) or 0) for bar in observed_bars) if observed_bars else None
        min_low_price = min((_to_int(bar.get('low_price')) or 0) for bar in observed_bars) if observed_bars else None
        best_exit_bar = (
            max(
                observed_bars,
                key=lambda bar: (_to_int(bar.get('high_price')) or 0, str(bar.get('trade_date') or '')),
            )
            if observed_bars
            else None
        )
        worst_drawdown_bar = (
            min(
                observed_bars,
                key=lambda bar: (_to_int(bar.get('low_price')) or 10**18, str(bar.get('trade_date') or '')),
            )
            if observed_bars
            else None
        )
        close_return_pct = self._return_pct(exit_price, entry_price)
        max_upside_return_pct = self._return_pct(max_high_price, entry_price)
        max_drawdown_return_pct = self._return_pct(min_low_price, entry_price)
        target_price = decision_fields.get('target_price')
        stop_loss_price = decision_fields.get('stop_loss_price')
        target_hits = [
            str(bar.get('trade_date'))
            for bar in observed_bars
            if target_price is not None and (_to_int(bar.get('high_price')) or 0) >= target_price
        ]
        stop_hits = [
            str(bar.get('trade_date'))
            for bar in observed_bars
            if stop_loss_price is not None and (_to_int(bar.get('low_price')) or 0) <= stop_loss_price
        ]
        target_stop_collision = bool(set(target_hits) & set(stop_hits))
        diagnostics = {
            'entry_trade_date': entry_bar.get('trade_date'),
            'exit_trade_date': exit_bar.get('trade_date'),
            'best_exit_trade_date': best_exit_bar.get('trade_date') if best_exit_bar else None,
            'worst_drawdown_trade_date': worst_drawdown_bar.get('trade_date') if worst_drawdown_bar else None,
            'first_target_hit_trade_date': target_hits[0] if target_hits else None,
            'first_stop_loss_hit_trade_date': stop_hits[0] if stop_hits else None,
            'observed_trade_dates': [bar.get('trade_date') for bar in observed_bars],
            'target_hit_trade_dates': target_hits,
            'stop_loss_hit_trade_dates': stop_hits,
            'target_stop_collision': target_stop_collision,
            'expected_horizon_days': horizon_days,
            'bars_observed': len(observed_bars),
        }
        return {
            'decision_snapshot_id': snapshot_row.id,
            'feedback_id': snapshot_row.feedback_id,
            'market_date': snapshot_row.market_date,
            'symbol': snapshot_row.symbol,
            'policy_version': decision['policy_version'],
            'decision_action': decision_fields.get('action') or 'wait',
            'horizon_days': horizon_days,
            'outcome_status': outcome_status,
            'entry_trade_date': self._maybe_date(entry_bar.get('trade_date')),
            'exit_trade_date': self._maybe_date(exit_bar.get('trade_date')),
            'bars_observed': len(observed_bars),
            'entry_price': entry_price,
            'exit_price': exit_price,
            'max_high_price': max_high_price,
            'min_low_price': min_low_price,
            'close_return_pct': close_return_pct,
            'max_upside_return_pct': max_upside_return_pct,
            'max_drawdown_return_pct': max_drawdown_return_pct,
            'target_price': target_price,
            'stop_loss_price': stop_loss_price,
            'expected_return_pct': decision_fields.get('expected_return_pct'),
            'target_hit': bool(target_hits),
            'stop_loss_hit': bool(stop_hits),
            'target_stop_collision': target_stop_collision,
            'price_path_payload': [
                {
                    'trade_date': bar.get('trade_date'),
                    'open_price': _to_int(bar.get('open_price')),
                    'high_price': _to_int(bar.get('high_price')),
                    'low_price': _to_int(bar.get('low_price')),
                    'close_price': _to_int(bar.get('close_price')),
                    'volume': _to_int(bar.get('volume')),
                }
                for bar in observed_bars
            ],
            'diagnostics_payload': diagnostics,
            'computed_at': now,
            'created_at': now,
            'updated_at': now,
        }

    def _build_evaluation_item(
        self,
        *,
        snapshot_row: SignalDeciderDecisionSnapshot,
        decision: dict[str, Any],
        outcome: dict[str, Any],
        horizon_days: int,
        evaluation_version: str,
        evaluation_profile: SignalDeciderEvaluationProfile,
    ) -> dict[str, Any]:
        now = self._utcnow()
        decision_fields = _maybe_dict(decision.get('decision'))
        thresholds = HORIZON_THRESHOLDS[horizon_days]
        decision_action = _normalize_action(decision_fields.get('action'))
        outcome_diagnostics = _maybe_dict(outcome.get('diagnostics'))
        close_return_pct = _to_float(outcome.get('close_return_pct'))
        max_upside_return_pct = _to_float(outcome.get('max_upside_return_pct'))
        max_drawdown_return_pct = _to_float(outcome.get('max_drawdown_return_pct'))
        target_hit = bool(outcome.get('target_hit'))
        stop_loss_hit = bool(outcome.get('stop_loss_hit'))
        severe_drawdown_limit_pct = round(
            thresholds.loss_return_pct * evaluation_profile.buy_severe_drawdown_multiplier,
            6,
        )
        price_path = outcome.get('price_path')
        worst_drawdown_trade_date = _normalize_optional_text(
            str(outcome_diagnostics.get('worst_drawdown_trade_date'))
            if outcome_diagnostics.get('worst_drawdown_trade_date') is not None
            else None
        )
        first_target_hit_trade_date = _normalize_optional_text(
            str(outcome_diagnostics.get('first_target_hit_trade_date'))
            if outcome_diagnostics.get('first_target_hit_trade_date') is not None
            else None
        )
        first_severe_drawdown_trade_date = self._first_severe_drawdown_trade_date(
            entry_price=_to_int(outcome.get('entry_price')),
            price_path=price_path,
            severe_drawdown_limit_pct=severe_drawdown_limit_pct,
            first_target_hit_trade_date=first_target_hit_trade_date,
        )
        severe_drawdown_before_target = bool(first_severe_drawdown_trade_date)
        if not isinstance(price_path, list) or not price_path:
            severe_drawdown_before_target = bool(
                max_drawdown_return_pct is not None
                and max_drawdown_return_pct <= -severe_drawdown_limit_pct
                and (
                    first_target_hit_trade_date is None
                    or (
                        worst_drawdown_trade_date is not None
                        and worst_drawdown_trade_date <= first_target_hit_trade_date
                    )
                )
            )
        evaluation_status = 'graded'
        grade_label = 'pending_horizon'
        missed_opportunity: bool | None = None
        counterfactual_action: str | None = None
        rationale: dict[str, Any]
        grade_score: float | None = None

        if outcome.get('outcome_status') != 'complete':
            evaluation_status = 'pending'
            rationale = {
                'status': 'pending_horizon',
                'reason': 'insufficient_price_history',
                'evaluation_profile': self._evaluation_profile_payload(
                    evaluation_profile=evaluation_profile,
                    horizon_days=horizon_days,
                ),
            }
        elif decision_action == 'buy':
            positive_signals = int(target_hit)
            if close_return_pct is not None and close_return_pct >= thresholds.good_return_pct:
                positive_signals += 1
            if max_upside_return_pct is not None and max_upside_return_pct >= thresholds.opportunity_return_pct:
                positive_signals += 1
            negative_signals = int(stop_loss_hit)
            if close_return_pct is not None and close_return_pct <= -thresholds.loss_return_pct:
                negative_signals += 1
            if max_drawdown_return_pct is not None and max_drawdown_return_pct <= -thresholds.loss_return_pct:
                negative_signals += 1
            if severe_drawdown_before_target:
                negative_signals += 1
            if severe_drawdown_before_target:
                grade_label = 'bad_buy'
            else:
                grade_label = 'good_buy' if positive_signals >= negative_signals else 'bad_buy'
            grade_score = self._buy_grade_score(
                close_return_pct=close_return_pct,
                max_drawdown_return_pct=max_drawdown_return_pct,
                target_hit=target_hit,
                stop_loss_hit=stop_loss_hit,
                severe_drawdown_before_target=severe_drawdown_before_target,
            )
            rationale = {
                'status': 'graded',
                'target_hit': target_hit,
                'stop_loss_hit': stop_loss_hit,
                'positive_signals': positive_signals,
                'negative_signals': negative_signals,
                'severe_drawdown_before_target': severe_drawdown_before_target,
                'evaluation_profile': self._evaluation_profile_payload(
                    evaluation_profile=evaluation_profile,
                    horizon_days=horizon_days,
                ),
            }
        else:
            missed = bool(
                (max_upside_return_pct is not None and max_upside_return_pct >= thresholds.opportunity_return_pct)
                or (close_return_pct is not None and close_return_pct >= thresholds.good_return_pct)
            )
            missed_opportunity = missed
            counterfactual_action = 'buy' if missed else decision_action
            grade_label = (
                f'missed_opportunity_after_{decision_action}'
                if missed
                else f'correct_{decision_action}'
            )
            grade_score = self._wait_reject_grade_score(
                close_return_pct=close_return_pct,
                max_upside_return_pct=max_upside_return_pct,
                missed_opportunity=missed,
            )
            rationale = {
                'status': 'graded',
                'missed_opportunity': missed,
                'counterfactual_action': counterfactual_action,
                'evaluation_profile': self._evaluation_profile_payload(
                    evaluation_profile=evaluation_profile,
                    horizon_days=horizon_days,
                ),
            }

        diagnostics = {
            'evaluation_rule_set_version': evaluation_profile.rule_set_version,
            'close_return_pct': close_return_pct,
            'max_upside_return_pct': max_upside_return_pct,
            'max_drawdown_return_pct': max_drawdown_return_pct,
            'target_hit': target_hit,
            'stop_loss_hit': stop_loss_hit,
            'best_exit_trade_date': outcome_diagnostics.get('best_exit_trade_date'),
            'worst_drawdown_trade_date': outcome_diagnostics.get('worst_drawdown_trade_date'),
            'first_severe_drawdown_trade_date': first_severe_drawdown_trade_date,
            'first_target_hit_trade_date': outcome_diagnostics.get('first_target_hit_trade_date'),
            'first_stop_loss_hit_trade_date': outcome_diagnostics.get('first_stop_loss_hit_trade_date'),
            'severe_drawdown_before_target': severe_drawdown_before_target if decision_action == 'buy' else None,
            'expected_return_pct': decision_fields.get('expected_return_pct'),
            'feature_snapshot': decision.get('feature_snapshot'),
        }
        return {
            'decision_snapshot_id': snapshot_row.id,
            'feedback_id': snapshot_row.feedback_id,
            'realized_outcome_id': outcome.get('id'),
            'market_date': snapshot_row.market_date,
            'symbol': snapshot_row.symbol,
            'policy_version': decision['policy_version'],
            'decision_action': decision_action,
            'horizon_days': horizon_days,
            'evaluation_version': evaluation_version,
            'evaluation_status': evaluation_status,
            'grade_label': grade_label,
            'grade_score': grade_score,
            'missed_opportunity': missed_opportunity,
            'counterfactual_action': counterfactual_action,
            'rationale_payload': rationale,
            'diagnostics_payload': diagnostics,
            'evaluated_at': now,
            'created_at': now,
            'updated_at': now,
        }

    def _candidate_support_flag(self, *, decision_action: str, grade_label: str) -> dict[str, Any] | None:
        if decision_action == 'buy':
            return {
                'candidate_kind': 'tighten_buy_gate',
                'supports_candidate': grade_label == 'bad_buy',
            }
        if decision_action == 'wait':
            return {
                'candidate_kind': 'relax_wait_gate',
                'supports_candidate': grade_label == 'missed_opportunity_after_wait',
            }
        if decision_action == 'reject':
            return {
                'candidate_kind': 'relax_reject_gate',
                'supports_candidate': grade_label == 'missed_opportunity_after_reject',
            }
        return None

    def _build_feature_flags(self, feature_snapshot: dict[str, Any]) -> list[tuple[str, str]]:
        return _build_feature_flags(feature_snapshot)

    def _candidate_version(
        self,
        *,
        base_policy_version: str,
        evaluation_version: str,
        horizon_days: int,
        candidate_kind: str,
        feature_key: str,
        feature_value: str,
        market_date_from: date,
        market_date_to: date,
    ) -> str:
        digest = hashlib.sha1(
            '|'.join(
                [
                    base_policy_version,
                    evaluation_version,
                    str(horizon_days),
                    candidate_kind,
                    feature_key,
                    feature_value,
                    market_date_from.isoformat(),
                    market_date_to.isoformat(),
                ]
            ).encode('utf-8')
        ).hexdigest()[:12]
        return f'{base_policy_version}-h{horizon_days}-{candidate_kind}-{digest}'

    def _candidate_summary(
        self,
        *,
        decision_action: str,
        candidate_kind: str,
        feature_key: str,
        feature_value: str,
        support_ratio: float,
        support_count: int,
        total_count: int,
        horizon_days: int,
    ) -> str:
        if candidate_kind == 'tighten_buy_gate':
            return (
                f'Tighten buy decisions when {feature_key}={feature_value}: '
                f'{support_count}/{total_count} buys graded bad over {horizon_days}d '
                f'({support_ratio:.0%}).'
            )
        return (
            f'Relax {decision_action} decisions when {feature_key}={feature_value}: '
            f'{support_count}/{total_count} {decision_action} calls missed upside over {horizon_days}d '
            f'({support_ratio:.0%}).'
        )

    def _candidate_hypothesis(
        self,
        *,
        decision_action: str,
        candidate_kind: str,
        feature_key: str,
        feature_value: str,
        horizon_days: int,
    ) -> str:
        if candidate_kind == 'tighten_buy_gate':
            return (
                f'When {feature_key}={feature_value}, buy decisions appear over-aggressive on the '
                f'{horizon_days}-day horizon and should face a stricter manual review gate.'
            )
        return (
            f'When {feature_key}={feature_value}, {decision_action} decisions appear too conservative on the '
            f'{horizon_days}-day horizon and should allow a looser path toward buy review.'
        )

    def _group_by_snapshot_id(self, rows: Sequence[dict[str, Any]]) -> dict[int, list[dict[str, Any]]]:
        grouped: dict[int, list[dict[str, Any]]] = {}
        for row in rows:
            snapshot_id = int(row['decision_snapshot_id'])
            grouped.setdefault(snapshot_id, []).append(row)
        for items in grouped.values():
            items.sort(key=lambda item: int(item.get('horizon_days') or 0))
        return grouped

    def _resolve_evaluation_profile(self, evaluation_version: str) -> SignalDeciderEvaluationProfile:
        requested_version = (
            _normalize_optional_text(evaluation_version, default=DEFAULT_SIGNAL_DECIDER_EVALUATION_VERSION)
            or DEFAULT_SIGNAL_DECIDER_EVALUATION_VERSION
        )
        return SignalDeciderEvaluationProfile(
            requested_version=requested_version,
            rule_set_version=DEFAULT_SIGNAL_DECIDER_EVALUATION_VERSION,
        )

    def _evaluation_profile_payload(
        self,
        *,
        evaluation_profile: SignalDeciderEvaluationProfile,
        horizon_days: int,
    ) -> dict[str, Any]:
        thresholds = self._threshold_payload(horizon_days)
        return {
            'requested_version': evaluation_profile.requested_version,
            'rule_set_version': evaluation_profile.rule_set_version,
            **thresholds,
            'buy_severe_drawdown_pct': round(
                thresholds['loss_return_pct'] * evaluation_profile.buy_severe_drawdown_multiplier,
                6,
            ),
        }

    def _threshold_payload(self, horizon_days: int) -> dict[str, Any]:
        thresholds = HORIZON_THRESHOLDS[horizon_days]
        return {
            'good_return_pct': thresholds.good_return_pct,
            'opportunity_return_pct': thresholds.opportunity_return_pct,
            'loss_return_pct': thresholds.loss_return_pct,
        }

    def _return_pct(self, numerator_price: int | None, denominator_price: int | None) -> float | None:
        if numerator_price is None or denominator_price is None or denominator_price <= 0:
            return None
        return round((numerator_price / denominator_price) - 1.0, 6)

    def _first_severe_drawdown_trade_date(
        self,
        *,
        entry_price: int | None,
        price_path: Any,
        severe_drawdown_limit_pct: float,
        first_target_hit_trade_date: str | None,
    ) -> str | None:
        if entry_price is None or not isinstance(price_path, list):
            return None
        for bar in price_path:
            bar_payload = _maybe_dict(bar)
            trade_date = _normalize_optional_text(
                str(bar_payload.get('trade_date')) if bar_payload.get('trade_date') is not None else None
            )
            if trade_date is None:
                continue
            if first_target_hit_trade_date is not None and trade_date > first_target_hit_trade_date:
                continue
            low_return_pct = self._return_pct(_to_int(bar_payload.get('low_price')), entry_price)
            if low_return_pct is not None and low_return_pct <= -severe_drawdown_limit_pct:
                return trade_date
        return None

    def _buy_grade_score(
        self,
        *,
        close_return_pct: float | None,
        max_drawdown_return_pct: float | None,
        target_hit: bool,
        stop_loss_hit: bool,
        severe_drawdown_before_target: bool,
    ) -> float | None:
        if close_return_pct is None and max_drawdown_return_pct is None:
            return None
        score = float(close_return_pct or 0.0) * 100.0
        score += 5.0 if target_hit else 0.0
        score -= 5.0 if stop_loss_hit else 0.0
        score -= 7.5 if severe_drawdown_before_target else 0.0
        if max_drawdown_return_pct is not None and max_drawdown_return_pct < 0:
            score += float(max_drawdown_return_pct) * 50.0
        return round(score, 6)

    def _wait_reject_grade_score(
        self,
        *,
        close_return_pct: float | None,
        max_upside_return_pct: float | None,
        missed_opportunity: bool,
    ) -> float | None:
        reference = max_upside_return_pct if max_upside_return_pct is not None else close_return_pct
        if reference is None:
            return None
        score = float(reference) * 100.0
        return round(-score if missed_opportunity else max(0.0, 10.0 - score), 6)

    def _utcnow(self):
        from datetime import datetime, timezone

        return datetime.now(timezone.utc)

    def _maybe_date(self, value: Any) -> date | None:
        if isinstance(value, date):
            return value
        if isinstance(value, str) and value.strip():
            return date.fromisoformat(value.strip())
        return None


signal_decider_loop_service = SignalDeciderLoopService()
