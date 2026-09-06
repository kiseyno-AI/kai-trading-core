from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal, InvalidOperation, ROUND_DOWN
from typing import Any


ALIGNMENT_GATE_SCHEMA_VERSION = 'signal_decider_alignment_gate_v1'
ALIGNMENT_GATE_MODE_DISABLED = 'disabled'
ALIGNMENT_GATE_MODE_REPORT_ONLY = 'report_only'
ALIGNMENT_GATE_MODE_HARD_BLOCK = 'hard_block'
ALIGNMENT_GATE_MODE_DOWNSIZE_PROBE = 'downsize_probe'
VALID_ALIGNMENT_GATE_MODES = {
    ALIGNMENT_GATE_MODE_DISABLED,
    ALIGNMENT_GATE_MODE_REPORT_ONLY,
    ALIGNMENT_GATE_MODE_HARD_BLOCK,
    ALIGNMENT_GATE_MODE_DOWNSIZE_PROBE,
}

SIGNAL_DECIDER_ALIGNMENT_ALIGNED = 'aligned'
SIGNAL_DECIDER_ALIGNMENT_NO_CONTEXT = 'no_signal_decider_context'
SIGNAL_DECIDER_ALIGNMENT_STALE_CONTEXT = 'stale_signal_decider_context'
SIGNAL_DECIDER_ALIGNMENT_WAIT_CONFLICT_BLOCK = 'wait_conflict_block'
SIGNAL_DECIDER_ALIGNMENT_WAIT_CONFLICT_OVERRIDE = 'wait_conflict_override'
SIGNAL_DECIDER_ALIGNMENT_REJECT_CONFLICT_BLOCK = 'reject_conflict_block'
SIGNAL_DECIDER_ALIGNMENT_CONFLICT = 'signal_decider_conflict'

ALIGNMENT_GATE_ACTION_ALLOW = 'allow'
ALIGNMENT_GATE_ACTION_ALLOW_BY_OVERRIDE = 'allow_by_override'
ALIGNMENT_GATE_ACTION_REPORT_ONLY_BLOCK = 'report_only_block_candidate'
ALIGNMENT_GATE_ACTION_BLOCK = 'block'
ALIGNMENT_GATE_ACTION_DOWNSIZE_PROBE = 'downsize_probe'
ALIGNMENT_GATE_ACTION_DISABLED = 'disabled'

STRONG_OVERRIDE_BUY_QUALITY_SCORE = Decimal('70')
STRONG_OVERRIDE_RETURN_PERCENTILE = Decimal('0.75')
STRONG_OVERRIDE_VOLUME_PERCENTILE = Decimal('0.60')
STRONG_OVERRIDE_REWARD_RISK_RATIO = Decimal('2.20')
MIN_OVERRIDE_RAW_TRADING_VALUE_KRW = Decimal('3000000000')


def normalize_alignment_gate_mode(value: Any) -> str:
    normalized = str(value or '').strip().lower()
    if normalized in {'probe', 'downsize'}:
        return ALIGNMENT_GATE_MODE_DOWNSIZE_PROBE
    return normalized if normalized in VALID_ALIGNMENT_GATE_MODES else ALIGNMENT_GATE_MODE_REPORT_ONLY


def _block_action_for_mode(mode: str) -> str:
    if mode == ALIGNMENT_GATE_MODE_DOWNSIZE_PROBE:
        return ALIGNMENT_GATE_ACTION_DOWNSIZE_PROBE
    return (
        ALIGNMENT_GATE_ACTION_BLOCK
        if mode == ALIGNMENT_GATE_MODE_HARD_BLOCK
        else ALIGNMENT_GATE_ACTION_REPORT_ONLY_BLOCK
    )


def _to_decimal(value: Any) -> Decimal | None:
    if value is None or value == '' or isinstance(value, bool):
        return None
    if isinstance(value, Decimal):
        return value
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None


def _decimal_to_text(value: Decimal | None) -> str | None:
    if value is None:
        return None
    return format(value, 'f')


def _json_safe_decimal(value: Decimal | None) -> int | float | None:
    if value is None:
        return None
    if value == value.to_integral_value():
        return int(value)
    return float(value)


def _json_safe(value: Any) -> Any:
    if isinstance(value, Decimal):
        return _json_safe_decimal(value)
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_json_safe(item) for item in value]
    if isinstance(value, tuple):
        return [_json_safe(item) for item in value]
    return value


def _as_unit_percentile(value: Any) -> Decimal | None:
    parsed = _to_decimal(value)
    if parsed is None:
        return None
    if parsed > Decimal('1'):
        return parsed / Decimal('100')
    return parsed


def _signal_decider_snapshot_field(snapshot: Any, field_name: str) -> Any:
    if snapshot is None:
        return None
    if isinstance(snapshot, dict):
        return snapshot.get(field_name)
    return getattr(snapshot, field_name, None)


def build_signal_decider_alignment_context(signal_decider_snapshot: Any) -> dict[str, Any] | None:
    if signal_decider_snapshot is None:
        return None
    action = (
        _signal_decider_snapshot_field(signal_decider_snapshot, 'next_day_open_action')
        or _signal_decider_snapshot_field(signal_decider_snapshot, 'decision_action')
    )
    response_payload = _signal_decider_snapshot_field(signal_decider_snapshot, 'response_payload')
    return {
        'action': str(action).strip().lower() if action is not None else None,
        'market_date': _date_text(_signal_decider_snapshot_field(signal_decider_snapshot, 'market_date')),
        'feedback_received_at': (
            _signal_decider_snapshot_field(signal_decider_snapshot, 'feedback_received_at').isoformat()
            if isinstance(_signal_decider_snapshot_field(signal_decider_snapshot, 'feedback_received_at'), datetime)
            else None
        ),
        'created_at': (
            _signal_decider_snapshot_field(signal_decider_snapshot, 'created_at').isoformat()
            if isinstance(_signal_decider_snapshot_field(signal_decider_snapshot, 'created_at'), datetime)
            else None
        ),
        'decision_snapshot_id': _signal_decider_snapshot_field(signal_decider_snapshot, 'id'),
        'policy_version': _signal_decider_snapshot_field(signal_decider_snapshot, 'policy_version'),
        'confidence_score_10': _json_safe(_signal_decider_snapshot_field(signal_decider_snapshot, 'confidence_score_10')),
        'conviction_score_10': _json_safe(_signal_decider_snapshot_field(signal_decider_snapshot, 'conviction_score_10')),
        'thesis_summary': _signal_decider_snapshot_field(signal_decider_snapshot, 'thesis_summary'),
        'decision_risks': _json_safe(_signal_decider_snapshot_field(signal_decider_snapshot, 'decision_risks_payload') or []),
        'invalidation_conditions': _json_safe(
            _signal_decider_snapshot_field(signal_decider_snapshot, 'invalidation_conditions_payload') or []
        ),
        'data_quality_warnings': _json_safe(
            _signal_decider_snapshot_field(signal_decider_snapshot, 'data_quality_warnings_payload') or []
        ),
        'response_payload': _json_safe(response_payload) if isinstance(response_payload, dict) else {},
    }


def _date_text(value: Any) -> str | None:
    if isinstance(value, datetime):
        return value.date().isoformat()
    if isinstance(value, date):
        return value.isoformat()
    text = str(value or '').strip()
    if not text:
        return None
    try:
        return date.fromisoformat(text[:10]).isoformat()
    except ValueError:
        return None


def _context_status(
    context: dict[str, Any],
    *,
    expected_market_date: date | str | None,
) -> tuple[str, str | None]:
    signal_action = str(context.get('action') or '').strip().lower()
    if not signal_action:
        return 'missing', 'missing_signal_decider_action'
    explicit_status = str(context.get('context_status') or context.get('freshness_status') or '').strip().lower()
    if explicit_status in {'missing', 'unavailable', 'error'}:
        return 'missing', f'signal_decider_context_{explicit_status}'
    if explicit_status in {'stale', 'expired'}:
        return 'stale', f'signal_decider_context_{explicit_status}'
    expected_date_text = _date_text(expected_market_date)
    context_date_text = _date_text(context.get('market_date'))
    if expected_date_text is not None and context_date_text is None:
        return 'missing', 'missing_signal_decider_context_market_date'
    if expected_date_text is not None and context_date_text != expected_date_text:
        return 'stale', 'signal_decider_context_market_date_mismatch'
    return 'fresh', None


def apply_alignment_quantity_enforcement(
    alignment_gate: dict[str, Any],
    *,
    requested_quantity: Any,
    probe_quantity: Any = 1,
) -> dict[str, Any]:
    """Attach the final, auditable quantity decision to an alignment gate."""

    gate = dict(alignment_gate)
    requested = _to_decimal(requested_quantity)
    if requested is not None:
        requested = max(requested.to_integral_value(rounding=ROUND_DOWN), Decimal('0'))
    probe = _to_decimal(probe_quantity)
    if probe is None or probe < Decimal('1'):
        probe = Decimal('1')
    probe = probe.to_integral_value(rounding=ROUND_DOWN)
    action = str(gate.get('alignment_gate_action') or '').strip().lower()

    if action == ALIGNMENT_GATE_ACTION_BLOCK:
        decision = 'blocked'
        final = Decimal('0')
    elif action == ALIGNMENT_GATE_ACTION_DOWNSIZE_PROBE:
        if requested is None or requested < Decimal('1'):
            decision = 'blocked'
            final = Decimal('0')
            gate['alignment_gate_action'] = ALIGNMENT_GATE_ACTION_BLOCK
            gate['alignment_gate_reason'] = (
                f"{gate.get('alignment_gate_reason') or 'Alignment conflict.'} "
                'Probe enforcement requires a positive deterministic requested quantity.'
            )
            gate['enforcement_error'] = 'requested_quantity_unavailable_for_probe'
        else:
            decision = 'downsized'
            final = min(requested, probe)
    elif action == ALIGNMENT_GATE_ACTION_ALLOW_BY_OVERRIDE:
        decision = 'allowed_by_override'
        final = requested
    elif action == ALIGNMENT_GATE_ACTION_REPORT_ONLY_BLOCK:
        decision = 'allowed_report_only'
        final = requested
    elif action == ALIGNMENT_GATE_ACTION_ALLOW:
        decision = 'allowed'
        final = requested
    else:
        decision = 'not_enforced'
        final = requested

    gate.update(
        {
            'enforcement_decision': decision,
            'requested_quantity': _json_safe_decimal(requested),
            'final_quantity': _json_safe_decimal(final),
            'probe_quantity': _json_safe_decimal(probe),
            'quantity_reduction': _json_safe_decimal(
                requested - final if requested is not None and final is not None else None
            ),
        }
    )
    return gate


def _override_evidence(short_term_entry_quality: dict[str, Any] | None) -> dict[str, Any]:
    quality = short_term_entry_quality if isinstance(short_term_entry_quality, dict) else {}
    buy_quality_score = _to_decimal(quality.get('buy_quality_score'))
    return_percentile = _as_unit_percentile(quality.get('return_percentile'))
    volume_percentile = _as_unit_percentile(quality.get('volume_percentile'))
    reward_risk_ratio = _to_decimal(quality.get('reward_risk_ratio'))
    raw_trading_value_krw = _to_decimal(quality.get('raw_trading_value_krw'))
    continuation_setup = bool(quality.get('continuation_setup_detected'))

    checks = {
        'continuation_setup_detected': continuation_setup,
        'reward_risk_ratio_strong': (
            reward_risk_ratio is not None
            and reward_risk_ratio >= STRONG_OVERRIDE_REWARD_RISK_RATIO
        ),
        'buy_quality_or_return_strength': (
            (buy_quality_score is not None and buy_quality_score >= STRONG_OVERRIDE_BUY_QUALITY_SCORE)
            or (return_percentile is not None and return_percentile >= STRONG_OVERRIDE_RETURN_PERCENTILE)
        ),
        'volume_participation_confirmed': (
            volume_percentile is not None
            and volume_percentile >= STRONG_OVERRIDE_VOLUME_PERCENTILE
        ),
        'liquidity_floor_ok': (
            raw_trading_value_krw is not None
            and raw_trading_value_krw >= MIN_OVERRIDE_RAW_TRADING_VALUE_KRW
        ),
    }
    allowed = all(checks.values())
    failed_checks = [key for key, passed in checks.items() if not passed]
    return {
        'allowed': allowed,
        'failed_checks': failed_checks,
        'checks': checks,
        'metrics': {
            'buy_quality_score': _json_safe_decimal(buy_quality_score),
            'return_percentile': _json_safe_decimal(return_percentile),
            'volume_percentile': _json_safe_decimal(volume_percentile),
            'reward_risk_ratio': _json_safe_decimal(reward_risk_ratio),
            'raw_trading_value_krw': _json_safe_decimal(raw_trading_value_krw),
            'continuation_setup_detected': continuation_setup,
        },
        'thresholds': {
            'buy_quality_score': _decimal_to_text(STRONG_OVERRIDE_BUY_QUALITY_SCORE),
            'return_percentile': _decimal_to_text(STRONG_OVERRIDE_RETURN_PERCENTILE),
            'volume_percentile': _decimal_to_text(STRONG_OVERRIDE_VOLUME_PERCENTILE),
            'reward_risk_ratio': _decimal_to_text(STRONG_OVERRIDE_REWARD_RISK_RATIO),
            'raw_trading_value_krw': _decimal_to_text(MIN_OVERRIDE_RAW_TRADING_VALUE_KRW),
        },
    }


def build_signal_decider_alignment_gate(
    *,
    signal_decider_context: dict[str, Any] | None,
    short_term_entry_quality: dict[str, Any] | None,
    tactical_action: str | None,
    mode: str = ALIGNMENT_GATE_MODE_REPORT_ONLY,
    expected_market_date: date | str | None = None,
) -> dict[str, Any]:
    resolved_mode = normalize_alignment_gate_mode(mode)
    if resolved_mode == ALIGNMENT_GATE_MODE_DISABLED:
        return {
            'schema_version': ALIGNMENT_GATE_SCHEMA_VERSION,
            'enforcement_mode': resolved_mode,
            'signal_decider_alignment': SIGNAL_DECIDER_ALIGNMENT_NO_CONTEXT,
            'alignment_gate_action': ALIGNMENT_GATE_ACTION_DISABLED,
            'alignment_gate_reason': 'Signal-decider alignment gate is disabled.',
            'decision_snapshot_id': None,
            'signal_decider_decision_snapshot_id': None,
            'signal_decider_action': None,
            'override': {'allowed': False, 'reason': 'gate_disabled'},
        }

    context = signal_decider_context if isinstance(signal_decider_context, dict) else {}
    signal_action = str(context.get('action') or '').strip().lower()
    context_status, context_status_reason = _context_status(
        context,
        expected_market_date=expected_market_date,
    )
    signal_snapshot_id = context.get('decision_snapshot_id')
    override = _override_evidence(short_term_entry_quality)
    base = {
        'schema_version': ALIGNMENT_GATE_SCHEMA_VERSION,
        'enforcement_mode': resolved_mode,
        'decision_snapshot_id': signal_snapshot_id,
        'signal_decider_decision_snapshot_id': signal_snapshot_id,
        'signal_decider_action': signal_action or None,
        'expected_market_date': _date_text(expected_market_date),
        'signal_decider_context_market_date': _date_text(context.get('market_date')),
        'signal_decider_context_status': context_status,
        'signal_decider_context_status_reason': context_status_reason,
        'signal_decider_policy_version': context.get('policy_version'),
        'signal_decider_confidence_score_10': context.get('confidence_score_10'),
        'signal_decider_conviction_score_10': context.get('conviction_score_10'),
        'signal_decider_context': {
            'thesis_summary': context.get('thesis_summary'),
            'decision_risks': context.get('decision_risks') or [],
            'invalidation_conditions': context.get('invalidation_conditions') or [],
            'data_quality_warnings': context.get('data_quality_warnings') or [],
        },
        'override': override,
    }

    if context_status == 'missing':
        return {
            **base,
            'signal_decider_alignment': SIGNAL_DECIDER_ALIGNMENT_NO_CONTEXT,
            'alignment_gate_action': _block_action_for_mode(resolved_mode),
            'alignment_gate_reason': (
                'No current same-symbol signal-decider context was available; the candidate is not silently approved.'
            ),
        }
    if context_status == 'stale':
        return {
            **base,
            'signal_decider_alignment': SIGNAL_DECIDER_ALIGNMENT_STALE_CONTEXT,
            'alignment_gate_action': _block_action_for_mode(resolved_mode),
            'alignment_gate_reason': (
                'Same-symbol signal-decider context is stale for the source market date; the candidate is not '
                'silently approved.'
            ),
        }
    if signal_action == 'buy':
        return {
            **base,
            'signal_decider_alignment': SIGNAL_DECIDER_ALIGNMENT_ALIGNED,
            'alignment_gate_action': ALIGNMENT_GATE_ACTION_ALLOW,
            'alignment_gate_reason': 'Same-symbol signal-decider context is aligned with a buy action.',
        }
    if signal_action == 'reject':
        return {
            **base,
            'signal_decider_alignment': SIGNAL_DECIDER_ALIGNMENT_REJECT_CONFLICT_BLOCK,
            'alignment_gate_action': _block_action_for_mode(resolved_mode),
            'alignment_gate_reason': 'Same-symbol signal-decider rejected the setup.',
        }
    if signal_action == 'wait':
        if tactical_action == 'watch_for_trigger' and override['allowed']:
            return {
                **base,
                'signal_decider_alignment': SIGNAL_DECIDER_ALIGNMENT_WAIT_CONFLICT_OVERRIDE,
                'alignment_gate_action': ALIGNMENT_GATE_ACTION_ALLOW_BY_OVERRIDE,
                'alignment_gate_reason': (
                    'Signal-decider is WAIT, but the short-term watch setup passed the stronger override threshold.'
                ),
            }
        return {
            **base,
            'signal_decider_alignment': SIGNAL_DECIDER_ALIGNMENT_WAIT_CONFLICT_BLOCK,
            'alignment_gate_action': _block_action_for_mode(resolved_mode),
            'alignment_gate_reason': (
                'Signal-decider is WAIT for the same symbol/date context and the short-term setup did not pass '
                'the stronger override threshold.'
            ),
        }
    return {
        **base,
        'signal_decider_alignment': SIGNAL_DECIDER_ALIGNMENT_CONFLICT,
        'alignment_gate_action': _block_action_for_mode(resolved_mode),
        'alignment_gate_reason': f'Same-symbol signal-decider action {signal_action!r} is not aligned with a short-term buy.',
    }
