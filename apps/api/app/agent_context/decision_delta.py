from __future__ import annotations

from datetime import date
from typing import Any

from app.agent_context.roles import SHORT_TERM_DECIDER_AGENT_NAME, SIGNAL_DECIDER_AGENT_NAME
from app.agent_context.wait_reason_taxonomy import (
    WAIT_REASON_DEFINITIONS_BY_CODE,
    derive_wait_reason,
    normalize_wait_reason_code,
)


SIGNAL_DECIDER_ACTIONS = {'buy', 'wait', 'reject'}
SHORT_TERM_DECIDER_ACTIONS = {'scalp_buy_candidate', 'watch_for_trigger', 'avoid_chase', 'wait', 'reject'}

DECISION_DELTA_COMPARISON_FIELDS = (
    'recommendation',
    'recommendation_class',
    'wait_reason',
    'summary',
    'confidence_score_10',
    'conviction_score_10',
    'target_price',
    'expected_return_pct',
    'stop_loss_price',
    'trigger_price',
    'chase_limit_price',
    'supporting_evidence',
    'decision_risks',
    'invalidation_conditions',
    'data_quality_warnings',
    'upgrade_trigger',
)

DEFAULT_UPGRADE_TRIGGER_SUMMARIES = {
    'extended_entry_price': 'A pullback into a better entry zone or a calmer re-entry price is needed before upgrading.',
    'unattractive_risk_reward': 'The setup needs a cheaper entry or stronger upside so reward clearly exceeds risk before upgrading.',
    'insufficient_liquidity': 'Turnover, spread quality, or execution liquidity needs to improve before upgrading.',
    'stale_quote_orderbook': 'Fresh realtime quote and orderbook evidence is needed before upgrading.',
    'high_volatility_drawdown_risk': 'Volatility and drawdown risk need to compress before upgrading.',
    'weak_trend_relative_strength': 'Trend and relative-strength confirmation need to improve before upgrading.',
    'unsuitable_execution_timing': 'A better trigger or session-timing confirmation is needed before upgrading.',
    'core_execution_evidence_gap': 'Missing core execution evidence must be restored before upgrading.',
}


def _normalize_optional_text(value: Any) -> str | None:
    if value is None:
        return None
    normalized = value.strip() if isinstance(value, str) else str(value).strip()
    return normalized or None


def _normalize_text_list(value: Any) -> list[str]:
    if isinstance(value, str):
        normalized = value.strip()
        return [normalized] if normalized else []
    if not isinstance(value, list):
        return []
    items: list[str] = []
    seen: set[str] = set()
    for item in value:
        normalized = _normalize_optional_text(item)
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        items.append(normalized)
    return items


def _normalize_number(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        normalized = value.strip()
        if not normalized:
            return None
        try:
            return float(normalized)
        except ValueError:
            return None
    return None


def _normalize_int(value: Any) -> int | None:
    normalized = _normalize_number(value)
    if normalized is None:
        return None
    return int(round(normalized))


def _normalize_iso_date(value: Any) -> str | None:
    if isinstance(value, date):
        return value.isoformat()
    return _normalize_optional_text(value)


def _normalize_string_list(value: Any) -> list[str]:
    return _normalize_text_list(value)


def _normalize_recommendation(role: str, value: Any) -> str:
    normalized = _normalize_optional_text(value)
    if normalized is None:
        return 'wait'
    normalized = normalized.lower()
    if role == SHORT_TERM_DECIDER_AGENT_NAME and normalized in SHORT_TERM_DECIDER_ACTIONS:
        return normalized
    if normalized in SIGNAL_DECIDER_ACTIONS:
        return normalized
    return 'wait'


def recommendation_class_for_role(*, role: str, recommendation: Any) -> str:
    normalized = _normalize_recommendation(role, recommendation)
    if role == SHORT_TERM_DECIDER_AGENT_NAME:
        if normalized == 'scalp_buy_candidate':
            return 'buy'
        if normalized == 'watch_for_trigger':
            return 'watch'
        if normalized in {'avoid_chase', 'wait'}:
            return 'wait'
        return 'reject'
    if normalized == 'buy':
        return 'buy'
    if normalized == 'wait':
        return 'wait'
    return 'reject'


def _normalize_wait_reason(value: Any) -> dict[str, Any] | None:
    if isinstance(value, dict):
        code = normalize_wait_reason_code(
            value.get('code') or value.get('reason_code') or value.get('key')
        )
        summary = _normalize_optional_text(
            value.get('summary')
            or value.get('message')
            or value.get('detail')
            or value.get('explanation')
        )
    elif isinstance(value, str):
        code = normalize_wait_reason_code(value)
        summary = None if code else _normalize_optional_text(value)
    else:
        return None

    if code is None and summary is None:
        return None
    definition = WAIT_REASON_DEFINITIONS_BY_CODE.get(code) if code else None
    return {
        'code': code,
        'label': definition.label if definition is not None else None,
        'bucket': definition.bucket if definition is not None else None,
        'blocking_by_default': definition.blocking_by_default if definition is not None else None,
        'summary': summary or (definition.default_summary if definition is not None else None),
    }


def _normalize_upgrade_trigger(
    value: Any,
    *,
    fallback_target_action: str | None = None,
) -> dict[str, Any] | None:
    if isinstance(value, str):
        summary = _normalize_optional_text(value)
        if summary is None:
            return None
        return {
            'target_action': fallback_target_action,
            'summary': summary,
            'conditions': [],
            'trigger_price': None,
            'chase_limit_price': None,
        }

    if not isinstance(value, dict):
        return None

    conditions = _normalize_text_list(
        value.get('conditions')
        or value.get('condition_list')
        or value.get('triggers')
    )
    summary = _normalize_optional_text(
        value.get('summary')
        or value.get('message')
        or value.get('detail')
        or value.get('guidance')
    )
    trigger_price = _normalize_int(value.get('trigger_price'))
    chase_limit_price = _normalize_int(value.get('chase_limit_price'))
    target_action = _normalize_optional_text(value.get('target_action')) or fallback_target_action

    if summary is None and conditions:
        summary = conditions[0]
    if summary is None and trigger_price is not None:
        summary = f'Trigger above {trigger_price}.'

    if summary is None and not conditions and trigger_price is None and chase_limit_price is None:
        return None

    return {
        'target_action': target_action,
        'summary': summary,
        'conditions': conditions,
        'trigger_price': trigger_price,
        'chase_limit_price': chase_limit_price,
    }


def _normalize_explicit_decision_delta(
    value: Any,
    *,
    fallback_target_action: str | None = None,
) -> dict[str, Any] | None:
    if not isinstance(value, dict):
        return None
    return {
        'previous_market_date': _normalize_iso_date(value.get('previous_market_date')),
        'previous_received_at': _normalize_optional_text(value.get('previous_received_at')),
        'previous_recommendation': _normalize_optional_text(value.get('previous_recommendation')),
        'previous_recommendation_class': _normalize_optional_text(value.get('previous_recommendation_class')),
        'previous_wait_reason': _normalize_wait_reason(value.get('previous_wait_reason')),
        'current_recommendation': _normalize_optional_text(value.get('current_recommendation')),
        'current_recommendation_class': _normalize_optional_text(value.get('current_recommendation_class')),
        'current_wait_reason': _normalize_wait_reason(value.get('current_wait_reason')),
        'transition': _normalize_optional_text(value.get('transition')),
        'persistence_trend': _normalize_optional_text(value.get('persistence_trend')),
        'changed_fields': _normalize_string_list(value.get('changed_fields')),
        'stable_fields': _normalize_string_list(value.get('stable_fields')),
        'what_changed': _normalize_optional_text(
            value.get('what_changed')
            or value.get('change_summary')
            or value.get('changed_summary')
        ),
        'what_stayed_same': _normalize_optional_text(
            value.get('what_stayed_same')
            or value.get('stability_summary')
            or value.get('stable_summary')
        ),
        'upgrade_trigger': _normalize_upgrade_trigger(
            value.get('upgrade_trigger'),
            fallback_target_action=fallback_target_action,
        ),
    }


def _derive_wait_reason_for_payload(*, role: str, recommendation: str, payload: dict[str, Any]) -> dict[str, Any] | None:
    wait_reason = _normalize_wait_reason(payload.get('wait_reason'))
    if wait_reason is not None:
        return wait_reason
    derived_wait_reason, _issues = derive_wait_reason(
        role=role,
        action=recommendation,
        payload=payload,
    )
    return _normalize_wait_reason(derived_wait_reason)


def _fallback_target_action_for_wait(*, role: str) -> str | None:
    if role == SHORT_TERM_DECIDER_AGENT_NAME:
        return 'watch_for_trigger'
    if role == SIGNAL_DECIDER_AGENT_NAME:
        return 'buy'
    return None


def build_decision_reference(
    *,
    role: str,
    payload: dict[str, Any] | None,
    market_date: str | date | None = None,
    symbol: str | None = None,
    feedback_id: int | None = None,
    received_at: str | None = None,
) -> dict[str, Any] | None:
    normalized_payload = payload if isinstance(payload, dict) else {}
    if not normalized_payload and market_date is None and symbol is None and feedback_id is None and received_at is None:
        return None

    if role == SHORT_TERM_DECIDER_AGENT_NAME:
        recommendation = _normalize_recommendation(
            role,
            normalized_payload.get('tactical_action') or normalized_payload.get('preferred_action'),
        )
        decision_risks = _normalize_text_list(
            normalized_payload.get('tactical_risks') or normalized_payload.get('decision_risks')
        )
        fallback_target_action = 'scalp_buy_candidate'
    else:
        recommendation = _normalize_recommendation(
            role,
            normalized_payload.get('next_day_open_action') or normalized_payload.get('preferred_action'),
        )
        decision_risks = _normalize_text_list(normalized_payload.get('decision_risks'))
        fallback_target_action = _fallback_target_action_for_wait(role=role)

    explicit_delta = _normalize_explicit_decision_delta(
        normalized_payload.get('decision_delta'),
        fallback_target_action=fallback_target_action,
    )
    wait_reason = _derive_wait_reason_for_payload(
        role=role,
        recommendation=recommendation,
        payload=normalized_payload,
    )
    trigger_conditions = _normalize_text_list(normalized_payload.get('trigger_conditions'))
    upgrade_trigger = (
        explicit_delta.get('upgrade_trigger')
        if explicit_delta is not None and explicit_delta.get('upgrade_trigger') is not None
        else _normalize_upgrade_trigger(
            normalized_payload.get('upgrade_trigger'),
            fallback_target_action=fallback_target_action,
        )
    )
    if upgrade_trigger is None and role == SHORT_TERM_DECIDER_AGENT_NAME and (
        trigger_conditions
        or normalized_payload.get('trigger_price') is not None
        or normalized_payload.get('chase_limit_price') is not None
    ):
        upgrade_trigger = {
            'target_action': fallback_target_action,
            'summary': trigger_conditions[0] if trigger_conditions else None,
            'conditions': trigger_conditions,
            'trigger_price': _normalize_int(normalized_payload.get('trigger_price')),
            'chase_limit_price': _normalize_int(normalized_payload.get('chase_limit_price')),
        }

    return {
        'feedback_id': feedback_id,
        'received_at': _normalize_optional_text(received_at),
        'market_date': _normalize_iso_date(market_date or normalized_payload.get('market_date')),
        'symbol': _normalize_optional_text(symbol or normalized_payload.get('symbol')),
        'recommendation': recommendation,
        'recommendation_class': recommendation_class_for_role(role=role, recommendation=recommendation),
        'wait_reason': wait_reason,
        'summary': _normalize_optional_text(
            normalized_payload.get('thesis_summary') or normalized_payload.get('summary')
        ),
        'confidence_score_10': _normalize_number(normalized_payload.get('confidence_score_10')),
        'conviction_score_10': _normalize_number(normalized_payload.get('conviction_score_10')),
        'target_price': _normalize_int(normalized_payload.get('target_price')),
        'expected_return_pct': _normalize_number(normalized_payload.get('expected_return_pct')),
        'stop_loss_price': _normalize_int(normalized_payload.get('stop_loss_price')),
        'trigger_price': _normalize_int(normalized_payload.get('trigger_price')),
        'chase_limit_price': _normalize_int(normalized_payload.get('chase_limit_price')),
        'supporting_evidence': _normalize_text_list(normalized_payload.get('supporting_evidence')),
        'decision_risks': decision_risks,
        'invalidation_conditions': _normalize_text_list(normalized_payload.get('invalidation_conditions')),
        'data_quality_warnings': _normalize_text_list(normalized_payload.get('data_quality_warnings')),
        'trigger_conditions': trigger_conditions,
        'upgrade_trigger': upgrade_trigger,
    }


def build_previous_decision_context(
    *,
    role: str,
    latest_feedback: dict[str, Any] | None,
) -> dict[str, Any] | None:
    if not isinstance(latest_feedback, dict):
        return None
    feedback_payload = latest_feedback.get('feedback_payload')
    reference = build_decision_reference(
        role=role,
        payload=feedback_payload if isinstance(feedback_payload, dict) else {},
        market_date=latest_feedback.get('market_date'),
        symbol=latest_feedback.get('symbol'),
        feedback_id=latest_feedback.get('id'),
        received_at=latest_feedback.get('received_at'),
    )
    if reference is None:
        return None
    return {
        **reference,
        'source': 'persisted_feedback',
        'available': True,
    }


def _comparison_value(reference: dict[str, Any], field_name: str) -> Any:
    value = reference.get(field_name)
    if field_name == 'wait_reason':
        wait_reason = _normalize_wait_reason(value)
        if wait_reason is None:
            return None
        return {'code': wait_reason.get('code'), 'bucket': wait_reason.get('bucket')}
    if field_name == 'upgrade_trigger':
        upgrade_trigger = _normalize_upgrade_trigger(value)
        if upgrade_trigger is None:
            return None
        return {
            'summary': upgrade_trigger.get('summary'),
            'conditions': upgrade_trigger.get('conditions') or [],
            'trigger_price': upgrade_trigger.get('trigger_price'),
            'chase_limit_price': upgrade_trigger.get('chase_limit_price'),
        }
    return value


def _build_changed_and_stable_fields(
    *,
    current_reference: dict[str, Any],
    previous_reference: dict[str, Any] | None,
) -> tuple[list[str], list[str]]:
    if previous_reference is None:
        return [], []
    changed_fields: list[str] = []
    stable_fields: list[str] = []
    for field_name in DECISION_DELTA_COMPARISON_FIELDS:
        current_value = _comparison_value(current_reference, field_name)
        previous_value = _comparison_value(previous_reference, field_name)
        if current_value is None and previous_value is None:
            continue
        if current_value == previous_value:
            stable_fields.append(field_name)
        else:
            changed_fields.append(field_name)
    return changed_fields, stable_fields


def _auto_persistence_trend(
    *,
    current_reference: dict[str, Any],
    previous_reference: dict[str, Any] | None,
    changed_fields: list[str],
) -> str | None:
    if previous_reference is None:
        return 'new'

    previous_class = previous_reference.get('recommendation_class')
    current_class = current_reference.get('recommendation_class')
    current_confidence = _normalize_number(current_reference.get('confidence_score_10'))
    previous_confidence = _normalize_number(previous_reference.get('confidence_score_10'))
    confidence_delta = None
    if current_confidence is not None and previous_confidence is not None:
        confidence_delta = round(current_confidence - previous_confidence, 4)

    if previous_class == 'wait' and current_class in {'watch', 'buy'}:
        return 'improving'
    if previous_class in {'watch', 'buy'} and current_class == 'wait':
        return 'worsening'
    if previous_class != current_class:
        return 'changed'

    if current_class != 'wait':
        if confidence_delta is not None:
            if confidence_delta >= 0.75:
                return 'improving'
            if confidence_delta <= -0.75:
                return 'worsening'
        return 'persistent' if not changed_fields else 'changed'

    previous_wait_code = _normalize_wait_reason(previous_reference.get('wait_reason') or {}).get('code')
    current_wait_code = _normalize_wait_reason(current_reference.get('wait_reason') or {}).get('code')
    if previous_wait_code != current_wait_code:
        return 'changed'
    if confidence_delta is not None:
        if confidence_delta >= 0.75:
            return 'improving'
        if confidence_delta <= -0.75:
            return 'worsening'
    return 'persistent' if not changed_fields else 'persistent'


def _auto_change_summary(
    *,
    changed_fields: list[str],
    stable_fields: list[str],
) -> tuple[str | None, str | None]:
    what_changed = None
    what_stayed_same = None
    if changed_fields:
        what_changed = 'Changed fields: ' + ', '.join(changed_fields) + '.'
    if stable_fields:
        what_stayed_same = 'Stable fields: ' + ', '.join(stable_fields) + '.'
    return what_changed, what_stayed_same


def _auto_upgrade_trigger(
    *,
    role: str,
    current_reference: dict[str, Any],
) -> dict[str, Any] | None:
    if current_reference.get('recommendation_class') != 'wait':
        return None

    target_action = _fallback_target_action_for_wait(role=role)
    current_upgrade_trigger = _normalize_upgrade_trigger(
        current_reference.get('upgrade_trigger'),
        fallback_target_action=target_action,
    )
    if current_upgrade_trigger is not None:
        return current_upgrade_trigger

    if role == SHORT_TERM_DECIDER_AGENT_NAME:
        trigger_conditions = _normalize_text_list(current_reference.get('trigger_conditions'))
        if trigger_conditions or current_reference.get('trigger_price') is not None:
            return {
                'target_action': 'scalp_buy_candidate',
                'summary': trigger_conditions[0] if trigger_conditions else None,
                'conditions': trigger_conditions,
                'trigger_price': _normalize_int(current_reference.get('trigger_price')),
                'chase_limit_price': _normalize_int(current_reference.get('chase_limit_price')),
            }

    wait_reason = _normalize_wait_reason(current_reference.get('wait_reason'))
    wait_reason_code = wait_reason.get('code') if wait_reason is not None else None
    summary = DEFAULT_UPGRADE_TRIGGER_SUMMARIES.get(wait_reason_code)
    if summary is None:
        return None
    return {
        'target_action': target_action,
        'summary': summary,
        'conditions': [],
        'trigger_price': None,
        'chase_limit_price': None,
    }


def build_decision_delta(
    *,
    role: str,
    payload: dict[str, Any] | None,
    market_date: str | date | None,
    symbol: str | None,
    previous_decision_context: dict[str, Any] | None = None,
    feedback_id: int | None = None,
    received_at: str | None = None,
) -> dict[str, Any]:
    current_reference = build_decision_reference(
        role=role,
        payload=payload,
        market_date=market_date,
        symbol=symbol,
        feedback_id=feedback_id,
        received_at=received_at,
    ) or {
        'feedback_id': feedback_id,
        'received_at': _normalize_optional_text(received_at),
        'market_date': _normalize_iso_date(market_date),
        'symbol': _normalize_optional_text(symbol),
        'recommendation': 'wait',
        'recommendation_class': 'wait',
        'wait_reason': None,
        'summary': None,
        'confidence_score_10': None,
        'conviction_score_10': None,
        'target_price': None,
        'expected_return_pct': None,
        'stop_loss_price': None,
        'trigger_price': None,
        'chase_limit_price': None,
        'supporting_evidence': [],
        'decision_risks': [],
        'invalidation_conditions': [],
        'data_quality_warnings': [],
        'trigger_conditions': [],
        'upgrade_trigger': None,
    }
    previous_reference = (
        previous_decision_context
        if isinstance(previous_decision_context, dict) and previous_decision_context.get('available') is True
        else None
    )
    explicit_delta = _normalize_explicit_decision_delta(
        _maybe_dict(payload).get('decision_delta'),
        fallback_target_action=_fallback_target_action_for_wait(role=role),
    )
    changed_fields, stable_fields = _build_changed_and_stable_fields(
        current_reference=current_reference,
        previous_reference=previous_reference,
    )
    auto_what_changed, auto_what_stayed_same = _auto_change_summary(
        changed_fields=changed_fields,
        stable_fields=stable_fields,
    )
    current_wait_reason = _normalize_wait_reason(current_reference.get('wait_reason'))
    previous_wait_reason = _normalize_wait_reason(previous_reference.get('wait_reason')) if previous_reference else None
    previous_recommendation_class = previous_reference.get('recommendation_class') if previous_reference else None
    current_recommendation_class = current_reference.get('recommendation_class')
    transition = (
        f'{previous_recommendation_class}_to_{current_recommendation_class}'
        if previous_recommendation_class and current_recommendation_class
        else None
    )
    repeat_wait = previous_recommendation_class == 'wait' and current_recommendation_class == 'wait'

    return {
        'has_previous_decision': previous_reference is not None,
        'previous_feedback_id': previous_reference.get('feedback_id') if previous_reference else None,
        'previous_market_date': (
            explicit_delta.get('previous_market_date')
            if explicit_delta is not None and explicit_delta.get('previous_market_date') is not None
            else (previous_reference.get('market_date') if previous_reference else None)
        ),
        'previous_received_at': (
            explicit_delta.get('previous_received_at')
            if explicit_delta is not None and explicit_delta.get('previous_received_at') is not None
            else (previous_reference.get('received_at') if previous_reference else None)
        ),
        'previous_recommendation': (
            explicit_delta.get('previous_recommendation')
            if explicit_delta is not None and explicit_delta.get('previous_recommendation') is not None
            else (previous_reference.get('recommendation') if previous_reference else None)
        ),
        'previous_recommendation_class': (
            explicit_delta.get('previous_recommendation_class')
            if explicit_delta is not None and explicit_delta.get('previous_recommendation_class') is not None
            else previous_recommendation_class
        ),
        'current_market_date': current_reference.get('market_date'),
        'current_recommendation': (
            explicit_delta.get('current_recommendation')
            if explicit_delta is not None and explicit_delta.get('current_recommendation') is not None
            else current_reference.get('recommendation')
        ),
        'current_recommendation_class': (
            explicit_delta.get('current_recommendation_class')
            if explicit_delta is not None and explicit_delta.get('current_recommendation_class') is not None
            else current_recommendation_class
        ),
        'previous_wait_reason': (
            explicit_delta.get('previous_wait_reason')
            if explicit_delta is not None and explicit_delta.get('previous_wait_reason') is not None
            else previous_wait_reason
        ),
        'current_wait_reason': (
            explicit_delta.get('current_wait_reason')
            if explicit_delta is not None and explicit_delta.get('current_wait_reason') is not None
            else current_wait_reason
        ),
        'transition': (
            explicit_delta.get('transition')
            if explicit_delta is not None and explicit_delta.get('transition') is not None
            else transition
        ),
        'repeat_wait': repeat_wait,
        'changed_fields': (
            explicit_delta.get('changed_fields')
            if explicit_delta is not None and explicit_delta.get('changed_fields')
            else changed_fields
        ),
        'stable_fields': (
            explicit_delta.get('stable_fields')
            if explicit_delta is not None and explicit_delta.get('stable_fields')
            else stable_fields
        ),
        'persistence_trend': (
            explicit_delta.get('persistence_trend')
            if explicit_delta is not None and explicit_delta.get('persistence_trend') is not None
            else _auto_persistence_trend(
                current_reference=current_reference,
                previous_reference=previous_reference,
                changed_fields=changed_fields,
            )
        ),
        'what_changed': (
            explicit_delta.get('what_changed')
            if explicit_delta is not None and explicit_delta.get('what_changed') is not None
            else auto_what_changed
        ),
        'what_stayed_same': (
            explicit_delta.get('what_stayed_same')
            if explicit_delta is not None and explicit_delta.get('what_stayed_same') is not None
            else auto_what_stayed_same
        ),
        'upgrade_trigger': (
            explicit_delta.get('upgrade_trigger')
            if explicit_delta is not None and explicit_delta.get('upgrade_trigger') is not None
            else _auto_upgrade_trigger(
                role=role,
                current_reference=current_reference,
            )
        ),
    }


def _maybe_dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}
