from __future__ import annotations

import hashlib
import json
import math
from datetime import date
from typing import Any

from app.agent_context.decision_delta import build_decision_delta
from app.agent_context.roles import SHORT_TERM_DECIDER_AGENT_NAME, SIGNAL_DECIDER_AGENT_NAME
from app.agent_context.wait_reason_taxonomy import (
    WAIT_REASON_ISSUE_GENERIC,
    WAIT_REASON_ISSUE_MISSING,
    derive_wait_reason,
    wait_reason_taxonomy_codes,
)


REQUIRED_RESPONSE_KEYS = ('role', 'summary', 'confidence', 'risk_flags', 'details', 'improvement_notes')
SIGNAL_DECIDER_ACTIONS = {'buy', 'wait', 'reject'}
SHORT_TERM_DECIDER_ACTIONS = {'scalp_buy_candidate', 'watch_for_trigger', 'avoid_chase', 'wait', 'reject'}
SIGNAL_DECIDER_REQUIRED_RESPONSE_KEYS = (
    'role',
    'agent_name',
    'symbol',
    'market_date',
    'preferred_action',
    'next_day_open_action',
    'wait_reason',
    'entry_price_basis',
    'target_price',
    'expected_return_pct',
    'stop_loss_price',
    'suggested_holding_period',
    'confidence_score_10',
    'conviction_score_10',
    'thesis_summary',
    'supporting_evidence',
    'decision_risks',
    'decision_delta',
    'invalidation_conditions',
    'data_quality_warnings',
    'specialist_disagreement',
    'traceability',
    'summary',
    'confidence',
    'risk_flags',
    'details',
    'improvement_notes',
)
SHORT_TERM_DECIDER_REQUIRED_RESPONSE_KEYS = (
    'role',
    'agent_name',
    'symbol',
    'market_date',
    'tactical_action',
    'preferred_action',
    'wait_reason',
    'trigger_price',
    'chase_limit_price',
    'target_price',
    'stop_loss_price',
    'expected_holding_period',
    'confidence_score_10',
    'conviction_score_10',
    'thesis_summary',
    'trigger_conditions',
    'supporting_evidence',
    'tactical_risks',
    'decision_delta',
    'invalidation_conditions',
    'data_quality_warnings',
    'traceability',
    'summary',
    'confidence',
    'risk_flags',
    'details',
    'improvement_notes',
)
STRUCTURAL_IDENTITY_MISMATCH_FLAGS = (
    'role_missing',
    'role_mismatch',
    'agent_name_missing',
    'agent_name_mismatch',
    'symbol_missing',
    'symbol_mismatch',
    'market_date_missing',
    'market_date_mismatch',
    'traceability_missing',
    'traceability_symbol_missing',
    'traceability_symbol_mismatch',
)
AGENT_ROLE_CONTRACT_VERSION = 'agent_role_contract_v1'


class _JSONDuplicateKeyError(ValueError):
    pass


def _reject_duplicate_json_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    decoded: dict[str, Any] = {}
    for key, value in pairs:
        if key in decoded:
            raise _JSONDuplicateKeyError(key)
        decoded[key] = value
    return decoded


def decode_strict_agent_json_object(
    candidate_text: str | None,
) -> tuple[dict[str, Any] | None, str | None]:
    normalized_text = str(candidate_text or '').strip()
    if not normalized_text:
        return None, 'json_object_not_found'
    try:
        candidate_decoded = json.loads(
            normalized_text,
            object_pairs_hook=_reject_duplicate_json_keys,
        )
    except _JSONDuplicateKeyError:
        return None, 'json_duplicate_key'
    except json.JSONDecodeError:
        if '{' not in normalized_text and '[' not in normalized_text:
            return None, 'json_object_not_found'
        return None, 'json_decode_error'
    if isinstance(candidate_decoded, dict):
        return candidate_decoded, None
    return None, 'json_shape_not_object'


def _select_feedback_payload(parsed: dict[str, Any] | None, *, symbol: str | None = None) -> dict[str, Any] | None:
    if not isinstance(parsed, dict):
        return None
    if isinstance(parsed.get('role'), str) and parsed['role'].strip():
        return parsed
    items_source = parsed.get('items')
    batch = parsed.get('batch')
    if not isinstance(items_source, list) and isinstance(batch, dict):
        items_source = batch.get('items')
    if not isinstance(items_source, list):
        return parsed

    normalized_symbol = str(symbol or '').strip() or None
    fallback_feedback: dict[str, Any] | None = None
    for item in items_source:
        if not isinstance(item, dict):
            continue
        item_symbol = str(item.get('symbol') or '').strip() or None
        feedback = item.get('feedback') if isinstance(item.get('feedback'), dict) else None
        if feedback is None:
            feedback = {key: value for key, value in item.items() if key != 'feedback'}
        if not isinstance(feedback, dict):
            continue
        if fallback_feedback is None:
            fallback_feedback = feedback
        feedback_symbol = str(feedback.get('symbol') or '').strip() or item_symbol
        if normalized_symbol is None or feedback_symbol == normalized_symbol or item_symbol == normalized_symbol:
            return feedback
    return fallback_feedback


def _response_envelope_violation_flags(parsed: dict[str, Any] | None) -> list[str]:
    if not isinstance(parsed, dict):
        return []
    if isinstance(parsed.get('role'), str) and parsed['role'].strip():
        return []
    if isinstance(parsed.get('items'), list):
        return ['response_structure_mismatch:batch_envelope']
    batch = parsed.get('batch')
    if isinstance(batch, dict) and isinstance(batch.get('items'), list):
        return ['response_structure_mismatch:batch_envelope']
    return []


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
        normalized = item.strip() if isinstance(item, str) else str(item).strip()
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        items.append(normalized)
    return items


def _normalize_number(value: Any) -> float | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        normalized = float(value)
        return normalized if math.isfinite(normalized) else None
    if isinstance(value, str):
        try:
            normalized = float(value.strip())
            return normalized if math.isfinite(normalized) else None
        except ValueError:
            return None
    return None


def _normalize_score_10(value: Any) -> float | None:
    normalized = _normalize_number(value)
    if normalized is None or normalized < 1 or normalized > 10:
        return None
    return float(normalized)


def _normalize_iso_date(value: Any) -> str | None:
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, str):
        normalized = value.strip()
        return normalized or None
    return None


def _is_raw_numeric_type(value: Any) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def _is_raw_number(value: Any) -> bool:
    return _is_raw_numeric_type(value) and math.isfinite(float(value))


def _non_finite_number_flags(value: Any, *, path: str = '') -> list[str]:
    if isinstance(value, float) and not math.isfinite(value):
        return [f'non_finite_number:{path or "response"}']
    if isinstance(value, dict):
        flags: list[str] = []
        for key, nested in value.items():
            nested_path = f'{path}.{key}' if path else str(key)
            flags.extend(_non_finite_number_flags(nested, path=nested_path))
        return flags
    if isinstance(value, list):
        flags = []
        for index, nested in enumerate(value):
            nested_path = f'{path}[{index}]' if path else f'[{index}]'
            flags.extend(_non_finite_number_flags(nested, path=nested_path))
        return flags
    return []


def _sanitize_non_finite_numbers(value: Any) -> Any:
    if isinstance(value, float) and not math.isfinite(value):
        return None
    if isinstance(value, dict):
        return {
            key: _sanitize_non_finite_numbers(nested)
            for key, nested in value.items()
        }
    if isinstance(value, list):
        return [_sanitize_non_finite_numbers(nested) for nested in value]
    return value


def _is_string_list(value: Any) -> bool:
    return isinstance(value, list) and all(isinstance(item, str) for item in value)


def _missing_required_field_flags(
    payload: dict[str, Any],
    required_fields: tuple[str, ...],
) -> list[str]:
    return [
        f'required_field_missing:{field}'
        for field in required_fields
        if field not in payload
    ]


def _append_core_fallback_violation(payload: dict[str, Any], flags: list[str]) -> None:
    details = payload.get('details')
    if isinstance(details, dict) and details.get('fallback_generated_by_core') is True:
        flags.append('core_generated_fallback')


def _validate_generic_response_schema(payload: dict[str, Any]) -> list[str]:
    flags = _missing_required_field_flags(payload, REQUIRED_RESPONSE_KEYS)
    _append_core_fallback_violation(payload, flags)
    flags.extend(_non_finite_number_flags(payload))
    validators = {
        'role': lambda value: isinstance(value, str) and bool(value.strip()),
        'summary': lambda value: isinstance(value, str) and bool(value.strip()),
        'confidence': lambda value: _is_raw_number(value) and 0 <= float(value) <= 1,
        'risk_flags': _is_string_list,
        'details': lambda value: isinstance(value, dict),
        'improvement_notes': _is_string_list,
    }
    for field in REQUIRED_RESPONSE_KEYS:
        if field not in payload:
            continue
        value = payload[field]
        if validators[field](value):
            continue
        if field in {'role', 'summary', 'confidence'} and (
            (isinstance(value, str) and not value.strip())
            or (field == 'confidence' and _is_raw_numeric_type(value))
        ):
            flags.append(f'required_field_invalid_value:{field}')
        else:
            flags.append(f'required_field_invalid_type:{field}')
    return flags


def _validate_nonempty_string_fields(
    *,
    payload: dict[str, Any],
    fields: tuple[str, ...],
    flags: list[str],
) -> None:
    for field in fields:
        if field not in payload:
            continue
        value = payload[field]
        if not isinstance(value, str):
            flags.append(f'required_field_invalid_type:{field}')
        elif not value.strip():
            flags.append(f'required_field_invalid_value:{field}')


def _validate_nullable_number_fields(
    *,
    payload: dict[str, Any],
    fields: tuple[str, ...],
    flags: list[str],
) -> None:
    for field in fields:
        if field not in payload or payload[field] is None:
            continue
        if not _is_raw_numeric_type(payload[field]):
            flags.append(f'required_field_invalid_type:{field}')
        elif not _is_raw_number(payload[field]):
            flags.append(f'required_field_invalid_value:{field}')


def _validate_score_fields(
    *,
    payload: dict[str, Any],
    fields: tuple[str, ...],
    flags: list[str],
) -> None:
    _validate_nullable_number_fields(payload=payload, fields=fields, flags=flags)
    for field in fields:
        value = payload.get(field)
        if _is_raw_number(value) and not 1 <= float(value) <= 10:
            flags.append(f'required_field_invalid_value:{field}')


def _validate_string_list_fields(
    *,
    payload: dict[str, Any],
    fields: tuple[str, ...],
    flags: list[str],
) -> None:
    for field in fields:
        if field in payload and not _is_string_list(payload[field]):
            flags.append(f'required_field_invalid_type:{field}')


def _validate_dict_fields(
    *,
    payload: dict[str, Any],
    fields: tuple[str, ...],
    flags: list[str],
) -> None:
    for field in fields:
        if field in payload and not isinstance(payload[field], dict):
            flags.append(f'required_field_invalid_type:{field}')


def _valid_wait_reason(value: Any) -> bool:
    if not isinstance(value, dict):
        return False
    if not all(
        isinstance(value.get(field), str) and bool(value[field].strip())
        for field in ('code', 'summary')
    ):
        return False
    return value['code'].strip() in wait_reason_taxonomy_codes()


def _append_actionable_common_requirements(
    *,
    payload: dict[str, Any],
    numeric_fields: tuple[str, ...],
    evidence_fields: tuple[str, ...],
    holding_period_field: str,
    flags: list[str],
) -> None:
    for field in numeric_fields:
        value = payload.get(field)
        if not _is_raw_number(value) or float(value) <= 0:
            flags.append(f'action_requirement_invalid:{field}')
    for field in ('confidence_score_10', 'conviction_score_10'):
        value = payload.get(field)
        if not _is_raw_number(value) or not 1 <= float(value) <= 10:
            flags.append(f'action_requirement_invalid:{field}')
    holding_period = payload.get(holding_period_field)
    if not isinstance(holding_period, str) or not holding_period.strip():
        flags.append(f'action_requirement_invalid:{holding_period_field}')
    for field in evidence_fields:
        value = payload.get(field)
        if not _is_string_list(value) or not any(item.strip() for item in value):
            flags.append(f'action_requirement_empty:{field}')


def _validate_signal_decider_response_schema(payload: dict[str, Any]) -> list[str]:
    flags = _missing_required_field_flags(payload, SIGNAL_DECIDER_REQUIRED_RESPONSE_KEYS)
    _append_core_fallback_violation(payload, flags)
    flags.extend(_non_finite_number_flags(payload))
    _validate_nonempty_string_fields(
        payload=payload,
        fields=(
            'role',
            'agent_name',
            'symbol',
            'market_date',
            'preferred_action',
            'next_day_open_action',
            'entry_price_basis',
            'thesis_summary',
            'summary',
        ),
        flags=flags,
    )
    _validate_nullable_number_fields(
        payload=payload,
        fields=('target_price', 'expected_return_pct', 'stop_loss_price', 'confidence'),
        flags=flags,
    )
    _validate_score_fields(
        payload=payload,
        fields=('confidence_score_10', 'conviction_score_10'),
        flags=flags,
    )
    _validate_string_list_fields(
        payload=payload,
        fields=(
            'supporting_evidence',
            'decision_risks',
            'invalidation_conditions',
            'data_quality_warnings',
            'specialist_disagreement',
            'risk_flags',
            'improvement_notes',
        ),
        flags=flags,
    )
    _validate_dict_fields(
        payload=payload,
        fields=('decision_delta', 'traceability', 'details'),
        flags=flags,
    )
    if 'suggested_holding_period' in payload:
        holding_period = payload['suggested_holding_period']
        if holding_period is not None and not isinstance(holding_period, str):
            flags.append('required_field_invalid_type:suggested_holding_period')
        elif isinstance(holding_period, str) and not holding_period.strip():
            flags.append('required_field_invalid_value:suggested_holding_period')
    if 'wait_reason' in payload and payload['wait_reason'] is not None and not _valid_wait_reason(payload['wait_reason']):
        flag_kind = 'type' if not isinstance(payload['wait_reason'], dict) else 'value'
        flags.append(f'required_field_invalid_{flag_kind}:wait_reason')
    if 'confidence' in payload and _is_raw_number(payload['confidence']) and not 0 <= float(payload['confidence']) <= 1:
        flags.append('required_field_invalid_value:confidence')

    preferred_action = payload.get('preferred_action')
    next_day_open_action = payload.get('next_day_open_action')
    for field, action in (
        ('preferred_action', preferred_action),
        ('next_day_open_action', next_day_open_action),
    ):
        if isinstance(action, str) and action.strip() and action not in SIGNAL_DECIDER_ACTIONS:
            flags.append(f'required_field_invalid_value:{field}')
    if (
        isinstance(preferred_action, str)
        and isinstance(next_day_open_action, str)
        and preferred_action != next_day_open_action
    ):
        flags.append('action_mismatch')

    action = (
        next_day_open_action
        if isinstance(next_day_open_action, str)
        else None
    )
    if action == 'buy':
        _append_actionable_common_requirements(
            payload=payload,
            numeric_fields=('target_price', 'expected_return_pct', 'stop_loss_price'),
            evidence_fields=('supporting_evidence', 'invalidation_conditions'),
            holding_period_field='suggested_holding_period',
            flags=flags,
        )
        target_price = payload.get('target_price')
        stop_loss_price = payload.get('stop_loss_price')
        if (
            _is_raw_number(target_price)
            and _is_raw_number(stop_loss_price)
            and float(target_price) <= float(stop_loss_price)
        ):
            flags.append('action_requirement_invalid:price_order')
        if payload.get('wait_reason') is not None:
            flags.append('action_requirement_invalid:wait_reason')
    elif action == 'wait':
        if not _valid_wait_reason(payload.get('wait_reason')):
            flags.append('action_requirement_invalid:wait_reason')
    elif action == 'reject' and payload.get('wait_reason') is not None:
        flags.append('action_requirement_invalid:wait_reason')
    return list(dict.fromkeys(flags))


def _validate_short_term_decider_response_schema(payload: dict[str, Any]) -> list[str]:
    flags = _missing_required_field_flags(payload, SHORT_TERM_DECIDER_REQUIRED_RESPONSE_KEYS)
    _append_core_fallback_violation(payload, flags)
    flags.extend(_non_finite_number_flags(payload))
    _validate_nonempty_string_fields(
        payload=payload,
        fields=(
            'role',
            'agent_name',
            'symbol',
            'market_date',
            'tactical_action',
            'preferred_action',
            'thesis_summary',
            'summary',
        ),
        flags=flags,
    )
    _validate_nullable_number_fields(
        payload=payload,
        fields=(
            'trigger_price',
            'chase_limit_price',
            'target_price',
            'stop_loss_price',
            'confidence',
        ),
        flags=flags,
    )
    _validate_score_fields(
        payload=payload,
        fields=('confidence_score_10', 'conviction_score_10'),
        flags=flags,
    )
    _validate_string_list_fields(
        payload=payload,
        fields=(
            'trigger_conditions',
            'supporting_evidence',
            'tactical_risks',
            'invalidation_conditions',
            'data_quality_warnings',
            'risk_flags',
            'improvement_notes',
        ),
        flags=flags,
    )
    _validate_dict_fields(
        payload=payload,
        fields=('decision_delta', 'traceability', 'details'),
        flags=flags,
    )
    if 'expected_holding_period' in payload:
        holding_period = payload['expected_holding_period']
        if holding_period is not None and not isinstance(holding_period, str):
            flags.append('required_field_invalid_type:expected_holding_period')
        elif isinstance(holding_period, str) and not holding_period.strip():
            flags.append('required_field_invalid_value:expected_holding_period')
    if 'wait_reason' in payload and payload['wait_reason'] is not None and not _valid_wait_reason(payload['wait_reason']):
        flag_kind = 'type' if not isinstance(payload['wait_reason'], dict) else 'value'
        flags.append(f'required_field_invalid_{flag_kind}:wait_reason')
    if 'confidence' in payload and _is_raw_number(payload['confidence']) and not 0 <= float(payload['confidence']) <= 1:
        flags.append('required_field_invalid_value:confidence')

    tactical_action = payload.get('tactical_action')
    preferred_action = payload.get('preferred_action')
    for field, action in (
        ('tactical_action', tactical_action),
        ('preferred_action', preferred_action),
    ):
        if isinstance(action, str) and action.strip() and action not in SHORT_TERM_DECIDER_ACTIONS:
            flags.append(f'required_field_invalid_value:{field}')
    if (
        isinstance(tactical_action, str)
        and isinstance(preferred_action, str)
        and tactical_action != preferred_action
    ):
        flags.append('action_mismatch')

    action = tactical_action if isinstance(tactical_action, str) else None
    if action in {'scalp_buy_candidate', 'watch_for_trigger'}:
        _append_actionable_common_requirements(
            payload=payload,
            numeric_fields=('trigger_price', 'chase_limit_price', 'target_price', 'stop_loss_price'),
            evidence_fields=('trigger_conditions', 'supporting_evidence', 'invalidation_conditions'),
            holding_period_field='expected_holding_period',
            flags=flags,
        )
        trigger_price = payload.get('trigger_price')
        chase_limit_price = payload.get('chase_limit_price')
        target_price = payload.get('target_price')
        stop_loss_price = payload.get('stop_loss_price')
        if all(
            _is_raw_number(value)
            for value in (trigger_price, chase_limit_price, target_price, stop_loss_price)
        ) and not (
            float(stop_loss_price)
            < float(trigger_price)
            <= float(chase_limit_price)
            < float(target_price)
        ):
            flags.append('action_requirement_invalid:price_order')
        if action == 'watch_for_trigger':
            if not _valid_wait_reason(payload.get('wait_reason')):
                flags.append('action_requirement_invalid:wait_reason')
        elif payload.get('wait_reason') is not None:
            flags.append('action_requirement_invalid:wait_reason')
    elif action in {'avoid_chase', 'wait'}:
        if not _valid_wait_reason(payload.get('wait_reason')):
            flags.append('action_requirement_invalid:wait_reason')
    elif action == 'reject' and payload.get('wait_reason') is not None:
        flags.append('action_requirement_invalid:wait_reason')
    return list(dict.fromkeys(flags))


def _normalize_signal_decider_action(value: Any) -> str:
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in SIGNAL_DECIDER_ACTIONS:
            return normalized
    return 'wait'


def _normalize_short_term_decider_action(value: Any) -> str:
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in SHORT_TERM_DECIDER_ACTIONS:
            return normalized
    return 'wait'


def _build_signal_decider_details(
    *,
    parsed_details: dict[str, Any],
    decision_delta: dict[str, Any],
    next_day_open_action: str,
    wait_reason: dict[str, Any] | None,
    entry_price_basis: str,
    target_price: float | None,
    expected_return_pct: float | None,
    stop_loss_price: float | None,
    suggested_holding_period: str | None,
    confidence_score_10: float | None,
    conviction_score_10: float | None,
    supporting_evidence: list[str],
    decision_risks: list[str],
    invalidation_conditions: list[str],
    data_quality_warnings: list[str],
    specialist_disagreement: list[str],
    traceability: dict[str, Any],
) -> dict[str, Any]:
    return {
        **parsed_details,
        'decision_support': {
            'next_day_open_action': next_day_open_action,
            'wait_reason': wait_reason,
            'decision_delta': decision_delta,
            'entry_price_basis': entry_price_basis,
            'target_price': target_price,
            'expected_return_pct': expected_return_pct,
            'stop_loss_price': stop_loss_price,
            'suggested_holding_period': suggested_holding_period,
            'confidence_score_10': confidence_score_10,
            'conviction_score_10': conviction_score_10,
            'supporting_evidence': supporting_evidence,
            'decision_risks': decision_risks,
            'invalidation_conditions': invalidation_conditions,
            'data_quality_warnings': data_quality_warnings,
            'specialist_disagreement': specialist_disagreement,
            'decision_support_only': True,
        },
        'traceability': traceability,
    }


def _normalize_signal_decider_response(
    *,
    role: str,
    parsed: dict[str, Any] | None,
    output_text: str | None,
    market_date: str | date | None,
    symbol: str | None,
    agent_name: str | None,
    json_failure_class: str | None,
) -> dict[str, Any]:
    payload = _select_feedback_payload(parsed, symbol=symbol) or {}
    response_schema_violation_flags = [
        *_validate_signal_decider_response_schema(payload),
        *_response_envelope_violation_flags(parsed),
    ]
    if parsed is None:
        response_schema_violation_flags = list(
            dict.fromkeys(
                [
                    'unstructured_response',
                    *(['json_duplicate_key'] if json_failure_class == 'json_duplicate_key' else []),
                    *response_schema_violation_flags,
                ]
            )
        )
    parsed_details = payload.get('details') if isinstance(payload.get('details'), dict) else {}
    next_day_open_action = _normalize_signal_decider_action(
        payload.get('next_day_open_action') or payload.get('preferred_action')
    )
    entry_price_basis = str(payload.get('entry_price_basis') or 'next_open').strip() or 'next_open'
    target_price = _normalize_number(payload.get('target_price'))
    expected_return_pct = _normalize_number(payload.get('expected_return_pct'))
    stop_loss_price = _normalize_number(payload.get('stop_loss_price'))
    suggested_holding_period = str(payload.get('suggested_holding_period')).strip() if payload.get('suggested_holding_period') is not None else None
    confidence_score_10 = _normalize_score_10(payload.get('confidence_score_10'))
    conviction_score_10 = _normalize_score_10(payload.get('conviction_score_10'))
    thesis_summary = str(payload.get('thesis_summary') or payload.get('summary') or output_text or '').strip()
    supporting_evidence = _normalize_text_list(payload.get('supporting_evidence'))
    decision_risks = _normalize_text_list(payload.get('decision_risks'))
    invalidation_conditions = _normalize_text_list(payload.get('invalidation_conditions'))
    data_quality_warnings = _normalize_text_list(payload.get('data_quality_warnings'))
    specialist_disagreement = _normalize_text_list(payload.get('specialist_disagreement'))
    improvement_notes = _normalize_text_list(payload.get('improvement_notes'))
    risk_flags = _normalize_text_list(payload.get('risk_flags'))
    wait_reason, wait_reason_issues = derive_wait_reason(
        role=role,
        action=next_day_open_action,
        payload=payload,
    )
    if wait_reason_issues:
        risk_flags = list(dict.fromkeys([*risk_flags, *wait_reason_issues]))
        if WAIT_REASON_ISSUE_MISSING in wait_reason_issues or WAIT_REASON_ISSUE_GENERIC in wait_reason_issues:
            improvement_notes = list(
                dict.fromkeys(
                    [
                        *improvement_notes,
                        'Wait decisions must include a concrete wait_reason code and blocker summary.',
                    ]
                )
            )
    raw_reported_role = payload.get('role')
    raw_reported_agent_name = payload.get('agent_name')
    raw_reported_symbol = payload.get('symbol')
    raw_reported_market_date = payload.get('market_date')
    reported_role = str(raw_reported_role).strip() or None if raw_reported_role is not None else None
    reported_agent_name = (
        str(raw_reported_agent_name).strip() or None
        if raw_reported_agent_name is not None
        else None
    )
    reported_symbol = (
        str(raw_reported_symbol).strip() or None
        if raw_reported_symbol is not None
        else None
    )
    reported_market_date = _normalize_iso_date(raw_reported_market_date)

    assigned_symbol = str(symbol).strip() or None if symbol is not None else None
    assigned_market_date = _normalize_iso_date(market_date)
    normalized_symbol = assigned_symbol or reported_symbol
    normalized_market_date = assigned_market_date or reported_market_date
    resolved_agent_name = (agent_name or role).strip() or role

    traceability_present = isinstance(payload.get('traceability'), dict)
    traceability = payload.get('traceability') if traceability_present else {}
    raw_reported_traceability_symbol = traceability.get('symbol')
    reported_traceability_symbol = (
        str(raw_reported_traceability_symbol).strip() or None
        if raw_reported_traceability_symbol is not None
        else None
    )
    if parsed is None:
        risk_flags = list(dict.fromkeys([*risk_flags, 'unstructured_response']))
        if not data_quality_warnings:
            data_quality_warnings = ['unstructured_response']
        improvement_notes = list(
            dict.fromkeys([*improvement_notes, 'Return the full structured decision-support contract in JSON.'])
        )
        if output_text:
            traceability = {**traceability, 'raw_output_text': output_text}
    if reported_role is None:
        risk_flags = list(dict.fromkeys([*risk_flags, 'role_missing']))
    elif raw_reported_role != role:
        traceability = {**traceability, 'reported_role': raw_reported_role}
        risk_flags = list(dict.fromkeys([*risk_flags, 'role_mismatch']))
    if reported_agent_name is None:
        risk_flags = list(dict.fromkeys([*risk_flags, 'agent_name_missing']))
    elif raw_reported_agent_name != resolved_agent_name:
        traceability = {**traceability, 'reported_agent_name': raw_reported_agent_name}
        risk_flags = list(dict.fromkeys([*risk_flags, 'agent_name_mismatch']))
    if reported_symbol is None:
        risk_flags = list(dict.fromkeys([*risk_flags, 'symbol_missing']))
    if reported_symbol and assigned_symbol and raw_reported_symbol != assigned_symbol:
        traceability = {**traceability, 'reported_symbol': raw_reported_symbol}
        risk_flags = list(dict.fromkeys([*risk_flags, 'symbol_mismatch']))
    if reported_market_date is None:
        risk_flags = list(dict.fromkeys([*risk_flags, 'market_date_missing']))
    elif assigned_market_date and raw_reported_market_date != assigned_market_date:
        traceability = {**traceability, 'reported_market_date': raw_reported_market_date}
        risk_flags = list(dict.fromkeys([*risk_flags, 'market_date_mismatch']))
    if not traceability_present:
        risk_flags = list(dict.fromkeys([*risk_flags, 'traceability_missing']))
    if reported_traceability_symbol is None:
        risk_flags = list(dict.fromkeys([*risk_flags, 'traceability_symbol_missing']))
    elif (
        reported_traceability_symbol
        and normalized_symbol
        and raw_reported_traceability_symbol != normalized_symbol
    ):
        traceability = {
            **traceability,
            'reported_traceability_symbol': raw_reported_traceability_symbol,
        }
        risk_flags = list(dict.fromkeys([*risk_flags, 'traceability_symbol_mismatch']))

    structural_mismatch_flags = [
        flag for flag in STRUCTURAL_IDENTITY_MISMATCH_FLAGS if flag in risk_flags
    ]
    contract_violation_flags = [
        *structural_mismatch_flags,
        *response_schema_violation_flags,
    ]
    if contract_violation_flags:
        next_day_open_action = 'reject'
        wait_reason = None
        improvement_notes = list(
            dict.fromkeys(
                [
                    *improvement_notes,
                    'Discard this response and retry with the complete canonical contract and exact assigned identity.',
                ]
            )
        )

    traceability = {
        **traceability,
        'agent_name': resolved_agent_name,
        'symbol': normalized_symbol,
        'market_date': normalized_market_date,
    }
    risk_flags = list(
        dict.fromkeys(
            [*risk_flags, *data_quality_warnings, *response_schema_violation_flags]
        )
    )

    confidence = float(payload['confidence']) if _is_raw_number(payload.get('confidence')) else None
    if confidence is not None and confidence > 1 and confidence <= 10:
        if confidence_score_10 is None:
            confidence_score_10 = float(confidence)
        confidence = round(float(confidence) / 10, 4)
    if confidence is None and confidence_score_10 is not None:
        confidence = round(confidence_score_10 / 10, 4)

    decision_delta_payload = payload
    if contract_violation_flags:
        decision_delta_payload = {
            **payload,
            'preferred_action': 'reject',
            'next_day_open_action': 'reject',
            'target_price': target_price,
            'expected_return_pct': expected_return_pct,
            'stop_loss_price': stop_loss_price,
            'confidence_score_10': confidence_score_10,
            'conviction_score_10': conviction_score_10,
            'confidence': confidence,
        }
    decision_delta = build_decision_delta(
        role=role,
        payload=decision_delta_payload,
        market_date=normalized_market_date,
        symbol=normalized_symbol,
        previous_decision_context=payload.get('previous_decision_context'),
    )

    normalized = {
        'role': role,
        'agent_name': resolved_agent_name,
        'symbol': normalized_symbol,
        'market_date': normalized_market_date,
        'preferred_action': next_day_open_action,
        'next_day_open_action': next_day_open_action,
        'wait_reason': wait_reason,
        'entry_price_basis': entry_price_basis,
        'target_price': target_price,
        'expected_return_pct': expected_return_pct,
        'stop_loss_price': stop_loss_price,
        'suggested_holding_period': suggested_holding_period,
        'confidence_score_10': confidence_score_10,
        'conviction_score_10': conviction_score_10,
        'thesis_summary': thesis_summary,
        'supporting_evidence': supporting_evidence,
        'decision_risks': decision_risks,
        'invalidation_conditions': invalidation_conditions,
        'data_quality_warnings': data_quality_warnings,
        'specialist_disagreement': specialist_disagreement,
        'decision_delta': decision_delta,
        'traceability': traceability,
        'summary': thesis_summary,
        'confidence': confidence,
        'risk_flags': risk_flags,
        'response_contract_valid': not contract_violation_flags,
        'structural_mismatch_flags': structural_mismatch_flags,
        'response_schema_violation_flags': response_schema_violation_flags,
        'details': _build_signal_decider_details(
            parsed_details=parsed_details,
            decision_delta=decision_delta,
            next_day_open_action=next_day_open_action,
            wait_reason=wait_reason,
            entry_price_basis=entry_price_basis,
            target_price=target_price,
            expected_return_pct=expected_return_pct,
            stop_loss_price=stop_loss_price,
            suggested_holding_period=suggested_holding_period,
            confidence_score_10=confidence_score_10,
            conviction_score_10=conviction_score_10,
            supporting_evidence=supporting_evidence,
            decision_risks=decision_risks,
            invalidation_conditions=invalidation_conditions,
            data_quality_warnings=data_quality_warnings,
            specialist_disagreement=specialist_disagreement,
            traceability=traceability,
        ),
        'improvement_notes': improvement_notes,
    }
    return _sanitize_non_finite_numbers(normalized)


def _build_short_term_decider_details(
    *,
    parsed_details: dict[str, Any],
    decision_delta: dict[str, Any],
    tactical_action: str,
    wait_reason: dict[str, Any] | None,
    trigger_price: float | None,
    chase_limit_price: float | None,
    target_price: float | None,
    stop_loss_price: float | None,
    expected_holding_period: str | None,
    confidence_score_10: float | None,
    conviction_score_10: float | None,
    trigger_conditions: list[str],
    supporting_evidence: list[str],
    tactical_risks: list[str],
    invalidation_conditions: list[str],
    data_quality_warnings: list[str],
    traceability: dict[str, Any],
) -> dict[str, Any]:
    return {
        **parsed_details,
        'tactical_decision_support': {
            'tactical_action': tactical_action,
            'wait_reason': wait_reason,
            'decision_delta': decision_delta,
            'trigger_price': trigger_price,
            'chase_limit_price': chase_limit_price,
            'target_price': target_price,
            'stop_loss_price': stop_loss_price,
            'expected_holding_period': expected_holding_period,
            'confidence_score_10': confidence_score_10,
            'conviction_score_10': conviction_score_10,
            'trigger_conditions': trigger_conditions,
            'supporting_evidence': supporting_evidence,
            'tactical_risks': tactical_risks,
            'invalidation_conditions': invalidation_conditions,
            'data_quality_warnings': data_quality_warnings,
            'decision_support_only': True,
        },
        'traceability': traceability,
    }


def _normalize_short_term_decider_response(
    *,
    role: str,
    parsed: dict[str, Any] | None,
    output_text: str | None,
    market_date: str | date | None,
    symbol: str | None,
    agent_name: str | None,
    json_failure_class: str | None,
) -> dict[str, Any]:
    payload = _select_feedback_payload(parsed, symbol=symbol) or {}
    response_schema_violation_flags = [
        *_validate_short_term_decider_response_schema(payload),
        *_response_envelope_violation_flags(parsed),
    ]
    if parsed is None:
        response_schema_violation_flags = list(
            dict.fromkeys(
                [
                    'unstructured_response',
                    *(['json_duplicate_key'] if json_failure_class == 'json_duplicate_key' else []),
                    *response_schema_violation_flags,
                ]
            )
        )
    parsed_details = payload.get('details') if isinstance(payload.get('details'), dict) else {}
    tactical_action = _normalize_short_term_decider_action(payload.get('tactical_action') or payload.get('preferred_action'))
    trigger_price = _normalize_number(payload.get('trigger_price'))
    chase_limit_price = _normalize_number(payload.get('chase_limit_price'))
    target_price = _normalize_number(payload.get('target_price'))
    stop_loss_price = _normalize_number(payload.get('stop_loss_price'))
    expected_holding_period = str(payload.get('expected_holding_period')).strip() if payload.get('expected_holding_period') is not None else None
    confidence_score_10 = _normalize_score_10(payload.get('confidence_score_10'))
    conviction_score_10 = _normalize_score_10(payload.get('conviction_score_10'))
    thesis_summary = str(payload.get('thesis_summary') or payload.get('summary') or output_text or '').strip()
    trigger_conditions = _normalize_text_list(payload.get('trigger_conditions'))
    supporting_evidence = _normalize_text_list(payload.get('supporting_evidence'))
    tactical_risks = _normalize_text_list(payload.get('tactical_risks') or payload.get('decision_risks'))
    invalidation_conditions = _normalize_text_list(payload.get('invalidation_conditions'))
    data_quality_warnings = _normalize_text_list(payload.get('data_quality_warnings'))
    improvement_notes = _normalize_text_list(payload.get('improvement_notes'))
    risk_flags = _normalize_text_list(payload.get('risk_flags'))
    wait_reason, wait_reason_issues = derive_wait_reason(
        role=role,
        action=tactical_action,
        payload=payload,
    )
    if wait_reason_issues:
        risk_flags = list(dict.fromkeys([*risk_flags, *wait_reason_issues]))
        if WAIT_REASON_ISSUE_MISSING in wait_reason_issues or WAIT_REASON_ISSUE_GENERIC in wait_reason_issues:
            improvement_notes = list(
                dict.fromkeys(
                    [
                        *improvement_notes,
                        'Wait-like tactical actions must include a concrete wait_reason code and blocker summary.',
                    ]
                )
            )
    raw_reported_role = payload.get('role')
    raw_reported_agent_name = payload.get('agent_name')
    raw_reported_symbol = payload.get('symbol')
    raw_reported_market_date = payload.get('market_date')
    reported_role = str(raw_reported_role).strip() or None if raw_reported_role is not None else None
    reported_agent_name = (
        str(raw_reported_agent_name).strip() or None
        if raw_reported_agent_name is not None
        else None
    )
    reported_symbol = (
        str(raw_reported_symbol).strip() or None
        if raw_reported_symbol is not None
        else None
    )
    reported_market_date = _normalize_iso_date(raw_reported_market_date)

    assigned_symbol = str(symbol).strip() or None if symbol is not None else None
    assigned_market_date = _normalize_iso_date(market_date)
    normalized_symbol = assigned_symbol or reported_symbol
    normalized_market_date = assigned_market_date or reported_market_date
    resolved_agent_name = (agent_name or role).strip() or role

    traceability_present = isinstance(payload.get('traceability'), dict)
    traceability = payload.get('traceability') if traceability_present else {}
    raw_reported_traceability_symbol = traceability.get('symbol')
    reported_traceability_symbol = (
        str(raw_reported_traceability_symbol).strip() or None
        if raw_reported_traceability_symbol is not None
        else None
    )
    if parsed is None:
        risk_flags = list(dict.fromkeys([*risk_flags, 'unstructured_response']))
        if not data_quality_warnings:
            data_quality_warnings = ['unstructured_response']
        improvement_notes = list(
            dict.fromkeys([*improvement_notes, 'Return the full structured short-term decision-support contract in JSON.'])
        )
        if output_text:
            traceability = {**traceability, 'raw_output_text': output_text}
    if reported_role is None:
        risk_flags = list(dict.fromkeys([*risk_flags, 'role_missing']))
    elif raw_reported_role != role:
        traceability = {**traceability, 'reported_role': raw_reported_role}
        risk_flags = list(dict.fromkeys([*risk_flags, 'role_mismatch']))
    if reported_agent_name is None:
        risk_flags = list(dict.fromkeys([*risk_flags, 'agent_name_missing']))
    elif raw_reported_agent_name != resolved_agent_name:
        traceability = {**traceability, 'reported_agent_name': raw_reported_agent_name}
        risk_flags = list(dict.fromkeys([*risk_flags, 'agent_name_mismatch']))
    if reported_symbol is None:
        risk_flags = list(dict.fromkeys([*risk_flags, 'symbol_missing']))
    if reported_symbol and assigned_symbol and raw_reported_symbol != assigned_symbol:
        traceability = {**traceability, 'reported_symbol': raw_reported_symbol}
        risk_flags = list(dict.fromkeys([*risk_flags, 'symbol_mismatch']))
    if reported_market_date is None:
        risk_flags = list(dict.fromkeys([*risk_flags, 'market_date_missing']))
    elif assigned_market_date and raw_reported_market_date != assigned_market_date:
        traceability = {**traceability, 'reported_market_date': raw_reported_market_date}
        risk_flags = list(dict.fromkeys([*risk_flags, 'market_date_mismatch']))
    if not traceability_present:
        risk_flags = list(dict.fromkeys([*risk_flags, 'traceability_missing']))
    if reported_traceability_symbol is None:
        risk_flags = list(dict.fromkeys([*risk_flags, 'traceability_symbol_missing']))
    elif (
        reported_traceability_symbol
        and normalized_symbol
        and raw_reported_traceability_symbol != normalized_symbol
    ):
        traceability = {
            **traceability,
            'reported_traceability_symbol': raw_reported_traceability_symbol,
        }
        risk_flags = list(dict.fromkeys([*risk_flags, 'traceability_symbol_mismatch']))

    structural_mismatch_flags = [
        flag for flag in STRUCTURAL_IDENTITY_MISMATCH_FLAGS if flag in risk_flags
    ]
    contract_violation_flags = [
        *structural_mismatch_flags,
        *response_schema_violation_flags,
    ]
    if contract_violation_flags:
        tactical_action = 'reject'
        wait_reason = None
        improvement_notes = list(
            dict.fromkeys(
                [
                    *improvement_notes,
                    'Discard this response and retry with the complete canonical contract and exact assigned identity.',
                ]
            )
        )

    traceability = {
        **traceability,
        'agent_name': resolved_agent_name,
        'symbol': normalized_symbol,
        'market_date': normalized_market_date,
    }
    risk_flags = list(
        dict.fromkeys(
            [*risk_flags, *data_quality_warnings, *response_schema_violation_flags]
        )
    )

    confidence = float(payload['confidence']) if _is_raw_number(payload.get('confidence')) else None
    if confidence is not None and confidence > 1 and confidence <= 10:
        if confidence_score_10 is None:
            confidence_score_10 = float(confidence)
        confidence = round(float(confidence) / 10, 4)
    if confidence is None and confidence_score_10 is not None:
        confidence = round(confidence_score_10 / 10, 4)

    decision_delta_payload = payload
    if contract_violation_flags:
        decision_delta_payload = {
            **payload,
            'preferred_action': 'reject',
            'tactical_action': 'reject',
            'trigger_price': trigger_price,
            'chase_limit_price': chase_limit_price,
            'target_price': target_price,
            'stop_loss_price': stop_loss_price,
            'confidence_score_10': confidence_score_10,
            'conviction_score_10': conviction_score_10,
            'confidence': confidence,
        }
    decision_delta = build_decision_delta(
        role=role,
        payload=decision_delta_payload,
        market_date=normalized_market_date,
        symbol=normalized_symbol,
        previous_decision_context=payload.get('previous_decision_context'),
    )

    return _sanitize_non_finite_numbers({
        'role': role,
        'agent_name': resolved_agent_name,
        'symbol': normalized_symbol,
        'market_date': normalized_market_date,
        'tactical_action': tactical_action,
        'preferred_action': tactical_action,
        'wait_reason': wait_reason,
        'trigger_price': trigger_price,
        'chase_limit_price': chase_limit_price,
        'target_price': target_price,
        'stop_loss_price': stop_loss_price,
        'expected_holding_period': expected_holding_period,
        'confidence_score_10': confidence_score_10,
        'conviction_score_10': conviction_score_10,
        'thesis_summary': thesis_summary,
        'trigger_conditions': trigger_conditions,
        'supporting_evidence': supporting_evidence,
        'tactical_risks': tactical_risks,
        'invalidation_conditions': invalidation_conditions,
        'data_quality_warnings': data_quality_warnings,
        'decision_delta': decision_delta,
        'traceability': traceability,
        'summary': thesis_summary,
        'confidence': confidence,
        'risk_flags': risk_flags,
        'response_contract_valid': not contract_violation_flags,
        'structural_mismatch_flags': structural_mismatch_flags,
        'response_schema_violation_flags': response_schema_violation_flags,
        'details': _build_short_term_decider_details(
            parsed_details=parsed_details,
            decision_delta=decision_delta,
            tactical_action=tactical_action,
            wait_reason=wait_reason,
            trigger_price=trigger_price,
            chase_limit_price=chase_limit_price,
            target_price=target_price,
            stop_loss_price=stop_loss_price,
            expected_holding_period=expected_holding_period,
            confidence_score_10=confidence_score_10,
            conviction_score_10=conviction_score_10,
            trigger_conditions=trigger_conditions,
            supporting_evidence=supporting_evidence,
            tactical_risks=tactical_risks,
            invalidation_conditions=invalidation_conditions,
            data_quality_warnings=data_quality_warnings,
            traceability=traceability,
        ),
        'improvement_notes': improvement_notes,
    })


def build_contract_prompt_suffix(*, role: str | None = None) -> str:
    wait_reason_codes = ', '.join(wait_reason_taxonomy_codes())
    if role == SHORT_TERM_DECIDER_AGENT_NAME:
        return (
            'Return JSON only. Do not wrap in markdown. '
            'For every short-term-decider feedback object, required keys are: role, agent_name, symbol, market_date, tactical_action, preferred_action, '
            'wait_reason, trigger_price, chase_limit_price, target_price, stop_loss_price, expected_holding_period, confidence_score_10, conviction_score_10, '
            'thesis_summary, trigger_conditions, supporting_evidence, tactical_risks, invalidation_conditions, data_quality_warnings, decision_delta, traceability, '
            'summary, confidence, risk_flags, details, improvement_notes. '
            'Operator-facing free-text fields must be written in Korean while keeping the outer response as raw JSON only. '
            'Do not translate or localize machine-readable enum fields such as tactical_action or preferred_action. '
            'role is mandatory and must equal the requested role. '
            'agent_name is mandatory and must equal short-term-decider. '
            'market_date is mandatory and must exactly equal the assigned market date. '
            'symbol is mandatory and must exactly equal the assigned/requested symbol string. '
            'Do not omit, alias, translate, normalize away leading zeroes, reorder, infer, or substitute symbols. '
            'traceability must be an object and traceability.symbol is mandatory. '
            'traceability.symbol must exactly match symbol, and both must match the assigned item symbol. '
            'tactical_action and preferred_action must be one of scalp_buy_candidate, watch_for_trigger, avoid_chase, wait, reject exactly as written. '
            'Use scalp_buy_candidate only when momentum, participation, liquidity, and trigger/risk levels are all supportable; otherwise prefer watch_for_trigger, avoid_chase, wait, or reject. '
            'Prioritize symbols with clear market-relative leadership, sufficient raw trading value, constructive relative volume, and a pullback/reclaim/breakout-continuation setup; do not recommend short-term buys merely because a symbol looks cheap or has fallen a lot. '
            'For scalp_buy_candidate or watch_for_trigger, target_price versus trigger_price must offer materially more upside than trigger_price versus stop_loss_price offers downside; weak reward/risk should become avoid_chase, wait, or reject. '
            f'wait_reason must be an object with code and summary when tactical_action/preferred_action is watch_for_trigger, avoid_chase, or wait; otherwise use null. Valid wait_reason.code values are: {wait_reason_codes}. '
            'wait_reason.summary must be a concise Korean explanation that names the concrete blocker. '
            'Do not use vague wait text such as insufficient information, need more data, or wait for more confirmation unless the same summary explicitly names the concrete blocker. '
            'decision_delta must be an object, not prose, and should summarize prior-vs-current change metadata when previous_decision_context is available in the request. '
            'decision_delta should include previous_market_date, previous_recommendation, previous_recommendation_class, previous_wait_reason, current_recommendation, current_recommendation_class, current_wait_reason, transition, persistence_trend, changed_fields, stable_fields, what_changed, what_stayed_same, and upgrade_trigger. '
            'When a wait-like recommendation repeats, do not copy-paste the earlier explanation. Summarize what changed, what stayed the same, whether the setup is persistent or improving or worsening, and the concrete trigger that upgrades the setup to scalp_buy_candidate or watch_for_trigger. '
            'trigger_price, chase_limit_price, target_price, and stop_loss_price must be included for every item and should be explicit conservative numeric levels whenever the inputs support them; use null rather than fake precision when unsupported and explain why in data_quality_warnings. '
            'expected_holding_period should be 1-3 trading days when the setup and evidence support a short-term position. '
            'confidence_score_10 and conviction_score_10 must be numbers from 1 to 10 when provided. '
            'thesis_summary must be a concise short-term tactical decision-support explanation in Korean. '
            'trigger_conditions, supporting_evidence, tactical_risks, invalidation_conditions, data_quality_warnings, risk_flags, and improvement_notes must be arrays of short Korean strings. '
            'details must be an object with structured tactical decision-support fields and traceability. '
            'Treat core execution/liquidity inputs such as liquidity, quote freshness, spread/slippage, and execution context as the main blocker domain. '
            'When supplied, use decision_input_quality_context.final_judgment and execution_feasibility as the authoritative buy-quality guardrail. '
            'When supplied, use market_regime_context as the authoritative market/sector posture guardrail: risk_off defaults to wait or reject unless exceptional relative/sector leadership is explicit, caution requires stronger evidence and reduced-size language, and weak sector context requires leadership or confirmation before fresh exposure. '
            'Treat news/disclosure and detailed quarterly/cash-flow context as auxiliary unless the tactic explicitly depends on them. '
            'Empty recent news means no captured recent news and is neutral unless other risk evidence exists. '
            'Do not forecast price reaction from news alone; news can support context only when source evidence and materiality are explicit. '
            'Do not summarize headline-only, paywalled, or fetch-failed news as if the full article text was read. '
            'Missing detailed quarterly/cash-flow fields should usually reduce confidence only. '
            'Do not classify a symbol as untradeable solely because auxiliary news or financial detail is absent. '
            'Optional news/disclosure or quarterly/cash-flow gaps must not become the primary wait_reason when price, liquidity, trend, or execution blockers already explain the conservative action. '
            'If core execution evidence is missing, stale, weak, conflicting, or chase risk is elevated, still return a complete structured conservative feedback object with avoid_chase, wait, or reject, null unsupported numeric fields, and explicit risk/data-quality warnings. '
            'If only auxiliary context is absent, a structured scalp_buy_candidate or watch_for_trigger decision may still be valid when price, liquidity, trigger, and risk evidence are otherwise strong. '
            'Before finalizing, self-check that the JSON is parseable, all required keys are present, role and agent_name are correct, symbol and traceability.symbol exactly match the assigned symbol, operator-facing free-text fields are in Korean, and arrays are arrays of strings. '
            'Immediately before sending, validate the final raw JSON text again and correct any extra or missing closing braces/brackets so it begins with {, ends with }, and parses successfully.'
        )
    if role == SIGNAL_DECIDER_AGENT_NAME:
        return (
            'Return JSON only. Do not wrap in markdown. '
            'For every signal-decider feedback object, required keys are: role, agent_name, symbol, market_date, preferred_action, next_day_open_action, '
            'wait_reason, entry_price_basis, target_price, expected_return_pct, stop_loss_price, suggested_holding_period, '
            'confidence_score_10, conviction_score_10, thesis_summary, supporting_evidence, decision_risks, '
            'decision_delta, '
            'invalidation_conditions, data_quality_warnings, specialist_disagreement, traceability, summary, confidence, '
            'risk_flags, details, improvement_notes. '
            'Operator-facing free-text fields must be written in Korean while keeping the outer response as raw JSON only. '
            'Do not translate or localize machine-readable enum fields such as preferred_action, next_day_open_action, or entry_price_basis. '
            'role is mandatory and must equal the requested role. '
            'agent_name is mandatory and must equal signal-decider. '
            'market_date is mandatory and must exactly equal the assigned market date. '
            'symbol is mandatory and must exactly equal the assigned/requested symbol string. '
            'Do not omit, alias, translate, normalize away leading zeroes, reorder, infer, or substitute symbols. '
            'traceability must be an object and traceability.symbol is mandatory. '
            'traceability.symbol must exactly match symbol, and both must match the assigned item symbol. '
            'next_day_open_action and preferred_action must be one of buy, wait, reject exactly as written. '
            f'wait_reason must be an object with code and summary when preferred_action/next_day_open_action is wait; otherwise use null. Valid wait_reason.code values are: {wait_reason_codes}. '
            'wait_reason.summary must be a concise Korean explanation that names the concrete blocker. '
            'Do not use vague wait text such as insufficient information, need more data, or wait for more confirmation unless the same summary explicitly names the concrete blocker. '
            'decision_delta must be an object, not prose, and should summarize prior-vs-current change metadata when previous_decision_context is available in the request. '
            'decision_delta should include previous_market_date, previous_recommendation, previous_recommendation_class, previous_wait_reason, current_recommendation, current_recommendation_class, current_wait_reason, transition, persistence_trend, changed_fields, stable_fields, what_changed, what_stayed_same, and upgrade_trigger. '
            'When wait repeats, do not copy-paste the earlier explanation. Summarize what changed, what stayed the same, whether the setup is persistent or improving or worsening, and the concrete trigger that would upgrade the setup to buy. '
            'entry_price_basis should be next_open unless the supplied inputs justify a different explicit basis. '
            'target_price and stop_loss_price must be included for every item and should be explicit conservative numeric levels whenever the inputs support them; if a concrete level is genuinely not supportable, keep the keys and explain the limitation in data_quality_warnings instead of omitting them. '
            'Use null rather than fake precision when a numeric field is not well-supported by the inputs. '
            'confidence_score_10 and conviction_score_10 must be numbers from 1 to 10 when provided. '
            'thesis_summary must be a concise decision-support explanation in Korean. '
            'supporting_evidence, decision_risks, invalidation_conditions, data_quality_warnings, specialist_disagreement, '
            'risk_flags, and improvement_notes must be arrays of short Korean strings. '
            'details must be an object with structured decision-support fields and traceability. '
            'Treat core execution/liquidity inputs such as liquidity, quote freshness, spread/slippage, and execution context as the main blocker domain. '
            'When supplied, use decision_input_quality_context.final_judgment and execution_feasibility as the authoritative buy-quality guardrail. '
            'When supplied, use market_regime_context as the authoritative market/sector posture guardrail: risk_off defaults to wait or reject unless exceptional relative/sector leadership is explicit, caution requires stronger evidence and reduced-size language, and weak sector context requires leadership or confirmation before fresh exposure. '
            'Treat news/disclosure and detailed quarterly/cash-flow context as auxiliary unless the thesis explicitly depends on them. '
            'Empty recent news means no captured recent news and is neutral unless other risk evidence exists. '
            'Do not forecast price reaction from news alone; news can support context only when source evidence and materiality are explicit. '
            'Do not summarize headline-only, paywalled, or fetch-failed news as if the full article text was read. '
            'Missing detailed quarterly/cash-flow fields should usually reduce confidence only. '
            'Do not classify a symbol as untradeable solely because auxiliary news or financial detail is absent. '
            'Optional news/disclosure or quarterly/cash-flow gaps must not become the primary wait_reason when price, liquidity, trend, execution timing, or volatility blockers already explain the wait. '
            'If core execution evidence is missing, weak, conflicting, or truncated, still return a complete structured conservative feedback object with wait or reject, null unsupported numeric fields, and explicit risk/data-quality warnings. '
            'If only auxiliary context is absent, a buy decision may still be valid when price, liquidity, trend, execution risk, and invalidation evidence are otherwise strong. '
            'Before finalizing, self-check that the JSON is parseable, all required keys are present, role and agent_name are correct, symbol and traceability.symbol exactly match the assigned symbol, operator-facing free-text fields are in Korean, and arrays are arrays of strings. '
            'Immediately before sending, validate the final raw JSON text again and correct any extra or missing closing braces/brackets so it begins with {, ends with }, and parses successfully.'
        )
    if role == 'fundamental-summarizer':
        return (
            'Return JSON only. Do not wrap in markdown. '
            'Required top-level keys: role, summary, confidence, risk_flags, details, improvement_notes. '
            'role must equal the requested role. '
            'Use the supplied market and source metadata to interpret fundamentals from either KRX DART filings/XBRL or US SEC EDGAR filings/company facts. '
            'Do not assume every payload is a DART payload, and do not reinterpret SEC fields as DART-specific fields. '
            'summary must be a concise string. confidence must be a number between 0 and 1. '
            'risk_flags must be an array of strings. '
            'details must be an object with structured supporting fields relevant to the role. '
            'improvement_notes must be an array of short strings describing missing inputs, ambiguities, desired fields, or algorithm/data improvements that would make the analysis better.'
        )
    return (
        'Return JSON only. Do not wrap in markdown. '
        'Required top-level keys: role, summary, confidence, risk_flags, details, improvement_notes. '
        'role must equal the requested role. '
        'summary must be a concise string. '
        'confidence must be a number between 0 and 1. '
        'risk_flags must be an array of strings. '
        'details must be an object with structured supporting fields relevant to the role. '
        'improvement_notes must be an array of short strings describing missing inputs, ambiguities, desired fields, or algorithm/data improvements that would make the analysis better.'
    )


def build_agent_contract_provenance(*, role: str) -> dict[str, Any]:
    contract_text = build_contract_prompt_suffix(role=role)
    allowed_actions: list[str] = []
    identity_fields = ['role']
    if role == SIGNAL_DECIDER_AGENT_NAME:
        allowed_actions = ['buy', 'wait', 'reject']
        identity_fields = ['role', 'agent_name', 'symbol', 'market_date', 'traceability.symbol']
    elif role == SHORT_TERM_DECIDER_AGENT_NAME:
        allowed_actions = ['scalp_buy_candidate', 'watch_for_trigger', 'avoid_chase', 'wait', 'reject']
        identity_fields = ['role', 'agent_name', 'symbol', 'market_date', 'traceability.symbol']
    return {
        'contract_version': AGENT_ROLE_CONTRACT_VERSION,
        'role': role,
        'contract_sha256': f"sha256:{hashlib.sha256(contract_text.encode('utf-8')).hexdigest()}",
        'allowed_actions': allowed_actions,
        'identity_fields': identity_fields,
    }


def normalize_agent_response(
    *,
    role: str,
    output_text: str | None,
    raw_response: dict[str, Any],
    market_date: str | date | None = None,
    symbol: str | None = None,
    agent_name: str | None = None,
) -> dict[str, Any]:
    parsed, json_failure_class = decode_strict_agent_json_object(output_text)

    if role == SHORT_TERM_DECIDER_AGENT_NAME:
        return _normalize_short_term_decider_response(
            role=role,
            parsed=parsed,
            output_text=output_text,
            market_date=market_date,
            symbol=symbol,
            agent_name=agent_name,
            json_failure_class=json_failure_class,
        )

    if role == SIGNAL_DECIDER_AGENT_NAME:
        return _normalize_signal_decider_response(
            role=role,
            parsed=parsed,
            output_text=output_text,
            market_date=market_date,
            symbol=symbol,
            agent_name=agent_name,
            json_failure_class=json_failure_class,
        )

    parsed_is_structured = parsed is not None
    if parsed is None:
        parsed = {
            'role': role,
            'summary': output_text or '',
            'confidence': None,
            'risk_flags': ['unstructured_response'],
            'details': {
                'raw_output_text': output_text,
            },
            'improvement_notes': ['Return structured JSON with explicit missing-data and improvement notes.'],
        }

    raw_reported_role = parsed.get('role')
    reported_role = (
        str(raw_reported_role).strip() or None
        if raw_reported_role is not None
        else None
    )
    response_schema_violation_flags = (
        [
            *_validate_generic_response_schema(parsed),
            *_response_envelope_violation_flags(parsed),
        ]
        if parsed_is_structured
        else [
            'unstructured_response',
            *(['json_duplicate_key'] if json_failure_class == 'json_duplicate_key' else []),
        ]
    )

    normalized = {
        'role': reported_role or role,
        'symbol': (str(symbol).strip() if symbol is not None else None),
        'market_date': _normalize_iso_date(market_date),
        'summary': parsed.get('summary') if isinstance(parsed.get('summary'), str) else str(parsed.get('summary') or ''),
        'confidence': float(parsed['confidence']) if _is_raw_number(parsed.get('confidence')) else None,
        'risk_flags': parsed.get('risk_flags') if isinstance(parsed.get('risk_flags'), list) else [],
        'details': parsed.get('details') if isinstance(parsed.get('details'), dict) else {},
        'improvement_notes': parsed.get('improvement_notes') if isinstance(parsed.get('improvement_notes'), list) else [],
    }
    if reported_role is None:
        normalized['risk_flags'] = list(dict.fromkeys([*normalized['risk_flags'], 'role_missing']))
    elif raw_reported_role != role:
        normalized['details'] = {**normalized['details'], 'reported_role': raw_reported_role}
        normalized['role'] = role
        normalized['risk_flags'] = list(dict.fromkeys([*normalized['risk_flags'], 'role_mismatch']))
    reported_symbol = str(parsed.get('symbol')).strip() if parsed.get('symbol') is not None else None
    if reported_symbol and normalized['symbol'] and reported_symbol != normalized['symbol']:
        normalized['details'] = {**normalized['details'], 'reported_symbol': reported_symbol}
        normalized['risk_flags'] = list(dict.fromkeys([*normalized['risk_flags'], 'symbol_mismatch']))
    reported_market_date = _normalize_iso_date(parsed.get('market_date'))
    if reported_market_date and normalized['market_date'] and reported_market_date != normalized['market_date']:
        normalized['details'] = {
            **normalized['details'],
            'reported_market_date': reported_market_date,
        }
        normalized['risk_flags'] = list(
            dict.fromkeys([*normalized['risk_flags'], 'market_date_mismatch'])
        )
    structural_mismatch_flags = [
        flag
        for flag in STRUCTURAL_IDENTITY_MISMATCH_FLAGS
        if flag in normalized['risk_flags']
    ]
    normalized['risk_flags'] = list(
        dict.fromkeys([*normalized['risk_flags'], *response_schema_violation_flags])
    )
    normalized['response_contract_valid'] = not (
        structural_mismatch_flags or response_schema_violation_flags
    )
    normalized['structural_mismatch_flags'] = structural_mismatch_flags
    normalized['response_schema_violation_flags'] = response_schema_violation_flags
    return _sanitize_non_finite_numbers(normalized)
