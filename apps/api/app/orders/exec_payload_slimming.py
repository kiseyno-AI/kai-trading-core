from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
import copy
import json
import math
from typing import Any, Sequence

from sqlalchemy import String, cast, func, select

from app.db import session_scope
from app.db_models import SignalDeciderExecutionPlanItem

SIGNAL_DECIDER_EXEC_SUBMISSION_MAX_BYTES = 24 * 1024
SIGNAL_DECIDER_EXEC_CLEANUP_MIN_BYTES = 64 * 1024
DEFAULT_CLEANUP_BATCH_SIZE = 100
_MAX_SUMMARY_ITEMS = 8
_MAX_TEXT_CHARS = 512
_MAX_LIST_ITEMS = 10
_MAX_DICT_KEYS = 40

_BULKY_STORAGE_KEYS = {
    'candidate_arrays',
    'candidate_payload',
    'candidates',
    'context',
    'decider_handoff_context',
    'decider_input_context',
    'input_feature_snapshot_payload',
    'items',
    'last_request_payload',
    'last_response_payload',
    'raw_context',
    'raw_decider_input_context',
    'raw_request_payload',
    'request_payload',
    'risk_context',
    'symbol_day_context',
}


@dataclass(frozen=True)
class SubmissionPayloadSlimmingResult:
    payload: dict[str, Any] | None
    changed: bool
    original_size_bytes: int
    slimmed_size_bytes: int
    truncated: bool


def _json_safe(value: Any) -> Any:
    if isinstance(value, Decimal):
        return format(value, 'f') if value.is_finite() else None
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_json_safe(item) for item in value]
    if isinstance(value, tuple):
        return [_json_safe(item) for item in value]
    return value


def json_size_bytes(value: Any) -> int:
    return len(json.dumps(_json_safe(value), ensure_ascii=False, sort_keys=True, separators=(',', ':')).encode('utf-8'))


def _normalize_text(value: Any, *, lowercase: bool = False) -> str | None:
    if value is None:
        return None
    normalized = str(value).strip()
    if not normalized:
        return None
    return normalized.lower() if lowercase else normalized


def _truncate_text(value: Any, *, max_chars: int = _MAX_TEXT_CHARS) -> Any:
    value = _json_safe(value)
    if not isinstance(value, str) or len(value) <= max_chars:
        return value
    return {
        'preview': value[:max_chars],
        'truncated': True,
        'original_length': len(value),
    }


def summarize_error(error: Any) -> dict[str, Any] | None:
    if not isinstance(error, dict):
        text = _normalize_text(error)
        return {'message': _truncate_text(text)} if text else None
    summary: dict[str, Any] = {}
    for key in ('code', 'status', 'status_code', 'retryable', 'message', 'detail', 'reason'):
        if key in error and error.get(key) is not None:
            summary[key] = _truncate_text(error.get(key))
    for key in ('exec_trace_id', 'request_id', 'trace_id'):
        if key in error and error.get(key) is not None:
            summary[key] = _truncate_text(error.get(key), max_chars=128)
    return summary or None


def _compact_quantity_policy(policy: Any) -> dict[str, Any] | None:
    if not isinstance(policy, dict):
        return None
    compact: dict[str, Any] = {}
    for key in (
        'type',
        'quantity',
        'budget_krw',
        'allocation_mode',
        'estimated_reference_price',
        'estimated_notional_krw',
        'limit_price',
        'trigger_price',
        'max_safe_notional_krw',
        'liquidity_tier',
        'pricing_note',
        'linked_entry_item_key',
    ):
        if key in policy and policy.get(key) is not None:
            compact[key] = _truncate_text(policy.get(key), max_chars=256)
    sizing_hints = policy.get('sizing_hints')
    if isinstance(sizing_hints, list) and sizing_hints:
        compact['sizing_hints'] = [_truncate_text(item, max_chars=160) for item in sizing_hints[:3]]
        if len(sizing_hints) > 3:
            compact['sizing_hints_omitted_count'] = len(sizing_hints) - 3
    return compact or None


def _compact_risk_context_summary(risk_context: Any) -> dict[str, Any] | None:
    if not isinstance(risk_context, dict):
        return None
    compact: dict[str, Any] = {}
    for key in (
        'classification',
        'agent_name',
        'decision_agent_name',
        'core_plan_item_id',
        'core_client_order_id',
        'account_reference',
        'account_label',
        'target_trade_date',
        'source_market_date',
        'policy_version',
        'decision_snapshot_id',
        'source_feedback_id',
        'decision_run_id',
        'target_price',
        'stop_loss_price',
        'allocation_budget_krw',
        'expected_return_pct',
        'confidence_score_10',
        'conviction_score_10',
        'entry_execution_mode',
        'tactical_action',
        'original_watch_trigger_price',
        'original_watch_chase_limit_price',
        'delegated_gate_confirmation_required',
    ):
        if key in risk_context and risk_context.get(key) is not None:
            compact[key] = _truncate_text(risk_context.get(key), max_chars=256)
    for list_key in ('data_quality_warnings', 'decision_risks', 'risk_flags'):
        values = risk_context.get(list_key)
        if isinstance(values, list) and values:
            compact[list_key] = [_truncate_text(value, max_chars=160) for value in values[:3]]
            if len(values) > 3:
                compact[f'{list_key}_omitted_count'] = len(values) - 3
    delegated_gate_codes = risk_context.get('delegated_gate_codes')
    if isinstance(delegated_gate_codes, list):
        compact['delegated_gate_codes'] = [
            _truncate_text(code, max_chars=160)
            for code in delegated_gate_codes[:10]
        ]
    delegated_gates = risk_context.get('delegated_gates')
    if isinstance(delegated_gates, list):
        compact['delegated_gates'] = [
            {
                key: _json_safe(gate.get(key))
                for key in (
                    'code',
                    'observed_value',
                    'field_path',
                    'comparator',
                    'threshold',
                    'policy_version',
                    'observed_at',
                    'recheck_source',
                    'expires_at',
                )
                if key in gate
            }
            for gate in delegated_gates
            if isinstance(gate, dict)
        ]
    execution_feasibility = risk_context.get('execution_feasibility_context')
    if isinstance(execution_feasibility, dict):
        compact['execution_feasibility_context'] = {
            key: _truncate_text(execution_feasibility.get(key), max_chars=160)
            for key in (
                'expected_slippage_bps',
                'expected_slippage_basis',
                'expected_slippage_status',
                'expected_slippage_reason',
                'liquidity_tier',
                'max_safe_notional_krw',
            )
            if key in execution_feasibility
        }
    decision_quality = risk_context.get('decision_input_quality_context')
    if isinstance(decision_quality, dict):
        compact['decision_input_quality_summary'] = _compact_decision_quality_summary(decision_quality)
    market_regime = risk_context.get('market_regime_context')
    if isinstance(market_regime, dict):
        compact['market_regime_context'] = {
            key: _truncate_text(market_regime.get(key), max_chars=160)
            for key in (
                'schema_version',
                'posture',
                'reason_code',
                'recommended_exec_action',
                'recommended_sizing_multiplier',
            )
            if market_regime.get(key) is not None
        }
    return compact or None


def _compact_decision_quality_summary(payload: dict[str, Any]) -> dict[str, Any]:
    summary: dict[str, Any] = {}
    for key in ('schema_version', 'overall_status', 'final_judgment', 'confidence_adjustment', 'paid_api_dependency'):
        if key in payload and payload.get(key) is not None:
            summary[key] = _truncate_text(payload.get(key), max_chars=160)
    execution_feasibility = payload.get('execution_feasibility')
    if isinstance(execution_feasibility, dict):
        summary['execution_feasibility'] = {
            key: _truncate_text(execution_feasibility.get(key), max_chars=160)
            for key in (
                'status',
                'score',
                'liquidity_tier',
                'max_safe_notional_krw',
                'expected_slippage_bps',
                'expected_slippage_basis',
                'expected_slippage_status',
                'expected_slippage_reason',
            )
            if key in execution_feasibility
        }
    return summary


def compact_exec_plan_item(item: Any) -> dict[str, Any] | None:
    if not isinstance(item, dict):
        return None
    compact: dict[str, Any] = {}
    for key in (
        'core_item_key',
        'agent_name',
        'decision_agent_name',
        'symbol',
        'side',
        'intent',
        'limit_price',
        'trigger_price',
        'time_in_force',
        'decision_snapshot_id',
    ):
        if key in item and item.get(key) is not None:
            compact[key] = _truncate_text(item.get(key), max_chars=256)
    quantity_policy = _compact_quantity_policy(item.get('quantity_policy'))
    if quantity_policy:
        compact['quantity_policy'] = quantity_policy
    risk_summary = _compact_risk_context_summary(item.get('risk_context'))
    if risk_summary:
        compact['risk_context_summary'] = risk_summary
    return compact or None


def compact_exec_response_item(item: Any) -> dict[str, Any] | None:
    if not isinstance(item, dict):
        return None
    compact: dict[str, Any] = {}
    for key in (
        'core_item_key',
        'client_key',
        'external_item_key',
        'execution_plan_id',
        'execution_plan_item_id',
        'plan_item_id',
        'id',
        'status',
        'plan_item_status',
        'decision_status',
        'symbol',
        'side',
        'intent',
        'quantity',
        'filled_quantity',
        'remaining_quantity',
        'limit_price',
        'trigger_price',
        'price',
        'avg_price',
        'broker_order_id',
        'order_id',
        'client_order_id',
        'exec_trace_id',
        'trace_id',
        'request_id',
        'reason',
        'reason_code',
        'message',
        'exec_agent_action',
        'agent_action',
        'approval_action',
        'decision_action',
        'risk_decision',
        'monitoring_status',
        'monitoring_state',
        'watch_status',
        'target_profile',
        'target_tag',
        'special_tactical_scalp',
    ):
        if key in item and item.get(key) is not None:
            compact[key] = _truncate_text(item.get(key), max_chars=256)
    error = summarize_error(item.get('error'))
    if error:
        compact['error'] = error
    agent_sizing = item.get('agent_sizing')
    if isinstance(agent_sizing, dict):
        compact['agent_sizing'] = {
            key: _truncate_text(agent_sizing.get(key), max_chars=256)
            for key in (
                'approved',
                'quantity',
                'price',
                'limit_price',
                'notional_krw',
                'approved_notional',
                'approved_notional_krw',
                'action',
                'reason_code',
                'reason',
                'rejection_reason',
                'risk_decision',
                'target_profile',
                'target_tag',
                'special_tactical_scalp',
                'exec_trace_id',
            )
            if agent_sizing.get(key) is not None
        }
    return compact or None


def compact_exec_item_link(link: Any) -> dict[str, Any] | None:
    if not isinstance(link, dict):
        return None
    compact: dict[str, Any] = {}
    identity_keys = {
        'core_item_key',
        'exec_plan_id',
        'exec_plan_item_id',
    }
    for key in (
        'core_item_key',
        'intent',
        'side',
        'symbol',
        'trigger_price',
        'limit_price',
        'exec_plan_id',
        'exec_plan_item_id',
        'remote_status',
    ):
        if key in link and link.get(key) is not None:
            compact[key] = (
                _json_safe(link.get(key))
                if key in identity_keys
                else _truncate_text(link.get(key), max_chars=256)
            )
    response_item = compact_exec_response_item(link.get('response_item'))
    if response_item:
        compact['response_item'] = response_item
    return compact or None


def _items_by_core_key(payload: dict[str, Any]) -> dict[str, dict[str, Any]]:
    items = payload.get('items') if isinstance(payload.get('items'), list) else []
    keyed: dict[str, dict[str, Any]] = {}
    for item in items:
        if not isinstance(item, dict):
            continue
        key = _normalize_text(item.get('core_item_key'))
        if key:
            keyed[key] = item
    return keyed


def compact_exec_request_summary(
    payload: Any,
    *,
    item_links: Sequence[dict[str, Any]] | None = None,
) -> dict[str, Any] | None:
    if not isinstance(payload, dict):
        return None
    items = payload.get('items') if isinstance(payload.get('items'), list) else []
    keyed_items = _items_by_core_key(payload)
    requested_keys = [
        _normalize_text(link.get('core_item_key'))
        for link in (item_links or [])
        if isinstance(link, dict) and _normalize_text(link.get('core_item_key'))
    ]
    selected_items = [keyed_items[key] for key in requested_keys if key in keyed_items]
    if not selected_items and len(items) == 1 and isinstance(items[0], dict):
        selected_items = [items[0]]

    summary: dict[str, Any] = {
        'schema_version': 'exec_dispatch_request_summary_v1',
        'plan_id': payload.get('plan_id'),
        'source': payload.get('source'),
        'mode': payload.get('mode'),
        'market_date': payload.get('market_date'),
        'strategy_run_id': payload.get('strategy_run_id'),
        'requested_at': payload.get('requested_at'),
        'total_item_count': len(items),
        'row_item_count': len(selected_items),
        'intent_counts': dict(
            sorted(
                Counter(
                    _normalize_text(item.get('intent'), lowercase=True) or 'unknown'
                    for item in items
                    if isinstance(item, dict)
                ).items()
            )
        ),
        'symbols': sorted({str(item.get('symbol')) for item in selected_items if isinstance(item, dict) and item.get('symbol')}),
        'row_items': [compact for item in selected_items[:_MAX_SUMMARY_ITEMS] if (compact := compact_exec_plan_item(item))],
        'omitted_payloads': {
            'full_items_array': True,
            'request_bodies': True,
            'raw_decider_inputs': True,
            'nested_handoff_contexts': True,
        },
    }
    metadata = payload.get('metadata')
    if isinstance(metadata, dict):
        summary['metadata'] = {
            key: _truncate_text(metadata.get(key), max_chars=256)
            for key in (
                'schema_version',
                'agent_name',
                'decision_agent_name',
                'trigger',
                'requested_at',
                'account_reference',
                'account_label',
                'source_market_date',
                'target_trade_date',
                'dispatch_material_fingerprint',
            )
            if metadata.get(key) is not None
        }
    if len(selected_items) > _MAX_SUMMARY_ITEMS:
        summary['row_items_omitted_count'] = len(selected_items) - _MAX_SUMMARY_ITEMS
    return enforce_json_size(summary, max_bytes=SIGNAL_DECIDER_EXEC_SUBMISSION_MAX_BYTES // 2, label='exec_request_summary')


def compact_exec_response_summary(
    payload: Any,
    *,
    item_links: Sequence[dict[str, Any]] | None = None,
) -> dict[str, Any] | None:
    if not isinstance(payload, dict):
        return None
    if isinstance(payload.get('item_acks'), list):
        response_items = payload['item_acks']
    elif isinstance(payload.get('items'), list):
        response_items = payload['items']
    else:
        response_items = []
    response_by_key: dict[str, dict[str, Any]] = {}
    for item in response_items:
        if not isinstance(item, dict):
            continue
        key = _normalize_text(item.get('core_item_key') or item.get('client_key') or item.get('external_item_key'))
        if key:
            response_by_key[key] = item
    requested_keys = [
        _normalize_text(link.get('core_item_key'))
        for link in (item_links or [])
        if isinstance(link, dict) and _normalize_text(link.get('core_item_key'))
    ]
    selected_items = [response_by_key[key] for key in requested_keys if key in response_by_key]
    validation_failures = payload.get('validation_failures') if isinstance(payload.get('validation_failures'), list) else []
    summary = {
        'schema_version': 'exec_dispatch_response_summary_v1',
        'execution_plan_id': _normalize_text(payload.get('execution_plan_id')),
        'accepted_item_count': payload.get('accepted_item_count'),
        'rejected_item_count': payload.get('rejected_item_count'),
        'total_item_count': len(response_items),
        'row_item_count': len(selected_items),
        'validation_failures': [_shallow_slim(value) for value in validation_failures[:3]],
        'row_items': [compact for item in selected_items[:_MAX_SUMMARY_ITEMS] if (compact := compact_exec_response_item(item))],
        'omitted_payloads': {
            'full_response_items_array': True,
            'bulky_nested_contexts': True,
        },
    }
    contract_ack = payload.get('contract_ack')
    if isinstance(contract_ack, dict):
        summary['contract_ack'] = {
            key: _json_safe(contract_ack.get(key))
            for key in (
                'schema_version',
                'contract_version',
                'capability',
                'plan_id',
                'status',
                'dispatch_material_fingerprint',
            )
            if contract_ack.get(key) is not None
        }
    if len(validation_failures) > 3:
        summary['validation_failures_omitted_count'] = len(validation_failures) - 3
    if len(selected_items) > _MAX_SUMMARY_ITEMS:
        summary['row_items_omitted_count'] = len(selected_items) - _MAX_SUMMARY_ITEMS
    return enforce_json_size(summary, max_bytes=SIGNAL_DECIDER_EXEC_SUBMISSION_MAX_BYTES // 2, label='exec_response_summary')


def _compact_remote_item(item: Any, *, kind: str) -> dict[str, Any] | None:
    if not isinstance(item, dict):
        return None
    fields_by_kind = {
        'order': (
            'id',
            'broker_order_pk',
            'broker_order_id',
            'order_id',
            'client_order_id',
            'execution_plan_item_id',
            'plan_item_id',
            'symbol',
            'side',
            'status',
            'current_status',
            'broker_status',
            'quantity',
            'filled_quantity',
            'remaining_quantity',
            'price',
            'limit_price',
            'avg_price',
            'average_fill_price',
            'avg_fill_price',
            'fill_price',
            'executed_quantity',
            'cumulative_filled_quantity',
            'filled_at',
            'last_filled_at',
            'submitted_at',
            'created_at',
            'updated_at',
            'reason',
            'message',
        ),
        'execution': (
            'id',
            'execution_fill_id',
            'execution_id',
            'broker_execution_id',
            'broker_order_id',
            'order_id',
            'broker_order_pk',
            'execution_plan_item_id',
            'plan_item_id',
            'exec_plan_item_id',
            'symbol',
            'side',
            'quantity',
            'filled_quantity',
            'price',
            'avg_price',
            'average_fill_price',
            'avg_fill_price',
            'fill_price',
            'executed_at',
            'filled_at',
            'last_filled_at',
            'created_at',
            'updated_at',
        ),
        'position': (
            'id',
            'account_reference',
            'provider',
            'account_scope',
            'exchange',
            'position_scope_proof',
            'symbol',
            'quantity',
            'available_quantity',
            'average_price',
            'avg_price',
            'market_value',
            'updated_at',
        ),
        'runtime': (
            'status',
            'ready',
            'mode',
            'paused',
            'kill_switch_enabled',
            'monitoring_alive',
            'last_heartbeat_at',
            'open_order_count',
            'active_execution_job_count',
        ),
    }
    compact = {
        key: _truncate_text(item.get(key), max_chars=256)
        for key in fields_by_kind.get(kind, ())
        if item.get(key) is not None
    }
    error = summarize_error(item.get('error'))
    if error:
        compact['error'] = error
    return compact or None


def compact_exec_readback(readback: Any) -> dict[str, Any] | None:
    if not isinstance(readback, dict):
        return None
    orders = readback.get('orders') if isinstance(readback.get('orders'), dict) else {}
    executions = readback.get('executions') if isinstance(readback.get('executions'), dict) else {}
    positions = readback.get('positions') if isinstance(readback.get('positions'), dict) else {}
    runtime_payload = readback.get('runtime') if isinstance(readback.get('runtime'), dict) else {}
    compact = {
        'schema_version': 'exec_dispatch_readback_summary_v1',
        'last_synced_at': readback.get('last_synced_at'),
        'trigger': readback.get('trigger'),
        'runtime': {
            'status': runtime_payload.get('status'),
            'payload': _compact_remote_item(runtime_payload.get('payload'), kind='runtime')
            if isinstance(runtime_payload.get('payload'), dict)
            else None,
            'error': summarize_error(runtime_payload.get('error')),
        },
        'execution_plans': {
            'status': (readback.get('execution_plans') or {}).get('status')
            if isinstance(readback.get('execution_plans'), dict)
            else None,
            'error': summarize_error((readback.get('execution_plans') or {}).get('error'))
            if isinstance(readback.get('execution_plans'), dict)
            else None,
        },
        'orders': {
            'status': orders.get('status'),
            'count': orders.get('count'),
            'items': [
                compact_item
                for item in (orders.get('items') or [])[:_MAX_LIST_ITEMS]
                if (compact_item := _compact_remote_item(item, kind='order'))
            ],
            'error': summarize_error(orders.get('error')),
        },
        'executions': {
            'status': executions.get('status'),
            'count': executions.get('count'),
            'items': [
                compact_item
                for item in (executions.get('items') or [])[:_MAX_LIST_ITEMS]
                if (compact_item := _compact_remote_item(item, kind='execution'))
            ],
            'error': summarize_error(executions.get('error')),
        },
        'positions': {
            'status': positions.get('status'),
            'count': positions.get('count'),
            'items': [
                compact_item
                for item in (positions.get('items') or [])[:_MAX_LIST_ITEMS]
                if (compact_item := _compact_remote_item(item, kind='position'))
            ],
            'error': summarize_error(positions.get('error')),
        },
        'derived_state': _shallow_slim(readback.get('derived_state')),
        'omitted_payloads': {
            'full_endpoint_payloads': True,
            'bulky_nested_contexts': True,
        },
    }
    for key in ('orders', 'executions', 'positions'):
        source_items = ((readback.get(key) or {}).get('items') or []) if isinstance(readback.get(key), dict) else []
        if len(source_items) > _MAX_LIST_ITEMS:
            compact[key]['items_omitted_count'] = len(source_items) - _MAX_LIST_ITEMS
    return enforce_json_size(compact, max_bytes=SIGNAL_DECIDER_EXEC_SUBMISSION_MAX_BYTES // 2, label='exec_readback')


def _shallow_slim(value: Any) -> Any:
    value = _json_safe(value)
    if isinstance(value, dict):
        compact: dict[str, Any] = {}
        for index, (key, item) in enumerate(value.items()):
            if index >= _MAX_DICT_KEYS:
                compact['omitted_key_count'] = len(value) - _MAX_DICT_KEYS
                break
            if key in _BULKY_STORAGE_KEYS:
                compact['omitted_bulky_context_count'] = int(compact.get('omitted_bulky_context_count') or 0) + 1
            elif isinstance(item, (dict, list)):
                compact[key] = _truncate_recursive(item, path=str(key), metadata={'truncated_paths': []}, depth=0)
            else:
                compact[key] = _truncate_text(item)
        return compact
    if isinstance(value, list):
        compact_list = [_shallow_slim(item) for item in value[:_MAX_LIST_ITEMS]]
        if len(value) > _MAX_LIST_ITEMS:
            compact_list.append({'omitted_item_count': len(value) - _MAX_LIST_ITEMS})
        return compact_list
    return _truncate_text(value)


def _truncate_recursive(value: Any, *, path: str, metadata: dict[str, Any], depth: int) -> Any:
    value = _json_safe(value)
    if isinstance(value, str):
        if len(value) > _MAX_TEXT_CHARS:
            metadata['truncated_paths'].append(path)
        return _truncate_text(value)
    if isinstance(value, list):
        if len(value) > _MAX_LIST_ITEMS:
            metadata['truncated_paths'].append(path)
        return [
            _truncate_recursive(item, path=f'{path}[{index}]', metadata=metadata, depth=depth + 1)
            for index, item in enumerate(value[:_MAX_LIST_ITEMS])
        ]
    if isinstance(value, dict):
        if depth >= 3:
            metadata['truncated_paths'].append(path)
            return {'omitted': True, 'reason': 'max_depth_exceeded'}
        compact: dict[str, Any] = {}
        for index, (key, item) in enumerate(value.items()):
            child_path = f'{path}.{key}' if path else str(key)
            if index >= _MAX_DICT_KEYS:
                compact['omitted_key_count'] = len(value) - _MAX_DICT_KEYS
                metadata['truncated_paths'].append(path)
                break
            if key in _BULKY_STORAGE_KEYS:
                compact['omitted_bulky_context_count'] = int(compact.get('omitted_bulky_context_count') or 0) + 1
                metadata['truncated_paths'].append(f'{path}.<bulky_context>' if path else '<bulky_context>')
                continue
            compact[key] = _truncate_recursive(item, path=child_path, metadata=metadata, depth=depth + 1)
        return compact
    return value


def enforce_json_size(payload: dict[str, Any], *, max_bytes: int, label: str) -> dict[str, Any]:
    safe_payload = _json_safe(payload)
    original_size = json_size_bytes(safe_payload)
    if original_size <= max_bytes:
        return safe_payload

    metadata: dict[str, Any] = {
        'truncated': True,
        'label': label,
        'max_size_bytes': max_bytes,
        'original_size_bytes': original_size,
        'truncated_paths': [],
    }
    slimmed = _truncate_recursive(safe_payload, path='', metadata=metadata, depth=0)
    if not isinstance(slimmed, dict):
        slimmed = {'payload': slimmed}
    metadata['final_size_bytes'] = json_size_bytes(slimmed)
    slimmed['storage_slimming'] = metadata
    if json_size_bytes(slimmed) <= max_bytes:
        return slimmed
    fallback = {
        'storage_slimming': {
            **metadata,
            'fallback_applied': True,
            'final_size_bytes': None,
        },
        'summary': 'Payload exceeded per-item storage limit and was reduced to metadata only.',
    }
    fallback['storage_slimming']['final_size_bytes'] = json_size_bytes(fallback)
    return fallback


def slim_signal_decider_exec_submission_payload(
    payload: Any,
    *,
    max_bytes: int = SIGNAL_DECIDER_EXEC_SUBMISSION_MAX_BYTES,
) -> SubmissionPayloadSlimmingResult:
    original_size = json_size_bytes(payload)
    if not isinstance(payload, dict):
        return SubmissionPayloadSlimmingResult(
            payload=None,
            changed=payload is not None,
            original_size_bytes=original_size,
            slimmed_size_bytes=0,
            truncated=False,
        )

    exec_dispatch = payload.get('exec_dispatch') if isinstance(payload.get('exec_dispatch'), dict) else None
    if exec_dispatch is None:
        return SubmissionPayloadSlimmingResult(
            payload=payload,
            changed=False,
            original_size_bytes=original_size,
            slimmed_size_bytes=original_size,
            truncated=False,
        )

    item_links = [
        compact
        for item in (exec_dispatch.get('item_links') or [])
        if (compact := compact_exec_item_link(item))
    ]
    slimmed_exec: dict[str, Any] = {
        'plan_id': exec_dispatch.get('plan_id'),
        'plan_fingerprint': exec_dispatch.get('plan_fingerprint'),
        'dispatch_revision_fingerprint': exec_dispatch.get(
            'dispatch_revision_fingerprint'
        ),
        'dispatch_material_fingerprint': exec_dispatch.get(
            'dispatch_material_fingerprint'
        ),
        'dispatch_batch_key': exec_dispatch.get('dispatch_batch_key'),
        'dispatch_status': exec_dispatch.get('dispatch_status'),
        'prior_execution_status': exec_dispatch.get('prior_execution_status'),
        'prior_eligibility_status': exec_dispatch.get('prior_eligibility_status'),
        'last_attempt_at': exec_dispatch.get('last_attempt_at'),
        'last_success_at': exec_dispatch.get('last_success_at'),
        'last_error': summarize_error(exec_dispatch.get('last_error')),
        'item_links': item_links,
        'attempts': [_shallow_slim(item) for item in (exec_dispatch.get('attempts') or [])[-5:]],
    }
    contract_proof = exec_dispatch.get('contract_proof')
    if isinstance(contract_proof, dict):
        slimmed_exec['contract_proof'] = _shallow_slim(contract_proof)
    destination_binding = exec_dispatch.get('destination_binding')
    if isinstance(destination_binding, dict):
        slimmed_exec['destination_binding'] = _shallow_slim(destination_binding)
    authority_generation_before = exec_dispatch.get('authority_generation_before')
    if isinstance(authority_generation_before, dict):
        slimmed_exec['authority_generation_before'] = _shallow_slim(
            authority_generation_before
        )
    request_summary = exec_dispatch.get('last_request_summary')
    if not isinstance(request_summary, dict):
        request_summary = compact_exec_request_summary(
            exec_dispatch.get('last_request_payload'),
            item_links=item_links,
        )
    if isinstance(request_summary, dict):
        slimmed_exec['last_request_summary'] = request_summary
    response_summary = exec_dispatch.get('last_response_summary')
    if not isinstance(response_summary, dict):
        response_summary = compact_exec_response_summary(
            exec_dispatch.get('last_response_payload'),
            item_links=item_links,
        )
    if isinstance(response_summary, dict):
        slimmed_exec['last_response_summary'] = response_summary
    readback = compact_exec_readback(exec_dispatch.get('readback'))
    if readback:
        slimmed_exec['readback'] = readback
    core_state_sync = exec_dispatch.get('core_state_sync')
    if isinstance(core_state_sync, dict):
        slimmed_exec['core_state_sync'] = _shallow_slim(core_state_sync)

    slimmed_payload = {
        'mode': payload.get('mode') or 'exec_dispatch',
        'latest': _shallow_slim(payload.get('latest')),
        'exec_dispatch': slimmed_exec,
    }
    raw_lifecycle_payload: dict[str, Any] = {}
    for lifecycle_key, expected_type in (
        ('revision_history', list),
        ('revocation', dict),
        ('revocation_history', list),
        ('dispatch_reconciliation', dict),
    ):
        raw_lifecycle_evidence = payload.get(lifecycle_key)
        if isinstance(raw_lifecycle_evidence, expected_type):
            raw_lifecycle_payload[lifecycle_key] = copy.deepcopy(
                _json_safe(
                    raw_lifecycle_evidence[-5:]
                    if isinstance(raw_lifecycle_evidence, list)
                    else raw_lifecycle_evidence
                )
            )
    lifecycle_payload = copy.deepcopy(raw_lifecycle_payload)
    if json_size_bytes(lifecycle_payload) > max_bytes // 2:
        lifecycle_payload = {
            key: _shallow_slim(value)
            for key, value in raw_lifecycle_payload.items()
        }
        raw_revocation = raw_lifecycle_payload.get('revocation')
        compact_revocation = lifecycle_payload.get('revocation')
        if isinstance(raw_revocation, dict) and isinstance(compact_revocation, dict):
            compact_revocation['item_links'] = copy.deepcopy(
                raw_revocation.get('item_links') or []
            )
        for raw_revision, compact_revision in zip(
            raw_lifecycle_payload.get('revision_history') or [],
            lifecycle_payload.get('revision_history') or [],
            strict=True,
        ):
            if not isinstance(raw_revision, dict) or not isinstance(
                compact_revision, dict
            ):
                continue
            raw_dispatch = raw_revision.get('dispatch_evidence')
            compact_dispatch = compact_revision.get('dispatch_evidence')
            if isinstance(raw_dispatch, dict) and isinstance(compact_dispatch, dict):
                compact_dispatch['item_links'] = copy.deepcopy(
                    raw_dispatch.get('item_links') or []
                )
        for raw_record, compact_record in zip(
            raw_lifecycle_payload.get('revocation_history') or [],
            lifecycle_payload.get('revocation_history') or [],
            strict=True,
        ):
            if not isinstance(raw_record, dict) or not isinstance(compact_record, dict):
                continue
            raw_request = raw_record.get('request')
            compact_request = compact_record.get('request')
            if isinstance(raw_request, dict) and isinstance(compact_request, dict):
                compact_request['items'] = copy.deepcopy(raw_request.get('items') or [])
            raw_response = raw_record.get('response')
            compact_response = compact_record.get('response')
            if isinstance(raw_response, dict) and isinstance(compact_response, dict):
                compact_response['item_acks'] = copy.deepcopy(
                    raw_response.get('item_acks') or []
                )
    lifecycle_reserve = json_size_bytes(lifecycle_payload) + 256
    slimmed_payload = enforce_json_size(
        slimmed_payload,
        max_bytes=max(max_bytes - lifecycle_reserve, 1024),
        label='signal_decider_submission_payload',
    )
    slimmed_payload.update(lifecycle_payload)
    slimmed_size = json_size_bytes(slimmed_payload)
    return SubmissionPayloadSlimmingResult(
        payload=slimmed_payload,
        changed=slimmed_payload != payload,
        original_size_bytes=original_size,
        slimmed_size_bytes=slimmed_size,
        truncated=bool(slimmed_payload.get('storage_slimming')),
    )


def cleanup_signal_decider_exec_submission_payloads(
    *,
    dry_run: bool,
    target_trade_date: str | date | None = None,
    min_payload_bytes: int = SIGNAL_DECIDER_EXEC_CLEANUP_MIN_BYTES,
    max_payload_bytes: int = SIGNAL_DECIDER_EXEC_SUBMISSION_MAX_BYTES,
    batch_size: int = DEFAULT_CLEANUP_BATCH_SIZE,
    limit: int | None = None,
) -> dict[str, Any]:
    scanned_count = 0
    would_update_count = 0
    updated_count = 0
    bytes_before = 0
    bytes_after = 0
    sampled_rows: list[dict[str, Any]] = []
    last_seen_id = 0
    parsed_target_date = date.fromisoformat(str(target_trade_date)) if target_trade_date else None

    while True:
        remaining = None if limit is None else max(limit - scanned_count, 0)
        if remaining == 0:
            break
        current_batch_size = min(batch_size, remaining) if remaining is not None else batch_size
        with session_scope() as session:
            size_expr = func.length(cast(SignalDeciderExecutionPlanItem.submission_payload, String))
            stmt = (
                select(
                    SignalDeciderExecutionPlanItem.id,
                    SignalDeciderExecutionPlanItem.target_trade_date,
                    SignalDeciderExecutionPlanItem.account_reference,
                    SignalDeciderExecutionPlanItem.symbol,
                    SignalDeciderExecutionPlanItem.execution_status,
                    SignalDeciderExecutionPlanItem.submission_payload,
                )
                .where(
                    SignalDeciderExecutionPlanItem.id > last_seen_id,
                    SignalDeciderExecutionPlanItem.submission_payload.is_not(None),
                    size_expr >= min_payload_bytes,
                )
                .order_by(SignalDeciderExecutionPlanItem.id.asc())
                .limit(current_batch_size)
            )
            if parsed_target_date is not None:
                stmt = stmt.where(SignalDeciderExecutionPlanItem.target_trade_date == parsed_target_date)
            if not dry_run:
                stmt = stmt.with_for_update()
            rows = session.execute(stmt).all()
            if not rows:
                break
            for row in rows:
                last_seen_id = max(last_seen_id, int(row.id))
                scanned_count += 1
                result = slim_signal_decider_exec_submission_payload(
                    copy.deepcopy(row.submission_payload),
                    max_bytes=max_payload_bytes,
                )
                bytes_before += result.original_size_bytes
                bytes_after += result.slimmed_size_bytes
                if not result.changed:
                    continue
                would_update_count += 1
                sample = {
                    'plan_item_id': row.id,
                    'target_trade_date': row.target_trade_date.isoformat() if row.target_trade_date else None,
                    'account_reference': row.account_reference,
                    'symbol': row.symbol,
                    'execution_status': row.execution_status,
                    'original_size_bytes': result.original_size_bytes,
                    'slimmed_size_bytes': result.slimmed_size_bytes,
                    'truncated': result.truncated,
                }
                if len(sampled_rows) < 20:
                    sampled_rows.append(sample)
                if dry_run:
                    continue
                persisted = session.get(SignalDeciderExecutionPlanItem, row.id)
                if persisted is not None:
                    persisted.submission_payload = result.payload
                    updated_count += 1
            if dry_run:
                session.rollback()

    return {
        'dry_run': dry_run,
        'target_trade_date': parsed_target_date.isoformat() if parsed_target_date else None,
        'min_payload_bytes': min_payload_bytes,
        'max_payload_bytes': max_payload_bytes,
        'batch_size': batch_size,
        'limit': limit,
        'scanned_count': scanned_count,
        'would_update_count': would_update_count,
        'updated_count': updated_count,
        'bytes_before': bytes_before,
        'bytes_after': bytes_after,
        'bytes_saved': max(bytes_before - bytes_after, 0),
        'sampled_rows': sampled_rows,
    }
