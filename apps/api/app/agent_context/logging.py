from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from app.core.audit_logging import sanitize_headers

_MAX_LOG_STRING_LENGTH = 500
_MAX_LOG_COLLECTION_ITEMS = 8
_MAX_LOG_MAPPING_ITEMS = 12
_MAX_LOG_DEPTH = 4
_SENSITIVE_KEYS = {
    'access_token',
    'api_key',
    'appkey',
    'appsecret',
    'authorization',
    'cookie',
    'password',
    'refresh_token',
    'secret',
    'set-cookie',
    'token',
    'upstream_bearer_token',
    'x-api-key',
}


def _is_sensitive_key(key: str | None) -> bool:
    normalized = str(key or '').strip().lower()
    return (
        normalized in _SENSITIVE_KEYS
        or normalized.endswith('_token')
        or normalized.endswith('_secret')
        or normalized.endswith('_password')
    )


def _truncate_string(value: str, *, limit: int = _MAX_LOG_STRING_LENGTH) -> str:
    if len(value) <= limit:
        return value
    return f'{value[: limit - 3]}...'


def build_text_snapshot(value: str | None) -> dict[str, Any] | None:
    if value is None:
        return None
    return {
        'chars': len(value),
        'preview': _truncate_string(value),
    }


def build_request_snapshot(body: dict[str, Any] | None) -> dict[str, Any] | None:
    if not isinstance(body, dict):
        return None
    return {
        'model': body.get('model'),
        'input': build_text_snapshot(body.get('input') if isinstance(body.get('input'), str) else None),
    }


def build_response_snapshot(
    response_payload: dict[str, Any] | None,
    *,
    output_text: str | None = None,
    status_code: int | None = None,
) -> dict[str, Any] | None:
    if not isinstance(response_payload, dict):
        return None
    output_items = response_payload.get('output')
    return {
        'id': response_payload.get('id'),
        'status': response_payload.get('status'),
        'model': response_payload.get('model') or response_payload.get('model_name'),
        'status_code': status_code,
        'output_item_count': len(output_items) if isinstance(output_items, list) else None,
        'output_item_types': [
            str(item.get('type') or type(item).__name__)
            for item in output_items[:_MAX_LOG_COLLECTION_ITEMS]
            if isinstance(item, dict)
        ] if isinstance(output_items, list) else [],
        'output_text': build_text_snapshot(output_text),
    }


def build_normalized_feedback_snapshot(normalized_feedback: dict[str, Any] | None) -> dict[str, Any] | None:
    if not isinstance(normalized_feedback, dict):
        return None
    return sanitize_log_value(
        {
            'role': normalized_feedback.get('role'),
            'agent_name': normalized_feedback.get('agent_name'),
            'symbol': normalized_feedback.get('symbol'),
            'market_date': normalized_feedback.get('market_date'),
            'summary': normalized_feedback.get('summary'),
            'confidence': normalized_feedback.get('confidence'),
            'preferred_action': normalized_feedback.get('preferred_action'),
            'risk_flags': normalized_feedback.get('risk_flags'),
            'traceability': normalized_feedback.get('traceability'),
        }
    )


def make_attempt_id(*, dispatch_id: str | None, attempt: int | None) -> str | None:
    if not dispatch_id or attempt is None:
        return None
    return f'{dispatch_id}:attempt-{attempt}'


def sanitize_log_value(value: Any, *, key: str | None = None, depth: int = 0) -> Any:
    if _is_sensitive_key(key):
        return '[REDACTED]'
    if value is None or isinstance(value, (bool, int, float)):
        return value
    if isinstance(value, str):
        return _truncate_string(value)
    if depth >= _MAX_LOG_DEPTH:
        return _truncate_string(str(value))
    if isinstance(value, Mapping):
        if key and 'headers' in key.lower():
            header_values = {
                str(item_key): str(item_value)
                for item_key, item_value in value.items()
            }
            return sanitize_headers(header_values)
        items = list(value.items())
        sanitized: dict[str, Any] = {}
        for item_key, item_value in items[:_MAX_LOG_MAPPING_ITEMS]:
            sanitized[str(item_key)] = sanitize_log_value(
                item_value,
                key=str(item_key),
                depth=depth + 1,
            )
        if len(items) > _MAX_LOG_MAPPING_ITEMS:
            sanitized['__truncated_items__'] = len(items) - _MAX_LOG_MAPPING_ITEMS
        return sanitized
    if isinstance(value, Sequence) and not isinstance(value, (bytes, bytearray, str)):
        items = list(value)
        sanitized_items = [
            sanitize_log_value(item, depth=depth + 1)
            for item in items[:_MAX_LOG_COLLECTION_ITEMS]
        ]
        if len(items) > _MAX_LOG_COLLECTION_ITEMS:
            sanitized_items.append({'__truncated_items__': len(items) - _MAX_LOG_COLLECTION_ITEMS})
        return sanitized_items
    return _truncate_string(str(value))


def log_agent_feedback_event(
    stage: str,
    *,
    status: str | None = None,
    request_run_id: str | None = None,
    dispatch_id: str | None = None,
    attempt_id: str | None = None,
    market_date: str | None = None,
    role: str | None = None,
    agent_name: str | None = None,
    symbol: str | None = None,
    batch_symbols: Sequence[str] | None = None,
    slot: int | None = None,
    session_key: str | None = None,
    persistence_target: Mapping[str, Any] | None = None,
    persistence_result: Mapping[str, Any] | None = None,
    terminal_status: str | None = None,
    error_type: str | None = None,
    error_message: str | None = None,
    **payload: Any,
) -> None:
    line: dict[str, Any] = {
        'scope': 'agent_feedback_runtime',
        'logged_at': datetime.now(timezone.utc).isoformat(),
        'stage': stage,
        'status': status,
        'request_run_id': request_run_id,
        'dispatch_id': dispatch_id,
        'attempt_id': attempt_id,
        'market_date': market_date,
        'role': role,
        'agent_name': agent_name,
        'symbol': symbol,
        'batch_symbols': list(batch_symbols) if batch_symbols is not None else None,
        'slot': slot,
        'session_key': session_key,
        'persistence_target': sanitize_log_value(dict(persistence_target or {}), key='persistence_target'),
        'persistence_result': sanitize_log_value(dict(persistence_result or {}), key='persistence_result'),
        'terminal_status': terminal_status,
        'error_type': error_type,
        'error_message': _truncate_string(error_message) if error_message else None,
    }
    for key, value in payload.items():
        line[key] = sanitize_log_value(value, key=key)

    debug_path = Path(__file__).resolve().parents[2] / 'var' / 'log' / 'agent-feedback-runtime.log'
    try:
        debug_path.parent.mkdir(parents=True, exist_ok=True)
        with debug_path.open('a', encoding='utf-8') as fh:
            fh.write(json.dumps(line, ensure_ascii=False, default=str) + '\n')
    except OSError:
        pass
