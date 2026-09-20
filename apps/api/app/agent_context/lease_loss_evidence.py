from __future__ import annotations

from datetime import date, datetime, timezone
from typing import Any, Mapping

from app.agent_context.roles import DECISION_SUPPORT_AGENT_NAMES
from app.agent_context.run_modes import (
    DAILY_RETRY_CYCLE_MODE,
    FEEDBACK_SCOPE_MODE_DECIDER_SCOPE_FAST_PATH,
)


RETRY_ADMISSION_SCHEMA_VERSION = 'agent_feedback_retry_admission_v1'
RETRY_ADMISSION_LEASE_LOSS_SCHEMA_VERSION = (
    'agent_feedback_retry_admission_lease_loss_v1'
)


def _field(value: object, name: str) -> Any:
    if isinstance(value, Mapping):
        return value.get(name)
    return getattr(value, name, None)


def _utc_datetime(value: Any) -> datetime | None:
    if value is None:
        return None
    parsed = value
    if isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(
                value.strip().replace('Z', '+00:00')
            )
        except ValueError:
            return None
    if not isinstance(parsed, datetime):
        return None
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _integer(value: Any) -> int | None:
    if isinstance(value, bool):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def terminal_feedback_lease_loss_evidence_error(
    *,
    child_run: object | None,
    child_run_id: str,
    market_date: date,
    metadata: Mapping[str, Any],
    knowledge_cutoff_at: datetime,
) -> str | None:
    """Validate the abandoned feedback parent proving a lease-loss event."""

    if (
        child_run is None
        or str(_field(child_run, 'run_id') or '').strip() != child_run_id
    ):
        return 'child_run'
    if _field(child_run, 'market_date') != market_date:
        return 'market_date'
    if str(_field(child_run, 'status') or '').strip().lower() != 'abandoned':
        return 'status'
    completed_at = _utc_datetime(_field(child_run, 'completed_at'))
    if completed_at is None:
        return 'completed_at'
    symbols_total = _integer(_field(child_run, 'symbols_total'))
    completed_count = _integer(_field(child_run, 'completed_count'))
    failed_count = _integer(_field(child_run, 'failed_count'))
    if not (
        symbols_total is not None
        and completed_count is not None
        and failed_count is not None
        and symbols_total > 0
        and completed_count >= 0
        and failed_count > 0
        and completed_count + failed_count == symbols_total
    ):
        return 'counts'

    request_payload = _field(child_run, 'request_payload')
    if not isinstance(request_payload, Mapping):
        return 'request_context'
    raw_symbols = metadata.get('symbols')
    if not (
        isinstance(raw_symbols, list)
        and raw_symbols
        and all(
            isinstance(symbol, str) and symbol.strip()
            for symbol in raw_symbols
        )
        and len(raw_symbols) == len(set(raw_symbols))
    ):
        return 'request_context'
    expected_roles = list(DECISION_SUPPORT_AGENT_NAMES)
    role_symbol_scopes = request_payload.get('role_symbol_scopes')
    role_symbol_counts = request_payload.get('role_symbol_counts')
    if not (
        request_payload.get('market_date') == market_date.isoformat()
        and request_payload.get('request_run_id') == child_run_id
        and request_payload.get('mode') == DAILY_RETRY_CYCLE_MODE
        and request_payload.get('roles') == expected_roles
        and request_payload.get('requested_symbols') == raw_symbols
        and request_payload.get('feedback_scope_mode')
        == FEEDBACK_SCOPE_MODE_DECIDER_SCOPE_FAST_PATH
        and request_payload.get('admission_scope_by_role')
        == {role: list(raw_symbols) for role in expected_roles}
        and str(request_payload.get('fs_div') or '').strip().upper()
        == str(metadata.get('fs_div') or '').strip().upper()
        and request_payload.get('reuse_existing')
        is metadata.get('reuse_existing_feedback')
        and request_payload.get('force_recompute')
        is bool(
            metadata.get('force')
            and not metadata.get('reuse_existing_feedback')
        )
        and _utc_datetime(request_payload.get('knowledge_cutoff_at'))
        == knowledge_cutoff_at
        and _utc_datetime(
            request_payload.get('admission_knowledge_cutoff_at')
        )
        == knowledge_cutoff_at
        and _utc_datetime(
            request_payload.get('requested_knowledge_cutoff_at')
        )
        == knowledge_cutoff_at
        and isinstance(role_symbol_scopes, Mapping)
        and isinstance(role_symbol_counts, Mapping)
        and set(role_symbol_scopes) == set(expected_roles)
        and set(role_symbol_counts) == set(expected_roles)
    ):
        return 'request_context'
    requested_symbol_set = set(raw_symbols)
    if not all(
        isinstance(role_symbol_scopes.get(role), list)
        and len(role_symbol_scopes[role])
        == len(set(role_symbol_scopes[role]))
        and all(
            symbol in requested_symbol_set
            for symbol in role_symbol_scopes[role]
        )
        and type(role_symbol_counts.get(role)) is int
        and role_symbol_counts[role] == len(role_symbol_scopes[role])
        for role in expected_roles
    ):
        return 'role_scope'
    if sum(role_symbol_counts.values()) != symbols_total:
        return 'role_scope_count'

    retry_admission = request_payload.get('retry_admission')
    if not isinstance(retry_admission, Mapping):
        return 'retry_admission'
    owner_token_hash = str(
        retry_admission.get('owner_token_hash') or ''
    ).strip().lower()
    if not (
        retry_admission.get('schema_version')
        == RETRY_ADMISSION_SCHEMA_VERSION
        and _utc_datetime(retry_admission.get('claimed_at')) is not None
        and 'owner_token' not in retry_admission
        and len(owner_token_hash) == 64
        and all(
            character in '0123456789abcdef'
            for character in owner_token_hash
        )
    ):
        return 'retry_admission'

    summary_payload = _field(child_run, 'summary_payload')
    if not isinstance(summary_payload, Mapping):
        return 'admission_lease_loss'
    lease_loss = summary_payload.get('admission_lease_loss')
    if not isinstance(lease_loss, Mapping):
        return 'admission_lease_loss'
    detected_at = _utc_datetime(lease_loss.get('detected_at'))
    owner_fingerprint = str(
        lease_loss.get('owner_token_fingerprint') or ''
    ).strip().lower()
    if not (
        lease_loss.get('schema_version')
        == RETRY_ADMISSION_LEASE_LOSS_SCHEMA_VERSION
        and str(lease_loss.get('reason') or '').strip()
        == 'heartbeat_refresh_grace_exceeded'
        and str(lease_loss.get('stage') or '').strip()
        and detected_at is not None
        and detected_at == completed_at
        and len(owner_fingerprint) == 16
        and owner_fingerprint == owner_token_hash[:16]
    ):
        return 'admission_lease_loss'
    return None
