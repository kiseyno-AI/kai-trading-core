from __future__ import annotations

import hashlib
from datetime import date, datetime, timezone
from typing import Any, Mapping, Sequence

from app.agent_context.roles import (
    DECISION_SUPPORT_AGENT_NAMES,
    SUMMARIZER_AGENT_NAMES,
)
from app.agent_context.run_modes import (
    DAILY_RETRY_CYCLE_MODE,
    FEEDBACK_SCOPE_AUTHORITY_COMPLETED_SUMMARY_INTERSECTION,
    FEEDBACK_SCOPE_AUTHORITY_SCHEMA_VERSION,
    FEEDBACK_SCOPE_MODE_BROAD_FEEDBACK_UNIVERSE,
    FEEDBACK_SCOPE_MODE_DECIDER_SCOPE_FAST_PATH,
)
from app.eod.summary_cutoff_contract import (
    COMPLETED_SUMMARIZER_INTERSECTION_PROOF_VERSION,
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


def _canonical_symbols(value: object, *, sorted_order: bool) -> list[str] | None:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
        return None
    if not all(isinstance(symbol, str) for symbol in value):
        return None
    symbols = [str(symbol).strip() for symbol in value]
    if (
        not symbols
        or any(not symbol for symbol in symbols)
        or len(symbols) != len(set(symbols))
    ):
        return None
    if sorted_order and symbols != sorted(symbols):
        return None
    return symbols


def _authoritative_completed_summary_scope(
    *,
    request_payload: Mapping[str, Any],
    parent_symbols: Sequence[str],
    knowledge_cutoff_at: datetime,
    fs_div: str,
    completed_summary_proof: Mapping[str, Any] | None,
    expected_summary_source_run_id: str | None,
) -> tuple[list[str] | None, str | None]:
    """Return the proven decider cohort, preserving legacy absence semantics."""

    if 'scope_authority' not in request_payload:
        if completed_summary_proof is not None:
            proof_scope = completed_summary_proof.get(
                'completed_scope_proof'
            )
            proof_requested_symbols = _canonical_symbols(
                completed_summary_proof.get('requested_symbols'),
                sorted_order=False,
            )
            proof_completed_symbols = (
                _canonical_symbols(
                    proof_scope.get('completed_symbols'),
                    sorted_order=False,
                )
                if isinstance(proof_scope, Mapping)
                else None
            )
            proof_admitted_symbols = (
                _canonical_symbols(
                    proof_scope.get('admitted_symbols'),
                    sorted_order=False,
                )
                if isinstance(proof_scope, Mapping)
                else None
            )
            proof_scope_mode = str(
                completed_summary_proof.get('feedback_scope_mode') or ''
            ).strip()
            if not (
                completed_summary_proof.get('ready') is True
                and isinstance(proof_scope, Mapping)
                and proof_scope.get('schema_version')
                == COMPLETED_SUMMARIZER_INTERSECTION_PROOF_VERSION
                and proof_requested_symbols
                and proof_completed_symbols == proof_requested_symbols
                and proof_admitted_symbols
                and set(proof_requested_symbols).issubset(
                    proof_admitted_symbols
                )
                and proof_scope_mode
                in {
                    FEEDBACK_SCOPE_MODE_BROAD_FEEDBACK_UNIVERSE,
                    FEEDBACK_SCOPE_MODE_DECIDER_SCOPE_FAST_PATH,
                }
                and len(proof_requested_symbols) == len(parent_symbols)
                and set(proof_requested_symbols) == set(parent_symbols)
                and len(proof_admitted_symbols) == len(parent_symbols)
                and set(proof_admitted_symbols) == set(parent_symbols)
                and type(
                    completed_summary_proof.get(
                        'requested_symbol_count'
                    )
                )
                is int
                and completed_summary_proof.get(
                    'requested_symbol_count'
                )
                == len(proof_requested_symbols)
                and _utc_datetime(
                    completed_summary_proof.get('knowledge_cutoff_at')
                )
                == knowledge_cutoff_at
                and (
                    expected_summary_source_run_id is None
                    or str(
                        completed_summary_proof.get('source_run_id') or ''
                    ).strip()
                    == expected_summary_source_run_id
                )
            ):
                return None, 'scope_authority'
        return list(parent_symbols), None

    authority = request_payload.get('scope_authority')
    if not isinstance(authority, Mapping):
        return None, 'scope_authority'
    completed_symbols = _canonical_symbols(
        authority.get('completed_symbols'),
        sorted_order=True,
    )
    parent_scope = _canonical_symbols(parent_symbols, sorted_order=False)
    source_summary_run_id = str(
        authority.get('source_summary_run_id') or ''
    ).strip()
    source_scope_mode = str(
        authority.get('source_feedback_scope_mode') or ''
    ).strip()
    decision_scope_mode = str(
        authority.get('decision_feedback_scope_mode') or ''
    ).strip()
    authority_fs_div = str(authority.get('fs_div') or '').strip().upper()
    authority_cutoff = _utc_datetime(authority.get('knowledge_cutoff_at'))
    if not (
        authority.get('schema_version')
        == FEEDBACK_SCOPE_AUTHORITY_SCHEMA_VERSION
        and authority.get('kind')
        == FEEDBACK_SCOPE_AUTHORITY_COMPLETED_SUMMARY_INTERSECTION
        and source_summary_run_id
        and (
            expected_summary_source_run_id is None
            or source_summary_run_id == expected_summary_source_run_id
        )
        and source_scope_mode
        in {
            FEEDBACK_SCOPE_MODE_BROAD_FEEDBACK_UNIVERSE,
            FEEDBACK_SCOPE_MODE_DECIDER_SCOPE_FAST_PATH,
        }
        and decision_scope_mode
        == FEEDBACK_SCOPE_MODE_DECIDER_SCOPE_FAST_PATH
        and completed_symbols
        and parent_scope
        and set(completed_symbols).issubset(parent_scope)
        and authority_cutoff == knowledge_cutoff_at
        and authority_fs_div == str(fs_div or '').strip().upper()
    ):
        return None, 'scope_authority'

    admitted_symbols = sorted(parent_scope)
    if completed_summary_proof is not None:
        proof_completed_scope = completed_summary_proof.get(
            'completed_scope_proof'
        )
        proof_requested_symbols = _canonical_symbols(
            completed_summary_proof.get('requested_symbols'),
            sorted_order=False,
        )
        if not isinstance(proof_completed_scope, Mapping):
            return None, 'scope_authority'
        proof_completed_symbols = _canonical_symbols(
            proof_completed_scope.get('completed_symbols'),
            sorted_order=False,
        )
        proof_admitted_symbols = _canonical_symbols(
            proof_completed_scope.get('admitted_symbols'),
            sorted_order=False,
        )
        proof_source_run_id = str(
            completed_summary_proof.get('source_run_id') or ''
        ).strip()
        proof_scope_mode = str(
            completed_summary_proof.get('feedback_scope_mode') or ''
        ).strip()
        proof_requested_symbol_count = _integer(
            completed_summary_proof.get('requested_symbol_count')
        )
        if not (
            completed_summary_proof.get('ready') is True
            and proof_source_run_id == source_summary_run_id
            and proof_scope_mode == source_scope_mode
            and _utc_datetime(
                completed_summary_proof.get('knowledge_cutoff_at')
            )
            == knowledge_cutoff_at
            and proof_completed_scope.get('schema_version')
            == COMPLETED_SUMMARIZER_INTERSECTION_PROOF_VERSION
            and proof_requested_symbols == completed_symbols
            and proof_completed_symbols == completed_symbols
            and proof_admitted_symbols
            and proof_requested_symbol_count == len(completed_symbols)
            and set(completed_symbols).issubset(proof_admitted_symbols)
            and (
                (
                    source_scope_mode
                    == FEEDBACK_SCOPE_MODE_BROAD_FEEDBACK_UNIVERSE
                    and len(proof_admitted_symbols) == len(parent_scope)
                    and set(proof_admitted_symbols) == set(parent_scope)
                )
                or (
                    source_scope_mode
                    == FEEDBACK_SCOPE_MODE_DECIDER_SCOPE_FAST_PATH
                    and set(proof_admitted_symbols).issubset(parent_scope)
                )
            )
        ):
            return None, 'scope_authority'
        admitted_symbols = sorted(proof_admitted_symbols)

    admitted_symbol_count = authority.get('admitted_symbol_count')
    admitted_symbols_sha256 = str(
        authority.get('admitted_symbols_sha256') or ''
    ).strip().lower()
    if not (
        type(admitted_symbol_count) is int
        and admitted_symbol_count == len(admitted_symbols)
        and admitted_symbols_sha256
        == hashlib.sha256(
            '\n'.join(admitted_symbols).encode('utf-8')
        ).hexdigest()
    ):
        return None, 'scope_authority'
    return completed_symbols, None


def _role_scope_matches(
    value: object,
    *,
    expected_symbols: Sequence[str],
) -> bool:
    if (
        not isinstance(value, list)
        or not all(isinstance(symbol, str) for symbol in value)
        or len(value) != len(set(value))
    ):
        return False
    requested_symbol_set = set(expected_symbols)
    return all(
        isinstance(symbol, str) and symbol in requested_symbol_set
        for symbol in value
    )


def terminal_feedback_lease_loss_evidence_error(
    *,
    child_run: object | None,
    child_run_id: str,
    market_date: date,
    metadata: Mapping[str, Any],
    knowledge_cutoff_at: datetime,
    completed_summary_proof: Mapping[str, Any] | None = None,
    expected_summary_source_run_id: str | None = None,
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
    expected_symbols, authority_error = _authoritative_completed_summary_scope(
        request_payload=request_payload,
        parent_symbols=raw_symbols,
        knowledge_cutoff_at=knowledge_cutoff_at,
        fs_div=str(metadata.get('fs_div') or ''),
        completed_summary_proof=completed_summary_proof,
        expected_summary_source_run_id=expected_summary_source_run_id,
    )
    if authority_error is not None or expected_symbols is None:
        return authority_error or 'scope_authority'
    expected_roles = list(DECISION_SUPPORT_AGENT_NAMES)
    role_symbol_scopes = request_payload.get('role_symbol_scopes')
    role_symbol_counts = request_payload.get('role_symbol_counts')
    if not (
        request_payload.get('market_date') == market_date.isoformat()
        and request_payload.get('request_run_id') == child_run_id
        and request_payload.get('mode') == DAILY_RETRY_CYCLE_MODE
        and request_payload.get('roles') == expected_roles
        and request_payload.get('requested_symbols') == expected_symbols
        and request_payload.get('feedback_scope_mode')
        == FEEDBACK_SCOPE_MODE_DECIDER_SCOPE_FAST_PATH
        and request_payload.get('admission_scope_by_role')
        == {role: list(expected_symbols) for role in expected_roles}
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
    if not all(
        _role_scope_matches(
            role_symbol_scopes.get(role),
            expected_symbols=expected_symbols,
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


def terminal_feedback_completed_run_evidence_error(
    *,
    child_run: object | None,
    child_run_id: str,
    stage_id: str,
    market_date: date,
    metadata: Mapping[str, Any],
    knowledge_cutoff_at: datetime,
    completed_summary_proof: Mapping[str, Any] | None = None,
    expected_summary_source_run_id: str | None = None,
) -> str | None:
    """Validate a completed feedback child before resuming past its stage."""

    expectations = {
        'agent_feedback_summaries': (
            list(SUMMARIZER_AGENT_NAMES),
            FEEDBACK_SCOPE_MODE_BROAD_FEEDBACK_UNIVERSE,
        ),
        'signal_decider': (
            list(DECISION_SUPPORT_AGENT_NAMES),
            FEEDBACK_SCOPE_MODE_DECIDER_SCOPE_FAST_PATH,
        ),
    }
    expectation = expectations.get(stage_id)
    if expectation is None:
        return 'stage_id'
    if (
        child_run is None
        or str(_field(child_run, 'run_id') or '').strip() != child_run_id
    ):
        return 'child_run'
    if _field(child_run, 'market_date') != market_date:
        return 'market_date'
    if str(_field(child_run, 'status') or '').strip().lower() != 'completed':
        return 'status'
    if _utc_datetime(_field(child_run, 'completed_at')) is None:
        return 'completed_at'
    if _integer(_field(child_run, 'failed_count')) != 0:
        return 'failed_count'

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
    request_payload = _field(child_run, 'request_payload')
    if not isinstance(request_payload, Mapping):
        return 'request_context'
    expected_roles, expected_scope_mode = expectation
    if stage_id == 'signal_decider':
        expected_symbols, authority_error = (
            _authoritative_completed_summary_scope(
                request_payload=request_payload,
                parent_symbols=raw_symbols,
                knowledge_cutoff_at=knowledge_cutoff_at,
                fs_div=str(metadata.get('fs_div') or ''),
                completed_summary_proof=completed_summary_proof,
                expected_summary_source_run_id=(
                    expected_summary_source_run_id
                ),
            )
        )
        if authority_error is not None or expected_symbols is None:
            return authority_error or 'scope_authority'
    else:
        if 'scope_authority' in request_payload:
            return 'scope_authority'
        expected_symbols = list(raw_symbols)
    expected_admission_scope = {
        role: list(expected_symbols) for role in expected_roles
    }
    role_symbol_scopes = request_payload.get('role_symbol_scopes')
    role_symbol_counts = request_payload.get('role_symbol_counts')
    if not (
        isinstance(role_symbol_scopes, Mapping)
        and isinstance(role_symbol_counts, Mapping)
        and set(role_symbol_scopes) == set(expected_roles)
        and set(role_symbol_counts) == set(expected_roles)
        and all(
            _role_scope_matches(
                role_symbol_scopes.get(role),
                expected_symbols=expected_symbols,
            )
            and type(role_symbol_counts.get(role)) is int
            and role_symbol_counts[role]
            == len(role_symbol_scopes[role])
            for role in expected_roles
        )
    ):
        return 'role_scope'
    scoped_symbol_total = sum(role_symbol_counts.values())
    if not (
        _integer(_field(child_run, 'symbols_total')) == scoped_symbol_total
        and _integer(_field(child_run, 'completed_count'))
        == scoped_symbol_total
    ):
        return 'counts'
    expected_force_recompute = bool(
        metadata.get('force')
        and not metadata.get('reuse_existing_feedback')
    )
    if not (
        request_payload.get('market_date') == market_date.isoformat()
        and request_payload.get('request_run_id') == child_run_id
        and request_payload.get('mode') == DAILY_RETRY_CYCLE_MODE
        and request_payload.get('roles') == expected_roles
        and request_payload.get('requested_symbols') == expected_symbols
        and request_payload.get('feedback_scope_mode')
        == expected_scope_mode
        and request_payload.get('admission_scope_by_role')
        == expected_admission_scope
        and str(request_payload.get('fs_div') or '').strip().upper()
        == str(metadata.get('fs_div') or '').strip().upper()
        and request_payload.get('reuse_existing')
        is metadata.get('reuse_existing_feedback')
        and request_payload.get('force_recompute')
        is expected_force_recompute
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
    ):
        return 'request_context'
    return None
