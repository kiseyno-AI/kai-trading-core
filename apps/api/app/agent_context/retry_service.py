from __future__ import annotations

from collections import defaultdict
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
import hashlib
import logging
import threading
import time
from typing import Any, Callable, Mapping, Sequence
from uuid import uuid4

from sqlalchemy import select, text

from app.agent_context.orchestrator_helpers import normalize_requested_symbols as _normalize_requested_symbols
from app.agent_context.prerequisite_service import agent_feedback_prerequisite_service
from app.agent_context.eligibility import (
    AGENT_FEEDBACK_ELIGIBILITY_FILTERS,
    AGENT_FEEDBACK_ELIGIBILITY_RULE,
    build_agent_feedback_eligibility_diagnostics,
    build_agent_feedback_role_funnel,
    load_agent_feedback_eligible_symbols,
    load_agent_feedback_eligibility_records,
    resolve_agent_feedback_scope,
)
from app.agent_context.lease_loss_evidence import (
    RETRY_ADMISSION_LEASE_LOSS_SCHEMA_VERSION,
    RETRY_ADMISSION_SCHEMA_VERSION,
    terminal_feedback_lease_loss_evidence_error,
)
from app.agent_context.orchestrator import agent_feedback_orchestrator
from app.agent_context.progress_service import agent_context_progress_service
from app.agent_context.read_service import (
    resolve_external_news_knowledge_cutoff_for_symbols,
)
from app.agent_context.reuse_service import agent_context_reuse_service
from app.agent_context.roles import (
    DECISION_SUPPORT_AGENT_NAMES,
    DEFAULT_AGENT_NAMES,
    SIGNAL_DECIDER_AGENT_NAME,
    SUMMARIZER_AGENT_NAMES,
)
from app.agent_context.run_modes import (
    DAILY_RETRY_CYCLE_MODE,
    FEEDBACK_SCOPE_MODE_DECIDER_SCOPE_FAST_PATH,
    normalize_feedback_scope_mode,
)
from app.agent_context.run_state_service import agent_context_feedback_run_state_service
from app.agent_context.runtime_state import RuntimeFeedbackRunState, runtime_feedback_state_store
from app.core.settings import settings
from app.db import session_scope
from app.db_models import (
    AgentContextFeedback,
    AgentContextFeedbackFailure,
    AgentContextFeedbackRun,
)

EXPECTED_ROLES: tuple[str, ...] = DEFAULT_AGENT_NAMES
CHILD_RUN_LINEAGE_SCHEMA_VERSION = 'agent_feedback_child_run_lineage_v1'
REUSE_FEEDBACK_EVIDENCE_SCHEMA_VERSION = 'agent_feedback_reuse_evidence_v1'
_LEASE_LOSS_REUSABLE_PARENT_STATUS = 'abandoned_lease_loss_reusable'
logger = logging.getLogger(__name__)


def _normalize_optional_symbol_scope(symbols: Sequence[str]) -> list[str]:
    normalized: list[str] = []
    seen: set[str] = set()
    for raw_symbol in symbols:
        for piece in str(raw_symbol or '').split(','):
            symbol = piece.strip().upper()
            if not symbol or symbol in seen:
                continue
            seen.add(symbol)
            normalized.append(symbol)
    return normalized


def _ordered_symbol_union(symbol_scopes: Sequence[Sequence[str]]) -> list[str]:
    ordered: list[str] = []
    seen: set[str] = set()
    for scope in symbol_scopes:
        for symbol in _normalize_optional_symbol_scope(scope):
            if symbol in seen:
                continue
            seen.add(symbol)
            ordered.append(symbol)
    return ordered


def _feedback_retry_admission_lock_key(
    *,
    market_date: date,
    roles: Sequence[str],
    symbols: Sequence[str] | None,
) -> int:
    # One short transaction lock per market date serializes claim admission.
    # Overlap is then checked against durable running rows while this lock is
    # held. Keeping roles/symbols out of the key prevents A+B and A requests
    # from racing through different advisory locks.
    del roles, symbols
    digest = hashlib.sha256(
        (
            'agent-feedback-retry-admission:'
            f'{market_date.isoformat()}'
        ).encode('utf-8')
    ).digest()
    return int.from_bytes(digest[:8], byteorder='big', signed=True)


def _feedback_retry_owner_token_hash(owner_token: str) -> str:
    return hashlib.sha256(owner_token.encode('utf-8')).hexdigest()


def _normalize_admission_knowledge_cutoff(
    value: datetime | str | None,
) -> str | None:
    if value is None:
        return None
    parsed = (
        value
        if isinstance(value, datetime)
        else datetime.fromisoformat(str(value).strip().replace('Z', '+00:00'))
    )
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError('knowledge_cutoff_at must include a timezone offset')
    return parsed.astimezone(timezone.utc).isoformat()


def _previous_run_matches_knowledge_cutoff(
    previous_run: dict[str, Any] | None,
    effective_knowledge_cutoff_at: datetime | str,
) -> bool:
    if not isinstance(previous_run, dict):
        return False
    request_payload = previous_run.get('request_payload')
    if not isinstance(request_payload, dict):
        return False
    prior_cutoff_at = request_payload.get('admission_knowledge_cutoff_at')
    if prior_cutoff_at is None:
        prior_cutoff_at = request_payload.get('knowledge_cutoff_at')
    try:
        return _normalize_admission_knowledge_cutoff(
            prior_cutoff_at
        ) == _normalize_admission_knowledge_cutoff(
            effective_knowledge_cutoff_at
        )
    except (TypeError, ValueError):
        return False


def _normalize_feedback_fs_div(value: str | None) -> str:
    normalized = str(
        value or settings.dart_fundamentals_default_fs_div
    ).strip().upper()
    if normalized not in {'CFS', 'OFS'}:
        raise ValueError('fs_div must be one of CFS or OFS')
    return normalized


def _previous_run_matches_fs_div(
    previous_run: dict[str, Any] | None,
    effective_fs_div: str,
) -> bool:
    if not isinstance(previous_run, dict):
        return False
    request_payload = previous_run.get('request_payload')
    if not isinstance(request_payload, dict):
        return False
    prior_fs_div = request_payload.get('fs_div')
    if prior_fs_div is None:
        return False
    try:
        return _normalize_feedback_fs_div(prior_fs_div) == effective_fs_div
    except ValueError:
        return False


def resolve_latest_completed_summary_knowledge_cutoff(
    *,
    market_date: date | str,
    feedback_scope_mode: str,
    source_run_id: str | None = None,
    requested_symbols: Sequence[str] | None = None,
    now: datetime | None = None,
    source_run_session: Any | None = None,
) -> dict[str, Any]:
    """Resolve the cutoff owned by a proven summary cycle.

    Decision-support retries must reuse the same point-in-time boundary as
    their upstream summaries. Falling back to the current wall clock would
    make valid persisted summaries look stale while the decider-only retry is
    unable to refresh them. When ``source_run_id`` is provided, resolve only
    that persisted run and require its requested-symbol scope when supplied.
    """

    normalized_market_date = (
        market_date
        if isinstance(market_date, date)
        else date.fromisoformat(str(market_date).strip()[:10])
    )
    normalized_scope_mode = normalize_feedback_scope_mode(
        feedback_scope_mode
    )
    normalized_source_run_id = (
        str(source_run_id or '').strip()
        if source_run_id is not None
        else None
    )
    if source_run_id is not None and not normalized_source_run_id:
        raise ValueError('source_run_id must be non-empty when provided')
    if isinstance(requested_symbols, (str, bytes)):
        raise ValueError('requested_symbols must be a sequence of symbols')
    normalized_requested_symbols = (
        sorted(_normalize_optional_symbol_scope(requested_symbols))
        if requested_symbols is not None
        else None
    )
    if source_run_session is not None and normalized_source_run_id is None:
        raise ValueError(
            'source_run_session requires an exact source_run_id'
        )
    if normalized_source_run_id is not None:
        get_run_kwargs: dict[str, Any] = {
            'run_id': normalized_source_run_id,
        }
        if source_run_session is not None:
            get_run_kwargs.update(
                {
                    'session': source_run_session,
                    'for_update': True,
                }
            )
        latest_run = agent_context_feedback_run_state_service.get_run(
            **get_run_kwargs
        )
    else:
        latest_run_kwargs: dict[str, Any] = {
            'market_date': normalized_market_date,
            'requested_roles': list(SUMMARIZER_AGENT_NAMES),
            'request_modes': [DAILY_RETRY_CYCLE_MODE],
            'feedback_scope_mode': normalized_scope_mode,
        }
        if normalized_requested_symbols is not None:
            latest_run_kwargs['requested_symbols'] = list(
                normalized_requested_symbols
            )
        latest_run = agent_context_feedback_run_state_service.find_latest_run(
            **latest_run_kwargs
        )
    resolved_source_run_id = normalized_source_run_id
    if resolved_source_run_id is None and isinstance(latest_run, dict):
        resolved_source_run_id = str(
            latest_run.get('run_id') or ''
        ).strip()
    base_result: dict[str, Any] = {
        'ready': False,
        'knowledge_cutoff_at': None,
        'source_run_id': resolved_source_run_id,
        'requested_symbols': normalized_requested_symbols,
        'feedback_scope_mode': normalized_scope_mode,
    }
    if not isinstance(latest_run, dict):
        return {**base_result, 'reason': 'summary_run_missing'}

    request_payload = (
        latest_run.get('request_payload')
        if isinstance(latest_run.get('request_payload'), dict)
        else {}
    )
    summary_payload = (
        latest_run.get('summary_payload')
        if isinstance(latest_run.get('summary_payload'), dict)
        else {}
    )
    summary_request = (
        summary_payload.get('request')
        if isinstance(summary_payload.get('request'), dict)
        else {}
    )
    represented_request_payloads = [
        payload
        for payload in (request_payload, summary_request)
        if payload
    ]
    actual_source_run_id = str(latest_run.get('run_id') or '').strip()
    if (
        normalized_source_run_id is not None
        and actual_source_run_id != normalized_source_run_id
    ):
        return {**base_result, 'reason': 'summary_source_run_id_mismatch'}

    if normalized_source_run_id is not None:
        raw_run_market_date = latest_run.get('market_date')
        if raw_run_market_date is None:
            raw_run_market_date = request_payload.get('market_date')
        try:
            run_market_date = (
                raw_run_market_date.date()
                if isinstance(raw_run_market_date, datetime)
                else raw_run_market_date
                if isinstance(raw_run_market_date, date)
                else date.fromisoformat(
                    str(raw_run_market_date or '').strip()[:10]
                )
            )
        except (TypeError, ValueError):
            run_market_date = None
        if run_market_date != normalized_market_date:
            return {**base_result, 'reason': 'summary_run_market_date_mismatch'}
        if (
            str(request_payload.get('mode') or '').strip()
            != DAILY_RETRY_CYCLE_MODE
        ):
            return {**base_result, 'reason': 'summary_run_mode_mismatch'}
        try:
            persisted_scope_mode = normalize_feedback_scope_mode(
                request_payload.get('feedback_scope_mode')
            )
        except ValueError:
            persisted_scope_mode = None
        if persisted_scope_mode != normalized_scope_mode:
            return {
                **base_result,
                'reason': 'summary_run_feedback_scope_mode_mismatch',
            }

    run_roles = request_payload.get('roles')
    if sorted(run_roles if isinstance(run_roles, list) else []) != sorted(
        SUMMARIZER_AGENT_NAMES
    ):
        return {**base_result, 'reason': 'summary_run_roles_mismatch'}

    if normalized_requested_symbols is not None:
        represented_symbol_scopes: list[list[str]] = []
        for payload in represented_request_payloads:
            if 'requested_symbols' not in payload:
                continue
            raw_symbols = payload.get('requested_symbols')
            if not isinstance(raw_symbols, (list, tuple, set)):
                return {
                    **base_result,
                    'reason': 'summary_run_symbol_scope_unavailable',
                }
            represented_symbol_scopes.append(
                sorted(_normalize_optional_symbol_scope(raw_symbols))
            )
        if not represented_symbol_scopes:
            return {
                **base_result,
                'reason': 'summary_run_symbol_scope_unavailable',
            }
        if any(
            persisted_scope != normalized_requested_symbols
            for persisted_scope in represented_symbol_scopes
        ):
            return {
                **base_result,
                'reason': 'summary_run_symbol_scope_mismatch',
            }

    if normalized_source_run_id is not None:
        for payload in represented_request_payloads:
            if (
                payload.get('request_run_id') is not None
                and str(payload.get('request_run_id') or '').strip()
                != normalized_source_run_id
            ):
                return {
                    **base_result,
                    'reason': 'summary_run_lineage_mismatch',
                }
            if 'child_run_lineage' not in payload:
                continue
            raw_lineage = payload.get('child_run_lineage')
            if not isinstance(raw_lineage, Mapping) or (
                str(raw_lineage.get('schema_version') or '').strip()
                != CHILD_RUN_LINEAGE_SCHEMA_VERSION
                or str(raw_lineage.get('parent_run_id') or '').strip()
                != normalized_source_run_id
                or str(raw_lineage.get('child_link_field') or '').strip()
                != 'parent_run_id'
            ):
                return {
                    **base_result,
                    'reason': 'summary_run_lineage_mismatch',
                }

    status = str(latest_run.get('status') or '').strip().lower()
    completed_at = latest_run.get('completed_at')
    if completed_at is None:
        return {**base_result, 'reason': 'summary_run_not_finalized'}
    try:
        symbols_total = max(int(latest_run.get('symbols_total') or 0), 0)
        completed_count = max(
            int(latest_run.get('completed_count') or 0),
            0,
        )
        failed_count = max(int(latest_run.get('failed_count') or 0), 0)
    except (TypeError, ValueError):
        return {**base_result, 'reason': 'summary_run_counts_invalid'}
    if (
        status != 'completed'
        or failed_count != 0
        or completed_count != symbols_total
    ):
        return {**base_result, 'reason': 'summary_run_incomplete'}

    raw_cutoff = request_payload.get('admission_knowledge_cutoff_at')
    if raw_cutoff is None:
        raw_cutoff = request_payload.get('knowledge_cutoff_at')
    try:
        normalized_cutoff = _normalize_admission_knowledge_cutoff(raw_cutoff)
        if normalized_cutoff is None:
            raise ValueError('summary cutoff missing')
        cutoff = datetime.fromisoformat(normalized_cutoff)
    except (TypeError, ValueError):
        return {**base_result, 'reason': 'summary_cutoff_invalid'}

    resolved_now = now or datetime.now(timezone.utc)
    if resolved_now.tzinfo is None or resolved_now.utcoffset() is None:
        raise ValueError('now must include a timezone offset')
    if cutoff > resolved_now.astimezone(timezone.utc):
        return {**base_result, 'reason': 'summary_cutoff_in_future'}

    return {
        **base_result,
        'ready': True,
        'reason': 'latest_completed_summary_cutoff',
        'knowledge_cutoff_at': cutoff,
    }


def _normalized_admission_scope(
    *,
    roles: Sequence[str],
    symbols: Sequence[str] | None,
) -> dict[str, frozenset[str] | None]:
    normalized_symbols = (
        frozenset(_normalize_optional_symbol_scope(symbols))
        if symbols is not None
        else None
    )
    return {
        str(role or '').strip().lower(): normalized_symbols
        for role in roles
        if str(role or '').strip()
    }


def _admission_scope_from_payload(
    payload: dict[str, Any],
) -> dict[str, frozenset[str] | None]:
    raw_admission_scope = payload.get('admission_scope_by_role')
    if isinstance(raw_admission_scope, dict):
        return {
            str(role or '').strip().lower(): (
                frozenset(_normalize_optional_symbol_scope(symbols))
                if isinstance(symbols, list)
                else None
            )
            for role, symbols in raw_admission_scope.items()
            if str(role or '').strip()
        }

    raw_role_scopes = payload.get('role_symbol_scopes')
    if isinstance(raw_role_scopes, dict) and raw_role_scopes:
        return {
            str(role or '').strip().lower(): frozenset(
                _normalize_optional_symbol_scope(symbols)
            )
            for role, symbols in raw_role_scopes.items()
            if str(role or '').strip() and isinstance(symbols, list)
        }

    roles = payload.get('roles')
    if not isinstance(roles, list):
        return {}
    if 'requested_symbols' in payload:
        raw_symbols = payload.get('requested_symbols')
    else:
        raw_symbols = payload.get('symbols')
    return _normalized_admission_scope(
        roles=roles,
        symbols=raw_symbols if isinstance(raw_symbols, list) else None,
    )


def _admission_scopes_overlap(
    left: dict[str, frozenset[str] | None],
    right: dict[str, frozenset[str] | None],
) -> bool:
    for role in set(left) & set(right):
        left_symbols = left[role]
        right_symbols = right[role]
        if left_symbols is None or right_symbols is None:
            return True
        if left_symbols & right_symbols:
            return True
    return False


def _feedback_retry_run_is_stale(row: dict[str, Any]) -> bool:
    if str(row.get('status') or '').strip().lower() != 'running':
        return False
    updated_at = row.get('updated_at') or row.get('started_at')
    if not isinstance(updated_at, datetime):
        return False
    if updated_at.tzinfo is None or updated_at.utcoffset() is None:
        updated_at = updated_at.replace(tzinfo=timezone.utc)
    else:
        updated_at = updated_at.astimezone(timezone.utc)
    max_age_minutes = max(
        1,
        int(settings.eod_auto_pykrx_stale_run_minutes or 30),
    )
    return datetime.now(timezone.utc) - updated_at > timedelta(
        minutes=max_age_minutes
    )


def _serialize_feedback_retry_run(row: AgentContextFeedbackRun) -> dict[str, Any]:
    return {
        'id': int(row.id),
        'run_id': str(row.run_id or ''),
        'market_date': row.market_date,
        'status': str(row.status or ''),
        'symbols_total': int(row.symbols_total or 0),
        'completed_count': int(row.completed_count or 0),
        'failed_count': int(row.failed_count or 0),
        'request_payload': (
            dict(row.request_payload)
            if isinstance(row.request_payload, dict)
            else {}
        ),
        'summary_payload': (
            dict(row.summary_payload)
            if isinstance(row.summary_payload, dict)
            else {}
        ),
        'started_at': row.started_at,
        'completed_at': row.completed_at,
        'updated_at': row.updated_at,
    }


def _claim_feedback_retry_run(
    *,
    market_date: date,
    roles: Sequence[str],
    requested_symbols: Sequence[str] | None,
    feedback_scope_mode: str,
    run_id: str,
    started_at: datetime,
    admission_knowledge_cutoff_at: datetime | str | None = None,
    fs_div: str | None = None,
) -> dict[str, Any]:
    """Atomically claim a durable retry scope without holding a pool slot."""

    normalized_roles = tuple(
        sorted(
            {
                str(role or '').strip().lower()
                for role in roles
                if str(role or '').strip()
            }
        )
    )
    normalized_symbols = (
        tuple(sorted(_normalize_optional_symbol_scope(requested_symbols)))
        if requested_symbols is not None
        else None
    )
    normalized_feedback_scope_mode = normalize_feedback_scope_mode(
        feedback_scope_mode
    )
    normalized_admission_knowledge_cutoff_at = (
        _normalize_admission_knowledge_cutoff(
            admission_knowledge_cutoff_at
        )
    )
    normalized_fs_div = _normalize_feedback_fs_div(fs_div)
    requested_scope = _normalized_admission_scope(
        roles=normalized_roles,
        symbols=normalized_symbols,
    )
    now = datetime.now(timezone.utc)
    owner_token = uuid4().hex
    lock_key = _feedback_retry_admission_lock_key(
        market_date=market_date,
        roles=normalized_roles,
        symbols=normalized_symbols,
    )
    with session_scope() as session:
        bind = session.get_bind()
        if bind is not None and bind.dialect.name == 'postgresql':
            session.execute(
                text('SELECT pg_advisory_xact_lock(:lock_key)'),
                {'lock_key': lock_key},
            )

        running_rows = session.execute(
            select(AgentContextFeedbackRun)
            .where(
                AgentContextFeedbackRun.market_date == market_date,
                AgentContextFeedbackRun.status == 'running',
            )
            .order_by(
                AgentContextFeedbackRun.started_at.asc(),
                AgentContextFeedbackRun.id.asc(),
            )
        ).scalars().all()
        for row in running_rows:
            payload = (
                dict(row.request_payload)
                if isinstance(row.request_payload, dict)
                else {}
            )
            if str(payload.get('mode') or '').strip() != DAILY_RETRY_CYCLE_MODE:
                continue
            snapshot = _serialize_feedback_retry_run(row)
            if _feedback_retry_run_is_stale(snapshot):
                stale_admission = (
                    dict(payload.get('retry_admission'))
                    if isinstance(payload.get('retry_admission'), dict)
                    else {}
                )
                stale_admission.update(
                    {
                        'ownership_revoked_at': now.isoformat(),
                        'ownership_revoked_reason': 'lease_expired_takeover',
                        'superseded_by_run_id': run_id,
                    }
                )
                payload['retry_admission'] = stale_admission
                row.request_payload = payload
                row.status = 'abandoned'
                row.failed_count = max(
                    int(row.failed_count or 0),
                    max(
                        int(row.symbols_total or 0)
                        - int(row.completed_count or 0),
                        0,
                    ),
                )
                row.completed_at = now
                row.updated_at = now
                stale_summary = dict(row.summary_payload or {})
                stale_summary['stale_running_abandoned_at'] = now.isoformat()
                stale_summary['stale_running_abandoned_reason'] = (
                    'Replaced during atomic retry admission after its durable '
                    'lease expired.'
                )
                row.summary_payload = stale_summary
                continue

            active_scope = _admission_scope_from_payload(payload)
            if not _admission_scopes_overlap(requested_scope, active_scope):
                continue
            try:
                active_feedback_scope_mode = normalize_feedback_scope_mode(
                    payload.get('feedback_scope_mode')
                )
            except ValueError:
                active_feedback_scope_mode = '__invalid_active_scope_mode__'
            raw_active_cutoff = (
                payload.get('admission_knowledge_cutoff_at')
                if 'admission_knowledge_cutoff_at' in payload
                else payload.get('requested_knowledge_cutoff_at')
                if 'requested_knowledge_cutoff_at' in payload
                else payload.get('knowledge_cutoff_at')
            )
            try:
                active_knowledge_cutoff_at = (
                    _normalize_admission_knowledge_cutoff(raw_active_cutoff)
                )
            except (TypeError, ValueError):
                active_knowledge_cutoff_at = '__invalid_active_cutoff__'
            raw_active_fs_div = payload.get('fs_div')
            try:
                active_fs_div = (
                    _normalize_feedback_fs_div(raw_active_fs_div)
                    if raw_active_fs_div is not None
                    else '__missing_active_fs_div__'
                )
            except ValueError:
                active_fs_div = '__invalid_active_fs_div__'
            return {
                'acquired': False,
                'run_id': None,
                'owner_token': None,
                'conflict_run': snapshot,
                'exact_scope': bool(
                    requested_scope == active_scope
                    and normalized_feedback_scope_mode
                    == active_feedback_scope_mode
                    and normalized_admission_knowledge_cutoff_at
                    == active_knowledge_cutoff_at
                    and normalized_fs_div == active_fs_div
                ),
            }

        admission_scope_payload = {
            role: (
                sorted(symbols) if symbols is not None else None
            )
            for role, symbols in requested_scope.items()
        }
        session.add(
            AgentContextFeedbackRun(
                run_id=run_id,
                market_date=market_date,
                status='running',
                symbols_total=(
                    len(normalized_symbols or ()) * len(normalized_roles)
                ),
                completed_count=0,
                failed_count=0,
                request_payload={
                    'market_date': market_date.isoformat(),
                    'mode': DAILY_RETRY_CYCLE_MODE,
                    'roles': list(normalized_roles),
                    'requested_symbols': (
                        list(normalized_symbols)
                        if normalized_symbols is not None
                        else None
                    ),
                    'feedback_scope_mode': normalized_feedback_scope_mode,
                    'fs_div': normalized_fs_div,
                    'admission_knowledge_cutoff_at': (
                        normalized_admission_knowledge_cutoff_at
                    ),
                    'admission_scope_by_role': admission_scope_payload,
                    'retry_admission': {
                        'schema_version': RETRY_ADMISSION_SCHEMA_VERSION,
                        'claimed_at': now.isoformat(),
                        'owner_token_hash': (
                            _feedback_retry_owner_token_hash(owner_token)
                        ),
                    },
                },
                summary_payload={
                    'lifecycle_stage': 'admission_claimed',
                    'admission_scope_by_role': admission_scope_payload,
                },
                started_at=started_at,
                completed_at=None,
            )
        )
    return {
        'acquired': True,
        'run_id': run_id,
        'owner_token': owner_token,
        'conflict_run': None,
        'exact_scope': True,
    }


def _retry_admission_owner_matches(
    row: AgentContextFeedbackRun,
    *,
    owner_token: str,
) -> bool:
    payload = (
        row.request_payload
        if isinstance(row.request_payload, dict)
        else {}
    )
    admission = (
        payload.get('retry_admission')
        if isinstance(payload.get('retry_admission'), dict)
        else {}
    )
    return bool(
        owner_token
        and admission.get('owner_token_hash')
        == _feedback_retry_owner_token_hash(owner_token)
        and not admission.get('ownership_revoked_at')
    )


def _refresh_feedback_retry_claim(
    *,
    run_id: str,
    owner_token: str,
) -> bool:
    with session_scope() as session:
        row = session.execute(
            select(AgentContextFeedbackRun).where(
                AgentContextFeedbackRun.run_id == run_id
            ).with_for_update()
        ).scalar_one_or_none()
        if (
            row is None
            or str(row.status or '').strip().lower() != 'running'
            or not _retry_admission_owner_matches(
                row,
                owner_token=owner_token,
            )
        ):
            return False
        row.updated_at = datetime.now(timezone.utc)
    return True


def _record_feedback_retry_lease_loss(
    *,
    run_id: str,
    owner_token: str,
    stage: str,
    reason: str,
    error_type: str | None = None,
) -> bool:
    now = datetime.now(timezone.utc)
    try:
        with session_scope() as session:
            row = session.execute(
                select(AgentContextFeedbackRun).where(
                    AgentContextFeedbackRun.run_id == run_id
                ).with_for_update()
            ).scalar_one_or_none()
            if row is None:
                return False
            payload = (
                row.request_payload
                if isinstance(row.request_payload, dict)
                else {}
            )
            admission = (
                payload.get('retry_admission')
                if isinstance(payload.get('retry_admission'), dict)
                else {}
            )
            if admission.get(
                'owner_token_hash'
            ) != _feedback_retry_owner_token_hash(owner_token):
                return False
            admission.setdefault('ownership_revoked_at', now.isoformat())
            admission.setdefault('ownership_revoked_reason', reason)
            admission['ownership_loss_detected_stage'] = stage
            payload = dict(payload)
            payload['retry_admission'] = admission
            row.request_payload = payload
            loss_event = {
                'schema_version': RETRY_ADMISSION_LEASE_LOSS_SCHEMA_VERSION,
                'detected_at': now.isoformat(),
                'stage': stage,
                'reason': reason,
                'owner_token_fingerprint': hashlib.sha256(
                    owner_token.encode('utf-8')
                ).hexdigest()[:16],
            }
            if error_type:
                loss_event['error_type'] = error_type
            summary_payload = dict(row.summary_payload or {})
            summary_payload['admission_lease_loss'] = loss_event
            row.summary_payload = summary_payload
            if str(row.status or '').strip().lower() == 'running':
                row.status = 'abandoned'
                row.failed_count = max(
                    int(row.failed_count or 0),
                    max(
                        int(row.symbols_total or 0)
                        - int(row.completed_count or 0),
                        0,
                    ),
                )
                row.completed_at = now
            row.updated_at = now
        return True
    except Exception:
        logger.warning(
            'Failed to persist feedback retry lease-loss event',
            extra={'run_id': run_id, 'stage': stage, 'reason': reason},
            exc_info=True,
        )
        return False


class FeedbackRetryLeaseLostError(RuntimeError):
    """Raised when a retry worker can no longer prove durable ownership."""


class _FeedbackRetryClaimLease:
    def __init__(
        self,
        *,
        run_id: str,
        owner_token: str,
        heartbeat_grace_seconds: float,
    ) -> None:
        self.run_id = run_id
        self.owner_token = owner_token
        self.heartbeat_grace_seconds = max(0.0, heartbeat_grace_seconds)
        self._lock = threading.RLock()
        self._lost_event = threading.Event()
        self._finalizing_event = threading.Event()
        self._last_confirmed_monotonic = time.monotonic()
        self._loss_reason: str | None = None
        self._loss_error_type: str | None = None
        self._loss_persisted = False

    @property
    def is_lost(self) -> bool:
        return self._lost_event.is_set()

    @property
    def is_finalizing(self) -> bool:
        return self._finalizing_event.is_set()

    def begin_finalization(self) -> None:
        with self._lock:
            if self._lost_event.is_set():
                self._persist_loss(stage='before_parent_finish')
                self._raise_lost(stage='before_parent_finish')
            self._finalizing_event.set()

    def mark_confirmed(self) -> None:
        with self._lock:
            if self._lost_event.is_set():
                return
            self._last_confirmed_monotonic = time.monotonic()

    def mark_lost(
        self,
        *,
        reason: str,
        stage: str,
        error_type: str | None = None,
    ) -> None:
        with self._lock:
            # Once the final ownership check has atomically moved this lease
            # into finalization, a late heartbeat result must not reinterpret
            # the run's normal completed status as an ownership loss.
            if self._finalizing_event.is_set():
                return
            if not self._lost_event.is_set():
                self._loss_reason = reason
                self._loss_error_type = error_type
                self._lost_event.set()
        self._persist_loss(stage=stage)

    def note_heartbeat_exception(self, exc: Exception) -> None:
        with self._lock:
            elapsed = time.monotonic() - self._last_confirmed_monotonic
        if elapsed < self.heartbeat_grace_seconds:
            return
        self.mark_lost(
            reason='heartbeat_refresh_grace_exceeded',
            stage='heartbeat',
            error_type=type(exc).__name__,
        )

    def assert_owned(self, *, stage: str) -> None:
        if self.is_lost:
            self._persist_loss(stage=stage)
            self._raise_lost(stage=stage)
        try:
            owned = _refresh_feedback_retry_claim(
                run_id=self.run_id,
                owner_token=self.owner_token,
            )
        except Exception as exc:
            self.mark_lost(
                reason='ownership_verification_failed',
                stage=stage,
                error_type=type(exc).__name__,
            )
            self._raise_lost(stage=stage)
        if not owned:
            self.mark_lost(
                reason='durable_claim_not_owned',
                stage=stage,
            )
            self._raise_lost(stage=stage)
        self.mark_confirmed()
        if self.is_lost:
            self._persist_loss(stage=stage)
            self._raise_lost(stage=stage)

    def _persist_loss(self, *, stage: str) -> None:
        with self._lock:
            if self._loss_persisted:
                return
            reason = self._loss_reason or 'durable_claim_not_owned'
            error_type = self._loss_error_type
        persisted = _record_feedback_retry_lease_loss(
            run_id=self.run_id,
            owner_token=self.owner_token,
            stage=stage,
            reason=reason,
            error_type=error_type,
        )
        if persisted:
            with self._lock:
                self._loss_persisted = True

    def _raise_lost(self, *, stage: str) -> None:
        reason = self._loss_reason or 'durable_claim_not_owned'
        raise FeedbackRetryLeaseLostError(
            'feedback retry lease ownership lost '
            f'for {self.run_id} at {stage}: {reason}'
        )


@contextmanager
def _feedback_retry_claim_lease(
    *,
    run_id: str,
    owner_token: str,
):
    """Refresh the durable claim without reserving a database connection."""

    stop_event = threading.Event()
    stale_seconds = max(
        60,
        max(1, int(settings.eod_auto_pykrx_stale_run_minutes or 30)) * 60,
    )
    interval_seconds = max(
        5,
        min(
            60,
            stale_seconds // 3,
        ),
    )
    heartbeat_grace_seconds = max(
        float(interval_seconds),
        min(120.0, stale_seconds / 2),
    )
    lease = _FeedbackRetryClaimLease(
        run_id=run_id,
        owner_token=owner_token,
        heartbeat_grace_seconds=heartbeat_grace_seconds,
    )

    def heartbeat() -> None:
        while not stop_event.wait(interval_seconds):
            if lease.is_finalizing:
                return
            try:
                owned = _refresh_feedback_retry_claim(
                    run_id=run_id,
                    owner_token=owner_token,
                )
                if stop_event.is_set() or lease.is_finalizing:
                    return
                if not owned:
                    lease.mark_lost(
                        reason='durable_claim_not_owned',
                        stage='heartbeat',
                    )
                    return
                lease.mark_confirmed()
            except Exception as exc:
                if stop_event.is_set() or lease.is_finalizing:
                    return
                lease.note_heartbeat_exception(exc)
                logger.warning(
                    'Failed to refresh feedback retry durable admission lease',
                    extra={
                        'run_id': run_id,
                        'heartbeat_grace_seconds': heartbeat_grace_seconds,
                    },
                    exc_info=True,
                )
                if lease.is_lost:
                    return

    thread = threading.Thread(
        target=heartbeat,
        name=f'feedback-retry-lease-{run_id[-12:]}',
        daemon=True,
    )
    thread.start()
    try:
        yield lease
    finally:
        stop_event.set()
        thread.join(timeout=1)


def _finish_feedback_retry_claim_if_running(
    *,
    run_id: str,
    owner_token: str,
    status: str,
    error: Exception | None = None,
) -> None:
    now = datetime.now(timezone.utc)
    with session_scope() as session:
        row = session.execute(
            select(AgentContextFeedbackRun).where(
                AgentContextFeedbackRun.run_id == run_id
            ).with_for_update()
        ).scalar_one_or_none()
        if (
            row is None
            or str(row.status or '').strip().lower() != 'running'
            or not _retry_admission_owner_matches(
                row,
                owner_token=owner_token,
            )
        ):
            return
        resolved_status = (
            status
            if status in {'completed', 'partial', 'failed'}
            else 'failed'
        )
        row.status = resolved_status
        if resolved_status == 'failed':
            row.failed_count = max(
                int(row.failed_count or 0),
                max(
                    int(row.symbols_total or 0)
                    - int(row.completed_count or 0),
                    0,
                ),
            )
        row.completed_at = now
        row.updated_at = now
        summary_payload = dict(row.summary_payload or {})
        summary_payload['admission_owner_finalized_at'] = now.isoformat()
        if error is not None:
            summary_payload['admission_owner_error'] = {
                'type': type(error).__name__,
                'message': str(error),
            }
        row.summary_payload = summary_payload


@dataclass(frozen=True)
class FeedbackRetryCycleConfig:
    market_date: date
    roles: tuple[str, ...]
    symbols: tuple[str, ...] | None = None
    knowledge_cutoff_at: datetime | str | None = None
    fs_div: str | None = None
    feedback_scope_mode: str = FEEDBACK_SCOPE_MODE_DECIDER_SCOPE_FAST_PATH
    max_cycles: int = 3
    outer_batch_size: int = 20
    reuse_existing: bool = True
    force_recompute: bool = False
    progress_callback: Callable[..., Any] | None = None


@dataclass(frozen=True)
class RoleSequenceRoleState:
    role: str
    total_symbols: int
    completed_symbols: tuple[str, ...]
    failed_symbols: tuple[str, ...]
    terminal_symbols: tuple[str, ...]
    boundary_pending_symbols: tuple[str, ...]
    retry_pending_symbols: tuple[str, ...]
    boundary_status: str
    reused_symbols: tuple[str, ...] = ()
    reused_feedback_evidence: dict[str, dict[str, Any]] = field(
        default_factory=dict
    )
    stale_symbols: tuple[str, ...] = ()


@dataclass(frozen=True)
class RoleSequenceSnapshot:
    market_date: date
    symbols: tuple[str, ...]
    roles: tuple[str, ...]
    role_states: dict[str, RoleSequenceRoleState]
    role_symbol_scopes: dict[str, tuple[str, ...]] = field(default_factory=dict)

    def boundary_pending_roles(self) -> list[str]:
        return [
            role
            for role in self.roles
            if self.role_states[role].boundary_pending_symbols
        ]

    def retry_pending_roles(self) -> list[str]:
        return [
            role
            for role in self.roles
            if self.role_states[role].retry_pending_symbols
        ]


@dataclass(frozen=True)
class RetryCyclePlan:
    cycle_number: int
    completed_cycles: int
    mode: str
    current_role: str
    plan_roles: tuple[str, ...]
    completed_roles: tuple[str, ...]
    remaining_roles: tuple[str, ...]


class AgentFeedbackRetryService:
    def _load_eligible_symbols(
        self,
        *,
        market_date: date,
        requested_symbols: Sequence[str] | None = None,
    ) -> list[str]:
        return load_agent_feedback_eligible_symbols(
            market_date=market_date,
            requested_symbols=requested_symbols,
        )

    def load_incomplete_symbols(
        self,
        *,
        market_date: date,
        roles: Sequence[str],
        eligible_symbols: Sequence[str] | None = None,
        force_recompute: bool = False,
        reuse_existing: bool = True,
        knowledge_cutoff_at: datetime | str | None = None,
        fs_div: str | None = None,
    ) -> list[str]:
        expected_roles = tuple(roles)
        symbols = list(eligible_symbols) if eligible_symbols is not None else self._load_eligible_symbols(market_date=market_date)
        if not symbols:
            return []

        decision_kwargs: dict[str, Any] = {
            'market_date': market_date,
            'symbols': symbols,
            'roles': expected_roles,
            'force_recompute': force_recompute,
            'fs_div': _normalize_feedback_fs_div(fs_div),
        }
        if knowledge_cutoff_at is not None:
            decision_kwargs['knowledge_cutoff_at'] = knowledge_cutoff_at
        decisions = (
            agent_context_reuse_service.build_latest_decisions(
                **decision_kwargs
            )
            if reuse_existing
            else {}
        )
        completed_by_role: dict[str, set[str]] = {}
        if not force_recompute and reuse_existing:
            completed_by_role, _ = self._load_same_day_terminal_symbols(
                market_date=market_date,
                roles=expected_roles,
                symbols=symbols,
            )
        incomplete: list[str] = []
        for symbol in symbols:
            if (
                not force_recompute
                and expected_roles
                and all(symbol in completed_by_role.get(role, set()) for role in expected_roles)
            ):
                continue
            symbol_decisions = decisions.get(symbol, {})
            if any(
                symbol_decisions.get(role) is None or symbol_decisions[role].should_dispatch
                for role in expected_roles
            ):
                incomplete.append(symbol)
        return incomplete

    def _load_same_day_terminal_symbols(
        self,
        *,
        market_date: date,
        roles: Sequence[str],
        symbols: Sequence[str],
        required_parent_run_id: str | None = None,
    ) -> tuple[dict[str, set[str]], dict[str, set[str]]]:
        completed_by_role: dict[str, set[str]] = defaultdict(set)
        failed_by_role: dict[str, set[str]] = defaultdict(set)
        if not roles or not symbols:
            return completed_by_role, failed_by_role

        normalized_roles = {
            str(role or '').strip().lower()
            for role in roles
            if str(role or '').strip()
        }
        normalized_symbols = {
            str(symbol or '').strip().upper()
            for symbol in symbols
            if str(symbol or '').strip()
        }

        with session_scope() as session:
            persisted_feedback_rows = session.execute(
                select(
                    AgentContextFeedback.id,
                    AgentContextFeedback.symbol,
                    AgentContextFeedback.agent_name,
                    AgentContextFeedback.run_id,
                    AgentContextFeedback.received_at,
                )
                .where(AgentContextFeedback.market_date == market_date)
                .where(AgentContextFeedback.symbol.in_(list(symbols)))
                .where(AgentContextFeedback.agent_name.in_(list(roles)))
            ).all()
            persisted_failure_rows = session.execute(
                select(
                    AgentContextFeedbackFailure.id,
                    AgentContextFeedbackFailure.symbol,
                    AgentContextFeedbackFailure.agent_name,
                    AgentContextFeedbackFailure.run_id,
                    AgentContextFeedbackFailure.received_at,
                )
                .where(AgentContextFeedbackFailure.market_date == market_date)
                .where(AgentContextFeedbackFailure.symbol.in_(list(symbols)))
                .where(AgentContextFeedbackFailure.agent_name.in_(list(roles)))
            ).all()
            feedback_rows = [
                {
                    'id': int(row.id),
                    'symbol': str(row.symbol).upper(),
                    'agent_name': str(row.agent_name).lower(),
                    'run_id': str(row.run_id or '').strip(),
                    'received_at': row.received_at,
                }
                for row in persisted_feedback_rows
            ]
            failure_rows = [
                {
                    'id': int(row.id),
                    'symbol': str(row.symbol).upper(),
                    'agent_name': str(row.agent_name).lower(),
                    'run_id': str(row.run_id or '').strip(),
                    'received_at': row.received_at,
                }
                for row in persisted_failure_rows
            ]
            child_run_rows: list[dict[str, Any]] = []
            if required_parent_run_id:
                persisted_child_run_rows = session.execute(
                    select(
                        AgentContextFeedbackRun.id,
                        AgentContextFeedbackRun.run_id,
                        AgentContextFeedbackRun.status,
                        AgentContextFeedbackRun.failed_count,
                        AgentContextFeedbackRun.request_payload,
                        AgentContextFeedbackRun.summary_payload,
                        AgentContextFeedbackRun.started_at,
                        AgentContextFeedbackRun.updated_at,
                    )
                    .where(AgentContextFeedbackRun.market_date == market_date)
                    .order_by(
                        AgentContextFeedbackRun.started_at.desc(),
                        AgentContextFeedbackRun.id.desc(),
                    )
                ).all()
                child_run_rows = [
                    {
                        'id': int(row.id),
                        'run_id': str(row.run_id or '').strip(),
                        'status': str(row.status or '').strip().lower(),
                        'failed_count': int(row.failed_count or 0),
                        'request_payload': (
                            dict(row.request_payload)
                            if isinstance(row.request_payload, dict)
                            else {}
                        ),
                        'summary_payload': (
                            dict(row.summary_payload)
                            if isinstance(row.summary_payload, dict)
                            else {}
                        ),
                        'started_at': row.started_at,
                        'updated_at': row.updated_at,
                    }
                    for row in persisted_child_run_rows
                ]

        def normalized_observed_at(value: Any) -> datetime:
            if not isinstance(value, datetime):
                return datetime.min.replace(tzinfo=timezone.utc)
            if value.tzinfo is None or value.utcoffset() is None:
                return value.replace(tzinfo=timezone.utc)
            return value.astimezone(timezone.utc)

        feedback_by_key = {
            (row['symbol'], row['agent_name']): row
            for row in feedback_rows
        }
        failure_by_key = {
            (row['symbol'], row['agent_name']): row
            for row in failure_rows
        }

        def latest_evidence_kind(
            *,
            key: tuple[str, str],
            required_run_id: str | None = None,
        ) -> str | None:
            feedback = feedback_by_key.get(key)
            failure = failure_by_key.get(key)
            if required_run_id is not None:
                if feedback is not None and feedback['run_id'] != required_run_id:
                    feedback = None
                if failure is not None and failure['run_id'] != required_run_id:
                    failure = None
            if failure is None:
                return 'completed' if feedback is not None else None
            if feedback is None:
                return 'failed'
            failure_key = (
                normalized_observed_at(failure['received_at']),
                int(failure['id']),
            )
            feedback_key = (
                normalized_observed_at(feedback['received_at']),
                int(feedback['id']),
            )
            # IDs belong to different tables, so an equal receipt time is
            # ambiguous and must fail closed rather than prefer success.
            return 'failed' if failure_key[0] >= feedback_key[0] else 'completed'

        if not required_parent_run_id:
            for key in set(feedback_by_key) | set(failure_by_key):
                symbol, role = key
                if symbol not in normalized_symbols or role not in normalized_roles:
                    continue
                evidence_kind = latest_evidence_kind(key=key)
                if evidence_kind == 'completed':
                    completed_by_role[role].add(symbol)
                elif evidence_kind == 'failed':
                    failed_by_role[role].add(symbol)
            return completed_by_role, failed_by_role

        def scopes_for_payload(payload: dict[str, Any]) -> dict[str, list[str]]:
            scopes: dict[str, list[str]] = {}
            raw_scopes = payload.get('role_symbol_scopes')
            if isinstance(raw_scopes, dict):
                for raw_role, raw_scope in raw_scopes.items():
                    role = str(raw_role or '').strip().lower()
                    if role not in normalized_roles or not isinstance(raw_scope, list):
                        continue
                    scopes[role] = _normalize_optional_symbol_scope(raw_scope)
            if scopes:
                return scopes
            raw_roles = payload.get('roles')
            raw_symbols = (
                payload.get('requested_symbols')
                if isinstance(payload.get('requested_symbols'), list)
                else payload.get('symbols')
            )
            if not isinstance(raw_roles, list) or not isinstance(raw_symbols, list):
                return {}
            fallback_symbols = _normalize_optional_symbol_scope(raw_symbols)
            return {
                str(raw_role or '').strip().lower(): fallback_symbols
                for raw_role in raw_roles
                if str(raw_role or '').strip().lower() in normalized_roles
            }

        latest_child_by_key: dict[tuple[str, str], dict[str, Any]] = {}
        for row in child_run_rows:
            payload = row['request_payload']
            if str(payload.get('parent_run_id') or '').strip() != required_parent_run_id:
                continue
            for role, scoped_symbols in scopes_for_payload(payload).items():
                for symbol in scoped_symbols:
                    key = (symbol, role)
                    if symbol in normalized_symbols:
                        latest_child_by_key.setdefault(key, row)

        def failed_scope_keys_for_child(
            child: dict[str, Any],
        ) -> set[tuple[str, str]]:
            child_scopes = scopes_for_payload(child['request_payload'])
            all_child_keys = {
                (symbol, role)
                for role, scoped_symbols in child_scopes.items()
                for symbol in scoped_symbols
            }
            summary_payload = child.get('summary_payload')
            containers = (
                [summary_payload]
                if isinstance(summary_payload, dict)
                else []
            )
            progress_summary = (
                summary_payload.get('progress_summary')
                if isinstance(summary_payload, dict)
                else None
            )
            if isinstance(progress_summary, dict):
                containers.append(progress_summary)
            failed_keys: set[tuple[str, str]] = set()
            for container in containers:
                raw_by_role = container.get('failed_symbols_by_role')
                if not isinstance(raw_by_role, dict):
                    continue
                for raw_role, raw_symbols in raw_by_role.items():
                    role = str(raw_role or '').strip().lower()
                    if not isinstance(raw_symbols, list):
                        continue
                    failed_keys.update(
                        (str(symbol or '').strip().upper(), role)
                        for symbol in raw_symbols
                        if str(symbol or '').strip()
                    )
            failed_count = max(0, int(child.get('failed_count') or 0))
            if child.get('status') == 'failed' or failed_count > len(
                failed_keys
            ):
                return all_child_keys
            return failed_keys & all_child_keys

        terminal_child_statuses = {
            'completed',
            'completed_with_failures',
            'partial',
            'failed',
        }
        for key, child in latest_child_by_key.items():
            symbol, role = key
            status = child['status']
            stale_running = (
                status == 'running'
                and agent_context_feedback_run_state_service._is_stale_running_row(
                    child
                )
            )
            if status in {'abandoned', 'failed'} or stale_running:
                failed_by_role[role].add(symbol)
                continue
            if key in failed_scope_keys_for_child(child):
                failed_by_role[role].add(symbol)
                continue
            if status not in terminal_child_statuses:
                continue
            evidence_kind = latest_evidence_kind(
                key=key,
                required_run_id=child['run_id'],
            )
            if evidence_kind == 'completed':
                completed_by_role[role].add(symbol)
            else:
                # A terminal child without its own success receipt is an
                # incomplete result, even if an older sibling succeeded.
                failed_by_role[role].add(symbol)
        return completed_by_role, failed_by_role

    @staticmethod
    def _reuse_feedback_evidence(decision: Any) -> dict[str, Any] | None:
        latest_feedback = getattr(decision, 'latest_feedback', None)
        if not isinstance(latest_feedback, dict):
            return None
        feedback_run_id = str(latest_feedback.get('run_id') or '').strip()
        feedback_received_at = str(
            getattr(decision, 'latest_feedback_received_at', None)
            or latest_feedback.get('received_at')
            or ''
        ).strip()
        if not feedback_run_id or not feedback_received_at:
            return None
        feedback_id = getattr(decision, 'latest_feedback_id', None)
        return {
            'schema_version': REUSE_FEEDBACK_EVIDENCE_SCHEMA_VERSION,
            'feedback_id': int(feedback_id) if feedback_id is not None else None,
            'feedback_run_id': feedback_run_id,
            'feedback_received_at': feedback_received_at,
            'decision_snapshot_id': getattr(
                decision,
                'latest_decision_snapshot_id',
                None,
            ),
            'decision_snapshot_fingerprint': getattr(
                decision,
                'latest_decision_snapshot_fingerprint',
                None,
            ),
            'decision_snapshot_observed_at': getattr(
                decision,
                'latest_decision_snapshot_observed_at',
                None,
            ),
        }

    def _load_reuse_lineage_context(
        self,
        *,
        market_date: date,
        roles: Sequence[str],
        symbols: Sequence[str],
        current_parent_run_id: str,
        current_parent_started_at: datetime,
    ) -> tuple[
        dict[tuple[str, str], tuple[str, str]],
        dict[str, str | None],
        dict[tuple[str, str, str], tuple[str, str]],
    ]:
        normalized_roles = {
            str(role or '').strip().lower()
            for role in roles
            if str(role or '').strip()
        }
        normalized_symbols = {
            str(symbol or '').strip().upper()
            for symbol in symbols
            if str(symbol or '').strip()
        }
        parent_started_at = current_parent_started_at
        if parent_started_at.tzinfo is None or parent_started_at.utcoffset() is None:
            parent_started_at = parent_started_at.replace(tzinfo=timezone.utc)
        else:
            parent_started_at = parent_started_at.astimezone(timezone.utc)

        with session_scope() as session:
            persisted_rows = session.execute(
                select(AgentContextFeedbackRun)
                .where(AgentContextFeedbackRun.market_date == market_date)
                .order_by(
                    AgentContextFeedbackRun.started_at.desc(),
                    AgentContextFeedbackRun.id.desc(),
                )
            ).scalars().all()
            rows = [
                {
                    'id': int(row.id),
                    'run_id': str(row.run_id or ''),
                    'request_payload': (
                        dict(row.request_payload)
                        if isinstance(row.request_payload, dict)
                        else {}
                    ),
                    'status': str(row.status or '').strip().lower(),
                    'market_date': row.market_date,
                    'symbols_total': row.symbols_total,
                    'completed_count': row.completed_count,
                    'failed_count': max(0, int(row.failed_count or 0)),
                    'summary_payload': (
                        dict(row.summary_payload)
                        if isinstance(row.summary_payload, dict)
                        else {}
                    ),
                    'started_at': row.started_at,
                    'completed_at': row.completed_at,
                }
                for row in persisted_rows
            ]

        def scopes_for_payload(payload: dict[str, Any]) -> dict[str, list[str]]:
            scopes: dict[str, list[str]] = {}
            raw_scopes = payload.get('role_symbol_scopes')
            if isinstance(raw_scopes, dict):
                for raw_role, raw_scope in raw_scopes.items():
                    role = str(raw_role or '').strip().lower()
                    if role not in normalized_roles or not isinstance(raw_scope, list):
                        continue
                    scopes[role] = _normalize_optional_symbol_scope(raw_scope)
            if scopes:
                return scopes
            raw_roles = payload.get('roles')
            raw_symbols = (
                payload.get('requested_symbols')
                if isinstance(payload.get('requested_symbols'), list)
                else payload.get('symbols')
            )
            if not isinstance(raw_roles, list) or not isinstance(raw_symbols, list):
                return {}
            fallback_symbols = _normalize_optional_symbol_scope(raw_symbols)
            return {
                str(raw_role or '').strip().lower(): fallback_symbols
                for raw_role in raw_roles
                if str(raw_role or '').strip().lower() in normalized_roles
            }

        def lease_loss_parent_is_reusable(row: dict[str, Any]) -> bool:
            payload = row.get('request_payload')
            if not isinstance(payload, dict):
                return False
            raw_cutoff = payload.get('knowledge_cutoff_at')
            try:
                normalized_cutoff = _normalize_admission_knowledge_cutoff(
                    raw_cutoff
                )
                knowledge_cutoff_at = (
                    datetime.fromisoformat(normalized_cutoff)
                    if normalized_cutoff is not None
                    else None
                )
            except (TypeError, ValueError):
                return False
            if knowledge_cutoff_at is None:
                return False
            return terminal_feedback_lease_loss_evidence_error(
                child_run=row,
                child_run_id=str(row.get('run_id') or '').strip(),
                market_date=market_date,
                metadata={
                    'symbols': payload.get('requested_symbols'),
                    'fs_div': payload.get('fs_div'),
                    'reuse_existing_feedback': payload.get(
                        'reuse_existing'
                    ),
                    'force': payload.get('force_recompute'),
                },
                knowledge_cutoff_at=knowledge_cutoff_at,
            ) is None

        parent_by_child_run_id: dict[str, str | None] = {}
        latest_parent_by_key: dict[tuple[str, str], tuple[str, str]] = {}
        latest_child_by_parent_key: dict[
            tuple[str, str, str],
            tuple[str, str],
        ] = {}
        current_parent_row_id = next(
            (
                int(row['id'])
                for row in rows
                if str(row['run_id'] or '').strip() == current_parent_run_id
            ),
            None,
        )

        def failed_scope_keys_for_run(
            *,
            row: dict[str, Any],
            scopes: dict[str, list[str]],
        ) -> set[tuple[str, str]]:
            all_scope_keys = {
                (symbol, role)
                for role, scoped_symbols in scopes.items()
                for symbol in scoped_symbols
            }
            summary_payload = row.get('summary_payload')
            containers = (
                [summary_payload]
                if isinstance(summary_payload, dict)
                else []
            )
            progress_summary = (
                summary_payload.get('progress_summary')
                if isinstance(summary_payload, dict)
                else None
            )
            if isinstance(progress_summary, dict):
                containers.append(progress_summary)
            failed_keys: set[tuple[str, str]] = set()
            for container in containers:
                raw_by_role = container.get('failed_symbols_by_role')
                if not isinstance(raw_by_role, dict):
                    continue
                for raw_role, raw_symbols in raw_by_role.items():
                    role = str(raw_role or '').strip().lower()
                    if role not in normalized_roles or not isinstance(
                        raw_symbols,
                        list,
                    ):
                        continue
                    failed_keys.update(
                        (str(symbol or '').strip().upper(), role)
                        for symbol in raw_symbols
                        if str(symbol or '').strip()
                    )
            if row.get('status') == 'failed' or int(
                row.get('failed_count') or 0
            ) > len(failed_keys):
                return all_scope_keys
            return failed_keys & all_scope_keys

        for row in rows:
            run_id = str(row['run_id'] or '').strip()
            payload = row['request_payload']
            parent_by_child_run_id[run_id] = str(
                payload.get('parent_run_id') or ''
            ).strip() or None
            child_parent_run_id = parent_by_child_run_id[run_id]
            scopes = scopes_for_payload(payload)
            status = str(row['status'] or '').strip().lower()
            if child_parent_run_id:
                failed_scope_keys = failed_scope_keys_for_run(
                    row=row,
                    scopes=scopes,
                )
                for role, scoped_symbols in scopes.items():
                    for symbol in scoped_symbols:
                        child_key = (child_parent_run_id, symbol, role)
                        if (
                            symbol in normalized_symbols
                            and child_key not in latest_child_by_parent_key
                        ):
                            latest_child_by_parent_key[child_key] = (
                                run_id,
                                'failed'
                                if status
                                in {
                                    'completed',
                                    'completed_with_failures',
                                    'partial',
                                    'failed',
                                }
                                and (symbol, role) in failed_scope_keys
                                else status,
                            )

            if run_id == current_parent_run_id:
                continue
            row_started_at = row['started_at']
            if row_started_at.tzinfo is None or row_started_at.utcoffset() is None:
                row_started_at = row_started_at.replace(tzinfo=timezone.utc)
            else:
                row_started_at = row_started_at.astimezone(timezone.utc)
            if row_started_at > parent_started_at or (
                row_started_at == parent_started_at
                and current_parent_row_id is not None
                and int(row['id']) >= current_parent_row_id
            ):
                continue
            if str(payload.get('mode') or '').strip() != DAILY_RETRY_CYCLE_MODE:
                continue

            failed_parent_scope_keys = failed_scope_keys_for_run(
                row=row,
                scopes=scopes,
            )
            lease_loss_parent_reusable = lease_loss_parent_is_reusable(row)
            for role, scoped_symbols in scopes.items():
                for symbol in scoped_symbols:
                    key = (symbol, role)
                    if symbol in normalized_symbols and key not in latest_parent_by_key:
                        latest_parent_by_key[key] = (
                            run_id,
                            _LEASE_LOSS_REUSABLE_PARENT_STATUS
                            if lease_loss_parent_reusable
                            else (
                                'failed'
                                if key in failed_parent_scope_keys
                                else status
                            ),
                        )

        return (
            latest_parent_by_key,
            parent_by_child_run_id,
            latest_child_by_parent_key,
        )

    def _filter_decider_retry_ready_symbols(
        self,
        *,
        market_date: date,
        symbols: Sequence[str],
        knowledge_cutoff_at: datetime | str | None = None,
        fs_div: str | None = None,
    ) -> tuple[str, ...]:
        normalized_symbols = _normalize_optional_symbol_scope(symbols)
        if not normalized_symbols:
            return ()

        decision_kwargs: dict[str, Any] = {
            'market_date': market_date,
            'symbols': normalized_symbols,
            'roles': SUMMARIZER_AGENT_NAMES,
            'force_recompute': False,
            'fs_div': _normalize_feedback_fs_div(fs_div),
        }
        if knowledge_cutoff_at is not None:
            decision_kwargs['knowledge_cutoff_at'] = knowledge_cutoff_at
        upstream_decisions = agent_context_reuse_service.build_latest_decisions(
            **decision_kwargs
        )
        ready_symbols: list[str] = []
        for symbol in normalized_symbols:
            symbol_decisions = upstream_decisions.get(symbol, {})
            upstream_ready = True
            for role in SUMMARIZER_AGENT_NAMES:
                decision = symbol_decisions.get(role)
                latest_feedback = getattr(decision, 'latest_feedback', None) if decision is not None else None
                has_reusable_feedback = (
                    decision is not None
                    and not bool(getattr(decision, 'should_dispatch', True))
                    and isinstance(latest_feedback, dict)
                )
                if not has_reusable_feedback:
                    upstream_ready = False
                    break
            if upstream_ready:
                ready_symbols.append(symbol)
        return tuple(ready_symbols)

    @staticmethod
    def _classify_role_boundary_status(
        *,
        total_symbols: int,
        completed_count: int,
        failed_count: int,
        pending_count: int,
    ) -> str:
        if total_symbols == 0:
            return 'completed'
        if pending_count > 0:
            return 'incomplete' if (completed_count or failed_count) else 'pending'
        if failed_count == 0:
            return 'completed'
        if completed_count > 0:
            return 'completed_with_failures'
        return 'failed'

    def _build_role_sequence_snapshot(
        self,
        *,
        market_date: date,
        roles: Sequence[str],
        eligible_symbols: Sequence[str],
        role_symbol_scopes: dict[str, Sequence[str]] | None = None,
        force_recompute: bool = False,
        reuse_existing: bool = True,
        parent_run_id: str | None = None,
        parent_started_at: datetime | None = None,
        knowledge_cutoff_at: datetime | str | None = None,
        fs_div: str | None = None,
    ) -> RoleSequenceSnapshot:
        normalized_roles = tuple(roles)
        normalized_symbols = tuple(eligible_symbols)
        normalized_symbol_set = set(normalized_symbols)
        normalized_role_scopes = {
            role: tuple(
                symbol
                for symbol in _normalize_optional_symbol_scope(scope)
                if symbol in normalized_symbol_set
            )
            for role, scope in (role_symbol_scopes or {}).items()
        }
        _, failed_by_role = self._load_same_day_terminal_symbols(
            market_date=market_date,
            roles=normalized_roles,
            symbols=normalized_symbols,
        )
        current_completed_by_role: dict[str, set[str]] = defaultdict(set)
        current_failed_by_role: dict[str, set[str]] = defaultdict(set)
        if parent_run_id:
            current_completed_by_role, current_failed_by_role = (
                self._load_same_day_terminal_symbols(
                    market_date=market_date,
                    roles=normalized_roles,
                    symbols=normalized_symbols,
                    required_parent_run_id=parent_run_id,
                )
            )
        if parent_run_id or force_recompute or not reuse_existing:
            failed_by_role = current_failed_by_role
        decision_kwargs: dict[str, Any] = {
            'market_date': market_date,
            'symbols': normalized_symbols,
            'roles': normalized_roles,
            'force_recompute': force_recompute,
            'fs_div': _normalize_feedback_fs_div(fs_div),
        }
        if knowledge_cutoff_at is not None:
            decision_kwargs['knowledge_cutoff_at'] = knowledge_cutoff_at
        reuse_decisions = (
            agent_context_reuse_service.build_latest_decisions(
                **decision_kwargs
            )
            if reuse_existing and normalized_symbols and normalized_roles
            else {}
        )

        normalized_parent_started_at = parent_started_at
        if (
            normalized_parent_started_at is not None
            and (
                normalized_parent_started_at.tzinfo is None
                or normalized_parent_started_at.utcoffset() is None
            )
        ):
            normalized_parent_started_at = normalized_parent_started_at.replace(
                tzinfo=timezone.utc
            )
        elif normalized_parent_started_at is not None:
            normalized_parent_started_at = normalized_parent_started_at.astimezone(
                timezone.utc
            )

        latest_prior_parent_by_key: dict[
            tuple[str, str],
            tuple[str, str],
        ] = {}
        parent_by_child_run_id: dict[str, str | None] = {}
        latest_child_by_parent_key: dict[
            tuple[str, str, str],
            tuple[str, str],
        ] = {}
        if parent_run_id and normalized_parent_started_at is not None:
            (
                latest_prior_parent_by_key,
                parent_by_child_run_id,
                latest_child_by_parent_key,
            ) = self._load_reuse_lineage_context(
                market_date=market_date,
                roles=normalized_roles,
                symbols=normalized_symbols,
                current_parent_run_id=parent_run_id,
                current_parent_started_at=normalized_parent_started_at,
            )

        def decision_is_reusable(symbol: str, role: str) -> bool:
            if force_recompute or not reuse_existing:
                return False
            if parent_run_id and (
                parent_run_id,
                symbol,
                role,
            ) in latest_child_by_parent_key:
                # Once the current parent dispatched this scope, that child is
                # the authority.  Pending, abandoned, or malformed children
                # must not fall back to an older persisted success.
                return False
            decision = reuse_decisions.get(symbol, {}).get(role)
            if decision is None:
                return False
            reuse_evidence = self._reuse_feedback_evidence(decision)
            failure_shadowed_by_latest_lineage = bool(
                decision.should_dispatch
                and str(getattr(decision, 'reason', '') or '')
                == 'latest_failure_pending_retry'
                and parent_run_id
                and normalized_parent_started_at is not None
            )
            if bool(decision.should_dispatch) and not failure_shadowed_by_latest_lineage:
                return False
            if parent_run_id and reuse_evidence is None:
                return False
            if (
                parent_run_id
                and role in DECISION_SUPPORT_AGENT_NAMES
                and not (reuse_evidence or {}).get('decision_snapshot_id')
                and not (reuse_evidence or {}).get(
                    'decision_snapshot_fingerprint'
                )
            ):
                return False
            if normalized_parent_started_at is None:
                return True
            raw_received_at = getattr(
                decision,
                'latest_feedback_received_at',
                None,
            )
            if not raw_received_at:
                return False
            try:
                received_at = datetime.fromisoformat(
                    str(raw_received_at).replace('Z', '+00:00')
                )
            except ValueError:
                return False
            if received_at.tzinfo is None or received_at.utcoffset() is None:
                received_at = received_at.replace(tzinfo=timezone.utc)
            else:
                received_at = received_at.astimezone(timezone.utc)
            if received_at > normalized_parent_started_at:
                return False

            prior_parent = latest_prior_parent_by_key.get((symbol, role))
            if prior_parent is None:
                return not failure_shadowed_by_latest_lineage
            prior_parent_run_id, prior_parent_status = prior_parent
            if prior_parent_status not in {
                'completed',
                'completed_with_failures',
                'partial',
                _LEASE_LOSS_REUSABLE_PARENT_STATUS,
            }:
                return False
            feedback_run_id = str(
                (reuse_evidence or {}).get('feedback_run_id') or ''
            ).strip()
            prior_child = latest_child_by_parent_key.get(
                (prior_parent_run_id, symbol, role)
            )
            return (
                parent_by_child_run_id.get(feedback_run_id)
                == prior_parent_run_id
                and prior_child is not None
                and prior_child[0] == feedback_run_id
                and prior_child[1]
                in {
                    'completed',
                    'completed_with_failures',
                    'partial',
                }
            )

        role_states: dict[str, RoleSequenceRoleState] = {}
        for role in normalized_roles:
            role_symbols = normalized_role_scopes.get(role, normalized_symbols)
            current_completed_symbols = current_completed_by_role.get(role, set())
            current_failed_symbols = current_failed_by_role.get(role, set())
            reused_symbols = tuple(
                symbol
                for symbol in role_symbols
                if symbol not in current_completed_symbols
                and symbol not in current_failed_symbols
                and decision_is_reusable(symbol, role)
            )
            reused_feedback_evidence = {
                symbol: evidence
                for symbol in reused_symbols
                if (
                    evidence := self._reuse_feedback_evidence(
                        reuse_decisions[symbol][role]
                    )
                ) is not None
            }
            completed_symbols = tuple(
                symbol
                for symbol in role_symbols
                if symbol in current_completed_symbols
                or symbol in set(reused_symbols)
            )
            failed_symbols = tuple(
                symbol
                for symbol in role_symbols
                if symbol in failed_by_role.get(role, set())
                and symbol not in set(completed_symbols)
            )
            stale_symbols = tuple(
                symbol
                for symbol in role_symbols
                if (reuse_decisions.get(symbol, {}).get(role) is not None)
                and reuse_decisions[symbol][role].should_dispatch
                and str(getattr(reuse_decisions[symbol][role], 'reason', '') or '')
                in {
                    'forced_recompute',
                    'fundamental_input_fingerprint_changed',
                    'fundamental_input_fingerprint_missing',
                    'knowledge_cutoff_mismatch',
                    'stored_fundamental_input_fingerprint_missing',
                    'source_updated',
                    'stale_market_feedback',
                }
            )
            terminal_symbol_set = set(completed_symbols) | set(failed_symbols)
            boundary_pending_symbols = tuple(symbol for symbol in role_symbols if symbol not in terminal_symbol_set)
            incomplete_symbols = set(
                self.load_incomplete_symbols(
                    market_date=market_date,
                    roles=[role],
                    eligible_symbols=role_symbols,
                    force_recompute=force_recompute,
                    reuse_existing=reuse_existing,
                    knowledge_cutoff_at=knowledge_cutoff_at,
                    fs_div=fs_div,
                )
            )
            retry_pending_symbols = tuple(
                symbol
                for symbol in role_symbols
                if symbol not in set(completed_symbols)
                and (
                    symbol in incomplete_symbols
                    or symbol in current_failed_symbols
                )
            )
            if role == SIGNAL_DECIDER_AGENT_NAME and not boundary_pending_symbols:
                retry_pending_symbols = self._filter_decider_retry_ready_symbols(
                    market_date=market_date,
                    symbols=failed_symbols,
                    knowledge_cutoff_at=knowledge_cutoff_at,
                    fs_div=fs_div,
                )
            role_states[role] = RoleSequenceRoleState(
                role=role,
                total_symbols=len(role_symbols),
                completed_symbols=completed_symbols,
                failed_symbols=failed_symbols,
                reused_symbols=reused_symbols,
                stale_symbols=stale_symbols,
                terminal_symbols=tuple(symbol for symbol in role_symbols if symbol in terminal_symbol_set),
                boundary_pending_symbols=boundary_pending_symbols,
                retry_pending_symbols=retry_pending_symbols,
                boundary_status=self._classify_role_boundary_status(
                    total_symbols=len(role_symbols),
                    completed_count=len(completed_symbols),
                    failed_count=len(failed_symbols),
                    pending_count=len(boundary_pending_symbols),
                ),
                reused_feedback_evidence=reused_feedback_evidence,
            )

        return RoleSequenceSnapshot(
            market_date=market_date,
            symbols=normalized_symbols,
            roles=normalized_roles,
            role_states=role_states,
            role_symbol_scopes={
                role: normalized_role_scopes.get(role, normalized_symbols)
                for role in normalized_roles
            },
        )

    @staticmethod
    def _extract_prior_cycle_state(previous_run: dict[str, Any] | None) -> tuple[int, int]:
        if not isinstance(previous_run, dict):
            return 0, 0
        summary_payload = previous_run.get('summary_payload') if isinstance(previous_run.get('summary_payload'), dict) else {}
        progress_summary = summary_payload.get('progress_summary') if isinstance(summary_payload.get('progress_summary'), dict) else {}
        previous_cycle_number = int(progress_summary.get('cycle_number') or summary_payload.get('cycle_number') or 0)
        previous_completed_cycles = int(progress_summary.get('completed_cycles') or summary_payload.get('completed_cycles') or 0)
        return previous_cycle_number, previous_completed_cycles

    def _resolve_cycle_plan(
        self,
        *,
        snapshot: RoleSequenceSnapshot,
        max_cycles: int,
        previous_cycle_number: int,
        previous_completed_cycles: int,
        enforce_max_cycles: bool = True,
        retry_attempted_symbols_by_role: dict[str, set[str]] | None = None,
    ) -> RetryCyclePlan | None:
        boundary_pending_roles = snapshot.boundary_pending_roles()
        if boundary_pending_roles:
            cycle_number = previous_cycle_number or 1
            if enforce_max_cycles and cycle_number > max_cycles:
                return None
            current_role = boundary_pending_roles[0]
            current_index = snapshot.roles.index(current_role)
            return RetryCyclePlan(
                cycle_number=cycle_number,
                completed_cycles=max(0, min(previous_completed_cycles, cycle_number - 1)),
                mode='boundary_resume',
                current_role=current_role,
                plan_roles=tuple(snapshot.roles[current_index:]),
                completed_roles=tuple(snapshot.roles[:current_index]),
                remaining_roles=tuple(snapshot.roles[current_index:]),
            )

        retry_pending_roles = self._retry_pending_roles(
            snapshot,
            retry_attempted_symbols_by_role=retry_attempted_symbols_by_role,
        )
        if not retry_pending_roles:
            return None

        cycle_number = max(1, previous_completed_cycles + 1)
        if enforce_max_cycles and cycle_number > max_cycles:
            return None
        return RetryCyclePlan(
            cycle_number=cycle_number,
            completed_cycles=max(0, cycle_number - 1),
            mode='retry_followup',
            current_role=retry_pending_roles[0],
            plan_roles=tuple(retry_pending_roles),
            completed_roles=tuple(role for role in snapshot.roles if role not in retry_pending_roles),
            remaining_roles=tuple(retry_pending_roles),
        )

    def _sync_runtime_state(
        self,
        *,
        market_date: date,
        run_id: str,
        snapshot: RoleSequenceSnapshot,
        current_role: str | None,
        completed_roles: Sequence[str],
        remaining_roles: Sequence[str],
        cycle_number: int | None,
        completed_cycles: int | None,
        max_cycles: int,
        cycle_mode: str | None,
        cycle_status: str,
        status: str,
        funnel_diagnostics: dict[str, Any] | None = None,
    ) -> RuntimeFeedbackRunState | None:
        role_symbol_scopes = {
            role: list(scope)
            for role, scope in snapshot.role_symbol_scopes.items()
        } if snapshot.role_symbol_scopes else {
            role: list(snapshot.symbols)
            for role in snapshot.roles
        }

        def _update(current: RuntimeFeedbackRunState | None) -> None:
            if current is None:
                return
            current.status = status
            current.current_role = current_role
            current.completed_roles = list(completed_roles)
            current.remaining_roles = list(remaining_roles)
            current.cycle_number = cycle_number
            current.completed_cycles = completed_cycles
            current.max_cycles = max_cycles
            current.cycle_mode = cycle_mode
            current.cycle_status = cycle_status
            current.completed_symbols_by_role = {
                role: set(role_state.completed_symbols)
                for role, role_state in snapshot.role_states.items()
                if role_state.completed_symbols
            }
            current.failed_symbols_by_role = {
                role: set(role_state.failed_symbols)
                for role, role_state in snapshot.role_states.items()
                if role_state.failed_symbols
            }
            current.reused_symbols_by_role = {
                role: set(role_state.reused_symbols)
                for role, role_state in snapshot.role_states.items()
                if role_state.reused_symbols
            }
            current.reused_feedback_evidence_by_role = {
                role: {
                    symbol: dict(evidence)
                    for symbol, evidence in role_state.reused_feedback_evidence.items()
                }
                for role, role_state in snapshot.role_states.items()
                if role_state.reused_feedback_evidence
            }
            current.stale_symbols_by_role = {
                role: set(role_state.stale_symbols)
                for role, role_state in snapshot.role_states.items()
                if role_state.stale_symbols
            }
            current.retry_pending_symbols_by_role = {
                role: set(self._retry_symbols_for_role(role_state))
                for role, role_state in snapshot.role_states.items()
                if self._retry_symbols_for_role(role_state)
            }
            for role, role_state in snapshot.role_states.items():
                if role_state.boundary_pending_symbols:
                    continue
                current.first_pass_completed_symbols_by_role.setdefault(
                    role,
                    set(role_state.terminal_symbols),
                )
                current.first_pass_failed_symbols_by_role.setdefault(
                    role,
                    set(role_state.failed_symbols),
                )
                retry_subset = self._retry_symbols_for_role(role_state)
                if retry_subset:
                    current.retry_queued_symbols_by_role.setdefault(role, set(retry_subset))
            current.role_boundary_statuses = {
                role: role_state.boundary_status
                for role, role_state in snapshot.role_states.items()
            }
            current.role_symbol_scopes = role_symbol_scopes
            if funnel_diagnostics is not None:
                current.funnel_diagnostics = funnel_diagnostics
            current.logical_total_symbols = len(snapshot.symbols)
            current.logical_expected_feedback_count = sum(
                len(role_symbol_scopes.get(role, list(snapshot.symbols)))
                for role in snapshot.roles
            )
            current.logical_completed_feedback_count = sum(
                len(role_state.completed_symbols)
                for role_state in snapshot.role_states.values()
            )
            current.logical_failed_items = sum(
                len(role_state.failed_symbols)
                for role_state in snapshot.role_states.values()
            )
            current.logical_completed_symbols = None
            current.logical_in_progress_symbols = None
            current.logical_pending_symbols = None
            current.last_activity_at = datetime.now(timezone.utc)
            current.in_flight_slots_by_role = {}

        return runtime_feedback_state_store.update_run(
            market_date,
            _update,
            run_id=run_id,
            logical=True,
        )

    def _build_run_summary_payload(
        self,
        *,
        market_date: date,
        request_payload: dict[str, Any],
        state: RuntimeFeedbackRunState,
        lifecycle_stage: str,
        cycle_result: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        progress_summary = state.to_progress_summary()
        role_progress = progress_summary.get('role_progress') if isinstance(progress_summary.get('role_progress'), dict) else {}
        return {
            'market_date': market_date.isoformat(),
            'request': request_payload,
            'mode': DAILY_RETRY_CYCLE_MODE,
            'feedback_scope_mode': state.feedback_scope_mode or request_payload.get('feedback_scope_mode'),
            'lifecycle_stage': lifecycle_stage,
            'current_role': progress_summary.get('current_role'),
            'completed_roles': list(progress_summary.get('completed_roles') or []),
            'remaining_roles': list(progress_summary.get('remaining_roles') or []),
            'cycle_number': progress_summary.get('cycle_number'),
            'completed_cycles': progress_summary.get('completed_cycles'),
            'max_cycles': progress_summary.get('max_cycles'),
            'cycle_mode': progress_summary.get('cycle_mode'),
            'cycle_status': progress_summary.get('cycle_status'),
            'role_sequence': list(state.role_sequence),
            'role_statuses': {
                role: role_payload.get('status')
                for role, role_payload in role_progress.items()
                if isinstance(role_payload, dict)
            },
            'role_retry_pending_counts': {
                role: int(role_payload.get('retry_pending_symbols') or 0)
                for role, role_payload in role_progress.items()
                if isinstance(role_payload, dict)
            },
            'reused_symbols_by_role': {
                role: sorted(symbols)
                for role, symbols in state.reused_symbols_by_role.items()
            },
            'reused_feedback_evidence_by_role': {
                role: {
                    symbol: dict(evidence)
                    for symbol, evidence in sorted(symbol_evidence.items())
                }
                for role, symbol_evidence in sorted(
                    state.reused_feedback_evidence_by_role.items()
                )
            },
            'role_symbol_counts': progress_summary.get('role_symbol_counts'),
            'funnel_diagnostics': progress_summary.get('funnel_diagnostics'),
            'progress_summary': progress_summary,
            'cycle_result': cycle_result,
            'resumed_from_run_id': request_payload.get('resumed_from_run_id'),
            'last_activity_at': state.last_activity_at.isoformat() if state.last_activity_at else None,
        }

    def _checkpoint_run_state(
        self,
        *,
        market_date: date,
        request_run_id: str,
        request_payload: dict[str, Any],
        lifecycle_stage: str,
        status: str,
        cycle_result: dict[str, Any] | None = None,
        progress_callback: Callable[..., Any] | None = None,
        claim_lease: _FeedbackRetryClaimLease | None = None,
    ) -> dict[str, Any]:
        if claim_lease is not None:
            claim_lease.assert_owned(
                stage=f'before_checkpoint:{lifecycle_stage}'
            )
        state = runtime_feedback_state_store.get_run(market_date, run_id=request_run_id, logical=True)
        if state is None:
            raise RuntimeError(f'feedback retry runtime state missing for {request_run_id}')
        summary_payload = self._build_run_summary_payload(
            market_date=market_date,
            request_payload=request_payload,
            state=state,
            lifecycle_stage=lifecycle_stage,
            cycle_result=cycle_result,
        )
        completed_count = int(summary_payload['progress_summary'].get('completed_feedback_count') or 0)
        failed_count = int(summary_payload['progress_summary'].get('failed_items') or 0)
        checkpoint = agent_context_feedback_run_state_service.checkpoint_run(
            run_id=request_run_id,
            status=status,
            completed_count=completed_count,
            failed_count=failed_count,
            summary_payload=summary_payload,
        )
        self._emit_progress_callback(
            progress_callback=progress_callback,
            request_run_id=request_run_id,
            lifecycle_stage=lifecycle_stage,
            status=status,
            completed_count=completed_count,
            failed_count=failed_count,
            summary_payload=summary_payload,
            cycle_result=cycle_result,
        )
        return checkpoint

    @staticmethod
    def _emit_progress_callback(
        *,
        progress_callback: Callable[..., Any] | None,
        request_run_id: str,
        lifecycle_stage: str,
        status: str,
        completed_count: int,
        failed_count: int,
        summary_payload: dict[str, Any],
        cycle_result: dict[str, Any] | None,
    ) -> None:
        if progress_callback is None:
            return
        progress_summary = (
            summary_payload.get('progress_summary')
            if isinstance(summary_payload.get('progress_summary'), dict)
            else {}
        )
        total = int(progress_summary.get('expected_feedback_count') or summary_payload.get('expected_feedback_count') or 0)
        processed = min(completed_count + failed_count, total) if total else completed_count + failed_count
        current_role = str(progress_summary.get('current_role') or summary_payload.get('current_role') or '').strip() or None
        cycle_status = str(progress_summary.get('cycle_status') or summary_payload.get('cycle_status') or status).strip()
        activity_role = current_role or 'agent feedback'
        progress_callback(
            status=status,
            current_step=lifecycle_stage,
            current_activity=f'Agent feedback {lifecycle_stage.replace("_", " ")}: {activity_role}',
            processed=processed,
            total=total,
            succeeded=completed_count,
            failed=failed_count,
            remaining=max(total - processed, 0) if total else 0,
            current_period=current_role,
            source_run_ids={'agent_feedback_run_id': request_run_id},
            details={
                'source': 'agent_context_feedback_run_state',
                'run_id': request_run_id,
                'cycle_status': cycle_status,
                'cycle_number': progress_summary.get('cycle_number') or summary_payload.get('cycle_number'),
                'completed_cycles': progress_summary.get('completed_cycles') or summary_payload.get('completed_cycles'),
                'max_cycles': progress_summary.get('max_cycles') or summary_payload.get('max_cycles'),
                'completed_roles': list(progress_summary.get('completed_roles') or summary_payload.get('completed_roles') or []),
                'remaining_roles': list(progress_summary.get('remaining_roles') or summary_payload.get('remaining_roles') or []),
                'expected_feedback_count': total,
                'completed_feedback_count': completed_count,
                'failed_items': failed_count,
                'role_progress': progress_summary.get('role_progress') if isinstance(progress_summary.get('role_progress'), dict) else {},
                'role_symbol_counts': progress_summary.get('role_symbol_counts') or summary_payload.get('role_symbol_counts'),
                'feedback_scope_mode': progress_summary.get('feedback_scope_mode') or summary_payload.get('feedback_scope_mode'),
                'current_role': current_role,
                'cycle_result': cycle_result,
            },
        )

    @staticmethod
    def _retry_symbols_for_role(
        role_state: RoleSequenceRoleState,
        *,
        attempted_symbols: set[str] | None = None,
    ) -> tuple[str, ...]:
        attempted = attempted_symbols or set()
        if role_state.role == SIGNAL_DECIDER_AGENT_NAME and not role_state.boundary_pending_symbols:
            retry_ready = set(role_state.retry_pending_symbols)
            return tuple(
                symbol for symbol in role_state.failed_symbols
                if symbol in retry_ready and symbol not in attempted
            )
        return tuple(symbol for symbol in role_state.retry_pending_symbols if symbol not in attempted)

    def _retry_pending_roles(
        self,
        snapshot: RoleSequenceSnapshot,
        *,
        retry_attempted_symbols_by_role: dict[str, set[str]] | None = None,
    ) -> list[str]:
        return [
            role
            for role in snapshot.roles
            if self._retry_symbols_for_role(
                snapshot.role_states[role],
                attempted_symbols=(retry_attempted_symbols_by_role or {}).get(role),
            )
        ]

    def _collect_retry_pending_symbols(
        self,
        snapshot: RoleSequenceSnapshot,
        *,
        retry_attempted_symbols_by_role: dict[str, set[str]] | None = None,
    ) -> list[str]:
        ordered: list[str] = []
        seen: set[str] = set()
        for role in snapshot.roles:
            for symbol in self._retry_symbols_for_role(
                snapshot.role_states[role],
                attempted_symbols=(retry_attempted_symbols_by_role or {}).get(role),
            ):
                if symbol in seen:
                    continue
                seen.add(symbol)
                ordered.append(symbol)
        return ordered

    @staticmethod
    def _mark_retry_attempted_symbols(
        *,
        market_date: date,
        run_id: str,
        role: str,
        symbols: Sequence[str],
    ) -> None:
        def _mark_retry_attempted(current: RuntimeFeedbackRunState) -> None:
            current.retry_attempted_symbols_by_role.setdefault(role, set()).update(symbols)
            current.last_activity_at = datetime.now(timezone.utc)

        runtime_feedback_state_store.update_run(
            market_date,
            _mark_retry_attempted,
            run_id=run_id,
            logical=True,
        )

    @staticmethod
    def _collect_terminal_items(snapshot: RoleSequenceSnapshot) -> int:
        return sum(
            len(role_state.terminal_symbols)
            for role_state in snapshot.role_states.values()
        )

    @staticmethod
    def _resolve_final_status(
        *,
        eligible_symbols: Sequence[str],
        snapshot: RoleSequenceSnapshot,
        retry_pending_symbols: Sequence[str],
        boundary_pending_roles: Sequence[str],
    ) -> str:
        if not eligible_symbols:
            return 'completed'

        failed_items = sum(
            len(role_state.failed_symbols)
            for role_state in snapshot.role_states.values()
        )
        terminal_items = AgentFeedbackRetryService._collect_terminal_items(snapshot)
        completed_items = max(terminal_items - failed_items, 0)
        if retry_pending_symbols or boundary_pending_roles:
            return 'partial' if terminal_items > 0 else 'failed'
        if failed_items > 0:
            return 'partial' if completed_items > 0 else 'failed'
        return 'completed'

    @staticmethod
    def _symbol_chunks(symbols: Sequence[str], *, batch_size: int) -> list[list[str]]:
        resolved_batch_size = max(1, int(batch_size or 1))
        return [
            list(symbols[index:index + resolved_batch_size])
            for index in range(0, len(symbols), resolved_batch_size)
        ]

    @staticmethod
    def _aggregate_dispatch_summaries(dispatch_results: Sequence[dict[str, Any]]) -> dict[str, Any]:
        summaries = [
            result.get('summary') if isinstance(result.get('summary'), dict) else {}
            for result in dispatch_results
        ]
        completed = sum(int(summary.get('completed') or 0) for summary in summaries)
        failed = sum(int(summary.get('failed') or 0) for summary in summaries)
        statuses = {str(summary.get('status') or '').strip().lower() for summary in summaries}
        status = 'partial' if failed > 0 or statuses.intersection({'failed', 'partial', 'blocked'}) else 'completed'
        return {
            'status': status,
            'completed': completed,
            'failed': failed,
            'chunks': len(dispatch_results),
        }

    def run_retry_cycles(
        self,
        *,
        config: FeedbackRetryCycleConfig,
        force: bool = False,
        bypass_prerequisite_gate: bool = False,
    ) -> dict[str, Any]:
        roles = tuple(config.roles)
        resolved_fs_div = _normalize_feedback_fs_div(
            getattr(config, 'fs_div', None)
        )
        raw_requested_symbols = getattr(config, 'symbols', None)
        requested_symbols = (
            tuple(_normalize_requested_symbols(raw_requested_symbols))
            if raw_requested_symbols is not None
            else None
        )
        feedback_scope_mode = normalize_feedback_scope_mode(
            getattr(config, 'feedback_scope_mode', None)
        )
        raw_knowledge_cutoff_at = getattr(config, 'knowledge_cutoff_at', None)
        effective_knowledge_cutoff_at: datetime | None = None
        knowledge_cutoff_source: str | None = None
        if requested_symbols is not None or raw_knowledge_cutoff_at is not None:
            (
                effective_knowledge_cutoff_at,
                knowledge_cutoff_source,
            ) = resolve_external_news_knowledge_cutoff_for_symbols(
                market_date=config.market_date,
                symbols=requested_symbols,
                knowledge_cutoff_at=raw_knowledge_cutoff_at,
            )
        should_bypass_prerequisite_gate = bool(
            force or bypass_prerequisite_gate
        )
        prerequisites = (
            agent_feedback_prerequisite_service.inspect_feedback_prerequisites(
                market_date=config.market_date,
                fs_div=resolved_fs_div,
            )
        )
        if not should_bypass_prerequisite_gate and not prerequisites['ready']:
            return self._run_retry_cycles_admitted(
                config=config,
                force=force,
                bypass_prerequisite_gate=bypass_prerequisite_gate,
                prerequisites=prerequisites,
            )

        eligible_symbols = self._load_eligible_symbols(
            market_date=config.market_date,
            requested_symbols=requested_symbols,
        )
        if effective_knowledge_cutoff_at is None:
            (
                effective_knowledge_cutoff_at,
                knowledge_cutoff_source,
            ) = resolve_external_news_knowledge_cutoff_for_symbols(
                market_date=config.market_date,
                symbols=eligible_symbols,
                knowledge_cutoff_at=None,
            )
        requested_knowledge_cutoff_at = (
            effective_knowledge_cutoff_at.isoformat()
            if raw_knowledge_cutoff_at is not None
            else None
        )

        previous_run = None
        if not bool(force or getattr(config, 'force_recompute', False)):
            previous_run = agent_context_feedback_run_state_service.find_latest_run(
                market_date=config.market_date,
                requested_symbols=(
                    list(requested_symbols)
                    if requested_symbols is not None
                    else None
                ),
                requested_roles=list(roles),
                request_modes=[DAILY_RETRY_CYCLE_MODE],
                feedback_scope_mode=feedback_scope_mode,
            )
            if not _previous_run_matches_knowledge_cutoff(
                previous_run,
                effective_knowledge_cutoff_at,
            ) or not _previous_run_matches_fs_div(
                previous_run,
                resolved_fs_div,
            ):
                previous_run = None
        request_run_id = (
            f"feedback-cycle-{config.market_date.isoformat()}-"
            f"{uuid4().hex[:10]}"
        )
        parent_started_at = datetime.now(timezone.utc)
        admission = _claim_feedback_retry_run(
            market_date=config.market_date,
            roles=roles,
            requested_symbols=requested_symbols,
            feedback_scope_mode=feedback_scope_mode,
            run_id=request_run_id,
            started_at=parent_started_at,
            admission_knowledge_cutoff_at=(
                effective_knowledge_cutoff_at.isoformat()
            ),
            fs_div=resolved_fs_div,
        )
        if not admission['acquired']:
            active_run = admission.get('conflict_run')
            active_run_id = (
                str(active_run.get('run_id') or '').strip()
                if isinstance(active_run, dict)
                else ''
            )
            progress = self._get_retry_progress(
                market_date=config.market_date,
                roles=roles,
                symbols=(
                    list(requested_symbols)
                    if requested_symbols is not None
                    else None
                ),
                run_id=active_run_id or None,
            )
            pending = int(
                (progress.get('summary') or {}).get('pending_symbols') or 0
            )
            return {
                'market_date': config.market_date.isoformat(),
                'roles': list(roles),
                'max_cycles': int(getattr(config, 'max_cycles', 3) or 3),
                'outer_batch_size': int(
                    getattr(config, 'outer_batch_size', 20) or 20
                ),
                'reuse_existing': bool(
                    getattr(config, 'reuse_existing', True)
                ),
                'force_recompute': bool(
                    force or getattr(config, 'force_recompute', False)
                ),
                'fs_div': resolved_fs_div,
                'feedback_scope_mode': feedback_scope_mode,
                'requested_symbols': (
                    list(requested_symbols)
                    if requested_symbols is not None
                    else None
                ),
                'eligible_symbols': None,
                'skipped_symbols': [],
                'run_id': active_run_id or None,
                'status': 'running' if active_run_id else 'waiting',
                'gated': not bool(admission.get('exact_scope')),
                'cycles': [],
                'final_pending': pending,
                'completed': False,
                'joined_existing_run': bool(
                    active_run_id and admission.get('exact_scope')
                ),
                'admission_status': (
                    'active_scope_joined'
                    if admission.get('exact_scope')
                    else 'overlapping_scope_active'
                ),
                'progress': progress,
            }

        owner_token = str(admission.get('owner_token') or '').strip()
        if not owner_token:
            raise RuntimeError(
                f'feedback retry durable admission owner token missing for {request_run_id}'
            )
        try:
            with _feedback_retry_claim_lease(
                run_id=request_run_id,
                owner_token=owner_token,
            ) as claim_lease:
                result = self._run_retry_cycles_admitted(
                    config=config,
                    force=force,
                    bypass_prerequisite_gate=bypass_prerequisite_gate,
                    request_run_id=request_run_id,
                    parent_started_at=parent_started_at,
                    prerequisites=prerequisites,
                    previous_run=previous_run,
                    claim_lease=claim_lease,
                    preloaded_eligible_symbols=eligible_symbols,
                    effective_knowledge_cutoff_at=(
                        effective_knowledge_cutoff_at
                    ),
                    knowledge_cutoff_source=knowledge_cutoff_source,
                    requested_knowledge_cutoff_at=(
                        requested_knowledge_cutoff_at
                    ),
                )
        except Exception as exc:
            _finish_feedback_retry_claim_if_running(
                run_id=request_run_id,
                owner_token=owner_token,
                status='failed',
                error=exc,
            )
            raise
        _finish_feedback_retry_claim_if_running(
            run_id=request_run_id,
            owner_token=owner_token,
            status=str(result.get('status') or ''),
        )
        return result

    def _run_retry_cycles_admitted(
        self,
        *,
        config: FeedbackRetryCycleConfig,
        force: bool = False,
        bypass_prerequisite_gate: bool = False,
        request_run_id: str | None = None,
        parent_started_at: datetime | None = None,
        prerequisites: dict[str, Any] | None = None,
        previous_run: dict[str, Any] | None = None,
        claim_lease: _FeedbackRetryClaimLease | None = None,
        preloaded_eligible_symbols: Sequence[str] | None = None,
        effective_knowledge_cutoff_at: datetime | None = None,
        knowledge_cutoff_source: str | None = None,
        requested_knowledge_cutoff_at: str | None = None,
    ) -> dict[str, Any]:
        roles = tuple(config.roles)
        resolved_fs_div = _normalize_feedback_fs_div(
            getattr(config, 'fs_div', None)
        )
        progress_callback = getattr(config, 'progress_callback', None)
        feedback_scope_mode = normalize_feedback_scope_mode(getattr(config, 'feedback_scope_mode', None))
        effective_force_recompute = bool(force or getattr(config, 'force_recompute', False))
        should_bypass_prerequisite_gate = bool(force or bypass_prerequisite_gate)
        raw_requested_symbols = getattr(config, 'symbols', None)
        requested_symbols = (
            tuple(_normalize_requested_symbols(raw_requested_symbols))
            if raw_requested_symbols is not None
            else None
        )
        prerequisites = prerequisites or (
            agent_feedback_prerequisite_service.inspect_feedback_prerequisites(
                market_date=config.market_date,
                fs_div=resolved_fs_div,
            )
        )
        if not should_bypass_prerequisite_gate and not prerequisites['ready']:
            progress = self._get_retry_progress(
                market_date=config.market_date,
                roles=roles,
                symbols=list(requested_symbols) if requested_symbols is not None else None,
                run_id=None,
            )
            return {
                'market_date': config.market_date.isoformat(),
                'roles': list(roles),
                'max_cycles': config.max_cycles,
                'outer_batch_size': config.outer_batch_size,
                'reuse_existing': config.reuse_existing,
                'force_recompute': effective_force_recompute,
                'feedback_scope_mode': feedback_scope_mode,
                'requested_symbols': list(requested_symbols) if requested_symbols is not None else None,
                'eligible_symbols': None,
                'skipped_symbols': [],
                'run_id': None,
                'status': prerequisites['status'],
                'gated': True,
                'cycles': [],
                'final_pending': int(progress.get('summary', {}).get('pending_symbols') or 0),
                'completed': False,
                'prerequisites': prerequisites,
                'progress': progress,
            }
        eligible_symbols = (
            list(preloaded_eligible_symbols)
            if preloaded_eligible_symbols is not None
            else self._load_eligible_symbols(
                market_date=config.market_date,
                requested_symbols=requested_symbols,
            )
        )
        eligibility_records = load_agent_feedback_eligibility_records(
            market_date=config.market_date,
            requested_symbols=requested_symbols,
        )
        eligibility_diagnostics = build_agent_feedback_eligibility_diagnostics(
            records=eligibility_records,
            requested_symbols=requested_symbols,
        )
        role_funnel = build_agent_feedback_role_funnel(
            records=eligibility_records,
            roles=roles,
            requested_symbols=requested_symbols,
            candidate_symbols=eligible_symbols,
            feedback_scope_mode=feedback_scope_mode,
        )
        role_symbol_scopes = {
            role: tuple(symbols)
            for role, symbols in role_funnel.get('role_symbols', {}).items()
            if isinstance(symbols, list)
        }
        scoped_role_symbol_total = sum(
            len(role_symbol_scopes.get(role, ()))
            for role in roles
        )
        scoped_feedback_symbols = _ordered_symbol_union(
            [role_symbol_scopes.get(role, ()) for role in roles]
        ) or list(eligible_symbols)
        if effective_knowledge_cutoff_at is None:
            raw_knowledge_cutoff_at = getattr(
                config,
                'knowledge_cutoff_at',
                None,
            )
            (
                effective_knowledge_cutoff_at,
                knowledge_cutoff_source,
            ) = resolve_external_news_knowledge_cutoff_for_symbols(
                market_date=config.market_date,
                symbols=(
                    requested_symbols
                    if requested_symbols is not None
                    else scoped_feedback_symbols
                ),
                knowledge_cutoff_at=raw_knowledge_cutoff_at,
            )
            requested_knowledge_cutoff_at = (
                effective_knowledge_cutoff_at.isoformat()
                if raw_knowledge_cutoff_at is not None
                else None
            )
        knowledge_cutoff_at = effective_knowledge_cutoff_at.isoformat()
        if scoped_role_symbol_total <= 0:
            scoped_role_symbol_total = len(scoped_feedback_symbols) * max(1, len(roles))
        skipped_symbols = (
            [symbol for symbol in requested_symbols or () if symbol not in set(eligible_symbols)]
            if requested_symbols is not None
            else []
        )
        previous_cycle_number, previous_completed_cycles = self._extract_prior_cycle_state(previous_run)
        request_run_id = request_run_id or (
            f"feedback-cycle-{config.market_date.isoformat()}-"
            f"{uuid4().hex[:10]}"
        )
        parent_started_at = parent_started_at or datetime.now(timezone.utc)
        initial_snapshot = self._build_role_sequence_snapshot(
            market_date=config.market_date,
            roles=roles,
            eligible_symbols=scoped_feedback_symbols,
            role_symbol_scopes=role_symbol_scopes,
            force_recompute=effective_force_recompute,
            reuse_existing=config.reuse_existing,
            parent_run_id=request_run_id,
            parent_started_at=parent_started_at,
            knowledge_cutoff_at=knowledge_cutoff_at,
            fs_div=resolved_fs_div,
        )
        next_plan = self._resolve_cycle_plan(
            snapshot=initial_snapshot,
            max_cycles=config.max_cycles,
            previous_cycle_number=previous_cycle_number,
            previous_completed_cycles=previous_completed_cycles,
        )

        request_payload = {
            'market_date': config.market_date.isoformat(),
            'symbols': list(scoped_feedback_symbols),
            'broad_symbols': list(eligible_symbols),
            'broad_symbol_count': len(eligible_symbols),
            'requested_symbols': list(requested_symbols) if requested_symbols is not None else list(scoped_feedback_symbols),
            'request_run_id': request_run_id,
            'child_run_lineage': {
                'schema_version': CHILD_RUN_LINEAGE_SCHEMA_VERSION,
                'parent_run_id': request_run_id,
                'child_link_field': 'parent_run_id',
            },
            'roles': list(roles),
            'max_cycles': config.max_cycles,
            'outer_batch_size': config.outer_batch_size,
            'fs_div': resolved_fs_div,
            'knowledge_cutoff_at': knowledge_cutoff_at,
            'admission_knowledge_cutoff_at': knowledge_cutoff_at,
            'knowledge_cutoff_source': knowledge_cutoff_source,
            'requested_knowledge_cutoff_at': (
                requested_knowledge_cutoff_at
            ),
            'parallel_subtasks': max(1, int(settings.openclaw_agent_parallel_subtasks)),
            'reuse_existing': config.reuse_existing,
            'force_recompute': effective_force_recompute,
            'scope': resolve_agent_feedback_scope(requested_symbols=requested_symbols),
            'feedback_scope_mode': feedback_scope_mode,
            'eligibility_rule': AGENT_FEEDBACK_ELIGIBILITY_RULE,
            'eligibility_filters': list(AGENT_FEEDBACK_ELIGIBILITY_FILTERS),
            'eligibility_diagnostics': eligibility_diagnostics,
            'funnel_diagnostics': role_funnel,
            'role_symbol_scopes': {
                role: list(symbols)
                for role, symbols in role_symbol_scopes.items()
            },
            'admission_scope_by_role': {
                role: (
                    list(requested_symbols)
                    if requested_symbols is not None
                    else None
                )
                for role in roles
            },
            'retry_admission': {
                'schema_version': RETRY_ADMISSION_SCHEMA_VERSION,
                'claimed_at': parent_started_at.isoformat(),
                'owner_token_hash': (
                    _feedback_retry_owner_token_hash(
                        claim_lease.owner_token
                    )
                    if claim_lease is not None
                    else None
                ),
            },
            'role_symbol_counts': {
                role: len(symbols)
                for role, symbols in role_symbol_scopes.items()
            },
            'skipped_symbols': skipped_symbols,
            'mode': DAILY_RETRY_CYCLE_MODE,
            'resumed_from_run_id': previous_run.get('run_id') if isinstance(previous_run, dict) else None,
        }
        logical_state = RuntimeFeedbackRunState(
            run_id=request_run_id,
            market_date=config.market_date,
            requested_symbols=list(scoped_feedback_symbols),
            requested_roles=list(roles),
            role_sequence=list(roles),
            parallel_per_role=max(1, int(settings.openclaw_agent_parallel_subtasks)),
            batch_size=max(1, config.outer_batch_size),
            started_at=parent_started_at,
            current_role=next_plan.current_role if next_plan is not None else None,
            role_symbol_scopes={
                role: list(symbols)
                for role, symbols in role_symbol_scopes.items()
            },
            funnel_diagnostics=role_funnel,
            feedback_scope_mode=feedback_scope_mode,
        )
        runtime_feedback_state_store.start_run(logical_state, replace=True, logical=True)
        self._sync_runtime_state(
            market_date=config.market_date,
            run_id=request_run_id,
            snapshot=initial_snapshot,
            current_role=next_plan.current_role if next_plan is not None else None,
            completed_roles=next_plan.completed_roles if next_plan is not None else tuple(roles),
            remaining_roles=next_plan.remaining_roles if next_plan is not None else (),
            cycle_number=next_plan.cycle_number if next_plan is not None else previous_completed_cycles,
            completed_cycles=next_plan.completed_cycles if next_plan is not None else previous_completed_cycles,
            max_cycles=config.max_cycles,
            cycle_mode=next_plan.mode if next_plan is not None else None,
            cycle_status='running' if next_plan is not None else 'completed',
            status='running' if next_plan is not None else 'completed',
            funnel_diagnostics=role_funnel,
        )

        state = runtime_feedback_state_store.get_run(config.market_date, run_id=request_run_id, logical=True)
        if state is None:
            raise RuntimeError(f'feedback retry runtime state missing for {request_run_id}')
        if claim_lease is not None:
            claim_lease.assert_owned(stage='before_parent_start')
        agent_context_feedback_run_state_service.start_run(
            run_id=request_run_id,
            market_date=config.market_date,
            symbols_total=scoped_role_symbol_total,
            request_payload=request_payload,
            started_at=parent_started_at,
            summary_payload=self._build_run_summary_payload(
                market_date=config.market_date,
                request_payload=request_payload,
                state=state,
                lifecycle_stage='run_started',
                cycle_result=None,
            ),
        )

        cycles: list[dict[str, Any]] = []
        blocked = False
        active_plan = next_plan
        completed_cycles = next_plan.completed_cycles if next_plan is not None else previous_completed_cycles

        try:
            while active_plan is not None:
                cycle_roles: list[dict[str, Any]] = []
                cycle_state_before = runtime_feedback_state_store.get_run(
                    config.market_date,
                    run_id=request_run_id,
                    logical=True,
                )
                retry_attempted_before = (
                    cycle_state_before.retry_attempted_symbols_by_role
                    if cycle_state_before is not None
                    else {}
                )
                cycle_start_pending = len(
                    self._collect_retry_pending_symbols(
                        initial_snapshot if not cycles else snapshot_after_cycle,
                        retry_attempted_symbols_by_role=retry_attempted_before,
                    )
                )
                self._sync_runtime_state(
                    market_date=config.market_date,
                    run_id=request_run_id,
                    snapshot=initial_snapshot if not cycles else snapshot_after_cycle,
                    current_role=active_plan.current_role,
                    completed_roles=active_plan.completed_roles,
                    remaining_roles=active_plan.remaining_roles,
                    cycle_number=active_plan.cycle_number,
                    completed_cycles=active_plan.completed_cycles,
                    max_cycles=config.max_cycles,
                    cycle_mode=active_plan.mode,
                    cycle_status='running',
                    status='running',
                )
                self._checkpoint_run_state(
                    market_date=config.market_date,
                    request_run_id=request_run_id,
                    request_payload=request_payload,
                    lifecycle_stage='cycle_started',
                    status='running',
                    progress_callback=progress_callback,
                    claim_lease=claim_lease,
                    cycle_result={
                        'cycle': active_plan.cycle_number,
                        'mode': active_plan.mode,
                        'completed_roles': list(active_plan.completed_roles),
                        'remaining_roles': list(active_plan.remaining_roles),
                    },
                )

                for index, role in enumerate(active_plan.plan_roles):
                    snapshot_before_role = self._build_role_sequence_snapshot(
                        market_date=config.market_date,
                        roles=roles,
                        eligible_symbols=scoped_feedback_symbols,
                        role_symbol_scopes=role_symbol_scopes,
                        force_recompute=effective_force_recompute,
                        reuse_existing=config.reuse_existing,
                        parent_run_id=request_run_id,
                        parent_started_at=parent_started_at,
                        knowledge_cutoff_at=knowledge_cutoff_at,
                        fs_div=resolved_fs_div,
                    )
                    completed_roles_before = tuple(
                        list(active_plan.completed_roles) + list(active_plan.plan_roles[:index])
                    )
                    remaining_roles_before = tuple(active_plan.plan_roles[index:])
                    self._sync_runtime_state(
                        market_date=config.market_date,
                        run_id=request_run_id,
                        snapshot=snapshot_before_role,
                        current_role=role,
                        completed_roles=completed_roles_before,
                        remaining_roles=remaining_roles_before,
                        cycle_number=active_plan.cycle_number,
                        completed_cycles=active_plan.completed_cycles,
                        max_cycles=config.max_cycles,
                        cycle_mode=active_plan.mode,
                        cycle_status='running',
                        status='running',
                        funnel_diagnostics=role_funnel,
                    )

                    role_state_before = snapshot_before_role.role_states[role]
                    dispatch_symbols = list(
                        role_symbol_scopes.get(role, scoped_feedback_symbols)
                        if effective_force_recompute
                        else role_state_before.boundary_pending_symbols
                        if active_plan.mode == 'boundary_resume'
                        else self._retry_symbols_for_role(role_state_before)
                    )
                    if active_plan.mode == 'retry_followup' and dispatch_symbols:
                        self._mark_retry_attempted_symbols(
                            market_date=config.market_date,
                            run_id=request_run_id,
                            role=role,
                            symbols=dispatch_symbols,
                        )
                    self._checkpoint_run_state(
                        market_date=config.market_date,
                        request_run_id=request_run_id,
                        request_payload=request_payload,
                        lifecycle_stage='role_started',
                        status='running',
                        progress_callback=progress_callback,
                        claim_lease=claim_lease,
                        cycle_result={
                            'cycle': active_plan.cycle_number,
                            'mode': active_plan.mode,
                            'role': role,
                            'dispatch_symbols': list(dispatch_symbols),
                        },
                    )

                    role_result: dict[str, Any] = {
                        'role': role,
                        'dispatch_symbols': list(dispatch_symbols),
                    }
                    role_exception: Exception | None = None
                    if dispatch_symbols:
                        try:
                            dispatch_results: list[dict[str, Any]] = []
                            dispatch_chunks = self._symbol_chunks(
                                dispatch_symbols,
                                batch_size=config.outer_batch_size,
                            )
                            for chunk_index, dispatch_chunk in enumerate(dispatch_chunks, start=1):
                                if claim_lease is not None:
                                    claim_lease.assert_owned(
                                        stage='before_child_dispatch'
                                    )
                                dispatch_result = agent_feedback_orchestrator.dispatch_for_symbols(
                                    market_date=config.market_date,
                                    symbols=dispatch_chunk,
                                    roles=[role],
                                    fs_div=resolved_fs_div,
                                    knowledge_cutoff_at=knowledge_cutoff_at,
                                    parent_run_id=request_run_id,
                                    # The parent already resolved reuse for this exact
                                    # role/symbol scope. Anything still queued here
                                    # must be recomputed, not reclassified by the child.
                                    reuse_existing=False,
                                    # `bypass_prerequisite_gate` is deliberately
                                    # separate from `force_recompute`, but the child
                                    # orchestrator uses `force` for its own copy of
                                    # the prerequisite gate. Propagate the resolved
                                    # gate-bypass decision so a safe resume is not
                                    # blocked again inside every child batch.
                                    force=should_bypass_prerequisite_gate,
                                    force_recompute=effective_force_recompute,
                                    auto_scope=requested_symbols is None,
                                    role_symbol_scopes={role: dispatch_chunk},
                                    funnel_diagnostics={
                                        **role_funnel,
                                        'role_symbols': {role: list(dispatch_chunk)},
                                        'role_symbol_counts': {role: len(dispatch_chunk)},
                                        'role_diagnostics': {
                                            role: role_funnel.get('role_diagnostics', {}).get(role, {})
                                        },
                                        'dispatch_chunk': {
                                            'index': chunk_index,
                                            'total': len(dispatch_chunks),
                                            'outer_batch_size': max(1, int(config.outer_batch_size or 1)),
                                        },
                                    },
                                )
                                dispatch_results.append(dispatch_result)
                                if chunk_index >= len(dispatch_chunks):
                                    continue
                                snapshot_after_chunk = self._build_role_sequence_snapshot(
                                    market_date=config.market_date,
                                    roles=roles,
                                    eligible_symbols=scoped_feedback_symbols,
                                    role_symbol_scopes=role_symbol_scopes,
                                    force_recompute=effective_force_recompute,
                                    reuse_existing=config.reuse_existing,
                                    parent_run_id=request_run_id,
                                    parent_started_at=parent_started_at,
                                    knowledge_cutoff_at=knowledge_cutoff_at,
                                    fs_div=resolved_fs_div,
                                )
                                self._sync_runtime_state(
                                    market_date=config.market_date,
                                    run_id=request_run_id,
                                    snapshot=snapshot_after_chunk,
                                    current_role=role,
                                    completed_roles=completed_roles_before,
                                    remaining_roles=remaining_roles_before,
                                    cycle_number=active_plan.cycle_number,
                                    completed_cycles=active_plan.completed_cycles,
                                    max_cycles=config.max_cycles,
                                    cycle_mode=active_plan.mode,
                                    cycle_status='running',
                                    status='running',
                                    funnel_diagnostics=role_funnel,
                                )
                                self._checkpoint_run_state(
                                    market_date=config.market_date,
                                    request_run_id=request_run_id,
                                    request_payload=request_payload,
                                    lifecycle_stage='role_batch_completed',
                                    status='running',
                                    progress_callback=progress_callback,
                                    claim_lease=claim_lease,
                                    cycle_result={
                                        'cycle': active_plan.cycle_number,
                                        'mode': active_plan.mode,
                                        'role': role,
                                        'chunk_index': chunk_index,
                                        'chunk_total': len(dispatch_chunks),
                                        'chunk_symbols': list(dispatch_chunk),
                                        'chunk_request': dispatch_result.get('request'),
                                        'chunk_summary': dispatch_result.get('summary'),
                                    },
                                )
                            aggregate_summary = self._aggregate_dispatch_summaries(dispatch_results)
                            role_result.update(
                                {
                                    'status': aggregate_summary['status'],
                                    'summary': aggregate_summary,
                                    'request': {
                                        'roles': [role],
                                        'symbols': list(dispatch_symbols),
                                        'chunks': len(dispatch_results),
                                    },
                                    'chunk_results': [
                                        {
                                            'request': result.get('request'),
                                            'summary': result.get('summary'),
                                        }
                                        for result in dispatch_results
                                    ],
                                }
                            )
                        except FeedbackRetryLeaseLostError:
                            raise
                        except Exception as exc:  # noqa: BLE001
                            role_exception = exc
                            role_result.update(
                                {
                                    'status': 'failed',
                                    'error_type': type(exc).__name__,
                                    'error': str(exc),
                                }
                            )
                    else:
                        role_result.update(
                            {
                                'status': 'skipped',
                                'reason': 'no_symbols_remaining_for_role',
                            }
                        )

                    snapshot_after_role = self._build_role_sequence_snapshot(
                        market_date=config.market_date,
                        roles=roles,
                        eligible_symbols=scoped_feedback_symbols,
                        role_symbol_scopes=role_symbol_scopes,
                        force_recompute=effective_force_recompute,
                        reuse_existing=config.reuse_existing,
                        parent_run_id=request_run_id,
                        parent_started_at=parent_started_at,
                        knowledge_cutoff_at=knowledge_cutoff_at,
                        fs_div=resolved_fs_div,
                    )
                    role_state_after = snapshot_after_role.role_states[role]
                    role_boundary_complete = len(role_state_after.boundary_pending_symbols) == 0
                    completed_roles_after = tuple(
                        list(active_plan.completed_roles) + list(active_plan.plan_roles[:index + (1 if role_boundary_complete else 0)])
                    )
                    remaining_roles_after = tuple(
                        active_plan.plan_roles[index + 1:]
                        if role_boundary_complete
                        else active_plan.plan_roles[index:]
                    )
                    current_role_after = (
                        active_plan.plan_roles[index + 1]
                        if role_boundary_complete and index + 1 < len(active_plan.plan_roles)
                        else (role if not role_boundary_complete else None)
                    )
                    cycle_status = 'blocked' if not role_boundary_complete else ('running' if remaining_roles_after else 'completed')
                    self._sync_runtime_state(
                        market_date=config.market_date,
                        run_id=request_run_id,
                        snapshot=snapshot_after_role,
                        current_role=current_role_after,
                        completed_roles=completed_roles_after,
                        remaining_roles=remaining_roles_after,
                        cycle_number=active_plan.cycle_number,
                        completed_cycles=active_plan.completed_cycles,
                        max_cycles=config.max_cycles,
                        cycle_mode=active_plan.mode,
                        cycle_status=cycle_status,
                        status='running',
                        funnel_diagnostics=role_funnel,
                    )
                    self._checkpoint_run_state(
                        market_date=config.market_date,
                        request_run_id=request_run_id,
                        request_payload=request_payload,
                        lifecycle_stage='role_completed',
                        status='running',
                        progress_callback=progress_callback,
                        claim_lease=claim_lease,
                        cycle_result={
                            **role_result,
                            'cycle': active_plan.cycle_number,
                            'mode': active_plan.mode,
                            'boundary_status': role_state_after.boundary_status,
                            'retry_pending_symbols': len(role_state_after.retry_pending_symbols),
                            'completed_roles': list(completed_roles_after),
                            'remaining_roles': list(remaining_roles_after),
                        },
                    )
                    cycle_roles.append(
                        {
                            **role_result,
                            'boundary_status': role_state_after.boundary_status,
                            'retry_pending_symbols': len(role_state_after.retry_pending_symbols),
                            'completed_roles': list(completed_roles_after),
                            'remaining_roles': list(remaining_roles_after),
                        }
                    )
                    if role_exception is not None and not role_boundary_complete:
                        blocked = True
                        break

                snapshot_after_cycle = self._build_role_sequence_snapshot(
                    market_date=config.market_date,
                    roles=roles,
                    eligible_symbols=scoped_feedback_symbols,
                    role_symbol_scopes=role_symbol_scopes,
                    force_recompute=effective_force_recompute,
                    reuse_existing=config.reuse_existing,
                    parent_run_id=request_run_id,
                    parent_started_at=parent_started_at,
                    knowledge_cutoff_at=knowledge_cutoff_at,
                    fs_div=resolved_fs_div,
                )
                cycle_state = runtime_feedback_state_store.get_run(
                    config.market_date,
                    run_id=request_run_id,
                    logical=True,
                )
                retry_attempted_after = (
                    cycle_state.retry_attempted_symbols_by_role
                    if cycle_state is not None
                    else {}
                )
                retry_pending_symbols = self._collect_retry_pending_symbols(
                    snapshot_after_cycle,
                    retry_attempted_symbols_by_role=retry_attempted_after,
                )
                cycle_status = 'blocked' if blocked else ('completed' if not retry_pending_symbols else 'partial')
                if not blocked:
                    completed_cycles = active_plan.cycle_number
                cycles.append(
                    {
                        'cycle': active_plan.cycle_number,
                        'mode': active_plan.mode,
                        'status': cycle_status,
                        'start_pending': cycle_start_pending,
                        'end_pending': len(retry_pending_symbols),
                        'completed_roles': list(cycle_state.completed_roles if cycle_state is not None else active_plan.completed_roles),
                        'remaining_roles': list(cycle_state.remaining_roles if cycle_state is not None else active_plan.remaining_roles),
                        'role_batches': cycle_roles,
                    }
                )
                if blocked or not retry_pending_symbols:
                    initial_snapshot = snapshot_after_cycle
                    break

                initial_snapshot = snapshot_after_cycle
                active_plan = self._resolve_cycle_plan(
                    snapshot=snapshot_after_cycle,
                    max_cycles=config.max_cycles,
                    previous_cycle_number=active_plan.cycle_number,
                    previous_completed_cycles=completed_cycles,
                    retry_attempted_symbols_by_role=retry_attempted_after,
                )
        finally:
            if claim_lease is not None:
                claim_lease.assert_owned(stage='before_parent_finalization')
            final_snapshot = self._build_role_sequence_snapshot(
                market_date=config.market_date,
                roles=roles,
                eligible_symbols=scoped_feedback_symbols,
                role_symbol_scopes=role_symbol_scopes,
                force_recompute=effective_force_recompute,
                reuse_existing=config.reuse_existing,
                parent_run_id=request_run_id,
                parent_started_at=parent_started_at,
                knowledge_cutoff_at=knowledge_cutoff_at,
                fs_div=resolved_fs_div,
            )
            final_state_before_finish = runtime_feedback_state_store.get_run(
                config.market_date,
                run_id=request_run_id,
                logical=True,
            )
            final_retry_attempted = (
                final_state_before_finish.retry_attempted_symbols_by_role
                if final_state_before_finish is not None
                else {}
            )
            final_retry_pending_symbols = self._collect_retry_pending_symbols(
                final_snapshot,
                retry_attempted_symbols_by_role=final_retry_attempted,
            )
            final_boundary_pending_roles = final_snapshot.boundary_pending_roles()
            final_status = self._resolve_final_status(
                eligible_symbols=eligible_symbols,
                snapshot=final_snapshot,
                retry_pending_symbols=final_retry_pending_symbols,
                boundary_pending_roles=final_boundary_pending_roles,
            )

            resume_plan = self._resolve_cycle_plan(
                snapshot=final_snapshot,
                max_cycles=max(config.max_cycles, completed_cycles + 1),
                previous_cycle_number=active_plan.cycle_number if active_plan is not None else completed_cycles,
                previous_completed_cycles=completed_cycles,
                enforce_max_cycles=False,
                retry_attempted_symbols_by_role=final_retry_attempted,
            )
            resolved_cycle_status = (
                'failed'
                if final_status == 'failed'
                else ('partial' if final_status == 'partial' else 'completed')
            )
            self._sync_runtime_state(
                market_date=config.market_date,
                run_id=request_run_id,
                snapshot=final_snapshot,
                current_role=resume_plan.current_role if resume_plan is not None else None,
                completed_roles=resume_plan.completed_roles if resume_plan is not None else tuple(roles),
                remaining_roles=resume_plan.remaining_roles if resume_plan is not None else (),
                cycle_number=resume_plan.cycle_number if resume_plan is not None else completed_cycles,
                completed_cycles=completed_cycles,
                max_cycles=config.max_cycles,
                cycle_mode=resume_plan.mode if resume_plan is not None else None,
                cycle_status=resolved_cycle_status,
                status=final_status,
                funnel_diagnostics=role_funnel,
            )
            final_state = runtime_feedback_state_store.get_run(config.market_date, run_id=request_run_id, logical=True)
            if final_state is not None:
                final_state.completed_at = datetime.now(timezone.utc)
                final_state.last_activity_at = final_state.completed_at
            if final_state is None:
                raise RuntimeError(f'feedback retry runtime state missing for {request_run_id}')
            if claim_lease is not None:
                claim_lease.assert_owned(stage='before_parent_finish')
                claim_lease.begin_finalization()
            agent_context_feedback_run_state_service.finish_run(
                run_id=request_run_id,
                status=final_status,
                completed_count=int(final_state.to_progress_summary().get('completed_feedback_count') or 0),
                failed_count=int(final_state.to_progress_summary().get('failed_items') or 0),
                summary_payload=self._build_run_summary_payload(
                    market_date=config.market_date,
                    request_payload=request_payload,
                    state=final_state,
                    lifecycle_stage='finished',
                    cycle_result={
                        'cycles_completed': len(cycles),
                        'blocked': blocked,
                        'final_pending_symbols': list(final_retry_pending_symbols),
                        'final_boundary_pending_roles': list(final_boundary_pending_roles),
                    },
                ),
            )

        progress = self._get_retry_progress(
            market_date=config.market_date,
            roles=roles,
            symbols=list(requested_symbols) if requested_symbols is not None else None,
            run_id=request_run_id,
        )
        final_pending_symbols = self._collect_retry_pending_symbols(
            self._build_role_sequence_snapshot(
                market_date=config.market_date,
                roles=roles,
                eligible_symbols=scoped_feedback_symbols,
                role_symbol_scopes=role_symbol_scopes,
                force_recompute=effective_force_recompute,
                reuse_existing=config.reuse_existing,
                parent_run_id=request_run_id,
                parent_started_at=parent_started_at,
                knowledge_cutoff_at=knowledge_cutoff_at,
                fs_div=resolved_fs_div,
            ),
            retry_attempted_symbols_by_role=(
                final_state.retry_attempted_symbols_by_role
                if final_state is not None
                else {}
            ),
        )

        return {
            'market_date': config.market_date.isoformat(),
            'roles': list(roles),
            'max_cycles': config.max_cycles,
            'outer_batch_size': config.outer_batch_size,
            'reuse_existing': config.reuse_existing,
            'force_recompute': effective_force_recompute,
            'feedback_scope_mode': feedback_scope_mode,
            'requested_symbols': list(requested_symbols) if requested_symbols is not None else None,
            'eligible_symbols': list(eligible_symbols) if requested_symbols is not None else None,
            'skipped_symbols': skipped_symbols,
            'run_id': request_run_id,
            'status': final_status,
            'cycles': cycles,
            'final_pending': len(final_pending_symbols),
            'completed': final_status == 'completed',
            'progress': progress,
        }

    def _get_retry_progress(
        self,
        *,
        market_date: date,
        roles: Sequence[str],
        symbols: Sequence[str] | None = None,
        run_id: str | None = None,
    ) -> dict[str, Any]:
        return agent_context_progress_service.get_feedback_progress(
            market_date=market_date,
            symbols=list(symbols) if symbols is not None else None,
            expected_agents=roles,
            run_id=run_id,
        )


agent_feedback_retry_service = AgentFeedbackRetryService()
