from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from typing import Any

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert

from app.db import session_scope
from app.db_models import AgentContextFeedbackRun
from app.core.settings import settings
from app.agent_context.eligibility import AGENT_FEEDBACK_SCOPE_REQUESTED
from app.agent_context.roles import DECISION_SUPPORT_AGENT_NAMES
from app.agent_context.run_modes import (
    DAILY_RETRY_CYCLE_MODE,
    FEEDBACK_SCOPE_MODE_DECIDER_SCOPE_FAST_PATH,
)


def resolve_effective_decider_scope_authority_from_runs(
    runs: list[dict[str, Any]],
) -> dict[str, Any]:
    """Compose the effective decider scope from base authority plus patches."""

    required_roles = set(DECISION_SUPPORT_AGENT_NAMES)

    def request_payload(run: dict[str, Any]) -> dict[str, Any]:
        payload = run.get('request_payload')
        return payload if isinstance(payload, dict) else {}

    def feedback_scope_modes(run: dict[str, Any]) -> set[str]:
        containers: list[dict[str, Any]] = []
        payload = request_payload(run)
        if payload:
            containers.append(payload)
        summary = run.get('summary_payload')
        if isinstance(summary, dict):
            containers.append(summary)
            progress = summary.get('progress_summary')
            if isinstance(progress, dict):
                containers.append(progress)
        return {
            str(container.get('feedback_scope_mode') or '').strip()
            for container in containers
            if str(container.get('feedback_scope_mode') or '').strip()
        }

    def role_symbol_scopes(
        run: dict[str, Any],
    ) -> dict[str, list[str]] | None:
        raw_scopes = request_payload(run).get('role_symbol_scopes')
        if not isinstance(raw_scopes, dict):
            return None
        normalized: dict[str, list[str]] = {}
        for raw_role, raw_symbols in raw_scopes.items():
            role = str(raw_role or '').strip().lower()
            if role not in required_roles:
                continue
            if not isinstance(raw_symbols, list):
                return None
            symbols = sorted(
                {
                    str(symbol or '').strip().upper()
                    for symbol in raw_symbols
                    if str(symbol or '').strip()
                }
            )
            if not symbols or len(symbols) != len(raw_symbols):
                return None
            normalized[role] = symbols
        return normalized or None

    parents: list[dict[str, Any]] = []
    for run in runs:
        if not isinstance(run, dict):
            continue
        payload = request_payload(run)
        if str(payload.get('mode') or '').strip() != DAILY_RETRY_CYCLE_MODE:
            continue
        if FEEDBACK_SCOPE_MODE_DECIDER_SCOPE_FAST_PATH not in (
            feedback_scope_modes(run)
        ):
            continue
        raw_roles = payload.get('roles')
        if not isinstance(raw_roles, list):
            continue
        roles = {
            str(role or '').strip().lower()
            for role in raw_roles
            if str(role or '').strip()
        }
        if roles.intersection(required_roles):
            parents.append(run)

    base_parent: dict[str, Any] | None = None
    newer_patches: list[dict[str, Any]] = []
    for index, run in enumerate(parents):
        payload = request_payload(run)
        if isinstance(payload.get('scope_authority'), dict):
            base_parent = run
            newer_patches = parents[:index]
            break
        if str(payload.get('scope') or '').strip() != (
            AGENT_FEEDBACK_SCOPE_REQUESTED
        ):
            return {
                'ready': False,
                'reason': 'decider_scope_authority_missing',
            }
    if base_parent is None:
        return {
            'ready': False,
            'reason': 'decider_scope_authority_missing',
        }

    base_scopes = role_symbol_scopes(base_parent)
    if base_scopes is None or set(base_scopes) != required_roles:
        return {
            'ready': False,
            'reason': 'decider_role_symbol_scopes_invalid',
        }
    effective_scopes = {
        role: set(symbols) for role, symbols in base_scopes.items()
    }
    for patch in reversed(newer_patches):
        payload = request_payload(patch)
        if str(payload.get('scope') or '').strip() != (
            AGENT_FEEDBACK_SCOPE_REQUESTED
        ):
            return {
                'ready': False,
                'reason': 'decider_scope_authority_missing',
            }
        patch_scopes = role_symbol_scopes(patch)
        if patch_scopes is None:
            return {
                'ready': False,
                'reason': 'decider_role_symbol_scopes_invalid',
            }
        for role, symbols in patch_scopes.items():
            effective_scopes.setdefault(role, set()).update(symbols)
    return {
        'ready': True,
        'reason': 'effective_decider_scope_authority',
        'base_parent': base_parent,
        'role_symbol_scopes': {
            role: sorted(effective_scopes.get(role, set()))
            for role in DECISION_SUPPORT_AGENT_NAMES
        },
    }


class AgentContextFeedbackRunStateService:
    @staticmethod
    def _normalize_scope(values: list[str] | None) -> list[str]:
        return sorted(list(values or []))

    @staticmethod
    def _normalize_scope_items(values: Any) -> list[str] | None:
        if not isinstance(values, list):
            return None
        normalized: list[str] = []
        seen: set[str] = set()
        for value in values:
            symbol = str(value or '').strip().upper()
            if not symbol or symbol in seen:
                continue
            seen.add(symbol)
            normalized.append(symbol)
        return normalized

    def _iter_scope_payloads(self, run_snapshot: dict[str, Any]) -> list[dict[str, Any]]:
        payloads: list[dict[str, Any]] = []
        request_payload = run_snapshot.get('request_payload') if isinstance(run_snapshot.get('request_payload'), dict) else None
        summary_payload = run_snapshot.get('summary_payload') if isinstance(run_snapshot.get('summary_payload'), dict) else None
        summary_request = summary_payload.get('request') if isinstance(summary_payload, dict) and isinstance(summary_payload.get('request'), dict) else None
        for payload in (request_payload, summary_request):
            if payload is None:
                continue
            payloads.append(payload)
        return payloads

    def _collect_run_symbol_scopes(self, run_snapshot: dict[str, Any]) -> list[list[str]]:
        scopes: list[list[str]] = []
        for payload in self._iter_scope_payloads(run_snapshot):
            for key in ('requested_symbols', 'symbols'):
                normalized_scope = self._normalize_scope_items(payload.get(key))
                if normalized_scope is None:
                    continue
                comparable_scope = self._normalize_scope(normalized_scope)
                if comparable_scope in scopes:
                    continue
                scopes.append(comparable_scope)
        return scopes

    @staticmethod
    def _collect_run_feedback_scope_modes(run_snapshot: dict[str, Any]) -> list[str]:
        payloads: list[dict[str, Any]] = []
        request_payload = run_snapshot.get('request_payload') if isinstance(run_snapshot.get('request_payload'), dict) else None
        summary_payload = run_snapshot.get('summary_payload') if isinstance(run_snapshot.get('summary_payload'), dict) else None
        if request_payload is not None:
            payloads.append(request_payload)
        if summary_payload is not None:
            payloads.append(summary_payload)
            progress_summary = summary_payload.get('progress_summary')
            if isinstance(progress_summary, dict):
                payloads.append(progress_summary)
            summary_request = summary_payload.get('request')
            if isinstance(summary_request, dict):
                payloads.append(summary_request)

        modes: list[str] = []
        for payload in payloads:
            value = payload.get('feedback_scope_mode')
            if value is None:
                continue
            mode = str(value).strip()
            if mode and mode not in modes:
                modes.append(mode)
        return modes

    def resolve_run_requested_symbols(self, *, run_snapshot: dict[str, Any]) -> list[str] | None:
        payloads = self._iter_scope_payloads(run_snapshot)
        if not payloads:
            return None

        symbols_total = run_snapshot.get('symbols_total')
        try:
            expected_total = int(symbols_total) if symbols_total is not None else None
        except (TypeError, ValueError):
            expected_total = None

        requested_fallback: list[str] | None = None
        symbols_fallback: list[str] | None = None
        for payload in payloads:
            dispatch_symbols = self._normalize_scope_items(payload.get('symbols'))
            requested_symbols = self._normalize_scope_items(payload.get('requested_symbols'))

            if dispatch_symbols is not None:
                if expected_total is not None and len(dispatch_symbols) == expected_total:
                    return dispatch_symbols
                if requested_symbols is None or len(dispatch_symbols) != len(requested_symbols):
                    return dispatch_symbols
                if symbols_fallback is None:
                    symbols_fallback = dispatch_symbols

            if requested_symbols is not None:
                if expected_total is not None and len(requested_symbols) == expected_total:
                    return requested_symbols
                if requested_fallback is None:
                    requested_fallback = requested_symbols

        if symbols_fallback is not None:
            return symbols_fallback
        return requested_fallback

    @staticmethod
    def _serialize_run(row: AgentContextFeedbackRun) -> dict[str, Any]:
        return {
            'id': row.id,
            'run_id': row.run_id,
            'market_date': row.market_date,
            'status': row.status,
            'symbols_total': row.symbols_total,
            'completed_count': row.completed_count,
            'failed_count': row.failed_count,
            'request_payload': row.request_payload,
            'summary_payload': row.summary_payload,
            'started_at': row.started_at,
            'completed_at': row.completed_at,
            'created_at': row.created_at,
            'updated_at': row.updated_at,
        }

    @staticmethod
    def _is_stale_running_row(row: dict[str, Any]) -> bool:
        if str(row.get('status') or '').strip().lower() != 'running':
            return False
        updated_at = row.get('updated_at') or row.get('started_at')
        if not isinstance(updated_at, datetime):
            return False
        if updated_at.tzinfo is None:
            updated_at = updated_at.replace(tzinfo=timezone.utc)
        max_age_minutes = max(1, int(settings.eod_auto_pykrx_stale_run_minutes or 30))
        return datetime.now(timezone.utc) - updated_at > timedelta(minutes=max_age_minutes)

    @staticmethod
    def _expected_feedback_count(row: AgentContextFeedbackRun) -> int:
        symbols_total = max(0, int(row.symbols_total or 0))
        summary_payload = (
            dict(row.summary_payload)
            if isinstance(row.summary_payload, dict)
            else {}
        )
        progress_summary = (
            summary_payload.get('progress_summary')
            if isinstance(summary_payload.get('progress_summary'), dict)
            else {}
        )
        for container in (progress_summary, summary_payload):
            try:
                expected_feedback_count = int(
                    container.get('expected_feedback_count')
                )
            except (TypeError, ValueError):
                continue
            if expected_feedback_count >= 0:
                return max(symbols_total, expected_feedback_count)
        return symbols_total

    def _mark_stale_running_rows_abandoned(self, *, market_date: date) -> int:
        now = datetime.now(timezone.utc)
        abandoned = 0
        with session_scope() as session:
            rows = (
                session.execute(
                    select(AgentContextFeedbackRun)
                    .where(
                        AgentContextFeedbackRun.market_date == market_date,
                        AgentContextFeedbackRun.status == 'running',
                    )
                    .order_by(AgentContextFeedbackRun.started_at.asc(), AgentContextFeedbackRun.id.asc())
                )
                .scalars()
                .all()
            )
            for row in rows:
                snapshot = self._serialize_run(row)
                if not self._is_stale_running_row(snapshot):
                    continue
                failed_remaining = max(
                    self._expected_feedback_count(row)
                    - int(row.completed_count or 0),
                    0,
                )
                row.status = 'abandoned'
                row.failed_count = max(int(row.failed_count or 0), failed_remaining)
                row.completed_at = now
                row.updated_at = now
                summary_payload = dict(row.summary_payload or {})
                summary_payload['stale_running_abandoned_at'] = now.isoformat()
                summary_payload['stale_running_abandoned_reason'] = (
                    f'Marked abandoned after {settings.eod_auto_pykrx_stale_run_minutes} minutes without completion.'
                )
                row.summary_payload = summary_payload
                abandoned += 1
        return abandoned

    def mark_active_runs_abandoned(self, *, reason: str) -> dict[str, Any]:
        """Fence durable feedback work that cannot survive a Core restart."""

        now = datetime.now(timezone.utc)
        abandoned_run_ids: list[str] = []
        active_statuses = {'queued', 'running'}
        with session_scope() as session:
            rows = (
                session.execute(
                    select(AgentContextFeedbackRun)
                    .where(
                        AgentContextFeedbackRun.status.in_(active_statuses)
                    )
                    .order_by(
                        AgentContextFeedbackRun.started_at.asc(),
                        AgentContextFeedbackRun.id.asc(),
                    )
                )
                .scalars()
                .all()
            )
            for row in rows:
                if (
                    str(row.status or '').strip().lower()
                    not in active_statuses
                ):
                    continue
                failed_remaining = max(
                    self._expected_feedback_count(row)
                    - int(row.completed_count or 0),
                    0,
                )
                row.status = 'abandoned'
                row.failed_count = max(
                    int(row.failed_count or 0),
                    failed_remaining,
                )
                row.completed_at = now
                row.updated_at = now

                request_payload = dict(row.request_payload or {})
                retry_admission = (
                    dict(request_payload.get('retry_admission'))
                    if isinstance(
                        request_payload.get('retry_admission'),
                        dict,
                    )
                    else {}
                )
                retry_admission.update(
                    {
                        'ownership_revoked_at': now.isoformat(),
                        'ownership_revoked_reason': 'core_restart',
                    }
                )
                request_payload['retry_admission'] = retry_admission
                row.request_payload = request_payload

                summary_payload = dict(row.summary_payload or {})
                progress_summary = (
                    dict(summary_payload.get('progress_summary'))
                    if isinstance(
                        summary_payload.get('progress_summary'),
                        dict,
                    )
                    else {}
                )
                try:
                    previous_failed_items = int(
                        progress_summary.get('failed_items') or 0
                    )
                except (TypeError, ValueError):
                    previous_failed_items = 0
                progress_summary.update(
                    {
                        'run_status': 'abandoned',
                        'run_table_status': 'abandoned',
                        'is_live': False,
                        'run_completed_at': now.isoformat(),
                        'current_role': None,
                        'failed_items': max(
                            previous_failed_items,
                            int(row.failed_count or 0),
                        ),
                    }
                )
                summary_payload.update(
                    {
                        'lifecycle_stage': 'startup_abandoned',
                        'startup_active_abandoned_at': now.isoformat(),
                        'startup_active_abandoned_reason': reason,
                        'progress_summary': progress_summary,
                    }
                )
                row.summary_payload = summary_payload
                abandoned_run_ids.append(str(row.run_id or ''))

        return {
            'abandoned_runs': len(abandoned_run_ids),
            'run_ids': abandoned_run_ids,
        }

    def start_run(
        self,
        *,
        run_id: str,
        market_date: date,
        symbols_total: int,
        request_payload: dict[str, Any],
        summary_payload: dict[str, Any] | None = None,
        status: str = 'running',
        started_at: datetime | None = None,
    ) -> dict[str, Any]:
        now = datetime.now(timezone.utc)
        resolved_started_at = started_at or now
        with session_scope() as session:
            stmt = insert(AgentContextFeedbackRun).values(
                run_id=run_id,
                market_date=market_date,
                status=status,
                symbols_total=symbols_total,
                completed_count=0,
                failed_count=0,
                request_payload=request_payload,
                summary_payload=summary_payload,
                started_at=resolved_started_at,
                completed_at=None,
            )
            stmt = stmt.on_conflict_do_update(
                index_elements=['run_id'],
                set_={
                    'market_date': market_date,
                    'status': status,
                    'symbols_total': symbols_total,
                    'completed_count': 0,
                    'failed_count': 0,
                    'request_payload': request_payload,
                    'summary_payload': summary_payload,
                    'started_at': resolved_started_at,
                    'completed_at': None,
                    'updated_at': now,
                },
            )
            session.execute(stmt)
        return {'ok': True, 'run_id': run_id, 'status': status}

    def checkpoint_run(
        self,
        *,
        run_id: str,
        status: str,
        completed_count: int,
        failed_count: int,
        summary_payload: dict[str, Any],
        completed_at: datetime | None = None,
    ) -> dict[str, Any]:
        now = datetime.now(timezone.utc)
        with session_scope() as session:
            row = session.execute(select(AgentContextFeedbackRun).where(AgentContextFeedbackRun.run_id == run_id)).scalar_one_or_none()
            if row is None:
                raise ValueError(f'run not found: {run_id}')
            row.status = status
            row.completed_count = completed_count
            row.failed_count = failed_count
            row.summary_payload = summary_payload
            row.completed_at = completed_at
            row.updated_at = now
        return {'ok': True, 'run_id': run_id, 'status': status}

    def finish_run(
        self,
        *,
        run_id: str,
        status: str,
        completed_count: int,
        failed_count: int,
        summary_payload: dict[str, Any],
    ) -> dict[str, Any]:
        return self.checkpoint_run(
            run_id=run_id,
            status=status,
            completed_count=completed_count,
            failed_count=failed_count,
            summary_payload=summary_payload,
            completed_at=datetime.now(timezone.utc),
        )

    def get_run(
        self,
        *,
        run_id: str,
        session: Any | None = None,
        for_update: bool = False,
    ) -> dict[str, Any] | None:
        if for_update and session is None:
            raise ValueError('for_update requires a caller-owned session')

        statement = select(AgentContextFeedbackRun).where(
            AgentContextFeedbackRun.run_id == run_id
        )
        if for_update:
            statement = statement.with_for_update()

        def load(active_session: Any) -> dict[str, Any] | None:
            row = active_session.execute(statement).scalar_one_or_none()
            if row is None:
                return None
            return self._serialize_run(row)

        if session is not None:
            return load(session)
        with session_scope() as owned_session:
            return load(owned_session)

    def find_latest_run(
        self,
        *,
        market_date: date,
        requested_symbols: list[str] | None = None,
        requested_roles: list[str] | None = None,
        request_modes: list[str] | tuple[str, ...] | None = None,
        feedback_scope_mode: str | None = None,
    ) -> dict[str, Any] | None:
        serialized_rows = self.list_runs(market_date=market_date)

        normalized_symbols = self._normalize_scope(requested_symbols)
        normalized_roles = self._normalize_scope(requested_roles)
        normalized_modes = set(self._normalize_scope(list(request_modes) if request_modes is not None else None))
        normalized_feedback_scope_mode = str(feedback_scope_mode or '').strip()
        for row in serialized_rows:
            request_payload = row.get('request_payload') if isinstance(row.get('request_payload'), dict) else {}
            run_symbol_scopes = self._collect_run_symbol_scopes(row)
            run_roles = request_payload.get('roles')
            run_mode = str(request_payload.get('mode') or '').strip()
            if normalized_symbols:
                if normalized_symbols not in run_symbol_scopes:
                    continue
            if normalized_roles:
                if self._normalize_scope(run_roles if isinstance(run_roles, list) else None) != normalized_roles:
                    continue
            if normalized_modes and run_mode not in normalized_modes:
                continue
            if normalized_feedback_scope_mode:
                if normalized_feedback_scope_mode not in self._collect_run_feedback_scope_modes(row):
                    continue
            return row
        return None

    def list_runs(
        self,
        *,
        market_date: date,
    ) -> list[dict[str, Any]]:
        """Return current durable runs newest-first for authority composition."""

        self._mark_stale_running_rows_abandoned(market_date=market_date)
        with session_scope() as session:
            serialized_rows = [
                self._serialize_run(row)
                for row in session.execute(
                select(AgentContextFeedbackRun)
                .where(AgentContextFeedbackRun.market_date == market_date)
                .order_by(AgentContextFeedbackRun.started_at.desc(), AgentContextFeedbackRun.id.desc())
                ).scalars().all()
            ]
        return [
            row
            for row in serialized_rows
            if str(row.get('status') or '').strip().lower() != 'abandoned'
            and not self._is_stale_running_row(row)
        ]


agent_context_feedback_run_state_service = AgentContextFeedbackRunStateService()
