from __future__ import annotations

import base64
from collections.abc import Mapping
import hashlib
import json
import os
from pathlib import Path
import re
import sqlite3
import subprocess
import time
import uuid
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from typing import Any, Sequence

from sqlalchemy import Select, func, select
from sqlalchemy.dialects.postgresql import insert

from app.core.settings import settings
from app.db import session_scope
from app.db_models import AgentFeedbackSessionCleanupFailure


class CleanupPersistenceDeadlineExceeded(TimeoutError):
    """Raised before a cleanup retry write that cannot fit its caller budget."""


class CleanupSafetyError(RuntimeError):
    """Raised when immutable session or native-thread ownership cannot be proven."""

    def __init__(self, reason: str, message: str):
        super().__init__(message)
        self.reason = reason


@dataclass(frozen=True)
class SessionCleanupTarget:
    market_date: date
    symbol: str
    agent_name: str
    run_id: str | None
    session_key: str
    delete_transcript: bool = True
    emit_lifecycle_hooks: bool = False
    retry_cleanup_result: dict[str, Any] | None = None


class AgentFeedbackSessionCleanupService:
    _CODEX_BINDING_NAMESPACE = 'app-server-thread-bindings'
    _CODEX_CLI_VERSION = '0.160.0'
    _AGENT_ID_PATTERN = re.compile(r'^[a-z0-9][a-z0-9_-]{0,127}$')
    _TERMINAL_SESSION_STATUSES = frozenset({'done', 'failed', 'killed', 'timeout'})
    _MANUAL_RECONCILIATION_REASONS = frozenset(
        {
            'legacy_retry_missing_native_cleanup_identity',
            'native_cleanup_identity_invalid',
            'native_cleanup_identity_conflict',
            'session_agent_scope_mismatch',
            'session_key_identity_mismatch',
            'codex_version_mismatch',
            'openclaw_session_generation_mismatch',
            'codex_binding_identity_mismatch',
            'codex_thread_identity_mismatch',
        }
    )

    def delete_now(
        self,
        *,
        target: SessionCleanupTarget,
        deadline_monotonic: float | None = None,
    ) -> dict[str, Any]:
        deadline = self._resolve_deadline(deadline_monotonic)
        prepared = self._prepare_cleanup(target=target, deadline_monotonic=deadline)
        if prepared.get('ok') is not True:
            return self._failed_result(
                target=target,
                result=prepared,
                deadline_monotonic=deadline,
            )

        identity = prepared['identity']
        session_was_present = prepared.get('openclaw_session_present') is True
        if session_was_present:
            result = self._invoke_delete(
                session_key=identity['session_key'],
                agent_name=identity['agent_name'],
                expected_session_id=identity['openclaw_session_id'],
                expected_lifecycle_revision=identity[
                    'openclaw_lifecycle_revision'
                ],
                expected_session_updated_at=identity[
                    'openclaw_session_updated_at'
                ],
                delete_transcript=target.delete_transcript,
                emit_lifecycle_hooks=target.emit_lifecycle_hooks,
                deadline_monotonic=deadline,
            )
            if result.get('ok') is not True or not isinstance(result.get('deleted'), bool):
                return self._failed_result(
                    target=target,
                    result={**result, 'native_cleanup_identity': identity},
                    deadline_monotonic=deadline,
                )
            absence = self._assert_openclaw_generation_absent(
                identity=identity,
                deadline_monotonic=deadline,
            )
            if absence.get('ok') is not True:
                return self._failed_result(
                    target=target,
                    result={**absence, 'native_cleanup_identity': identity},
                    deadline_monotonic=deadline,
                )
        else:
            result = {
                'ok': True,
                'deleted': False,
                'skipped': True,
                'reason': 'openclaw_session_already_absent',
            }

        try:
            self._require_time_remaining(deadline)
            owners = self._binding_owner_entry_keys(
                thread_id=identity['codex_thread_id'],
                deadline_monotonic=deadline,
            )
        except (CleanupSafetyError, OSError, sqlite3.Error, ValueError) as exc:
            reason = exc.reason if isinstance(exc, CleanupSafetyError) else 'codex_binding_owner_read_failed'
            return self._failed_result(
                target=target,
                result={
                    **result,
                    'ok': False,
                    'reason': reason,
                    'error': str(exc),
                    'native_cleanup_identity': identity,
                },
                deadline_monotonic=deadline,
            )
        if owners:
            return self._failed_result(
                target=target,
                result={
                    **result,
                    'ok': False,
                    'reason': 'codex_thread_still_owned',
                    'error': 'native Codex thread still has active OpenClaw binding owners',
                    'binding_owner_count': len(owners),
                    'binding_owner_entry_keys': owners,
                    'native_cleanup_identity': identity,
                },
                deadline_monotonic=deadline,
            )

        native_cleanup = self._assess_native_cleanup(
            identity=identity,
            deadline_monotonic=deadline,
        )
        if native_cleanup.get('ok') is not True:
            return self._failed_result(
                target=target,
                result={
                    **result,
                    'ok': False,
                    'reason': native_cleanup.get('reason') or 'codex_native_delete_failed',
                    'error': (
                        native_cleanup.get('error')
                        or 'native Codex cleanup readiness check failed'
                    ),
                    'retryable': native_cleanup.get('retryable', False),
                    'offline_maintenance_required': native_cleanup.get(
                        'offline_maintenance_required',
                        False,
                    ),
                    'native_cleanup': native_cleanup,
                    'native_cleanup_identity': identity,
                },
                deadline_monotonic=deadline,
            )

        try:
            self._clear_failure(
                session_key=target.session_key,
                deadline_monotonic=deadline,
            )
        except (CleanupSafetyError, TimeoutError) as exc:
            return self._failed_result(
                target=target,
                result={
                    'ok': False,
                    'deleted': session_was_present,
                    'reason': 'cleanup_finalization_deferred',
                    'error': str(exc),
                    'retryable': True,
                    'native_cleanup': native_cleanup,
                    'native_cleanup_identity': identity,
                },
                deadline_monotonic=deadline,
            )
        return {
            'status': 'deleted',
            'outcome': 'deleted' if session_was_present else 'already_absent',
            'session_key': target.session_key,
            'result': result,
            'native_cleanup': native_cleanup,
        }

    def _failed_result(
        self,
        *,
        target: SessionCleanupTarget,
        result: dict[str, Any],
        deadline_monotonic: float,
    ) -> dict[str, Any]:
        try:
            failure = self._record_failure(
                target=target,
                result=result,
                deadline_monotonic=deadline_monotonic,
            )
        except (CleanupSafetyError, TimeoutError) as exc:
            failure = {
                'persistence_confirmed': False,
                'reason': 'cleanup_failure_persistence_deadline_exceeded',
                'error': str(exc),
            }
        return {
            'status': 'failed',
            'session_key': target.session_key,
            'result': result,
            'failure': failure,
        }

    @staticmethod
    def _codex_binding_entry_key(*, agent_name: str, session_key: str) -> str:
        digest = base64.urlsafe_b64encode(
            hashlib.sha256(session_key.strip().encode('utf-8')).digest()
        ).decode('ascii').rstrip('=')
        return f'session-key:{agent_name.strip().lower()}:{digest}'

    @staticmethod
    def _read_only_sqlite(
        db_path: Path,
        *,
        deadline_monotonic: float | None = None,
    ) -> sqlite3.Connection:
        timeout_seconds = 5.0
        if deadline_monotonic is not None:
            remaining_seconds = deadline_monotonic - time.monotonic()
            if remaining_seconds <= 0:
                raise CleanupSafetyError(
                    'cleanup_deadline_exceeded',
                    'session cleanup deadline was exhausted before a SQLite read',
                )
            timeout_seconds = min(timeout_seconds, remaining_seconds)
        connection = sqlite3.connect(
            f'{db_path.resolve().as_uri()}?mode=ro',
            uri=True,
            timeout=timeout_seconds,
        )
        connection.execute('PRAGMA query_only = ON')
        return connection

    def _resolve_deadline(self, deadline_monotonic: float | None) -> float:
        configured_timeout = max(
            1.0,
            float(settings.openclaw_session_cleanup_timeout_seconds or 30.0),
        )
        local_deadline = time.monotonic() + configured_timeout
        return min(local_deadline, deadline_monotonic) if deadline_monotonic is not None else local_deadline

    @staticmethod
    def _require_time_remaining(deadline_monotonic: float) -> float:
        remaining = deadline_monotonic - time.monotonic()
        if remaining <= 0:
            raise CleanupSafetyError(
                'cleanup_deadline_exceeded',
                'session cleanup deadline was exhausted before the next mutation',
            )
        return remaining

    def _subprocess_timeout(self, deadline_monotonic: float) -> float:
        configured_timeout = max(
            1.0,
            float(settings.openclaw_session_cleanup_timeout_seconds or 30.0),
        )
        return min(configured_timeout, self._require_time_remaining(deadline_monotonic))

    def _require_database_budget(self, deadline_monotonic: float) -> None:
        remaining = self._require_time_remaining(deadline_monotonic)
        if remaining <= float(settings.db_connect_timeout_seconds):
            raise CleanupSafetyError(
                'cleanup_deadline_exceeded',
                'session cleanup deadline cannot bound a database connection',
            )

    @staticmethod
    def _require_queue_persistence_budget(deadline_monotonic: float) -> None:
        if (
            deadline_monotonic - time.monotonic()
            <= float(settings.db_connect_timeout_seconds)
        ):
            raise CleanupPersistenceDeadlineExceeded(
                'cleanup retry prequeue deadline cannot bound database connect'
            )

    def _prepare_cleanup(
        self,
        *,
        target: SessionCleanupTarget,
        deadline_monotonic: float,
    ) -> dict[str, Any]:
        identity: dict[str, Any] | None = None
        try:
            self._require_time_remaining(deadline_monotonic)
            retry_result = target.retry_cleanup_result
            checkpoint_exists = retry_result is not None
            if retry_result is None:
                checkpoint_exists, retry_result = self._load_cleanup_checkpoint(
                    session_key=target.session_key,
                    deadline_monotonic=deadline_monotonic,
                )
            identity = self._identity_from_cleanup_result(retry_result)
            if checkpoint_exists and identity is None:
                raise CleanupSafetyError(
                    'legacy_retry_missing_native_cleanup_identity',
                    'legacy cleanup retry lacks immutable OpenClaw/Codex identity',
                )
            if identity is None:
                captured = self._capture_native_cleanup_identity(
                    target=target,
                    deadline_monotonic=deadline_monotonic,
                )
                if captured.get('ok') is not True:
                    return captured
                identity = captured['identity']
                self._persist_identity_checkpoint(
                    target=target,
                    identity=identity,
                    deadline_monotonic=deadline_monotonic,
                )
            return self._validate_persisted_identity(
                target=target,
                identity=identity,
                deadline_monotonic=deadline_monotonic,
            )
        except CleanupSafetyError as exc:
            result = {
                'ok': False,
                'deleted': False,
                'reason': exc.reason,
                'error': str(exc),
            }
            if exc.reason in self._MANUAL_RECONCILIATION_REASONS:
                result.update(
                    {
                        'blocked': True,
                        'manual_reconciliation_required': True,
                        'retryable': False,
                    }
                )
            if identity is not None:
                result['native_cleanup_identity'] = identity
            return result
        except (OSError, sqlite3.Error, json.JSONDecodeError, ValueError) as exc:
            result = {
                'ok': False,
                'deleted': False,
                'reason': 'native_cleanup_identity_capture_failed',
                'error_type': type(exc).__name__,
                'error': str(exc),
            }
            if identity is not None:
                result['native_cleanup_identity'] = identity
            return result

    @staticmethod
    def _identity_from_cleanup_result(
        cleanup_result: dict[str, Any] | None,
    ) -> dict[str, Any] | None:
        if not isinstance(cleanup_result, dict):
            return None
        identity = cleanup_result.get('native_cleanup_identity')
        return dict(identity) if isinstance(identity, dict) else None

    def _load_cleanup_checkpoint(
        self,
        *,
        session_key: str,
        deadline_monotonic: float,
    ) -> tuple[bool, dict[str, Any] | None]:
        self._require_database_budget(deadline_monotonic)
        with session_scope(
            statement_timeout_ms=settings.agent_feedback_db_statement_timeout_ms,
            lock_timeout_ms=settings.agent_feedback_db_lock_timeout_ms,
            deadline_monotonic=deadline_monotonic,
            isolated_connection=True,
        ) as session:
            record = session.execute(
                select(AgentFeedbackSessionCleanupFailure).where(
                    AgentFeedbackSessionCleanupFailure.session_key == session_key
                )
            ).scalar_one_or_none()
            if record is None:
                return False, None
            result = record.cleanup_result
            return True, dict(result) if isinstance(result, dict) else None

    def _persist_identity_checkpoint(
        self,
        *,
        target: SessionCleanupTarget,
        identity: dict[str, Any],
        deadline_monotonic: float,
    ) -> None:
        self._require_database_budget(deadline_monotonic)
        now = datetime.now(UTC)
        checkpoint = {
            'ok': False,
            'deleted': False,
            'deferred': True,
            'reason': 'native_cleanup_identity_checkpointed',
            'native_cleanup_identity': identity,
        }
        with session_scope(
            statement_timeout_ms=settings.agent_feedback_db_statement_timeout_ms,
            lock_timeout_ms=settings.agent_feedback_db_lock_timeout_ms,
            deadline_monotonic=deadline_monotonic,
            isolated_connection=True,
        ) as session:
            existing = session.execute(
                select(AgentFeedbackSessionCleanupFailure).where(
                    AgentFeedbackSessionCleanupFailure.session_key == target.session_key
                )
            ).scalar_one_or_none()
            if existing is not None:
                stored_identity = self._identity_from_cleanup_result(existing.cleanup_result)
                if stored_identity is None:
                    raise CleanupSafetyError(
                        'legacy_retry_missing_native_cleanup_identity',
                        'existing cleanup retry lacks immutable native identity',
                    )
                if stored_identity != identity:
                    raise CleanupSafetyError(
                        'native_cleanup_identity_conflict',
                        'existing cleanup retry identity differs from current capture',
                    )
                return
            session.add(
                AgentFeedbackSessionCleanupFailure(
                    market_date=target.market_date,
                    symbol=target.symbol,
                    agent_name=target.agent_name,
                    run_id=target.run_id,
                    session_key=target.session_key,
                    provider='openclaw',
                    delete_transcript=target.delete_transcript,
                    emit_lifecycle_hooks=target.emit_lifecycle_hooks,
                    cleanup_attempts=0,
                    last_attempt_at=None,
                    next_attempt_at=now,
                    last_error='native_cleanup_identity_checkpointed',
                    cleanup_result=checkpoint,
                )
            )

    def _resolve_agent_runtime_paths(self, *, agent_name: str) -> dict[str, Path]:
        normalized_agent = agent_name.strip().lower()
        if not self._AGENT_ID_PATTERN.fullmatch(normalized_agent):
            raise CleanupSafetyError('invalid_agent_home', 'invalid OpenClaw agent id')
        state_root = Path(settings.openclaw_state_dir).expanduser().resolve(strict=True)
        config_path = (state_root / 'openclaw.json').resolve(strict=True)
        if config_path.parent != state_root:
            raise CleanupSafetyError(
                'invalid_agent_home',
                'OpenClaw config resolves outside the configured state root',
            )
        config = json.loads(config_path.read_text(encoding='utf-8'))
        agents = config.get('agents') if isinstance(config, dict) else None
        entries = agents.get('entries') if isinstance(agents, dict) else None
        entry = entries.get(normalized_agent) if isinstance(entries, dict) else None
        configured_agent_dir = entry.get('agentDir') if isinstance(entry, dict) else None
        if not isinstance(configured_agent_dir, str) or not configured_agent_dir.strip():
            raise CleanupSafetyError(
                'invalid_agent_home',
                f'OpenClaw agent {normalized_agent} has no explicit agentDir',
            )
        expected_agent_home = (
            state_root / 'agents' / normalized_agent / 'agent'
        ).resolve(strict=True)
        agent_home = Path(configured_agent_dir).expanduser().resolve(strict=True)
        if (
            agent_home != expected_agent_home
            or not agent_home.is_dir()
            or not agent_home.is_relative_to(state_root)
        ):
            raise CleanupSafetyError(
                'invalid_agent_home',
                f'OpenClaw agentDir is outside the exact managed agent home for {normalized_agent}',
            )
        openclaw_agent_db = (agent_home / 'openclaw-agent.sqlite').resolve(strict=True)
        codex_home = (agent_home / 'codex-home').resolve(strict=True)
        if (
            not openclaw_agent_db.is_file()
            or openclaw_agent_db.parent != agent_home
            or not codex_home.is_dir()
            or codex_home.parent != agent_home
        ):
            raise CleanupSafetyError(
                'invalid_agent_home',
                f'OpenClaw agent state is incomplete for {normalized_agent}',
            )
        codex_state_dbs = sorted(
            path.resolve()
            for path in codex_home.glob('state_*.sqlite')
            if path.is_file()
        )
        if len(codex_state_dbs) != 1:
            raise CleanupSafetyError(
                'invalid_agent_home',
                f'expected exactly one Codex state database for {normalized_agent}',
            )
        if not codex_state_dbs[0].is_relative_to(codex_home):
            raise CleanupSafetyError(
                'invalid_agent_home',
                'Codex state database resolves outside the validated Codex home',
            )
        shared_state_root = (state_root / 'state').resolve(strict=True)
        shared_state_db = (shared_state_root / 'openclaw.sqlite').resolve(strict=True)
        if (
            shared_state_root.parent != state_root
            or not shared_state_db.is_file()
            or shared_state_db.parent != shared_state_root
        ):
            raise CleanupSafetyError(
                'openclaw_state_database_missing',
                'OpenClaw shared state database is unavailable',
            )
        return {
            'state_root': state_root,
            'agent_home': agent_home,
            'codex_home': codex_home,
            'codex_state_db': codex_state_dbs[0],
            'openclaw_agent_db': openclaw_agent_db,
            'shared_state_db': shared_state_db,
        }

    def _session_key_candidates(self, *, agent_name: str, session_key: str) -> list[str]:
        normalized_key = session_key.strip()
        prefix = f'agent:{agent_name.strip().lower()}:'
        if normalized_key.startswith('agent:'):
            if not normalized_key.startswith(prefix):
                raise CleanupSafetyError(
                    'session_agent_scope_mismatch',
                    'session key belongs to a different OpenClaw agent',
                )
            return [normalized_key]
        return [normalized_key, f'{prefix}{normalized_key}']

    def _read_session_snapshot(
        self,
        *,
        agent_name: str,
        session_key: str,
        paths: dict[str, Path],
        deadline_monotonic: float | None = None,
    ) -> dict[str, Any]:
        candidates = self._session_key_candidates(
            agent_name=agent_name,
            session_key=session_key,
        )
        with self._read_only_sqlite(
            paths['openclaw_agent_db'],
            deadline_monotonic=deadline_monotonic,
        ) as connection:
            placeholders = ','.join('?' for _ in candidates)
            rows = connection.execute(
                f'''
                SELECT session_key, current_session_id, entry_json, status, updated_at
                FROM session_nodes
                WHERE session_key IN ({placeholders})
                ''',
                candidates,
            ).fetchall()
            if not rows:
                return {'present': False}
            if len(rows) != 1:
                raise CleanupSafetyError(
                    'openclaw_session_scope_ambiguous',
                    'multiple OpenClaw session keys matched one cleanup target',
                )
            canonical_key, session_id, entry_json, node_status, node_updated_at = rows[0]
            entry = json.loads(entry_json)
            entry_session_id = entry.get('sessionId') if isinstance(entry, dict) else None
            if not isinstance(session_id, str) or not session_id.strip() or entry_session_id != session_id:
                raise CleanupSafetyError(
                    'openclaw_session_generation_invalid',
                    'OpenClaw session generation is missing or internally inconsistent',
                )
            lifecycle_revision = entry.get('lifecycleRevision')
            if lifecycle_revision is not None and (
                not isinstance(lifecycle_revision, str)
                or not lifecycle_revision.strip()
            ):
                raise CleanupSafetyError(
                    'openclaw_session_generation_invalid',
                    'OpenClaw lifecycle revision is malformed',
                )
            session_updated_at = entry.get('updatedAt')
            if (
                isinstance(session_updated_at, bool)
                or not isinstance(session_updated_at, (int, float))
                or session_updated_at < 0
                or session_updated_at != node_updated_at
            ):
                raise CleanupSafetyError(
                    'openclaw_session_generation_invalid',
                    'OpenClaw session updatedAt is missing or inconsistent',
                )
            window = connection.execute(
                'SELECT status FROM session_windows WHERE session_id = ?',
                (session_id,),
            ).fetchone()
            pending_count = connection.execute(
                "SELECT COUNT(*) FROM session_pending_inputs WHERE session_id = ? AND state = 'queued'",
                (session_id,),
            ).fetchone()[0]
        statuses = {
            value
            for value in (
                node_status,
                entry.get('status') if isinstance(entry, dict) else None,
                window[0] if window else None,
            )
            if value is not None
        }
        active_run_id = entry.get('activeRunId') if isinstance(entry, dict) else None
        active = (
            bool(active_run_id)
            or int(pending_count or 0) > 0
            or 'running' in statuses
            or not statuses
            or any(status not in self._TERMINAL_SESSION_STATUSES for status in statuses)
        )
        return {
            'present': True,
            'session_key': canonical_key,
            'session_id': session_id,
            'lifecycle_revision': lifecycle_revision,
            'session_updated_at': session_updated_at,
            'active': active,
            'statuses': sorted(statuses),
        }

    def _read_binding(
        self,
        *,
        entry_key: str,
        shared_state_db: Path,
        deadline_monotonic: float | None = None,
    ) -> dict[str, Any] | None:
        with self._read_only_sqlite(
            shared_state_db,
            deadline_monotonic=deadline_monotonic,
        ) as connection:
            row = connection.execute(
                '''
                SELECT value_json
                FROM plugin_state_entries
                WHERE plugin_id = ? AND namespace = ? AND entry_key = ?
                ''',
                ('codex', self._CODEX_BINDING_NAMESPACE, entry_key),
            ).fetchone()
        if row is None:
            return None
        value = json.loads(row[0])
        if not isinstance(value, dict):
            raise CleanupSafetyError(
                'codex_binding_invalid',
                'Codex binding is not a JSON object',
            )
        return value

    def _capture_native_cleanup_identity(
        self,
        *,
        target: SessionCleanupTarget,
        deadline_monotonic: float | None = None,
    ) -> dict[str, Any]:
        try:
            paths = self._resolve_agent_runtime_paths(agent_name=target.agent_name)
            snapshot = self._read_session_snapshot(
                agent_name=target.agent_name,
                session_key=target.session_key,
                paths=paths,
                deadline_monotonic=deadline_monotonic,
            )
            if snapshot.get('present') is not True:
                raise CleanupSafetyError(
                    'openclaw_session_identity_unavailable',
                    'cannot capture immutable identity for an absent OpenClaw session',
                )
            if snapshot.get('active') is True:
                raise CleanupSafetyError(
                    'openclaw_session_active',
                    'refusing to delete an active OpenClaw session',
                )
            canonical_key = snapshot['session_key']
            entry_key = self._codex_binding_entry_key(
                agent_name=target.agent_name,
                session_key=canonical_key,
            )
            stored = self._read_binding(
                entry_key=entry_key,
                shared_state_db=paths['shared_state_db'],
                deadline_monotonic=deadline_monotonic,
            )
            if stored is None or stored.get('state') != 'active':
                raise CleanupSafetyError(
                    'codex_binding_missing',
                    'active Codex binding is unavailable for this OpenClaw generation',
                )
            if stored.get('sessionId') != snapshot['session_id']:
                raise CleanupSafetyError(
                    'openclaw_session_generation_mismatch',
                    'Codex binding belongs to a different OpenClaw session generation',
                )
            binding = stored.get('binding')
            if not isinstance(binding, dict) or binding.get('connectionScope') == 'supervision':
                raise CleanupSafetyError(
                    'codex_binding_invalid',
                    'Codex binding is missing or supervision-owned',
                )
            thread_id = str(binding.get('threadId') or '').strip()
            uuid.UUID(thread_id)
            rollout_path = Path(str(binding.get('rolloutPath') or '')).expanduser().resolve()
            sessions_root = (paths['codex_home'] / 'sessions').resolve()
            if not rollout_path.is_relative_to(sessions_root):
                raise CleanupSafetyError(
                    'invalid_agent_home',
                    'Codex rollout path is outside the validated agent Codex home',
                )
            fingerprint_raw = binding.get('appServerRuntimeFingerprint')
            fingerprint = json.loads(fingerprint_raw) if isinstance(fingerprint_raw, str) else None
            runtime_identity = fingerprint.get('runtimeIdentity') if isinstance(fingerprint, dict) else None
            runtime_codex_home = runtime_identity.get('codexHome') if isinstance(runtime_identity, dict) else None
            app_server_version = runtime_identity.get('serverVersion') if isinstance(runtime_identity, dict) else None
            if (
                not isinstance(runtime_codex_home, str)
                or Path(runtime_codex_home).expanduser().resolve() != paths['codex_home']
                or app_server_version != self._CODEX_CLI_VERSION
            ):
                raise CleanupSafetyError(
                    'invalid_agent_home',
                    'Codex binding runtime identity does not match the validated agent home/version',
                )
            native_record = self._native_thread_record(
                state_db=paths['codex_state_db'],
                thread_id=thread_id,
                deadline_monotonic=deadline_monotonic,
            )
            if native_record is None or Path(native_record['rollout_path']).resolve() != rollout_path:
                raise CleanupSafetyError(
                    'codex_thread_identity_mismatch',
                    'Codex thread record does not match the bound rollout path',
                )
            identity = {
                'version': 1,
                'agent_name': target.agent_name.strip().lower(),
                'session_key': canonical_key,
                'openclaw_session_id': snapshot['session_id'],
                'openclaw_lifecycle_revision': snapshot[
                    'lifecycle_revision'
                ],
                'openclaw_session_updated_at': snapshot['session_updated_at'],
                'binding_entry_key': entry_key,
                'codex_thread_id': thread_id,
                'agent_home': str(paths['agent_home']),
                'codex_home': str(paths['codex_home']),
                'codex_state_db': str(paths['codex_state_db']),
                'codex_rollout_path': str(rollout_path),
                'codex_app_server_version': app_server_version,
            }
            identity['identity_sha256'] = self._identity_sha256(identity)
            return {'ok': True, 'identity': identity}
        except CleanupSafetyError as exc:
            return {'ok': False, 'deleted': False, 'reason': exc.reason, 'error': str(exc)}
        except (OSError, sqlite3.Error, json.JSONDecodeError, ValueError) as exc:
            return {
                'ok': False,
                'deleted': False,
                'reason': 'native_cleanup_identity_capture_failed',
                'error_type': type(exc).__name__,
                'error': str(exc),
            }

    @staticmethod
    def _identity_sha256(identity: dict[str, Any]) -> str:
        payload = {key: value for key, value in identity.items() if key != 'identity_sha256'}
        encoded = json.dumps(payload, sort_keys=True, separators=(',', ':')).encode('utf-8')
        return hashlib.sha256(encoded).hexdigest()

    def _validate_persisted_identity(
        self,
        *,
        target: SessionCleanupTarget,
        identity: dict[str, Any],
        deadline_monotonic: float | None = None,
    ) -> dict[str, Any]:
        required_strings = (
            'agent_name',
            'session_key',
            'openclaw_session_id',
            'binding_entry_key',
            'codex_thread_id',
            'agent_home',
            'codex_home',
            'codex_state_db',
            'codex_rollout_path',
            'codex_app_server_version',
            'identity_sha256',
        )
        if identity.get('version') != 1 or any(
            not isinstance(identity.get(key), str) or not identity[key].strip()
            for key in required_strings
        ):
            raise CleanupSafetyError(
                'native_cleanup_identity_invalid',
                'persisted native cleanup identity is incomplete',
            )
        lifecycle_revision = identity.get('openclaw_lifecycle_revision')
        if lifecycle_revision is not None and (
            not isinstance(lifecycle_revision, str)
            or not lifecycle_revision.strip()
        ):
            raise CleanupSafetyError(
                'native_cleanup_identity_invalid',
                'persisted OpenClaw lifecycle revision is malformed',
            )
        session_updated_at = identity.get('openclaw_session_updated_at')
        if (
            isinstance(session_updated_at, bool)
            or not isinstance(session_updated_at, (int, float))
            or session_updated_at < 0
        ):
            raise CleanupSafetyError(
                'native_cleanup_identity_invalid',
                'persisted OpenClaw updatedAt is missing or malformed',
            )
        if identity['identity_sha256'] != self._identity_sha256(identity):
            raise CleanupSafetyError(
                'native_cleanup_identity_invalid',
                'persisted native cleanup identity digest does not match',
            )
        if identity['agent_name'] != target.agent_name.strip().lower():
            raise CleanupSafetyError(
                'session_agent_scope_mismatch',
                'persisted native cleanup identity belongs to a different agent',
            )
        if identity['session_key'] not in self._session_key_candidates(
            agent_name=identity['agent_name'],
            session_key=target.session_key,
        ):
            raise CleanupSafetyError(
                'session_key_identity_mismatch',
                'persisted native cleanup identity belongs to a different session key',
            )
        uuid.UUID(identity['openclaw_session_id'])
        uuid.UUID(identity['codex_thread_id'])
        paths = self._resolve_agent_runtime_paths(agent_name=target.agent_name)
        for key in ('agent_home', 'codex_home', 'codex_state_db'):
            if Path(identity[key]).expanduser().resolve() != paths[key]:
                raise CleanupSafetyError(
                    'invalid_agent_home',
                    f'persisted {key} no longer matches the validated agent home',
                )
        if identity['codex_app_server_version'] != self._CODEX_CLI_VERSION:
            raise CleanupSafetyError(
                'codex_version_mismatch',
                'persisted Codex runtime version is not the supported cleanup version',
            )
        rollout_path = Path(identity['codex_rollout_path']).expanduser().resolve()
        if not rollout_path.is_relative_to((paths['codex_home'] / 'sessions').resolve()):
            raise CleanupSafetyError(
                'invalid_agent_home',
                'persisted rollout path is outside the validated Codex home',
            )
        snapshot = self._read_session_snapshot(
            agent_name=target.agent_name,
            session_key=identity['session_key'],
            paths=paths,
            deadline_monotonic=deadline_monotonic,
        )
        stored = self._read_binding(
            entry_key=identity['binding_entry_key'],
            shared_state_db=paths['shared_state_db'],
            deadline_monotonic=deadline_monotonic,
        )
        binding = stored.get('binding') if isinstance(stored, dict) else None
        if snapshot.get('present') is True:
            if (
                snapshot.get('session_id') != identity['openclaw_session_id']
                or snapshot.get('lifecycle_revision')
                != identity['openclaw_lifecycle_revision']
                or snapshot.get('session_updated_at')
                != identity['openclaw_session_updated_at']
            ):
                raise CleanupSafetyError(
                    'openclaw_session_generation_mismatch',
                    'session generation changed before cleanup',
                )
            if snapshot.get('active') is True:
                raise CleanupSafetyError(
                    'openclaw_session_active',
                    'refusing to delete an active OpenClaw session',
                )
            if (
                stored is None
                or stored.get('state') != 'active'
                or stored.get('sessionId') != identity['openclaw_session_id']
                or not isinstance(binding, dict)
                or binding.get('threadId') != identity['codex_thread_id']
            ):
                raise CleanupSafetyError(
                    'codex_binding_identity_mismatch',
                    'current Codex binding no longer matches the immutable cleanup identity',
                )
        elif stored is not None and stored.get('state') == 'active' and (
            stored.get('sessionId') != identity['openclaw_session_id']
            or not isinstance(binding, dict)
            or binding.get('threadId') != identity['codex_thread_id']
        ):
            raise CleanupSafetyError(
                'codex_binding_identity_mismatch',
                'active Codex binding was rebound after the cleanup checkpoint',
            )
        native_record = self._native_thread_record(
            state_db=paths['codex_state_db'],
            thread_id=identity['codex_thread_id'],
            deadline_monotonic=deadline_monotonic,
        )
        if (
            native_record is not None
            and Path(native_record['rollout_path']).expanduser().resolve() != rollout_path
        ):
            raise CleanupSafetyError(
                'codex_thread_identity_mismatch',
                'current Codex thread record no longer matches the immutable rollout path',
            )
        return {
            'ok': True,
            'identity': identity,
            'openclaw_session_present': snapshot.get('present') is True,
        }

    def _assert_openclaw_generation_absent(
        self,
        *,
        identity: dict[str, Any],
        deadline_monotonic: float | None = None,
    ) -> dict[str, Any]:
        try:
            paths = self._resolve_agent_runtime_paths(agent_name=identity['agent_name'])
            snapshot = self._read_session_snapshot(
                agent_name=identity['agent_name'],
                session_key=identity['session_key'],
                paths=paths,
                deadline_monotonic=deadline_monotonic,
            )
        except (CleanupSafetyError, OSError, sqlite3.Error, ValueError) as exc:
            reason = exc.reason if isinstance(exc, CleanupSafetyError) else 'openclaw_generation_read_failed'
            return {'ok': False, 'deleted': False, 'reason': reason, 'error': str(exc)}
        if snapshot.get('present') is True:
            return {
                'ok': False,
                'deleted': False,
                'reason': 'openclaw_session_delete_not_committed',
                'error': 'OpenClaw session generation remains present after sessions.delete',
            }
        return {'ok': True}

    def _binding_owner_entry_keys(
        self,
        *,
        thread_id: str,
        deadline_monotonic: float | None = None,
    ) -> list[str]:
        state_root = Path(settings.openclaw_state_dir).expanduser().resolve(strict=True)
        shared_state_db = state_root / 'state' / 'openclaw.sqlite'
        with self._read_only_sqlite(
            shared_state_db,
            deadline_monotonic=deadline_monotonic,
        ) as connection:
            rows = connection.execute(
                '''
                SELECT entry_key, value_json
                FROM plugin_state_entries
                WHERE plugin_id = ?
                  AND namespace = ?
                  AND instr(value_json, ?) > 0
                ''',
                ('codex', self._CODEX_BINDING_NAMESPACE, thread_id),
            ).fetchall()
        owners: list[str] = []
        for entry_key, value_json in rows:
            stored = json.loads(value_json)
            binding = stored.get('binding') if isinstance(stored, dict) else None
            if (
                isinstance(binding, dict)
                and stored.get('state') == 'active'
                and binding.get('threadId') == thread_id
            ):
                owners.append(str(entry_key))
        return sorted(owners)

    def _native_thread_record(
        self,
        *,
        state_db: Path,
        thread_id: str,
        deadline_monotonic: float | None = None,
    ) -> dict[str, Any] | None:
        with self._read_only_sqlite(
            state_db,
            deadline_monotonic=deadline_monotonic,
        ) as connection:
            row = connection.execute(
                'SELECT id, rollout_path FROM threads WHERE id = ?',
                (thread_id,),
            ).fetchone()
        if row is None:
            return None
        return {'id': row[0], 'rollout_path': row[1]}

    def _assess_native_cleanup(
        self,
        *,
        identity: dict[str, Any],
        deadline_monotonic: float,
    ) -> dict[str, Any]:
        state_db = Path(identity['codex_state_db'])
        thread_id = identity['codex_thread_id']
        rollout_path = Path(identity['codex_rollout_path']).expanduser()
        try:
            native_record = self._native_thread_record(
                state_db=state_db,
                thread_id=thread_id,
                deadline_monotonic=deadline_monotonic,
            )
            rollout_present = os.path.lexists(rollout_path)
            if native_record is not None and Path(
                native_record['rollout_path']
            ).expanduser().resolve() != rollout_path.resolve():
                raise CleanupSafetyError(
                    'codex_thread_identity_mismatch',
                    'Codex thread rollout changed before offline maintenance',
                )
            if native_record is not None or rollout_present:
                if native_record is None:
                    maintenance_reason = 'native_row_missing_rollout_present'
                elif rollout_present:
                    maintenance_reason = 'native_row_and_rollout_present'
                else:
                    maintenance_reason = 'native_row_present_rollout_missing'
                return {
                    'ok': False,
                    'outcome': 'offline_maintenance_required',
                    'reason': 'native_delete_requires_offline_maintenance',
                    'maintenance_reason': maintenance_reason,
                    'retryable': True,
                    'offline_maintenance_required': True,
                    'native_row_present': native_record is not None,
                    'rollout_present': rollout_present,
                    'thread_id': thread_id,
                    'error': (
                        'native Codex state remains; hard delete is owned by the '
                        'stopped-Gateway offline maintenance controller'
                    ),
                }
            return {
                'ok': True,
                'outcome': 'already_absent',
                'thread_id': thread_id,
                'native_row_present': False,
                'rollout_present': False,
            }
        except (CleanupSafetyError, OSError, sqlite3.Error, ValueError) as exc:
            reason = (
                exc.reason
                if isinstance(exc, CleanupSafetyError)
                else 'codex_native_state_read_failed'
            )
            return {
                'ok': False,
                'outcome': 'failed',
                'reason': reason,
                'error_type': type(exc).__name__,
                'error': str(exc),
            }

    def enqueue_deferred(
        self,
        *,
        targets: Sequence[SessionCleanupTarget],
        reason: str,
        deadline_monotonic: float | None = None,
    ) -> dict[str, Any]:
        unique_targets = list({target.session_key: target for target in targets}.values())
        if not unique_targets:
            return {
                'status': 'noop',
                'queued': 0,
                'inserted': 0,
                'already_queued': 0,
                'blocked': 0,
                'persistence_confirmed': True,
                'audit_persistence_confirmed': True,
                'manual_reconciliation_required': False,
            }

        if deadline_monotonic is not None:
            # Cleanup prequeueing uses an isolated, unpooled connection below.
            # Refuse work only when even its bounded connect cannot fit.
            self._require_queue_persistence_budget(deadline_monotonic)

        now = datetime.now(UTC)
        values: list[dict[str, Any]] = []
        identity_captured = 0
        for target in unique_targets:
            if deadline_monotonic is not None:
                self._require_time_remaining(deadline_monotonic)
            captured = self._capture_native_cleanup_identity(
                target=target,
                deadline_monotonic=deadline_monotonic,
            )
            cleanup_result = {
                'ok': False,
                'deleted': False,
                'deferred': True,
                'reason': reason,
            }
            if captured.get('ok') is True:
                cleanup_result['native_cleanup_identity'] = captured['identity']
                identity_captured += 1
            else:
                cleanup_result.update(
                    {
                        'deferred': False,
                        'blocked': True,
                        'reason': 'native_cleanup_identity_unavailable',
                        'requested_defer_reason': reason,
                        'manual_reconciliation_required': True,
                    }
                )
                cleanup_result['identity_capture'] = captured
            values.append(
                {
                    'market_date': target.market_date,
                    'symbol': target.symbol,
                    'agent_name': target.agent_name,
                    'run_id': target.run_id,
                    'session_key': target.session_key,
                    'provider': 'openclaw',
                    'delete_transcript': target.delete_transcript,
                    'emit_lifecycle_hooks': target.emit_lifecycle_hooks,
                    'cleanup_attempts': 0,
                    'last_attempt_at': None,
                    'next_attempt_at': now,
                    'last_error': reason,
                    'cleanup_result': cleanup_result,
                }
            )
        if deadline_monotonic is not None:
            self._require_queue_persistence_budget(deadline_monotonic)
        with session_scope(
            statement_timeout_ms=settings.agent_feedback_db_statement_timeout_ms,
            lock_timeout_ms=settings.agent_feedback_db_lock_timeout_ms,
            deadline_monotonic=deadline_monotonic,
            isolated_connection=True,
        ) as session:
            stmt = (
                insert(AgentFeedbackSessionCleanupFailure)
                .values(values)
                .on_conflict_do_nothing(
                    constraint='uq_agent_feedback_session_cleanup_failures_session_key'
                )
                .returning(AgentFeedbackSessionCleanupFailure.id)
            )
            inserted = len(session.execute(stmt).scalars().all())
            stored_rows = session.execute(
                select(AgentFeedbackSessionCleanupFailure).where(
                    AgentFeedbackSessionCleanupFailure.session_key.in_(
                        [target.session_key for target in unique_targets]
                    )
                )
            ).scalars().all()
            stored_results = {
                row.session_key: row.cleanup_result for row in stored_rows
            }

        requested = len(unique_targets)
        queued = sum(
            1
            for target in unique_targets
            if self._identity_from_cleanup_result(
                stored_results.get(target.session_key)
            )
            is not None
        )
        blocked = requested - queued
        audit_persistence_confirmed = len(stored_results) == requested
        if blocked == 0 and audit_persistence_confirmed:
            status = 'queued'
        elif queued == 0:
            status = 'blocked'
        else:
            status = 'partially_blocked'
        return {
            'status': status,
            'requested': requested,
            'queued': queued,
            'blocked': blocked,
            'inserted': inserted,
            'already_queued': requested - inserted,
            'persistence_confirmed': (
                audit_persistence_confirmed and blocked == 0
            ),
            'audit_persistence_confirmed': audit_persistence_confirmed,
            'manual_reconciliation_required': blocked > 0,
            'identity_captured': identity_captured,
            'identity_missing': requested - identity_captured,
        }

    def run_pending_cleanup(
        self,
        *,
        limit: int = 100,
        max_runtime_seconds: float | None = None,
    ) -> dict[str, Any]:
        deadline_monotonic = (
            time.monotonic() + max(0.0, float(max_runtime_seconds))
            if max_runtime_seconds is not None
            else None
        )
        now = datetime.now(UTC)
        if deadline_monotonic is not None:
            try:
                self._require_database_budget(deadline_monotonic)
            except CleanupSafetyError:
                return {
                    'ok': True,
                    'summary': {
                        'selected': 0,
                        'attempted': 0,
                        'deleted': 0,
                        'failed': 0,
                        'deferred_for_budget': 0,
                        'budget_exhausted': True,
                        'selection_deferred_for_budget': True,
                    },
                    'items': [],
                }
        try:
            with session_scope(
                statement_timeout_ms=settings.agent_feedback_db_statement_timeout_ms,
                lock_timeout_ms=settings.agent_feedback_db_lock_timeout_ms,
                deadline_monotonic=deadline_monotonic,
                isolated_connection=deadline_monotonic is not None,
            ) as session:
                stmt: Select[tuple[AgentFeedbackSessionCleanupFailure]] = (
                    select(AgentFeedbackSessionCleanupFailure)
                    .where(
                        (AgentFeedbackSessionCleanupFailure.next_attempt_at.is_(None))
                        | (AgentFeedbackSessionCleanupFailure.next_attempt_at <= now)
                    )
                    .where(
                        func.coalesce(
                            AgentFeedbackSessionCleanupFailure.cleanup_result[
                                'manual_reconciliation_required'
                            ].as_boolean(),
                            False,
                        ).is_(False)
                    )
                    .order_by(AgentFeedbackSessionCleanupFailure.next_attempt_at.asc().nullsfirst(), AgentFeedbackSessionCleanupFailure.id.asc())
                    .limit(limit)
                )
                rows = session.execute(stmt).scalars().all()
                targets = [self._target_from_failure(row) for row in rows]
        except TimeoutError:
            return {
                'ok': True,
                'summary': {
                    'selected': 0,
                    'attempted': 0,
                    'deleted': 0,
                    'failed': 0,
                    'deferred_for_budget': 0,
                    'budget_exhausted': True,
                    'selection_deferred_for_budget': True,
                },
                'items': [],
            }

        processed: list[dict[str, Any]] = []
        budget_exhausted = False
        per_item_timeout_seconds = max(
            1.0,
            float(settings.openclaw_session_cleanup_timeout_seconds or 30.0),
        )
        for target in targets:
            if (
                deadline_monotonic is not None
                and deadline_monotonic - time.monotonic()
                < per_item_timeout_seconds
            ):
                budget_exhausted = True
                break
            processed.append(
                self.delete_now(
                    target=target,
                    deadline_monotonic=deadline_monotonic,
                )
            )
        return {
            'ok': True,
            'summary': {
                'selected': len(targets),
                'attempted': len(processed),
                'deleted': sum(1 for item in processed if item.get('status') == 'deleted'),
                'failed': sum(1 for item in processed if item.get('status') == 'failed'),
                'deferred_for_budget': len(targets) - len(processed),
                'budget_exhausted': budget_exhausted,
                'selection_deferred_for_budget': False,
            },
            'items': processed,
        }

    def list_failures(
        self,
        *,
        market_date: date | None = None,
        agent_name: str | None = None,
        limit: int = 100,
    ) -> dict[str, Any]:
        with session_scope() as session:
            stmt: Select[tuple[AgentFeedbackSessionCleanupFailure]] = select(AgentFeedbackSessionCleanupFailure)
            if market_date is not None:
                stmt = stmt.where(AgentFeedbackSessionCleanupFailure.market_date == market_date)
            if agent_name:
                stmt = stmt.where(AgentFeedbackSessionCleanupFailure.agent_name == agent_name)
            stmt = stmt.order_by(AgentFeedbackSessionCleanupFailure.id.desc()).limit(limit)
            rows = session.execute(stmt).scalars().all()
            return {'items': [self._serialize_failure(row) for row in rows]}

    def _record_failure(
        self,
        *,
        target: SessionCleanupTarget,
        result: dict[str, Any],
        deadline_monotonic: float,
    ) -> dict[str, Any]:
        self._require_database_budget(deadline_monotonic)
        now = datetime.now(UTC)
        raw_error = result.get('error')
        if not self._has_error_content(raw_error):
            raw_error = result.get('stderr')
        last_error = self._normalize_last_error(raw_error)
        with session_scope(
            statement_timeout_ms=settings.agent_feedback_db_statement_timeout_ms,
            lock_timeout_ms=settings.agent_feedback_db_lock_timeout_ms,
            deadline_monotonic=deadline_monotonic,
            isolated_connection=True,
        ) as session:
            existing = session.execute(
                select(AgentFeedbackSessionCleanupFailure).where(AgentFeedbackSessionCleanupFailure.session_key == target.session_key)
            ).scalar_one_or_none()
            attempts = int(existing.cleanup_attempts or 0) + 1 if existing else 1
            next_attempt_at = now + timedelta(seconds=self._retry_delay_seconds(attempts))
            stmt = insert(AgentFeedbackSessionCleanupFailure).values(
                market_date=target.market_date,
                symbol=target.symbol,
                agent_name=target.agent_name,
                run_id=target.run_id,
                session_key=target.session_key,
                provider='openclaw',
                delete_transcript=target.delete_transcript,
                emit_lifecycle_hooks=target.emit_lifecycle_hooks,
                cleanup_attempts=attempts,
                last_attempt_at=now,
                next_attempt_at=next_attempt_at,
                last_error=last_error,
                cleanup_result=result,
            )
            stmt = stmt.on_conflict_do_update(
                constraint='uq_agent_feedback_session_cleanup_failures_session_key',
                set_={
                    'market_date': target.market_date,
                    'symbol': target.symbol,
                    'agent_name': target.agent_name,
                    'run_id': target.run_id,
                    'delete_transcript': target.delete_transcript,
                    'emit_lifecycle_hooks': target.emit_lifecycle_hooks,
                    'cleanup_attempts': attempts,
                    'last_attempt_at': now,
                    'next_attempt_at': next_attempt_at,
                    'last_error': last_error,
                    'cleanup_result': result,
                    'updated_at': now,
                },
            ).returning(AgentFeedbackSessionCleanupFailure.id)
            record_id = session.execute(stmt).scalar_one()
            record = session.get(AgentFeedbackSessionCleanupFailure, record_id)
            return self._serialize_failure(record)

    @staticmethod
    def _has_error_content(value: Any) -> bool:
        if value is None:
            return False
        if isinstance(value, str):
            return bool(value.strip())
        if isinstance(value, Mapping):
            if not value:
                return False
            if set(value) == {'message'}:
                message = value.get('message')
                return not isinstance(message, str) or bool(message.strip())
            return True
        if isinstance(value, (list, tuple, set, frozenset)):
            return bool(value)
        return True

    @staticmethod
    def _normalize_last_error(value: Any) -> str:
        if isinstance(value, str):
            normalized = value.strip()
            if normalized:
                return normalized
        if isinstance(value, Mapping):
            message = value.get('message')
            if isinstance(message, str) and message.strip():
                return message.strip()
        if value is not None:
            return json.dumps(
                value,
                ensure_ascii=False,
                sort_keys=True,
                default=str,
            )
        return 'session delete failed'

    def _clear_failure(
        self,
        *,
        session_key: str,
        deadline_monotonic: float,
    ) -> None:
        self._require_database_budget(deadline_monotonic)
        with session_scope(
            statement_timeout_ms=settings.agent_feedback_db_statement_timeout_ms,
            lock_timeout_ms=settings.agent_feedback_db_lock_timeout_ms,
            deadline_monotonic=deadline_monotonic,
            isolated_connection=True,
        ) as session:
            record = session.execute(
                select(AgentFeedbackSessionCleanupFailure).where(AgentFeedbackSessionCleanupFailure.session_key == session_key)
            ).scalar_one_or_none()
            if record is not None:
                session.delete(record)

    def _invoke_delete(
        self,
        *,
        session_key: str,
        agent_name: str,
        expected_session_id: str,
        expected_lifecycle_revision: str | None,
        expected_session_updated_at: int | float,
        delete_transcript: bool,
        emit_lifecycle_hooks: bool,
        deadline_monotonic: float,
    ) -> dict[str, Any]:
        token = settings.openclaw_gateway_token
        if not token:
            return {
                'ok': False,
                'deleted': False,
                'error': 'OPENCLAW_GATEWAY_TOKEN is not configured',
            }
        delete_params: dict[str, Any] = {
            'key': session_key,
            'agentId': agent_name,
            'expectedSessionId': expected_session_id,
            'expectedSessionUpdatedAt': expected_session_updated_at,
            'deleteTranscript': delete_transcript,
            'emitLifecycleHooks': emit_lifecycle_hooks,
        }
        if expected_lifecycle_revision is not None:
            delete_params['expectedLifecycleRevision'] = expected_lifecycle_revision
        params = json.dumps(
            delete_params,
            ensure_ascii=False,
        )
        cmd = [
            'openclaw', 'gateway', 'call', 'sessions.delete', '--json', '--params', params,
        ]
        process_env = {
            **os.environ,
            'OPENCLAW_GATEWAY_TOKEN': token,
        }
        try:
            timeout_seconds = self._subprocess_timeout(deadline_monotonic)
        except CleanupSafetyError as exc:
            return {
                'ok': False,
                'deleted': False,
                'reason': exc.reason,
                'error': str(exc),
            }
        try:
            completed = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=timeout_seconds,
                env=process_env,
            )
        except subprocess.TimeoutExpired as exc:
            stdout = (exc.stdout or '').strip() if isinstance(exc.stdout, str) else ''
            stderr = (exc.stderr or '').strip() if isinstance(exc.stderr, str) else ''
            return {
                'ok': False,
                'deleted': False,
                'returncode': None,
                'stdout': stdout,
                'stderr': stderr,
                'parsed': None,
                'timeout_seconds': timeout_seconds,
                'error': f'sessions.delete timed out after {timeout_seconds:g}s',
            }
        stdout = completed.stdout.strip()
        stderr = completed.stderr.strip()
        parsed = self._extract_json(stdout) if stdout else None
        response_shape_valid = (
            isinstance(parsed, dict)
            and isinstance(parsed.get('ok'), bool)
            and isinstance(parsed.get('deleted'), bool)
        )
        ok = completed.returncode == 0 and response_shape_valid and parsed.get('ok') is True
        deleted = parsed['deleted'] if response_shape_valid else False
        error = None
        if not ok:
            parsed_error = parsed.get('error') if isinstance(parsed, dict) else None
            if isinstance(parsed_error, dict):
                parsed_error = parsed_error.get('message') or json.dumps(
                    parsed_error,
                    ensure_ascii=False,
                    sort_keys=True,
                )
            error = stderr or parsed_error
            if not error:
                error = (
                    'sessions.delete returned an invalid response'
                    if not response_shape_valid
                    else 'sessions.delete failed'
                )
        return {
            'ok': ok,
            'deleted': deleted,
            'returncode': completed.returncode,
            'stdout': stdout,
            'stderr': stderr,
            'parsed': parsed,
            'error': error,
        }

    @staticmethod
    def _extract_json(stdout: str) -> dict[str, Any] | None:
        decoder = json.JSONDecoder()
        content = stdout.rstrip()
        for index in range(len(content) - 1, -1, -1):
            if content[index] != '{':
                continue
            try:
                parsed, end_index = decoder.raw_decode(content, index)
            except json.JSONDecodeError:
                continue
            if isinstance(parsed, dict) and not content[end_index:].strip():
                return parsed
        return None

    @staticmethod
    def _retry_delay_seconds(attempts: int) -> int:
        return min(3600, 60 * max(1, attempts))

    @staticmethod
    def _target_from_failure(record: AgentFeedbackSessionCleanupFailure) -> SessionCleanupTarget:
        return SessionCleanupTarget(
            market_date=record.market_date,
            symbol=record.symbol,
            agent_name=record.agent_name,
            run_id=record.run_id,
            session_key=record.session_key,
            delete_transcript=record.delete_transcript,
            emit_lifecycle_hooks=record.emit_lifecycle_hooks,
            retry_cleanup_result=(
                dict(record.cleanup_result)
                if isinstance(record.cleanup_result, dict)
                else {}
            ),
        )

    @staticmethod
    def _serialize_failure(record: AgentFeedbackSessionCleanupFailure | None) -> dict[str, Any]:
        if record is None:
            return {}
        return {
            'id': record.id,
            'market_date': record.market_date.isoformat(),
            'symbol': record.symbol,
            'agent_name': record.agent_name,
            'run_id': record.run_id,
            'session_key': record.session_key,
            'cleanup_attempts': record.cleanup_attempts,
            'last_attempt_at': record.last_attempt_at.isoformat() if record.last_attempt_at else None,
            'next_attempt_at': record.next_attempt_at.isoformat() if record.next_attempt_at else None,
            'last_error': record.last_error,
            'cleanup_result': record.cleanup_result,
        }


agent_feedback_session_cleanup_service = AgentFeedbackSessionCleanupService()
