from __future__ import annotations

import json
import subprocess
import time
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from typing import Any, Sequence

from sqlalchemy import Select, select
from sqlalchemy.dialects.postgresql import insert

from app.core.settings import settings
from app.db import session_scope
from app.db_models import AgentFeedbackSessionCleanupFailure


class CleanupPersistenceDeadlineExceeded(TimeoutError):
    """Raised before a cleanup retry write that cannot fit its caller budget."""


@dataclass(frozen=True)
class SessionCleanupTarget:
    market_date: date
    symbol: str
    agent_name: str
    run_id: str | None
    session_key: str
    delete_transcript: bool = True
    emit_lifecycle_hooks: bool = False


class AgentFeedbackSessionCleanupService:
    def delete_now(self, *, target: SessionCleanupTarget) -> dict[str, Any]:
        result = self._invoke_delete(
            session_key=target.session_key,
            delete_transcript=target.delete_transcript,
            emit_lifecycle_hooks=target.emit_lifecycle_hooks,
        )
        if result.get('ok') is True and isinstance(result.get('deleted'), bool):
            self._clear_failure(session_key=target.session_key)
            return {
                'status': 'deleted',
                'outcome': 'deleted' if result.get('deleted') else 'already_absent',
                'session_key': target.session_key,
                'result': result,
            }
        failure = self._record_failure(target=target, result=result)
        return {
            'status': 'failed',
            'session_key': target.session_key,
            'result': result,
            'failure': failure,
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
                'persistence_confirmed': True,
            }

        if deadline_monotonic is not None:
            remaining_seconds = deadline_monotonic - time.monotonic()
            # The shared QueuePool can wait this long before it raises. Do not
            # begin a checkout when its worst-case wait already exceeds the
            # remaining caller budget.
            if remaining_seconds <= float(settings.db_pool_timeout_seconds):
                raise CleanupPersistenceDeadlineExceeded(
                    'cleanup retry prequeue deadline cannot bound database pool checkout'
                )

        now = datetime.now(UTC)
        cleanup_result = {
            'ok': False,
            'deleted': False,
            'deferred': True,
            'reason': reason,
        }
        values = [
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
            for target in unique_targets
        ]
        with session_scope(
            statement_timeout_ms=settings.agent_feedback_db_statement_timeout_ms,
            lock_timeout_ms=settings.agent_feedback_db_lock_timeout_ms,
            deadline_monotonic=deadline_monotonic,
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

        queued = len(unique_targets)
        return {
            'status': 'queued',
            'queued': queued,
            'inserted': inserted,
            'already_queued': queued - inserted,
            'persistence_confirmed': True,
        }

    def run_pending_cleanup(self, *, limit: int = 100) -> dict[str, Any]:
        now = datetime.now(UTC)
        with session_scope() as session:
            stmt: Select[tuple[AgentFeedbackSessionCleanupFailure]] = (
                select(AgentFeedbackSessionCleanupFailure)
                .where(
                    (AgentFeedbackSessionCleanupFailure.next_attempt_at.is_(None))
                    | (AgentFeedbackSessionCleanupFailure.next_attempt_at <= now)
                )
                .order_by(AgentFeedbackSessionCleanupFailure.next_attempt_at.asc().nullsfirst(), AgentFeedbackSessionCleanupFailure.id.asc())
                .limit(limit)
            )
            rows = session.execute(stmt).scalars().all()
            targets = [self._target_from_failure(row) for row in rows]

        processed = [self.delete_now(target=target) for target in targets]
        return {
            'ok': True,
            'summary': {
                'selected': len(targets),
                'deleted': sum(1 for item in processed if item.get('status') == 'deleted'),
                'failed': sum(1 for item in processed if item.get('status') == 'failed'),
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

    def _record_failure(self, *, target: SessionCleanupTarget, result: dict[str, Any]) -> dict[str, Any]:
        now = datetime.now(UTC)
        with session_scope(
            statement_timeout_ms=settings.agent_feedback_db_statement_timeout_ms,
            lock_timeout_ms=settings.agent_feedback_db_lock_timeout_ms,
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
                last_error=result.get('error') or result.get('stderr') or 'session delete failed',
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
                    'last_error': result.get('error') or result.get('stderr') or 'session delete failed',
                    'cleanup_result': result,
                    'updated_at': now,
                },
            ).returning(AgentFeedbackSessionCleanupFailure.id)
            record_id = session.execute(stmt).scalar_one()
            record = session.get(AgentFeedbackSessionCleanupFailure, record_id)
            return self._serialize_failure(record)

    def _clear_failure(self, *, session_key: str) -> None:
        with session_scope() as session:
            record = session.execute(
                select(AgentFeedbackSessionCleanupFailure).where(AgentFeedbackSessionCleanupFailure.session_key == session_key)
            ).scalar_one_or_none()
            if record is not None:
                session.delete(record)

    def _invoke_delete(self, *, session_key: str, delete_transcript: bool, emit_lifecycle_hooks: bool) -> dict[str, Any]:
        token = settings.openclaw_gateway_token
        if not token:
            return {
                'ok': False,
                'deleted': False,
                'error': 'OPENCLAW_GATEWAY_TOKEN is not configured',
            }
        params = json.dumps(
            {
                'key': session_key,
                'deleteTranscript': delete_transcript,
                'emitLifecycleHooks': emit_lifecycle_hooks,
            },
            ensure_ascii=False,
        )
        cmd = [
            'openclaw', 'gateway', 'call', 'sessions.delete', '--json', '--token', token, '--params', params,
        ]
        timeout_seconds = max(1.0, float(settings.openclaw_session_cleanup_timeout_seconds or 30.0))
        try:
            completed = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=timeout_seconds,
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
