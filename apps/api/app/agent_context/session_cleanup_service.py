from __future__ import annotations

import json
import subprocess
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from typing import Any

from sqlalchemy import Select, select
from sqlalchemy.dialects.postgresql import insert

from app.core.settings import settings
from app.db import session_scope
from app.db_models import AgentFeedbackSessionCleanupFailure


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
        if result.get('ok') and result.get('deleted'):
            self._clear_failure(session_key=target.session_key)
            return {
                'status': 'deleted',
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
        with session_scope() as session:
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
            'openclaw', 'gateway', 'call', 'sessions.delete', '--token', token, '--params', params,
        ]
        completed = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=max(30, int(settings.openclaw_agent_timeout_seconds)),
        )
        stdout = completed.stdout.strip()
        stderr = completed.stderr.strip()
        parsed = self._extract_json(stdout) if stdout else None
        ok = completed.returncode == 0 and isinstance(parsed, dict) and bool(parsed.get('ok'))
        deleted = bool(parsed.get('deleted')) if isinstance(parsed, dict) else False
        return {
            'ok': ok,
            'deleted': deleted,
            'returncode': completed.returncode,
            'stdout': stdout,
            'stderr': stderr,
            'parsed': parsed,
            'error': None if ok and deleted else (stderr or (parsed.get('error') if isinstance(parsed, dict) else None)),
        }

    @staticmethod
    def _extract_json(stdout: str) -> dict[str, Any] | None:
        lines = [line.strip() for line in stdout.splitlines() if line.strip()]
        for line in reversed(lines):
            if not line.startswith('{'):
                continue
            try:
                parsed = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(parsed, dict):
                return parsed
        try:
            parsed = json.loads(stdout)
        except json.JSONDecodeError:
            return None
        return parsed if isinstance(parsed, dict) else None

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
