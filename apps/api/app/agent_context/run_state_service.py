from __future__ import annotations

from datetime import date, datetime, timezone
from typing import Any

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert

from app.db import session_scope
from app.db_models import AgentContextFeedbackRun


class AgentContextFeedbackRunStateService:
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

    def start_run(
        self,
        *,
        run_id: str,
        market_date: date,
        symbols_total: int,
        request_payload: dict[str, Any],
        summary_payload: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        now = datetime.now(timezone.utc)
        with session_scope() as session:
            stmt = insert(AgentContextFeedbackRun).values(
                run_id=run_id,
                market_date=market_date,
                status='running',
                symbols_total=symbols_total,
                completed_count=0,
                failed_count=0,
                request_payload=request_payload,
                summary_payload=summary_payload,
                started_at=now,
                completed_at=None,
            )
            stmt = stmt.on_conflict_do_update(
                index_elements=['run_id'],
                set_={
                    'market_date': market_date,
                    'status': 'running',
                    'symbols_total': symbols_total,
                    'completed_count': 0,
                    'failed_count': 0,
                    'request_payload': request_payload,
                    'summary_payload': summary_payload,
                    'started_at': now,
                    'completed_at': None,
                    'updated_at': now,
                },
            )
            session.execute(stmt)
        return {'ok': True, 'run_id': run_id, 'status': 'running'}

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

    def get_run(self, *, run_id: str) -> dict[str, Any] | None:
        with session_scope() as session:
            row = session.execute(
                select(AgentContextFeedbackRun).where(AgentContextFeedbackRun.run_id == run_id)
            ).scalar_one_or_none()
            if row is None:
                return None
            return self._serialize_run(row)

    def find_latest_run(
        self,
        *,
        market_date: date,
        requested_symbols: list[str] | None = None,
        requested_roles: list[str] | None = None,
    ) -> dict[str, Any] | None:
        with session_scope() as session:
            serialized_rows = [
                self._serialize_run(row)
                for row in session.execute(
                select(AgentContextFeedbackRun)
                .where(AgentContextFeedbackRun.market_date == market_date)
                .order_by(AgentContextFeedbackRun.started_at.desc(), AgentContextFeedbackRun.id.desc())
                ).scalars().all()
            ]

        normalized_symbols = list(requested_symbols or [])
        normalized_roles = list(requested_roles or [])
        for row in serialized_rows:
            request_payload = row.get('request_payload') if isinstance(row.get('request_payload'), dict) else {}
            run_symbols = request_payload.get('symbols')
            if not isinstance(run_symbols, list):
                run_symbols = request_payload.get('requested_symbols')
            run_roles = request_payload.get('roles')
            if normalized_symbols and list(run_symbols or []) != normalized_symbols:
                continue
            if normalized_roles and list(run_roles or []) != normalized_roles:
                continue
            return row
        return None


agent_context_feedback_run_state_service = AgentContextFeedbackRunStateService()
