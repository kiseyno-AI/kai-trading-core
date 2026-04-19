from __future__ import annotations

from datetime import date, datetime, timezone
from typing import Any

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert

from app.db import session_scope
from app.db_models import AgentContextFeedbackRun


class AgentContextFeedbackRunStateService:
    def start_run(
        self,
        *,
        run_id: str,
        market_date: date,
        symbols_total: int,
        request_payload: dict[str, Any],
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
                summary_payload=None,
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
                    'summary_payload': None,
                    'started_at': now,
                    'completed_at': None,
                    'updated_at': now,
                },
            )
            session.execute(stmt)
        return {'ok': True, 'run_id': run_id, 'status': 'running'}

    def finish_run(
        self,
        *,
        run_id: str,
        status: str,
        completed_count: int,
        failed_count: int,
        summary_payload: dict[str, Any],
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
            row.completed_at = now
            row.updated_at = now
        return {'ok': True, 'run_id': run_id, 'status': status}


agent_context_feedback_run_state_service = AgentContextFeedbackRunStateService()
