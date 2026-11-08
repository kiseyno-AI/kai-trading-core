from datetime import date, datetime, timezone
from types import SimpleNamespace
from uuid import uuid4

from sqlalchemy import delete

from app.agent_context.retry_service import AgentFeedbackRetryService
from app.db import session_scope
from app.db_models import AgentContextFeedback, AgentContextFeedbackRun


def test_incomplete_symbols_only_accepts_same_cutoff_terminal_feedback():
    market_date = date(2001, 1, 3)
    run_id = f'retry-cutoff-{uuid4().hex}'
    recorded_cutoff = datetime(2001, 1, 3, 7, tzinfo=timezone.utc)
    earlier_cutoff = datetime(2001, 1, 3, 6, tzinfo=timezone.utc)
    symbol = '005930'
    role = 'eod-summarizer'
    with session_scope() as session:
        session.add(AgentContextFeedbackRun(
            run_id=run_id,
            market_date=market_date,
            status='completed',
            symbols_total=1,
            completed_count=1,
            failed_count=0,
            request_payload={'knowledge_cutoff_at': recorded_cutoff.isoformat()},
            started_at=recorded_cutoff,
            completed_at=recorded_cutoff,
        ))
        session.add(AgentContextFeedback(
            run_id=run_id,
            market_date=market_date,
            symbol=symbol,
            agent_name=role,
            summary_payload={},
            feedback_payload={},
            received_at=recorded_cutoff,
        ))
    try:
        service = AgentFeedbackRetryService()
        common = {
            'market_date': market_date,
            'roles': [role],
            'eligible_symbols': [symbol],
            'reuse_decisions': {symbol: {role: SimpleNamespace(should_dispatch=True)}},
        }
        assert service.load_incomplete_symbols(**common) == []
        assert service.load_incomplete_symbols(
            **common, knowledge_cutoff_at=recorded_cutoff,
        ) == []
        assert service.load_incomplete_symbols(
            **common, knowledge_cutoff_at=earlier_cutoff,
        ) == [symbol]
    finally:
        with session_scope() as session:
            session.execute(delete(AgentContextFeedback).where(
                AgentContextFeedback.run_id == run_id,
            ))
            session.execute(delete(AgentContextFeedbackRun).where(
                AgentContextFeedbackRun.run_id == run_id,
            ))
