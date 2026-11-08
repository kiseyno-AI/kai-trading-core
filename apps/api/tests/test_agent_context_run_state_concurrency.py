from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timedelta, timezone
from threading import Event, get_ident
from uuid import uuid4

from sqlalchemy import delete, event, select

from app.agent_context.run_state_service import AgentContextFeedbackRunStateService
from app.db import engine, session_scope
from app.db_models import AgentContextFeedbackRun


def test_stale_cleanup_does_not_overwrite_an_inflight_heartbeat():
    service = AgentContextFeedbackRunStateService()
    run_id = f'stale-cleanup-heartbeat-{uuid4().hex}'
    market_date = date(2001, 1, 1)
    stale_at = datetime.now(timezone.utc) - timedelta(hours=2)
    fresh_at = datetime.now(timezone.utc)
    with session_scope() as session:
        session.add(AgentContextFeedbackRun(
            run_id=run_id,
            market_date=market_date,
            status='running',
            symbols_total=1,
            started_at=stale_at,
            updated_at=stale_at,
        ))

    reader_selected = Event()
    writer_thread_id = get_ident()

    def after_select(connection, cursor, statement, parameters, context, executemany):
        if (
            get_ident() != writer_thread_id
            and statement.lstrip().startswith('SELECT')
            and 'agent_context_feedback_runs' in statement
        ):
            reader_selected.set()

    event.listen(engine, 'after_cursor_execute', after_select)
    try:
        with ThreadPoolExecutor(max_workers=1) as executor:
            with session_scope() as heartbeat_session:
                row = heartbeat_session.execute(
                    select(AgentContextFeedbackRun)
                    .where(AgentContextFeedbackRun.run_id == run_id)
                    .with_for_update()
                ).scalar_one()
                row.updated_at = fresh_at
                heartbeat_session.flush()
                cleanup = executor.submit(
                    service._mark_stale_running_rows_abandoned,
                    market_date=market_date,
                )
                assert reader_selected.wait(timeout=5)
            assert cleanup.result(timeout=5) == 0

        with session_scope() as session:
            row = session.execute(
                select(AgentContextFeedbackRun)
                .where(AgentContextFeedbackRun.run_id == run_id)
            ).scalar_one()
            assert row.status == 'running'
            assert row.updated_at == fresh_at
            assert row.completed_at is None
    finally:
        event.remove(engine, 'after_cursor_execute', after_select)
        with session_scope() as session:
            session.execute(
                delete(AgentContextFeedbackRun)
                .where(AgentContextFeedbackRun.run_id == run_id)
            )
