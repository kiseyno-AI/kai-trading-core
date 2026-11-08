from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timedelta, timezone
from threading import Event, get_ident
from time import monotonic
from uuid import uuid4

from sqlalchemy import delete, event, select, text

from app.agent_context.retry_service import _claim_feedback_retry_run
from app.agent_context.run_modes import DAILY_RETRY_CYCLE_MODE
from app.db import engine, session_scope
from app.db_models import AgentContextFeedbackRun


def test_retry_admission_waits_for_an_inflight_heartbeat_before_takeover():
    run_ids = [f'admission-heartbeat-{uuid4().hex}' for _ in range(2)]
    market_date = date(2001, 1, 2)
    stale_at = datetime.now(timezone.utc) - timedelta(hours=2)
    fresh_at = datetime.now(timezone.utc)
    with session_scope() as session:
        session.add(AgentContextFeedbackRun(
            run_id=run_ids[0],
            market_date=market_date,
            status='running',
            symbols_total=1,
            request_payload={
                'mode': DAILY_RETRY_CYCLE_MODE,
                'roles': ['signal-decider'],
                'requested_symbols': ['005930'],
            },
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
                    .where(AgentContextFeedbackRun.run_id == run_ids[0])
                    .with_for_update()
                ).scalar_one()
                row.updated_at = fresh_at
                heartbeat_session.flush()
                writer_pid = heartbeat_session.execute(
                    text('SELECT pg_backend_pid()')
                ).scalar_one()
                admission = executor.submit(
                    _claim_feedback_retry_run,
                    market_date=market_date,
                    roles=('signal-decider',),
                    requested_symbols=('005930',),
                    feedback_scope_mode='decider_scope_fast_path',
                    run_id=run_ids[1],
                    started_at=fresh_at,
                )
                deadline = monotonic() + 5
                with engine.connect() as observer:
                    while not reader_selected.is_set():
                        waiting_on_heartbeat = observer.execute(text(
                            'SELECT EXISTS (SELECT 1 FROM pg_stat_activity '
                            'WHERE :writer_pid = ANY(pg_blocking_pids(pid)))'
                        ), {'writer_pid': writer_pid}).scalar_one()
                        if waiting_on_heartbeat:
                            break
                        assert monotonic() < deadline
                        reader_selected.wait(timeout=0.01)
            result = admission.result(timeout=5)
            assert result['acquired'] is False
            assert result['conflict_run']['run_id'] == run_ids[0]

        with session_scope() as session:
            rows = session.execute(
                select(AgentContextFeedbackRun)
                .where(AgentContextFeedbackRun.run_id.in_(run_ids))
            ).scalars().all()
            assert len(rows) == 1
            assert rows[0].status == 'running'
            assert rows[0].updated_at == fresh_at
            assert rows[0].completed_at is None
    finally:
        event.remove(engine, 'after_cursor_execute', after_select)
        with session_scope() as session:
            session.execute(
                delete(AgentContextFeedbackRun)
                .where(AgentContextFeedbackRun.run_id.in_(run_ids))
            )
