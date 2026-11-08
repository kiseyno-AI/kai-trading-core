from datetime import date, datetime, timezone
from unittest.mock import patch

from app.agent_context.progress_service import AgentContextProgressService
from app.agent_context.runtime_state import RuntimeFeedbackRunState


def test_completed_progress_preserves_an_empty_role_scope():
    roles = ['eod-summarizer', 'signal-decider']
    state = RuntimeFeedbackRunState(
        run_id='completed-empty-decider-scope',
        market_date=date(2026, 5, 8),
        requested_symbols=['005930'],
        requested_roles=roles,
        role_sequence=roles,
        parallel_per_role=1,
        batch_size=1,
        started_at=datetime(2026, 5, 8, 6, tzinfo=timezone.utc),
        status='completed',
        completed_at=datetime(2026, 5, 8, 7, tzinfo=timezone.utc),
        completed_roles=roles,
        completed_symbols_by_role={'eod-summarizer': {'005930'}},
        role_symbol_scopes={'eod-summarizer': ['005930'], 'signal-decider': []},
    )
    snapshot = {
        'run_id': state.run_id,
        'status': state.status,
        'started_at': state.started_at,
        'completed_at': state.completed_at,
        'request_payload': {'symbols': state.requested_symbols, 'roles': roles},
        'summary_payload': {'progress_summary': state.to_progress_summary()},
    }
    service = AgentContextProgressService()
    with (
        patch.object(service, '_resolve_run_snapshot', return_value=snapshot),
        patch.object(service, '_build_persisted_progress', return_value={'result': {}, 'summary': {}}),
        patch('app.agent_context.progress_service.runtime_feedback_state_store.get_run', return_value=None),
        patch(
            'app.agent_context.progress_service.agent_feedback_prerequisite_service.inspect_feedback_prerequisites',
            return_value={'ready': True},
        ),
    ):
        result = service.get_feedback_progress(
            market_date=state.market_date,
            run_id=state.run_id,
        )

    assert result['result']['roles']['eod-summarizer'] == {
        'requested_count': 1,
        'completed_count': 1,
        'error_count': 0,
    }
    assert result['result']['roles']['signal-decider'] == {
        'requested_count': 0,
        'completed_count': 0,
        'error_count': 0,
    }
    empty_role = result['summary']['role_progress']['signal-decider']
    assert empty_role['total_symbols'] == 0
    assert empty_role['pending_symbols'] == 0
    assert result['summary']['expected_feedback_count'] == 1
    assert result['summary']['completed_feedback_count'] == 1
