from copy import deepcopy
from datetime import date, datetime, timezone
from unittest.mock import Mock

import pytest

from app.agent_context import retry_service
from app.agent_context.retry_service import (
    AgentFeedbackRetryService, FeedbackRetryCycleConfig, RoleSequenceRoleState, RoleSequenceSnapshot,
)
from app.agent_context.roles import DECISION_SUPPORT_AGENT_NAMES, SUMMARIZER_AGENT_NAMES
from app.agent_context.run_modes import DAILY_RETRY_CYCLE_MODE


DAY = date(2026, 11, 13)
CUTOFF = datetime(2026, 11, 13, 9, tzinfo=timezone.utc)
SYMBOLS = ['005930', '196450', '037950']
ROLE_SCOPES = {'signal-decider': SYMBOLS[:2], 'short-term-decider': [SYMBOLS[0], SYMBOLS[2]]}


def _summary():
    return {
        'run_id': 'summary-prepared', 'market_date': DAY.isoformat(), 'status': 'partial',
        'completed_at': CUTOFF.isoformat(),
        'request_payload': {
            'mode': DAILY_RETRY_CYCLE_MODE, 'roles': list(SUMMARIZER_AGENT_NAMES),
            'symbols': SYMBOLS, 'role_symbol_scopes': {role: SYMBOLS for role in SUMMARIZER_AGENT_NAMES},
            'knowledge_cutoff_at': CUTOFF.isoformat(), 'fs_div': 'CFS',
            'feedback_scope_mode': 'decider_scope_fast_path',
            'prepared_decider_role_symbol_scopes': deepcopy(ROLE_SCOPES),
        },
    }


@pytest.mark.parametrize('legacy', [False, True])
@pytest.mark.parametrize('requested', [None, (*SYMBOLS, '044820', '033560', '236200')])
def test_deciders_bind_prepared_summary_scopes_without_dropping_failed_summary_symbols(monkeypatch, legacy, requested):
    source = _summary()
    if legacy:
        source['request_payload'].pop('prepared_decider_role_symbol_scopes')
    service = AgentFeedbackRetryService()
    load = Mock(side_effect=lambda **kwargs: list(kwargs['requested_symbols'] or []))
    admitted = Mock(return_value={'status': 'waiting'})
    monkeypatch.setattr(service, '_load_eligible_symbols', load)
    monkeypatch.setattr(service, '_run_retry_cycles_admitted', admitted)
    monkeypatch.setattr(retry_service.agent_context_feedback_run_state_service, 'get_run', Mock(return_value=source))
    monkeypatch.setattr(retry_service.agent_feedback_prerequisite_service, 'inspect_feedback_prerequisites',
                        lambda **kwargs: {'ready': False, 'status': 'waiting'})
    service.run_retry_cycles(config=FeedbackRetryCycleConfig(
        market_date=DAY, roles=DECISION_SUPPORT_AGENT_NAMES, knowledge_cutoff_at=CUTOFF,
        source_summary_run_id='summary-prepared',
        symbols=requested,
    ))
    bound = admitted.call_args.kwargs['config']
    assert set(bound.symbols) == set(SYMBOLS)
    assert bound.role_symbol_scopes == (
        {role: sorted(SYMBOLS) for role in DECISION_SUPPORT_AGENT_NAMES} if legacy else ROLE_SCOPES
    )
    proof = admitted.call_args.kwargs['source_summary_scope']
    assert proof['source_summary_run_id'] == 'summary-prepared'
    assert proof['prepared_symbols'] == sorted(SYMBOLS)
    assert proof['provenance'] == ('legacy_prepared_summary_scope' if legacy else 'frozen_decider_role_scopes_v1')
    assert set(load.call_args.kwargs['requested_symbols']) == set(SYMBOLS)


@pytest.mark.parametrize('field,value', [('market_date', '2026-11-12'), ('knowledge_cutoff_at', '2026-11-13T08:00:00+00:00')])
def test_summary_scope_binding_rejects_wrong_date_or_cutoff_before_dispatch(monkeypatch, field, value):
    source = _summary()
    if field == 'market_date':
        source[field] = value
    else:
        source['request_payload'][field] = value
    monkeypatch.setattr(retry_service.agent_context_feedback_run_state_service, 'get_run', Mock(return_value=source))
    service = AgentFeedbackRetryService()
    load = Mock()
    monkeypatch.setattr(service, '_load_eligible_symbols', load)
    with pytest.raises(ValueError, match='source summary'):
        service.run_retry_cycles(config=FeedbackRetryCycleConfig(
            market_date=DAY, roles=DECISION_SUPPORT_AGENT_NAMES, knowledge_cutoff_at=CUTOFF,
            source_summary_run_id='summary-prepared',
        ))
    load.assert_not_called()


def test_summary_parent_persists_independent_decider_scopes_before_any_later_ranking(monkeypatch):
    from tests.test_feedback_retry_cycles import RecordingRunStateService

    service = AgentFeedbackRetryService()
    state = RecordingRunStateService()
    records = [
        {
            'symbol': symbol, 'eligible': True, 'hard_blocked': False, 'cap_excluded': False,
            'candidate_score': 10, 'candidate_rank': index,
            'candidate_score_payload': {'role_scores': {
                'signal-decider': 100 if index == 0 else 1,
                'short-term-decider': 100 if index == 1 else 1,
            }},
        }
        for index, symbol in enumerate(SYMBOLS)
    ]
    for role in SUMMARIZER_AGENT_NAMES:
        monkeypatch.setattr(retry_service.settings, 'openclaw_feedback_role_cap_' + role.replace('-', '_'), 2)
    for role in DECISION_SUPPORT_AGENT_NAMES:
        monkeypatch.setattr(retry_service.settings, 'openclaw_feedback_role_cap_' + role.replace('-', '_'), 1)
    monkeypatch.setattr(retry_service, 'load_agent_feedback_eligibility_records', lambda **kwargs: records)
    monkeypatch.setattr(retry_service, 'agent_context_feedback_run_state_service', state)
    monkeypatch.setattr(service, '_get_retry_progress', lambda **kwargs: {'summary': {'pending_symbols': 0}})

    def completed_snapshot(**kwargs):
        return RoleSequenceSnapshot(
            market_date=DAY, symbols=tuple(kwargs['eligible_symbols']), roles=tuple(kwargs['roles']),
            role_states={role: RoleSequenceRoleState(
                role=role, total_symbols=len(scope), completed_symbols=tuple(scope), failed_symbols=(),
                terminal_symbols=tuple(scope), boundary_pending_symbols=(), retry_pending_symbols=(),
                boundary_status='completed',
            ) for role, scope in kwargs['role_symbol_scopes'].items()},
            role_symbol_scopes=kwargs['role_symbol_scopes'],
        )

    monkeypatch.setattr(service, '_build_role_sequence_snapshot', completed_snapshot)
    service._run_retry_cycles_admitted(
        config=FeedbackRetryCycleConfig(market_date=DAY, roles=SUMMARIZER_AGENT_NAMES, knowledge_cutoff_at=CUTOFF),
        prerequisites={'ready': True}, preloaded_eligible_symbols=SYMBOLS,
        effective_knowledge_cutoff_at=CUTOFF,
    )
    prepared = state.started[0]['request_payload']
    assert prepared['prepared_decider_role_symbol_scopes'] == {
        'signal-decider': ['005930'], 'short-term-decider': ['196450'],
    }
    assert set(prepared['roles']) == set(SUMMARIZER_AGENT_NAMES)
    assert all(set(scope) == {'005930', '196450'} for scope in prepared['role_symbol_scopes'].values())


def test_bound_decider_child_dispatch_uses_the_exact_summary_parent(monkeypatch):
    from tests.test_feedback_retry_cycles import RecordingRunStateService

    role, symbol = 'signal-decider', '005930'
    service = AgentFeedbackRetryService()
    dispatch = Mock(return_value={
        'request': {'roles': [role], 'symbols': [symbol]},
        'summary': {'status': 'completed', 'completed': 1, 'failed': 0},
    })
    monkeypatch.setattr(retry_service, 'agent_context_feedback_run_state_service', RecordingRunStateService())
    monkeypatch.setattr(retry_service.agent_feedback_orchestrator, 'dispatch_for_symbols', dispatch)
    monkeypatch.setattr(retry_service, 'load_agent_feedback_eligibility_records', lambda **kwargs: [])
    monkeypatch.setattr(service, '_get_retry_progress', lambda **kwargs: {'summary': {'pending_symbols': 0}})

    def snapshot(**kwargs):
        done = dispatch.called
        return RoleSequenceSnapshot(
            market_date=DAY, symbols=(symbol,), roles=(role,), role_symbol_scopes={role: (symbol,)},
            role_states={role: RoleSequenceRoleState(
                role=role, total_symbols=1, completed_symbols=(symbol,) if done else (),
                failed_symbols=(), terminal_symbols=(symbol,) if done else (),
                boundary_pending_symbols=() if done else (symbol,),
                retry_pending_symbols=() if done else (symbol,), boundary_status='completed' if done else 'pending',
            )},
        )

    monkeypatch.setattr(service, '_build_role_sequence_snapshot', snapshot)
    service._run_retry_cycles_admitted(
        config=FeedbackRetryCycleConfig(
            market_date=DAY, roles=(role,), symbols=(symbol,), role_symbol_scopes={role: (symbol,)},
            knowledge_cutoff_at=CUTOFF, source_summary_run_id='summary-prepared',
        ),
        prerequisites={'ready': True}, preloaded_eligible_symbols=[symbol], effective_knowledge_cutoff_at=CUTOFF,
    )
    assert dispatch.call_args.kwargs['upstream_summary_run_id'] == 'summary-prepared'
    assert dispatch.call_args.kwargs['reuse_existing'] is False


def test_retry_admission_never_joins_a_different_summary_source():
    from uuid import uuid4

    from sqlalchemy import delete

    from app.db import session_scope
    from app.db_models import AgentContextFeedbackRun

    ids = [f'summary-source-binding-{uuid4().hex}' for _ in range(3)]
    common = {
        'market_date': date(2001, 1, 4), 'roles': ('signal-decider',),
        'requested_symbols': ('005930',), 'feedback_scope_mode': 'decider_scope_fast_path',
        'started_at': CUTOFF, 'admission_knowledge_cutoff_at': CUTOFF,
        'role_symbol_scopes': {'signal-decider': ['005930']}, 'fs_div': 'CFS',
    }
    try:
        first = retry_service._claim_feedback_retry_run(**common, run_id=ids[0], source_summary_run_id='summary-a')
        assert first['acquired'] is True
        other = retry_service._claim_feedback_retry_run(**common, run_id=ids[1], source_summary_run_id='summary-b')
        assert other['acquired'] is False
        assert other['exact_scope'] is False
        same = retry_service._claim_feedback_retry_run(**common, run_id=ids[2], source_summary_run_id='summary-a')
        assert same['acquired'] is False
        assert same['exact_scope'] is True
    finally:
        with session_scope() as session:
            session.execute(delete(AgentContextFeedbackRun).where(AgentContextFeedbackRun.run_id.in_(ids)))
