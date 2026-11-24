from copy import deepcopy
from datetime import date, datetime, timezone
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from app.agent_context import retry_service as module
from app.agent_context.retry_service import (
    AgentFeedbackRetryService, FeedbackRetryCycleConfig,
    RoleSequenceRoleState, RoleSequenceSnapshot,
)
from app.agent_context.roles import DECISION_SUPPORT_AGENT_NAMES, SUMMARIZER_AGENT_NAMES
from app.agent_context.run_modes import DAILY_RETRY_CYCLE_MODE
from tests.test_feedback_retry_cycles import RecordingRunStateService


DAY = date(2026, 11, 23)
CUTOFF = datetime(2026, 11, 23, 12, tzinfo=timezone.utc)
SYMBOLS = ['005930', '000660', '035420']
FROZEN = {'signal-decider': SYMBOLS[:2], 'short-term-decider': SYMBOLS[1:]}


def _parent():
    return {
        'run_id': 'original-summary', 'market_date': DAY, 'status': 'partial',
        'completed_at': CUTOFF,
        'request_payload': {
            'mode': DAILY_RETRY_CYCLE_MODE, 'roles': list(SUMMARIZER_AGENT_NAMES),
            'symbols': SYMBOLS, 'requested_symbols': SYMBOLS,
            'role_symbol_scopes': {role: SYMBOLS for role in SUMMARIZER_AGENT_NAMES},
            'knowledge_cutoff_at': CUTOFF.isoformat(), 'fs_div': 'CFS',
            'feedback_scope_mode': 'decider_scope_fast_path', 'outer_batch_size': 20,
            'prepared_decider_role_symbol_scopes': deepcopy(FROZEN),
        },
    }


def _run(monkeypatch, *, parent=None, reuse=False, legacy_config=False):
    service = AgentFeedbackRetryService()
    state = RecordingRunStateService()
    monkeypatch.setattr(module, 'agent_context_feedback_run_state_service', state)
    monkeypatch.setattr(module, 'load_agent_feedback_eligibility_records', lambda **kw: [])
    monkeypatch.setattr(module, 'build_agent_feedback_eligibility_diagnostics', lambda **kw: {})
    monkeypatch.setattr(module, 'build_agent_feedback_role_funnel', lambda **kw: {
        'role_symbols': {
            **{role: SYMBOLS for role in SUMMARIZER_AGENT_NAMES},
            **{role: [SYMBOLS[2]] for role in DECISION_SUPPORT_AGENT_NAMES},
        },
    })
    monkeypatch.setattr(service, '_get_retry_progress', lambda **kw: {'summary': {'pending_symbols': 0}})

    def snapshot(**kw):
        return RoleSequenceSnapshot(
            market_date=DAY, symbols=tuple(kw['eligible_symbols']), roles=tuple(kw['roles']),
            role_states={role: RoleSequenceRoleState(
                role=role, total_symbols=len(scope), completed_symbols=tuple(scope),
                failed_symbols=(), terminal_symbols=tuple(scope), boundary_pending_symbols=(),
                retry_pending_symbols=(), boundary_status='completed',
            ) for role, scope in kw['role_symbol_scopes'].items()},
            role_symbol_scopes=kw['role_symbol_scopes'],
        )

    build = Mock(side_effect=snapshot)
    monkeypatch.setattr(service, '_build_role_sequence_snapshot', build)
    config = FeedbackRetryCycleConfig(
        market_date=DAY, roles=SUMMARIZER_AGENT_NAMES, symbols=tuple(SYMBOLS),
        role_symbol_scopes={role: SYMBOLS for role in SUMMARIZER_AGENT_NAMES},
        knowledge_cutoff_at=CUTOFF, fs_div='CFS',
        reuse_parent_run_id='original-summary' if reuse else None,
    )
    if legacy_config:
        config = SimpleNamespace(**{
            key: value for key, value in vars(config).items() if key != 'reuse_parent_run_id'
        })
    service._run_retry_cycles_admitted(
        config=config,
        previous_run=parent, prerequisites={'ready': True},
        preloaded_eligible_symbols=SYMBOLS, effective_knowledge_cutoff_at=CUTOFF,
    )
    return state.started[0]['request_payload']


def test_fresh_summary_selects_current_decider_ranking(monkeypatch):
    assert _run(monkeypatch)['prepared_decider_role_symbol_scopes'] == {
        role: [SYMBOLS[2]] for role in DECISION_SUPPORT_AGENT_NAMES
    }


def test_legacy_config_without_optional_reuse_parent_keeps_fresh_selection(monkeypatch):
    assert _run(monkeypatch, legacy_config=True)['prepared_decider_role_symbol_scopes'] == {
        role: [SYMBOLS[2]] for role in DECISION_SUPPORT_AGENT_NAMES
    }


def test_summary_retry_preserves_original_independent_decider_scopes(monkeypatch):
    payload = _run(monkeypatch, parent=_parent(), reuse=True)
    assert payload['prepared_decider_role_symbol_scopes'] == FROZEN
    assert payload['resumed_from_run_id'] == 'original-summary'


def test_summary_retry_preserves_definitive_empty_decider_scope(monkeypatch):
    parent = _parent()
    empty = {role: [] for role in DECISION_SUPPORT_AGENT_NAMES}
    parent['request_payload']['prepared_decider_role_symbol_scopes'] = empty
    assert _run(monkeypatch, parent=parent, reuse=True)['prepared_decider_role_symbol_scopes'] == empty


@pytest.mark.parametrize('mismatch', [
    'missing', 'run_id', 'market_date', 'knowledge_cutoff_at', 'fs_div',
    'policy_context_as_of_date', 'prepared_missing', 'prepared_outside', 'summary_cohort',
])
def test_summary_retry_rejects_unverified_or_conflicting_frozen_scope(monkeypatch, mismatch):
    parent = _parent()
    if mismatch == 'missing':
        parent = None
    elif mismatch in {'run_id', 'market_date'}:
        parent[mismatch] = 'wrong'
    elif mismatch == 'prepared_missing':
        parent['request_payload'].pop('prepared_decider_role_symbol_scopes')
    elif mismatch == 'prepared_outside':
        parent['request_payload']['prepared_decider_role_symbol_scopes']['signal-decider'] = ['999999']
    elif mismatch == 'summary_cohort':
        parent['request_payload']['role_symbol_scopes']['technical-summarizer'] = SYMBOLS[:1]
    else:
        parent['request_payload'][mismatch] = {
            'knowledge_cutoff_at': '2026-11-23T13:00:00+00:00',
            'fs_div': 'OFS', 'policy_context_as_of_date': '2026-11-24',
        }[mismatch]
    with pytest.raises(ValueError, match='frozen decider'):
        _run(monkeypatch, parent=parent, reuse=True)
