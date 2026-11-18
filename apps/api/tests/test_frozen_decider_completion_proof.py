from copy import deepcopy
from datetime import date, datetime, timezone
from unittest.mock import Mock, patch

import pytest

from app.agent_context import retry_service
from app.agent_context.retry_service import FeedbackRetryCycleConfig
from app.agent_context.roles import DECISION_SUPPORT_AGENT_NAMES, SUMMARIZER_AGENT_NAMES
from app.agent_context.run_modes import DAILY_RETRY_CYCLE_MODE
from app.eod.daily_pipeline_stages import DailyPipelineStageService
from app.eod.pykrx_pipeline_runner import _run_daily_signal_decider_followup
from app.orders.next_open_execution import _normalize_strict_decider_completion_authority
from tests.test_daily_pipeline_stages import (
    _FakePipelineRunStateService,
    _feedback_prerequisites,
    _next_open_completion_result,
    _terminal_decider_outcome_with_role_failures,
)


SYMBOLS = ['000660', '005930', '035420']
SCOPES = {'signal-decider': ['000660'], 'short-term-decider': ['005930']}


def frozen_outcome(*, failed=False):
    outcome, cutoff = _terminal_decider_outcome_with_role_failures(
        symbols=SYMBOLS,
        role_scopes=SCOPES,
        failed_symbols_by_role={'signal-decider': ['000660']} if failed else {},
        cutoff_at=datetime(2026, 4, 14, 8, 30, tzinfo=timezone.utc),
    )
    outcome['result']['source_summary_scope'] = {
        'source_summary_run_id': cutoff['source_run_id'],
        'knowledge_cutoff_at': cutoff['knowledge_cutoff_at'].isoformat(),
        'provenance': 'frozen_decider_role_scopes_v1',
        'prepared_symbols': list(SYMBOLS),
        'prepared_decider_role_symbol_scopes': deepcopy(SCOPES),
    }
    # A later ranking on the summary intersection cannot reproduce the source ranking.
    outcome['result']['progress']['summary']['funnel_diagnostics'] = {
        'role_diagnostics': {role: {'cap_excluded_symbols': [], 'cap_excluded_symbol_count': 0} for role in SCOPES},
    }
    return outcome, cutoff


def build_proof(outcome, cutoff):
    return DailyPipelineStageService._terminal_decider_progression_proof(
        outcome, expected_decider_symbols=SYMBOLS, feedback_cutoff_resolution=cutoff,
    )


@pytest.mark.parametrize('failed', [False, True])
def test_frozen_scope_proves_exact_independent_lanes_without_economics_claims(failed):
    outcome, cutoff = frozen_outcome(failed=failed)
    proof = build_proof(outcome, cutoff)
    assert proof is not None
    assert proof['admitted_symbols'] == SYMBOLS[:2]
    assert 'entry_economics_excluded_symbols' not in proof
    assert proof['role_symbol_scopes'] == SCOPES
    assert proof['failed_symbols_by_role']['signal-decider'] == (['000660'] if failed else [])
    assert _normalize_strict_decider_completion_authority(proof) is not None
    assert DailyPipelineStageService.validated_stage_completion_scope_proof('signal_decider', proof) == proof


def test_native_partial_summary_binding_uses_completed_intersection(monkeypatch):
    cutoff_at = datetime(2026, 4, 14, 8, 30, tzinfo=timezone.utc)
    completed_symbols = SYMBOLS[:2]
    prepared_scopes = {
        'signal-decider': ['000660', '035420'],
        'short-term-decider': ['005930', '035420'],
    }
    source = {
        'run_id': 'summary-run',
        'market_date': date(2026, 4, 14).isoformat(),
        'status': 'partial',
        'completed_at': cutoff_at.isoformat(),
        'request_payload': {
            'mode': DAILY_RETRY_CYCLE_MODE,
            'roles': list(SUMMARIZER_AGENT_NAMES),
            'symbols': list(SYMBOLS),
            'role_symbol_scopes': {
                role: list(SYMBOLS) for role in SUMMARIZER_AGENT_NAMES
            },
            'knowledge_cutoff_at': cutoff_at.isoformat(),
            'fs_div': 'CFS',
            'prepared_decider_role_symbol_scopes': prepared_scopes,
        },
    }
    monkeypatch.setattr(
        retry_service.agent_context_feedback_run_state_service,
        'get_run',
        Mock(return_value=source),
    )
    _, binding = retry_service._bind_source_summary_scope(
        FeedbackRetryCycleConfig(
            market_date=date(2026, 4, 14),
            roles=DECISION_SUPPORT_AGENT_NAMES,
            symbols=tuple(completed_symbols),
            knowledge_cutoff_at=cutoff_at,
            source_summary_run_id='summary-run',
            scope_authority={'source_summary_run_id': 'summary-run'},
        )
    )
    outcome, cutoff = _terminal_decider_outcome_with_role_failures(
        symbols=completed_symbols,
        role_scopes=SCOPES,
        failed_symbols_by_role={},
        cutoff_at=cutoff_at,
    )
    cutoff['completed_scope_proof']['admitted_symbols'] = list(SYMBOLS)
    outcome['result']['source_summary_scope'] = binding

    assert binding['prepared_symbols'] == completed_symbols
    assert binding['prepared_decider_role_symbol_scopes'] == SCOPES
    proof = DailyPipelineStageService._terminal_decider_progression_proof(
        outcome,
        expected_decider_symbols=completed_symbols,
        feedback_cutoff_resolution=cutoff,
    )
    assert proof is not None

    outcome['result']['source_summary_scope'][
        'prepared_decider_role_symbol_scopes'
    ]['signal-decider'].append('035420')
    assert DailyPipelineStageService._terminal_decider_progression_proof(
        outcome,
        expected_decider_symbols=completed_symbols,
        feedback_cutoff_resolution=cutoff,
    ) is None


def test_native_narrow_retry_cannot_shrink_frozen_completion_authority(monkeypatch):
    from tests.test_feedback_summary_scope import CUTOFF, DAY, ROLE_SCOPES, _summary

    source = _summary()
    symbols = sorted(source['request_payload']['symbols'])
    selected = {role: scope[:1] for role, scope in ROLE_SCOPES.items()}
    outcome, cutoff = _terminal_decider_outcome_with_role_failures(
        symbols=symbols, role_scopes=selected, failed_symbols_by_role={},
        cutoff_at=CUTOFF, source_summary_run_id=source['run_id'],
    )
    monkeypatch.setattr(
        retry_service.agent_context_feedback_run_state_service,
        'get_run', Mock(return_value=source),
    )
    config, binding = retry_service._bind_source_summary_scope(FeedbackRetryCycleConfig(
        market_date=DAY, roles=DECISION_SUPPORT_AGENT_NAMES, symbols=tuple(symbols),
        role_symbol_scopes=selected, knowledge_cutoff_at=CUTOFF,
        source_summary_run_id=source['run_id'], scope_authority=outcome['result']['scope_authority'],
    ))
    outcome['result']['source_summary_scope'] = binding

    assert DailyPipelineStageService._terminal_decider_progression_proof(
        outcome, expected_decider_symbols=symbols, feedback_cutoff_resolution=cutoff,
    ) is None
    assert config.symbols == tuple(symbols)
    assert config.scope_authority == outcome['result']['scope_authority']
    assert config.role_symbol_scopes == selected
    assert binding['prepared_symbols'] == symbols
    assert binding['prepared_decider_role_symbol_scopes'] == ROLE_SCOPES


@pytest.mark.parametrize('mutation', [
    'wrong_source', 'wrong_cutoff', 'naive_cutoff', 'wrong_prepared', 'swapped_roles',
    'narrowed_role', 'extra_symbol', 'unknown_role', 'unknown_field', 'unknown_provenance',
    'malformed',
])
def test_frozen_scope_binding_fails_closed(mutation):
    outcome, cutoff = frozen_outcome()
    binding = outcome['result']['source_summary_scope']
    if mutation == 'wrong_source':
        binding['source_summary_run_id'] = 'another-summary'
    elif mutation == 'wrong_cutoff':
        binding['knowledge_cutoff_at'] = '2026-04-14T08:31:00+00:00'
    elif mutation == 'naive_cutoff':
        binding['knowledge_cutoff_at'] = '2026-04-14T08:30:00'
    elif mutation == 'wrong_prepared':
        binding['prepared_symbols'] = SYMBOLS[:2]
    elif mutation == 'swapped_roles':
        binding['prepared_decider_role_symbol_scopes'] = dict(zip(SCOPES, reversed(list(SCOPES.values()))))
    elif mutation == 'narrowed_role':
        binding['prepared_decider_role_symbol_scopes']['signal-decider'] = []
    elif mutation == 'extra_symbol':
        binding['prepared_decider_role_symbol_scopes']['signal-decider'].append('035420')
    elif mutation == 'unknown_role':
        binding['prepared_decider_role_symbol_scopes']['unknown'] = []
    elif mutation == 'unknown_field':
        binding['extra'] = True
    elif mutation == 'unknown_provenance':
        binding['provenance'] = 'invented'
    else:
        outcome['result']['source_summary_scope'] = []
    assert build_proof(outcome, cutoff) is None


def test_stage_run_completes_frozen_decider_scope_with_source_omission(monkeypatch):
    outcome, cutoff = frozen_outcome()
    state = _FakePipelineRunStateService()
    monkeypatch.setattr('app.eod.daily_pipeline_stages.daily_pipeline_run_state_service', state)
    monkeypatch.setattr('app.eod.daily_pipeline_stages.agent_feedback_prerequisite_service.inspect_feedback_prerequisites', lambda **_: _feedback_prerequisites())
    service = DailyPipelineStageService()
    with (
        patch.object(service, '_normalize_market_date', return_value='2026-04-14'),
        patch.object(service, '_execute_stage', return_value=outcome['result']),
        patch('app.eod.daily_pipeline_stages.resolve_latest_completed_summary_knowledge_cutoff', return_value=cutoff),
    ):
        result = service.run(market_date='2026-04-14', stages=['signal_decider'], symbols=SYMBOLS)
    assert result['stages']['signal_decider']['status'] == 'completed'
    finished = [event for event in state.stage_events if event.get('status') == 'completed'][-1]
    assert finished['details']['completion_scope_proof']['role_symbol_scopes'] == SCOPES


def test_stage_run_advances_healthy_frozen_lane_after_peer_failure(monkeypatch):
    outcome, cutoff = frozen_outcome(failed=True)
    plan = _next_open_completion_result(
        parent_run_id=outcome['result']['run_id'],
        completed_symbols=['005930'],
        role_symbol_scopes={'signal-decider': [], 'short-term-decider': ['005930']},
    )
    outcome['result']['decision_identities'][0]['decision_snapshot_id'] = (
        plan['completion_manifest']['decision_partition'][0]['decision_snapshot_id']
    )
    monkeypatch.setattr('app.eod.daily_pipeline_stages.daily_pipeline_run_state_service', _FakePipelineRunStateService())
    monkeypatch.setattr('app.eod.daily_pipeline_stages.agent_feedback_prerequisite_service.inspect_feedback_prerequisites', lambda **_: _feedback_prerequisites())
    service = DailyPipelineStageService()

    def execute(stage_id, **kwargs):
        if stage_id == 'signal_decider':
            return outcome['result']
        assert stage_id == 'next_open_execution_plan'
        assert kwargs['symbols'] == ['005930']
        return plan

    with (
        patch.object(service, '_normalize_market_date', return_value='2026-04-14'),
        patch.object(service, '_execute_stage', side_effect=execute),
        patch('app.eod.daily_pipeline_stages.resolve_latest_completed_summary_knowledge_cutoff', return_value=cutoff),
    ):
        result = service.run(
            market_date='2026-04-14',
            stages=['signal_decider', 'next_open_execution_plan'], symbols=SYMBOLS,
        )
    assert result['stages']['next_open_execution_plan']['status'] == 'completed'


@pytest.mark.parametrize('field', ['identity', 'count', 'pending', 'partial_selected'])
def test_frozen_binding_does_not_replace_terminal_publication_evidence(field):
    outcome, cutoff = frozen_outcome()
    summary = outcome['result']['progress']['summary']
    if field == 'identity':
        outcome['result']['decision_identities'].pop()
    elif field == 'count':
        summary['completed_feedback_count'] += 1
    elif field == 'pending':
        summary['role_progress']['signal-decider']['pending_symbols'] = 1
    else:
        summary['role_symbol_scopes']['signal-decider'] = []
    assert build_proof(outcome, cutoff) is None


def test_scheduled_decider_followup_preserves_native_frozen_binding(monkeypatch):
    outcome, cutoff = frozen_outcome()
    monkeypatch.setattr('app.eod.pykrx_pipeline_runner.build_expired_decision_replay_result', lambda **_: None)
    monkeypatch.setattr('app.eod.pykrx_pipeline_runner.get_market_state', lambda: None)
    monkeypatch.setattr('app.agent_context.prerequisite_service.agent_feedback_prerequisite_service.inspect_feedback_prerequisites', lambda **_: {'ready': True})
    with patch('app.agent_context.retry_service.agent_feedback_retry_service.run_retry_cycles', return_value=outcome['result']) as retry:
        result = _run_daily_signal_decider_followup(
            market_date='2026-04-14', enabled=True, symbols=SYMBOLS,
            knowledge_cutoff_at=cutoff['knowledge_cutoff_at'], source_summary_run_id=cutoff['source_run_id'],
        )
    assert retry.call_args.kwargs['config'].source_summary_run_id == cutoff['source_run_id']
    assert result['source_summary_scope'] == outcome['result']['source_summary_scope']
