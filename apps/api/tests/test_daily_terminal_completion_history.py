from copy import deepcopy
from unittest.mock import patch

import pytest

from app.agent_context.roles import DECISION_SUPPORT_AGENT_NAMES
from app.eod.daily_pipeline_stages import DailyPipelineStageService
from app.eod.pipeline_status import EODPipelineStatusService
from tests.test_daily_terminal_completion import _partial_summary_result, _scheduled_chain
from tests.test_eod_pipeline_status import _completed_official_stage


def _scheduled_history(symbol_count=2):
    run, items, identities = _scheduled_chain(symbol_count=symbol_count)
    history = {}
    for stage_id, stage in run['stages'].items():
        stage['pipeline_run_id'] = run['id']
        history[stage_id] = {
            'run': deepcopy({key: value for key, value in run.items() if key != 'stages'}),
            'stage': deepcopy(stage),
        }
    return run, history, items, identities


def _read_history(
    run, history, items, identities, *, summary_resolution=None,
    latest_run=None, publication_calls=None,
):
    service = EODPipelineStatusService()

    def validate(**kwargs):
        if publication_calls is not None:
            publication_calls.append(kwargs['result']['run_id'])
        return {
            'valid': True,
            'summary_cutoff_resolution': summary_resolution,
            'decision_identities': (
                identities
                if kwargs['result']['roles'] == list(DECISION_SUPPORT_AGENT_NAMES)
                else []
            ),
        }

    fallback = {
        stage_id: service._build_generic_pending_stage(
            stage_id=stage_id, market_date=run['market_date'],
        )
        for stage_id in run['stages']
    }
    latest = latest_run if latest_run is not None else {
        **run, 'id': 682, 'trigger': 'manual_api',
        'stages': {'technical_snapshots': _completed_official_stage(
            run_id=682, stage_id='technical_snapshots', market_date=run['market_date'],
        )['stage']},
        'run_metadata': {'requested_stages': ['technical_snapshots']},
    }
    with (
        patch.object(DailyPipelineStageService, '_validate_terminal_feedback_publications', side_effect=validate),
        patch('app.eod.terminal_completion.load_plan_publication_items', return_value=items),
        patch('app.agent_context.retry_service.agent_feedback_retry_service.run_retry_cycles') as dispatch,
        patch('app.eod.pipeline_status.session_scope', side_effect=AssertionError('status must not write')),
    ):
        stages = service._overlay_official_stage_history(
            stages=fallback, stage_history=history, market_date=run['market_date'],
            latest_run=latest,
        )
        stages = service._overlay_official_run_stages(
            stages=stages, official_run=latest, market_date=run['market_date'],
        )
        stages = service._normalize_stage_contract(stages)
    dispatch.assert_not_called()
    return stages


def test_completed_scheduled_history_survives_newer_run_omitting_terminal_stages():
    run, history, items, identities = _scheduled_history()
    original = deepcopy(history)
    stages = _read_history(run, history, items, identities)

    assert [stages[stage_id]['status'] for stage_id in run['stages']] == ['completed'] * 3
    for stage_id in run['stages']:
        details = stages[stage_id]['details']
        assert details['pipeline_run_id'] == run['id']
        assert details['run_metadata'] == run['run_metadata']
        assert details['progress_source'] == 'official_stage_history'
        assert details['completion_proof_source'] == 'validated_scheduled_publications'
        assert stages[stage_id]['updated_at'] == original[stage_id]['stage']['updated_at']
    proof = stages['next_open_execution_plan']['details']['completion_scope_proof']
    assert proof['active_item_count'] == 0
    assert proof['parent_run_id'] == run['stages']['signal_decider']['details']['result_summary']['run_id']
    assert history == original


def test_same_latest_and_history_chain_validates_publications_only_once():
    run, history, items, identities = _scheduled_history()
    publication_calls = []

    stages = _read_history(
        run, history, items, identities, latest_run=run,
        publication_calls=publication_calls,
    )

    assert [stages[stage_id]['status'] for stage_id in run['stages']] == ['completed'] * 3
    assert len(publication_calls) == 2


@pytest.mark.parametrize('invalid_summary_proof', [False, True])
def test_partial_summary_history_keeps_only_its_validated_child_scope(invalid_summary_proof):
    run, history, items, identities = _scheduled_history(symbol_count=1)
    summary = history['agent_feedback_summaries']['stage']
    result = _partial_summary_result(
        run_id=summary['details']['result_summary']['run_id'],
    )
    summary.update(status='partial', total=6, processed=6, succeeded=5, failed=1)
    summary['details']['result_summary'] = result
    if invalid_summary_proof:
        summary['details']['completion_scope_proof'] = {'proof_digest': 'invalid'}
    resolution = {
        'ready': True, 'source_run_id': result['run_id'],
        'knowledge_cutoff_at': result['knowledge_cutoff_at'],
        'feedback_scope_mode': result['feedback_scope_mode'],
        'requested_symbols': ['000001'],
        'completed_scope_proof': {
            'schema_version': 'completed_summarizer_intersection_v1',
            'completed_symbols': ['000001'], 'admitted_symbols': ['000001', '000002'],
        },
    }

    stages = _read_history(run, history, items, identities, summary_resolution=resolution)

    assert stages['agent_feedback_summaries']['status'] == 'partial'
    if invalid_summary_proof:
        assert stages['signal_decider']['status'] != 'completed'
        assert stages['next_open_execution_plan']['status'] != 'completed'
    else:
        assert 'completion_scope_proof' not in stages['agent_feedback_summaries']['details']
        assert stages['signal_decider']['status'] == 'completed'
        assert stages['next_open_execution_plan']['status'] == 'completed'
        proof = stages['signal_decider']['details']['completion_scope_proof']
        assert proof['completed_symbols'] == ['000001']
        assert proof['source_summary_run_id'] == result['run_id']


@pytest.mark.parametrize('mutation', [
    'foreign_parent', 'foreign_stage_parent', 'foreign_eod', 'foreign_eod_source', 'wrong_run_date',
    'wrong_stage_date', 'wrong_summary_parent', 'wrong_cutoff', 'partial_scope',
    'invalid_decider_proof', 'invalid_summary_proof', 'missing_summary',
    'malformed_summary', 'bad_manifest', 'foreign_plan_parent', 'foreign_plan_summary',
])
def test_scheduled_history_reconstruction_rejects_unrelated_or_invalid_chain(mutation):
    run, history, items, identities = _scheduled_history()
    decider = history['signal_decider']
    result = decider['stage']['details']['result_summary']
    if mutation == 'foreign_parent':
        decider['run']['id'] = 680
        decider['stage']['pipeline_run_id'] = 680
    elif mutation == 'foreign_stage_parent':
        decider['stage']['pipeline_run_id'] = 680
    elif mutation == 'foreign_eod':
        decider['run']['run_metadata']['eod_collection_run_id'] = 600
        decider['stage']['source_run_ids']['eod_run_id'] = 600
    elif mutation == 'foreign_eod_source':
        decider['stage']['source_run_ids']['eod_run_id'] = 600
    elif mutation == 'wrong_run_date':
        decider['run']['market_date'] = '2026-11-19'
    elif mutation == 'wrong_stage_date':
        decider['stage']['market_date'] = '2026-11-19'
    elif mutation == 'wrong_summary_parent':
        result['scope_authority'] = {'source_summary_run_id': 'unrelated-summary'}
    elif mutation == 'wrong_cutoff':
        result['knowledge_cutoff_at'] = '2026-11-20T13:00:00+00:00'
    elif mutation == 'partial_scope':
        result['requested_symbols'] = ['000001']
    elif mutation == 'invalid_decider_proof':
        decider['stage']['details']['completion_scope_proof'] = {'proof_digest': 'invalid'}
    elif mutation == 'invalid_summary_proof':
        history['agent_feedback_summaries']['stage']['details']['completion_scope_proof'] = {'proof_digest': 'invalid'}
    elif mutation == 'missing_summary':
        del history['agent_feedback_summaries']
    elif mutation == 'malformed_summary':
        history['agent_feedback_summaries']['stage']['details'] = ['not-a-dict']
    elif mutation in {'foreign_plan_parent', 'foreign_plan_summary'}:
        key = 'parent_signal_decider_run_id' if mutation == 'foreign_plan_parent' else 'source_summary_run_id'
        history['next_open_execution_plan']['stage']['source_run_ids'][key] = 'unrelated-parent'
    else:
        history['next_open_execution_plan']['stage']['details']['result_summary']['completion_manifest']['manifest_digest'] = 'invalid'

    stages = _read_history(run, history, items, identities)

    assert stages['next_open_execution_plan']['status'] != 'completed'
    if mutation not in {'bad_manifest', 'foreign_plan_parent', 'foreign_plan_summary'}:
        assert stages['signal_decider']['status'] != 'completed'

