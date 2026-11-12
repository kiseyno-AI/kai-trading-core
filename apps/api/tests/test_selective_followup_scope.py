from copy import deepcopy
from datetime import date
from unittest.mock import patch

import pytest

from app.eod.pykrx_backfill import (
    AUTO_FOLLOWUP_RECOVERY_TRIGGER,
    PykrxBackfillService,
    _proven_followup_recovery_symbols,
)
from app.eod.scope_outcomes import build_exact_symbol_scope_outcome
from tests.test_auto_pykrx import _atomic_exact_scope_proof


SCOPE = ['000660', '005930', '035420']
DAY = '2026-04-03'


def _partial(stage, retry=None):
    retry = SCOPE[2:] if retry is None else retry
    return {
        'status': 'partial',
        'result': {
            'status': 'partial', 'market_date': DAY,
            **_atomic_exact_scope_proof(
                manifest_version=f'{stage}_scope_outcomes_v1',
                selected_symbols=SCOPE,
                successful_symbols=[symbol for symbol in SCOPE if symbol not in retry],
                retry_debt_symbols=retry,
            ),
        },
    }


@pytest.mark.parametrize('restarted', [False, True])
def test_automatic_partial_repair_keeps_exact_scope_and_deferred_work(restarted):
    stages = {
        'technical_snapshots': _partial('technical_snapshots'),
        'action_summaries': _partial('action_summaries'),
    }
    run = {
        'id': 92, 'status': 'partial', 'market_date': DAY,
        'trigger': 'auto_close_scheduler',
        'run_metadata': {
            'resolved_stages': list(stages), 'eod_collection_run_id': 7,
        },
        'stages': stages,
    }
    expected_stage = 'technical_snapshots'
    if restarted:
        run['trigger'] = AUTO_FOLLOWUP_RECOVERY_TRIGGER
        run['run_metadata'] = {
            'resolved_stages': ['technical_snapshots'],
            'reused_eod_collection_run_id': 7,
            'deferred_recovery_stage_outcomes': {
                'action_summaries': deepcopy(stages['action_summaries']),
            },
        }
        run['stages'] = {'technical_snapshots': {'status': 'completed'}}
        expected_stage = 'action_summaries'
    parent = build_exact_symbol_scope_outcome(
        manifest_version='auto_eod_followup_admission_scope_outcomes_v1',
        selected_symbols=SCOPE, successful_symbols=SCOPE[:2],
        retry_debt_symbols=SCOPE[2:],
    )
    with (
        patch('app.eod.pykrx_backfill._current_krx_calendar_date',
              return_value=date.fromisoformat(DAY)),
        patch('app.eod.pykrx_backfill.daily_pipeline_run_state_service.'
              'get_latest_run_for_context', return_value=run),
        patch('app.eod.pykrx_backfill.daily_pipeline_run_state_service.'
              'count_runs', return_value=0),
        patch('app.eod.pykrx_backfill.count_eod_run_persisted_symbols', return_value=3),
        patch('app.eod.pykrx_backfill.load_eod_run_persisted_symbols', return_value=SCOPE),
        patch('app.eod.daily_pipeline_stages.daily_pipeline_stage_service.run',
              return_value={'status': 'partial', 'pipeline_run_id': 93}) as execute,
    ):
        result = PykrxBackfillService()._resume_auto_daily_followups_if_due(
            market_date=DAY, latest_available_trade_date=DAY,
            latest_eod_run={
                'id': 7, 'status': 'completed', 'symbols_failed': 0, 'symbols_total': 3,
                'run_metadata': {
                    'requested_symbols': SCOPE,
                    'downstream_retry_symbols': SCOPE[2:],
                    'followup_admission_scope_outcome': parent,
                },
            },
            markets=('KOSPI',),
        )
    assert result['followup_recovery']['stages'] == [expected_stage]
    request = execute.call_args.kwargs
    assert request['symbols'] == SCOPE[2:]
    assert request['exact_membership_symbols'] == SCOPE
    assert request['reuse_existing_feedback'] is True
    deferred = request['recovery_stage_scope_outcome']['deferred_stage_outcomes']
    assert set(deferred) == (set() if restarted else {'action_summaries'})


@pytest.mark.parametrize('wrong', ['symbol', 'date', 'stage'])
def test_completion_proof_cannot_clear_unrelated_retry_debt(wrong):
    stage = _partial('technical_snapshots')
    if wrong == 'date':
        stage['result']['scope_finalization']['market_date'] = '2026-04-02'
    stage_id = 'action_summaries' if wrong == 'stage' else 'technical_snapshots'
    requested = ['099999'] if wrong == 'symbol' else SCOPE[2:]
    assert _proven_followup_recovery_symbols(
        stages={stage_id: stage}, stage_ids=[stage_id],
        symbols=requested, market_date=DAY,
    ) == []


def test_completion_proof_resolves_only_actual_successful_symbols():
    assert _proven_followup_recovery_symbols(
        stages={'technical_snapshots': _partial('technical_snapshots')},
        stage_ids=['technical_snapshots'], symbols=SCOPE, market_date=DAY,
    ) == SCOPE[:2]


@pytest.mark.parametrize('status', ['failed', 'interrupted', 'pending', 'blocked', 'partial'])
def test_unknown_early_failure_never_borrows_generic_retry_symbols(status):
    run = {
        'id': 92, 'status': 'partial', 'trigger': 'auto_close_scheduler',
        'run_metadata': {'resolved_stages': ['technical_snapshots'],
                         'eod_collection_run_id': 7},
        'stages': {'technical_snapshots': {'status': status}},
    }
    with (
        patch('app.eod.pykrx_backfill._current_krx_calendar_date',
              return_value=date.fromisoformat(DAY)),
        patch('app.eod.pykrx_backfill.daily_pipeline_run_state_service.'
              'get_latest_run_for_context', return_value=run),
        patch('app.eod.pykrx_backfill.daily_pipeline_run_state_service.'
              'count_runs', return_value=0),
        patch('app.eod.daily_pipeline_stages.daily_pipeline_stage_service.run') as execute,
    ):
        result = PykrxBackfillService()._resume_auto_daily_followups_if_due(
            market_date=DAY, latest_available_trade_date=DAY,
            latest_eod_run={'id': 7, 'status': 'completed', 'symbols_failed': 0,
                            'run_metadata': {'downstream_retry_symbols': SCOPE}},
            markets=('KOSPI',),
        )
    assert result['reason'] == 'followup_recovery_scope_unavailable'
    assert result['followup_recovery']['blocker'] == 'early_stage_exact_retry_scope_unavailable'
    execute.assert_not_called()


def test_different_deferred_cohorts_clear_only_individually_proven_debt():
    technical_debt, action_debt = SCOPE[2], SCOPE[1]
    remaining = [technical_debt, action_debt]
    latest = {
        'id': 92, 'status': 'partial', 'trigger': 'auto_close_scheduler',
        'run_metadata': {'resolved_stages': ['technical_snapshots', 'action_summaries'],
                         'eod_collection_run_id': 7},
        'stages': {'technical_snapshots': _partial('technical_snapshots', [technical_debt]),
                   'action_summaries': _partial('action_summaries', [action_debt])},
    }
    resolved = []

    def execute(**request):
        nonlocal latest
        stage = request['stages'][0]
        published = {'status': 'completed', 'result': {
            'status': 'completed', 'market_date': DAY,
            **_atomic_exact_scope_proof(
                manifest_version=f'{stage}_scope_outcomes_v1',
                selected_symbols=request['symbols'],
            ),
        }}
        latest = {
            'id': 93, 'status': 'completed', 'market_date': DAY,
            'trigger': AUTO_FOLLOWUP_RECOVERY_TRIGGER,
            'run_metadata': {
                'resolved_stages': [stage], 'reused_eod_collection_run_id': 7,
                'deferred_recovery_stage_outcomes': request[
                    'recovery_stage_scope_outcome']['deferred_stage_outcomes'],
            },
            'stages': {stage: published},
        }
        return {'status': 'completed', 'pipeline_run_id': 93,
                'stages': {stage: published}, 'pipeline_run': latest}

    def persist(**kwargs):
        proven = kwargs['recovery_result']['_proven_resolved_downstream_symbols']
        resolved.append(list(proven))
        remaining[:] = [symbol for symbol in remaining if symbol not in proven]
        return {'updated': True}

    parent = build_exact_symbol_scope_outcome(
        manifest_version='auto_eod_followup_admission_scope_outcomes_v1',
        selected_symbols=SCOPE, successful_symbols=SCOPE[:1],
        retry_debt_symbols=[technical_debt, action_debt],
    )
    service = PykrxBackfillService()
    with (
        patch('app.eod.pykrx_backfill._current_krx_calendar_date',
              return_value=date.fromisoformat(DAY)),
        patch('app.eod.pykrx_backfill.daily_pipeline_run_state_service.'
              'get_latest_run_for_context', side_effect=lambda **_: latest),
        patch('app.eod.pykrx_backfill.daily_pipeline_run_state_service.count_runs', return_value=0),
        patch('app.eod.pykrx_backfill.daily_pipeline_run_state_service.'
              'converge_terminal_stages_run', return_value={'updated': True}),
        patch('app.eod.pykrx_backfill.count_eod_run_persisted_symbols', return_value=3),
        patch('app.eod.pykrx_backfill.load_eod_run_persisted_symbols', return_value=SCOPE),
        patch('app.eod.daily_pipeline_stages.daily_pipeline_stage_service.run', side_effect=execute),
        patch.object(service, '_persist_successful_followup_recovery', side_effect=persist),
    ):
        for _ in range(2):
            service._resume_auto_daily_followups_if_due(
                market_date=DAY, latest_available_trade_date=DAY, markets=('KOSPI',),
                latest_eod_run={'id': 7, 'status': 'completed', 'symbols_failed': 0,
                                'symbols_total': 3, 'run_metadata': {
                                    'requested_symbols': SCOPE,
                                    'downstream_retry_symbols': list(remaining),
                                    'followup_admission_scope_outcome': parent,
                                }},
            )
    assert resolved == [[technical_debt], [action_debt]]
    assert remaining == []
