from types import SimpleNamespace
from unittest.mock import patch

import pytest

from app.eod.daily_pipeline_stages import DailyPipelineStageService
from app.eod.scope_outcomes import build_exact_symbol_scope_outcome
from tests.test_daily_pipeline_stages import (
    _FakePipelineRunStateService,
    _atomic_scope_finalization,
)


MARKET_DATE = '2026-04-14'
MEMBERSHIP = ['000660', '005930', '035420']
RETRY = ['035420']


def _partition(stage_id, *, successful, retry):
    return build_exact_symbol_scope_outcome(
        manifest_version=f'{stage_id}_scope_outcomes_v1',
        selected_symbols=MEMBERSHIP,
        successful_symbols=successful,
        retry_debt_symbols=retry,
    )


def _repair_request(stage_id):
    return {
        'market_date': MARKET_DATE,
        'stages': [stage_id],
        'symbols': RETRY,
        'exact_membership_symbols': MEMBERSHIP,
        'markets': ['KOSPI'],
        'reused_eod_collection_run_id': 905,
        'point_in_time_recovery': True,
        'trigger': 'auto_eod_followup_resume',
        'recovery_parent_scope_outcome': _partition(
            'auto_eod_followup_admission', successful=MEMBERSHIP[:2], retry=RETRY,
        ),
        'recovery_stage_scope_outcome': {
            'schema_version': 'followup_stage_recovery_scope_v1',
            'stage_id': stage_id,
            'market_date': MARKET_DATE,
            'eod_collection_run_id': 905,
            **_partition(stage_id, successful=MEMBERSHIP[:2], retry=RETRY),
        },
    }


@pytest.fixture
def service():
    with (
        patch('app.eod.daily_pipeline_stages.daily_pipeline_run_state_service',
              _FakePipelineRunStateService()),
        patch('app.eod.daily_pipeline_stages.get_market_state',
              return_value=SimpleNamespace(
                  state='CLOSED', server_time='2026-04-14T16:00:00+09:00',
              )),
    ):
        yield DailyPipelineStageService()


@pytest.mark.parametrize('stage_id', ['technical_snapshots', 'action_summaries'])
def test_exact_early_repair_publishes_only_failed_symbol(service, stage_id):
    stored_rows = dict.fromkeys(MEMBERSHIP[:2], 'original successful row')
    calls = []

    def publish(published_stage):
        def rebuild(**kwargs):
            calls.append((published_stage, kwargs))
            # Atomic finalizers may delete every unsuccessful row in this scope.
            assert kwargs['symbols'] == RETRY
            assert kwargs['exact_scope_symbols'] == RETRY
            stored_rows.update(dict.fromkeys(kwargs['symbols'], 'repaired row'))
            return {
                'status': 'completed', 'market_date': MARKET_DATE,
                'contexts_persisted': 1, 'snapshots_persisted': 1,
                'summaries_persisted': 1, 'contexts_failed': 0,
                'snapshots_failed': 0, 'summaries_failed': 0,
                'scope_finalization': _atomic_scope_finalization(
                    stage_id=published_stage, exact_symbols=RETRY,
                    successful_symbols=RETRY, market_date=MARKET_DATE,
                ),
                **build_exact_symbol_scope_outcome(
                    manifest_version=f'{published_stage}_scope_outcomes_v1',
                    selected_symbols=RETRY, successful_symbols=RETRY,
                    retry_debt_symbols=[],
                ),
            }
        return rebuild

    with (
        patch('app.eod.daily_pipeline_stages.daily_price_context_service.'
              'rebuild_for_market_date', side_effect=publish('price_contexts')),
        patch('app.eod.daily_pipeline_stages.daily_technical_snapshot_service.'
              'rebuild_for_market_date', side_effect=publish('technical_snapshots')),
        patch('app.eod.daily_pipeline_stages.daily_action_summary_service.'
              'rebuild_for_market_date', side_effect=publish('action_summaries')),
    ):
        result = service.run(**_repair_request(stage_id))

    assert result['status'] == 'completed'
    assert result['resolved_stages'] == [stage_id]
    assert [published_stage for published_stage, _ in calls] == (
        ['price_contexts', 'technical_snapshots']
        if stage_id == 'technical_snapshots' else ['action_summaries']
    )
    assert [stored_rows[symbol] for symbol in MEMBERSHIP[:2]] == [
        'original successful row', 'original successful row',
    ]


@pytest.mark.parametrize('override', [
    {'market_date': '2026-04-13'},
    {'eod_collection_run_id': 904},
    {'stage_id': 'value_factors'},
    {'market_date': None},
    {'eod_collection_run_id': None},
])
def test_exact_early_repair_rejects_unrelated_scope_before_execution(service, override):
    request = _repair_request('action_summaries')
    request['recovery_stage_scope_outcome'].update(override)
    with patch.object(service, '_execute_stage') as execute:
        with pytest.raises(ValueError):
            service.run(**request)
    execute.assert_not_called()


def test_exact_early_repair_persists_unfinished_siblings_for_restart(service):
    request = _repair_request('action_summaries')
    deferred = {
        'external_news_collect': {'status': 'failed', 'details': {'reason': 'provider_error'}},
        'signal_decider': {'status': 'blocked', 'details': {'reason': 'summaries_incomplete'}},
    }
    request['recovery_stage_scope_outcome']['deferred_stage_outcomes'] = deferred
    with patch.object(service, '_execute_stage', return_value={
        'status': 'completed',
        'scope_finalization': _atomic_scope_finalization(
            stage_id='action_summaries', exact_symbols=RETRY,
            successful_symbols=RETRY, market_date=MARKET_DATE,
        ),
        **build_exact_symbol_scope_outcome(
            manifest_version='action_summaries_scope_outcomes_v1',
            selected_symbols=RETRY, successful_symbols=RETRY, retry_debt_symbols=[],
        ),
    }) as execute:
        result = service.run(**request)

    execute.assert_called_once()
    assert service.run_state_service.runs[0]['run_metadata'][
        'deferred_recovery_stage_outcomes'
    ] == deferred
    assert result['pipeline_run']['run_metadata'][
        'deferred_recovery_stage_outcomes'
    ] == deferred
    assert result['resolved_stages'] == ['action_summaries']
    assert set(result['stages']) == {'action_summaries'}


@pytest.mark.parametrize('deferred', [
    {'unknown_stage': {'status': 'failed'}},
    {'action_summaries': {'status': 'failed'}},
    {'signal_decider': {'status': 'completed'}},
    {'signal_decider': {'status': 'running'}},
    {'signal_decider': []},
    ['signal_decider'],
])
def test_exact_early_repair_rejects_invalid_deferred_work(service, deferred):
    request = _repair_request('action_summaries')
    request['recovery_stage_scope_outcome']['deferred_stage_outcomes'] = deferred
    with patch.object(service, '_execute_stage') as execute:
        with pytest.raises(ValueError):
            service.run(**request)
    execute.assert_not_called()
