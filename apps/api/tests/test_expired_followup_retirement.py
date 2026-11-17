from copy import deepcopy
from contextlib import ExitStack
from datetime import date
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from app.eod.market_membership import market_membership_sha256
from app.eod.pykrx_backfill import (
    AUTO_DAILY_TRACKED_STAGE_ORDER,
    TERMINAL_FOLLOWUP_CONTINUATION_STAGES,
    PykrxBackfillService,
)
from app.eod.scope_outcomes import build_exact_symbol_scope_outcome


def _expired_followup():
    symbols = ['000660', '005930']
    stage_rows = {
        stage: {
            'stage_id': stage,
            'market_date': '2026-11-12',
            'status': 'skipped',
            **dict.fromkeys(
                ['processed', 'total', 'succeeded', 'failed', 'remaining'], 0,
            ),
            'source_run_ids': {},
            'details': {'result_summary': {
                'triggered': False,
                'status': 'skipped',
                'reason': 'next_open_window_expired',
                'market_date': '2026-11-12',
                'target_trade_date': '2026-11-13',
                'current_market_date': '2026-11-13',
                'market_state': 'CLOSED',
                'stage': stage,
            }},
        }
        for stage in TERMINAL_FOLLOWUP_CONTINUATION_STAGES
    }
    stage_progress = {
        stage: {'status': 'completed'}
        for stage in AUTO_DAILY_TRACKED_STAGE_ORDER
        if stage not in TERMINAL_FOLLOWUP_CONTINUATION_STAGES
    }
    stage_progress['macro_context_refresh'] = {'status': 'partial'}
    eod = {
        'id': 7, 'trade_date': '2026-11-12', 'status': 'completed',
        'symbols_total': 2, 'symbols_succeeded': 2, 'symbols_failed': 0,
        'run_metadata': {
            'requested_symbols': symbols,
            'failed_symbols': [], 'unattempted_symbols': [],
            'downstream_retry_symbols': symbols,
            'inherited_downstream_retry_symbols': [],
            'followup_recovery_required': True,
            'expected_daily_pipeline_stages': list(AUTO_DAILY_TRACKED_STAGE_ORDER),
            'stage_progress': stage_progress,
            'followup_admission_scope_outcome': build_exact_symbol_scope_outcome(
                manifest_version='auto_eod_followup_admission_scope_outcomes_v1',
                selected_symbols=symbols, successful_symbols=symbols,
                retry_debt_symbols=[],
            ),
        },
    }
    pipeline = {
        'id': 92, 'market_date': '2026-11-12', 'status': 'completed',
        'trigger': 'auto_eod_followup_resume', 'stages': stage_rows,
        'run_metadata': {
            'resolved_stages': list(TERMINAL_FOLLOWUP_CONTINUATION_STAGES),
            'requested_stages': list(TERMINAL_FOLLOWUP_CONTINUATION_STAGES),
            'resume_from_stage': 'agent_feedback_summaries',
            'symbols': ['005930'],
            'exact_membership_symbol_count': len(symbols),
            'exact_membership_sha256': market_membership_sha256(symbols),
            'reused_eod_collection_run_id': 7,
            'point_in_time_recovery': True, 'force': False,
            'reuse_existing_feedback': True, 'bypass_prerequisite_gate': False,
            'allow_expired_analysis_replay': False,
        },
    }
    result = {
        'status': 'completed', 'pipeline_run_id': 92,
        'market_date': '2026-11-12', 'pipeline_run': pipeline,
        'resolved_stages': list(TERMINAL_FOLLOWUP_CONTINUATION_STAGES),
        'stages': stage_rows,
    }
    return eod, result


def test_expired_feedback_debt_is_retired_without_claiming_publications():
    eod, result = _expired_followup()
    original_stages = deepcopy(result['stages'])
    with (
        patch('app.eod.pykrx_backfill._current_krx_calendar_date', return_value=date(2026, 11, 18)),
        patch('app.eod.pykrx_backfill.count_eod_run_persisted_symbols', return_value=2),
        patch('app.eod.pykrx_backfill.load_eod_run_persisted_symbols', return_value=['000660', '005930']),
        patch('app.eod.pykrx_backfill.complete_eod_collection_run') as complete,
    ):
        persisted = PykrxBackfillService()._persist_successful_followup_recovery(
            latest_eod_run=eod, recovery_result=result,
        )

    assert persisted['updated'] is True
    metadata = complete.call_args.kwargs['run_metadata']
    assert metadata['downstream_retry_symbols'] == []
    assert metadata['followup_recovery_required'] is False
    assert metadata['downstream_recovery_symbols_count'] == 0
    retirement = metadata['expired_terminal_followup_retirement']
    assert retirement['retired_symbols_count'] == 2
    assert retirement['reason'] == 'next_open_window_expired'
    assert retirement['source_pipeline_run_id'] == 92
    assert retirement['target_trade_date'] == '2026-11-13'
    assert result['stages'] == original_stages


@pytest.mark.parametrize('case', [
    'wrong_eod', 'wrong_date', 'wrong_membership', 'missing_stage',
    'mixed_failure', 'missing_expiry', 'live_window', 'false_open_expiry',
    'provider_failure', 'stage_debt', 'price_failure', 'missing_admission',
    'published_source', 'malformed_upstream', 'wrong_stage_run', 'wrong_stage_date',
])
def test_uncertain_or_nonterminal_debt_is_not_retired(case):
    eod, result = _expired_followup()
    meta = eod['run_metadata']
    pipeline_meta = result['pipeline_run']['run_metadata']
    today = date(2026, 11, 18)
    if case == 'wrong_eod':
        pipeline_meta['reused_eod_collection_run_id'] = 8
    elif case == 'wrong_date':
        result['pipeline_run']['market_date'] = '2026-11-11'
    elif case == 'wrong_membership':
        pipeline_meta['exact_membership_sha256'] = 'wrong'
    elif case == 'missing_stage':
        result['stages'].pop('signal_decider')
    elif case == 'mixed_failure':
        result['stages']['signal_decider']['status'] = 'failed'
    elif case == 'missing_expiry':
        result['stages']['signal_decider']['details']['result_summary'].pop('target_trade_date')
    elif case == 'live_window':
        today = date(2026, 11, 13)
    elif case == 'false_open_expiry':
        for stage in result['stages'].values():
            stage['details']['result_summary']['market_state'] = 'OPEN'
    elif case == 'provider_failure':
        meta['stage_progress']['market_risk_filter']['status'] = 'failed'
    elif case == 'stage_debt':
        meta['followup_stage_debt'] = {'pending_by_stage': {'market_risk_filter': ['005930']}}
    elif case == 'price_failure':
        meta['failed_symbols'] = ['005930']
    elif case == 'missing_admission':
        meta.pop('followup_admission_scope_outcome')
    elif case == 'published_source':
        result['stages']['signal_decider']['source_run_ids'] = {'run_id': 'actual-feedback'}
    elif case == 'malformed_upstream':
        meta['stage_progress']['market_risk_filter'] = 3
    elif case == 'wrong_stage_run':
        result['stages']['signal_decider']['pipeline_run_id'] = 93
    elif case == 'wrong_stage_date':
        result['stages']['signal_decider']['market_date'] = '2026-11-11'
    with (
        patch('app.eod.pykrx_backfill._current_krx_calendar_date', return_value=today),
        patch('app.eod.pykrx_backfill.complete_eod_collection_run') as complete,
        patch('app.eod.pykrx_backfill.update_eod_collection_run_progress') as update,
    ):
        persisted = PykrxBackfillService()._persist_successful_followup_recovery(
            latest_eod_run=eod, recovery_result=result,
        )
    assert persisted['updated'] is False
    assert meta['downstream_retry_symbols'] == ['000660', '005930']
    complete.assert_not_called()
    update.assert_not_called()


def test_scheduled_recovery_converges_expired_suffix_without_models_or_providers():
    eod, expired = _expired_followup()
    pipeline = expired['pipeline_run']
    with (
        patch('app.eod.pykrx_backfill._current_krx_calendar_date', return_value=date(2026, 11, 18)),
        patch('app.eod.pykrx_backfill.daily_pipeline_run_state_service.get_latest_run_for_context', return_value=pipeline),
        patch('app.eod.pykrx_backfill.daily_pipeline_run_state_service.converge_terminal_stages_run',
              return_value={'updated': False, 'reason': 'already_non_resumable', 'status': 'completed'}),
        patch('app.eod.pykrx_backfill.count_eod_run_persisted_symbols', return_value=2),
        patch('app.eod.pykrx_backfill.load_eod_run_persisted_symbols', return_value=['000660', '005930']),
        patch('app.eod.pykrx_backfill.complete_eod_collection_run') as complete,
        patch('app.eod.daily_pipeline_stages.daily_pipeline_stage_service.run') as dispatch,
    ):
        result = PykrxBackfillService()._resume_auto_daily_followups_if_due(
            market_date='2026-11-12', latest_available_trade_date='2026-11-17',
            latest_eod_run=eod, markets=('KOSPI', 'KOSDAQ'),
        )
    assert result['reason'] == 'followup_recovery_state_converged'
    assert complete.call_args.kwargs['run_metadata']['followup_recovery_required'] is False
    dispatch.assert_not_called()


def test_expired_retirement_rechecks_actual_price_membership():
    eod, result = _expired_followup()
    with (
        patch('app.eod.pykrx_backfill._current_krx_calendar_date', return_value=date(2026, 11, 18)),
        patch('app.eod.pykrx_backfill.count_eod_run_persisted_symbols', return_value=2),
        patch('app.eod.pykrx_backfill.load_eod_run_persisted_symbols', return_value=['000660', '035420']),
        patch('app.eod.pykrx_backfill.complete_eod_collection_run') as complete,
        patch('app.eod.pykrx_backfill.update_eod_collection_run_progress') as update,
    ):
        persisted = PykrxBackfillService()._persist_successful_followup_recovery(
            latest_eod_run=eod, recovery_result=result,
        )
    assert persisted == {'updated': False, 'reason': 'expired_recovery_price_scope_unverified'}
    complete.assert_not_called()
    update.assert_not_called()


def test_live_stage_result_shape_retains_the_same_expiry_evidence():
    eod, result = _expired_followup()
    result['stages'] = {
        stage: {'stage': stage, 'status': 'skipped',
                'result': deepcopy(row['details']['result_summary'])}
        for stage, row in result['stages'].items()
    }
    with (
        patch('app.eod.pykrx_backfill._current_krx_calendar_date', return_value=date(2026, 11, 18)),
        patch('app.eod.pykrx_backfill.count_eod_run_persisted_symbols', return_value=2),
        patch('app.eod.pykrx_backfill.load_eod_run_persisted_symbols', return_value=['000660', '005930']),
        patch('app.eod.pykrx_backfill.complete_eod_collection_run') as complete,
    ):
        result = PykrxBackfillService()._persist_successful_followup_recovery(
            latest_eod_run=eod, recovery_result=result,
        )
    assert result['updated'] is True
    assert complete.call_args.kwargs['run_metadata']['downstream_recovery_symbols_count'] == 0


@pytest.mark.parametrize('requested_pit', [False, True])
@pytest.mark.parametrize('verified_prices', [False, True])
@pytest.mark.parametrize('inherit_retry', [False, True])
@pytest.mark.parametrize('expired_target', [False, True])
def test_scheduler_produces_retirable_expiry_through_real_stage_service(
    requested_pit, verified_prices, inherit_retry, expired_target,
):
    from app.agent_context.roles import SUMMARIZER_AGENT_NAMES
    from app.eod.daily_pipeline_stages import DailyPipelineStageService

    eod, _ = _expired_followup()
    symbols = eod['run_metadata']['requested_symbols']
    eod['run_metadata'].update({'markets': ['KOSPI'], 'fs_div': 'CFS'})
    prior_stages = deepcopy(eod['run_metadata']['stage_progress'])
    prior_stages.update({
        'agent_feedback_summaries': {'status': 'partial'},
        'signal_decider': {'status': 'interrupted'},
    })
    prior = {
        'id': 91, 'market_date': '2026-11-12', 'status': 'interrupted',
        'trigger': 'auto_close_scheduler', 'stages': prior_stages,
        'run_metadata': {
            'eod_collection_run_id': 7,
            'resolved_stages': list(AUTO_DAILY_TRACKED_STAGE_ORDER),
            'point_in_time_recovery': False, 'fs_div': 'CFS',
        },
    }
    retry_config = {
        'source_run_id': 'original-partial-summary',
        'knowledge_cutoff_at': '2026-11-12T08:00:00+00:00',
        'feedback_scope_mode': 'decider_scope_fast_path',
        'point_in_time_recovery': False,
        'requested_symbols': ['005930'],
        'roles': list(SUMMARIZER_AGENT_NAMES),
        'role_symbol_scopes': {role: ['005930'] for role in SUMMARIZER_AGENT_NAMES},
        'outer_batch_size': 20, 'fs_div': 'CFS',
    }
    tracked = {}
    run_state = MagicMock()

    def start_run(**kwargs):
        tracked.update({'id': 92, **kwargs, 'status': 'running', 'stages': {}})
        tracked['run_metadata'].update({
            'requested_stages': kwargs['requested_stages'],
            'resolved_stages': kwargs['resolved_stages'],
        })
        return deepcopy(tracked)

    def finish_stage(**kwargs):
        tracked['stages'][kwargs['stage_id']] = deepcopy(kwargs)
        return kwargs

    def finish_run(**kwargs):
        tracked['status'] = kwargs['status']
        tracked['run_metadata'].update(kwargs.get('run_metadata') or {})
        return deepcopy(tracked)

    run_state.start_run.side_effect = start_run
    run_state.finish_stage.side_effect = finish_stage
    run_state.finish_run.side_effect = finish_run
    model_runner = MagicMock()
    stage_service = DailyPipelineStageService(
        run_state_service=run_state, feedback_retry_service=model_runner,
    )
    service = PykrxBackfillService()
    today = date(2026, 11, 18) if expired_target else date(2026, 11, 13)
    market = SimpleNamespace(state='CLOSED', server_time=f'{today}T20:00:00+09:00')
    with ExitStack() as stack:
        for target, value in {
            '_current_krx_calendar_date': today,
            'get_market_state': market,
            'daily_pipeline_run_state_service.get_latest_run_for_context': prior,
            'daily_pipeline_run_state_service.count_runs': 0,
            'daily_pipeline_run_state_service.converge_terminal_stages_run': {
                'updated': False, 'reason': 'already_non_resumable', 'status': 'completed',
            },
            '_select_summary_cutoff_proof': None,
            '_is_retryable_feedback_rearm_candidate': inherit_retry,
            '_retryable_feedback_rearm_config': retry_config if inherit_retry else None,
            '_retryable_feedback_rearm_max_cycles': 3,
            'count_eod_run_persisted_symbols': 2,
            'load_eod_run_persisted_symbols': symbols if verified_prices else ['000660', '035420'],
        }.items():
            stack.enter_context(patch(f'app.eod.pykrx_backfill.{target}', return_value=value))
        stack.enter_context(patch.object(
            service, '_resolve_point_in_time_followup_membership',
            return_value=(set(symbols), ('KOSPI',)),
        ))
        stack.enter_context(patch.object(
            service, '_preflight_untracked_followup_price_scope',
            return_value={'result': None, 'symbols': [], 'downstream_symbols': []},
        ))
        stack.enter_context(patch(
            'app.eod.daily_pipeline_stages.get_market_state',
            return_value=market,
        ))
        stack.enter_context(patch(
            'app.orders.next_open_execution.normalize_next_open_target_trade_date',
            return_value=date(2026, 11, 13),
        ))
        stack.enter_context(patch(
            'app.eod.daily_pipeline_stages.agent_feedback_prerequisite_service.inspect_feedback_prerequisites',
            return_value={'ready': True},
        ))
        dispatch = stack.enter_context(patch(
            'app.eod.daily_pipeline_stages.daily_pipeline_stage_service.run',
            side_effect=stage_service.run,
        ))
        complete = stack.enter_context(patch('app.eod.pykrx_backfill.complete_eod_collection_run'))
        update = stack.enter_context(patch('app.eod.pykrx_backfill.update_eod_collection_run_progress'))
        provider = stack.enter_context(patch.object(service, 'fetch_recent_universe_snapshot'))
        result = service._resume_auto_daily_followups_if_due(
            market_date='2026-11-12', latest_available_trade_date='2026-11-17',
            latest_eod_run=eod, markets=('KOSPI',),
            point_in_time_recovery=requested_pit,
        )

    assert result['status'] == 'completed'
    if expired_target or requested_pit:
        assert tracked['run_metadata']['exact_membership_symbol_count'] == 2
        assert tracked['run_metadata']['exact_membership_sha256'] == market_membership_sha256(symbols)
    else:
        assert 'exact_membership_symbols' not in dispatch.call_args.kwargs
        assert tracked['run_metadata']['exact_membership_symbol_count'] == 0
    if inherit_retry:
        assert dispatch.call_args.kwargs['inherited_feedback_retry_config'] == retry_config
    else:
        assert 'inherited_feedback_retry_config' not in dispatch.call_args.kwargs
        if expired_target or requested_pit:
            assert dispatch.call_args.kwargs['symbols'] == symbols
    assert tracked['run_metadata']['point_in_time_recovery'] is True
    assert set(tracked['stages']) == set(TERMINAL_FOLLOWUP_CONTINUATION_STAGES)
    assert all(row['status'] == 'skipped' for row in tracked['stages'].values())
    assert all(
        row['details']['result_summary']['reason'] == 'next_open_window_expired'
        for row in tracked['stages'].values()
    )
    assert model_runner.mock_calls == []
    provider.assert_not_called()
    if expired_target and verified_prices:
        assert complete.called, result
        metadata = complete.call_args.kwargs['run_metadata']
        assert metadata['followup_recovery_required'] is False
        assert metadata['downstream_recovery_symbols_count'] == 0
        assert metadata['expired_terminal_followup_retirement']['retired_symbols_count'] == 2
    else:
        assert result['followup_recovery']['state_convergence']['reason'] == (
            'expired_recovery_price_scope_unverified' if expired_target
            else 'resolved_downstream_scope_unavailable'
        )
        complete.assert_not_called()
        update.assert_not_called()
