from copy import deepcopy
from datetime import date
from unittest.mock import patch

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
