from copy import deepcopy
from datetime import date, datetime
from unittest.mock import patch

from app.agent_context.run_modes import (
    OPENCLAW_SHARED_DISPATCH_CIRCUIT_BREAKER_SCHEMA_VERSION,
)
from app.eod.pykrx_backfill import (
    PykrxBackfillService,
    _is_retryable_feedback_rearm_candidate,
    _retryable_feedback_rearm_max_cycles,
    _select_summary_cutoff_proof,
)
from app.eod.pykrx_pipeline_runner import AUTO_DAILY_TRACKED_STAGE_ORDER


def scheduled_parent():
    cutoff = '2026-11-23T12:56:35+00:00'
    stages = {
        stage_id: {'status': 'completed'}
        for stage_id in AUTO_DAILY_TRACKED_STAGE_ORDER
    }
    stages['agent_feedback_summaries'] = {
        'status': 'partial',
        'source_run_ids': {'agent_feedback_run_id': 'summary-parent'},
        'details': {'result_summary': {
            'run_id': 'summary-parent',
            'market_date': '2026-11-23',
            'knowledge_cutoff_at': cutoff,
        }},
    }
    stages['signal_decider'] = {
        'status': 'partial',
        'details': {'result_summary': {
            'run_id': 'decider-parent',
            'market_date': '2026-11-23',
            'knowledge_cutoff_at': cutoff,
            'source_summary_scope': {
                'source_summary_run_id': 'summary-parent',
                'knowledge_cutoff_at': cutoff,
                'provenance': 'frozen_decider_role_scopes_v1',
                'prepared_decider_role_symbol_scopes': {
                    'short-term-decider': ['005930'],
                    'signal-decider': ['005930'],
                },
            },
            'prerequisites': {'ready': True},
            'openclaw_dispatch_circuit_breaker': {
                'schema_version': OPENCLAW_SHARED_DISPATCH_CIRCUIT_BREAKER_SCHEMA_VERSION,
                'failure_kind': 'openclaw_transport_error',
                'failure_status': 'transport/error',
                'role': 'short-term-decider',
                'retryable': True,
                'source': 'child_dispatch_circuit_breaker',
            },
            'progress': {'summary': {
                'run_status': 'partial',
                'failed_items': 60,
                'completed_cycles': 0,
                'cycle_number': 1,
                'role_progress': {
                    'short-term-decider': {
                        'retry_pending_symbols': 79,
                        'in_progress_symbols': 0,
                        'pending_symbols': 19,
                    },
                    'signal-decider': {
                        'retry_pending_symbols': 78,
                        'in_progress_symbols': 0,
                        'pending_symbols': 78,
                    },
                },
            }},
        }},
    }
    stages['next_open_execution_plan'] = {'status': 'blocked'}
    return {
        'id': 682,
        'market_date': '2026-11-23',
        'trigger': 'auto_close_scheduler',
        'status': 'partial',
        'run_metadata': {
            'entrypoint': 'PYKRX_BACKFILL_SERVICE.run_daily_pipeline_if_due',
            'mode': 'daily_auto',
            'eod_collection_run_id': 602,
            'resolved_stages': list(AUTO_DAILY_TRACKED_STAGE_ORDER),
        },
        'stages': stages,
    }


def cutoff_proof():
    return {
        'source_run_id': 'summary-parent',
        'fs_div': 'CFS',
        'feedback_scope_mode': 'decider_scope_fast_path',
        'knowledge_cutoff_at': '2026-11-23T12:56:35+00:00',
        'completed_scope_proof': {'schema_version': 'verified-test-proof'},
        'terminal_role_scope_proof': {'schema_version': 'verified-test-proof'},
        'expected_completed_symbols': ['005930'],
    }


def test_initial_scheduled_decider_outage_uses_verified_summary_authority():
    parent = scheduled_parent()
    proof = cutoff_proof()
    assert _is_retryable_feedback_rearm_candidate(
        parent, summary_cutoff_proof=proof,
    )
    assert _retryable_feedback_rearm_max_cycles(
        parent, summary_cutoff_proof=proof,
    ) == 1


def test_scheduled_rearm_never_invents_missing_or_conflicting_summary_authority():
    parent = scheduled_parent()
    assert not _is_retryable_feedback_rearm_candidate(parent)
    invalid = deepcopy(cutoff_proof())
    invalid['source_run_id'] = 'other-summary'
    assert not _is_retryable_feedback_rearm_candidate(
        parent, summary_cutoff_proof=invalid,
    )
    invalid = deepcopy(cutoff_proof())
    invalid['knowledge_cutoff_at'] = '2026-11-23T13:00:00+00:00'
    assert not _is_retryable_feedback_rearm_candidate(
        parent, summary_cutoff_proof=invalid,
    )


def test_scheduled_rearm_rejects_wrong_lineage_and_invalid_marker():
    for field, value in [('trigger', 'manual'), ('status', 'running')]:
        parent = scheduled_parent()
        parent[field] = value
        assert not _is_retryable_feedback_rearm_candidate(
            parent, summary_cutoff_proof=cutoff_proof(),
        )
    parent = scheduled_parent()
    marker = parent['stages']['signal_decider']['details']['result_summary'][
        'openclaw_dispatch_circuit_breaker'
    ]
    marker['schema_version'] = 'future-v2'
    assert not _is_retryable_feedback_rearm_candidate(
        parent, summary_cutoff_proof=cutoff_proof(),
    )
    parent = scheduled_parent()
    parent['stages']['signal_decider']['details']['result_summary'][
        'market_date'
    ] = '2026-11-20'
    assert not _is_retryable_feedback_rearm_candidate(
        parent, summary_cutoff_proof=cutoff_proof(),
    )


def test_partial_summary_cannot_authorize_an_uncovered_frozen_decider_symbol():
    parent = scheduled_parent()
    parent['stages']['signal_decider']['details']['result_summary'][
        'source_summary_scope'
    ]['prepared_decider_role_symbol_scopes']['signal-decider'].append('000660')
    assert not _is_retryable_feedback_rearm_candidate(
        parent, summary_cutoff_proof=cutoff_proof(),
    )


def test_scheduler_distinguishes_valid_circuit_from_missing_summary_authority():
    service = PykrxBackfillService()
    with (
        patch('app.eod.pykrx_backfill._current_krx_calendar_date',
              return_value=date(2026, 11, 24)),
        patch('app.eod.pykrx_backfill.daily_pipeline_run_state_service.'
              'get_latest_run_for_context', return_value=scheduled_parent()),
        patch('app.eod.pykrx_backfill.daily_pipeline_run_state_service.'
              'count_runs', return_value=0),
        patch('app.eod.pykrx_backfill._select_summary_cutoff_proof',
              return_value=None),
        patch('app.eod.daily_pipeline_stages.daily_pipeline_stage_service.run') as run,
    ):
        result = service._resume_auto_daily_followups_if_due(
            market_date='2026-11-23',
            latest_available_trade_date='2026-11-23',
            latest_eod_run={'id': 602, 'status': 'completed', 'symbols_failed': 0},
            markets=('KOSPI', 'KOSDAQ'),
        )
    assert result['reason'] == 'followup_feedback_rearm_contract_unavailable'
    assert result['followup_recovery']['blocker'] == 'exact_parent_feedback_contract_unavailable'
    assert result['followup_recovery']['retryable_feedback_rearm'] is True
    assert result['followup_recovery']['retry_scheduled'] is False
    run.assert_not_called()


def test_scheduler_keeps_verified_original_parent_retry_on_exact_decider_suffix():
    service = PykrxBackfillService()
    proof = cutoff_proof()
    with (
        patch('app.eod.pykrx_backfill._current_krx_calendar_date',
              return_value=date(2026, 11, 24)),
        patch('app.eod.pykrx_backfill.daily_pipeline_run_state_service.'
              'get_latest_run_for_context', return_value=scheduled_parent()),
        patch('app.eod.pykrx_backfill.daily_pipeline_run_state_service.'
              'count_runs', return_value=0),
        patch('app.eod.pykrx_backfill._select_summary_cutoff_proof',
              return_value=proof),
        patch('app.eod.daily_pipeline_stages.daily_pipeline_stage_service.run',
              return_value={'status': 'partial', 'pipeline_run_id': 683}) as run,
        patch.object(service, '_persist_successful_followup_recovery',
                     return_value={'updated': False}),
    ):
        service._resume_auto_daily_followups_if_due(
            market_date='2026-11-23',
            latest_available_trade_date='2026-11-23',
            latest_eod_run={'id': 602, 'status': 'completed', 'symbols_failed': 0},
            markets=('KOSPI', 'KOSDAQ'),
        )
    kwargs = run.call_args.kwargs
    assert kwargs['stages'] == ['signal_decider', 'next_open_execution_plan']
    assert kwargs['resume_from_stage'] == 'signal_decider'
    assert kwargs['summary_cutoff_proof'] == proof
    assert kwargs['reuse_existing_feedback'] is True
    assert kwargs['force'] is False


def test_completed_scheduled_summary_resolves_financial_basis_from_exact_parent():
    service = PykrxBackfillService()
    parent = scheduled_parent()
    stage = parent['stages']['agent_feedback_summaries']
    stage['status'] = 'completed'
    stage['details']['result_summary'].update({
        'status': 'completed',
        'feedback_scope_mode': 'decider_scope_fast_path',
        'requested_symbols': ['005930'],
    })
    source = {
        'run_id': 'summary-parent', 'market_date': '2026-11-23',
        'request_payload': {'fs_div': 'OFS'},
    }
    resolution = {
        'ready': True, 'source_run_id': 'summary-parent',
        'knowledge_cutoff_at': datetime.fromisoformat('2026-11-23T12:56:35+00:00'),
        'requested_symbols': ['005930'],
    }
    with (
        patch('app.eod.pykrx_backfill._current_krx_calendar_date',
              return_value=date(2026, 11, 24)),
        patch('app.eod.pykrx_backfill.daily_pipeline_run_state_service.'
              'get_latest_run_for_context', return_value=parent),
        patch('app.eod.pykrx_backfill.daily_pipeline_run_state_service.'
              'count_runs', return_value=0),
        patch('app.eod.pykrx_backfill.agent_context_feedback_run_state_service.'
              'get_run', return_value=source),
        patch('app.eod.pykrx_backfill.resolve_latest_completed_summary_knowledge_cutoff',
              return_value=resolution) as resolve,
        patch('app.eod.daily_pipeline_stages.daily_pipeline_stage_service.run',
              return_value={'status': 'partial', 'pipeline_run_id': 683}) as run,
        patch.object(service, '_persist_successful_followup_recovery',
                     return_value={'updated': False}),
    ):
        service._resume_auto_daily_followups_if_due(
            market_date='2026-11-23', latest_available_trade_date='2026-11-23',
            latest_eod_run={'id': 602, 'status': 'completed', 'symbols_failed': 0},
            markets=('KOSPI', 'KOSDAQ'),
        )
    assert run.call_args.kwargs['fs_div'] == 'OFS'
    assert resolve.call_args.kwargs['source_run_id'] == 'summary-parent'
    assert resolve.call_args.kwargs['fs_div'] == 'OFS'


def test_scheduled_summary_financial_basis_is_never_invented_or_overridden():
    parent = scheduled_parent()
    stage = parent['stages']['agent_feedback_summaries']
    stage['status'] = 'completed'
    stage['details']['result_summary'].update({
        'status': 'completed', 'feedback_scope_mode': 'decider_scope_fast_path',
        'requested_symbols': ['005930'],
    })
    for stored_basis, requested_basis in [(None, None), ('unknown', None), ('OFS', 'CFS')]:
        with (
            patch('app.eod.pykrx_backfill.agent_context_feedback_run_state_service.'
                  'get_run', return_value={
                      'run_id': 'summary-parent', 'market_date': '2026-11-23',
                      'request_payload': {'fs_div': stored_basis},
                  }) as source,
            patch('app.eod.pykrx_backfill.resolve_latest_completed_summary_knowledge_cutoff') as resolve,
        ):
            proof = _select_summary_cutoff_proof(
                pipeline_stages=parent['stages'], run_metadata=parent['run_metadata'],
                eod_stage_progress={}, market_date='2026-11-23', fs_div=requested_basis,
            )
        assert proof is None
        source.assert_called_once_with(run_id='summary-parent')
        resolve.assert_not_called()
    proof = cutoff_proof()
    del proof['fs_div']
    assert not _is_retryable_feedback_rearm_candidate(parent, summary_cutoff_proof=proof)
