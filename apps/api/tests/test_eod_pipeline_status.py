import unittest
from unittest.mock import patch

from app.eod.pipeline_status import (
    DEFAULT_FACTOR_STAGE_ORDER,
    EODPipelineStatusService,
    _load_latest_pipeline_pykrx_run,
)


class _FakeMarketState:
    market = 'KRX'
    state = 'CLOSED'
    is_trading_day = True
    is_open = False
    reason = None
    server_time = '2026-04-21T18:15:00+09:00'


class _FakeBreakDayMarketState:
    market = 'KRX'
    state = 'CLOSED'
    is_trading_day = False
    is_open = False
    reason = 'holiday'
    server_time = '2026-04-14T12:15:00+09:00'


def _full_factor_stage_counts(*, total: int, updated_at: str) -> dict[str, dict]:
    return {
        stage_id: {
            'persisted': total,
            'updated_at': updated_at,
        }
        for stage_id in DEFAULT_FACTOR_STAGE_ORDER
    }


class EODPipelineStatusServiceTests(unittest.TestCase):
    def test_load_latest_pipeline_pykrx_run_reconciles_selected_daily_run_instead_of_newer_historical_backfill(self):
        historical_run = {
            'id': 900,
            'market': 'KRX',
            'trade_date': '2026-04-21',
            'source': 'pykrx',
            'trigger': 'manual_backfill_api',
            'status': 'running',
            'symbols_total': 100,
            'symbols_succeeded': 20,
            'symbols_failed': 0,
            'error_message': None,
            'started_at': '2026-04-21T10:00:00+00:00',
            'completed_at': None,
            'run_metadata': {
                'mode': 'historical_backfill',
            },
        }
        daily_running_run = {
            'id': 901,
            'market': 'KRX',
            'trade_date': '2026-04-21',
            'source': 'pykrx',
            'trigger': 'auto_close_scheduler',
            'status': 'running',
            'symbols_total': 3,
            'symbols_succeeded': 1,
            'symbols_failed': 0,
            'error_message': None,
            'started_at': '2026-04-21T09:00:00+00:00',
            'completed_at': None,
            'run_metadata': {
                'mode': 'daily_auto',
                'stage': 'technical_snapshots',
            },
        }
        daily_failed_run = {
            **daily_running_run,
            'status': 'failed',
            'symbols_failed': 2,
            'error_message': 'Marked stale after 30 minutes without completion.',
        }

        with (
            patch(
                'app.eod.pipeline_status._load_pipeline_pykrx_run_rows',
                side_effect=[
                    [historical_run, daily_running_run],
                    [historical_run, daily_failed_run],
                ],
            ),
            patch(
                'app.eod.pipeline_status.PYKRX_BACKFILL_SERVICE.reconcile_latest_daily_run_status',
            ) as reconcile_run,
        ):
            result = _load_latest_pipeline_pykrx_run(trade_date='2026-04-21')

        self.assertEqual(result['id'], daily_failed_run['id'])
        self.assertEqual(result['status'], 'failed')
        reconcile_run.assert_called_once_with(latest_run=daily_running_run)

    def test_read_returns_pending_when_no_runs_exist(self):
        service = EODPipelineStatusService(
            market_state_loader=lambda: _FakeMarketState(),
            pykrx_run_loader=lambda **_: None,
            pykrx_stage_counts_loader=lambda *_: {},
            live_symbol_count_loader=lambda: 3579,
            fundamentals_refresh_loader=lambda **_: None,
        )

        result = service.read()

        self.assertEqual(result['market_date'], '2026-04-21')
        self.assertEqual(result['status'], 'pending')
        self.assertEqual(result['stages']['price_collection']['status'], 'pending')
        self.assertEqual(result['stages']['price_collection']['total'], 3579)
        self.assertEqual(result['stages']['trending_ranks']['total'], 3)
        self.assertEqual(result['stages']['fundamentals_daily_refresh']['status'], 'pending')
        self.assertEqual(result['stages']['momentum_factors']['status'], 'pending')
        self.assertEqual(result['stages']['composite_factors']['status'], 'pending')
        self.assertEqual(
            result['stages']['fundamentals_daily_refresh']['details'],
            {'fs_div': 'CFS'},
        )

    def test_read_keeps_break_day_default_market_date_on_current_calendar_day(self):
        service = EODPipelineStatusService(
            market_state_loader=lambda: _FakeBreakDayMarketState(),
            pykrx_run_loader=lambda **_: None,
            pykrx_stage_counts_loader=lambda *_: {},
            live_symbol_count_loader=lambda: 3579,
            fundamentals_refresh_loader=lambda **_: None,
        )

        result = service.read()

        self.assertEqual(result['market_date'], '2026-04-14')
        self.assertEqual(
            result['market'],
            {
                'market': 'KRX',
                'state': 'CLOSED',
                'is_trading_day': False,
                'is_open': False,
                'reason': 'holiday',
                'server_time': '2026-04-14T12:15:00+09:00',
            },
        )
        self.assertEqual(result['status'], 'pending')
        self.assertEqual(result['stages']['price_collection']['market_date'], '2026-04-14')
        self.assertEqual(result['stages']['price_collection']['status'], 'pending')
        self.assertEqual(result['stages']['fundamentals_daily_refresh']['market_date'], '2026-04-14')
        self.assertEqual(result['stages']['fundamentals_daily_refresh']['status'], 'pending')

    def test_read_reports_live_stage_progress_for_pykrx_and_fundamentals(self):
        pykrx_run = {
            'id': 101,
            'market': 'KRX',
            'trade_date': '2026-04-21',
            'source': 'pykrx',
            'trigger': 'manual_daily_api',
            'status': 'running',
            'symbols_total': 3,
            'symbols_succeeded': 3,
            'symbols_failed': 0,
            'error_message': None,
            'started_at': '2026-04-21T09:10:00+00:00',
            'completed_at': None,
            'run_metadata': {
                'mode': 'daily_auto',
                'requested_start_date': '20260421',
                'requested_end_date': '20260421',
                'latest_available_trade_date': '2026-04-21',
                'markets': ['KOSPI', 'KOSDAQ'],
                'stage': 'technical_snapshots',
                'current_step': 'computing_symbol_snapshot',
                'current_activity': 'Computing daily technical snapshots for 000660',
                'current_symbol': '000660',
                'stage_progress': {
                    'price_collection': {
                        'status': 'completed',
                        'started_at': '2026-04-21T09:10:00+00:00',
                        'updated_at': '2026-04-21T09:20:00+00:00',
                        'completed_at': '2026-04-21T09:20:00+00:00',
                        'processed': 3,
                        'total': 3,
                        'succeeded': 3,
                        'failed': 0,
                        'bars_upserted': 3,
                    },
                    'technical_snapshots': {
                        'status': 'running',
                        'started_at': '2026-04-21T09:20:10+00:00',
                        'updated_at': '2026-04-21T09:20:30+00:00',
                        'processed': 1,
                        'total': 3,
                        'succeeded': 1,
                        'failed': 0,
                        'current_symbol': '000660',
                        'current_step': 'computing_symbol_snapshot',
                        'current_activity': 'Computing daily technical snapshots for 000660',
                    },
                },
            },
        }
        stage_counts = {
            'price_collection': {'persisted': 3, 'updated_at': '2026-04-21T09:20:00+00:00'},
            'technical_snapshots': {'persisted': 1, 'updated_at': '2026-04-21T09:20:30+00:00'},
            'action_summaries': {'persisted': 0, 'updated_at': None},
            'trending_ranks': {'persisted': 0, 'periods_persisted': 0, 'updated_at': None},
        }
        fundamentals_run = {
            'id': 55,
            'market_date': '2026-04-21',
            'trigger': 'eod_pykrx_followup',
            'status': 'running',
            'fs_div': 'CFS',
            'force': False,
            'symbol_limit': None,
            'symbol_offset': 0,
            'symbols_total': 3579,
            'symbols_checked': 120,
            'symbols_missing': 10,
            'symbols_stale': 35,
            'symbols_fresh': 60,
            'symbols_refreshed': 15,
            'symbols_failed': 2,
            'events_detected': 440,
            'events_captured': 37,
            'events_failed': 1,
            'error_message': None,
            'started_at': '2026-04-21T09:21:00+00:00',
            'completed_at': None,
            'run_metadata': {
                'symbols_selected': 200,
                'stage': 'capturing_fundamentals',
                'capture_event_count': 75,
                'retry_cycle': 2,
                'retry_cycles_requested': 3,
                'batch_index': 1,
                'batch_count': 2,
                'capture_run_id': 700,
                'capture_run_ids': [699, 700],
                'sync_runs': [{'run_id': 880}, {'run_id': 881}],
                'remaining_candidate_events': 403,
                'last_progress_at': '2026-04-21T09:22:10+00:00',
            },
        }
        service = EODPipelineStatusService(
            market_state_loader=lambda: _FakeMarketState(),
            pykrx_run_loader=lambda **_: pykrx_run,
            pykrx_stage_counts_loader=lambda *_: stage_counts,
            live_symbol_count_loader=lambda: 3579,
            fundamentals_refresh_loader=lambda **_: fundamentals_run,
        )

        result = service.read()

        self.assertEqual(result['status'], 'running')
        self.assertEqual(result['stages']['price_collection']['status'], 'completed')
        self.assertEqual(result['stages']['price_collection']['processed'], 3)
        self.assertEqual(result['stages']['technical_snapshots']['status'], 'running')
        self.assertEqual(result['stages']['technical_snapshots']['current_symbol'], '000660')
        self.assertEqual(result['stages']['technical_snapshots']['processed'], 1)
        self.assertEqual(result['stages']['action_summaries']['status'], 'pending')
        self.assertEqual(result['stages']['fundamentals_daily_refresh']['status'], 'running')
        self.assertEqual(
            result['stages']['fundamentals_daily_refresh']['current_activity'],
            'Capturing DART fundamentals for 75 events, retry cycle 2/3, batch 1/2',
        )
        self.assertEqual(result['stages']['fundamentals_daily_refresh']['source_run_ids']['capture_run_ids'], [699, 700])
        self.assertNotIn(
            'trigger_after_capture_enabled',
            result['stages']['fundamentals_daily_refresh']['details'],
        )

    def test_read_downgrades_failed_pykrx_stage_when_progress_row_was_left_running(self):
        pykrx_run = {
            'id': 202,
            'market': 'KRX',
            'trade_date': '2026-04-21',
            'source': 'pykrx',
            'trigger': 'manual_daily_api',
            'status': 'failed',
            'symbols_total': 3,
            'symbols_succeeded': 1,
            'symbols_failed': 2,
            'error_message': 'Marked stale after 30 minutes without completion.',
            'started_at': '2026-04-21T09:10:00+00:00',
            'completed_at': '2026-04-21T10:00:00+00:00',
            'run_metadata': {
                'mode': 'daily_auto',
                'stage': 'technical_snapshots',
                'stage_progress': {
                    'price_collection': {
                        'status': 'completed',
                        'processed': 3,
                        'total': 3,
                        'succeeded': 3,
                        'failed': 0,
                    },
                    'technical_snapshots': {
                        'status': 'running',
                        'processed': 1,
                        'total': 3,
                        'succeeded': 1,
                        'failed': 0,
                        'current_symbol': '000660',
                        'current_step': 'computing_symbol_snapshot',
                        'current_activity': 'Computing daily technical snapshots for 000660',
                    },
                },
            },
        }
        service = EODPipelineStatusService(
            market_state_loader=lambda: _FakeMarketState(),
            pykrx_run_loader=lambda **_: pykrx_run,
            pykrx_stage_counts_loader=lambda *_: {},
            live_symbol_count_loader=lambda: 3579,
            fundamentals_refresh_loader=lambda **_: None,
        )

        result = service.read()

        self.assertEqual(result['status'], 'partial')
        self.assertEqual(result['stages']['technical_snapshots']['status'], 'failed')
        self.assertEqual(
            result['stages']['technical_snapshots']['current_activity'],
            'Marked stale after 30 minutes without completion.',
        )

    def test_read_uses_failed_refresh_error_message_instead_of_running_activity(self):
        fundamentals_run = {
            'id': 77,
            'market_date': '2026-04-21',
            'trigger': 'eod_pykrx_followup',
            'status': 'failed',
            'fs_div': 'CFS',
            'force': False,
            'symbol_limit': None,
            'symbol_offset': 0,
            'symbols_total': 3579,
            'symbols_checked': 120,
            'symbols_missing': 10,
            'symbols_stale': 35,
            'symbols_fresh': 60,
            'symbols_refreshed': 15,
            'symbols_failed': 2,
            'events_detected': 440,
            'events_captured': 37,
            'events_failed': 1,
            'error_message': 'Marked failed after exceeding the stale running threshold before a daily refresh status read',
            'started_at': '2026-04-21T09:21:00+00:00',
            'completed_at': '2026-04-21T10:21:00+00:00',
            'run_metadata': {
                'symbols_selected': 200,
                'stage': 'capturing_fundamentals',
                'capture_event_count': 75,
                'retry_cycle': 2,
                'retry_cycles_requested': 3,
                'batch_index': 1,
                'batch_count': 2,
                'capture_run_id': 700,
                'capture_run_ids': [699, 700],
                'sync_runs': [{'run_id': 880}, {'run_id': 881}],
                'remaining_candidate_events': 403,
                'last_progress_at': '2026-04-21T09:22:10+00:00',
            },
        }
        service = EODPipelineStatusService(
            market_state_loader=lambda: _FakeMarketState(),
            pykrx_run_loader=lambda **_: None,
            pykrx_stage_counts_loader=lambda *_: {},
            live_symbol_count_loader=lambda: 3579,
            fundamentals_refresh_loader=lambda **_: fundamentals_run,
        )

        result = service.read()

        self.assertEqual(result['stages']['fundamentals_daily_refresh']['status'], 'failed')
        self.assertEqual(
            result['stages']['fundamentals_daily_refresh']['current_activity'],
            'Marked failed after exceeding the stale running threshold before a daily refresh status read',
        )

    def test_read_marks_completed_core_pipeline_partial_when_factor_rows_are_missing(self):
        pykrx_run = {
            'id': 303,
            'market': 'KRX',
            'trade_date': '2026-04-21',
            'source': 'pykrx',
            'trigger': 'manual_daily_api',
            'status': 'completed',
            'symbols_total': 3,
            'symbols_succeeded': 3,
            'symbols_failed': 0,
            'error_message': None,
            'started_at': '2026-04-21T09:10:00+00:00',
            'completed_at': '2026-04-21T10:00:00+00:00',
            'run_metadata': {
                'mode': 'daily_auto',
                'latest_available_trade_date': '2026-04-21',
                'markets': ['KOSPI', 'KOSDAQ'],
            },
        }
        stage_counts = {
            'price_collection': {'persisted': 3, 'updated_at': '2026-04-21T09:20:00+00:00'},
            'technical_snapshots': {'persisted': 3, 'updated_at': '2026-04-21T09:35:00+00:00'},
            'action_summaries': {'persisted': 3, 'updated_at': '2026-04-21T09:45:00+00:00'},
            'trending_ranks': {'persisted': 9, 'periods_persisted': 3, 'updated_at': '2026-04-21T09:55:00+00:00'},
        }
        fundamentals_run = {
            'id': 88,
            'market_date': '2026-04-21',
            'trigger': 'eod_pykrx_followup',
            'status': 'completed',
            'fs_div': 'CFS',
            'force': False,
            'symbol_limit': None,
            'symbol_offset': 0,
            'symbols_total': 3,
            'symbols_checked': 3,
            'symbols_missing': 0,
            'symbols_stale': 0,
            'symbols_fresh': 2,
            'symbols_refreshed': 1,
            'symbols_failed': 0,
            'events_detected': 5,
            'events_captured': 5,
            'events_failed': 0,
            'error_message': None,
            'started_at': '2026-04-21T10:01:00+00:00',
            'completed_at': '2026-04-21T10:05:00+00:00',
            'run_metadata': {
                'symbols_selected': 3,
                'last_progress_at': '2026-04-21T10:05:00+00:00',
            },
        }
        service = EODPipelineStatusService(
            market_state_loader=lambda: _FakeMarketState(),
            pykrx_run_loader=lambda **_: pykrx_run,
            pykrx_stage_counts_loader=lambda *_: stage_counts,
            live_symbol_count_loader=lambda: 3579,
            fundamentals_refresh_loader=lambda **_: fundamentals_run,
        )

        result = service.read()

        self.assertEqual(result['status'], 'partial')
        self.assertEqual(result['stages']['fundamentals_daily_refresh']['status'], 'completed')
        self.assertEqual(result['stages']['momentum_factors']['status'], 'pending')
        self.assertEqual(result['stages']['value_factors']['status'], 'pending')
        self.assertEqual(result['stages']['composite_factors']['status'], 'pending')
        self.assertEqual(
            result['stage_order'],
            [
                'price_collection',
                'technical_snapshots',
                'action_summaries',
                'trending_ranks',
                'fundamentals_daily_refresh',
                'momentum_factors',
                'value_factors',
                'low_risk_factors',
                'beta_factors',
                'advanced_value_factors',
                'unified_value_factors',
                'quality_factors',
                'composite_factors',
            ],
        )

    def test_read_maps_completed_pipeline_stages_from_completed_run_and_aggregate_counts(self):
        pykrx_run = {
            'id': 303,
            'market': 'KRX',
            'trade_date': '2026-04-21',
            'source': 'pykrx',
            'trigger': 'manual_daily_api',
            'status': 'completed',
            'symbols_total': 3,
            'symbols_succeeded': 3,
            'symbols_failed': 0,
            'error_message': None,
            'started_at': '2026-04-21T09:10:00+00:00',
            'completed_at': '2026-04-21T10:00:00+00:00',
            'run_metadata': {
                'mode': 'daily_auto',
                'latest_available_trade_date': '2026-04-21',
                'markets': ['KOSPI', 'KOSDAQ'],
            },
        }
        stage_counts = {
            'price_collection': {'persisted': 3, 'updated_at': '2026-04-21T09:20:00+00:00'},
            'technical_snapshots': {'persisted': 3, 'updated_at': '2026-04-21T09:35:00+00:00'},
            'action_summaries': {'persisted': 3, 'updated_at': '2026-04-21T09:45:00+00:00'},
            'trending_ranks': {'persisted': 9, 'periods_persisted': 3, 'updated_at': '2026-04-21T09:55:00+00:00'},
        }
        fundamentals_run = {
            'id': 88,
            'market_date': '2026-04-21',
            'trigger': 'eod_pykrx_followup',
            'status': 'completed',
            'fs_div': 'CFS',
            'force': False,
            'symbol_limit': None,
            'symbol_offset': 0,
            'symbols_total': 3,
            'symbols_checked': 3,
            'symbols_missing': 0,
            'symbols_stale': 0,
            'symbols_fresh': 2,
            'symbols_refreshed': 1,
            'symbols_failed': 0,
            'events_detected': 5,
            'events_captured': 5,
            'events_failed': 0,
            'error_message': None,
            'started_at': '2026-04-21T10:01:00+00:00',
            'completed_at': '2026-04-21T10:05:00+00:00',
            'run_metadata': {
                'symbols_selected': 3,
                'last_progress_at': '2026-04-21T10:05:00+00:00',
            },
        }
        service = EODPipelineStatusService(
            market_state_loader=lambda: _FakeMarketState(),
            pykrx_run_loader=lambda **_: pykrx_run,
            pykrx_stage_counts_loader=lambda *_: stage_counts,
            live_symbol_count_loader=lambda: 3579,
            fundamentals_refresh_loader=lambda **_: fundamentals_run,
            factor_stage_counts_loader=lambda **_: _full_factor_stage_counts(
                total=3,
                updated_at='2026-04-21T10:15:00+00:00',
            ),
        )

        result = service.read()

        self.assertEqual(result['status'], 'completed')
        self.assertEqual(result['stages']['price_collection']['status'], 'completed')
        self.assertEqual(result['stages']['technical_snapshots']['status'], 'completed')
        self.assertEqual(result['stages']['action_summaries']['status'], 'completed')
        self.assertEqual(result['stages']['trending_ranks']['status'], 'completed')
        self.assertEqual(result['stages']['fundamentals_daily_refresh']['status'], 'completed')
        self.assertEqual(result['stages']['momentum_factors']['status'], 'completed')
        self.assertEqual(result['stages']['composite_factors']['status'], 'completed')
        self.assertEqual(result['stages']['trending_ranks']['processed'], 3)
        self.assertEqual(result['stages']['fundamentals_daily_refresh']['succeeded'], 3)
