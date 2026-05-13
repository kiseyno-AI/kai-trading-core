import unittest
from unittest.mock import patch

from app.eod.pipeline_status import (
    DEFAULT_FACTOR_STAGE_ORDER,
    DEFAULT_PYKRX_ENRICHMENT_CATEGORIES,
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
    current_calendar_date = '2026-04-21'
    latest_valid_trading_date = '2026-04-21'
    market_session = 'closed'


class _FakeBreakDayMarketState:
    market = 'KRX'
    state = 'HOLIDAY'
    is_trading_day = False
    is_open = False
    reason = 'holiday'
    server_time = '2026-05-05T12:15:00+09:00'
    current_calendar_date = '2026-05-05'
    latest_valid_trading_date = '2026-05-04'
    market_session = 'holiday'


def _full_factor_stage_counts(*, total: int, updated_at: str) -> dict[str, dict]:
    return {
        stage_id: {
            'persisted': total,
            'updated_at': updated_at,
        }
        for stage_id in DEFAULT_FACTOR_STAGE_ORDER
    }


def _full_feedback_prerequisite_stage_counts(*, total: int, updated_at: str) -> dict[str, dict]:
    return {
        'market_risk_filter': {
            'persisted': total,
            'updated_at': updated_at,
        },
        'piotroski_filter': {
            'persisted': total,
            'updated_at': updated_at,
        },
        **_full_factor_stage_counts(total=total, updated_at=updated_at),
    }


def _full_pykrx_enrichment_coverage(*, symbol_count: int, market_count: int, updated_at: str) -> dict[str, dict]:
    return {
        'requested_symbol_count': symbol_count,
        'requested_market_count': market_count,
        'categories': {
            category: {
                'present_rows': symbol_count if category not in {'market_breadth', 'investor_flow_market'} else market_count,
                'expected_rows': symbol_count if category not in {'market_breadth', 'investor_flow_market'} else market_count,
                'coverage_status': 'complete',
                'latest_captured_at': updated_at,
            }
            for category in DEFAULT_PYKRX_ENRICHMENT_CATEGORIES
        },
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

        self.assertEqual(result['market_date'], '2026-05-05')
        self.assertEqual(
            result['market'],
            {
                'market': 'KRX',
                'state': 'HOLIDAY',
                'is_trading_day': False,
                'is_open': False,
                'reason': 'holiday',
                'server_time': '2026-05-05T12:15:00+09:00',
                'current_calendar_date': '2026-05-05',
                'latest_valid_trading_date': '2026-05-04',
                'market_session': 'holiday',
            },
        )
        self.assertEqual(result['market_session']['status'], 'holiday')
        self.assertEqual(result['market_session']['latest_valid_trading_date'], '2026-05-04')
        self.assertFalse(result['market_session']['intraday_data_expected'])
        self.assertEqual(result['status'], 'pending')
        self.assertEqual(result['stages']['price_collection']['market_date'], '2026-05-05')
        self.assertEqual(result['stages']['price_collection']['status'], 'pending')
        self.assertEqual(result['stages']['fundamentals_daily_refresh']['market_date'], '2026-05-05')
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
                'pykrx_enrichment',
                'technical_snapshots',
                'action_summaries',
                'trending_ranks',
                'fundamentals_daily_refresh',
                'market_risk_filter',
                'piotroski_filter',
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

    def test_read_keeps_blocked_factor_stage_pending_when_stored_rows_exist_before_fundamentals(self):
        pykrx_run = {
            'id': 304,
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
        factor_stage_counts = {
            'momentum_factors': {
                'persisted': 2,
                'updated_at': '2026-04-21T10:10:00+00:00',
            },
            'advanced_value_factors': {
                'persisted': 3,
                'updated_at': '2026-04-21T10:15:00+00:00',
            },
        }
        service = EODPipelineStatusService(
            market_state_loader=lambda: _FakeMarketState(),
            pykrx_run_loader=lambda **_: pykrx_run,
            pykrx_stage_counts_loader=lambda *_: stage_counts,
            live_symbol_count_loader=lambda: 3579,
            fundamentals_refresh_loader=lambda **_: None,
            factor_stage_counts_loader=lambda **_: factor_stage_counts,
        )

        result = service.read()

        self.assertEqual(result['status'], 'partial')
        self.assertEqual(result['stages']['momentum_factors']['status'], 'partial')
        self.assertEqual(
            result['stages']['momentum_factors']['current_activity'],
            'Stored 2/3 rows for momentum factor compute',
        )
        self.assertEqual(result['stages']['advanced_value_factors']['status'], 'pending')
        self.assertEqual(
            result['stages']['advanced_value_factors']['current_activity'],
            'Waiting on DART fundamentals daily refresh',
        )
        self.assertEqual(
            result['stages']['advanced_value_factors']['details']['blocking_stages'],
            ['fundamentals_daily_refresh'],
        )
        self.assertEqual(
            result['stages']['advanced_value_factors']['source_status'],
            'stored_rows_present',
        )
        self.assertIsNone(result['stages']['advanced_value_factors']['completed_at'])

    def test_read_keeps_blocked_factor_stage_pending_when_dependency_failed(self):
        pykrx_run = {
            'id': 305,
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
            'id': 89,
            'market_date': '2026-04-21',
            'trigger': 'eod_pykrx_followup',
            'status': 'failed',
            'fs_div': 'CFS',
            'force': False,
            'symbol_limit': None,
            'symbol_offset': 0,
            'symbols_total': 3,
            'symbols_checked': 3,
            'symbols_missing': 1,
            'symbols_stale': 1,
            'symbols_fresh': 1,
            'symbols_refreshed': 0,
            'symbols_failed': 1,
            'events_detected': 5,
            'events_captured': 2,
            'events_failed': 1,
            'error_message': 'Fundamentals refresh failed',
            'started_at': '2026-04-21T10:01:00+00:00',
            'completed_at': '2026-04-21T10:05:00+00:00',
            'run_metadata': {
                'symbols_selected': 3,
                'last_progress_at': '2026-04-21T10:05:00+00:00',
            },
        }
        factor_stage_counts = {
            'advanced_value_factors': {
                'persisted': 3,
                'updated_at': '2026-04-21T10:15:00+00:00',
            },
        }
        service = EODPipelineStatusService(
            market_state_loader=lambda: _FakeMarketState(),
            pykrx_run_loader=lambda **_: pykrx_run,
            pykrx_stage_counts_loader=lambda *_: stage_counts,
            live_symbol_count_loader=lambda: 3579,
            fundamentals_refresh_loader=lambda **_: fundamentals_run,
            factor_stage_counts_loader=lambda **_: factor_stage_counts,
        )

        result = service.read()

        self.assertEqual(result['status'], 'partial')
        self.assertEqual(result['stages']['fundamentals_daily_refresh']['status'], 'failed')
        self.assertEqual(result['stages']['advanced_value_factors']['status'], 'pending')
        self.assertEqual(
            result['stages']['advanced_value_factors']['current_activity'],
            'Blocked by failed DART fundamentals daily refresh',
        )
        self.assertEqual(
            result['stages']['advanced_value_factors']['details']['failed_blocking_stages'],
            ['fundamentals_daily_refresh'],
        )
        self.assertIsNone(result['stages']['advanced_value_factors']['completed_at'])

    def test_read_prefers_stored_price_bar_count_when_price_stage_metadata_is_stale(self):
        pykrx_run = {
            'id': 306,
            'market': 'KRX',
            'trade_date': '2026-05-06',
            'source': 'pykrx',
            'trigger': 'auto_close_scheduler',
            'status': 'completed',
            'symbols_total': 2811,
            'symbols_succeeded': 2811,
            'symbols_failed': 0,
            'error_message': None,
            'started_at': '2026-05-06T06:10:00+00:00',
            'completed_at': '2026-05-06T07:00:00+00:00',
            'run_metadata': {
                'mode': 'daily_auto',
                'latest_available_trade_date': '2026-05-06',
                'markets': ['KOSPI', 'KOSDAQ'],
            },
        }
        stage_counts = {
            'price_collection': {'persisted': 2810, 'updated_at': '2026-05-06T06:20:00+00:00'},
            'technical_snapshots': {'persisted': 2810, 'updated_at': '2026-05-06T06:35:00+00:00'},
            'action_summaries': {'persisted': 2810, 'updated_at': '2026-05-06T06:45:00+00:00'},
            'trending_ranks': {'persisted': 8430, 'periods_persisted': 3, 'updated_at': '2026-05-06T06:55:00+00:00'},
        }
        fundamentals_run = {
            'id': 90,
            'market_date': '2026-05-06',
            'trigger': 'eod_pykrx_followup',
            'status': 'completed',
            'fs_div': 'CFS',
            'force': False,
            'symbol_limit': None,
            'symbol_offset': 0,
            'symbols_total': 2810,
            'symbols_checked': 2810,
            'symbols_missing': 0,
            'symbols_stale': 0,
            'symbols_fresh': 2750,
            'symbols_refreshed': 60,
            'symbols_failed': 0,
            'events_detected': 5,
            'events_captured': 5,
            'events_failed': 0,
            'error_message': None,
            'started_at': '2026-05-06T07:01:00+00:00',
            'completed_at': '2026-05-06T07:05:00+00:00',
            'run_metadata': {
                'symbols_selected': 2810,
                'last_progress_at': '2026-05-06T07:05:00+00:00',
            },
        }
        service = EODPipelineStatusService(
            market_state_loader=lambda: _FakeMarketState(),
            pykrx_run_loader=lambda **_: pykrx_run,
            pykrx_stage_counts_loader=lambda *_: stage_counts,
            live_symbol_count_loader=lambda: 3579,
            fundamentals_refresh_loader=lambda **_: fundamentals_run,
            row_count_stage_counts_loader=lambda **_: _full_feedback_prerequisite_stage_counts(
                total=2810,
                updated_at='2026-05-06T07:15:00+00:00',
            ),
            price_bar_count_loader=lambda **_: 2810,
        )

        result = service.read(market_date='2026-05-06')

        self.assertEqual(result['stages']['momentum_factors']['status'], 'completed')
        self.assertEqual(result['stages']['unified_value_factors']['status'], 'completed')
        self.assertEqual(result['stages']['quality_factors']['status'], 'completed')
        self.assertEqual(result['stages']['momentum_factors']['details']['expected_rows'], 2810)
        self.assertEqual(
            result['stages']['momentum_factors']['details']['expected_rows_source'],
            'daily_price_bars',
        )
        self.assertEqual(
            result['stages']['momentum_factors']['details']['price_bars_persisted'],
            2810,
        )
        self.assertEqual(
            result['stages']['momentum_factors']['details']['price_stage_expected_rows'],
            2810,
        )

    def test_read_prefers_date_scoped_price_bars_over_limited_retry_run_counts_for_factor_progress(self):
        pykrx_run = {
            'id': 307,
            'market': 'KRX',
            'trade_date': '2026-05-06',
            'source': 'pykrx',
            'trigger': 'manual_daily_api',
            'status': 'completed',
            'symbols_total': 100,
            'symbols_succeeded': 100,
            'symbols_failed': 0,
            'error_message': None,
            'started_at': '2026-05-06T06:10:00+00:00',
            'completed_at': '2026-05-06T07:00:00+00:00',
            'run_metadata': {
                'mode': 'daily_auto',
                'latest_available_trade_date': '2026-05-06',
                'requested_start_date': '20260506',
                'requested_end_date': '20260506',
                'limited_run': True,
                'retry': {
                    'retry_of_run_id': 300,
                    'retry_cycle': 2,
                    'symbol_count': 100,
                },
                'markets': ['KOSPI', 'KOSDAQ'],
            },
        }
        stage_counts = {
            'price_collection': {'persisted': 100, 'updated_at': '2026-05-06T06:20:00+00:00'},
            'technical_snapshots': {'persisted': 100, 'updated_at': '2026-05-06T06:35:00+00:00'},
            'action_summaries': {'persisted': 100, 'updated_at': '2026-05-06T06:45:00+00:00'},
            'trending_ranks': {'persisted': 300, 'periods_persisted': 3, 'updated_at': '2026-05-06T06:55:00+00:00'},
        }
        fundamentals_run = {
            'id': 91,
            'market_date': '2026-05-06',
            'trigger': 'eod_pykrx_followup',
            'status': 'completed',
            'fs_div': 'CFS',
            'force': False,
            'symbol_limit': None,
            'symbol_offset': 0,
            'symbols_total': 2810,
            'symbols_checked': 2810,
            'symbols_missing': 0,
            'symbols_stale': 0,
            'symbols_fresh': 2750,
            'symbols_refreshed': 60,
            'symbols_failed': 0,
            'events_detected': 5,
            'events_captured': 5,
            'events_failed': 0,
            'error_message': None,
            'started_at': '2026-05-06T07:01:00+00:00',
            'completed_at': '2026-05-06T07:05:00+00:00',
            'run_metadata': {
                'symbols_selected': 2810,
                'last_progress_at': '2026-05-06T07:05:00+00:00',
            },
        }
        factor_stage_counts = {
            'momentum_factors': {
                'persisted': 1000,
                'updated_at': '2026-05-06T07:15:00+00:00',
            },
        }
        service = EODPipelineStatusService(
            market_state_loader=lambda: _FakeMarketState(),
            pykrx_run_loader=lambda **_: pykrx_run,
            pykrx_stage_counts_loader=lambda *_: stage_counts,
            live_symbol_count_loader=lambda: 3579,
            fundamentals_refresh_loader=lambda **_: fundamentals_run,
            factor_stage_counts_loader=lambda **_: factor_stage_counts,
            price_bar_count_loader=lambda **_: 2810,
        )

        result = service.read(market_date='2026-05-06')

        self.assertEqual(result['status'], 'partial')
        self.assertEqual(result['stages']['momentum_factors']['status'], 'partial')
        self.assertEqual(result['stages']['momentum_factors']['total'], 2810)
        self.assertEqual(
            result['stages']['momentum_factors']['current_activity'],
            'Stored 1000/2810 rows for momentum factor compute',
        )
        self.assertEqual(result['stages']['momentum_factors']['details']['expected_rows'], 2810)
        self.assertEqual(
            result['stages']['momentum_factors']['details']['expected_rows_source'],
            'daily_price_bars',
        )
        self.assertEqual(
            result['stages']['momentum_factors']['details']['price_bars_persisted'],
            2810,
        )
        self.assertEqual(
            result['stages']['momentum_factors']['details']['price_stage_expected_rows'],
            2810,
        )

    def test_read_treats_stored_full_coverage_as_completed_after_scoped_retry_run(self):
        pykrx_run = {
            'id': 401,
            'market': 'KRX',
            'trade_date': '2026-04-21',
            'source': 'pykrx',
            'trigger': 'auto_close_scheduler',
            'status': 'completed_with_errors',
            'symbols_total': 877,
            'symbols_succeeded': 874,
            'symbols_failed': 3,
            'error_message': 'Failed symbols: 139050,217620,452980',
            'started_at': '2026-04-21T09:50:00+00:00',
            'completed_at': '2026-04-21T09:55:00+00:00',
            'run_metadata': {
                'mode': 'daily_auto',
                'stage_progress': {
                    'price_collection': {'status': 'partial', 'processed': 3, 'total': 3, 'succeeded': 0, 'failed': 3},
                    'technical_snapshots': {'status': 'partial', 'processed': 3, 'total': 3, 'succeeded': 0, 'failed': 3},
                    'action_summaries': {'status': 'partial', 'processed': 3, 'total': 3, 'succeeded': 0, 'failed': 3},
                },
            },
        }
        stage_counts = {
            'price_collection': {'persisted': 0, 'updated_at': None},
            'technical_snapshots': {'persisted': 0, 'updated_at': None},
            'action_summaries': {'persisted': 0, 'updated_at': None},
            'trending_ranks': {'persisted': 9, 'periods_persisted': 3, 'updated_at': '2026-04-21T09:55:00+00:00'},
        }
        service = EODPipelineStatusService(
            market_state_loader=lambda: _FakeMarketState(),
            pykrx_run_loader=lambda **_: pykrx_run,
            pykrx_stage_counts_loader=lambda *_: stage_counts,
            live_symbol_count_loader=lambda: 4458,
            fundamentals_refresh_loader=lambda **_: None,
            price_bar_count_loader=lambda **_: 3683,
            technical_snapshot_count_loader=lambda _: 3683,
            action_summary_count_loader=lambda _: 3683,
            row_count_stage_counts_loader=lambda **_: {
                'market_risk_filter': {'persisted': 3683, 'updated_at': '2026-04-21T10:15:00+00:00'},
                'piotroski_filter': {'persisted': 3683, 'updated_at': '2026-04-21T10:15:00+00:00'},
            },
        )

        result = service.read()

        for stage_id in ('price_collection', 'technical_snapshots', 'action_summaries'):
            self.assertEqual(result['stages'][stage_id]['status'], 'completed')
            self.assertEqual(result['stages'][stage_id]['total'], 3683)
            self.assertEqual(result['stages'][stage_id]['succeeded'], 3683)
            self.assertEqual(result['stages'][stage_id]['failed'], 0)
        self.assertEqual(result['stages']['market_risk_filter']['status'], 'completed')
        self.assertEqual(result['stages']['market_risk_filter']['details']['blocking_stages'], [])

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
            row_count_stage_counts_loader=lambda **_: _full_feedback_prerequisite_stage_counts(
                total=3,
                updated_at='2026-04-21T10:15:00+00:00',
            ),
            pykrx_enrichment_loader=lambda **_: _full_pykrx_enrichment_coverage(
                symbol_count=3,
                market_count=2,
                updated_at='2026-04-21T10:12:00+00:00',
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

    def test_read_includes_pykrx_enrichment_stage_from_coverage(self):
        coverage = {
            'requested_symbol_count': 2,
            'requested_market_count': 1,
            'categories': {
                category: {
                    'present_rows': 0,
                    'expected_rows': 2 if category not in {'market_breadth', 'investor_flow_market'} else 1,
                    'coverage_status': 'missing',
                    'latest_captured_at': None,
                }
                for category in DEFAULT_PYKRX_ENRICHMENT_CATEGORIES
            },
        }
        coverage['categories']['sector_classification'] = {
            'present_rows': 2,
            'expected_rows': 2,
            'coverage_status': 'complete',
            'latest_captured_at': '2026-04-21T15:10:00+09:00',
        }
        coverage['categories']['market_breadth'] = {
            'present_rows': 1,
            'expected_rows': 1,
            'coverage_status': 'complete',
            'latest_captured_at': '2026-04-21T15:10:00+09:00',
        }

        service = EODPipelineStatusService(
            market_state_loader=lambda: _FakeMarketState(),
            pykrx_run_loader=lambda **_: None,
            pykrx_stage_counts_loader=lambda *_: {},
            live_symbol_count_loader=lambda: 2,
            fundamentals_refresh_loader=lambda **_: None,
            factor_stage_counts_loader=lambda **_: {},
            price_bar_count_loader=lambda **_: 0,
            pykrx_enrichment_loader=lambda **_: coverage,
        )

        result = service.read(market_date='2026-04-21', fs_div='CFS')

        self.assertEqual(result['stage_order'][1], 'pykrx_enrichment')
        self.assertEqual(result['stages']['pykrx_enrichment']['status'], 'partial')
        self.assertEqual(result['stages']['pykrx_enrichment']['details']['requested_symbol_count'], 2)
        self.assertEqual(
            result['stages']['pykrx_enrichment']['details']['categories']['sector_classification']['coverage_status'],
            'complete',
        )
