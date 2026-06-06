from __future__ import annotations

from types import SimpleNamespace
from contextlib import ExitStack, contextmanager
from datetime import datetime, timedelta, timezone
from unittest import IsolatedAsyncioTestCase, TestCase
from unittest.mock import ANY, AsyncMock, patch

import pandas as pd

from app.eod.auto_pykrx import AutoPykrxEODScheduler
from app.eod.pykrx_backfill import PYKRX_STAGE_SEQUENCE, PykrxBackfillService, PykrxSymbol
from app.eod.pykrx_pipeline_runner import _OfficialDailyPipelineTracker, _normalize_stage_status
from app.market.clock import KST, MarketState


@contextmanager
def _lock(acquired: bool = True):
    yield acquired


def _ready_pykrx_enrichment_result(*, market_date: str = '2026-04-03') -> dict:
    categories = {
        'sector_classification': {'rows_persisted': 1, 'errors': []},
        'investor_flow_market': {'rows_persisted': 1, 'errors': []},
        'investor_flow_symbol': {'rows_persisted': 1, 'errors': []},
        'short_selling': {'rows_persisted': 1, 'errors': []},
        'valuation_snapshot': {'rows_persisted': 1, 'errors': []},
        'market_breadth': {'rows_persisted': 1, 'errors': []},
        'foreign_ownership': {'rows_persisted': 1, 'errors': []},
    }
    return {
        'market_date': market_date,
        'status': 'completed',
        'categories_requested': list(categories),
        'categories': categories,
        'coverage': {
            'requested_symbol_count': 1,
            'requested_market_count': 1,
            'categories': {
                category: {
                    'scope_type': 'symbol' if category in {'investor_flow_symbol', 'short_selling'} else 'market',
                    'expected_rows': 1,
                    'present_rows': 1,
                    'coverage_ratio': 1.0,
                    'coverage_status': 'complete',
                }
                for category in categories
            },
        },
        'summary': {'categories_failed': 0},
    }


class _FakeDailyPipelineRunStateService:
    def __init__(self) -> None:
        self.start_runs: list[dict] = []
        self.stage_updates: list[dict] = []
        self.finish_runs: list[dict] = []

    def start_run(self, **kwargs):
        self.start_runs.append(kwargs)
        return {'id': 501}

    def update_stage(self, **kwargs):
        self.stage_updates.append(kwargs)
        return kwargs

    def finish_run(self, **kwargs):
        self.finish_runs.append(kwargs)
        return kwargs


class PykrxBackfillServiceTests(TestCase):
    def test_auto_tracker_normalizes_warning_followup_status_to_terminal_partial(self) -> None:
        fake_run_state_service = _FakeDailyPipelineRunStateService()
        log_events: list[dict] = []
        tracker = _OfficialDailyPipelineTracker(
            service=fake_run_state_service,
            log_fetch=lambda operation, status, **kwargs: log_events.append(
                {'operation': operation, 'status': status, **kwargs}
            ),
        )

        tracker.start(
            market_date='2026-04-03',
            trigger='auto_close_scheduler',
            requested_stages=['signal_decider_evaluations'],
            resolved_stages=['signal_decider_evaluations'],
            eod_run_id=12,
            run_metadata={'mode': 'daily_auto'},
        )
        tracker.update_stage(
            stage='signal_decider_evaluations',
            status='warning',
            current_step='signal_decider_evaluations_partial',
            current_activity='Completed signal-decider evaluation follow-up with mature coverage gaps',
            processed=4,
            total=4,
            succeeded=3,
            failed=1,
            details={'result_summary': {'status': 'warning', 'summary': {'not_computed_count': 1}}},
        )
        tracker.finish_run()

        self.assertEqual(_normalize_stage_status('warning'), 'partial')
        self.assertEqual(log_events, [])
        self.assertEqual(fake_run_state_service.stage_updates[-1]['status'], 'partial')
        self.assertEqual(tracker.stage_statuses['signal_decider_evaluations'], 'partial')
        self.assertEqual(fake_run_state_service.finish_runs[-1]['status'], 'partial')
        self.assertEqual(
            fake_run_state_service.finish_runs[-1]['run_metadata']['stage_statuses'],
            {'signal_decider_evaluations': 'partial'},
        )

    def test_fetch_recent_universe_uses_index_when_market_frame_columns_change(self) -> None:
        service = PykrxBackfillService()
        service.name_pause_seconds = 0
        frame = pd.DataFrame({'현재가': [196500]}, index=['005930'])

        def fake_call(func, *args, **kwargs):
            if func.__name__ == 'get_market_ohlcv_by_ticker':
                return frame
            if func.__name__ == 'get_market_ticker_name':
                return '삼성전자'
            raise AssertionError(f'unexpected pykrx function: {func.__name__}')

        with (
            patch.object(service, 'resolve_latest_trade_date', return_value='2026-04-07'),
            patch('app.eod.pykrx_backfill._call_pykrx', side_effect=fake_call),
        ):
            result = service.fetch_recent_universe_snapshot(markets=('KOSPI',))

        self.assertEqual(result['latest_trade_date'], '2026-04-07')
        self.assertEqual(result['universe_source'], 'pykrx_market_ohlcv')
        self.assertEqual(
            result['symbols'],
            [PykrxSymbol(symbol='005930', market='KOSPI', name_ko='삼성전자')],
        )

    def test_fetch_recent_universe_falls_back_to_ticker_when_name_lookup_returns_frame(self) -> None:
        service = PykrxBackfillService()
        service.name_pause_seconds = 0
        frame = pd.DataFrame({'현재가': [196500]}, index=['005930'])
        bad_name = pd.DataFrame({'unexpected': ['html']})

        def fake_call(func, *args, **kwargs):
            if func.__name__ == 'get_market_ohlcv_by_ticker':
                return frame
            if func.__name__ == 'get_market_ticker_name':
                return bad_name
            raise AssertionError(f'unexpected pykrx function: {func.__name__}')

        with (
            patch.object(service, 'resolve_latest_trade_date', return_value='2026-04-07'),
            patch('app.eod.pykrx_backfill._call_pykrx', side_effect=fake_call),
        ):
            result = service.fetch_recent_universe_snapshot(markets=('KOSPI',))

        self.assertEqual(
            result['symbols'],
            [PykrxSymbol(symbol='005930', market='KOSPI', name_ko='005930')],
        )

    def test_fetch_recent_universe_falls_back_per_market_when_pykrx_market_ohlcv_breaks(self) -> None:
        service = PykrxBackfillService()
        service.name_pause_seconds = 0

        with (
            patch.object(service, 'resolve_latest_trade_date', return_value='2026-04-07'),
            patch(
                'app.eod.pykrx_backfill._call_pykrx',
                side_effect=KeyError("None of [Index(['시가', '고가', '저가', '종가'], dtype='object')] are in the [columns]"),
            ),
            patch.object(
                service,
                '_fetch_master_fallback_universe',
                return_value=[PykrxSymbol(symbol='000020', market='KOSPI', name_ko='동화약품')],
            ) as fallback_universe,
        ):
            result = service.fetch_recent_universe_snapshot(markets=('KOSPI',))

        self.assertEqual(result['latest_trade_date'], '2026-04-07')
        self.assertEqual(result['universe_source'], 'pykrx_market_ohlcv_with_master_fallback')
        self.assertEqual(
            result['symbols'],
            [PykrxSymbol(symbol='000020', market='KOSPI', name_ko='동화약품')],
        )
        fallback_universe.assert_called_once_with(markets=('KOSPI',), limit=None)

    def test_refresh_recent_universe_preserves_snapshot_and_persist_counts(self) -> None:
        service = PykrxBackfillService()
        snapshot = {
            'symbols': [PykrxSymbol(symbol='005930', market='KOSPI', name_ko='삼성전자')],
            'latest_trade_date': '2026-04-07',
            'universe_source': 'pykrx_market_ohlcv',
        }

        with (
            patch.object(service, 'fetch_recent_universe_snapshot', return_value=snapshot) as fetch_snapshot,
            patch.object(service, '_persist_universe', return_value=1) as persist_universe,
        ):
            result = service.refresh_recent_universe(markets=('KOSPI',), deactivate_missing=False)

        self.assertEqual(
            result,
            {
                'source': 'pykrx',
                'markets': ['KOSPI'],
                'symbol_count': 1,
                'persisted_count': 1,
                'latest_trade_date': '2026-04-07',
                'universe_source': 'pykrx_market_ohlcv',
                'deactivate_missing': False,
            },
        )
        fetch_snapshot.assert_called_once_with(markets=('KOSPI',), limit=None)
        persist_universe.assert_called_once_with(snapshot['symbols'], deactivate_missing=False)

    def test_run_daily_pipeline_skips_completed_trade_date(self) -> None:
        service = PykrxBackfillService()

        with (
            patch.object(service, 'resolve_latest_trade_date', return_value='2026-04-03'),
            patch('app.eod.pykrx_backfill.advisory_eod_collection_lock', return_value=_lock()),
            patch(
                'app.eod.pykrx_backfill.load_latest_eod_collection_run',
                return_value={
                    'id': 7,
                    'status': 'completed',
                    'symbols_failed': 0,
                },
            ),
            patch.object(service, '_run_market_data_pipeline') as run_collection,
        ):
            result = service.run_daily_pipeline_if_due(
                market_date='2026-04-03',
                trigger='auto_close_scheduler',
            )

        self.assertFalse(result['triggered'])
        self.assertEqual(result['reason'], 'already_completed')
        run_collection.assert_not_called()

    def test_run_daily_pipeline_uses_scheduled_stage_sequence_with_pykrx_enrichment(self) -> None:
        service = PykrxBackfillService()

        with (
            patch.object(service, 'resolve_latest_trade_date', return_value='2026-04-03'),
            patch('app.eod.pykrx_backfill.advisory_eod_collection_lock', return_value=_lock()),
            patch('app.eod.pykrx_backfill.load_latest_eod_collection_run', return_value=None),
            patch.object(
                service,
                '_run_market_data_pipeline',
                return_value={
                    'run_id': 9,
                    'source': 'pykrx',
                    'status': 'completed',
                },
            ) as run_collection,
        ):
            result = service.run_daily_pipeline_if_due(
                market_date='2026-04-03',
                trigger='auto_close_scheduler',
            )

        self.assertTrue(result['triggered'])
        kwargs = run_collection.call_args.kwargs
        self.assertEqual(kwargs['selected_stages'], PYKRX_STAGE_SEQUENCE)
        self.assertTrue(kwargs['track_daily_pipeline'])
        self.assertIn('pykrx_enrichment', kwargs['selected_stages'])
        self.assertLess(
            kwargs['selected_stages'].index('price_collection'),
            kwargs['selected_stages'].index('pykrx_enrichment'),
        )
        self.assertLess(
            kwargs['selected_stages'].index('pykrx_enrichment'),
            kwargs['selected_stages'].index('technical_snapshots'),
        )

    def test_run_daily_pipeline_force_bypasses_stale_running_row(self) -> None:
        service = PykrxBackfillService()

        with (
            patch.object(service, 'resolve_latest_trade_date', return_value='2026-04-03'),
            patch('app.eod.pykrx_backfill.advisory_eod_collection_lock', return_value=_lock()),
            patch(
                'app.eod.pykrx_backfill.load_latest_eod_collection_run',
                return_value={
                    'id': 8,
                    'status': 'running',
                    'symbols_failed': 0,
                },
            ),
            patch.object(
                service,
                '_run_market_data_pipeline',
                return_value={
                    'run_id': 9,
                    'source': 'pykrx',
                    'status': 'completed',
                },
            ) as run_collection,
        ):
            result = service.run_daily_pipeline_if_due(
                market_date='2026-04-03',
                trigger='manual_daily_api',
                force=True,
            )

        self.assertTrue(result['triggered'])
        self.assertEqual(result['run_id'], 9)
        run_collection.assert_called_once()

    def test_run_daily_pipeline_marks_stale_running_row_failed_and_retries(self) -> None:
        service = PykrxBackfillService()
        stale_started_at = (datetime.now(timezone.utc) - timedelta(minutes=45)).isoformat()

        with (
            patch.object(service, 'resolve_latest_trade_date', return_value='2026-04-03'),
            patch('app.eod.pykrx_backfill.advisory_eod_collection_lock', return_value=_lock()),
            patch(
                'app.eod.pykrx_backfill.load_latest_eod_collection_run',
                side_effect=[
                    {
                        'id': 8,
                        'status': 'running',
                        'symbols_total': 10,
                        'symbols_succeeded': 0,
                        'symbols_failed': 0,
                        'started_at': stale_started_at,
                        'run_metadata': {
                            'markets': ['KOSPI'],
                            'requested_symbols': [
                                '000001',
                                '000002',
                                '000003',
                                '000004',
                                '000005',
                                '000006',
                                '000007',
                                '000008',
                                '000009',
                                '000010',
                            ],
                        },
                    },
                    {
                        'id': 8,
                        'status': 'failed',
                        'symbols_total': 10,
                        'symbols_succeeded': 3,
                        'symbols_failed': 7,
                        'started_at': stale_started_at,
                        'run_metadata': {
                            'markets': ['KOSPI'],
                            'failed_symbols': [
                                '000004',
                                '000005',
                                '000006',
                                '000007',
                                '000008',
                                '000009',
                                '000010',
                            ],
                        },
                    },
                ],
            ),
            patch('app.eod.pykrx_backfill.count_eod_run_persisted_symbols', return_value=3),
            patch(
                'app.eod.pykrx_backfill.load_eod_run_persisted_symbols',
                return_value=['000001', '000002', '000003'],
            ),
            patch('app.eod.pykrx_backfill.count_eod_collection_runs', return_value=1),
            patch('app.eod.pykrx_backfill.complete_eod_collection_run') as complete_run,
            patch.object(
                service,
                '_load_retry_symbols_for_run',
                return_value=[PykrxSymbol(symbol='000004', market='KOSPI', name_ko='Retry Me')],
            ),
            patch.object(
                service,
                '_run_market_data_pipeline',
                return_value={
                    'run_id': 9,
                    'source': 'pykrx',
                    'status': 'completed',
                },
            ) as run_collection,
        ):
            result = service.run_daily_pipeline_if_due(
                market_date='2026-04-03',
                trigger='auto_close_scheduler',
            )

        self.assertTrue(result['triggered'])
        complete_run.assert_called_once_with(
            8,
            status='failed',
            symbols_succeeded=3,
            symbols_failed=7,
            error_message='Marked stale after 30 minutes without completion.',
            run_metadata=ANY,
        )
        run_collection.assert_called_once()

    def test_reconcile_latest_daily_run_status_uses_last_progress_heartbeat(self) -> None:
        service = PykrxBackfillService()
        stale_started_at = (datetime.now(timezone.utc) - timedelta(hours=2)).isoformat()
        recent_progress_at = (datetime.now(timezone.utc) - timedelta(minutes=5)).isoformat()
        running_run = {
            'id': 18,
            'status': 'running',
            'trade_date': '2026-04-03',
            'symbols_total': 10,
            'symbols_succeeded': 2,
            'symbols_failed': 0,
            'started_at': stale_started_at,
            'run_metadata': {
                'stage': 'technical_snapshots',
                'last_progress_at': recent_progress_at,
                'stage_progress': {
                    'technical_snapshots': {
                        'status': 'running',
                        'current_step': 'computing_symbol_snapshot',
                    },
                },
            },
        }

        with patch(
            'app.eod.pykrx_backfill.load_latest_eod_collection_run',
            return_value=running_run,
        ), patch.object(service, '_fail_stale_running_run') as fail_stale_run:
            result = service.reconcile_latest_daily_run_status(trade_date='2026-04-03')

        self.assertEqual(result, running_run)
        fail_stale_run.assert_not_called()

    def test_reconcile_latest_daily_run_status_marks_stale_stage_progress_failed(self) -> None:
        service = PykrxBackfillService()
        stale_progress_at = (datetime.now(timezone.utc) - timedelta(minutes=45)).isoformat()
        stale_run = {
            'id': 19,
            'status': 'running',
            'trade_date': '2026-04-03',
            'symbols_total': 4,
            'symbols_succeeded': 1,
            'symbols_failed': 0,
            'started_at': (datetime.now(timezone.utc) - timedelta(hours=2)).isoformat(),
            'run_metadata': {
                'stage': 'technical_snapshots',
                'markets': ['KOSPI'],
                'last_progress_at': stale_progress_at,
                'stage_progress': {
                    'price_collection': {
                        'status': 'completed',
                        'processed': 4,
                        'total': 4,
                        'succeeded': 4,
                        'failed': 0,
                    },
                    'technical_snapshots': {
                        'status': 'running',
                        'processed': 1,
                        'total': 4,
                        'succeeded': 1,
                        'failed': 0,
                        'current_step': 'computing_symbol_snapshot',
                        'current_activity': 'Computing daily technical snapshots for 000660',
                        'current_symbol': '000660',
                    },
                },
            },
        }
        failed_run = {
            **stale_run,
            'status': 'failed',
            'symbols_failed': 3,
            'error_message': 'Marked stale after 30 minutes without completion.',
            'run_metadata': {
                **stale_run['run_metadata'],
                'stage_progress': {
                    **stale_run['run_metadata']['stage_progress'],
                    'technical_snapshots': {
                        **stale_run['run_metadata']['stage_progress']['technical_snapshots'],
                        'status': 'failed',
                        'current_step': 'stale_run_failed',
                        'current_activity': 'Marked stale after 30 minutes without completion.',
                    },
                },
            },
        }

        with (
            patch(
                'app.eod.pykrx_backfill.load_latest_eod_collection_run',
                side_effect=[stale_run, failed_run],
            ),
            patch('app.eod.pykrx_backfill.count_eod_run_persisted_symbols', return_value=1),
            patch('app.eod.pykrx_backfill.load_eod_run_persisted_symbols', return_value=['000001']),
            patch('app.eod.pykrx_backfill.complete_eod_collection_run') as complete_run,
        ):
            result = service.reconcile_latest_daily_run_status(trade_date='2026-04-03')

        self.assertEqual(result['status'], 'failed')
        complete_run.assert_called_once()
        completed_metadata = complete_run.call_args.kwargs['run_metadata']
        self.assertEqual(
            completed_metadata['stage_progress']['technical_snapshots']['status'],
            'failed',
        )
        self.assertEqual(
            completed_metadata['stage_progress']['technical_snapshots']['current_activity'],
            'Marked stale after 30 minutes without completion.',
        )
        self.assertEqual(completed_metadata['stale_failure_reason'], 'Marked stale after 30 minutes without completion.')

    def test_run_market_data_pipeline_continues_when_progress_update_fails(self) -> None:
        service = PykrxBackfillService()

        with (
            patch('app.eod.pykrx_backfill.log_fetch_event') as log_fetch_event,
            patch.object(
                service,
                'load_active_universe',
                return_value=[PykrxSymbol(symbol='005930', market='KOSPI', name_ko='Samsung Electronics')],
            ),
            patch.object(service, 'resolve_latest_trade_date', return_value='2026-04-03'),
            patch.object(service, '_map_symbol_bars', return_value=[{
                'trade_date': '2026-04-03',
                'open_price': 1,
                'high_price': 2,
                'low_price': 1,
                'close_price': 2,
                'volume': 3,
                'turnover': 4,
            }]),
            patch('app.eod.pykrx_backfill.create_eod_collection_run', return_value=12),
            patch('app.eod.pykrx_backfill._get_market_ohlcv_by_date', return_value=type('Frame', (), {'empty': False})()),
            patch('app.eod.pykrx_backfill.persist_daily_price_bars', return_value=1),
            patch(
                'app.eod.pykrx_backfill.daily_price_context_service.rebuild_for_market_date',
                return_value={'contexts_persisted': 1, 'contexts_failed': 0},
            ) as price_contexts,
            patch(
                'app.eod.pykrx_backfill.daily_technical_snapshot_service.rebuild_for_market_date',
                return_value={'snapshots_persisted': 1, 'snapshots_failed': 0},
            ) as technical_snapshots,
            patch(
                'app.eod.pykrx_backfill.daily_action_summary_service.rebuild_for_market_date',
                return_value={'action_summaries_persisted': 1, 'action_summaries_failed': 0},
            ) as action_summaries,
            patch(
                'app.eod.pykrx_backfill.compute_and_persist_daily_trending_ranks',
                return_value={'trending_ranks_persisted': 3},
            ) as trending_ranks,
            patch(
                'app.eod.pykrx_backfill.daily_signal_feature_service.refresh_for_market_date',
                return_value={'features_persisted': 2, 'features_failed': 0},
            ) as signal_features,
            patch(
                'app.eod.pykrx_backfill.daily_market_risk_filter_service.rebuild_for_market_date',
                return_value={'results_persisted': 1, 'results_failed': 0, 'summary': {'passed_count': 1, 'watchlist_count': 0}},
            ) as market_risk_filter,
            patch('app.eod.pykrx_backfill.update_eod_collection_run_progress', side_effect=RuntimeError('progress write failed')),
            patch('app.eod.pykrx_backfill.complete_eod_collection_run') as complete_run,
        ):
            result = service.run_backfill(
                start_date='20260403',
                end_date='20260403',
                refresh_universe=False,
            )

        self.assertEqual(result['status'], 'completed')
        self.assertEqual(result['symbols_succeeded'], 1)
        self.assertEqual(result['price_contexts']['contexts_persisted'], 1)
        self.assertEqual(result['technical_snapshots']['snapshots_persisted'], 1)
        self.assertEqual(result['action_summaries']['action_summaries_persisted'], 1)
        self.assertEqual(result['trending_ranks']['trending_ranks_persisted'], 3)
        self.assertEqual(result['market_risk_filter']['results_persisted'], 1)
        price_contexts.assert_called_once_with(
            market_date='2026-04-03',
            symbols=['005930'],
            source='pykrx',
            run_id=12,
        )
        technical_snapshots.assert_called_once_with(
            market_date='2026-04-03',
            symbols=['005930'],
            source='pykrx',
            run_id=12,
            progress_callback=ANY,
        )
        action_summaries.assert_called_once_with(
            market_date='2026-04-03',
            symbols=['005930'],
            source='pykrx',
            run_id=12,
            progress_callback=ANY,
        )
        trending_ranks.assert_called_once_with(
            market_date='2026-04-03',
            source='pykrx',
            run_id=12,
            progress_callback=ANY,
        )
        signal_features.assert_called_once_with(market_date='2026-04-03')
        market_risk_filter.assert_called_once_with(
            market_date='2026-04-03',
            symbols=['005930'],
            source='kis_market_risk_v1',
            run_id=12,
        )
        self.assertTrue(
            any(
                call.kwargs.get('operation') == 'update_collection_progress'
                and call.kwargs.get('status') == 'failed'
                for call in log_fetch_event.call_args_list
            )
        )
        complete_run.assert_called_once_with(
            12,
            status='completed',
            symbols_succeeded=1,
            symbols_failed=0,
            error_message=None,
            run_metadata=ANY,
        )

    @patch('app.eod.pykrx_backfill.settings.openclaw_feedback_trigger_after_fundamentals_refresh', False)
    @patch(
        'app.eod.pykrx_pipeline_runner._run_daily_signal_decider_evaluation_followup',
        return_value={'triggered': True, 'summary': {'not_computed_count': 0}},
    )
    def test_run_collection_always_chains_fundamentals_for_daily_auto(self, evaluation_followup) -> None:
        service = PykrxBackfillService()

        with ExitStack() as stack:
            stack.enter_context(patch('app.eod.pykrx_backfill.settings.dart_api_key', 'test-dart-key'))
            stack.enter_context(
                patch.object(
                    service,
                    'load_active_universe',
                    return_value=[PykrxSymbol(symbol='005930', market='KOSPI', name_ko='Samsung Electronics')],
                )
            )
            stack.enter_context(patch.object(service, 'resolve_latest_trade_date', return_value='2026-04-03'))
            stack.enter_context(patch.object(service, '_load_symbol_fundamentals', return_value=None))
            stack.enter_context(
                patch.object(
                    service,
                    '_map_symbol_bars',
                    return_value=[{
                        'trade_date': '2026-04-03',
                        'open_price': 1,
                        'high_price': 2,
                        'low_price': 1,
                        'close_price': 2,
                        'volume': 3,
                        'turnover': 4,
                    }],
                )
            )
            stack.enter_context(patch('app.eod.pykrx_backfill.create_eod_collection_run', return_value=12))
            stack.enter_context(
                patch('app.eod.pykrx_backfill._get_market_ohlcv_by_date', return_value=SimpleNamespace(empty=False))
            )
            stack.enter_context(patch('app.eod.pykrx_backfill.persist_daily_price_bars', return_value=1))
            stack.enter_context(patch('app.eod.pykrx_backfill.update_eod_collection_run_progress'))
            stack.enter_context(
                patch(
                    'app.eod.pykrx_backfill.pykrx_enrichment_service.collect_for_market_date',
                    return_value=_ready_pykrx_enrichment_result(),
                )
            )
            stack.enter_context(
                patch(
                    'app.eod.pykrx_backfill.daily_price_context_service.rebuild_for_market_date',
                    return_value={'contexts_persisted': 1, 'contexts_failed': 0},
                )
            )
            stack.enter_context(
                patch(
                    'app.eod.pykrx_backfill.daily_technical_snapshot_service.rebuild_for_market_date',
                    return_value={'snapshots_persisted': 1, 'snapshots_failed': 0},
                )
            )
            stack.enter_context(
                patch(
                    'app.eod.pykrx_backfill.daily_action_summary_service.rebuild_for_market_date',
                    return_value={'action_summaries_persisted': 1, 'action_summaries_failed': 0},
                )
            )
            stack.enter_context(
                patch(
                    'app.eod.pykrx_backfill.compute_and_persist_daily_trending_ranks',
                    return_value={'trending_ranks_persisted': 3},
                )
            )
            stack.enter_context(
                patch(
                    'app.eod.pykrx_backfill.daily_signal_feature_service.refresh_for_market_date',
                    return_value={'features_persisted': 0, 'features_failed': 0, 'sources': [], 'source_counts': {}},
                )
            )
            stack.enter_context(
                patch(
                    'app.eod.pykrx_backfill.daily_market_risk_filter_service.rebuild_for_market_date',
                    return_value={'results_persisted': 1, 'results_failed': 0, 'summary': {'passed_count': 1, 'watchlist_count': 0}},
                )
            )
            fundamentals_refresh = stack.enter_context(
                patch(
                    'app.eod.pykrx_backfill.dart_fundamentals_capture_service.run_daily_incremental_refresh',
                    return_value={'status': 'completed', 'trigger': 'eod_pykrx_followup'},
                )
            )
            factor_followup = stack.enter_context(
                patch(
                    'app.eod.pykrx_pipeline_runner._run_daily_factor_followup',
                    return_value={'triggered': True, 'status': 'completed', 'stages': {}},
                )
            )
            external_news_followup = stack.enter_context(
                patch(
                    'app.eod.pykrx_pipeline_runner._run_daily_external_news_followup',
                    return_value={'triggered': True, 'status': 'disabled', 'reason': 'naver_search_credentials_missing'},
                )
            )
            feedback_followup = stack.enter_context(
                patch(
                    'app.eod.pykrx_pipeline_runner._run_daily_feedback_followup',
                    return_value={'triggered': True, 'status': 'completed'},
                )
            )
            evaluation_followup = stack.enter_context(
                patch(
                    'app.eod.pykrx_pipeline_runner._run_daily_signal_decider_evaluation_followup',
                    return_value={'triggered': True, 'status': 'completed', 'summary': {'computed_count': 4}},
                )
            )
            stack.enter_context(patch('app.eod.pykrx_backfill.complete_eod_collection_run'))
            result = service._run_market_data_pipeline(
                start_date='20260403',
                end_date='20260403',
                markets=('KOSPI',),
                trigger='manual_daily_api',
                refresh_universe=False,
                mode='daily_auto',
            )

        self.assertEqual(result['status'], 'completed')
        self.assertEqual(
            result['fundamentals_followup'],
            {'status': 'completed', 'trigger': 'eod_pykrx_followup'},
        )
        fundamentals_refresh.assert_called_once_with(
            market_date='2026-04-03',
            capture_limit=500,
            trigger='eod_pykrx_followup',
            run_feedback_followup=False,
            progress_callback=ANY,
        )
        factor_followup.assert_called_once_with(market_date='2026-04-03')
        external_news_followup.assert_called_once_with(market_date='2026-04-03', enabled=True)
        feedback_followup.assert_called_once_with(market_date='2026-04-03', enabled=False)
        evaluation_followup.assert_called_once_with(market_date='2026-04-03', enabled=True)
        self.assertEqual(
            result['factor_scoring_followup'],
            {'triggered': True, 'status': 'completed', 'stages': {}},
        )
        self.assertEqual(
            result['external_news_followup'],
            {'triggered': True, 'status': 'disabled', 'reason': 'naver_search_credentials_missing'},
        )
        self.assertEqual(
            result['feedback_followup'],
            {'triggered': True, 'status': 'completed'},
        )
        self.assertEqual(
            result['signal_decider_evaluation_followup'],
            {'triggered': True, 'status': 'completed', 'summary': {'computed_count': 4}},
        )

    @patch('app.eod.pykrx_backfill.settings.openclaw_feedback_trigger_after_fundamentals_refresh', False)
    def test_run_market_data_pipeline_tracks_auto_followups_in_daily_pipeline_state(self) -> None:
        service = PykrxBackfillService()
        fake_run_state_service = _FakeDailyPipelineRunStateService()

        with ExitStack() as stack:
            stack.enter_context(patch('app.eod.pykrx_backfill.settings.dart_api_key', 'test-dart-key'))
            stack.enter_context(
                patch('app.eod.pykrx_backfill.daily_pipeline_run_state_service', fake_run_state_service)
            )
            stack.enter_context(
                patch.object(
                    service,
                    'load_active_universe',
                    return_value=[PykrxSymbol(symbol='005930', market='KOSPI', name_ko='Samsung Electronics')],
                )
            )
            stack.enter_context(patch.object(service, 'resolve_latest_trade_date', return_value='2026-04-03'))
            stack.enter_context(patch.object(service, '_load_symbol_fundamentals', return_value=None))
            stack.enter_context(
                patch.object(
                    service,
                    '_map_symbol_bars',
                    return_value=[{
                        'trade_date': '2026-04-03',
                        'open_price': 1,
                        'high_price': 2,
                        'low_price': 1,
                        'close_price': 2,
                        'volume': 3,
                        'turnover': 4,
                    }],
                )
            )
            stack.enter_context(patch('app.eod.pykrx_backfill.create_eod_collection_run', return_value=12))
            stack.enter_context(
                patch('app.eod.pykrx_backfill._get_market_ohlcv_by_date', return_value=SimpleNamespace(empty=False))
            )
            stack.enter_context(patch('app.eod.pykrx_backfill.persist_daily_price_bars', return_value=1))
            stack.enter_context(patch('app.eod.pykrx_backfill.update_eod_collection_run_progress'))
            stack.enter_context(
                patch(
                    'app.eod.pykrx_backfill.pykrx_enrichment_service.collect_for_market_date',
                    return_value=_ready_pykrx_enrichment_result(),
                )
            )
            stack.enter_context(
                patch(
                    'app.eod.pykrx_backfill.daily_price_context_service.rebuild_for_market_date',
                    return_value={'contexts_persisted': 1, 'contexts_failed': 0},
                )
            )
            stack.enter_context(
                patch(
                    'app.eod.pykrx_backfill.daily_technical_snapshot_service.rebuild_for_market_date',
                    return_value={'snapshots_persisted': 1, 'snapshots_failed': 0},
                )
            )
            stack.enter_context(
                patch(
                    'app.eod.pykrx_backfill.daily_action_summary_service.rebuild_for_market_date',
                    return_value={'action_summaries_persisted': 1, 'action_summaries_failed': 0},
                )
            )
            stack.enter_context(
                patch(
                    'app.eod.pykrx_backfill.compute_and_persist_daily_trending_ranks',
                    return_value={'trending_ranks_persisted': 3},
                )
            )
            stack.enter_context(
                patch(
                    'app.eod.pykrx_backfill.daily_signal_feature_service.refresh_for_market_date',
                    return_value={'features_persisted': 2, 'features_failed': 0, 'sources': [], 'source_counts': {}},
                )
            )
            stack.enter_context(
                patch(
                    'app.eod.pykrx_backfill.daily_market_risk_filter_service.rebuild_for_market_date',
                    return_value={'results_persisted': 1, 'results_failed': 0, 'summary': {'passed_count': 1, 'watchlist_count': 0}},
                )
            )
            fundamentals_refresh = stack.enter_context(
                patch(
                    'app.eod.pykrx_backfill.dart_fundamentals_capture_service.run_daily_incremental_refresh',
                    return_value={'status': 'completed', 'trigger': 'eod_pykrx_followup'},
                )
            )
            stack.enter_context(
                patch(
                    'app.eod.pykrx_pipeline_runner._run_daily_factor_followup',
                    return_value={'triggered': True, 'status': 'completed', 'stages': {}},
                )
            )
            stack.enter_context(
                patch(
                    'app.eod.pykrx_pipeline_runner._run_daily_external_news_followup',
                    return_value={'triggered': True, 'status': 'completed', 'items_persisted': 3},
                )
            )
            stack.enter_context(
                patch(
                    'app.eod.pykrx_pipeline_runner._run_daily_feedback_followup',
                    return_value={'triggered': False, 'reason': 'followup_disabled', 'market_date': '2026-04-03'},
                )
            )
            stack.enter_context(
                patch(
                    'app.eod.pykrx_pipeline_runner._run_daily_signal_decider_evaluation_followup',
                    return_value={'triggered': True, 'status': 'completed', 'summary': {'computed_count': 4}},
                )
            )
            stack.enter_context(patch('app.eod.pykrx_backfill.complete_eod_collection_run'))

            result = service._run_market_data_pipeline(
                start_date='20260403',
                end_date='20260403',
                markets=('KOSPI',),
                trigger='auto_close_scheduler',
                refresh_universe=False,
                mode='daily_auto',
                selected_stages=PYKRX_STAGE_SEQUENCE,
                track_daily_pipeline=True,
            )

        self.assertEqual(result['status'], 'completed')
        self.assertEqual(fake_run_state_service.start_runs[0]['market_date'], '2026-04-03')
        self.assertIn('fundamentals_daily_refresh', fake_run_state_service.start_runs[0]['resolved_stages'])
        self.assertIn('momentum_factors', fake_run_state_service.start_runs[0]['resolved_stages'])
        self.assertIn('signal_decider_evaluations', fake_run_state_service.start_runs[0]['resolved_stages'])
        stage_statuses = {
            (item['stage_id'], item['status'])
            for item in fake_run_state_service.stage_updates
        }
        self.assertIn(('fundamentals_daily_refresh', 'running'), stage_statuses)
        self.assertIn(('fundamentals_daily_refresh', 'completed'), stage_statuses)
        self.assertIn(('momentum_factors', 'running'), stage_statuses)
        self.assertIn(('signal_decider_evaluations', 'running'), stage_statuses)
        self.assertEqual(fake_run_state_service.finish_runs[-1]['status'], 'completed')
        self.assertEqual(
            fake_run_state_service.finish_runs[-1]['run_metadata']['eod_collection_run_id'],
            12,
        )
        fundamentals_refresh.assert_called_once()
        self.assertIn('progress_callback', fundamentals_refresh.call_args.kwargs)

    def test_run_collection_daily_auto_rebuilds_price_contexts_when_selected_stages_are_explicit(self) -> None:
        service = PykrxBackfillService()

        with (
            patch.object(
                service,
                'load_active_universe',
                return_value=[PykrxSymbol(symbol='005930', market='KOSPI', name_ko='Samsung Electronics')],
            ),
            patch.object(service, 'resolve_latest_trade_date', return_value='2026-04-03'),
            patch.object(service, '_load_symbol_fundamentals', return_value=None),
            patch.object(service, '_map_symbol_bars', return_value=[{
                'trade_date': '2026-04-03',
                'open_price': 1,
                'high_price': 2,
                'low_price': 1,
                'close_price': 2,
                'volume': 3,
                'turnover': 4,
            }]),
            patch('app.eod.pykrx_backfill.create_eod_collection_run', return_value=12),
            patch('app.eod.pykrx_backfill._get_market_ohlcv_by_date', return_value=SimpleNamespace(empty=False)),
            patch('app.eod.pykrx_backfill.persist_daily_price_bars', return_value=1),
            patch('app.eod.pykrx_backfill.update_eod_collection_run_progress'),
            patch(
                'app.eod.pykrx_backfill.daily_price_context_service.rebuild_for_market_date',
                return_value={'contexts_persisted': 1, 'contexts_failed': 0},
            ) as price_contexts,
            patch(
                'app.eod.pykrx_backfill.pykrx_enrichment_service.collect_for_market_date',
                return_value=_ready_pykrx_enrichment_result(),
            ) as pykrx_enrichment,
            patch(
                'app.eod.pykrx_backfill.daily_technical_snapshot_service.rebuild_for_market_date',
                return_value={'snapshots_persisted': 1, 'snapshots_failed': 0},
            ),
            patch(
                'app.eod.pykrx_backfill.daily_action_summary_service.rebuild_for_market_date',
                return_value={'action_summaries_persisted': 1, 'action_summaries_failed': 0},
            ),
            patch(
                'app.eod.pykrx_backfill.compute_and_persist_daily_trending_ranks',
                return_value={'trending_ranks_persisted': 3},
            ),
            patch(
                'app.eod.pykrx_backfill.daily_signal_feature_service.refresh_for_market_date',
                return_value={'features_persisted': 0, 'features_failed': 0, 'sources': [], 'source_counts': {}},
            ),
            patch(
                'app.eod.pykrx_backfill.daily_market_risk_filter_service.rebuild_for_market_date',
                return_value={
                    'results_persisted': 1,
                    'results_failed': 0,
                    'summary': {'passed_count': 1, 'watchlist_count': 0},
                },
            ),
            patch('app.eod.pykrx_backfill.complete_eod_collection_run'),
        ):
            result = service._run_market_data_pipeline(
                start_date='20260403',
                end_date='20260403',
                markets=('KOSPI',),
                trigger='auto_close_scheduler',
                refresh_universe=False,
                mode='daily_auto',
                selected_stages=PYKRX_STAGE_SEQUENCE,
                chain_fundamentals_followup=False,
            )

        self.assertEqual(result['status'], 'completed')
        self.assertEqual(result['pykrx_enrichment']['status'], 'completed')
        self.assertTrue(result['pykrx_enrichment']['downstream_preflight']['allowed'])
        self.assertEqual(
            result['pykrx_enrichment']['downstream_preflight']['category_row_counts'],
            {
                'sector_classification': 1,
                'investor_flow_symbol': 1,
                'short_selling': 1,
            },
        )
        self.assertEqual(result['price_contexts']['contexts_persisted'], 1)
        pykrx_enrichment.assert_called_once_with(
            market_date='2026-04-03',
            markets=('KOSPI',),
            symbols=['005930'],
        )
        price_contexts.assert_called_once_with(
            market_date='2026-04-03',
            symbols=['005930'],
            source='pykrx',
            run_id=12,
        )

    @patch(
        'app.eod.pykrx_pipeline_runner._run_daily_signal_decider_evaluation_followup',
        return_value={'triggered': True, 'summary': {'not_computed_count': 0}},
    )
    def test_run_collection_skips_fundamentals_followup_when_dart_is_not_configured(self, evaluation_followup) -> None:
        service = PykrxBackfillService()

        with ExitStack() as stack:
            stack.enter_context(patch('app.eod.pykrx_backfill.settings.dart_api_key', ''))
            stack.enter_context(
                patch.object(
                    service,
                    'load_active_universe',
                    return_value=[PykrxSymbol(symbol='005930', market='KOSPI', name_ko='Samsung Electronics')],
                )
            )
            stack.enter_context(patch.object(service, 'resolve_latest_trade_date', return_value='2026-04-03'))
            stack.enter_context(patch.object(service, '_load_symbol_fundamentals', return_value=None))
            stack.enter_context(
                patch.object(
                    service,
                    '_map_symbol_bars',
                    return_value=[{
                        'trade_date': '2026-04-03',
                        'open_price': 1,
                        'high_price': 2,
                        'low_price': 1,
                        'close_price': 2,
                        'volume': 3,
                        'turnover': 4,
                    }],
                )
            )
            stack.enter_context(patch('app.eod.pykrx_backfill.create_eod_collection_run', return_value=12))
            stack.enter_context(
                patch('app.eod.pykrx_backfill._get_market_ohlcv_by_date', return_value=SimpleNamespace(empty=False))
            )
            stack.enter_context(patch('app.eod.pykrx_backfill.persist_daily_price_bars', return_value=1))
            stack.enter_context(patch('app.eod.pykrx_backfill.update_eod_collection_run_progress'))
            stack.enter_context(
                patch(
                    'app.eod.pykrx_backfill.daily_price_context_service.rebuild_for_market_date',
                    return_value={'contexts_persisted': 1, 'contexts_failed': 0},
                )
            )
            stack.enter_context(
                patch(
                    'app.eod.pykrx_backfill.daily_technical_snapshot_service.rebuild_for_market_date',
                    return_value={'snapshots_persisted': 1, 'snapshots_failed': 0},
                )
            )
            stack.enter_context(
                patch(
                    'app.eod.pykrx_backfill.daily_action_summary_service.rebuild_for_market_date',
                    return_value={'action_summaries_persisted': 1, 'action_summaries_failed': 0},
                )
            )
            stack.enter_context(
                patch(
                    'app.eod.pykrx_backfill.compute_and_persist_daily_trending_ranks',
                    return_value={'trending_ranks_persisted': 3},
                )
            )
            stack.enter_context(
                patch(
                    'app.eod.pykrx_backfill.daily_signal_feature_service.refresh_for_market_date',
                    return_value={'features_persisted': 0, 'features_failed': 0, 'sources': [], 'source_counts': {}},
                )
            )
            stack.enter_context(
                patch(
                    'app.eod.pykrx_backfill.daily_market_risk_filter_service.rebuild_for_market_date',
                    return_value={'results_persisted': 1, 'results_failed': 0, 'summary': {'passed_count': 1, 'watchlist_count': 0}},
                )
            )
            fundamentals_refresh = stack.enter_context(
                patch('app.eod.pykrx_backfill.dart_fundamentals_capture_service.run_daily_incremental_refresh')
            )
            evaluation_followup = stack.enter_context(
                patch(
                    'app.eod.pykrx_pipeline_runner._run_daily_signal_decider_evaluation_followup',
                    return_value={'triggered': True, 'status': 'warning', 'summary': {'not_computed_count': 1}},
                )
            )
            stack.enter_context(patch('app.eod.pykrx_backfill.complete_eod_collection_run'))
            result = service._run_market_data_pipeline(
                start_date='20260403',
                end_date='20260403',
                markets=('KOSPI',),
                trigger='manual_daily_api',
                refresh_universe=False,
                mode='daily_auto',
            )

        self.assertEqual(result['status'], 'completed')
        self.assertEqual(
            result['fundamentals_followup'],
            {
                'triggered': False,
                'reason': 'dart_api_key_not_configured',
                'market_date': '2026-04-03',
            },
        )
        self.assertEqual(
            result['factor_scoring_followup'],
            {
                'triggered': False,
                'reason': 'dart_api_key_not_configured',
                'market_date': '2026-04-03',
            },
        )
        self.assertIn('external_news_followup', result)
        evaluation_followup.assert_called_once_with(market_date='2026-04-03', enabled=True)
        self.assertEqual(
            result['signal_decider_evaluation_followup'],
            {'triggered': True, 'status': 'warning', 'summary': {'not_computed_count': 1}},
        )
        fundamentals_refresh.assert_not_called()

    def test_run_market_data_pipeline_keeps_retryable_status_when_snapshot_persistence_has_failures(self) -> None:
        service = PykrxBackfillService()

        with (
            patch.object(
                service,
                'load_active_universe',
                return_value=[PykrxSymbol(symbol='005930', market='KOSPI', name_ko='Samsung Electronics')],
            ),
            patch.object(service, 'resolve_latest_trade_date', return_value='2026-04-03'),
            patch.object(service, '_map_symbol_bars', return_value=[{
                'trade_date': '2026-04-03',
                'open_price': 1,
                'high_price': 2,
                'low_price': 1,
                'close_price': 2,
                'volume': 3,
                'turnover': 4,
            }]),
            patch('app.eod.pykrx_backfill.create_eod_collection_run', return_value=12),
            patch('app.eod.pykrx_backfill._get_market_ohlcv_by_date', return_value=type('Frame', (), {'empty': False})()),
            patch('app.eod.pykrx_backfill.persist_daily_price_bars', return_value=1),
            patch('app.eod.pykrx_backfill.update_eod_collection_run_progress'),
            patch(
                'app.eod.pykrx_backfill.daily_price_context_service.rebuild_for_market_date',
                return_value={
                    'contexts_persisted': 1,
                    'contexts_failed': 0,
                    'failed_symbols': [],
                },
            ),
            patch(
                'app.eod.pykrx_backfill.daily_technical_snapshot_service.rebuild_for_market_date',
                return_value={
                    'snapshots_persisted': 0,
                    'snapshots_failed': 1,
                    'failed_symbols': ['005930'],
                    'items': [],
                },
            ),
            patch(
                'app.eod.pykrx_backfill.daily_action_summary_service.rebuild_for_market_date',
                return_value={
                    'action_summaries_persisted': 0,
                    'action_summaries_failed': 1,
                    'failed_symbols': ['005930'],
                    'items': [],
                },
            ),
            patch(
                'app.eod.pykrx_backfill.compute_and_persist_daily_trending_ranks',
                return_value={'trending_ranks_persisted': 0},
            ),
            patch(
                'app.eod.pykrx_backfill.daily_signal_feature_service.refresh_for_market_date',
                return_value={'features_persisted': 0, 'features_failed': 0, 'failed_symbols': []},
            ),
            patch(
                'app.eod.pykrx_backfill.daily_market_risk_filter_service.rebuild_for_market_date',
                return_value={'results_persisted': 1, 'results_failed': 0, 'summary': {'passed_count': 1, 'watchlist_count': 0}},
            ),
            patch('app.eod.pykrx_backfill.complete_eod_collection_run') as complete_run,
        ):
            result = service.run_backfill(
                start_date='20260403',
                end_date='20260403',
                refresh_universe=False,
            )

        self.assertEqual(result['status'], 'completed_with_errors')
        complete_run.assert_called_once_with(
            12,
            status='completed_with_errors',
            symbols_succeeded=1,
            symbols_failed=0,
            error_message='Technical snapshot persistence incomplete for symbols: 005930 | Action summary persistence incomplete for symbols: 005930',
            run_metadata=ANY,
        )

    def test_run_market_data_pipeline_marks_partial_when_market_risk_filter_has_symbol_failures(self) -> None:
        service = PykrxBackfillService()

        with (
            patch.object(
                service,
                'load_active_universe',
                return_value=[PykrxSymbol(symbol='005930', market='KOSPI', name_ko='Samsung Electronics')],
            ),
            patch.object(service, 'resolve_latest_trade_date', return_value='2026-04-03'),
            patch.object(service, '_map_symbol_bars', return_value=[{
                'trade_date': '2026-04-03',
                'open_price': 1,
                'high_price': 2,
                'low_price': 1,
                'close_price': 2,
                'volume': 3,
                'turnover': 4,
            }]),
            patch('app.eod.pykrx_backfill.create_eod_collection_run', return_value=12),
            patch('app.eod.pykrx_backfill._get_market_ohlcv_by_date', return_value=type('Frame', (), {'empty': False})()),
            patch('app.eod.pykrx_backfill.persist_daily_price_bars', return_value=1),
            patch('app.eod.pykrx_backfill.update_eod_collection_run_progress'),
            patch(
                'app.eod.pykrx_backfill.daily_price_context_service.rebuild_for_market_date',
                return_value={'contexts_persisted': 1, 'contexts_failed': 0, 'failed_symbols': []},
            ),
            patch(
                'app.eod.pykrx_backfill.daily_technical_snapshot_service.rebuild_for_market_date',
                return_value={'snapshots_persisted': 1, 'snapshots_failed': 0, 'failed_symbols': []},
            ),
            patch(
                'app.eod.pykrx_backfill.daily_action_summary_service.rebuild_for_market_date',
                return_value={'action_summaries_persisted': 1, 'action_summaries_failed': 0, 'failed_symbols': []},
            ),
            patch(
                'app.eod.pykrx_backfill.compute_and_persist_daily_trending_ranks',
                return_value={'trending_ranks_persisted': 3},
            ),
            patch(
                'app.eod.pykrx_backfill.daily_signal_feature_service.refresh_for_market_date',
                return_value={'features_persisted': 0, 'features_failed': 0, 'failed_symbols': []},
            ),
            patch(
                'app.eod.pykrx_backfill.daily_market_risk_filter_service.rebuild_for_market_date',
                return_value={
                    'results_persisted': 0,
                    'results_failed': 1,
                    'failed_symbols': ['005930'],
                    'summary': {'passed_count': 0, 'watchlist_count': 0},
                },
            ),
            patch('app.eod.pykrx_backfill.complete_eod_collection_run') as complete_run,
        ):
            result = service.run_backfill(
                start_date='20260403',
                end_date='20260403',
                refresh_universe=False,
            )

        self.assertEqual(result['status'], 'completed_with_errors')
        self.assertEqual(result['market_risk_filter']['results_failed'], 1)
        complete_run.assert_called_once_with(
            12,
            status='completed_with_errors',
            symbols_succeeded=1,
            symbols_failed=0,
            error_message='Market risk filter persistence incomplete for symbols: 005930',
            run_metadata=ANY,
        )

    def test_run_market_data_pipeline_preserves_price_collection_counts_in_result(self) -> None:
        service = PykrxBackfillService()

        with (
            patch.object(
                service,
                'load_active_universe',
                return_value=[PykrxSymbol(symbol='005930', market='KOSPI', name_ko='Samsung Electronics')],
            ),
            patch.object(service, 'resolve_latest_trade_date', return_value='2026-04-03'),
            patch.object(service, '_load_symbol_fundamentals', return_value=None),
            patch.object(
                service,
                '_map_symbol_bars',
                return_value=[
                    {
                        'trade_date': '2026-04-02',
                        'open_price': 1,
                        'high_price': 2,
                        'low_price': 1,
                        'close_price': 2,
                        'volume': 3,
                        'turnover': 4,
                    },
                    {
                        'trade_date': '2026-04-03',
                        'open_price': 2,
                        'high_price': 3,
                        'low_price': 2,
                        'close_price': 3,
                        'volume': 5,
                        'turnover': 6,
                    },
                ],
            ),
            patch('app.eod.pykrx_backfill.create_eod_collection_run', return_value=12),
            patch('app.eod.pykrx_backfill._get_market_ohlcv_by_date', return_value=SimpleNamespace(empty=False)),
            patch('app.eod.pykrx_backfill.persist_daily_price_bars', return_value=2),
            patch('app.eod.pykrx_backfill.update_eod_collection_run_progress'),
            patch(
                'app.eod.pykrx_backfill.daily_price_context_service.rebuild_for_market_date',
                return_value={'contexts_persisted': 1, 'contexts_failed': 0},
            ),
            patch(
                'app.eod.pykrx_backfill.daily_technical_snapshot_service.rebuild_for_market_date',
                return_value={'snapshots_persisted': 1, 'snapshots_failed': 0},
            ),
            patch(
                'app.eod.pykrx_backfill.daily_action_summary_service.rebuild_for_market_date',
                return_value={'action_summaries_persisted': 1, 'action_summaries_failed': 0},
            ),
            patch(
                'app.eod.pykrx_backfill.compute_and_persist_daily_trending_ranks',
                return_value={'trending_ranks_persisted': 3},
            ),
            patch(
                'app.eod.pykrx_backfill.daily_signal_feature_service.refresh_for_market_date',
                return_value={'features_persisted': 0, 'features_failed': 0, 'sources': [], 'source_counts': {}},
            ),
            patch(
                'app.eod.pykrx_backfill.daily_market_risk_filter_service.rebuild_for_market_date',
                return_value={'results_persisted': 1, 'results_failed': 0, 'summary': {'passed_count': 1, 'watchlist_count': 0}},
            ),
            patch('app.eod.pykrx_backfill.complete_eod_collection_run') as complete_run,
        ):
            result = service.run_backfill(
                start_date='20260403',
                end_date='20260403',
                refresh_universe=False,
            )

        self.assertEqual(result['status'], 'completed')
        self.assertEqual(result['symbols_total'], 1)
        self.assertEqual(result['symbols_succeeded'], 1)
        self.assertEqual(result['symbols_failed'], 0)
        self.assertEqual(result['bars_upserted'], 2)
        complete_run.assert_called_once_with(
            12,
            status='completed',
            symbols_succeeded=1,
            symbols_failed=0,
            error_message=None,
            run_metadata=ANY,
        )

    def test_run_daily_collection_skips_completed_with_errors_without_failed_symbols(self) -> None:
        service = PykrxBackfillService()

        with (
            patch.object(service, 'resolve_latest_trade_date', return_value='2026-04-03'),
            patch('app.eod.pykrx_backfill.advisory_eod_collection_lock', return_value=_lock()),
            patch(
                'app.eod.pykrx_backfill.load_latest_eod_collection_run',
                return_value={
                    'id': 7,
                    'status': 'completed_with_errors',
                    'symbols_failed': 0,
                },
            ),
            patch.object(
                service,
                '_run_market_data_pipeline',
                return_value={
                    'run_id': 8,
                    'source': 'pykrx',
                    'status': 'completed',
                    'technical_snapshots': {'snapshots_failed': 0},
                },
            ) as run_collection,
        ):
            result = service.run_daily_pipeline_if_due(
                market_date='2026-04-03',
                trigger='auto_close_scheduler',
            )

        self.assertFalse(result['triggered'])
        self.assertEqual(result['reason'], 'already_completed_with_errors')
        run_collection.assert_not_called()

    def test_auto_daily_retry_uses_previous_failed_symbols_only(self) -> None:
        service = PykrxBackfillService()
        retry_symbols = [
            PykrxSymbol(symbol='000660', market='KOSPI', name_ko='SK hynix'),
            PykrxSymbol(symbol='005930', market='KOSPI', name_ko='Samsung Electronics'),
        ]

        with (
            patch.object(service, 'resolve_latest_trade_date', return_value='2026-04-03'),
            patch('app.eod.pykrx_backfill.advisory_eod_collection_lock', return_value=_lock()),
            patch(
                'app.eod.pykrx_backfill.load_latest_eod_collection_run',
                return_value={
                    'id': 7,
                    'status': 'completed_with_errors',
                    'symbols_failed': 2,
                    'run_metadata': {'failed_symbols': ['000660', '005930']},
                },
            ),
            patch('app.eod.pykrx_backfill.count_eod_collection_runs', return_value=2),
            patch(
                'app.eod.pykrx_backfill.load_symbols',
                return_value=[
                    {
                        'symbol': '000660',
                        'market': 'KOSPI',
                        'name_ko': 'SK hynix',
                        'standard_code': None,
                    },
                    {
                        'symbol': '005930',
                        'market': 'KOSPI',
                        'name_ko': 'Samsung Electronics',
                        'standard_code': None,
                    },
                ],
            ),
            patch.object(
                service,
                '_run_market_data_pipeline',
                return_value={
                    'run_id': 8,
                    'source': 'pykrx',
                    'status': 'completed',
                },
            ) as run_collection,
        ):
            result = service.run_daily_pipeline_if_due(
                market_date='2026-04-03',
                trigger='auto_close_scheduler',
            )

        self.assertTrue(result['triggered'])
        run_collection.assert_called_once()
        kwargs = run_collection.call_args.kwargs
        self.assertEqual(kwargs['symbols_override'], retry_symbols)
        self.assertFalse(kwargs['refresh_universe'])
        self.assertEqual(
            kwargs['retry_context'],
            {
                'retry_of_run_id': 7,
                'previous_status': 'completed_with_errors',
                'previous_symbols_failed': 2,
                'attempt': 3,
                'max_attempts': 5,
                'symbol_count': 2,
            },
        )

    def test_auto_daily_retry_stops_after_five_auto_runs(self) -> None:
        service = PykrxBackfillService()

        with (
            patch.object(service, 'resolve_latest_trade_date', return_value='2026-04-03'),
            patch('app.eod.pykrx_backfill.advisory_eod_collection_lock', return_value=_lock()),
            patch(
                'app.eod.pykrx_backfill.load_latest_eod_collection_run',
                return_value={
                    'id': 7,
                    'status': 'failed',
                    'symbols_failed': 4,
                    'run_metadata': {'failed_symbols': ['000660', '005930', '035420', '051910']},
                },
            ),
            patch('app.eod.pykrx_backfill.count_eod_collection_runs', return_value=5),
            patch.object(service, '_run_market_data_pipeline') as run_collection,
        ):
            result = service.run_daily_pipeline_if_due(
                market_date='2026-04-03',
                trigger='auto_close_scheduler',
            )

        self.assertFalse(result['triggered'])
        self.assertEqual(result['reason'], 'auto_retry_limit_reached')
        self.assertEqual(result['auto_retry']['attempts'], 5)
        self.assertEqual(result['auto_retry']['max_attempts'], 5)
        run_collection.assert_not_called()

    def test_manual_daily_retry_keeps_full_collection_behavior(self) -> None:
        service = PykrxBackfillService()

        with (
            patch.object(service, 'resolve_latest_trade_date', return_value='2026-04-03'),
            patch('app.eod.pykrx_backfill.advisory_eod_collection_lock', return_value=_lock()),
            patch(
                'app.eod.pykrx_backfill.load_latest_eod_collection_run',
                return_value={
                    'id': 7,
                    'status': 'failed',
                    'symbols_failed': 2,
                    'run_metadata': {'failed_symbols': ['000660', '005930']},
                },
            ),
            patch('app.eod.pykrx_backfill.count_eod_collection_runs') as count_runs,
            patch.object(
                service,
                '_run_market_data_pipeline',
                return_value={
                    'run_id': 8,
                    'source': 'pykrx',
                    'status': 'completed',
                },
            ) as run_collection,
        ):
            result = service.run_daily_pipeline_if_due(
                market_date='2026-04-03',
                trigger='manual_daily_api',
            )

        self.assertTrue(result['triggered'])
        count_runs.assert_not_called()
        kwargs = run_collection.call_args.kwargs
        self.assertIsNone(kwargs['symbols_override'])
        self.assertIsNone(kwargs['retry_context'])
        self.assertTrue(kwargs['refresh_universe'])

    def test_run_market_data_pipeline_logs_per_symbol_failures(self) -> None:
        service = PykrxBackfillService()

        with (
            patch('app.eod.pykrx_backfill.log_fetch_event') as log_fetch_event,
            patch.object(
                service,
                'load_active_universe',
                return_value=[PykrxSymbol(symbol='005930', market='KOSPI', name_ko='Samsung Electronics')],
            ),
            patch.object(service, 'resolve_latest_trade_date', return_value='2026-04-03'),
            patch('app.eod.pykrx_backfill.create_eod_collection_run', return_value=12),
            patch(
                'app.eod.pykrx_backfill._get_market_ohlcv_by_date',
                side_effect=RuntimeError('pykrx timeout'),
            ),
            patch('app.eod.pykrx_backfill.update_eod_collection_run_progress'),
            patch(
                'app.eod.pykrx_backfill.daily_price_context_service.rebuild_for_market_date',
                return_value={
                    'contexts_persisted': 0,
                    'contexts_failed': 0,
                    'failed_symbols': [],
                },
            ) as price_contexts,
            patch(
                'app.eod.pykrx_backfill.daily_technical_snapshot_service.rebuild_for_market_date',
                return_value={'snapshots_persisted': 0, 'snapshots_failed': 1},
            ) as technical_snapshots,
            patch(
                'app.eod.pykrx_backfill.daily_action_summary_service.rebuild_for_market_date',
                return_value={'action_summaries_persisted': 0, 'action_summaries_failed': 1},
            ) as action_summaries,
            patch(
                'app.eod.pykrx_backfill.compute_and_persist_daily_trending_ranks',
                return_value={'trending_ranks_persisted': 0},
            ),
            patch('app.eod.pykrx_backfill.complete_eod_collection_run'),
        ):
            result = service.run_backfill(
                start_date='20260403',
                end_date='20260403',
                refresh_universe=False,
            )

        self.assertEqual(result['status'], 'completed_with_errors')
        self.assertEqual(result['failed_symbols'], ['005930'])
        price_contexts.assert_called_once_with(
            market_date='2026-04-03',
            symbols=[],
            source='pykrx',
            run_id=12,
        )
        technical_snapshots.assert_called_once_with(
            market_date='2026-04-03',
            symbols=['005930'],
            source='pykrx',
            run_id=12,
            progress_callback=ANY,
        )
        self.assertTrue(
            any(
                call.kwargs.get('operation') == 'fetch_symbol_ohlcv'
                and call.kwargs.get('status') == 'failed'
                and call.kwargs.get('symbol') == '005930'
                for call in log_fetch_event.call_args_list
            )
        )


class AutoPykrxEODSchedulerTests(IsolatedAsyncioTestCase):
    async def test_schedule_daily_pipeline_if_due_waits_for_close_buffer(self) -> None:
        scheduler = AutoPykrxEODScheduler()
        now = datetime.now(KST)
        close_at = (now + timedelta(minutes=1)).replace(second=0, microsecond=0)
        market = MarketState(
            market='KRX',
            state='CLOSED',
            is_trading_day=True,
            is_open=False,
            server_time=now.isoformat(),
            session_open_at=(close_at - timedelta(hours=6, minutes=30)).isoformat(),
            session_close_at=close_at.isoformat(),
        )

        with patch('app.eod.auto_pykrx.async_get_market_state', return_value=market):
            await scheduler.schedule_daily_pipeline_if_due()

        snapshot = scheduler.snapshot()
        self.assertFalse(snapshot['active_task'])
        self.assertEqual(snapshot['last_status'], 'idle')
        self.assertIn('waiting_until:', snapshot['last_reason'])

    async def test_schedule_daily_pipeline_if_due_starts_background_task_after_close_buffer(self) -> None:
        scheduler = AutoPykrxEODScheduler()
        now = datetime.now(KST)
        close_at = (now - timedelta(minutes=30)).replace(second=0, microsecond=0)
        market = MarketState(
            market='KRX',
            state='CLOSED',
            is_trading_day=True,
            is_open=False,
            server_time=now.isoformat(),
            session_open_at=(close_at - timedelta(hours=6, minutes=30)).isoformat(),
            session_close_at=close_at.isoformat(),
        )

        with patch('app.eod.auto_pykrx.async_get_market_state', return_value=market), patch.object(
            scheduler,
            '_run_scheduled_daily_pipeline',
            new=AsyncMock(return_value=None),
        ):
            await scheduler.schedule_daily_pipeline_if_due()
            await scheduler.stop()

        snapshot = scheduler.snapshot()
        self.assertEqual(snapshot['last_status'], 'running')
