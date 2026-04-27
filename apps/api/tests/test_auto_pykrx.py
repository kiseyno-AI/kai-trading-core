from __future__ import annotations

from types import SimpleNamespace
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from unittest import IsolatedAsyncioTestCase, TestCase
from unittest.mock import ANY, AsyncMock, patch

import pandas as pd

from app.eod.auto_pykrx import AutoPykrxEODScheduler
from app.eod.pykrx_backfill import PYKRX_STAGE_SEQUENCE, PykrxBackfillService, PykrxSymbol
from app.market.clock import KST, MarketState


@contextmanager
def _lock(acquired: bool = True):
    yield acquired


class PykrxBackfillServiceTests(TestCase):
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

    def test_run_collection_always_chains_fundamentals_for_daily_auto(self) -> None:
        service = PykrxBackfillService()

        with (
            patch('app.eod.pykrx_backfill.settings.dart_api_key', 'test-dart-key'),
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
            patch(
                'app.eod.pykrx_backfill.dart_fundamentals_capture_service.run_daily_incremental_refresh',
                return_value={'status': 'completed', 'trigger': 'eod_pykrx_followup'},
            ) as fundamentals_refresh,
            patch('app.eod.pykrx_backfill.complete_eod_collection_run'),
        ):
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
        )

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
        self.assertEqual(result['price_contexts']['contexts_persisted'], 1)
        price_contexts.assert_called_once_with(
            market_date='2026-04-03',
            symbols=['005930'],
            source='pykrx',
            run_id=12,
        )

    def test_run_collection_skips_fundamentals_followup_when_dart_is_not_configured(self) -> None:
        service = PykrxBackfillService()

        with (
            patch('app.eod.pykrx_backfill.settings.dart_api_key', ''),
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
            patch(
                'app.eod.pykrx_backfill.dart_fundamentals_capture_service.run_daily_incremental_refresh',
            ) as fundamentals_refresh,
            patch('app.eod.pykrx_backfill.complete_eod_collection_run'),
        ):
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

        with patch('app.eod.auto_pykrx.get_market_state', return_value=market):
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

        with patch('app.eod.auto_pykrx.get_market_state', return_value=market), patch.object(
            scheduler,
            '_run_scheduled_daily_pipeline',
            new=AsyncMock(return_value=None),
        ):
            await scheduler.schedule_daily_pipeline_if_due()
            await scheduler.stop()

        snapshot = scheduler.snapshot()
        self.assertEqual(snapshot['last_status'], 'running')
