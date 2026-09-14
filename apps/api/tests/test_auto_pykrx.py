from __future__ import annotations

import hashlib
import os
from types import SimpleNamespace
from contextlib import ExitStack, contextmanager
from datetime import datetime, timedelta, timezone
from unittest import IsolatedAsyncioTestCase, TestCase
from unittest.mock import ANY, AsyncMock, patch

import pandas as pd

from app.core.settings import Settings
from app.eod.auto_pykrx import AutoPykrxEODScheduler
from app.eod.pykrx_backfill import (
    AUTO_FOLLOWUP_RECOVERY_TRIGGER,
    PYKRX_STAGE_SEQUENCE,
    PykrxBackfillService,
    PykrxSymbol,
)
from app.eod.pykrx_import import suppress_pykrx_login_env
from app.eod.pykrx_pipeline_runner import (
    AUTO_DAILY_FOLLOWUP_STAGE_ORDER,
    AUTO_DAILY_TRACKED_STAGE_ORDER,
    HISTORICAL_BACKFILL_STAGE_ORDER,
    _OfficialDailyPipelineTracker,
    _classify_pykrx_enrichment_categories,
    _extract_result_counts,
    _normalize_stage_status,
    _resolve_result_status,
    _run_daily_external_news_enrichment_followup,
    _run_daily_feedback_followup,
    _run_daily_macro_context_followup,
    _run_daily_next_open_execution_plan_followup,
    _run_daily_signal_decider_evaluation_followup,
    _run_daily_signal_decider_followup,
)
from app.market.clock import KST, MarketState


class PykrxPipelineResultStatusTests(TestCase):
    def test_completed_payload_with_failed_items_is_partial(self) -> None:
        result = {
            'status': 'completed',
            'completed_count': 60,
            'failed_count': 100,
        }

        self.assertEqual(_resolve_result_status(result), 'partial')

    def test_enrichment_provider_error_is_recovered_only_when_final_coverage_is_complete(self) -> None:
        classification = _classify_pykrx_enrichment_categories(
            category_results={
                'sector_classification': {'errors': [{'error': 'provider timeout'}]},
                'foreign_ownership': {'errors': []},
            },
            coverage_categories={
                'sector_classification': {'expected_rows': 2, 'present_rows': 2},
                'foreign_ownership': {'expected_rows': 2, 'present_rows': 1},
            },
            preflight_failed_categories=[],
        )

        self.assertEqual(classification['provider_error_categories'], ['sector_classification'])
        self.assertEqual(classification['recovered_categories'], ['sector_classification'])
        self.assertEqual(classification['coverage_gap_categories'], ['foreign_ownership'])
        self.assertEqual(classification['unresolved_categories'], ['foreign_ownership'])


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


def _point_in_time_universe_snapshot(
    symbols: list[PykrxSymbol],
    *,
    market_date: str = '2026-04-03',
) -> dict:
    markets = sorted({item.market.upper() for item in symbols})
    return {
        'symbols': symbols,
        'latest_trade_date': market_date,
        'universe_source': 'pykrx_market_ohlcv',
        'membership_basis': 'end_date_snapshot',
        'membership_scope_warning': 'test end-date snapshot',
        'complete': True,
        'market_status': {market: 'pykrx_market_ohlcv' for market in markets},
        'unavailable_markets': [],
        'market_errors': [],
    }


class NextOpenExecutionPlanFollowupTests(TestCase):
    def test_refreshes_plan_for_day_after_market_date(self) -> None:
        with patch(
            'app.orders.next_open_execution.signal_decider_next_open_execution_service.refresh_plan_items',
            return_value={'summary': {'items_total': 3}},
        ) as refresh_plan:
            result = _run_daily_next_open_execution_plan_followup(
                market_date='2026-04-03',
                enabled=True,
            )

        self.assertEqual(result['summary']['items_total'], 3)
        refresh_plan.assert_called_once_with(
            target_trade_date='2026-04-04',
            trigger='eod_post_decider',
        )


def _completed_factor_followup_result(*, market_date: str = '2026-04-03') -> dict:
    stages = {
        stage_id: {
            'status': 'completed',
            'result': {
                'market_date': market_date,
                'results_persisted': 1,
                'results_failed': 0,
            },
        }
        for stage_id in (
            'momentum_factors',
            'value_factors',
            'low_risk_factors',
            'beta_factors',
            'advanced_value_factors',
            'unified_value_factors',
            'quality_factors',
            'composite_factors',
        )
    }
    return {
        'triggered': True,
        'status': 'completed',
        'market_date': market_date,
        'stages': stages,
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


class _FakeDeduplicatingDailyPipelineRunStateService(_FakeDailyPipelineRunStateService):
    def start_run(self, **kwargs):
        self.start_runs.append(kwargs)
        return {
            'id': 744,
            'market_date': kwargs['market_date'],
            'trigger': 'manual_api',
            'status': 'running',
            'deduplicated': True,
            'deduplication_reason': 'overlapping_run_already_active',
            'stages': {'signal_decider': {'status': 'running'}},
        }


class PykrxBackfillServiceTests(TestCase):
    def setUp(self) -> None:
        self.next_open_plan_patcher = patch(
            'app.eod.pykrx_pipeline_runner._run_daily_next_open_execution_plan_followup',
            return_value={
                'triggered': True,
                'status': 'completed',
                'summary': {'items_total': 2, 'eligible_count': 1},
            },
        )
        self.next_open_plan_patcher.start()
        self.addCleanup(self.next_open_plan_patcher.stop)

    def test_daily_signal_decider_evaluation_refreshes_maturing_calendar_window(self) -> None:
        with patch(
            'app.agent_context.signal_decider_evaluation_maintenance_service.'
            'signal_decider_evaluation_maintenance_service.refresh_recent_coverage',
            return_value={'status': 'completed'},
        ) as refresh_recent_coverage:
            result = _run_daily_signal_decider_evaluation_followup(
                market_date='2026-08-19',
                enabled=True,
            )

        self.assertEqual(result, {'status': 'completed'})
        refresh_recent_coverage.assert_called_once_with(
            as_of_market_date='2026-08-19',
            refresh_calendar=True,
            calendar_lookback_days=21,
            trigger='eod_pykrx_followup',
        )

    def test_suppress_pykrx_login_env_temporarily_removes_credentials(self) -> None:
        original_id = os.environ.get('KRX_ID')
        original_pw = os.environ.get('KRX_PW')
        os.environ['KRX_ID'] = 'test-id'
        os.environ['KRX_PW'] = 'test-pw'
        try:
            with suppress_pykrx_login_env():
                self.assertNotIn('KRX_ID', os.environ)
                self.assertNotIn('KRX_PW', os.environ)
            self.assertEqual(os.environ.get('KRX_ID'), 'test-id')
            self.assertEqual(os.environ.get('KRX_PW'), 'test-pw')
        finally:
            if original_id is None:
                os.environ.pop('KRX_ID', None)
            else:
                os.environ['KRX_ID'] = original_id
            if original_pw is None:
                os.environ.pop('KRX_PW', None)
            else:
                os.environ['KRX_PW'] = original_pw

    def test_extract_result_counts_reads_agent_feedback_progress_summary(self) -> None:
        result = {
            'status': 'partial',
            'progress': {
                'summary': {
                    'completed_feedback_count': 60,
                    'failed_items': 0,
                    'expected_feedback_count': 986,
                }
            },
        }

        counts = _extract_result_counts(result)

        self.assertEqual(
            counts,
            {
                'processed': 60,
                'total': 986,
                'succeeded': 60,
                'failed': 0,
            },
        )

    def test_extract_result_counts_includes_every_next_open_plan_outcome(self) -> None:
        counts = _extract_result_counts(
            {
                'status': 'completed',
                'summary': {
                    'items_total': 18,
                    'eligible_count': 4,
                    'monitor_candidate_count': 10,
                    'blocked_count': 4,
                },
            }
        )

        self.assertEqual(
            counts,
            {
                'processed': 18,
                'total': 18,
                'succeeded': 18,
                'failed': 0,
            },
        )

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

    def test_auto_feedback_followups_use_matching_decider_scope(self) -> None:
        from app.agent_context.roles import DECISION_SUPPORT_AGENT_NAMES, SUMMARIZER_AGENT_NAMES
        from app.agent_context.run_modes import FEEDBACK_SCOPE_MODE_DECIDER_SCOPE_FAST_PATH

        configs = []

        def run_retry_cycles(*, config, **_kwargs):
            configs.append(config)
            return {'status': 'completed'}

        with (
            patch(
                'app.agent_context.prerequisite_service.agent_feedback_prerequisite_service.inspect_feedback_prerequisites',
                return_value={'ready': True, 'status': 'ready'},
            ),
            patch(
                'app.agent_context.retry_service.agent_feedback_retry_service.run_retry_cycles',
                side_effect=run_retry_cycles,
            ),
        ):
            summary_result = _run_daily_feedback_followup(market_date='2026-04-03', enabled=True)
            decider_result = _run_daily_signal_decider_followup(market_date='2026-04-03', enabled=True)

        self.assertEqual(summary_result['status'], 'completed')
        self.assertEqual(decider_result['status'], 'completed')
        self.assertEqual(
            [config.roles for config in configs],
            [SUMMARIZER_AGENT_NAMES, DECISION_SUPPORT_AGENT_NAMES],
        )
        self.assertEqual(
            [config.feedback_scope_mode for config in configs],
            [
                FEEDBACK_SCOPE_MODE_DECIDER_SCOPE_FAST_PATH,
                FEEDBACK_SCOPE_MODE_DECIDER_SCOPE_FAST_PATH,
            ],
        )
        for config in configs:
            self.assertIsInstance(config.knowledge_cutoff_at, datetime)
            self.assertIsNotNone(config.knowledge_cutoff_at.utcoffset())
            self.assertLessEqual(
                config.knowledge_cutoff_at,
                datetime.now(timezone.utc),
            )

    def test_auto_followup_order_matches_canonical_news_macro_feedback_dependencies(self) -> None:
        collect_index = AUTO_DAILY_FOLLOWUP_STAGE_ORDER.index('external_news_collect')

        self.assertEqual(
            AUTO_DAILY_FOLLOWUP_STAGE_ORDER[collect_index : collect_index + 4],
            (
                'external_news_collect',
                'external_news_enrich',
                'macro_context_refresh',
                'agent_feedback_summaries',
            ),
        )

    def test_external_news_enrichment_followup_uses_idempotent_classification_stage(self) -> None:
        with patch(
            'app.external_news.service.external_news_service.enrich_recent_classification',
            return_value={'status': 'completed', 'classifications_persisted': 2},
        ) as enrich_recent_classification:
            result = _run_daily_external_news_enrichment_followup(
                market_date='2026-04-03',
                enabled=True,
            )

        self.assertTrue(result['triggered'])
        self.assertEqual(result['market_date'], '2026-04-03')
        enrich_recent_classification.assert_called_once_with(
            lookback_days=ANY,
            limit=1000,
        )

    def test_macro_context_followup_uses_service_freshness_gate_instead_of_forcing_duplicate_refresh(self) -> None:
        macro_result = {
            'status': 'skipped',
            'reason': 'macro_context_refresh_not_due',
            'context': {'freshness': {'status': 'fresh'}},
        }
        with patch(
            'app.macro_context.service.macro_context_service.refresh_context',
            return_value=macro_result,
        ) as refresh_context:
            result = _run_daily_macro_context_followup(
                market_date='2026-04-03',
                enabled=True,
            )

        self.assertEqual(result['market_date'], '2026-04-03')
        self.assertEqual(result['freshness_gate'], 'fresh')
        refresh_context.assert_called_once_with(force=False, trigger='eod_pykrx_followup')

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

    def test_resolve_reference_trade_date_falls_back_to_krx_calendar_when_pykrx_probe_fails(self) -> None:
        service = PykrxBackfillService()

        with (
            patch('app.eod.pykrx_backfill._get_market_ohlcv_by_date', side_effect=ConnectionError('naver reset')),
            patch('app.eod.pykrx_backfill.latest_krx_trading_date', return_value=datetime(2026, 6, 9).date()) as latest,
        ):
            result = service.resolve_latest_trade_date(end_date='20260609')

        self.assertEqual(result, '2026-06-09')
        latest.assert_called_once()

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

    def test_historical_universe_uses_requested_end_date_and_never_current_master_fallback(self) -> None:
        service = PykrxBackfillService()
        service.name_pause_seconds = 0
        frame = pd.DataFrame({'종가': [5000]}, index=['099999'])

        def fake_call(func, *args, **kwargs):
            if func.__name__ == 'get_market_ohlcv_by_ticker':
                self.assertEqual(args[0], '20200429')
                return frame
            if func.__name__ == 'get_market_ticker_name':
                return '과거상장종목'
            raise AssertionError(f'unexpected pykrx function: {func.__name__}')

        with (
            patch.object(service, 'resolve_latest_trade_date', return_value='2020-04-29') as resolve_date,
            patch('app.eod.pykrx_backfill._call_pykrx', side_effect=fake_call),
            patch.object(service, '_fetch_master_fallback_universe') as master_fallback,
        ):
            result = service.fetch_recent_universe_snapshot(
                markets=('KOSPI',),
                end_date='20200430',
                allow_master_fallback=False,
            )

        resolve_date.assert_called_once_with(end_date='20200430')
        master_fallback.assert_not_called()
        self.assertEqual(
            result['symbols'],
            [PykrxSymbol(symbol='099999', market='KOSPI', name_ko='과거상장종목')],
        )
        self.assertEqual(result['membership_basis'], 'end_date_snapshot')

    def test_universe_limit_marks_later_markets_unqueried_without_source_failure(self) -> None:
        service = PykrxBackfillService()
        service.name_pause_seconds = 0
        frame = pd.DataFrame({'종가': [5000]}, index=['099999'])

        def fake_call(func, *args, **kwargs):
            if func.__name__ == 'get_market_ohlcv_by_ticker':
                self.assertEqual(kwargs['market'], 'KOSPI')
                return frame
            if func.__name__ == 'get_market_ticker_name':
                return '표본종목'
            raise AssertionError(f'unexpected pykrx function: {func.__name__}')

        with (
            patch.object(service, 'resolve_latest_trade_date', return_value='2020-04-29'),
            patch('app.eod.pykrx_backfill._call_pykrx', side_effect=fake_call),
        ):
            result = service.fetch_recent_universe_snapshot(
                markets=('KOSPI', 'KOSDAQ'),
                end_date='20200430',
                limit=1,
                allow_master_fallback=False,
            )

        self.assertTrue(result['complete'])
        self.assertEqual(result['unavailable_markets'], [])
        self.assertEqual(result['market_status']['KOSDAQ'], 'not_queried_due_to_limit')

    def test_historical_universe_fails_closed_when_point_in_time_source_is_unavailable(self) -> None:
        service = PykrxBackfillService()
        service.name_pause_seconds = 0

        with (
            patch.object(service, 'resolve_latest_trade_date', return_value='2020-04-29'),
            patch('app.eod.pykrx_backfill._call_pykrx', side_effect=ConnectionError('source unavailable')),
            patch.object(service, '_fetch_master_fallback_universe') as master_fallback,
        ):
            result = service.fetch_recent_universe_snapshot(
                markets=('KOSPI',),
                end_date='20200430',
                allow_master_fallback=False,
            )

        master_fallback.assert_not_called()
        self.assertEqual(result['symbols'], [])
        self.assertEqual(result['universe_source'], 'pykrx_market_ohlcv')
        self.assertFalse(result['complete'])
        self.assertEqual(result['unavailable_markets'], ['KOSPI'])

    def test_historical_universe_marks_partial_requested_market_snapshot_incomplete(self) -> None:
        service = PykrxBackfillService()
        service.name_pause_seconds = 0
        frame = pd.DataFrame({'종가': [5000]}, index=['099999'])

        def fake_call(func, *args, **kwargs):
            if func.__name__ == 'get_market_ohlcv_by_ticker':
                if kwargs['market'] == 'KOSPI':
                    return frame
                raise ConnectionError('KOSDAQ source unavailable')
            if func.__name__ == 'get_market_ticker_name':
                return '과거상장종목'
            raise AssertionError(f'unexpected pykrx function: {func.__name__}')

        with (
            patch.object(service, 'resolve_latest_trade_date', return_value='2020-04-29'),
            patch('app.eod.pykrx_backfill._call_pykrx', side_effect=fake_call),
        ):
            result = service.fetch_recent_universe_snapshot(
                markets=('KOSPI', 'KOSDAQ'),
                end_date='20200430',
                allow_master_fallback=False,
            )

        self.assertFalse(result['complete'])
        self.assertEqual(result['market_status']['KOSPI'], 'pykrx_market_ohlcv')
        self.assertEqual(result['market_status']['KOSDAQ'], 'unavailable')
        self.assertEqual(result['unavailable_markets'], ['KOSDAQ'])
        self.assertEqual(result['market_errors'][0]['status'], 'fetch_failed')

    def test_historical_pipeline_rejects_partial_requested_market_snapshot(self) -> None:
        service = PykrxBackfillService()
        snapshot = {
            'symbols': [PykrxSymbol(symbol='099999', market='KOSPI', name_ko='과거상장종목')],
            'latest_trade_date': '2020-04-29',
            'universe_source': 'pykrx_market_ohlcv',
            'membership_basis': 'end_date_snapshot',
            'complete': False,
            'market_status': {
                'KOSPI': 'pykrx_market_ohlcv',
                'KOSDAQ': 'unavailable',
            },
            'unavailable_markets': ['KOSDAQ'],
            'market_errors': [{'market': 'KOSDAQ', 'status': 'fetch_failed'}],
        }

        with (
            patch.object(service, 'fetch_recent_universe_snapshot', return_value=snapshot),
            patch('app.eod.pykrx_backfill.create_eod_collection_run') as create_run,
        ):
            with self.assertRaisesRegex(RuntimeError, 'incomplete.*KOSDAQ'):
                service._run_market_data_pipeline(
                    start_date='20200430',
                    end_date='20200430',
                    markets=('KOSPI', 'KOSDAQ'),
                    trigger='test',
                    refresh_universe=True,
                    mode='historical_backfill',
                )

        create_run.assert_not_called()

    def test_historical_pipeline_forces_point_in_time_universe_when_refresh_is_disabled(self) -> None:
        service = PykrxBackfillService()
        snapshot = {
            'symbols': [PykrxSymbol(symbol='099999', market='KOSPI', name_ko='과거상장종목')],
            'latest_trade_date': '2020-04-29',
            'universe_source': 'pykrx_market_ohlcv',
            'complete': True,
            'market_status': {'KOSPI': 'pykrx_market_ohlcv'},
            'unavailable_markets': [],
            'market_errors': [],
        }

        with (
            patch.object(service, 'fetch_recent_universe_snapshot', return_value=snapshot) as fetch_snapshot,
            patch.object(service, 'load_active_universe') as load_active_universe,
            patch.object(service, 'find_symbol') as find_symbol,
            patch.object(service, 'resolve_latest_trade_date', return_value='2020-04-29'),
            patch(
                'app.eod.pykrx_backfill.create_eod_collection_run',
                side_effect=RuntimeError('stop after universe selection'),
            ),
        ):
            with self.assertRaisesRegex(RuntimeError, 'stop after universe selection'):
                service._run_market_data_pipeline(
                    start_date='20200430',
                    end_date='20200430',
                    markets=('KOSPI',),
                    trigger='test',
                    refresh_universe=False,
                    mode='historical_backfill',
                    limit=1,
                    symbol='099999',
                )

        fetch_snapshot.assert_called_once_with(
            markets=('KOSPI',),
            limit=None,
            end_date='20200430',
            allow_master_fallback=False,
        )
        load_active_universe.assert_not_called()
        find_symbol.assert_not_called()

    def test_pipeline_persists_point_in_time_market_membership_proof(self) -> None:
        service = PykrxBackfillService()
        symbols = [
            PykrxSymbol(symbol='005930', market='KOSPI', name_ko='삼성전자'),
            PykrxSymbol(symbol='247540', market='KOSDAQ', name_ko='에코프로비엠'),
        ]

        with (
            patch.object(
                service,
                'fetch_recent_universe_snapshot',
                return_value=_point_in_time_universe_snapshot(
                    symbols,
                    market_date='2026-04-07',
                ),
            ),
            patch(
                'app.eod.pykrx_backfill.create_eod_collection_run',
                side_effect=RuntimeError('stop after run admission'),
            ) as create_run,
        ):
            with self.assertRaisesRegex(RuntimeError, 'stop after run admission'):
                service._run_market_data_pipeline(
                    start_date='20260407',
                    end_date='20260407',
                    markets=('KOSPI', 'KOSDAQ'),
                    trigger='test',
                    refresh_universe=True,
                    mode='historical_backfill',
                )

        metadata = create_run.call_args.kwargs['run_metadata']
        membership = metadata['universe_snapshot']['market_membership']
        self.assertEqual(membership['schema_version'], 'pykrx_market_membership_v1')
        self.assertEqual(membership['symbol_count'], 2)
        self.assertEqual(membership['markets']['KOSPI']['symbols'], ['005930'])
        self.assertEqual(membership['markets']['KOSDAQ']['symbols'], ['247540'])
        self.assertEqual(
            membership['markets']['KOSPI']['membership_sha256'],
            hashlib.sha256(b'005930').hexdigest(),
        )

    def test_historical_pipeline_rejects_symbol_override_membership_bypass(self) -> None:
        service = PykrxBackfillService()

        with (
            patch.object(service, 'fetch_recent_universe_snapshot') as fetch_snapshot,
            patch.object(service, 'load_active_universe') as load_active_universe,
        ):
            with self.assertRaisesRegex(
                ValueError,
                'membership must be derived from the requested end date',
            ):
                service._run_market_data_pipeline(
                    start_date='20200401',
                    end_date='20200430',
                    markets=('KOSPI',),
                    trigger='test',
                    refresh_universe=False,
                    mode='historical_backfill',
                    symbols_override=[
                        PykrxSymbol(
                            symbol='005930',
                            market='KOSPI',
                            name_ko='Current master row',
                        )
                    ],
                )

        fetch_snapshot.assert_not_called()
        load_active_universe.assert_not_called()

    def test_historical_pipeline_rejects_multi_day_range_before_io(self) -> None:
        service = PykrxBackfillService()

        with (
            patch.object(service, 'fetch_recent_universe_snapshot') as fetch_snapshot,
            patch('app.eod.pykrx_backfill.create_eod_collection_run') as create_run,
            patch('app.eod.pykrx_backfill.log_fetch_event') as log_fetch,
        ):
            with self.assertRaisesRegex(ValueError, 'one market date per request'):
                service._run_market_data_pipeline(
                    start_date='20200401',
                    end_date='20200430',
                    markets=('KOSPI',),
                    trigger='test',
                    refresh_universe=True,
                    mode='historical_backfill',
                )

        fetch_snapshot.assert_not_called()
        create_run.assert_not_called()
        log_fetch.assert_not_called()

    def test_run_backfill_uses_only_point_in_time_safe_stages_and_disables_followups(self) -> None:
        service = PykrxBackfillService()
        with (
            patch.object(service, 'resolve_latest_trade_date', return_value='2026-05-04') as latest,
            patch.object(
                service,
                '_run_market_data_pipeline',
                return_value={'status': 'completed'},
            ) as run_pipeline,
        ):
            service.run_backfill()

        latest.assert_called_once_with()
        kwargs = run_pipeline.call_args.kwargs
        self.assertEqual(kwargs['start_date'], '20260504')
        self.assertEqual(kwargs['end_date'], '20260504')
        self.assertEqual(kwargs['selected_stages'], HISTORICAL_BACKFILL_STAGE_ORDER)
        self.assertFalse(kwargs['chain_fundamentals_followup'])

    def test_run_backfill_preserves_explicit_trading_date(self) -> None:
        service = PykrxBackfillService()
        with patch.object(
            service,
            '_run_market_data_pipeline',
            return_value={'status': 'completed'},
        ) as run_pipeline:
            service.run_backfill(end_date='20200430')

        kwargs = run_pipeline.call_args.kwargs
        self.assertEqual(kwargs['start_date'], '20200430')
        self.assertEqual(kwargs['end_date'], '20200430')
        self.assertEqual(kwargs['selected_stages'], HISTORICAL_BACKFILL_STAGE_ORDER)
        self.assertFalse(kwargs['chain_fundamentals_followup'])

    def test_historical_pipeline_rejects_current_only_and_invalid_stage_dependencies_before_io(self) -> None:
        service = PykrxBackfillService()
        for stages, message in (
            (('price_collection', 'signal_features'), 'point-in-time-safe stages'),
            (('technical_snapshots',), 'price_collection stage is required'),
            (('price_collection', 'action_summaries'), 'technical_snapshots is required'),
            ((), 'At least one pykrx stage'),
        ):
            with self.subTest(stages=stages), patch.object(
                service,
                'fetch_recent_universe_snapshot',
            ) as fetch_snapshot:
                with self.assertRaisesRegex(ValueError, message):
                    service._run_market_data_pipeline(
                        start_date='20200430',
                        end_date='20200430',
                        markets=('KOSPI',),
                        trigger='test',
                        refresh_universe=True,
                        mode='historical_backfill',
                        selected_stages=stages,
                    )
                fetch_snapshot.assert_not_called()

    def test_historical_pipeline_does_not_find_missing_symbol_in_current_master(self) -> None:
        service = PykrxBackfillService()
        snapshot = {
            'symbols': [PykrxSymbol(symbol='099999', market='KOSPI', name_ko='과거상장종목')],
            'latest_trade_date': '2020-04-29',
            'universe_source': 'pykrx_market_ohlcv',
            'complete': True,
            'market_status': {'KOSPI': 'pykrx_market_ohlcv'},
            'unavailable_markets': [],
            'market_errors': [],
        }

        with (
            patch.object(service, 'fetch_recent_universe_snapshot', return_value=snapshot),
            patch.object(service, 'find_symbol') as find_symbol,
        ):
            with self.assertRaisesRegex(RuntimeError, 'No active pykrx symbols'):
                service._run_market_data_pipeline(
                    start_date='20200430',
                    end_date='20200430',
                    markets=('KOSPI',),
                    trigger='test',
                    refresh_universe=True,
                    mode='historical_backfill',
                    symbol='005930',
                )

        find_symbol.assert_not_called()

    def test_refresh_recent_universe_preserves_snapshot_and_persist_counts(self) -> None:
        service = PykrxBackfillService()
        snapshot = {
            'symbols': [PykrxSymbol(symbol='005930', market='KOSPI', name_ko='삼성전자')],
            'latest_trade_date': '2026-04-07',
            'universe_source': 'pykrx_market_ohlcv',
            'membership_basis': 'end_date_snapshot',
            'membership_scope_warning': 'end-date snapshot only',
            'complete': True,
            'market_status': {'KOSPI': 'pykrx_market_ohlcv'},
            'unavailable_markets': [],
            'market_errors': [],
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
                'membership_basis': 'end_date_snapshot',
                'membership_scope_warning': 'end-date snapshot only',
                'complete': True,
                'market_status': {'KOSPI': 'pykrx_market_ohlcv'},
                'unavailable_markets': [],
                'market_errors': [],
                'deactivate_missing': False,
            },
        )
        fetch_snapshot.assert_called_once_with(markets=('KOSPI',), limit=None)
        persist_universe.assert_called_once_with(snapshot['symbols'], deactivate_missing=False)

    def test_load_active_universe_filters_inactive_symbols_at_query_boundary(self) -> None:
        service = PykrxBackfillService()
        with patch('app.eod.pykrx_backfill.load_symbols', return_value=[]) as load_symbol_rows:
            result = service.load_active_universe(markets=('KOSPI',), limit=25)

        self.assertEqual(result, [])
        load_symbol_rows.assert_called_once_with(
            is_active=True,
            markets=['KOSPI'],
            limit=25,
        )

    def test_refresh_recent_universe_rejects_incomplete_snapshot_without_persisting(self) -> None:
        service = PykrxBackfillService()
        snapshot = {
            'symbols': [PykrxSymbol(symbol='005930', market='KOSPI', name_ko='삼성전자')],
            'latest_trade_date': '2026-04-07',
            'universe_source': 'pykrx_market_ohlcv',
            'complete': False,
            'market_status': {
                'KOSPI': 'pykrx_market_ohlcv',
                'KOSDAQ': 'unavailable',
            },
            'unavailable_markets': ['KOSDAQ'],
            'market_errors': [
                {
                    'market': 'KOSDAQ',
                    'status': 'fetch_failed',
                    'error_type': 'ConnectionError',
                    'error_message': 'source unavailable',
                }
            ],
        }

        with (
            patch.object(service, 'fetch_recent_universe_snapshot', return_value=snapshot),
            patch.object(service, '_persist_universe') as persist_universe,
            patch('app.eod.pykrx_backfill._log_pykrx_fetch') as log_fetch,
        ):
            with self.assertRaisesRegex(RuntimeError, 'incomplete.*KOSDAQ'):
                service.refresh_recent_universe(markets=('KOSPI', 'KOSDAQ'))

        persist_universe.assert_not_called()
        failure_log = log_fetch.call_args_list[-1]
        self.assertEqual(failure_log.args, ('refresh_recent_universe', 'failed'))
        self.assertFalse(failure_log.kwargs['complete'])
        self.assertEqual(failure_log.kwargs['unavailable_markets'], ['KOSDAQ'])
        self.assertEqual(failure_log.kwargs['market_errors'], snapshot['market_errors'])

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
            patch.object(service, '_resume_auto_daily_followups_if_due', return_value=None),
            patch.object(service, '_run_market_data_pipeline') as run_collection,
        ):
            result = service.run_daily_pipeline_if_due(
                market_date='2026-04-03',
                trigger='auto_close_scheduler',
            )

        self.assertFalse(result['triggered'])
        self.assertEqual(result['reason'], 'already_completed')
        run_collection.assert_not_called()

    def test_completed_collection_resumes_only_incomplete_followups(self) -> None:
        service = PykrxBackfillService()
        failed_stage = 'external_news_collect'
        failed_index = AUTO_DAILY_FOLLOWUP_STAGE_ORDER.index(failed_stage)
        stage_rows = {
            stage_id: {
                'status': (
                    'completed'
                    if index < failed_index
                    else 'failed'
                    if index == failed_index
                    else 'blocked'
                )
            }
            for index, stage_id in enumerate(AUTO_DAILY_FOLLOWUP_STAGE_ORDER)
        }
        latest_pipeline_run = {
            'id': 91,
            'trigger': 'auto_close_scheduler',
            'status': 'partial',
            'updated_at': '2026-04-03T08:00:00+00:00',
            'run_metadata': {'resolved_stages': list(AUTO_DAILY_FOLLOWUP_STAGE_ORDER)},
            'stages': stage_rows,
        }

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
            patch(
                'app.eod.pykrx_backfill.daily_pipeline_run_state_service.get_latest_run_for_context',
                return_value=latest_pipeline_run,
            ),
            patch(
                'app.eod.pykrx_backfill.daily_pipeline_run_state_service.count_runs',
                return_value=0,
            ),
            patch(
                'app.eod.daily_pipeline_stages.daily_pipeline_stage_service.run',
                return_value={'status': 'completed', 'pipeline_run_id': 92},
            ) as resume_followups,
            patch.object(service, '_run_market_data_pipeline') as run_collection,
        ):
            result = service.run_daily_pipeline_if_due(
                market_date='2026-04-03',
                trigger='auto_close_scheduler',
            )

        self.assertTrue(result['triggered'])
        self.assertEqual(result['reason'], 'incomplete_followups_resumed')
        self.assertEqual(result['followup_recovery']['resume_from_stage'], failed_stage)
        expected_stages = list(AUTO_DAILY_FOLLOWUP_STAGE_ORDER[failed_index:])
        self.assertEqual(result['followup_recovery']['stages'], expected_stages)
        self.assertNotIn('price_collection', expected_stages)
        resume_followups.assert_called_once_with(
            market_date='2026-04-03',
            stages=expected_stages,
            markets=('KOSPI', 'KOSDAQ', 'KONEX'),
            force=False,
            reuse_existing_feedback=True,
            resume_from_stage=failed_stage,
            trigger=AUTO_FOLLOWUP_RECOVERY_TRIGGER,
        )
        run_collection.assert_not_called()

    def test_downstream_only_failure_resumes_without_downloading_prices_again(self) -> None:
        service = PykrxBackfillService()
        failed_stage = 'technical_snapshots'
        failed_index = AUTO_DAILY_TRACKED_STAGE_ORDER.index(failed_stage)
        stage_rows = {
            stage_id: {
                'status': (
                    'completed'
                    if index < failed_index
                    else 'failed'
                    if index == failed_index
                    else 'completed'
                )
            }
            for index, stage_id in enumerate(AUTO_DAILY_TRACKED_STAGE_ORDER)
        }
        latest_pipeline_run = {
            'id': 91,
            'trigger': 'auto_close_scheduler',
            'status': 'partial',
            'updated_at': '2026-04-03T08:00:00+00:00',
            'run_metadata': {'resolved_stages': list(AUTO_DAILY_TRACKED_STAGE_ORDER)},
            'stages': stage_rows,
        }

        with (
            patch.object(service, 'resolve_latest_trade_date', return_value='2026-04-03'),
            patch('app.eod.pykrx_backfill.advisory_eod_collection_lock', return_value=_lock()),
            patch(
                'app.eod.pykrx_backfill.load_latest_eod_collection_run',
                return_value={
                    'id': 7,
                    'status': 'completed_with_errors',
                    'symbols_failed': 0,
                    'run_metadata': {
                        'failed_symbols': [],
                        'downstream_retry_symbols': ['005930'],
                    },
                },
            ),
            patch(
                'app.eod.pykrx_backfill.daily_pipeline_run_state_service.get_latest_run_for_context',
                return_value=latest_pipeline_run,
            ),
            patch(
                'app.eod.pykrx_backfill.daily_pipeline_run_state_service.count_runs',
                return_value=0,
            ),
            patch(
                'app.eod.daily_pipeline_stages.daily_pipeline_stage_service.run',
                return_value={'status': 'completed', 'pipeline_run_id': 92},
            ) as resume_stages,
            patch.object(service, '_run_market_data_pipeline') as run_collection,
        ):
            result = service.run_daily_pipeline_if_due(
                market_date='2026-04-03',
                trigger='auto_close_scheduler',
            )

        expected_stages = list(AUTO_DAILY_TRACKED_STAGE_ORDER[failed_index:])
        self.assertTrue(result['triggered'])
        self.assertEqual(result['reason'], 'incomplete_followups_resumed')
        self.assertEqual(result['followup_recovery']['resume_from_stage'], failed_stage)
        self.assertEqual(result['followup_recovery']['stages'], expected_stages)
        self.assertEqual(result['followup_recovery']['symbols'], ['005930'])
        self.assertNotIn('price_collection', expected_stages)
        resume_stages.assert_called_once_with(
            market_date='2026-04-03',
            stages=expected_stages,
            markets=('KOSPI', 'KOSDAQ', 'KONEX'),
            force=False,
            reuse_existing_feedback=True,
            resume_from_stage=failed_stage,
            trigger=AUTO_FOLLOWUP_RECOVERY_TRIGGER,
            symbols=['005930'],
        )
        run_collection.assert_not_called()

    def test_price_failure_does_not_starve_existing_downstream_recovery(self) -> None:
        service = PykrxBackfillService()
        failed_stage = 'technical_snapshots'
        failed_index = AUTO_DAILY_TRACKED_STAGE_ORDER.index(failed_stage)
        stage_rows = {
            stage_id: {
                'status': (
                    'completed'
                    if index < failed_index
                    else 'failed'
                    if index == failed_index
                    else 'blocked'
                )
            }
            for index, stage_id in enumerate(AUTO_DAILY_TRACKED_STAGE_ORDER)
        }
        latest_pipeline_run = {
            'id': 91,
            'trigger': 'auto_close_scheduler',
            'status': 'partial',
            'updated_at': '2026-04-03T08:00:00+00:00',
            'run_metadata': {
                'resolved_stages': list(AUTO_DAILY_TRACKED_STAGE_ORDER)
            },
            'stages': stage_rows,
        }
        latest_eod_run = {
            'id': 7,
            'status': 'completed_with_errors',
            'symbols_failed': 1,
            'run_metadata': {
                'failed_symbols': ['472220'],
                # Never send a price-missing symbol into stages that require
                # a same-day bar, even if legacy metadata overlaps the scopes.
                'downstream_retry_symbols': ['005930', '472220'],
            },
        }

        with (
            patch.object(
                service,
                'resolve_latest_trade_date',
                return_value='2026-04-03',
            ),
            patch(
                'app.eod.pykrx_backfill.advisory_eod_collection_lock',
                return_value=_lock(),
            ),
            patch(
                'app.eod.pykrx_backfill.load_latest_eod_collection_run',
                return_value=latest_eod_run,
            ),
            patch(
                'app.eod.pykrx_backfill.daily_pipeline_run_state_service.get_latest_run_for_context',
                return_value=latest_pipeline_run,
            ),
            patch(
                'app.eod.pykrx_backfill.daily_pipeline_run_state_service.count_runs',
                return_value=0,
            ),
            patch(
                'app.eod.daily_pipeline_stages.daily_pipeline_stage_service.run',
                return_value={'status': 'completed', 'pipeline_run_id': 92},
            ) as resume_stages,
            patch(
                'app.eod.pykrx_backfill.count_eod_collection_runs',
                return_value=1,
            ),
            patch.object(
                service,
                '_load_retry_symbols_for_run',
                return_value=[
                    PykrxSymbol(
                        symbol='472220',
                        market='KOSDAQ',
                        name_ko='test',
                        standard_code=None,
                    )
                ],
            ),
            patch.object(service, '_run_market_data_pipeline') as run_collection,
        ):
            result = service.run_daily_pipeline_if_due(
                market_date='2026-04-03',
                trigger='auto_close_scheduler',
            )

        self.assertEqual(result['reason'], 'incomplete_followups_resumed')
        self.assertEqual(result['followup_recovery']['symbols'], ['005930'])
        resume_stages.assert_called_once()
        run_collection.assert_not_called()

    def test_price_retry_continues_after_followup_recovery_limit(self) -> None:
        service = PykrxBackfillService()
        latest_eod_run = {
            'id': 7,
            'status': 'completed_with_errors',
            'symbols_failed': 1,
            'run_metadata': {
                'failed_symbols': ['472220'],
                'downstream_retry_symbols': ['005930'],
            },
        }
        retry_symbol = PykrxSymbol(
            symbol='472220',
            market='KOSDAQ',
            name_ko='test',
            standard_code=None,
        )

        with (
            patch.object(
                service,
                'resolve_latest_trade_date',
                return_value='2026-04-03',
            ),
            patch(
                'app.eod.pykrx_backfill.advisory_eod_collection_lock',
                return_value=_lock(),
            ),
            patch(
                'app.eod.pykrx_backfill.load_latest_eod_collection_run',
                return_value=latest_eod_run,
            ),
            patch.object(
                service,
                '_resume_auto_daily_followups_if_due',
                return_value={
                    'triggered': False,
                    'status': 'skipped',
                    'reason': 'followup_recovery_limit_reached',
                },
            ) as resume_followups,
            patch(
                'app.eod.pykrx_backfill.count_eod_collection_runs',
                return_value=1,
            ),
            patch.object(
                service,
                '_load_retry_symbols_for_run',
                return_value=[retry_symbol],
            ),
            patch.object(
                service,
                '_run_market_data_pipeline',
                return_value={'status': 'completed', 'run_id': 8},
            ) as run_collection,
        ):
            result = service.run_daily_pipeline_if_due(
                market_date='2026-04-03',
                trigger='auto_close_scheduler',
            )

        self.assertTrue(result['triggered'])
        resume_followups.assert_called_once()
        run_collection.assert_called_once()
        self.assertEqual(
            run_collection.call_args.kwargs['symbols_override'],
            [retry_symbol],
        )
        self.assertFalse(run_collection.call_args.kwargs['refresh_universe'])

    def test_mixed_symbol_partial_and_incomplete_followup_resumes_full_universe_without_prices(self) -> None:
        service = PykrxBackfillService()
        failed_stage = 'technical_snapshots'
        failed_index = AUTO_DAILY_TRACKED_STAGE_ORDER.index(failed_stage)
        stage_rows = {
            stage_id: {'status': 'completed'}
            for stage_id in AUTO_DAILY_TRACKED_STAGE_ORDER
        }
        stage_rows[failed_stage] = {'status': 'partial'}
        stage_rows['momentum_factors'] = {'status': 'failed'}
        stage_rows.pop('signal_decider')
        latest_pipeline_run = {
            'id': 91,
            'trigger': 'auto_close_scheduler',
            'status': 'partial',
            'updated_at': '2026-04-03T08:00:00+00:00',
            'run_metadata': {'resolved_stages': list(AUTO_DAILY_TRACKED_STAGE_ORDER)},
            'stages': stage_rows,
        }

        with (
            patch.object(service, 'resolve_latest_trade_date', return_value='2026-04-03'),
            patch('app.eod.pykrx_backfill.advisory_eod_collection_lock', return_value=_lock()),
            patch(
                'app.eod.pykrx_backfill.load_latest_eod_collection_run',
                return_value={
                    'id': 7,
                    'status': 'completed_with_errors',
                    'symbols_failed': 0,
                    'run_metadata': {
                        'failed_symbols': [],
                        'downstream_retry_symbols': ['005930'],
                    },
                },
            ),
            patch(
                'app.eod.pykrx_backfill.daily_pipeline_run_state_service.get_latest_run_for_context',
                return_value=latest_pipeline_run,
            ),
            patch(
                'app.eod.pykrx_backfill.daily_pipeline_run_state_service.count_runs',
                return_value=0,
            ),
            patch(
                'app.eod.daily_pipeline_stages.daily_pipeline_stage_service.run',
                return_value={'status': 'completed', 'pipeline_run_id': 92},
            ) as resume_stages,
            patch.object(service, '_run_market_data_pipeline') as run_collection,
        ):
            result = service.run_daily_pipeline_if_due(
                market_date='2026-04-03',
                trigger='auto_close_scheduler',
            )

        expected_stages = list(AUTO_DAILY_TRACKED_STAGE_ORDER[failed_index:])
        self.assertTrue(result['triggered'])
        self.assertEqual(result['reason'], 'incomplete_followups_resumed')
        self.assertEqual(result['followup_recovery']['resume_from_stage'], failed_stage)
        self.assertEqual(result['followup_recovery']['stages'], expected_stages)
        self.assertEqual(result['followup_recovery']['symbols'], [])
        self.assertNotIn('price_collection', expected_stages)
        resume_stages.assert_called_once_with(
            market_date='2026-04-03',
            stages=expected_stages,
            markets=('KOSPI', 'KOSDAQ', 'KONEX'),
            force=False,
            reuse_existing_feedback=True,
            resume_from_stage=failed_stage,
            trigger=AUTO_FOLLOWUP_RECOVERY_TRIGGER,
        )
        run_collection.assert_not_called()

    def test_followup_recovery_respects_retry_backoff(self) -> None:
        service = PykrxBackfillService()
        latest_pipeline_run = {
            'id': 92,
            'trigger': AUTO_FOLLOWUP_RECOVERY_TRIGGER,
            'status': 'failed',
            'updated_at': datetime.now(timezone.utc).isoformat(),
            'completed_at': datetime.now(timezone.utc).isoformat(),
            'run_metadata': {'resolved_stages': ['signal_decider']},
            'stages': {'signal_decider': {'status': 'failed'}},
        }

        with (
            patch.object(service, 'resolve_latest_trade_date', return_value='2026-04-03'),
            patch('app.eod.pykrx_backfill.advisory_eod_collection_lock', return_value=_lock()),
            patch(
                'app.eod.pykrx_backfill.load_latest_eod_collection_run',
                return_value={'id': 7, 'status': 'completed', 'symbols_failed': 0},
            ),
            patch(
                'app.eod.pykrx_backfill.daily_pipeline_run_state_service.get_latest_run_for_context',
                return_value=latest_pipeline_run,
            ),
            patch(
                'app.eod.pykrx_backfill.daily_pipeline_run_state_service.count_runs',
                return_value=1,
            ),
            patch('app.eod.daily_pipeline_stages.daily_pipeline_stage_service.run') as resume_followups,
            patch.object(service, '_run_market_data_pipeline') as run_collection,
        ):
            result = service.run_daily_pipeline_if_due(
                market_date='2026-04-03',
                trigger='auto_close_scheduler',
            )

        self.assertFalse(result['triggered'])
        self.assertEqual(result['reason'], 'followup_recovery_backoff')
        self.assertGreater(result['followup_recovery']['retry_after_seconds'], 0)
        resume_followups.assert_not_called()
        run_collection.assert_not_called()

    def test_followup_recovery_stops_after_bounded_attempts(self) -> None:
        service = PykrxBackfillService()
        latest_pipeline_run = {
            'id': 96,
            'trigger': AUTO_FOLLOWUP_RECOVERY_TRIGGER,
            'status': 'failed',
            'updated_at': '2026-04-03T08:00:00+00:00',
            'completed_at': '2026-04-03T08:00:00+00:00',
            'run_metadata': {'resolved_stages': ['signal_decider']},
            'stages': {'signal_decider': {'status': 'failed'}},
        }

        with (
            patch.object(service, 'resolve_latest_trade_date', return_value='2026-04-03'),
            patch('app.eod.pykrx_backfill.advisory_eod_collection_lock', return_value=_lock()),
            patch(
                'app.eod.pykrx_backfill.load_latest_eod_collection_run',
                return_value={'id': 7, 'status': 'completed', 'symbols_failed': 0},
            ),
            patch(
                'app.eod.pykrx_backfill.daily_pipeline_run_state_service.get_latest_run_for_context',
                return_value=latest_pipeline_run,
            ),
            patch(
                'app.eod.pykrx_backfill.daily_pipeline_run_state_service.count_runs',
                return_value=5,
            ),
            patch('app.eod.daily_pipeline_stages.daily_pipeline_stage_service.run') as resume_followups,
            patch.object(service, '_run_market_data_pipeline') as run_collection,
        ):
            result = service.run_daily_pipeline_if_due(
                market_date='2026-04-03',
                trigger='auto_close_scheduler',
            )

        self.assertFalse(result['triggered'])
        self.assertEqual(result['reason'], 'followup_recovery_limit_reached')
        self.assertEqual(result['followup_recovery']['attempts'], 5)
        resume_followups.assert_not_called()
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

    def test_run_daily_pipeline_stops_before_collection_when_admission_is_deduplicated(self) -> None:
        service = PykrxBackfillService()
        run_state_service = _FakeDeduplicatingDailyPipelineRunStateService()

        with (
            patch.object(service, 'resolve_latest_trade_date', return_value='2026-04-03'),
            patch.object(
                service,
                'load_active_universe',
                return_value=[PykrxSymbol(symbol='005930', market='KOSPI', name_ko='Samsung Electronics')],
            ),
            patch('app.eod.pykrx_backfill.advisory_eod_collection_lock', return_value=_lock()),
            patch('app.eod.pykrx_backfill.load_latest_eod_collection_run', return_value=None),
            patch('app.eod.pykrx_backfill.daily_pipeline_run_state_service', run_state_service),
            patch('app.eod.pykrx_backfill.create_eod_collection_run') as create_run,
            patch('app.eod.pykrx_backfill._get_market_ohlcv_by_date') as collect_prices,
            patch(
                'app.eod.pykrx_backfill.dart_fundamentals_capture_service.run_daily_incremental_refresh'
            ) as fundamentals_followup,
        ):
            result = service.run_daily_pipeline_if_due(
                market_date='2026-04-03',
                trigger='auto_close_scheduler',
                refresh_universe=False,
            )

        self.assertFalse(result['triggered'])
        self.assertEqual(result['status'], 'skipped')
        self.assertEqual(result['reason'], 'overlapping_run_already_active')
        self.assertEqual(result['pipeline_run_id'], 744)
        create_run.assert_not_called()
        collect_prices.assert_not_called()
        fundamentals_followup.assert_not_called()
        self.assertEqual(run_state_service.stage_updates, [])
        self.assertEqual(run_state_service.finish_runs, [])

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

    def test_daily_signal_decider_followup_bypasses_blocked_full_market_prerequisites_for_fast_path(self) -> None:
        blocked_prerequisites = {
            'ready': False,
            'status': 'blocked',
            'missing_prerequisites': [{'stage': 'piotroski_filter'}],
        }

        with (
            patch(
                'app.agent_context.prerequisite_service.agent_feedback_prerequisite_service.inspect_feedback_prerequisites',
                return_value=blocked_prerequisites,
            ),
            patch(
                'app.agent_context.retry_service.agent_feedback_retry_service.run_retry_cycles',
                return_value={'status': 'completed', 'completed': True},
            ) as run_retry_cycles,
            patch('app.core.settings.settings.openclaw_feedback_fast_path_bypass_prerequisite_gate', True),
        ):
            result = _run_daily_signal_decider_followup(
                market_date='2026-04-03',
                enabled=True,
            )

        self.assertTrue(result['triggered'])
        self.assertTrue(result['prerequisite_gate_bypassed'])
        self.assertEqual(result['prerequisites'], blocked_prerequisites)
        self.assertTrue(run_retry_cycles.call_args.kwargs['bypass_prerequisite_gate'])
        self.assertFalse(run_retry_cycles.call_args.kwargs.get('force', False))
        self.assertFalse(run_retry_cycles.call_args.kwargs['config'].force_recompute)

    def test_daily_signal_decider_followup_does_not_bypass_or_force_when_prerequisites_are_ready(self) -> None:
        ready_prerequisites = {'ready': True, 'status': 'ready'}

        with (
            patch(
                'app.agent_context.prerequisite_service.agent_feedback_prerequisite_service.inspect_feedback_prerequisites',
                return_value=ready_prerequisites,
            ),
            patch(
                'app.agent_context.retry_service.agent_feedback_retry_service.run_retry_cycles',
                return_value={'status': 'completed', 'completed': True},
            ) as run_retry_cycles,
            patch('app.core.settings.settings.openclaw_feedback_fast_path_bypass_prerequisite_gate', True),
        ):
            result = _run_daily_signal_decider_followup(
                market_date='2026-04-03',
                enabled=True,
            )

        self.assertTrue(result['triggered'])
        self.assertFalse(result['prerequisite_gate_bypassed'])
        self.assertIsNone(result['prerequisites'])
        self.assertFalse(run_retry_cycles.call_args.kwargs['bypass_prerequisite_gate'])
        self.assertFalse(run_retry_cycles.call_args.kwargs.get('force', False))
        self.assertFalse(run_retry_cycles.call_args.kwargs['config'].force_recompute)

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
        symbols = [PykrxSymbol(symbol='005930', market='KOSPI', name_ko='Samsung Electronics')]

        with (
            patch('app.eod.pykrx_backfill.log_fetch_event') as log_fetch_event,
            patch.object(
                service,
                'fetch_recent_universe_snapshot',
                return_value=_point_in_time_universe_snapshot(symbols),
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
            patch('app.eod.pykrx_backfill.persist_daily_price_bars', return_value=1) as persist_bars,
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
        self.assertEqual(result['trending_ranks']['reason'], 'stage_not_requested')
        self.assertEqual(result['market_risk_filter']['reason'], 'stage_not_requested')
        self.assertFalse(persist_bars.call_args.kwargs['update_current_symbol_state'])
        price_contexts.assert_called_once_with(
            market_date='2026-04-03',
            symbols=['005930'],
            source='pykrx',
            run_id=12,
            include_retry_metadata=True,
        )
        technical_snapshots.assert_called_once_with(
            market_date='2026-04-03',
            symbols=['005930'],
            source='pykrx',
            run_id=12,
            progress_callback=ANY,
            include_retry_metadata=True,
        )
        action_summaries.assert_called_once_with(
            market_date='2026-04-03',
            symbols=['005930'],
            source='pykrx',
            run_id=12,
            progress_callback=ANY,
            include_retry_metadata=True,
        )
        trending_ranks.assert_not_called()
        signal_features.assert_not_called()
        market_risk_filter.assert_not_called()
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
        followup_order: list[str] = []

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
                    return_value=_completed_factor_followup_result(),
                )
            )
            external_news_followup = stack.enter_context(
                patch(
                    'app.eod.pykrx_pipeline_runner._run_daily_external_news_followup',
                    side_effect=lambda **_: followup_order.append('external_news_collect')
                    or {'triggered': True, 'status': 'disabled', 'reason': 'naver_search_credentials_missing'},
                )
            )
            external_news_enrichment_followup = stack.enter_context(
                patch(
                    'app.eod.pykrx_pipeline_runner._run_daily_external_news_enrichment_followup',
                    side_effect=lambda **_: followup_order.append('external_news_enrich')
                    or {'triggered': True, 'status': 'completed', 'classifications_persisted': 0},
                )
            )
            macro_context_followup = stack.enter_context(
                patch(
                    'app.eod.pykrx_pipeline_runner._run_daily_macro_context_followup',
                    side_effect=lambda **_: followup_order.append('macro_context_refresh')
                    or {'triggered': True, 'status': 'completed', 'freshness_gate': 'fresh'},
                )
            )
            feedback_followup = stack.enter_context(
                patch(
                    'app.eod.pykrx_pipeline_runner._run_daily_feedback_followup',
                    side_effect=lambda **_: followup_order.append('agent_feedback_summaries')
                    or {'triggered': True, 'status': 'completed'},
                )
            )
            signal_decider_followup = stack.enter_context(
                patch(
                    'app.eod.pykrx_pipeline_runner._run_daily_signal_decider_followup',
                    return_value={'triggered': True, 'status': 'completed', 'decision_snapshots': 2},
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
        external_news_enrichment_followup.assert_called_once_with(market_date='2026-04-03', enabled=True)
        macro_context_followup.assert_called_once_with(market_date='2026-04-03', enabled=True)
        self.assertEqual(
            followup_order[:4],
            [
                'external_news_collect',
                'external_news_enrich',
                'macro_context_refresh',
                'agent_feedback_summaries',
            ],
        )
        feedback_followup.assert_called_once_with(
            market_date='2026-04-03',
            enabled=False,
            progress_callback=ANY,
        )
        signal_decider_followup.assert_called_once_with(
            market_date='2026-04-03',
            enabled=False,
            progress_callback=ANY,
        )
        evaluation_followup.assert_called_once_with(market_date='2026-04-03', enabled=True)
        self.assertEqual(
            result['factor_scoring_followup'],
            _completed_factor_followup_result(),
        )
        self.assertEqual(
            result['external_news_followup'],
            {'triggered': True, 'status': 'disabled', 'reason': 'naver_search_credentials_missing'},
        )
        self.assertEqual(
            result['external_news_enrichment_followup'],
            {'triggered': True, 'status': 'completed', 'classifications_persisted': 0},
        )
        self.assertEqual(
            result['macro_context_followup'],
            {'triggered': True, 'status': 'completed', 'freshness_gate': 'fresh'},
        )
        self.assertEqual(
            result['feedback_followup'],
            {'triggered': True, 'status': 'completed'},
        )
        self.assertEqual(
            result['signal_decider_followup'],
            {'triggered': True, 'status': 'completed', 'decision_snapshots': 2},
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
                    return_value=_completed_factor_followup_result(),
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
                    'app.eod.pykrx_pipeline_runner._run_daily_signal_decider_followup',
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
        self.assertIn('signal_decider', fake_run_state_service.start_runs[0]['resolved_stages'])
        self.assertIn('signal_decider_evaluations', fake_run_state_service.start_runs[0]['resolved_stages'])
        stage_statuses = {
            (item['stage_id'], item['status'])
            for item in fake_run_state_service.stage_updates
        }
        self.assertIn(('fundamentals_daily_refresh', 'running'), stage_statuses)
        self.assertIn(('fundamentals_daily_refresh', 'completed'), stage_statuses)
        self.assertIn(('momentum_factors', 'running'), stage_statuses)
        self.assertIn(('signal_decider', 'blocked'), stage_statuses)
        self.assertIn(('signal_decider_evaluations', 'blocked'), stage_statuses)
        self.assertEqual(fake_run_state_service.finish_runs[-1]['status'], 'blocked')
        self.assertEqual(
            fake_run_state_service.finish_runs[-1]['run_metadata']['eod_collection_run_id'],
            12,
        )
        fundamentals_refresh.assert_called_once()
        self.assertIn('progress_callback', fundamentals_refresh.call_args.kwargs)

    def test_run_collection_fast_fails_when_price_provider_fails_all_symbols(self) -> None:
        service = PykrxBackfillService()
        fake_run_state_service = _FakeDailyPipelineRunStateService()

        with ExitStack() as stack:
            stack.enter_context(
                patch.object(
                    service,
                    'load_active_universe',
                    return_value=[
                        PykrxSymbol(symbol='005930', market='KOSPI', name_ko='Samsung Electronics'),
                        PykrxSymbol(symbol='000660', market='KOSPI', name_ko='SK Hynix'),
                    ],
                )
            )
            stack.enter_context(patch.object(service, 'resolve_latest_trade_date', return_value='2026-04-03'))
            stack.enter_context(
                patch(
                    'app.eod.pykrx_backfill._get_market_ohlcv_by_date',
                    side_effect=RuntimeError('PyKRX stock client is unavailable'),
                )
            )
            stack.enter_context(patch('app.eod.pykrx_backfill.create_eod_collection_run', return_value=12))
            stack.enter_context(patch('app.eod.pykrx_backfill.update_eod_collection_run_progress'))
            complete_run = stack.enter_context(patch('app.eod.pykrx_backfill.complete_eod_collection_run'))
            fundamentals_refresh = stack.enter_context(
                patch('app.eod.pykrx_backfill.dart_fundamentals_capture_service.run_daily_incremental_refresh')
            )
            technical_snapshots = stack.enter_context(
                patch('app.eod.pykrx_backfill.daily_technical_snapshot_service.rebuild_for_market_date')
            )
            action_summaries = stack.enter_context(
                patch('app.eod.pykrx_backfill.daily_action_summary_service.rebuild_for_market_date')
            )
            market_risk = stack.enter_context(
                patch('app.eod.pykrx_backfill.daily_market_risk_filter_service.rebuild_for_market_date')
            )
            stack.enter_context(
                patch('app.eod.pykrx_backfill.daily_pipeline_run_state_service', fake_run_state_service)
            )

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

        self.assertEqual(result['status'], 'failed')
        self.assertEqual(result['error_type'], 'PykrxProviderUnavailable')
        self.assertEqual(result['symbols_succeeded'], 0)
        self.assertEqual(result['symbols_failed'], 2)
        complete_run.assert_called_once()
        self.assertEqual(complete_run.call_args.kwargs['status'], 'failed')
        self.assertIn('provider appears unavailable', complete_run.call_args.kwargs['error_message'])
        fundamentals_refresh.assert_not_called()
        technical_snapshots.assert_not_called()
        action_summaries.assert_not_called()
        market_risk.assert_not_called()
        self.assertEqual(fake_run_state_service.finish_runs[-1]['status'], 'failed')
        price_collection_updates = [
            item
            for item in fake_run_state_service.stage_updates
            if item['stage_id'] == 'price_collection'
        ]
        self.assertEqual(price_collection_updates[-1]['status'], 'failed')
        self.assertEqual(price_collection_updates[-1]['current_step'], 'price_collection_provider_unavailable')

    def test_run_collection_aborts_daily_auto_price_loop_after_initial_provider_failures(self) -> None:
        service = PykrxBackfillService()
        symbols = [
            PykrxSymbol(symbol=str(index).zfill(6), market='KOSPI', name_ko=f'Symbol {index}')
            for index in range(25)
        ]

        with ExitStack() as stack:
            stack.enter_context(patch.object(service, 'load_active_universe', return_value=symbols))
            stack.enter_context(patch.object(service, 'resolve_latest_trade_date', return_value='2026-04-03'))
            price_fetch = stack.enter_context(
                patch(
                    'app.eod.pykrx_backfill._get_market_ohlcv_by_date',
                    side_effect=RuntimeError('PyKRX stock client is unavailable'),
                )
            )
            stack.enter_context(patch('app.eod.pykrx_backfill.create_eod_collection_run', return_value=12))
            update_progress = stack.enter_context(
                patch('app.eod.pykrx_backfill.update_eod_collection_run_progress')
            )
            complete_run = stack.enter_context(patch('app.eod.pykrx_backfill.complete_eod_collection_run'))
            fundamentals_refresh = stack.enter_context(
                patch('app.eod.pykrx_backfill.dart_fundamentals_capture_service.run_daily_incremental_refresh')
            )

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

        self.assertEqual(result['status'], 'failed')
        self.assertEqual(result['symbols_total'], 25)
        self.assertEqual(result['symbols_failed'], 20)
        self.assertEqual(price_fetch.call_count, 20)
        self.assertEqual(complete_run.call_args.kwargs['status'], 'failed')
        self.assertEqual(result['unattempted_symbols'], [f'{index:06d}' for index in range(20, 25)])
        completed_metadata = complete_run.call_args.kwargs['run_metadata']
        self.assertEqual(completed_metadata['requested_symbols'], [item.symbol for item in symbols])
        self.assertEqual(completed_metadata['failed_symbols'], [f'{index:06d}' for index in range(20)])
        self.assertEqual(completed_metadata['unattempted_symbols'], [f'{index:06d}' for index in range(20, 25)])
        latest_progress_metadata = update_progress.call_args.kwargs['run_metadata']
        self.assertEqual(latest_progress_metadata['failed_symbols'], [f'{index:06d}' for index in range(20)])
        self.assertEqual(latest_progress_metadata['unattempted_symbols'], [f'{index:06d}' for index in range(20, 25)])
        fundamentals_refresh.assert_not_called()

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
            include_retry_metadata=True,
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
            signal_decider_followup = stack.enter_context(
                patch(
                    'app.eod.pykrx_pipeline_runner._run_daily_signal_decider_followup',
                    return_value={'triggered': True, 'status': 'completed', 'decision_snapshots': 1},
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
                'status': 'blocked',
                'reason': 'dart_api_key_not_configured',
                'market_date': '2026-04-03',
            },
        )
        self.assertIn('external_news_followup', result)
        signal_decider_followup.assert_not_called()
        evaluation_followup.assert_not_called()
        self.assertEqual(
            result['signal_decider_followup'],
            {
                'triggered': False,
                'status': 'blocked',
                'reason': 'factor_scoring_followup_incomplete',
                'market_date': '2026-04-03',
                'upstream_status': 'blocked',
                'stage': 'signal_decider',
            },
        )
        self.assertEqual(
            result['signal_decider_evaluation_followup'],
            {
                'triggered': False,
                'status': 'blocked',
                'reason': 'factor_scoring_followup_incomplete',
                'market_date': '2026-04-03',
                'upstream_status': 'blocked',
                'stage': 'signal_decider_evaluations',
            },
        )
        fundamentals_refresh.assert_not_called()

    def test_run_market_data_pipeline_keeps_retryable_status_when_snapshot_persistence_has_failures(self) -> None:
        service = PykrxBackfillService()
        symbols = [PykrxSymbol(symbol='005930', market='KOSPI', name_ko='Samsung Electronics')]

        with (
            patch.object(
                service,
                'fetch_recent_universe_snapshot',
                return_value=_point_in_time_universe_snapshot(symbols),
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
        symbols = [PykrxSymbol(symbol='005930', market='KOSPI', name_ko='Samsung Electronics')]

        with (
            patch.object(service, '_persist_universe', return_value=1),
            patch.object(
                service,
                'fetch_recent_universe_snapshot',
                return_value=_point_in_time_universe_snapshot(symbols),
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
            result = service._run_market_data_pipeline(
                start_date='20260403',
                end_date='20260403',
                markets=('KOSPI',),
                trigger='test',
                refresh_universe=True,
                mode='daily_auto',
                selected_stages=(
                    'price_collection',
                    'technical_snapshots',
                    'action_summaries',
                    'market_risk_filter',
                ),
                chain_fundamentals_followup=False,
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

    def test_run_market_data_pipeline_preserves_minor_stage_gaps_as_retryable_errors(self) -> None:
        service = PykrxBackfillService()
        symbols = [
            PykrxSymbol(symbol=f'{index:06d}', market='KOSPI', name_ko=f'Symbol {index}')
            for index in range(1, 21)
        ]
        symbol_codes = [item.symbol for item in symbols]

        with (
            patch.object(service, '_persist_universe', return_value=len(symbols)),
            patch.object(
                service,
                'fetch_recent_universe_snapshot',
                return_value=_point_in_time_universe_snapshot(symbols),
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
            patch('app.eod.pykrx_backfill._get_market_ohlcv_by_date', return_value=SimpleNamespace(empty=False)),
            patch('app.eod.pykrx_backfill.persist_daily_price_bars', return_value=1),
            patch('app.eod.pykrx_backfill.update_eod_collection_run_progress'),
            patch(
                'app.eod.pykrx_backfill.daily_price_context_service.rebuild_for_market_date',
                return_value={'contexts_persisted': 20, 'contexts_failed': 0, 'failed_symbols': []},
            ),
            patch(
                'app.eod.pykrx_backfill.daily_technical_snapshot_service.rebuild_for_market_date',
                return_value={'snapshots_persisted': 20, 'snapshots_failed': 0, 'failed_symbols': []},
            ),
            patch(
                'app.eod.pykrx_backfill.daily_action_summary_service.rebuild_for_market_date',
                return_value={'action_summaries_persisted': 20, 'action_summaries_failed': 0, 'failed_symbols': []},
            ),
            patch(
                'app.eod.pykrx_backfill.compute_and_persist_daily_trending_ranks',
                return_value={'trending_ranks_persisted': 3},
            ),
            patch(
                'app.eod.pykrx_backfill.daily_signal_feature_service.refresh_for_market_date',
                return_value={'features_persisted': 19, 'features_failed': 1, 'failed_symbols': ['000020']},
            ),
            patch(
                'app.eod.pykrx_backfill.daily_market_risk_filter_service.rebuild_for_market_date',
                return_value={
                    'matched_symbols': symbol_codes,
                    'results_persisted': 19,
                    'results_failed': 1,
                    'failed_symbols': ['000020'],
                    'summary': {'passed_count': 19, 'watchlist_count': 0},
                },
            ),
            patch('app.eod.pykrx_backfill.complete_eod_collection_run') as complete_run,
        ):
            result = service._run_market_data_pipeline(
                start_date='20260403',
                end_date='20260403',
                markets=('KOSPI',),
                trigger='test',
                refresh_universe=True,
                mode='daily_auto',
                selected_stages=(
                    'price_collection',
                    'technical_snapshots',
                    'action_summaries',
                    'signal_features',
                    'market_risk_filter',
                ),
                chain_fundamentals_followup=False,
            )

        self.assertEqual(result['status'], 'completed_with_errors')
        self.assertEqual(result['signal_features']['features_failed'], 1)
        self.assertEqual(result['market_risk_filter']['results_failed'], 1)
        self.assertEqual(result['retry_symbols'], ['000020'])
        complete_run.assert_called_once_with(
            12,
            status='completed_with_errors',
            symbols_succeeded=20,
            symbols_failed=0,
            error_message=(
                'Daily signal feature persistence incomplete for symbols: 000020 | '
                'Market risk filter persistence incomplete for symbols: 000020'
            ),
            run_metadata=ANY,
        )

    def test_run_market_data_pipeline_retries_full_failure_scope_beyond_public_preview(self) -> None:
        service = PykrxBackfillService()
        service.symbol_pause_seconds = 0
        symbols = [
            PykrxSymbol(symbol=f'{index:06d}', market='KOSPI', name_ko=f'Symbol {index}')
            for index in range(1, 61)
        ]
        symbol_codes = [item.symbol for item in symbols]

        with (
            patch.object(service, 'load_active_universe', return_value=symbols),
            patch.object(service, 'resolve_latest_trade_date', return_value='2026-04-03'),
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
            ),
            patch('app.eod.pykrx_backfill.create_eod_collection_run', return_value=12),
            patch(
                'app.eod.pykrx_backfill._get_market_ohlcv_by_date',
                return_value=SimpleNamespace(empty=False),
            ),
            patch('app.eod.pykrx_backfill.persist_daily_price_bars', return_value=1),
            patch('app.eod.pykrx_backfill.update_eod_collection_run_progress'),
            patch(
                'app.eod.pykrx_backfill.daily_price_context_service.rebuild_for_market_date',
                return_value={
                    'contexts_persisted': len(symbols),
                    'contexts_failed': 0,
                    'failed_symbols': [],
                    '_retry_metadata': {'failed_symbols': []},
                },
            ),
            patch(
                'app.eod.pykrx_backfill.daily_technical_snapshot_service.rebuild_for_market_date',
                return_value={
                    'snapshots_persisted': 0,
                    'snapshots_failed': len(symbols),
                    'failed_symbols': symbol_codes[:50],
                    '_retry_metadata': {'failed_symbols': symbol_codes},
                },
            ),
            patch('app.eod.pykrx_backfill.complete_eod_collection_run') as complete_run,
        ):
            result = service._run_market_data_pipeline(
                start_date='20260403',
                end_date='20260403',
                markets=('KOSPI',),
                trigger='test',
                refresh_universe=False,
                mode='daily_auto',
                selected_stages=('price_collection', 'technical_snapshots'),
                chain_fundamentals_followup=False,
            )

        self.assertEqual(result['status'], 'completed_with_errors')
        self.assertEqual(result['retry_symbols_count'], 60)
        self.assertEqual(result['retry_symbols'], symbol_codes[:50])
        self.assertEqual(result['technical_snapshots']['failed_symbols'], symbol_codes[:50])
        self.assertNotIn('_retry_metadata', result['technical_snapshots'])
        completed_metadata = complete_run.call_args.kwargs['run_metadata']
        self.assertEqual(completed_metadata['failed_symbols'], [])
        self.assertEqual(completed_metadata['failed_symbols_count'], 0)
        self.assertEqual(completed_metadata['downstream_retry_symbols'], symbol_codes)
        self.assertEqual(completed_metadata['downstream_retry_symbols_count'], 60)
        self.assertEqual(result['price_retry_symbols_count'], 0)
        self.assertEqual(result['downstream_retry_symbols_count'], 60)

    def test_run_market_data_pipeline_separates_recovered_enrichment_from_unresolved_failures(self) -> None:
        service = PykrxBackfillService()
        symbol = PykrxSymbol(symbol='005930', market='KOSPI', name_ko='Samsung Electronics')
        enrichment_result = _ready_pykrx_enrichment_result()
        enrichment_result['status'] = 'recovered'
        enrichment_result['categories']['market_breadth']['errors'] = [
            {'market': 'KOSPI', 'error': 'provider timeout'}
        ]

        with (
            patch.object(service, 'load_active_universe', return_value=[symbol]),
            patch.object(service, 'resolve_latest_trade_date', return_value='2026-04-03'),
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
            ),
            patch('app.eod.pykrx_backfill.create_eod_collection_run', return_value=12),
            patch(
                'app.eod.pykrx_backfill._get_market_ohlcv_by_date',
                return_value=SimpleNamespace(empty=False),
            ),
            patch('app.eod.pykrx_backfill.persist_daily_price_bars', return_value=1),
            patch('app.eod.pykrx_backfill.update_eod_collection_run_progress'),
            patch(
                'app.eod.pykrx_backfill.daily_price_context_service.rebuild_for_market_date',
                return_value={
                    'contexts_persisted': 1,
                    'contexts_failed': 0,
                    'failed_symbols': [],
                    '_retry_metadata': {'failed_symbols': []},
                },
            ),
            patch(
                'app.eod.pykrx_backfill.pykrx_enrichment_service.collect_for_market_date',
                return_value=enrichment_result,
            ),
            patch(
                'app.eod.pykrx_backfill.compute_and_persist_daily_trending_ranks',
                return_value={'periods_completed': 2, 'periods_failed': 1},
            ),
            patch(
                'app.eod.pykrx_backfill.daily_signal_feature_service.refresh_for_market_date',
                return_value={
                    'features_persisted': 1,
                    'features_failed': 0,
                    'failed_symbols': [],
                    'source_errors': {'volume-power': 'provider timeout'},
                    '_retry_metadata': {'failed_symbols': []},
                },
            ),
            patch('app.eod.pykrx_backfill.complete_eod_collection_run') as complete_run,
        ):
            result = service._run_market_data_pipeline(
                start_date='20260403',
                end_date='20260403',
                markets=('KOSPI',),
                trigger='test',
                refresh_universe=False,
                mode='daily_auto',
                selected_stages=(
                    'price_collection',
                    'pykrx_enrichment',
                    'trending_ranks',
                    'signal_features',
                ),
                chain_fundamentals_followup=False,
            )

        self.assertEqual(result['status'], 'completed_with_errors')
        self.assertEqual(result['retry_symbols'], ['005930'])
        self.assertEqual(result['pykrx_enrichment']['recovered_categories'], ['market_breadth'])
        self.assertEqual(result['pykrx_enrichment']['unresolved_categories'], [])
        self.assertNotIn('_retry_metadata', result['pykrx_enrichment'])
        self.assertNotIn('_retry_metadata', result['trending_ranks'])
        self.assertNotIn('_retry_metadata', result['signal_features'])
        self.assertEqual(
            complete_run.call_args.kwargs['error_message'],
            'Trending rank persistence incomplete: 1 period(s) failed | '
            'Daily signal feature sources incomplete: volume-power',
        )
        self.assertEqual(
            complete_run.call_args.kwargs['run_metadata']['downstream_retry_symbols'],
            ['005930'],
        )
        self.assertEqual(
            complete_run.call_args.kwargs['run_metadata']['failed_symbols'],
            [],
        )

    def test_run_market_data_pipeline_preserves_price_collection_counts_in_result(self) -> None:
        service = PykrxBackfillService()
        symbols = [PykrxSymbol(symbol='005930', market='KOSPI', name_ko='Samsung Electronics')]

        with (
            patch.object(
                service,
                'fetch_recent_universe_snapshot',
                return_value=_point_in_time_universe_snapshot(symbols),
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
            patch.object(service, '_resume_auto_daily_followups_if_due', return_value=None),
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

    def test_retry_scope_unions_attempted_failures_and_unattempted_outage_tail(self) -> None:
        service = PykrxBackfillService()
        run = {
            'id': 7,
            'status': 'failed',
            'run_metadata': {
                'requested_symbols': ['000001', '000002', '000003', '000004'],
                'failed_symbols': ['000001', '000002'],
                'unattempted_symbols': ['000003', '000004'],
            },
        }

        with patch('app.eod.pykrx_backfill.load_eod_run_persisted_symbols') as persisted:
            retry_codes = service._failed_symbol_codes_for_run(run, markets=('KOSPI',))

        self.assertEqual(retry_codes, ['000001', '000002', '000003', '000004'])
        persisted.assert_not_called()

    def test_auto_daily_retries_only_minor_symbol_gap(self) -> None:
        service = PykrxBackfillService()
        retry_symbols = [
            PykrxSymbol(symbol=symbol, market='KOSPI', name_ko=symbol)
            for symbol in ('082640', '489010', '096610', '471050')
        ]

        with (
            patch.object(service, 'resolve_latest_trade_date', return_value='2026-04-03'),
            patch('app.eod.pykrx_backfill.advisory_eod_collection_lock', return_value=_lock()),
            patch(
                'app.eod.pykrx_backfill.load_latest_eod_collection_run',
                return_value={
                    'id': 7,
                    'status': 'completed_with_errors',
                    'symbols_total': 3552,
                    'symbols_failed': 4,
                    'run_metadata': {'failed_symbols': ['082640', '489010', '096610', '471050']},
                },
            ),
            patch('app.eod.pykrx_backfill.count_eod_collection_runs', return_value=1) as count_runs,
            patch.object(service, '_load_retry_symbols_for_run', return_value=retry_symbols),
            patch.object(
                service,
                '_run_market_data_pipeline',
                return_value={'run_id': 8, 'source': 'pykrx', 'status': 'completed'},
            ) as run_collection,
        ):
            result = service.run_daily_pipeline_if_due(
                market_date='2026-04-03',
                trigger='auto_close_scheduler',
            )

        self.assertTrue(result['triggered'])
        count_runs.assert_called_once()
        kwargs = run_collection.call_args.kwargs
        self.assertEqual(kwargs['symbols_override'], retry_symbols)
        self.assertFalse(kwargs['refresh_universe'])
        self.assertEqual(kwargs['retry_context']['symbol_count'], 4)

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

    def test_manual_daily_triggers_retry_minor_symbol_gaps_with_full_collection(self) -> None:
        service = PykrxBackfillService()

        with (
            patch.object(service, 'resolve_latest_trade_date', return_value='2026-04-03'),
            patch(
                'app.eod.pykrx_backfill.advisory_eod_collection_lock',
                side_effect=[_lock(), _lock()],
            ),
            patch(
                'app.eod.pykrx_backfill.load_latest_eod_collection_run',
                return_value={
                    'id': 7,
                    'status': 'completed_with_errors',
                    'symbols_total': 3552,
                    'symbols_failed': 4,
                    'run_metadata': {'failed_symbols': ['082640', '489010', '096610', '471050']},
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
            results = [
                service.run_daily_pipeline_if_due(market_date='2026-04-03', trigger=trigger)
                for trigger in ('manual_daily_api', 'internal_ops_cli')
            ]

        self.assertTrue(all(result['triggered'] for result in results))
        count_runs.assert_not_called()
        self.assertEqual(run_collection.call_count, 2)
        for call in run_collection.call_args_list:
            self.assertIsNone(call.kwargs['symbols_override'])
            self.assertIsNone(call.kwargs['retry_context'])
            self.assertTrue(call.kwargs['refresh_universe'])

    def test_run_market_data_pipeline_limits_symbol_stages_to_fresh_price_scope(self) -> None:
        service = PykrxBackfillService()
        service.symbol_pause_seconds = 0
        symbols = [
            PykrxSymbol(symbol='005930', market='KOSPI', name_ko='Samsung Electronics'),
            PykrxSymbol(symbol='472220', market='KOSPI', name_ko='Missing Price'),
        ]

        def price_frame(_start: str, _end: str, symbol: str) -> SimpleNamespace:
            return SimpleNamespace(empty=symbol == '472220')

        with (
            patch.object(service, 'load_active_universe', return_value=symbols),
            patch.object(service, 'resolve_latest_trade_date', return_value='2026-04-03'),
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
            ),
            patch('app.eod.pykrx_backfill.create_eod_collection_run', return_value=12),
            patch('app.eod.pykrx_backfill._get_market_ohlcv_by_date', side_effect=price_frame),
            patch(
                'app.eod.pykrx_pipeline_runner._enrich_bars_with_market_turnover',
                side_effect=lambda **kwargs: kwargs['bars'],
            ),
            patch('app.eod.pykrx_backfill.persist_daily_price_bars', return_value=1),
            patch('app.eod.pykrx_backfill.update_eod_collection_run_progress'),
            patch(
                'app.eod.pykrx_backfill.daily_price_context_service.rebuild_for_market_date',
                return_value={
                    'requested_symbols': 1,
                    'contexts_persisted': 1,
                    'contexts_failed': 0,
                    '_retry_metadata': {'failed_symbols': []},
                },
            ) as price_contexts,
            patch(
                'app.eod.pykrx_backfill.daily_technical_snapshot_service.rebuild_for_market_date',
                return_value={
                    'requested_symbols': 1,
                    'snapshots_persisted': 1,
                    'snapshots_failed': 0,
                    '_retry_metadata': {'failed_symbols': []},
                },
            ) as technical_snapshots,
            patch(
                'app.eod.pykrx_backfill.daily_action_summary_service.rebuild_for_market_date',
                return_value={
                    'requested_symbols': 1,
                    'action_summaries_persisted': 1,
                    'action_summaries_failed': 0,
                    '_retry_metadata': {'failed_symbols': []},
                },
            ) as action_summaries,
            patch('app.eod.pykrx_backfill.complete_eod_collection_run') as complete_run,
        ):
            result = service._run_market_data_pipeline(
                start_date='20260403',
                end_date='20260403',
                markets=('KOSPI',),
                trigger='test',
                refresh_universe=False,
                mode='daily_auto',
                selected_stages=('price_collection', 'technical_snapshots', 'action_summaries'),
                chain_fundamentals_followup=False,
            )

        for stage in (price_contexts, technical_snapshots, action_summaries):
            self.assertEqual(stage.call_args.kwargs['symbols'], ['005930'])
        self.assertEqual(result['status'], 'completed_with_errors')
        self.assertEqual(result['price_retry_symbols'], ['472220'])
        self.assertEqual(result['downstream_retry_symbols'], [])
        self.assertEqual(result['technical_snapshots']['causally_skipped_symbols'], ['472220'])
        completed_metadata = complete_run.call_args.kwargs['run_metadata']
        self.assertEqual(completed_metadata['failed_symbols'], ['472220'])
        self.assertEqual(completed_metadata['downstream_retry_symbols'], [])

    def test_run_market_data_pipeline_skips_symbol_stages_when_no_price_was_collected(self) -> None:
        service = PykrxBackfillService()
        symbols = [PykrxSymbol(symbol='005930', market='KOSPI', name_ko='Samsung Electronics')]

        with (
            patch('app.eod.pykrx_backfill.log_fetch_event') as log_fetch_event,
            patch.object(
                service,
                'fetch_recent_universe_snapshot',
                return_value=_point_in_time_universe_snapshot(symbols),
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
        price_contexts.assert_not_called()
        technical_snapshots.assert_not_called()
        action_summaries.assert_not_called()
        self.assertEqual(result['price_contexts']['reason'], 'no_fresh_price_rows')
        self.assertEqual(result['technical_snapshots']['reason'], 'no_fresh_price_rows')
        self.assertEqual(result['action_summaries']['reason'], 'no_fresh_price_rows')
        self.assertEqual(result['technical_snapshots']['causally_skipped_symbols'], ['005930'])
        self.assertTrue(
            any(
                call.kwargs.get('operation') == 'fetch_symbol_ohlcv'
                and call.kwargs.get('status') == 'failed'
                and call.kwargs.get('symbol') == '005930'
                for call in log_fetch_event.call_args_list
            )
        )


class AutoPykrxEODSchedulerTests(IsolatedAsyncioTestCase):
    def test_automatic_market_default_excludes_unreliable_konex_feed(self) -> None:
        field = Settings.model_fields['eod_auto_pykrx_markets']

        self.assertEqual(field.default, 'KOSPI,KOSDAQ')

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

    async def test_schedule_daily_pipeline_if_due_uses_latest_session_on_weekend(self) -> None:
        scheduler = AutoPykrxEODScheduler()
        market = MarketState(
            market='KRX',
            state='WEEKEND',
            is_trading_day=False,
            is_open=False,
            server_time='2026-09-13T14:00:00+09:00',
            session_open_at=None,
            session_close_at=None,
            latest_valid_trading_date='2026-09-11',
        )

        with (
            patch(
                'app.eod.auto_pykrx.async_get_market_state',
                return_value=market,
            ),
            patch.object(
                scheduler,
                '_run_scheduled_daily_pipeline',
                new=AsyncMock(return_value=None),
            ) as run_pipeline,
        ):
            await scheduler.schedule_daily_pipeline_if_due()
            self.assertIsNotNone(scheduler._task)
            await scheduler._task

        run_pipeline.assert_awaited_once_with(
            '2026-09-11',
            markets=('KOSPI', 'KOSDAQ'),
        )
        self.assertEqual(scheduler.snapshot()['last_market_date'], '2026-09-11')

    async def test_triggered_failed_pipeline_remains_failed_in_scheduler_status(self) -> None:
        scheduler = AutoPykrxEODScheduler()

        with patch(
            'app.eod.auto_pykrx.PYKRX_BACKFILL_SERVICE.run_daily_pipeline_if_due',
            return_value={
                'triggered': True,
                'status': 'failed',
                'reason': 'price_collection_failed',
            },
        ):
            await scheduler._run_scheduled_daily_pipeline(
                '2026-09-11',
                markets=('KOSPI', 'KOSDAQ'),
            )

        self.assertEqual(scheduler.snapshot()['last_status'], 'failed')

    async def test_triggered_completed_with_errors_pipeline_is_partial(self) -> None:
        scheduler = AutoPykrxEODScheduler()

        with patch(
            'app.eod.auto_pykrx.PYKRX_BACKFILL_SERVICE.run_daily_pipeline_if_due',
            return_value={
                'triggered': True,
                'status': 'completed_with_errors',
            },
        ):
            await scheduler._run_scheduled_daily_pipeline(
                '2026-09-11',
                markets=('KOSPI', 'KOSDAQ'),
            )

        self.assertEqual(scheduler.snapshot()['last_status'], 'partial')

    async def test_incomplete_universe_preflight_uses_backoff_then_retries(self) -> None:
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
        incomplete_error = RuntimeError(
            'PyKRX universe snapshot is incomplete for requested markets: KONEX.'
        )

        with (
            patch('app.eod.auto_pykrx.async_get_market_state', return_value=market),
            patch(
                'app.eod.auto_pykrx.PYKRX_BACKFILL_SERVICE.run_daily_pipeline_if_due',
                side_effect=[
                    incomplete_error,
                    {'triggered': False, 'reason': 'already_completed'},
                ],
            ) as run_daily,
            patch(
                'app.eod.auto_pykrx.settings.eod_auto_pykrx_markets',
                'KOSPI,KOSDAQ,KONEX',
            ),
        ):
            await scheduler.schedule_daily_pipeline_if_due()
            self.assertIsNotNone(scheduler._task)
            await scheduler._task

            await scheduler.schedule_daily_pipeline_if_due()

            self.assertEqual(run_daily.call_count, 1)
            self.assertEqual(scheduler.snapshot()['last_status'], 'blocked')
            self.assertEqual(
                scheduler.snapshot()['last_reason'],
                'incomplete_universe_preflight_backoff',
            )

            scheduler._state.preflight_retry_not_before = (
                datetime.now(timezone.utc) - timedelta(seconds=1)
            )
            await scheduler.schedule_daily_pipeline_if_due()
            self.assertIsNotNone(scheduler._task)
            await scheduler._task

        self.assertEqual(run_daily.call_count, 2)
