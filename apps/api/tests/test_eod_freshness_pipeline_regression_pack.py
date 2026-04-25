import json
import unittest
from datetime import date, datetime, timezone
from unittest.mock import patch

from sqlalchemy import delete, select

from app.core.settings import settings
from app.dart.fundamentals_store import DartFundamentalsRepository
from app.db import session_scope
from app.db_models import DartFundamentalsRefreshRun
from app.eod import freshness as freshness_module
from app.eod.freshness import EODFreshnessService, LATEST_AVAILABLE_FUNDAMENTALS_RUN_STATUSES
from app.eod.freshness_repository import (
    _load_latest_available_fundamentals_refresh_run,
    _load_latest_fundamentals_refresh_run,
    _load_today_fundamentals_refresh_run,
)
from app.eod.pipeline_status import EODPipelineStatusService, _load_latest_pipeline_pykrx_run


class _FakeMarketState:
    server_time = '2026-04-10T18:05:00+09:00'


class _FakeNextTradeDayMarketState:
    server_time = '2026-04-13T10:05:00+09:00'


class _FakeWeekendMarketState:
    server_time = '2026-04-11T12:05:00+09:00'


class _FakePipelineMarketState:
    market = 'KRX'
    state = 'CLOSED'
    is_trading_day = True
    is_open = False
    reason = None
    server_time = '2026-04-21T18:15:00+09:00'


class _FakeDetachedRunRegressionMarketState:
    server_time = '2199-12-31T18:05:00+09:00'


def _latest_eod_run(
    *,
    status: str,
    symbols_total: int,
    symbols_succeeded: int,
    symbols_failed: int,
    started_at: str = '2026-04-10T06:00:00+00:00',
    completed_at: str | None = '2026-04-10T06:31:55+00:00',
    run_metadata: dict | None = None,
    run_id: int = 1,
    trade_date: str = '2026-04-10',
    trigger: str = 'manual_daily_api',
    error_message: str | None = None,
) -> dict:
    return {
        'id': run_id,
        'market': 'KRX',
        'trade_date': trade_date,
        'source': 'pykrx',
        'trigger': trigger,
        'status': status,
        'symbols_total': symbols_total,
        'symbols_succeeded': symbols_succeeded,
        'symbols_failed': symbols_failed,
        'error_message': error_message,
        'started_at': started_at,
        'completed_at': completed_at,
        'run_metadata': run_metadata,
    }


def _fundamentals_refresh_run(
    *,
    market_date: str,
    status: str,
    symbols_total: int,
    symbols_checked: int,
    symbols_fresh: int = 0,
    symbols_refreshed: int = 0,
    symbols_failed: int = 0,
    started_at: str = '2026-04-10T06:00:00+00:00',
    completed_at: str | None = '2026-04-10T06:31:55+00:00',
    fs_div: str = 'CFS',
    trigger: str = 'eod_pykrx_followup',
    run_metadata: dict | None = None,
    events_detected: int = 0,
    events_captured: int = 0,
    events_failed: int = 0,
    error_message: str | None = None,
) -> dict:
    return {
        'market_date': market_date,
        'status': status,
        'fs_div': fs_div,
        'trigger': trigger,
        'symbols_total': symbols_total,
        'symbols_checked': symbols_checked,
        'symbols_fresh': symbols_fresh,
        'symbols_refreshed': symbols_refreshed,
        'symbols_failed': symbols_failed,
        'events_detected': events_detected,
        'events_captured': events_captured,
        'events_failed': events_failed,
        'error_message': error_message,
        'started_at': started_at,
        'completed_at': completed_at,
        'run_metadata': run_metadata,
    }


class EODFreshnessRegressionPackTests(unittest.TestCase):
    def test_latest_run_selection_prefers_daily_run_over_newer_historical_backfill_row(self):
        historical_run = _latest_eod_run(
            status='completed',
            symbols_total=100,
            symbols_succeeded=100,
            symbols_failed=0,
            started_at='2026-04-10T07:00:00+00:00',
            completed_at='2026-04-10T08:00:00+00:00',
            run_metadata={
                'mode': 'historical_backfill',
                'last_progress_at': '2026-04-10T07:59:00+00:00',
            },
            run_id=500,
            trigger='manual_backfill_api',
        )
        daily_run = _latest_eod_run(
            status='completed',
            symbols_total=3,
            symbols_succeeded=2,
            symbols_failed=1,
            started_at='2026-04-10T06:00:00+00:00',
            completed_at='2026-04-10T06:45:00+00:00',
            run_metadata={
                'mode': 'daily_auto',
                'last_progress_at': '2026-04-10T06:44:00+00:00',
            },
            run_id=501,
            trigger='auto_close_scheduler',
        )
        service = EODFreshnessService(
            market_state_loader=lambda: _FakeMarketState(),
            latest_run_loader=lambda **_: historical_run,
            latest_run_rows_loader=lambda **_: [historical_run, daily_run],
            live_symbol_count_loader=lambda: 3,
            daily_price_bar_count_loader=lambda _: 2,
            daily_technical_snapshot_count_loader=lambda _: 0,
            daily_action_summary_count_loader=lambda _: 0,
            daily_fundamentals_count_loader=lambda _: {'symbols_succeeded': 0, 'symbols_failed': 0, 'symbols_total': 0},
            fundamentals_today_refresh_loader=lambda _: None,
            latest_fundamentals_refresh_loader=lambda: None,
            latest_price_trade_date_loader=lambda: '2026-04-10',
            latest_price_bar_count_loader=lambda _: 2,
            latest_technical_market_date_loader=lambda: None,
            latest_action_market_date_loader=lambda: None,
        )

        result = service.read()

        self.assertEqual(result['today']['price']['symbols_total'], 3)
        self.assertEqual(result['latest']['trade_date'], '2026-04-10')
        self.assertEqual(result['latest']['symbols_total'], 3)
        self.assertEqual(result['latest']['symbols_succeeded'], 2)
        self.assertEqual(result['latest']['symbols_failed'], 1)
        self.assertEqual(result['latest']['updated_at'], '2026-04-10T06:44:00+00:00')
        self.assertEqual(result['latest']['summary']['basis'], 'latest_eod_run_symbol_universe')

    def test_today_vs_latest_semantics_keep_today_missing_while_latest_stays_on_previous_trade_date(self):
        previous_fundamentals_run = _fundamentals_refresh_run(
            market_date='2026-04-09',
            status='completed',
            symbols_total=120,
            symbols_checked=120,
            symbols_fresh=119,
            symbols_refreshed=1,
            completed_at='2026-04-09T07:45:00+00:00',
        )

        def latest_run_loader(**kwargs):
            if kwargs['trade_date'] == '2026-04-09':
                return _latest_eod_run(
                    status='completed',
                    symbols_total=3584,
                    symbols_succeeded=3581,
                    symbols_failed=3,
                    completed_at='2026-04-09T07:31:55+00:00',
                    trade_date='2026-04-09',
                )
            return None

        service = EODFreshnessService(
            market_state_loader=lambda: _FakeMarketState(),
            latest_run_loader=latest_run_loader,
            live_symbol_count_loader=lambda: 3584,
            daily_price_bar_count_loader=lambda _: 0,
            daily_technical_snapshot_count_loader=lambda _: 0,
            daily_action_summary_count_loader=lambda _: 0,
            daily_fundamentals_count_loader=lambda _: {'symbols_succeeded': 0, 'symbols_failed': 0},
            fundamentals_today_refresh_loader=lambda _: None,
            latest_fundamentals_refresh_loader=lambda: previous_fundamentals_run,
            latest_price_trade_date_loader=lambda: '2026-04-09',
            latest_price_bar_count_loader=lambda _: 3581,
            latest_technical_market_date_loader=lambda: '2026-04-09',
            latest_action_market_date_loader=lambda: '2026-04-09',
        )

        result = service.read()

        self.assertEqual(result['today']['trade_date'], '2026-04-10')
        self.assertEqual(result['today']['price']['status'], 'missing')
        self.assertEqual(result['today']['price']['symbols_total'], 3584)
        self.assertEqual(result['latest']['trade_date'], '2026-04-09')
        self.assertEqual(result['latest']['price_trade_date'], '2026-04-09')
        self.assertEqual(result['latest']['eod_status'], 'completed_with_errors')
        self.assertEqual(result['latest']['fundamentals']['market_date'], '2026-04-09')
        self.assertEqual(result['latest']['fundamentals']['symbols_succeeded'], 120)

    def test_running_override_keeps_today_fundamentals_running_and_latest_on_latest_available_refresh(self):
        running_today_refresh = _fundamentals_refresh_run(
            market_date='2026-04-10',
            status='running',
            symbols_total=100,
            symbols_checked=12,
            symbols_fresh=7,
            symbols_refreshed=5,
            symbols_failed=1,
            completed_at=None,
            run_metadata={'last_progress_at': '2026-04-10T06:20:00+00:00'},
            events_detected=14,
            events_captured=6,
            events_failed=1,
        )
        latest_completed_refresh = _fundamentals_refresh_run(
            market_date='2026-04-09',
            status='completed',
            symbols_total=100,
            symbols_checked=100,
            symbols_fresh=97,
            symbols_refreshed=3,
            completed_at='2026-04-09T07:40:00+00:00',
            run_metadata={'last_progress_at': '2026-04-09T07:40:00+00:00'},
        )
        service = EODFreshnessService(
            market_state_loader=lambda: _FakeMarketState(),
            latest_run_loader=lambda **_: _latest_eod_run(
                status='running',
                symbols_total=100,
                symbols_succeeded=12,
                symbols_failed=1,
                completed_at=None,
                run_metadata={'last_progress_at': '2026-04-10T06:15:00+00:00'},
            ),
            live_symbol_count_loader=lambda: 100,
            daily_price_bar_count_loader=lambda _: 12,
            daily_technical_snapshot_count_loader=lambda _: 0,
            daily_action_summary_count_loader=lambda _: 0,
            daily_fundamentals_count_loader=lambda _: {'symbols_succeeded': 5, 'symbols_failed': 1, 'symbols_total': 5},
            fundamentals_today_refresh_loader=lambda _: running_today_refresh,
            latest_fundamentals_refresh_loader=lambda: latest_completed_refresh,
            latest_price_trade_date_loader=lambda: '2026-04-10',
            latest_price_bar_count_loader=lambda _: 12,
            latest_technical_market_date_loader=lambda: '2026-04-09',
            latest_action_market_date_loader=lambda: '2026-04-09',
        )

        result = service.read()

        self.assertEqual(result['today']['fundamentals']['status'], 'running')
        self.assertEqual(result['today']['fundamentals']['current_run']['status'], 'running')
        self.assertEqual(result['today']['fundamentals']['current_run']['symbols_succeeded'], 12)
        self.assertEqual(result['latest']['eod_status'], 'running')
        self.assertEqual(result['latest']['fundamentals']['market_date'], '2026-04-09')
        self.assertEqual(result['latest']['fundamentals']['status'], 'completed')
        self.assertEqual(result['latest']['fundamentals']['symbols_refreshed'], 3)

    def test_weekend_break_day_keeps_today_missing_while_latest_stays_on_previous_trade_date(self):
        friday_refresh = _fundamentals_refresh_run(
            market_date='2026-04-10',
            status='completed',
            symbols_total=250,
            symbols_checked=250,
            symbols_fresh=250,
            completed_at='2026-04-10T07:45:00+00:00',
            run_metadata={'last_progress_at': '2026-04-10T07:45:00+00:00'},
        )
        service = EODFreshnessService(
            market_state_loader=lambda: _FakeWeekendMarketState(),
            latest_run_loader=lambda **_: None,
            live_symbol_count_loader=lambda: 250,
            daily_price_bar_count_loader=lambda _: 0,
            daily_technical_snapshot_count_loader=lambda _: 0,
            daily_action_summary_count_loader=lambda _: 0,
            daily_fundamentals_count_loader=lambda _: {'symbols_succeeded': 0, 'symbols_failed': 0, 'symbols_total': 0},
            fundamentals_today_refresh_loader=lambda _: None,
            latest_fundamentals_refresh_loader=lambda: friday_refresh,
            latest_price_trade_date_loader=lambda: '2026-04-10',
            latest_price_bar_count_loader=lambda _: 250,
            latest_price_bar_updated_at_loader=lambda _: '2026-04-10T06:05:12+00:00',
            latest_technical_market_date_loader=lambda: '2026-04-10',
            latest_action_market_date_loader=lambda: '2026-04-10',
        )

        result = service.read()

        self.assertEqual(result['today']['trade_date'], '2026-04-11')
        self.assertEqual(result['today']['price']['status'], 'missing')
        self.assertEqual(result['today']['technical_analysis']['status'], 'missing')
        self.assertEqual(result['today']['action_summary']['status'], 'missing')
        self.assertEqual(result['today']['fundamentals']['status'], 'missing')
        self.assertEqual(result['latest']['trade_date'], '2026-04-10')
        self.assertEqual(result['latest']['price_trade_date'], '2026-04-10')
        self.assertEqual(result['latest']['eod_status'], 'completed')
        self.assertEqual(result['latest']['fundamentals']['market_date'], '2026-04-10')

    def test_latest_available_fundamentals_stays_available_on_next_trade_day(self):
        friday_refresh = _fundamentals_refresh_run(
            market_date='2026-04-10',
            status='completed',
            symbols_total=250,
            symbols_checked=250,
            symbols_fresh=249,
            symbols_refreshed=1,
            completed_at='2026-04-10T07:45:00+00:00',
            run_metadata={'last_progress_at': '2026-04-10T07:45:00+00:00'},
        )
        service = EODFreshnessService(
            market_state_loader=lambda: _FakeNextTradeDayMarketState(),
            latest_run_loader=lambda **_: None,
            live_symbol_count_loader=lambda: 250,
            daily_price_bar_count_loader=lambda _: 0,
            daily_technical_snapshot_count_loader=lambda _: 0,
            daily_action_summary_count_loader=lambda _: 0,
            daily_fundamentals_count_loader=lambda _: {'symbols_succeeded': 0, 'symbols_failed': 0, 'symbols_total': 0},
            fundamentals_today_refresh_loader=lambda _: None,
            latest_fundamentals_refresh_loader=lambda: friday_refresh,
            latest_price_trade_date_loader=lambda: '2026-04-10',
            latest_price_bar_count_loader=lambda _: 250,
            latest_price_bar_updated_at_loader=lambda _: '2026-04-10T06:05:12+00:00',
            latest_technical_market_date_loader=lambda: '2026-04-10',
            latest_action_market_date_loader=lambda: '2026-04-10',
        )

        result = service.read()

        self.assertEqual(result['today']['trade_date'], '2026-04-13')
        self.assertEqual(result['today']['fundamentals']['status'], 'missing')
        self.assertEqual(result['latest']['fundamentals']['market_date'], '2026-04-10')
        self.assertEqual(result['latest']['fundamentals']['status'], 'completed')
        self.assertEqual(result['latest']['fundamentals']['symbols_total'], 250)
        self.assertEqual(result['latest']['fundamentals']['symbols_succeeded'], 250)


class EODPipelineStatusRegressionPackTests(unittest.TestCase):
    def test_latest_pykrx_pipeline_run_selection_prefers_daily_run_and_reconciles_running_row(self):
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
            'run_metadata': {'mode': 'historical_backfill'},
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

    def test_pipeline_status_preserves_running_stage_progress_and_fundamentals_activity_mapping(self):
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
            market_state_loader=lambda: _FakePipelineMarketState(),
            pykrx_run_loader=lambda **_: pykrx_run,
            pykrx_stage_counts_loader=lambda *_: stage_counts,
            live_symbol_count_loader=lambda: 3579,
            fundamentals_refresh_loader=lambda **_: fundamentals_run,
        )

        result = service.read()

        self.assertEqual(result['status'], 'running')
        self.assertEqual(result['stages']['price_collection']['status'], 'completed')
        self.assertEqual(result['stages']['technical_snapshots']['status'], 'running')
        self.assertEqual(result['stages']['technical_snapshots']['current_symbol'], '000660')
        self.assertEqual(result['stages']['technical_snapshots']['processed'], 1)
        self.assertEqual(result['stages']['fundamentals_daily_refresh']['status'], 'running')
        self.assertEqual(
            result['stages']['fundamentals_daily_refresh']['current_activity'],
            'Capturing DART fundamentals for 75 events, retry cycle 2/3, batch 1/2',
        )
        self.assertEqual(result['stages']['fundamentals_daily_refresh']['source_run_ids']['capture_run_ids'], [699, 700])


class FundamentalsRefreshRegressionPackTests(unittest.TestCase):
    def setUp(self):
        self.repository = DartFundamentalsRepository()
        self.created_refresh_run_ids: list[int] = []
        self.fs_div = settings.dart_fundamentals_default_fs_div.strip().upper()
        self.trigger = 'test_issue_189_regression_pack'

    def tearDown(self):
        if not self.created_refresh_run_ids:
            return
        with session_scope() as session:
            session.execute(
                delete(DartFundamentalsRefreshRun).where(
                    DartFundamentalsRefreshRun.id.in_(self.created_refresh_run_ids)
                )
            )

    def _create_refresh_run(
        self,
        *,
        market_date: date,
        status: str,
        started_at: datetime,
        completed_at: datetime | None,
        last_progress_at: str,
        fs_div: str | None = None,
        symbols_total: int = 25,
        symbols_checked: int = 20,
        symbols_missing: int = 2,
        symbols_stale: int = 4,
        symbols_fresh: int = 16,
        symbols_refreshed: int = 3,
        symbols_failed: int = 1,
        events_detected: int = 5,
        events_captured: int = 3,
        events_failed: int = 1,
        error_message: str | None = None,
    ) -> int:
        resolved_fs_div = fs_div or self.fs_div
        run_metadata = {
            'symbols_selected': symbols_total,
            'last_progress_at': last_progress_at,
        }
        run_id = self.repository.create_refresh_run(
            market_date=market_date,
            trigger=self.trigger,
            fs_div=resolved_fs_div,
            force=False,
            symbol_limit=25,
            symbol_offset=0,
            symbols_total=symbols_total,
            run_metadata=run_metadata,
        )
        self.created_refresh_run_ids.append(run_id)

        if status == 'running':
            self.repository.update_refresh_run_progress(
                run_id,
                symbols_checked=symbols_checked,
                symbols_missing=symbols_missing,
                symbols_stale=symbols_stale,
                symbols_fresh=symbols_fresh,
                symbols_refreshed=symbols_refreshed,
                symbols_failed=symbols_failed,
                events_detected=events_detected,
                events_captured=events_captured,
                events_failed=events_failed,
                run_metadata=run_metadata,
            )
        else:
            self.repository.complete_refresh_run(
                run_id,
                status=status,
                symbols_checked=symbols_checked,
                symbols_missing=symbols_missing,
                symbols_stale=symbols_stale,
                symbols_fresh=symbols_fresh,
                symbols_refreshed=symbols_refreshed,
                symbols_failed=symbols_failed,
                events_detected=events_detected,
                events_captured=events_captured,
                events_failed=events_failed,
                error_message=error_message,
                run_metadata=run_metadata,
            )

        with session_scope() as session:
            row = session.execute(
                select(DartFundamentalsRefreshRun).where(DartFundamentalsRefreshRun.id == run_id)
            ).scalar_one()
            row.started_at = started_at
            row.completed_at = completed_at
            row.error_message = error_message

        return run_id

    def test_refresh_loader_preserves_today_vs_latest_available_semantics(self):
        self._create_refresh_run(
            market_date=date(2199, 12, 31),
            status='running',
            started_at=datetime(2199, 12, 31, 8, 0, tzinfo=timezone.utc),
            completed_at=None,
            last_progress_at='2199-12-31T08:20:00+00:00',
            symbols_total=50,
            symbols_checked=12,
            symbols_fresh=7,
            symbols_refreshed=5,
            symbols_failed=1,
        )
        self._create_refresh_run(
            market_date=date(2199, 12, 30),
            status='completed_with_errors',
            started_at=datetime(2199, 12, 30, 8, 0, tzinfo=timezone.utc),
            completed_at=datetime(2199, 12, 30, 8, 46, tzinfo=timezone.utc),
            last_progress_at='2199-12-30T08:45:00+00:00',
            symbols_total=50,
            symbols_checked=48,
            symbols_fresh=45,
            symbols_refreshed=2,
            symbols_failed=1,
            error_message='one symbol failed',
        )
        self._create_refresh_run(
            market_date=date(2199, 12, 31),
            status='completed',
            fs_div='OFS',
            started_at=datetime(2199, 12, 31, 7, 0, tzinfo=timezone.utc),
            completed_at=datetime(2199, 12, 31, 7, 30, tzinfo=timezone.utc),
            last_progress_at='2199-12-31T07:29:00+00:00',
            symbols_total=30,
            symbols_checked=30,
            symbols_missing=0,
            symbols_stale=0,
            symbols_fresh=30,
            symbols_refreshed=0,
            symbols_failed=0,
            events_detected=0,
            events_captured=0,
            events_failed=0,
        )

        today_run = _load_today_fundamentals_refresh_run('2199-12-31')
        same_day_latest_available = _load_latest_fundamentals_refresh_run(
            market_date='2199-12-31',
            statuses=LATEST_AVAILABLE_FUNDAMENTALS_RUN_STATUSES,
        )
        latest_available_run = _load_latest_available_fundamentals_refresh_run()

        self.assertEqual(today_run['market_date'], '2199-12-31')
        self.assertEqual(today_run['status'], 'running')
        self.assertIsNone(same_day_latest_available)
        self.assertEqual(latest_available_run['market_date'], '2199-12-30')
        self.assertEqual(latest_available_run['status'], 'completed_with_errors')
        self.assertEqual(latest_available_run['fs_div'], self.fs_div)

    def test_get_latest_refresh_run_returns_detached_json_safe_mapping(self):
        self._create_refresh_run(
            market_date=date(2199, 12, 29),
            status='completed_with_errors',
            started_at=datetime(2199, 12, 29, 8, 0, tzinfo=timezone.utc),
            completed_at=datetime(2199, 12, 29, 8, 46, tzinfo=timezone.utc),
            last_progress_at='2199-12-29T08:45:00+00:00',
            error_message='one symbol failed',
        )

        payload = self.repository.get_latest_refresh_run(
            market_date=date(2199, 12, 29),
            fs_div=self.fs_div,
            statuses=LATEST_AVAILABLE_FUNDAMENTALS_RUN_STATUSES,
        )

        self.assertIsInstance(payload, dict)
        self.assertEqual(payload['market_date'], '2199-12-29')
        self.assertEqual(payload['status'], 'completed_with_errors')
        self.assertEqual(
            datetime.fromisoformat(payload['started_at']).astimezone(timezone.utc),
            datetime(2199, 12, 29, 8, 0, tzinfo=timezone.utc),
        )
        self.assertEqual(
            datetime.fromisoformat(payload['completed_at']).astimezone(timezone.utc),
            datetime(2199, 12, 29, 8, 46, tzinfo=timezone.utc),
        )
        self.assertEqual(payload['run_metadata']['symbols_selected'], 25)
        json.dumps(payload)

    def test_freshness_service_maps_latest_fundamentals_refresh_metrics_from_serialized_run(self):
        self._create_refresh_run(
            market_date=date(2199, 12, 31),
            status='completed_with_errors',
            started_at=datetime(2199, 12, 31, 8, 0, tzinfo=timezone.utc),
            completed_at=datetime(2199, 12, 31, 8, 46, tzinfo=timezone.utc),
            last_progress_at='2199-12-31T08:45:00+00:00',
            error_message='one symbol failed',
        )
        service = EODFreshnessService(
            market_state_loader=lambda: _FakeDetachedRunRegressionMarketState(),
            latest_run_loader=lambda **_: None,
            live_symbol_count_loader=lambda: 25,
            daily_price_bar_count_loader=lambda _: 0,
            daily_technical_snapshot_count_loader=lambda _: 0,
            daily_action_summary_count_loader=lambda _: 0,
            daily_fundamentals_count_loader=lambda _: {'symbols_succeeded': 0, 'symbols_failed': 0, 'symbols_total': 0},
            fundamentals_today_refresh_loader=freshness_module._load_today_fundamentals_refresh_run,
            latest_fundamentals_refresh_loader=lambda: freshness_module._load_latest_fundamentals_refresh_run(
                market_date='2199-12-31',
                statuses=LATEST_AVAILABLE_FUNDAMENTALS_RUN_STATUSES,
            ),
            latest_price_trade_date_loader=lambda: None,
            latest_price_bar_count_loader=lambda _: 0,
            latest_technical_market_date_loader=lambda: None,
            latest_action_market_date_loader=lambda: None,
        )

        result = service.read()

        self.assertEqual(result['today']['trade_date'], '2199-12-31')
        self.assertEqual(result['today']['fundamentals']['status'], 'missing')
        self.assertEqual(result['today']['fundamentals']['current_run']['basis'], 'market_date_refresh_run')
        self.assertEqual(result['today']['fundamentals']['current_run']['market_date'], '2199-12-31')
        self.assertEqual(result['today']['fundamentals']['current_run']['status'], 'partial')
        self.assertEqual(result['today']['fundamentals']['current_run']['updated_at'], '2199-12-31T08:45:00+00:00')
        self.assertEqual(result['today']['fundamentals']['current_run']['symbols_total'], 25)
        self.assertEqual(result['today']['fundamentals']['current_run']['symbols_checked'], 20)
        self.assertEqual(result['today']['fundamentals']['current_run']['symbols_succeeded'], 19)
        self.assertEqual(result['today']['fundamentals']['current_run']['symbols_remaining'], 5)
        self.assertEqual(result['latest']['fundamentals']['market_date'], '2199-12-31')
        self.assertEqual(result['latest']['fundamentals']['status'], 'partial')
        self.assertEqual(result['latest']['fundamentals']['symbols_total'], 25)
        self.assertEqual(result['latest']['fundamentals']['symbols_checked'], 20)
        self.assertEqual(result['latest']['fundamentals']['symbols_succeeded'], 19)
        self.assertEqual(result['latest']['fundamentals']['symbols_fresh'], 16)
        self.assertEqual(result['latest']['fundamentals']['symbols_refreshed'], 3)
        self.assertEqual(result['latest']['fundamentals']['symbols_failed'], 1)
