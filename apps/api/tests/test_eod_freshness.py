import unittest
from datetime import date, datetime, timezone

from sqlalchemy import delete, select

from app.core.settings import settings
from app.dart.fundamentals_store import DartFundamentalsRepository
from app.db import session_scope
from app.db_models import DartFundamentalsRefreshRun
from app.eod import freshness as freshness_module
from app.eod.freshness import (
    EODFreshnessService,
    LATEST_AVAILABLE_FUNDAMENTALS_RUN_STATUSES,
)


class _FakeMarketState:
    server_time = '2026-04-10T18:05:00+09:00'


class _FakeNextTradeDayMarketState:
    server_time = '2026-04-13T10:05:00+09:00'


class _FakeWeekendMarketState:
    server_time = '2026-04-11T12:05:00+09:00'


class _FakeDetachedRunRegressionMarketState:
    server_time = '2099-12-31T18:05:00+09:00'


def _latest_run(
    *,
    status: str,
    symbols_total: int,
    symbols_succeeded: int,
    symbols_failed: int,
    started_at: str = '2026-04-10T06:00:00+00:00',
    completed_at: str | None = '2026-04-10T06:31:55+00:00',
    run_metadata: dict | None = None,
) -> dict:
    return {
        'status': status,
        'symbols_total': symbols_total,
        'symbols_succeeded': symbols_succeeded,
        'symbols_failed': symbols_failed,
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


def _empty_fundamentals_refresh_summary() -> dict:
    return {
        'basis': None,
        'market_date': None,
        'status': None,
        'fs_div': None,
        'trigger': None,
        'updated_at': None,
        'completed_at': None,
        'symbols_total': None,
        'symbols_checked': None,
        'symbols_succeeded': None,
        'symbols_fresh': None,
        'symbols_refreshed': None,
        'symbols_failed': None,
        'symbols_remaining': None,
        'events_detected': None,
        'events_captured': None,
        'events_failed': None,
        'error_message': None,
    }


class EODFreshnessServiceTests(unittest.TestCase):
    def test_read_returns_missing_when_no_run_or_data(self):
        service = EODFreshnessService(
            market_state_loader=lambda: _FakeMarketState(),
            latest_run_loader=lambda **_: None,
            live_symbol_count_loader=lambda: 3,
            daily_price_bar_count_loader=lambda _: 0,
            daily_technical_snapshot_count_loader=lambda _: 0,
            daily_action_summary_count_loader=lambda _: 0,
            daily_fundamentals_count_loader=lambda _: {'symbols_succeeded': 0, 'symbols_failed': 0},
            fundamentals_today_refresh_loader=lambda _: None,
            latest_fundamentals_refresh_loader=lambda: None,
            latest_price_trade_date_loader=lambda: None,
            latest_price_bar_count_loader=lambda _: 0,
            latest_technical_market_date_loader=lambda: None,
            latest_action_market_date_loader=lambda: None,
        )

        result = service.read()

        self.assertEqual(result['today']['trade_date'], '2026-04-10')
        self.assertEqual(
            result['today']['price'],
            {'status': 'missing', 'symbols_total': 3, 'symbols_succeeded': 0, 'symbols_failed': 0},
        )
        self.assertEqual(
            result['today']['technical_analysis'],
            {'status': 'missing', 'symbols_total': 3, 'symbols_succeeded': 0, 'symbols_failed': 0},
        )
        self.assertEqual(
            result['today']['action_summary'],
            {'status': 'missing', 'symbols_total': 3, 'symbols_succeeded': 0, 'symbols_failed': 0},
        )
        self.assertEqual(
            result['today']['fundamentals'],
            {
                'status': 'missing',
                'basis': 'same_day_snapshot_updates',
                'symbols_total': 0,
                'symbols_succeeded': 0,
                'symbols_failed': 0,
                'current_run': _empty_fundamentals_refresh_summary(),
            },
        )
        self.assertEqual(result['latest']['price_trade_date'], None)
        self.assertEqual(result['latest']['eod_status'], 'missing')
        self.assertEqual(result['latest']['trade_date'], None)
        self.assertEqual(result['latest']['updated_at'], None)
        self.assertEqual(result['latest']['succeeded'], None)
        self.assertEqual(result['latest']['failed'], None)
        self.assertEqual(result['latest']['total'], None)
        self.assertEqual(
            result['latest']['summary'],
            {'basis': None, 'updated_at': None, 'succeeded': None, 'failed': None, 'total': None},
        )
        self.assertEqual(result['latest']['symbols_total'], None)
        self.assertEqual(result['latest']['symbols_succeeded'], None)
        self.assertEqual(result['latest']['symbols_failed'], None)
        self.assertEqual(result['latest']['fundamentals'], _empty_fundamentals_refresh_summary())

    def test_read_keeps_today_missing_when_only_previous_date_run_exists(self):
        requested_run_dates = []
        requested_live_count_dates = []
        requested_latest_count_dates = []
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
            requested_run_dates.append(kwargs['trade_date'])
            if kwargs['trade_date'] == '2026-04-09':
                return _latest_run(
                    status='completed',
                    symbols_total=3584,
                    symbols_succeeded=3581,
                    symbols_failed=3,
                    completed_at='2026-04-09T07:31:55+00:00',
                )
            return None

        def daily_price_bar_count_loader(trade_date):
            requested_live_count_dates.append(trade_date)
            return {'2026-04-10': 0}[trade_date]

        def latest_price_bar_count_loader(trade_date):
            requested_latest_count_dates.append(trade_date)
            return {'2026-04-09': 3581}[trade_date]

        service = EODFreshnessService(
            market_state_loader=lambda: _FakeMarketState(),
            latest_run_loader=latest_run_loader,
            live_symbol_count_loader=lambda: 3584,
            daily_price_bar_count_loader=daily_price_bar_count_loader,
            daily_technical_snapshot_count_loader=lambda _: 0,
            daily_action_summary_count_loader=lambda _: 0,
            daily_fundamentals_count_loader=lambda _: {'symbols_succeeded': 0, 'symbols_failed': 0},
            fundamentals_today_refresh_loader=lambda _: None,
            latest_fundamentals_refresh_loader=lambda: previous_fundamentals_run,
            latest_price_trade_date_loader=lambda: '2026-04-09',
            latest_price_bar_count_loader=latest_price_bar_count_loader,
            latest_technical_market_date_loader=lambda: '2026-04-09',
            latest_action_market_date_loader=lambda: '2026-04-09',
        )

        result = service.read()

        self.assertEqual(result['today']['trade_date'], '2026-04-10')
        self.assertEqual(result['today']['price']['status'], 'missing')
        self.assertEqual(result['today']['price']['symbols_total'], 3584)
        self.assertEqual(result['today']['price']['symbols_succeeded'], 0)
        self.assertEqual(result['today']['price']['symbols_failed'], 0)
        self.assertEqual(result['latest']['trade_date'], '2026-04-09')
        self.assertEqual(result['latest']['price_trade_date'], '2026-04-09')
        self.assertEqual(result['latest']['eod_status'], 'completed_with_errors')
        self.assertEqual(result['latest']['updated_at'], '2026-04-09T07:31:55+00:00')
        self.assertEqual(result['latest']['succeeded'], 3581)
        self.assertEqual(result['latest']['failed'], 3)
        self.assertEqual(result['latest']['total'], 3584)
        self.assertEqual(
            result['latest']['summary'],
            {
                'basis': 'latest_eod_run_symbol_universe',
                'updated_at': '2026-04-09T07:31:55+00:00',
                'succeeded': 3581,
                'failed': 3,
                'total': 3584,
            },
        )
        self.assertEqual(result['latest']['symbols_total'], 3584)
        self.assertEqual(result['latest']['symbols_succeeded'], 3581)
        self.assertEqual(result['latest']['symbols_failed'], 3)
        self.assertEqual(result['latest']['technical_market_date'], '2026-04-09')
        self.assertEqual(result['latest']['action_market_date'], '2026-04-09')
        self.assertEqual(result['latest']['fundamentals']['market_date'], '2026-04-09')
        self.assertEqual(result['latest']['fundamentals']['status'], 'completed')
        self.assertEqual(result['latest']['fundamentals']['symbols_succeeded'], 120)
        self.assertEqual(result['latest']['fundamentals']['symbols_refreshed'], 1)
        self.assertLess(result['latest']['trade_date'], result['today']['trade_date'])
        self.assertEqual(requested_run_dates, ['2026-04-09', '2026-04-10', '2026-04-10'])
        self.assertEqual(requested_live_count_dates, ['2026-04-10'])
        self.assertEqual(requested_latest_count_dates, ['2026-04-09', '2026-04-09'])

    def test_latest_status_uses_all_stored_price_bars_not_only_live_symbols(self):
        requested_live_count_dates = []
        requested_latest_count_dates = []

        def daily_price_bar_count_loader(trade_date):
            requested_live_count_dates.append(trade_date)
            return 0

        def latest_price_bar_count_loader(trade_date):
            requested_latest_count_dates.append(trade_date)
            return 5

        service = EODFreshnessService(
            market_state_loader=lambda: _FakeMarketState(),
            latest_run_loader=lambda **_: None,
            live_symbol_count_loader=lambda: 3,
            daily_price_bar_count_loader=daily_price_bar_count_loader,
            daily_technical_snapshot_count_loader=lambda _: 0,
            daily_action_summary_count_loader=lambda _: 0,
            daily_fundamentals_count_loader=lambda _: {'symbols_succeeded': 0, 'symbols_failed': 0},
            fundamentals_today_refresh_loader=lambda _: None,
            latest_fundamentals_refresh_loader=lambda: None,
            latest_price_trade_date_loader=lambda: '2026-04-10',
            latest_price_bar_count_loader=latest_price_bar_count_loader,
            latest_price_bar_updated_at_loader=lambda _: '2026-04-10T06:05:12+00:00',
            latest_technical_market_date_loader=lambda: None,
            latest_action_market_date_loader=lambda: None,
        )

        result = service.read()

        self.assertEqual(result['today']['price']['status'], 'missing')
        self.assertEqual(result['latest']['trade_date'], '2026-04-10')
        self.assertEqual(result['latest']['price_trade_date'], '2026-04-10')
        self.assertEqual(result['latest']['eod_status'], 'completed')
        self.assertEqual(result['latest']['updated_at'], '2026-04-10T06:05:12+00:00')
        self.assertEqual(result['latest']['succeeded'], 5)
        self.assertEqual(result['latest']['failed'], 0)
        self.assertEqual(result['latest']['total'], 5)
        self.assertEqual(
            result['latest']['summary'],
            {
                'basis': 'stored_price_bars_for_latest_trade_date',
                'updated_at': '2026-04-10T06:05:12+00:00',
                'succeeded': 5,
                'failed': 0,
                'total': 5,
            },
        )
        self.assertEqual(result['latest']['symbols_total'], None)
        self.assertEqual(result['latest']['symbols_succeeded'], None)
        self.assertEqual(result['latest']['symbols_failed'], None)
        self.assertEqual(requested_live_count_dates, ['2026-04-10'])
        self.assertEqual(requested_latest_count_dates, ['2026-04-10'])

    def test_latest_summary_uses_same_fallback_run_selection_logic_as_today(self):
        def latest_run_loader(**_kwargs):
            return _latest_run(
                status='completed',
                symbols_total=2,
                symbols_succeeded=0,
                symbols_failed=0,
                completed_at='2026-04-10T06:10:00+00:00',
            )

        service = EODFreshnessService(
            market_state_loader=lambda: _FakeMarketState(),
            latest_run_loader=latest_run_loader,
            live_symbol_count_loader=lambda: 3,
            daily_price_bar_count_loader=lambda _: 4,
            daily_technical_snapshot_count_loader=lambda _: 0,
            daily_action_summary_count_loader=lambda _: 0,
            daily_fundamentals_count_loader=lambda _: {'symbols_succeeded': 0, 'symbols_failed': 0, 'symbols_total': 0},
            fundamentals_today_refresh_loader=lambda _: None,
            latest_fundamentals_refresh_loader=lambda: None,
            latest_price_trade_date_loader=lambda: '2026-04-10',
            latest_price_bar_count_loader=lambda _: 5,
            latest_technical_market_date_loader=lambda: None,
            latest_action_market_date_loader=lambda: None,
            best_run_loader=lambda **_: {
                'id': 99,
                'market': 'KRX',
                'trade_date': '2026-04-10',
                'source': 'pykrx',
                'trigger': 'manual_daily_api',
                'status': 'completed',
                'symbols_total': 5,
                'symbols_succeeded': 4,
                'symbols_failed': 1,
                'error_message': None,
                'started_at': datetime(2026, 4, 10, 6, 0, tzinfo=timezone.utc).isoformat(),
                'completed_at': datetime(2026, 4, 10, 6, 45, tzinfo=timezone.utc).isoformat(),
                'run_metadata': {'last_progress_at': '2026-04-10T06:44:00+00:00'},
            },
        )

        result = service.read()

        self.assertEqual(result['today']['price']['symbols_total'], 5)
        self.assertEqual(result['today']['price']['symbols_succeeded'], 4)
        self.assertEqual(result['today']['price']['symbols_failed'], 1)
        self.assertEqual(result['latest']['trade_date'], '2026-04-10')
        self.assertEqual(result['latest']['symbols_total'], 5)
        self.assertEqual(result['latest']['symbols_succeeded'], 4)
        self.assertEqual(result['latest']['symbols_failed'], 1)
        self.assertEqual(
            result['latest']['summary'],
            {
                'basis': 'latest_eod_run_symbol_universe',
                'updated_at': '2026-04-10T06:44:00+00:00',
                'succeeded': 4,
                'failed': 1,
                'total': 5,
            },
        )

    def test_latest_summary_exposes_healthy_run_counts_and_timestamp(self):
        service = EODFreshnessService(
            market_state_loader=lambda: _FakeMarketState(),
            latest_run_loader=lambda **_: _latest_run(
                status='completed',
                symbols_total=3,
                symbols_succeeded=3,
                symbols_failed=0,
                completed_at='2026-04-10T07:31:55+00:00',
                run_metadata={'last_progress_at': '2026-04-10T07:31:55+00:00'},
            ),
            live_symbol_count_loader=lambda: 3,
            daily_price_bar_count_loader=lambda _: 3,
            daily_technical_snapshot_count_loader=lambda _: 3,
            daily_action_summary_count_loader=lambda _: 3,
            daily_fundamentals_count_loader=lambda _: {'symbols_succeeded': 2, 'symbols_failed': 0, 'symbols_total': 2},
            fundamentals_today_refresh_loader=lambda _: None,
            latest_fundamentals_refresh_loader=lambda: None,
            latest_price_trade_date_loader=lambda: '2026-04-10',
            latest_price_bar_count_loader=lambda _: 3,
            latest_technical_market_date_loader=lambda: '2026-04-10',
            latest_action_market_date_loader=lambda: '2026-04-10',
        )

        result = service.read()

        self.assertEqual(result['latest']['trade_date'], '2026-04-10')
        self.assertEqual(result['latest']['eod_status'], 'completed')
        self.assertEqual(result['latest']['updated_at'], '2026-04-10T07:31:55+00:00')
        self.assertEqual(result['latest']['succeeded'], 3)
        self.assertEqual(result['latest']['failed'], 0)
        self.assertEqual(result['latest']['total'], 3)
        self.assertEqual(result['latest']['symbols_total'], 3)
        self.assertEqual(result['latest']['symbols_succeeded'], 3)
        self.assertEqual(result['latest']['symbols_failed'], 0)
        self.assertEqual(result['latest']['summary']['basis'], 'latest_eod_run_symbol_universe')
        self.assertEqual(result['latest']['failed'], result['latest']['total'] - result['latest']['succeeded'])
        self.assertIsNotNone(datetime.fromisoformat(result['latest']['updated_at']))

    def test_read_returns_completed_with_errors_from_run(self):
        service = EODFreshnessService(
            market_state_loader=lambda: _FakeMarketState(),
            latest_run_loader=lambda **_: _latest_run(
                status='completed',
                symbols_total=3,
                symbols_succeeded=2,
                symbols_failed=1,
                completed_at='2026-04-10T07:31:55+00:00',
                run_metadata={'last_progress_at': '2026-04-10T07:31:55+00:00'},
            ),
            live_symbol_count_loader=lambda: 3,
            daily_price_bar_count_loader=lambda _: 2,
            daily_technical_snapshot_count_loader=lambda _: 3,
            daily_action_summary_count_loader=lambda _: 2,
            daily_fundamentals_count_loader=lambda _: {'symbols_succeeded': 2, 'symbols_failed': 1, 'symbols_total': 2},
            fundamentals_today_refresh_loader=lambda _: None,
            latest_fundamentals_refresh_loader=lambda: None,
            latest_price_trade_date_loader=lambda: '2026-04-10',
            latest_price_bar_count_loader=lambda _: 2,
            latest_technical_market_date_loader=lambda: '2026-04-10',
            latest_action_market_date_loader=lambda: '2026-04-09',
        )

        result = service.read()

        self.assertEqual(
            result['today']['price'],
            {'status': 'ready', 'symbols_total': 3, 'symbols_succeeded': 2, 'symbols_failed': 1},
        )
        self.assertEqual(
            result['today']['technical_analysis'],
            {'status': 'ready', 'symbols_total': 3, 'symbols_succeeded': 3, 'symbols_failed': 0},
        )
        self.assertEqual(
            result['today']['action_summary'],
            {'status': 'ready', 'symbols_total': 3, 'symbols_succeeded': 2, 'symbols_failed': 1},
        )
        self.assertEqual(
            result['today']['fundamentals'],
            {
                'status': 'ready',
                'basis': 'same_day_snapshot_updates',
                'symbols_total': 2,
                'symbols_succeeded': 2,
                'symbols_failed': 1,
                'current_run': _empty_fundamentals_refresh_summary(),
            },
        )
        self.assertEqual(result['latest']['trade_date'], '2026-04-10')
        self.assertEqual(result['latest']['eod_status'], 'completed_with_errors')
        self.assertEqual(result['latest']['updated_at'], '2026-04-10T07:31:55+00:00')
        self.assertEqual(result['latest']['succeeded'], 2)
        self.assertEqual(result['latest']['failed'], 1)
        self.assertEqual(result['latest']['total'], 3)
        self.assertEqual(result['latest']['symbols_total'], 3)
        self.assertEqual(result['latest']['symbols_succeeded'], 2)
        self.assertEqual(result['latest']['symbols_failed'], 1)
        self.assertEqual(result['latest']['summary']['basis'], 'latest_eod_run_symbol_universe')
        self.assertEqual(result['latest']['action_market_date'], '2026-04-09')

    def test_read_preserves_completed_with_errors_run_status(self):
        service = EODFreshnessService(
            market_state_loader=lambda: _FakeMarketState(),
            latest_run_loader=lambda **_: _latest_run(
                status='completed_with_errors',
                symbols_total=3,
                symbols_succeeded=2,
                symbols_failed=1,
                completed_at='2026-04-09T07:31:55+00:00',
            ),
            live_symbol_count_loader=lambda: 3,
            daily_price_bar_count_loader=lambda _: 0,
            daily_technical_snapshot_count_loader=lambda _: 0,
            daily_action_summary_count_loader=lambda _: 0,
            daily_fundamentals_count_loader=lambda _: {'symbols_succeeded': 0, 'symbols_failed': 0},
            fundamentals_today_refresh_loader=lambda _: None,
            latest_fundamentals_refresh_loader=lambda: None,
            latest_price_trade_date_loader=lambda: '2026-04-09',
            latest_price_bar_count_loader=lambda _: 0,
            latest_technical_market_date_loader=lambda: '2026-04-09',
            latest_action_market_date_loader=lambda: '2026-04-09',
        )

        result = service.read()

        self.assertEqual(result['latest']['eod_status'], 'completed_with_errors')
        self.assertEqual(result['latest']['updated_at'], '2026-04-09T07:31:55+00:00')
        self.assertEqual(result['latest']['succeeded'], 2)
        self.assertEqual(result['latest']['failed'], 1)
        self.assertEqual(result['latest']['total'], 3)
        self.assertEqual(
            result['today']['price'],
            {'status': 'missing', 'symbols_total': 3, 'symbols_succeeded': 0, 'symbols_failed': 0},
        )

    def test_today_fundamentals_running_progress_preserves_latest_available_baseline(self):
        running_today_refresh = _fundamentals_refresh_run(
            market_date='2026-04-10',
            status='running',
            symbols_total=100,
            symbols_checked=12,
            symbols_fresh=7,
            symbols_refreshed=5,
            symbols_failed=1,
            started_at='2026-04-10T06:00:00+00:00',
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
            latest_run_loader=lambda **_: _latest_run(
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

        self.assertEqual(result['today']['price']['status'], 'ready')
        self.assertEqual(result['today']['price']['symbols_total'], 100)
        self.assertEqual(result['today']['price']['symbols_succeeded'], 12)
        self.assertEqual(result['today']['price']['symbols_failed'], 88)
        self.assertEqual(result['today']['technical_analysis']['status'], 'missing')
        self.assertEqual(result['today']['action_summary']['status'], 'missing')
        self.assertEqual(
            result['today']['fundamentals'],
            {
                'status': 'running',
                'basis': 'same_day_snapshot_updates',
                'symbols_total': 5,
                'symbols_succeeded': 5,
                'symbols_failed': 1,
                'current_run': {
                    'basis': 'market_date_refresh_run',
                    'market_date': '2026-04-10',
                    'status': 'running',
                    'fs_div': 'CFS',
                    'trigger': 'eod_pykrx_followup',
                    'updated_at': '2026-04-10T06:20:00+00:00',
                    'completed_at': None,
                    'symbols_total': 100,
                    'symbols_checked': 12,
                    'symbols_succeeded': 12,
                    'symbols_fresh': 7,
                    'symbols_refreshed': 5,
                    'symbols_failed': 1,
                    'symbols_remaining': 88,
                    'events_detected': 14,
                    'events_captured': 6,
                    'events_failed': 1,
                    'error_message': None,
                },
            },
        )
        self.assertEqual(result['latest']['eod_status'], 'running')
        self.assertEqual(result['latest']['updated_at'], '2026-04-10T06:15:00+00:00')
        self.assertEqual(result['latest']['succeeded'], 12)
        self.assertEqual(result['latest']['failed'], 1)
        self.assertEqual(result['latest']['total'], 100)
        self.assertEqual(result['latest']['fundamentals']['market_date'], '2026-04-09')
        self.assertEqual(result['latest']['fundamentals']['status'], 'completed')
        self.assertEqual(result['latest']['fundamentals']['symbols_succeeded'], 100)
        self.assertEqual(result['latest']['fundamentals']['symbols_refreshed'], 3)

    def test_latest_fundamentals_uses_latest_completed_run_on_next_trade_day(self):
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
        self.assertEqual(result['today']['fundamentals']['current_run'], _empty_fundamentals_refresh_summary())
        self.assertEqual(result['latest']['fundamentals']['market_date'], '2026-04-10')
        self.assertEqual(result['latest']['fundamentals']['status'], 'completed')
        self.assertEqual(result['latest']['fundamentals']['symbols_total'], 250)
        self.assertEqual(result['latest']['fundamentals']['symbols_checked'], 250)
        self.assertEqual(result['latest']['fundamentals']['symbols_succeeded'], 250)
        self.assertEqual(result['latest']['fundamentals']['symbols_refreshed'], 1)

    def test_read_keeps_weekend_today_missing_while_latest_stays_on_previous_trade_date(self):
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

    def test_latest_fundamentals_keeps_same_day_noop_completed_refresh_available(self):
        completed_today_refresh = _fundamentals_refresh_run(
            market_date='2026-04-10',
            status='completed',
            symbols_total=180,
            symbols_checked=180,
            symbols_fresh=180,
            symbols_refreshed=0,
            completed_at='2026-04-10T07:50:00+00:00',
            run_metadata={'last_progress_at': '2026-04-10T07:50:00+00:00'},
        )
        service = EODFreshnessService(
            market_state_loader=lambda: _FakeMarketState(),
            latest_run_loader=lambda **_: None,
            live_symbol_count_loader=lambda: 180,
            daily_price_bar_count_loader=lambda _: 0,
            daily_technical_snapshot_count_loader=lambda _: 0,
            daily_action_summary_count_loader=lambda _: 0,
            daily_fundamentals_count_loader=lambda _: {'symbols_succeeded': 0, 'symbols_failed': 0, 'symbols_total': 0},
            fundamentals_today_refresh_loader=lambda _: completed_today_refresh,
            latest_fundamentals_refresh_loader=lambda: completed_today_refresh,
            latest_price_trade_date_loader=lambda: None,
            latest_price_bar_count_loader=lambda _: 0,
            latest_technical_market_date_loader=lambda: None,
            latest_action_market_date_loader=lambda: None,
        )

        result = service.read()

        self.assertEqual(result['today']['fundamentals']['status'], 'missing')
        self.assertEqual(result['today']['fundamentals']['current_run']['status'], 'completed')
        self.assertEqual(result['today']['fundamentals']['current_run']['symbols_refreshed'], 0)
        self.assertEqual(result['latest']['fundamentals']['market_date'], '2026-04-10')
        self.assertEqual(result['latest']['fundamentals']['status'], 'completed')
        self.assertEqual(result['latest']['fundamentals']['symbols_succeeded'], 180)
        self.assertEqual(result['latest']['fundamentals']['symbols_refreshed'], 0)


class EODFreshnessServiceDatabaseTests(unittest.TestCase):
    def setUp(self):
        self.repository = DartFundamentalsRepository()
        self.created_refresh_run_ids: list[int] = []

    def tearDown(self):
        if not self.created_refresh_run_ids:
            return
        with session_scope() as session:
            session.execute(
                delete(DartFundamentalsRefreshRun).where(
                    DartFundamentalsRefreshRun.id.in_(self.created_refresh_run_ids)
                )
            )

    def _create_completed_refresh_run(self, *, market_date: date) -> None:
        fs_div = settings.dart_fundamentals_default_fs_div.strip().upper()
        run_id = self.repository.create_refresh_run(
            market_date=market_date,
            trigger='test_eod_freshness_detached_refresh_run',
            fs_div=fs_div,
            force=False,
            symbol_limit=25,
            symbol_offset=0,
            symbols_total=25,
            run_metadata={'symbols_selected': 25},
        )
        self.created_refresh_run_ids.append(run_id)
        self.repository.complete_refresh_run(
            run_id,
            status='completed_with_errors',
            symbols_checked=20,
            symbols_missing=2,
            symbols_stale=4,
            symbols_fresh=16,
            symbols_refreshed=3,
            symbols_failed=1,
            events_detected=5,
            events_captured=3,
            events_failed=1,
            error_message='one symbol failed',
            run_metadata={
                'symbols_selected': 25,
                'last_progress_at': '2099-12-31T08:45:00+00:00',
            },
        )
        with session_scope() as session:
            row = session.execute(
                select(DartFundamentalsRefreshRun).where(DartFundamentalsRefreshRun.id == run_id)
            ).scalar_one()
            row.started_at = datetime(2099, 12, 31, 8, 0, tzinfo=timezone.utc)
            row.completed_at = datetime(2099, 12, 31, 8, 46, tzinfo=timezone.utc)

    def test_read_serializes_latest_fundamentals_refresh_run_inside_session(self):
        self._create_completed_refresh_run(market_date=date(2099, 12, 31))
        service = EODFreshnessService(
            market_state_loader=lambda: _FakeDetachedRunRegressionMarketState(),
            latest_run_loader=lambda **_: None,
            live_symbol_count_loader=lambda: 25,
            daily_price_bar_count_loader=lambda _: 0,
            daily_technical_snapshot_count_loader=lambda _: 0,
            daily_action_summary_count_loader=lambda _: 0,
            daily_fundamentals_count_loader=lambda _: {
                'symbols_succeeded': 0,
                'symbols_failed': 0,
                'symbols_total': 0,
            },
            fundamentals_today_refresh_loader=freshness_module._load_today_fundamentals_refresh_run,
            latest_fundamentals_refresh_loader=lambda: freshness_module._load_latest_fundamentals_refresh_run(
                market_date='2099-12-31',
                statuses=LATEST_AVAILABLE_FUNDAMENTALS_RUN_STATUSES,
            ),
            latest_price_trade_date_loader=lambda: None,
            latest_price_bar_count_loader=lambda _: 0,
            latest_technical_market_date_loader=lambda: None,
            latest_action_market_date_loader=lambda: None,
        )

        result = service.read()

        self.assertEqual(result['today']['trade_date'], '2099-12-31')
        self.assertEqual(result['today']['fundamentals']['status'], 'missing')
        self.assertEqual(
            result['today']['fundamentals']['current_run']['basis'],
            'market_date_refresh_run',
        )
        self.assertEqual(result['today']['fundamentals']['current_run']['market_date'], '2099-12-31')
        self.assertEqual(result['today']['fundamentals']['current_run']['status'], 'partial')
        self.assertEqual(
            result['today']['fundamentals']['current_run']['updated_at'],
            '2099-12-31T08:45:00+00:00',
        )
        self.assertEqual(result['today']['fundamentals']['current_run']['symbols_total'], 25)
        self.assertEqual(result['today']['fundamentals']['current_run']['symbols_checked'], 20)
        self.assertEqual(result['today']['fundamentals']['current_run']['symbols_succeeded'], 19)
        self.assertEqual(result['today']['fundamentals']['current_run']['symbols_remaining'], 5)
        self.assertEqual(result['today']['fundamentals']['current_run']['events_detected'], 5)
        self.assertEqual(result['today']['fundamentals']['current_run']['events_failed'], 1)
        self.assertEqual(
            result['today']['fundamentals']['current_run']['error_message'],
            'one symbol failed',
        )
        self.assertEqual(result['latest']['fundamentals']['basis'], 'latest_available_refresh_run')
        self.assertEqual(result['latest']['fundamentals']['market_date'], '2099-12-31')
        self.assertEqual(result['latest']['fundamentals']['status'], 'partial')
        self.assertEqual(
            result['latest']['fundamentals']['updated_at'],
            '2099-12-31T08:45:00+00:00',
        )
        self.assertEqual(result['latest']['fundamentals']['symbols_total'], 25)
        self.assertEqual(result['latest']['fundamentals']['symbols_checked'], 20)
        self.assertEqual(result['latest']['fundamentals']['symbols_succeeded'], 19)
        self.assertEqual(result['latest']['fundamentals']['symbols_fresh'], 16)
        self.assertEqual(result['latest']['fundamentals']['symbols_refreshed'], 3)
        self.assertEqual(result['latest']['fundamentals']['symbols_failed'], 1)
