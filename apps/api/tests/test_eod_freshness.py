import unittest
from datetime import datetime

from app.eod.freshness import EODFreshnessService


class _FakeMarketState:
    server_time = '2026-04-10T18:05:00+09:00'


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
            fundamentals_running_refresh_loader=lambda _: False,
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
            {'status': 'missing', 'symbols_total': 0, 'symbols_succeeded': 0, 'symbols_failed': 0},
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

    def test_read_keeps_today_missing_when_only_previous_date_run_exists(self):
        requested_run_dates = []
        requested_live_count_dates = []
        requested_latest_count_dates = []

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
            fundamentals_running_refresh_loader=lambda _: False,
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
            fundamentals_running_refresh_loader=lambda _: False,
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
            fundamentals_running_refresh_loader=lambda _: False,
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
            fundamentals_running_refresh_loader=lambda _: False,
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
            {'status': 'ready', 'symbols_total': 2, 'symbols_succeeded': 2, 'symbols_failed': 1},
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
            fundamentals_running_refresh_loader=lambda _: False,
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

    def test_today_stages_use_persisted_counts_when_run_in_progress(self):
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
            fundamentals_running_refresh_loader=lambda _: False,
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
            {'status': 'ready', 'symbols_total': 5, 'symbols_succeeded': 5, 'symbols_failed': 1},
        )
        self.assertEqual(result['latest']['eod_status'], 'running')
        self.assertEqual(result['latest']['updated_at'], '2026-04-10T06:15:00+00:00')
        self.assertEqual(result['latest']['succeeded'], 12)
        self.assertEqual(result['latest']['failed'], 1)
        self.assertEqual(result['latest']['total'], 100)
