import unittest

from app.eod.freshness import EODFreshnessService


class _FakeMarketState:
    server_time = '2026-04-10T18:05:00+09:00'


class EODFreshnessServiceTests(unittest.TestCase):
    def test_read_returns_missing_when_no_run_or_data(self):
        service = EODFreshnessService(
            market_state_loader=lambda: _FakeMarketState(),
            latest_run_loader=lambda **_: None,
            daily_price_bar_count_loader=lambda _: 0,
            latest_price_trade_date_loader=lambda: None,
            latest_technical_market_date_loader=lambda: None,
            latest_action_market_date_loader=lambda: None,
        )

        result = service.read()

        self.assertEqual(result['today']['trade_date'], '2026-04-10')
        self.assertEqual(result['today']['eod_status'], 'missing')
        self.assertEqual(result['today']['symbols_total'], None)
        self.assertEqual(result['today']['symbols_succeeded'], None)
        self.assertEqual(result['today']['symbols_failed'], None)
        self.assertEqual(result['today']['received'], False)
        self.assertEqual(result['latest']['price_trade_date'], None)

    def test_read_returns_completed_with_errors_from_run(self):
        service = EODFreshnessService(
            market_state_loader=lambda: _FakeMarketState(),
            latest_run_loader=lambda **_: {
                'status': 'completed',
                'symbols_total': 3,
                'symbols_succeeded': 2,
                'symbols_failed': 1,
            },
            daily_price_bar_count_loader=lambda _: 2,
            latest_price_trade_date_loader=lambda: '2026-04-10',
            latest_technical_market_date_loader=lambda: '2026-04-10',
            latest_action_market_date_loader=lambda: '2026-04-09',
        )

        result = service.read()

        self.assertEqual(result['today']['eod_status'], 'completed_with_errors')
        self.assertEqual(result['today']['symbols_total'], 3)
        self.assertEqual(result['today']['symbols_succeeded'], 2)
        self.assertEqual(result['today']['symbols_failed'], 1)
        self.assertEqual(result['today']['received'], True)
        self.assertEqual(result['latest']['action_market_date'], '2026-04-09')

    def test_read_returns_running_when_run_in_progress(self):
        service = EODFreshnessService(
            market_state_loader=lambda: _FakeMarketState(),
            latest_run_loader=lambda **_: {
                'status': 'running',
                'symbols_total': 100,
                'symbols_succeeded': 12,
                'symbols_failed': 1,
            },
            daily_price_bar_count_loader=lambda _: 12,
            latest_price_trade_date_loader=lambda: '2026-04-10',
            latest_technical_market_date_loader=lambda: '2026-04-09',
            latest_action_market_date_loader=lambda: '2026-04-09',
        )

        result = service.read()

        self.assertEqual(result['today']['eod_status'], 'running')
        self.assertEqual(result['today']['received'], True)
