import unittest
from unittest.mock import patch

from app.agent_context.derived_contexts import DailyPriceContextService


class DailyPriceContextServiceTests(unittest.TestCase):
    @patch('app.agent_context.derived_contexts.upsert_daily_price_context')
    @patch('app.agent_context.derived_contexts.load_recent_price_bars_by_symbol')
    def test_compute_and_persist_skips_symbols_without_bar_for_requested_market_date(
        self,
        load_recent_price_bars_by_symbol_mock,
        upsert_daily_price_context_mock,
    ):
        load_recent_price_bars_by_symbol_mock.return_value = {
            '005930': [
                {
                    'trade_date': '2026-04-07',
                    'open_price': 70000,
                    'high_price': 71000,
                    'low_price': 69000,
                    'close_price': 70500,
                    'volume': 100,
                    'turnover': 1000,
                    'source': 'pykrx',
                    'captured_at': '2026-04-07T15:30:00+09:00',
                },
                {
                    'trade_date': '2026-04-04',
                    'open_price': 68000,
                    'high_price': 70000,
                    'low_price': 67500,
                    'close_price': 69500,
                    'volume': 90,
                    'turnover': 900,
                    'source': 'pykrx',
                    'captured_at': '2026-04-04T15:30:00+09:00',
                },
            ]
        }

        result = DailyPriceContextService().compute_and_persist_for_market_date(
            market_date='2026-04-08',
            symbols=['005930'],
        )

        self.assertEqual(result['contexts_persisted'], 0)
        self.assertEqual(result['contexts_failed'], 1)
        self.assertEqual(result['failed_symbols'], ['005930'])
        self.assertEqual(result['items'], [])
        upsert_daily_price_context_mock.assert_not_called()

    @patch('app.agent_context.derived_contexts.upsert_daily_price_context')
    @patch('app.agent_context.derived_contexts.load_recent_price_bars_by_symbol')
    def test_compute_and_persist_persists_when_latest_bar_matches_requested_market_date(
        self,
        load_recent_price_bars_by_symbol_mock,
        upsert_daily_price_context_mock,
    ):
        load_recent_price_bars_by_symbol_mock.return_value = {
            '005930': [
                {
                    'trade_date': '2026-04-08',
                    'open_price': 70000,
                    'high_price': 72000,
                    'low_price': 69950,
                    'close_price': 71500,
                    'volume': 123456,
                    'turnover': 8800000000,
                    'source': 'pykrx',
                    'captured_at': '2026-04-08T15:30:00+09:00',
                },
                {
                    'trade_date': '2026-04-07',
                    'open_price': 69800,
                    'high_price': 70500,
                    'low_price': 69000,
                    'close_price': 69800,
                    'volume': 120000,
                    'turnover': 8200000000,
                    'source': 'pykrx',
                    'captured_at': '2026-04-07T15:30:00+09:00',
                },
            ]
        }

        result = DailyPriceContextService().compute_and_persist_for_market_date(
            market_date='2026-04-08',
            symbols=['005930'],
        )

        self.assertEqual(result['contexts_persisted'], 1)
        self.assertEqual(result['contexts_failed'], 0)
        self.assertEqual(result['failed_symbols'], [])
        upsert_daily_price_context_mock.assert_called_once()


if __name__ == '__main__':
    unittest.main()
