import unittest

from app.crypto.market_data import CryptoMarketDataValidationError, UpbitCryptoMarketDataService
from app.crypto.strategy_context import CryptoStrategyContextService


class FakeUpbitClient:
    async def fetch_tickers(self, *, markets):
        return [
            {
                'market': market,
                'trade_timestamp': 1781391963000,
                'timestamp': 1781391964000,
                'opening_price': 100,
                'high_price': 118,
                'low_price': 98,
                'trade_price': 115,
                'prev_closing_price': 110,
                'change': 'RISE',
                'change_price': 5,
                'change_rate': 0.04545,
                'signed_change_price': 5,
                'signed_change_rate': 0.04545,
                'trade_volume': 1,
                'acc_trade_price': 1_000_000_000,
                'acc_trade_price_24h': 70_000_000_000,
                'acc_trade_volume': 10,
                'acc_trade_volume_24h': 500,
            }
            for market in markets
        ]

    async def fetch_day_candles(self, *, market, count, to=None):
        closes = [100, 103, 106, 108, 110, 115]
        rows = []
        for offset, close in enumerate(closes, start=9):
            rows.append(
                {
                    'market': market,
                    'candle_date_time_utc': f'2026-06-{offset:02d}T00:00:00',
                    'candle_date_time_kst': f'2026-06-{offset:02d}T09:00:00',
                    'opening_price': close - 1,
                    'high_price': close + 1,
                    'low_price': close - 2,
                    'trade_price': close,
                    'timestamp': 1781391964000,
                    'candle_acc_trade_price': 60_000_000_000,
                    'candle_acc_trade_volume': 100,
                }
            )
        return list(reversed(rows))

    async def fetch_minute_candles(self, *, market, unit, count, to=None):
        raise AssertionError('strategy context should not request minute candles')

    async def fetch_markets(self, *, is_details=True):
        raise AssertionError('strategy context should not request market metadata')


class IncompleteUpbitClient(FakeUpbitClient):
    async def fetch_tickers(self, *, markets):
        return []

    async def fetch_day_candles(self, *, market, count, to=None):
        return [
            {
                'market': market,
                'candle_date_time_utc': '2026-06-14T00:00:00',
                'candle_date_time_kst': '2026-06-14T09:00:00',
                'opening_price': 100,
                'high_price': 105,
                'low_price': 95,
                'trade_price': 100,
                'timestamp': 1781391964000,
                'candle_acc_trade_price': 1_000_000_000,
                'candle_acc_trade_volume': 10,
            },
            {
                'market': market,
                'candle_date_time_utc': '2026-06-13T00:00:00',
                'candle_date_time_kst': '2026-06-13T09:00:00',
                'opening_price': 101,
                'high_price': 103,
                'low_price': 99,
                'trade_price': None,
                'timestamp': 1781305564000,
                'candle_acc_trade_price': 1_000_000_000,
                'candle_acc_trade_volume': 10,
            },
        ]


class CryptoStrategyContextServiceTests(unittest.IsolatedAsyncioTestCase):
    async def test_strategy_context_returns_deterministic_read_model_for_whitelisted_crypto(self):
        market_data_service = UpbitCryptoMarketDataService(client=FakeUpbitClient())
        service = CryptoStrategyContextService(market_data_service=market_data_service)

        result = await service.get_strategy_context(markets='KRW-BTC', count=6)

        self.assertEqual(result['provider'], 'upbit')
        self.assertEqual(result['asset_class'], 'crypto')
        self.assertEqual(result['venue'], 'UPBIT')
        self.assertEqual(result['status'], 'available')
        self.assertFalse(result['trading_enabled_in_core'])
        self.assertTrue(result['decision_support_only'])
        self.assertFalse(result['private_api_used'])
        item = result['items'][0]
        self.assertEqual(item['symbol'], 'KRW-BTC')
        self.assertEqual(item['quote_currency'], 'KRW')
        self.assertEqual(item['base_currency'], 'BTC')
        self.assertEqual(item['status'], 'available')
        self.assertEqual(item['trend_state'], 'uptrend')
        self.assertGreater(item['momentum_score'], 0)
        self.assertEqual(item['liquidity_tier'], 'high')
        self.assertEqual(item['observed_window']['observed_count'], 6)
        self.assertTrue(item['session_flags']['weekend_session'])
        self.assertFalse(item['session_flags']['krx_market_close_applies'])
        self.assertIsNotNone(item['invalidation_level'])
        self.assertIsNotNone(item['target_level'])
        self.assertEqual(item['source']['rule_version'], 'crypto_strategy_context_v1')

    async def test_strategy_context_marks_incomplete_data_partial_without_fabricating_metrics(self):
        market_data_service = UpbitCryptoMarketDataService(client=IncompleteUpbitClient())
        service = CryptoStrategyContextService(market_data_service=market_data_service)

        result = await service.get_strategy_context(markets='KRW-BTC', count=5)

        self.assertEqual(result['status'], 'partial')
        item = result['items'][0]
        self.assertEqual(item['status'], 'partial')
        self.assertEqual(item['trend_state'], 'unavailable')
        self.assertIsNone(item['momentum_score'])
        self.assertIn('ticker', item['missing_fields'])
        self.assertIn('complete_candles', item['missing_fields'])
        self.assertIn('minimum_strategy_window', item['missing_fields'])
        self.assertTrue(any('data_state=partial' in reason for reason in item['reasons']))

    async def test_strategy_context_rejects_non_whitelisted_crypto_markets(self):
        market_data_service = UpbitCryptoMarketDataService(client=FakeUpbitClient())
        service = CryptoStrategyContextService(market_data_service=market_data_service)

        with self.assertRaisesRegex(CryptoMarketDataValidationError, 'Unsupported crypto market'):
            await service.get_strategy_context(markets='KRW-XRP', count=5)


if __name__ == '__main__':
    unittest.main()
