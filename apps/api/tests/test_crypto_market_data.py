import unittest

from app.crypto.market_data import CryptoMarketDataValidationError, UpbitCryptoMarketDataService


class FakeUpbitClient:
    def __init__(self):
        self.calls = []

    async def fetch_markets(self, *, is_details=True):
        self.calls.append(('markets', {'is_details': is_details}))
        return [
            {
                'market': 'KRW-BTC',
                'korean_name': '비트코인',
                'english_name': 'Bitcoin',
                'market_warning': 'NONE',
            },
            {
                'market': 'KRW-ETH',
                'korean_name': '이더리움',
                'english_name': 'Ethereum',
                'market_warning': 'NONE',
            },
        ]

    async def fetch_tickers(self, *, markets):
        self.calls.append(('tickers', {'markets': markets}))
        return [
            {
                'market': market,
                'trade_timestamp': 1781391963000,
                'timestamp': 1781391964000,
                'opening_price': 145000000,
                'high_price': 146000000,
                'low_price': 144000000,
                'trade_price': 145831000,
                'prev_closing_price': 145000000,
                'change': 'RISE',
                'change_price': 831000,
                'change_rate': 0.0057310344,
                'signed_change_price': 831000,
                'signed_change_rate': 0.0057310344,
                'trade_volume': 0.01,
                'acc_trade_price': 1000000,
                'acc_trade_price_24h': 344100708901.19,
                'acc_trade_volume': 1.2,
                'acc_trade_volume_24h': 2389.39562434,
            }
            for market in markets
        ]

    async def fetch_day_candles(self, *, market, count, to=None):
        self.calls.append(('day_candles', {'market': market, 'count': count, 'to': to}))
        return [
            {
                'market': market,
                'candle_date_time_utc': '2026-06-13T00:00:00',
                'candle_date_time_kst': '2026-06-13T09:00:00',
                'opening_price': 145000000,
                'high_price': 146000000,
                'low_price': 144000000,
                'trade_price': 145831000,
                'timestamp': 1781391964000,
                'candle_acc_trade_price': 4022470467.03,
                'candle_acc_trade_volume': 27.58904602,
            }
        ]

    async def fetch_minute_candles(self, *, market, unit, count, to=None):
        self.calls.append(('minute_candles', {'market': market, 'unit': unit, 'count': count, 'to': to}))
        return [
            {
                'market': market,
                'candle_date_time_utc': '2026-06-13T22:45:00',
                'candle_date_time_kst': '2026-06-14T07:45:00',
                'opening_price': 145800000,
                'high_price': 145900000,
                'low_price': 145700000,
                'trade_price': 145831000,
                'timestamp': 1781391599000,
                'candle_acc_trade_price': 4022470467.03,
                'candle_acc_trade_volume': 27.58904602,
            }
        ]


class UpbitCryptoMarketDataServiceTests(unittest.IsolatedAsyncioTestCase):
    async def test_list_markets_returns_explicit_read_only_crypto_instruments(self):
        service = UpbitCryptoMarketDataService(client=FakeUpbitClient())

        result = await service.list_markets()

        self.assertEqual(result['provider'], 'upbit')
        self.assertEqual(result['asset_class'], 'crypto')
        self.assertEqual(result['venue'], 'UPBIT')
        self.assertFalse(result['trading_enabled_in_core'])
        self.assertEqual(result['whitelist'], ['KRW-BTC', 'KRW-ETH'])
        self.assertEqual(result['items'][0]['market'], 'KRW-BTC')
        self.assertEqual(result['items'][0]['instrument_type'], 'spot_crypto_pair')
        self.assertEqual(result['items'][0]['quote_currency'], 'KRW')
        self.assertEqual(result['items'][0]['base_currency'], 'BTC')
        self.assertFalse(result['items'][0]['trading_enabled_in_core'])

    async def test_get_tickers_normalizes_source_and_rejects_non_whitelisted_markets(self):
        service = UpbitCryptoMarketDataService(client=FakeUpbitClient())

        result = await service.get_tickers(markets='KRW-BTC,KRW-ETH')

        self.assertEqual(result['requested_markets'], ['KRW-BTC', 'KRW-ETH'])
        self.assertEqual(result['source']['endpoint'], '/v1/ticker')
        self.assertFalse(result['source']['auth_required'])
        self.assertFalse(result['source']['order_submission_enabled'])
        self.assertEqual(result['items'][0]['price'], 145831000)
        self.assertEqual(result['items'][0]['source']['provider'], 'upbit')
        self.assertFalse(result['items'][0]['trading_enabled_in_core'])

        with self.assertRaisesRegex(CryptoMarketDataValidationError, 'Unsupported crypto market'):
            await service.get_tickers(markets='KRW-XRP')

    async def test_get_candles_supports_day_and_minute_intervals(self):
        fake_client = FakeUpbitClient()
        service = UpbitCryptoMarketDataService(client=fake_client)

        day = await service.get_candles(market='KRW-BTC', interval='day', count=2)
        minute = await service.get_candles(market='KRW-BTC', interval='minute', unit=15, count=2)

        self.assertEqual(day['source']['endpoint'], '/v1/candles/days')
        self.assertIsNone(day['unit'])
        self.assertEqual(day['items'][0]['interval'], 'day')
        self.assertEqual(minute['source']['endpoint'], '/v1/candles/minutes/15')
        self.assertEqual(minute['unit'], 15)
        self.assertEqual(minute['items'][0]['unit'], 15)
        self.assertFalse(minute['trading_enabled_in_core'])
        self.assertIn(('day_candles', {'market': 'KRW-BTC', 'count': 2, 'to': None}), fake_client.calls)
        self.assertIn(('minute_candles', {'market': 'KRW-BTC', 'unit': 15, 'count': 2, 'to': None}), fake_client.calls)

    async def test_get_candles_rejects_unsupported_minute_units(self):
        service = UpbitCryptoMarketDataService(client=FakeUpbitClient())

        with self.assertRaisesRegex(CryptoMarketDataValidationError, 'unit must be one of'):
            await service.get_candles(market='KRW-BTC', interval='minute', unit=2, count=1)


if __name__ == '__main__':
    unittest.main()
