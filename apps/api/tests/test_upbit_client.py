import unittest

from app.integrations.upbit.client import UpbitQuotationAPIError, UpbitQuotationClient


class FakeResponse:
    def __init__(self, payload, status_code=200, text=''):
        self._payload = payload
        self.status_code = status_code
        self.text = text

    def json(self):
        return self._payload


class TestableUpbitClient(UpbitQuotationClient):
    __test__ = False

    def __init__(self, response):
        super().__init__()
        self.response = response
        self.calls = []

    async def _get(self, path, params=None):
        self.calls.append({'path': path, 'params': params})
        return self.response


class UpbitQuotationClientTests(unittest.IsolatedAsyncioTestCase):
    async def test_fetch_tickers_uses_public_ticker_path_and_market_list(self):
        client = TestableUpbitClient(
            [
                {
                    'market': 'KRW-BTC',
                    'trade_price': 145831000,
                }
            ]
        )

        result = await client.fetch_tickers(markets=['KRW-BTC', 'KRW-ETH'])

        self.assertEqual(result[0]['market'], 'KRW-BTC')
        self.assertEqual(client.calls[0]['path'], 'ticker')
        self.assertEqual(client.calls[0]['params']['markets'], 'KRW-BTC,KRW-ETH')

    async def test_fetch_day_candles_uses_public_candle_path(self):
        client = TestableUpbitClient(
            [
                {
                    'market': 'KRW-BTC',
                    'trade_price': 145831000,
                }
            ]
        )

        await client.fetch_day_candles(market='KRW-BTC', count=30, to='2026-06-14T00:00:00Z')

        self.assertEqual(client.calls[0]['path'], 'candles/days')
        self.assertEqual(client.calls[0]['params']['market'], 'KRW-BTC')
        self.assertEqual(client.calls[0]['params']['count'], 30)
        self.assertEqual(client.calls[0]['params']['to'], '2026-06-14T00:00:00Z')

    async def test_rejects_non_list_payloads(self):
        client = TestableUpbitClient({'error': 'bad'})

        with self.assertRaises(UpbitQuotationAPIError):
            await client.fetch_tickers(markets=['KRW-BTC'])


if __name__ == '__main__':
    unittest.main()
