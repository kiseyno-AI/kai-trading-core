import unittest

from app.brokers.kis.client import KISClient


class CapturingKISClient(KISClient):
    def __init__(self):
        super().__init__(use_mock=True)
        self.calls = []

    async def _get(self, path, *, tr_id, params):
        self.calls.append((path, tr_id, params))
        return {'output': {'last': '220.10'}}


class PaginatedKISClient(KISClient):
    def __init__(self):
        super().__init__(use_mock=True)
        self.calls = []
        self.responses = [
            ({'output2': [{'symb': 'AAPL'}], 'keyb': 'NEXT'}, 'M'),
            ({'output2': [{'symb': 'MSFT'}], 'keyb': ''}, ''),
        ]

    async def _get_response(self, path, *, tr_id, params, tr_cont=''):
        self.calls.append((path, tr_id, dict(params), tr_cont))
        return self.responses.pop(0)


class KISUSMarketDataClientTests(unittest.IsolatedAsyncioTestCase):
    async def test_fetches_us_quote_with_price_exchange_code(self):
        client = CapturingKISClient()

        await client.fetch_overseas_quote(exchange='NASDAQ', symbol='AAPL')

        self.assertEqual(client.calls, [(
            '/uapi/overseas-price/v1/quotations/price',
            'HHDFS00000300',
            {'AUTH': '', 'EXCD': 'NAS', 'SYMB': 'AAPL'},
        )])

    async def test_fetches_adjusted_daily_history(self):
        client = CapturingKISClient()

        await client.fetch_overseas_daily_prices(
            exchange='NYSE', symbol='BRK/B', as_of_date='20260819', adjusted=True
        )

        self.assertEqual(client.calls[0][0], '/uapi/overseas-price/v1/quotations/dailyprice')
        self.assertEqual(client.calls[0][1], 'HHDFS76240000')
        self.assertEqual(client.calls[0][2], {
            'AUTH': '', 'EXCD': 'NYS', 'SYMB': 'BRK/B', 'GUBN': '0', 'BYMD': '20260819', 'MODP': '1'
        })

    async def test_market_cap_ranking_paginates_with_keyb(self):
        client = PaginatedKISClient()

        result = await client.fetch_overseas_market_cap_ranking(exchange='NASDAQ', max_pages=3)

        self.assertEqual([item['symb'] for item in result['output2']], ['AAPL', 'MSFT'])
        self.assertEqual(client.calls[1][2]['KEYB'], 'NEXT')
        self.assertEqual(client.calls[1][3], 'N')
        self.assertEqual(result['page_count'], 2)


if __name__ == '__main__':
    unittest.main()
