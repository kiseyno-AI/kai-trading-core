import unittest

import httpx

from app.integrations.nasdaq_trader.client import NasdaqTraderClient


class NasdaqTraderClientTests(unittest.IsolatedAsyncioTestCase):
    async def test_fetches_and_combines_official_symbol_directories(self):
        requested_paths: list[str] = []

        def handler(request: httpx.Request) -> httpx.Response:
            requested_paths.append(request.url.path)
            if request.url.path.endswith('/nasdaqlisted.txt'):
                text = (
                    'Symbol|Security Name|Market Category|Test Issue|Financial Status|Round Lot Size|ETF|NextShares\n'
                    'AAPL|Apple Inc. - Common Stock|Q|N|N|100|N|N\n'
                    'File Creation Time: 0819202617:00|||||||\n'
                )
            else:
                text = (
                    'ACT Symbol|Security Name|Exchange|CQS Symbol|ETF|Round Lot Size|Test Issue|NASDAQ Symbol\n'
                    'IBM|International Business Machines Corporation Common Stock |N|IBM|N|100|N|IBM\n'
                    'File Creation Time: 0819202617:00|||||||\n'
                )
            return httpx.Response(200, text=text, request=request)

        client = NasdaqTraderClient(transport=httpx.MockTransport(handler))

        result = await client.fetch_universe()

        self.assertEqual(requested_paths, [
            '/dynamic/SymDir/nasdaqlisted.txt',
            '/dynamic/SymDir/otherlisted.txt',
        ])
        self.assertEqual([item.symbol for item in result], ['AAPL', 'IBM'])


if __name__ == '__main__':
    unittest.main()
