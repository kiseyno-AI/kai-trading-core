import unittest

import httpx

from app.integrations.sec_edgar.client import (
    SECEdgarClient,
    SECEdgarConfigurationError,
    normalize_cik,
    parse_company_ticker_exchange_mapping,
)


class SECEdgarClientTests(unittest.IsolatedAsyncioTestCase):
    def test_normalizes_cik_to_ten_digits(self):
        self.assertEqual(normalize_cik(320193), '0000320193')
        self.assertEqual(normalize_cik('CIK0000320193'), '0000320193')

    def test_rejects_invalid_cik(self):
        with self.assertRaises(ValueError):
            normalize_cik('AAPL')

    def test_parses_official_ticker_exchange_mapping(self):
        payload = {
            'fields': ['cik', 'name', 'ticker', 'exchange'],
            'data': [[320193, 'Apple Inc.', 'aapl', 'Nasdaq'], [1067983, 'Berkshire Hathaway', 'BRK-B', 'NYSE']],
        }

        rows = parse_company_ticker_exchange_mapping(payload)

        self.assertEqual(rows[0].cik, '0000320193')
        self.assertEqual(rows[0].ticker, 'AAPL')
        self.assertEqual(rows[1].exchange, 'NYSE')

    async def test_declared_user_agent_is_required(self):
        with self.assertRaises(SECEdgarConfigurationError):
            SECEdgarClient(user_agent='')

    async def test_fetches_submissions_and_companyfacts_with_declared_headers(self):
        requests: list[httpx.Request] = []

        def handler(request: httpx.Request) -> httpx.Response:
            requests.append(request)
            return httpx.Response(200, json={'cik': '0000320193'}, request=request)

        client = SECEdgarClient(
            user_agent='Kai Trading admin@example.com',
            transport=httpx.MockTransport(handler),
            requests_per_second=10,
        )

        await client.fetch_submissions('320193')
        await client.fetch_companyfacts('0000320193')

        self.assertEqual(requests[0].url.path, '/submissions/CIK0000320193.json')
        self.assertEqual(requests[1].url.path, '/api/xbrl/companyfacts/CIK0000320193.json')
        self.assertEqual(requests[0].headers['user-agent'], 'Kai Trading admin@example.com')
        self.assertEqual(requests[0].headers['accept-encoding'], 'gzip, deflate')


if __name__ == '__main__':
    unittest.main()
