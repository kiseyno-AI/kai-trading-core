import asyncio
import time
import unittest

import httpx

from app.integrations.sec_edgar import client as sec_client_module

from app.integrations.sec_edgar.client import (
    SECEdgarClient,
    SECEdgarConfigurationError,
    normalize_cik,
    parse_company_ticker_exchange_mapping,
)


class SECEdgarClientTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        # Keep process-global limiter state deterministic between tests while
        # still exercising sharing within each individual test.
        sec_client_module._PROCESS_RATE_LIMITER = None

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

    async def test_concurrent_clients_share_one_process_rate_limit(self):
        request_times: list[float] = []

        def handler(request: httpx.Request) -> httpx.Response:
            request_times.append(time.monotonic())
            return httpx.Response(200, json={'ok': True}, request=request)

        transport = httpx.MockTransport(handler)
        clients = [
            SECEdgarClient(
                user_agent='Kai Trading admin@example.com',
                transport=transport,
                requests_per_second=10,
            )
            for _ in range(2)
        ]

        await asyncio.gather(
            clients[0].fetch_submissions('320193'),
            clients[1].fetch_companyfacts('0000320193'),
        )

        self.assertEqual(len(request_times), 2)
        self.assertGreaterEqual(request_times[1] - request_times[0], 0.08)

    async def test_clients_with_different_rates_share_the_stricter_process_limit(self):
        request_times: list[float] = []

        def handler(request: httpx.Request) -> httpx.Response:
            request_times.append(time.monotonic())
            return httpx.Response(200, json={'ok': True}, request=request)

        transport = httpx.MockTransport(handler)
        fast_client = SECEdgarClient(
            user_agent='Kai Trading admin@example.com',
            transport=transport,
            requests_per_second=10,
        )
        strict_client = SECEdgarClient(
            user_agent='Kai Trading admin@example.com',
            transport=transport,
            requests_per_second=5,
        )

        await asyncio.gather(
            fast_client.fetch_submissions('320193'),
            strict_client.fetch_companyfacts('0000320193'),
        )

        self.assertEqual(len(request_times), 2)
        self.assertGreaterEqual(request_times[1] - request_times[0], 0.18)


if __name__ == '__main__':
    unittest.main()
