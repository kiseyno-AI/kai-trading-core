import unittest
from unittest.mock import patch

import httpx

from app.integrations.bok.client import ECOSClient


class FakeResponse:
    status_code = 200
    text = ''
    headers = {}

    def json(self):
        return {'StatisticSearch': {'row': []}}


class FakeAsyncClient:
    last_url = None

    def __init__(self, *args, **kwargs):
        pass

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    def build_request(self, method, url, headers=None, params=None, json=None):
        self.__class__.last_url = str(httpx.URL(url, params=params))
        return httpx.Request(method, url, headers=headers, params=params, json=json)

    async def send(self, request):
        return FakeResponse()


class ECOSClientTests(unittest.IsolatedAsyncioTestCase):
    async def test_fetch_series_preserves_empty_item_code_slots_before_later_codes(self):
        client = ECOSClient()
        client.base_url = 'https://ecos.example.test/api'
        client.api_key = 'test-key'

        with (
            patch('app.integrations.bok.client.httpx.AsyncClient', FakeAsyncClient),
            patch('app.integrations.bok.client.log_fetch_event') as log_fetch_event,
        ):
            await client.fetch_series(
                stat_code='STAT123',
                cycle='M',
                start_period='202401',
                end_period='202403',
                item_code_2='X',
            )

        self.assertEqual(
            FakeAsyncClient.last_url,
            'https://ecos.example.test/api/StatisticSearch/test-key/json/kr/1/100/STAT123/M/202401/202403//X',
        )
        self.assertEqual(log_fetch_event.call_args.kwargs['operation'], 'fetch_series')
        self.assertEqual(log_fetch_event.call_args.kwargs['status'], 'completed')


if __name__ == '__main__':
    unittest.main()
