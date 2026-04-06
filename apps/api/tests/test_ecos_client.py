import unittest
from unittest.mock import patch

from app.integrations.bok.client import ECOSClient


class FakeResponse:
    status_code = 200
    text = ''

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

    async def get(self, url):
        self.__class__.last_url = url
        return FakeResponse()


class ECOSClientTests(unittest.IsolatedAsyncioTestCase):
    async def test_fetch_series_preserves_empty_item_code_slots_before_later_codes(self):
        client = ECOSClient()
        client.base_url = 'https://ecos.example.test/api'
        client.api_key = 'test-key'

        with patch('app.integrations.bok.client.httpx.AsyncClient', FakeAsyncClient):
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


if __name__ == '__main__':
    unittest.main()
