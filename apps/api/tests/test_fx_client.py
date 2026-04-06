import unittest

from app.integrations.fx.client import KoreaEximAPIError, KoreaEximClient, KoreaEximConfigurationError
from app.core.settings import settings


class FakeResponse:
    def __init__(self, payload, status_code=200, text=''):
        self._payload = payload
        self.status_code = status_code
        self.text = text

    def json(self):
        return self._payload


class TestableKoreaEximClient(KoreaEximClient):
    def __init__(self, response):
        super().__init__()
        self.response = response
        self.last_params = None

    async def _get(self, params):
        self.last_params = params
        return self.response


class KoreaEximClientTests(unittest.IsolatedAsyncioTestCase):
    async def test_fetch_exchange_rates_requires_api_key(self):
        original_api_key = settings.koreaexim_api_key
        settings.koreaexim_api_key = ''
        client = TestableKoreaEximClient(FakeResponse([]))
        try:
            with self.assertRaises(KoreaEximConfigurationError):
                await client.fetch_exchange_rates(search_date='20260406')
        finally:
            settings.koreaexim_api_key = original_api_key

    async def test_fetch_exchange_rates_returns_list_payload(self):
        original_api_key = settings.koreaexim_api_key
        settings.koreaexim_api_key = 'test-key'
        client = TestableKoreaEximClient(FakeResponse([{'result': 1, 'cur_unit': 'USD'}]))
        try:
            result = await client.fetch_exchange_rates(search_date='20260406')
        finally:
            settings.koreaexim_api_key = original_api_key

        self.assertEqual(result[0]['cur_unit'], 'USD')
        self.assertEqual(client.last_params['searchdate'], '20260406')
        self.assertEqual(client.last_params['data'], 'AP01')

    async def test_fetch_exchange_rates_raises_for_upstream_result_error(self):
        original_api_key = settings.koreaexim_api_key
        settings.koreaexim_api_key = 'test-key'
        client = TestableKoreaEximClient(FakeResponse([{'result': 4}]))
        try:
            with self.assertRaises(KoreaEximAPIError):
                await client.fetch_exchange_rates(search_date='20260406')
        finally:
            settings.koreaexim_api_key = original_api_key


if __name__ == '__main__':
    unittest.main()
