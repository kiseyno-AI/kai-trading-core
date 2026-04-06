import unittest

from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.api import routes_macro
from app.integrations.bok.client import ECOSAPIError, ECOSConfigurationError


class FakeECOSClient:
    def __init__(self):
        self.calls = []

    async def fetch_series(self, **kwargs):
        self.calls.append(kwargs)
        return {
            'StatisticSearch': {
                'row': [
                    {
                        'STAT_CODE': kwargs['stat_code'],
                        'CYCLE': kwargs['cycle'],
                    }
                ]
            }
        }


class FailingECOSClient:
    async def fetch_series(self, **kwargs):
        raise ECOSAPIError('upstream unavailable')


class UnconfiguredECOSClient:
    async def fetch_series(self, **kwargs):
        raise ECOSConfigurationError('BOK_ECOS_API_KEY is not configured')


class ECOSRouteTests(unittest.TestCase):
    def setUp(self):
        self.app = FastAPI()
        self.app.include_router(routes_macro.router)

    def test_ecos_series_route_returns_payload(self):
        original_client = routes_macro.ecos_client
        fake_client = FakeECOSClient()
        routes_macro.ecos_client = fake_client
        try:
            client = TestClient(self.app)
            response = client.get(
                '/macro/ecos/series',
                params={
                    'stat_code': '722Y001',
                    'cycle': 'M',
                    'start_period': '202401',
                    'end_period': '202403',
                    'item_code_1': '0101000',
                },
            )
        finally:
            routes_macro.ecos_client = original_client

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()['StatisticSearch']['row'][0]['STAT_CODE'], '722Y001')
        self.assertEqual(fake_client.calls[0]['item_code_1'], '0101000')

    def test_bok_series_route_keeps_legacy_query_shape(self):
        original_client = routes_macro.ecos_client
        fake_client = FakeECOSClient()
        routes_macro.ecos_client = fake_client
        try:
            client = TestClient(self.app)
            response = client.get(
                '/macro/bok/series',
                params={
                    'series_id': '817Y002',
                    'cycle': 'D',
                    'start_period': '20260401',
                    'end_period': '20260403',
                },
            )
        finally:
            routes_macro.ecos_client = original_client

        self.assertEqual(response.status_code, 200)
        self.assertEqual(fake_client.calls[0]['stat_code'], '817Y002')

    def test_ecos_series_route_maps_upstream_errors_to_502(self):
        original_client = routes_macro.ecos_client
        routes_macro.ecos_client = FailingECOSClient()
        try:
            client = TestClient(self.app)
            response = client.get(
                '/macro/ecos/series',
                params={
                    'stat_code': '722Y001',
                    'start_period': '202401',
                    'end_period': '202403',
                },
            )
        finally:
            routes_macro.ecos_client = original_client

        self.assertEqual(response.status_code, 502)
        self.assertIn('ECOS series fetch failed', response.json()['detail'])

    def test_ecos_series_route_maps_missing_config_to_500(self):
        original_client = routes_macro.ecos_client
        routes_macro.ecos_client = UnconfiguredECOSClient()
        try:
            client = TestClient(self.app)
            response = client.get(
                '/macro/ecos/series',
                params={
                    'stat_code': '722Y001',
                    'start_period': '202401',
                    'end_period': '202403',
                },
            )
        finally:
            routes_macro.ecos_client = original_client

        self.assertEqual(response.status_code, 500)
        self.assertEqual(response.json()['detail'], 'BOK_ECOS_API_KEY is not configured')


if __name__ == '__main__':
    unittest.main()
