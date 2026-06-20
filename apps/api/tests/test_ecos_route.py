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
                        'STAT_NAME': 'Test Series',
                        'CYCLE': kwargs['cycle'],
                        'TIME': kwargs['start_period'],
                        'DATA_VALUE': '3.50',
                        'ITEM_CODE1': kwargs.get('item_code_1'),
                        'ITEM_NAME1': 'Test Item',
                        'UNIT_NAME': '%',
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


class FakeMacroContextService:
    def __init__(self):
        self.calls = []

    def get_context(self, **kwargs):
        self.calls.append({'method': 'get_context', **kwargs})
        return {
            'schema_version': 'macro_context_v1',
            'status': 'available',
            'macro_regime_summary': {'llm_generated': False},
            'usage_policy': {'contextual_input_only': True, 'direct_buy_sell_signal': False},
        }

    def refresh_context(self, **kwargs):
        self.calls.append({'method': 'refresh_context', **kwargs})
        return {
            'status': 'completed',
            'observations_upserted': 3,
            'source_statuses': {'fred': {'status': 'disabled_missing_api_key'}},
        }


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
        self.assertEqual(response.json()['series']['stat_code'], '722Y001')
        self.assertEqual(response.json()['observations'][0]['value'], 3.5)
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

    def test_ecos_series_route_accepts_series_alias(self):
        original_client = routes_macro.ecos_client
        fake_client = FakeECOSClient()
        routes_macro.ecos_client = fake_client
        try:
            client = TestClient(self.app)
            response = client.get(
                '/macro/ecos/series',
                params={
                    'series_alias': 'kr_base_rate',
                    'start_period': '202401',
                    'end_period': '202403',
                },
            )
        finally:
            routes_macro.ecos_client = original_client

        self.assertEqual(response.status_code, 200)
        self.assertEqual(fake_client.calls[0]['stat_code'], '722Y001')
        self.assertEqual(fake_client.calls[0]['item_code_1'], '0101000')
        self.assertEqual(fake_client.calls[0]['cycle'], 'M')
        self.assertEqual(response.json()['request']['series_alias'], 'kr_base_rate')

    def test_ecos_catalog_route_lists_aliases(self):
        client = TestClient(self.app)
        response = client.get('/macro/ecos/catalog')

        self.assertEqual(response.status_code, 200)
        aliases = response.json()['aliases']
        self.assertTrue(any(item['alias'] == 'kr_base_rate' for item in aliases))

    def test_macro_context_route_returns_compact_context(self):
        original_service = routes_macro.macro_context_service
        fake_service = FakeMacroContextService()
        routes_macro.macro_context_service = fake_service
        try:
            client = TestClient(self.app)
            response = client.get('/macro/context', params={'as_of_date': '2026-06-20'})
        finally:
            routes_macro.macro_context_service = original_service

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload['schema_version'], 'macro_context_v1')
        self.assertEqual(payload['status'], 'available')
        self.assertTrue(payload['usage_policy']['contextual_input_only'])
        self.assertEqual(fake_service.calls[0], {'method': 'get_context', 'as_of_date': '2026-06-20'})

    def test_macro_context_refresh_route_reports_disabled_keyed_source(self):
        original_service = routes_macro.macro_context_service
        fake_service = FakeMacroContextService()
        routes_macro.macro_context_service = fake_service
        try:
            client = TestClient(self.app)
            response = client.post('/macro/context/refresh', params={'force': 'true'})
        finally:
            routes_macro.macro_context_service = original_service

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload['status'], 'completed')
        self.assertEqual(payload['source_statuses']['fred']['status'], 'disabled_missing_api_key')
        self.assertEqual(fake_service.calls[0], {'method': 'refresh_context', 'force': True, 'trigger': 'manual_api'})

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

    def test_ecos_series_route_requires_stat_code_or_alias(self):
        client = TestClient(self.app)
        response = client.get(
            '/macro/ecos/series',
            params={
                'start_period': '202401',
                'end_period': '202403',
            },
        )

        self.assertEqual(response.status_code, 400)
        self.assertEqual(
            response.json()['detail'],
            'One of stat_code/series_id or series_alias is required',
        )


if __name__ == '__main__':
    unittest.main()
