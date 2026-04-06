import unittest

from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.api import routes_fx
from app.fx.service import KoreaEximRateNotFoundError
from app.integrations.fx.client import KoreaEximAPIError, KoreaEximConfigurationError


class FakeFXService:
    async def get_exchange_rates(self, **kwargs):
        return {
            'provider': 'koreaexim',
            'data_code': 'AP01',
            'requested_date': kwargs.get('search_date') or '20260406',
            'effective_date': kwargs.get('search_date') or '20260406',
            'fallback_applied': False,
            'currency': kwargs.get('currency'),
            'count': 1,
            'items': [{'currency_code': kwargs.get('currency') or 'USD', 'exchange_rate': 1444.5}],
        }


class MissingConfigService:
    async def get_exchange_rates(self, **kwargs):
        raise KoreaEximConfigurationError('KOREAEXIM_API_KEY is not configured')


class NotFoundService:
    async def get_exchange_rates(self, **kwargs):
        raise KoreaEximRateNotFoundError('No Korea Exim FX rates found')


class UpstreamErrorService:
    async def get_exchange_rates(self, **kwargs):
        raise KoreaEximAPIError('Korea Exim returned result=4')


class GenericValueErrorService:
    async def get_exchange_rates(self, **kwargs):
        raise ValueError('unexpected upstream field')


class KoreaEximRouteTests(unittest.TestCase):
    def setUp(self):
        self.app = FastAPI()
        self.app.include_router(routes_fx.router)

    def test_rates_route_returns_payload(self):
        original_service = routes_fx.fx_service
        routes_fx.fx_service = FakeFXService()
        try:
            client = TestClient(self.app)
            response = client.get('/fx/koreaexim/rates', params={'search_date': '20260406', 'currency': 'USD'})
        finally:
            routes_fx.fx_service = original_service

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()['items'][0]['currency_code'], 'USD')

    def test_rates_route_validates_search_date_format(self):
        client = TestClient(self.app)
        response = client.get('/fx/koreaexim/rates', params={'search_date': '2026-04-06'})

        self.assertEqual(response.status_code, 422)

    def test_rates_route_rejects_nonexistent_calendar_dates(self):
        client = TestClient(self.app)
        response = client.get('/fx/koreaexim/rates', params={'search_date': '20260230'})

        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json()['detail'], 'Invalid search_date: 20260230')

    def test_rates_route_maps_missing_config_to_503(self):
        original_service = routes_fx.fx_service
        routes_fx.fx_service = MissingConfigService()
        try:
            client = TestClient(self.app)
            response = client.get('/fx/koreaexim/rates')
        finally:
            routes_fx.fx_service = original_service

        self.assertEqual(response.status_code, 503)
        self.assertEqual(response.json()['detail'], 'KOREAEXIM_API_KEY is not configured')

    def test_rates_route_maps_not_found_to_404(self):
        original_service = routes_fx.fx_service
        routes_fx.fx_service = NotFoundService()
        try:
            client = TestClient(self.app)
            response = client.get('/fx/koreaexim/rates')
        finally:
            routes_fx.fx_service = original_service

        self.assertEqual(response.status_code, 404)
        self.assertEqual(response.json()['detail'], 'No Korea Exim FX rates found')

    def test_rates_route_maps_upstream_errors_to_502(self):
        original_service = routes_fx.fx_service
        routes_fx.fx_service = UpstreamErrorService()
        try:
            client = TestClient(self.app)
            response = client.get('/fx/koreaexim/rates')
        finally:
            routes_fx.fx_service = original_service

        self.assertEqual(response.status_code, 502)
        self.assertIn('Korea Exim request failed', response.json()['detail'])

    def test_rates_route_does_not_map_generic_value_errors_to_400(self):
        original_service = routes_fx.fx_service
        routes_fx.fx_service = GenericValueErrorService()
        try:
            client = TestClient(self.app)
            response = client.get('/fx/koreaexim/rates')
        finally:
            routes_fx.fx_service = original_service

        self.assertEqual(response.status_code, 500)
        self.assertIn('Korea Exim FX service failed', response.json()['detail'])


if __name__ == '__main__':
    unittest.main()
