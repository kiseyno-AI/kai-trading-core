import unittest

from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.api import routes_dart
from app.integrations.dart.client import DartAPIError, DartLookupError


class FakeRouteService:
    async def get_company_snapshot(self, **kwargs):
        return {
            'lookup': {
                'corp_code': '00126380',
                'corp_name': '삼성전자',
                'stock_code': kwargs.get('stock_code') or '005930',
                'modify_date': '20250101',
            },
            'company': {'corp_name': '삼성전자'},
            'recent_filings': {'items': []},
        }

    async def get_company_fundamentals(self, **kwargs):
        return {
            'lookup': {
                'corp_code': '00126380',
                'corp_name': '삼성전자',
                'stock_code': kwargs.get('stock_code') or '005930',
            },
            'request': {
                'fiscal_year': str(kwargs['fiscal_year']),
                'period': kwargs.get('period', 'annual'),
                'report_code': kwargs.get('report_code') or '11011',
            },
            'highlights': {'revenue': {'current_amount': '1000'}},
            'statements': {'items': []},
        }


class LookupErrorService:
    async def get_company_snapshot(self, **kwargs):
        raise DartLookupError('Unknown stock_code: 999999')


class UpstreamErrorService:
    async def get_company_snapshot(self, **kwargs):
        raise DartAPIError('DART upstream unavailable')

    async def get_company_fundamentals(self, **kwargs):
        raise DartAPIError('DART upstream unavailable')


class ValidationErrorService:
    async def get_company_fundamentals(self, **kwargs):
        raise ValueError('period and report_code refer to different filing windows')


class DartRouteTests(unittest.TestCase):
    def setUp(self):
        self.app = FastAPI()
        self.app.include_router(routes_dart.router)

    def test_company_snapshot_route_returns_payload(self):
        original_service = routes_dart.dart_service
        routes_dart.dart_service = FakeRouteService()
        try:
            client = TestClient(self.app)
            response = client.get('/disclosures/dart/company-snapshot', params={'stock_code': '005930'})
        finally:
            routes_dart.dart_service = original_service

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()['lookup']['stock_code'], '005930')

    def test_company_snapshot_route_requires_one_lookup_key(self):
        client = TestClient(self.app)
        response = client.get('/disclosures/dart/company-snapshot')

        self.assertEqual(response.status_code, 400)
        self.assertEqual(
            response.json()['detail'],
            'One of corp_code, stock_code, or corp_name is required',
        )

    def test_company_snapshot_route_maps_lookup_errors_to_404(self):
        original_service = routes_dart.dart_service
        routes_dart.dart_service = LookupErrorService()
        try:
            client = TestClient(self.app)
            response = client.get('/disclosures/dart/company-snapshot', params={'stock_code': '999999'})
        finally:
            routes_dart.dart_service = original_service

        self.assertEqual(response.status_code, 404)
        self.assertEqual(response.json()['detail'], 'Unknown stock_code: 999999')

    def test_company_snapshot_route_maps_upstream_errors_to_502(self):
        original_service = routes_dart.dart_service
        routes_dart.dart_service = UpstreamErrorService()
        try:
            client = TestClient(self.app)
            response = client.get('/disclosures/dart/company-snapshot', params={'stock_code': '005930'})
        finally:
            routes_dart.dart_service = original_service

        self.assertEqual(response.status_code, 502)
        self.assertIn('DART request failed', response.json()['detail'])

    def test_company_fundamentals_route_returns_payload(self):
        original_service = routes_dart.dart_service
        routes_dart.dart_service = FakeRouteService()
        try:
            client = TestClient(self.app)
            response = client.get(
                '/disclosures/dart/fundamentals',
                params={'stock_code': '005930', 'fiscal_year': '2025', 'period': 'annual'},
            )
        finally:
            routes_dart.dart_service = original_service

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()['request']['report_code'], '11011')

    def test_company_fundamentals_route_maps_validation_errors_to_400(self):
        original_service = routes_dart.dart_service
        routes_dart.dart_service = ValidationErrorService()
        try:
            client = TestClient(self.app)
            response = client.get(
                '/disclosures/dart/fundamentals',
                params={
                    'stock_code': '005930',
                    'fiscal_year': '2025',
                    'period': 'annual',
                    'report_code': '11013',
                },
            )
        finally:
            routes_dart.dart_service = original_service

        self.assertEqual(response.status_code, 400)
        self.assertEqual(
            response.json()['detail'],
            'period and report_code refer to different filing windows',
        )


if __name__ == '__main__':
    unittest.main()
