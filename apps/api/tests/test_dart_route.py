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


class FakeSyncService:
    async def sync_recent_disclosures(self, **kwargs):
        return {
            'run_id': 7,
            'status': 'completed',
            'events_seen': 3,
            'financial_report_events': 2,
        }

    def inspect_sync(self, **kwargs):
        return {
            'latest_run': {'id': 7, 'status': 'completed'},
            'summary': {'total_events': 3, 'financial_report_events': 2},
            'runs': [{'id': 7, 'status': 'completed'}],
            'events': [{'rcept_no': '20260407000001'}],
        }


class LookupErrorService:
    async def get_company_snapshot(self, **kwargs):
        raise DartLookupError('Unknown stock_code: 999999')


class UpstreamErrorService:
    async def get_company_snapshot(self, **kwargs):
        raise DartAPIError('DART upstream unavailable')


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

    def test_sync_events_route_returns_sync_payload(self):
        original_service = routes_dart.dart_disclosure_sync_service
        routes_dart.dart_disclosure_sync_service = FakeSyncService()
        try:
            client = TestClient(self.app)
            response = client.post('/disclosures/dart/sync/events', params={'days': 5})
        finally:
            routes_dart.dart_disclosure_sync_service = original_service

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()['run_id'], 7)
        self.assertEqual(response.json()['financial_report_events'], 2)

    def test_sync_inspect_route_returns_runs_and_events(self):
        original_service = routes_dart.dart_disclosure_sync_service
        routes_dart.dart_disclosure_sync_service = FakeSyncService()
        try:
            client = TestClient(self.app)
            response = client.get('/disclosures/dart/sync/inspect', params={'financial_only': 'true'})
        finally:
            routes_dart.dart_disclosure_sync_service = original_service

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()['latest_run']['id'], 7)
        self.assertEqual(response.json()['summary']['total_events'], 3)


if __name__ == '__main__':
    unittest.main()
