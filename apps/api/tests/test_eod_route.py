
import unittest

from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.api import routes_eod


class FakeFreshnessService:
    def read(self):
        return {
            'today': {
                'trade_date': '2026-04-10',
                'eod_status': 'completed',
                'symbols_total': 3,
                'symbols_succeeded': 3,
                'symbols_failed': 0,
                'received': True,
            },
            'latest': {
                'price_trade_date': '2026-04-10',
                'technical_market_date': '2026-04-10',
                'action_market_date': '2026-04-10',
            },
        }


class FakeTechnicalSnapshotService:
    def __init__(self):
        self.compute_args = None
        self.inspect_args = None

    def compute_and_persist_for_market_date(self, **kwargs):
        self.compute_args = kwargs
        return {
            'market_date': kwargs['market_date'],
            'snapshots_persisted': 2,
            'snapshots_failed': 0,
            'items': [{'symbol': '005930'}],
        }

    def inspect(self, **kwargs):
        self.inspect_args = kwargs
        return {
            'market_date': kwargs['market_date'],
            'summary': {'entries': 1},
            'items': [{'symbol': '005930', 'trend_state': 'uptrend'}],
        }


class FakeActionSummaryService:
    def __init__(self):
        self.compute_args = None
        self.inspect_args = None

    def compute_and_persist_for_market_date(self, **kwargs):
        self.compute_args = kwargs
        return {
            'market_date': kwargs['market_date'],
            'action_summaries_persisted': 2,
            'action_summaries_failed': 0,
            'items': [{'symbol': '005930', 'action_label': 'long_breakout'}],
        }

    def inspect(self, **kwargs):
        self.inspect_args = kwargs
        return {
            'market_date': kwargs['market_date'],
            'summary': {'entries': 1},
            'items': [{'symbol': '005930', 'action_label': 'long_breakout'}],
        }


class EODRouteTests(unittest.TestCase):
    def setUp(self):
        self.app = FastAPI()
        self.app.include_router(routes_eod.router)

    def test_get_eod_freshness_route_returns_service_payload(self):
        fake_service = FakeFreshnessService()
        original_service = routes_eod.eod_freshness_service
        routes_eod.eod_freshness_service = fake_service
        try:
            client = TestClient(self.app)
            response = client.get('/eod/freshness')
        finally:
            routes_eod.eod_freshness_service = original_service

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload['today']['trade_date'], '2026-04-10')
        self.assertEqual(payload['today']['eod_status'], 'completed')
        self.assertEqual(payload['latest']['action_market_date'], '2026-04-10')

    def test_compute_technical_snapshots_route_calls_service(self):
        fake_service = FakeTechnicalSnapshotService()
        original_service = routes_eod.daily_technical_snapshot_service
        routes_eod.daily_technical_snapshot_service = fake_service
        try:
            client = TestClient(self.app)
            response = client.post(
                '/eod/technical-snapshots/compute',
                params={'market_date': '2026-04-07', 'symbols': '005930,000660'},
            )
        finally:
            routes_eod.daily_technical_snapshot_service = original_service

        self.assertEqual(response.status_code, 200)
        self.assertEqual(fake_service.compute_args['symbols'], ['005930', '000660'])
        self.assertEqual(response.json()['snapshots_persisted'], 2)

    def test_inspect_technical_snapshots_route_calls_service(self):
        fake_service = FakeTechnicalSnapshotService()
        original_service = routes_eod.daily_technical_snapshot_service
        routes_eod.daily_technical_snapshot_service = fake_service
        try:
            client = TestClient(self.app)
            response = client.get(
                '/eod/technical-snapshots',
                params={'market_date': '2026-04-07', 'symbols': '005930', 'limit': 5},
            )
        finally:
            routes_eod.daily_technical_snapshot_service = original_service

        self.assertEqual(response.status_code, 200)
        self.assertEqual(fake_service.inspect_args['symbols'], ['005930'])
        self.assertEqual(fake_service.inspect_args['limit'], 5)
        self.assertEqual(response.json()['items'][0]['trend_state'], 'uptrend')

    def test_compute_action_summaries_route_calls_service(self):
        fake_service = FakeActionSummaryService()
        original_service = routes_eod.daily_action_summary_service
        routes_eod.daily_action_summary_service = fake_service
        try:
            client = TestClient(self.app)
            response = client.post(
                '/eod/action-summaries/compute',
                params={'market_date': '2026-04-07', 'symbols': '005930,000660'},
            )
        finally:
            routes_eod.daily_action_summary_service = original_service

        self.assertEqual(response.status_code, 200)
        self.assertEqual(fake_service.compute_args['symbols'], ['005930', '000660'])
        self.assertEqual(response.json()['action_summaries_persisted'], 2)

    def test_inspect_action_summaries_route_calls_service(self):
        fake_service = FakeActionSummaryService()
        original_service = routes_eod.daily_action_summary_service
        routes_eod.daily_action_summary_service = fake_service
        try:
            client = TestClient(self.app)
            response = client.get(
                '/eod/action-summaries',
                params={'market_date': '2026-04-07', 'symbols': '005930', 'limit': 5},
            )
        finally:
            routes_eod.daily_action_summary_service = original_service

        self.assertEqual(response.status_code, 200)
        self.assertEqual(fake_service.inspect_args['symbols'], ['005930'])
        self.assertEqual(fake_service.inspect_args['limit'], 5)
        self.assertEqual(response.json()['items'][0]['action_label'], 'long_breakout')
