import unittest

from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.api import routes_market
from app.market.snapshot import MarketSnapshotConfigurationError, MarketSnapshotUpstreamError


class FakeMarketSnapshotService:
    async def get_market_snapshot(self) -> dict:
        return {
            'as_of': '2026-04-29T10:05:12+09:00',
            'status': 'ready',
            'missing_fields': [],
            'stale_reason': None,
            'markets': {
                'kospi': {
                    'label': 'KOSPI',
                    'index_code': '0001',
                    'value': 2642.07,
                    'change': 2.78,
                    'change_percent': 0.11,
                    'open': 2651.63,
                    'high': 2653.87,
                    'low': 2634.29,
                    'volume': 305715,
                    'trading_value': 4736153,
                    'trading_value_unit': 'million_krw',
                    'advancers': 512,
                    'unchanged': 94,
                    'decliners': 330,
                    'foreign_net_quantity': -5290,
                    'foreign_net_value': -1455933000000,
                    'institution_net_quantity': 720,
                    'institution_net_value': 283792000000,
                    'investor_flow_value_unit': 'krw',
                },
                'kosdaq': {
                    'label': 'KOSDAQ',
                    'index_code': '1001',
                    'value': 857.6,
                    'change': -1.61,
                    'change_percent': -0.19,
                    'open': 863.69,
                    'high': 864.24,
                    'low': 854.72,
                    'volume': 1312496,
                    'trading_value': 11507962,
                    'trading_value_unit': 'million_krw',
                    'advancers': 828,
                    'unchanged': 94,
                    'decliners': 716,
                    'foreign_net_quantity': -18565,
                    'foreign_net_value': -211205000000,
                    'institution_net_quantity': -4738,
                    'institution_net_value': -304502000000,
                    'investor_flow_value_unit': 'krw',
                },
            },
            'usd_krw': {
                'pair': 'USD/KRW',
                'rate': 1444.5,
                'effective_date': '2026-04-28',
                'fallback_applied': False,
                'status': 'ready',
                'source': 'live',
                'stale_reason': None,
                'detail': None,
                'captured_at': '2026-04-29T10:05:12+09:00',
            },
            'freshness': {
                'served_from_cache': True,
                'refresh_interval_seconds': 3600,
                'last_refresh_attempted_at': '2026-04-29T10:05:12+09:00',
                'last_refresh_succeeded_at': '2026-04-29T10:05:12+09:00',
                'next_refresh_due_at': '2026-04-29T11:05:12+09:00',
                'last_refresh_status': 'success',
                'last_refresh_error': None,
            },
        }


class PartialMarketSnapshotService:
    async def get_market_snapshot(self) -> dict:
        return {
            'as_of': '2026-04-30T13:15:00+09:00',
            'status': 'partial',
            'missing_fields': ['usd_krw.rate'],
            'stale_reason': 'koreaexim_quota_exhausted',
            'markets': {
                'kospi': {'label': 'KOSPI', 'index_code': '0001', 'value': 2642.07},
                'kosdaq': {'label': 'KOSDAQ', 'index_code': '1001', 'value': 857.6},
            },
            'usd_krw': {
                'pair': 'USD/KRW',
                'rate': None,
                'effective_date': None,
                'fallback_applied': False,
                'status': 'missing',
                'source': 'unavailable',
                'stale_reason': 'koreaexim_quota_exhausted',
                'detail': 'Korea Exim returned result=4',
                'captured_at': None,
            },
            'freshness': {
                'served_from_cache': True,
                'refresh_interval_seconds': 3600,
                'last_refresh_attempted_at': '2026-04-30T13:15:00+09:00',
                'last_refresh_succeeded_at': '2026-04-30T13:15:00+09:00',
                'next_refresh_due_at': '2026-04-30T14:15:00+09:00',
                'last_refresh_status': 'degraded',
                'last_refresh_error': 'Korea Exim returned result=4',
            },
        }


class UnavailableMarketSnapshotService:
    async def get_market_snapshot(self) -> dict:
        raise MarketSnapshotConfigurationError('Enable MARKET_SNAPSHOT_KIS_USE_REAL or run with KIS_USE_MOCK=false.')


class BrokenMarketSnapshotService:
    async def get_market_snapshot(self) -> dict:
        raise MarketSnapshotUpstreamError('KIS investor-flow payload did not include a usable output row')


class MarketRouteTests(unittest.TestCase):
    def setUp(self) -> None:
        self.app = FastAPI()
        self.app.include_router(routes_market.router)
        self.original_snapshot_service = routes_market.market_snapshot_service

    def tearDown(self) -> None:
        routes_market.market_snapshot_service = self.original_snapshot_service

    def test_market_snapshot_route_returns_compact_payload(self) -> None:
        routes_market.market_snapshot_service = FakeMarketSnapshotService()

        client = TestClient(self.app)
        response = client.get('/market/snapshot')

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload['markets']['kospi']['label'], 'KOSPI')
        self.assertEqual(payload['markets']['kosdaq']['foreign_net_quantity'], -18565)
        self.assertEqual(payload['markets']['kospi']['trading_value_unit'], 'million_krw')
        self.assertEqual(payload['markets']['kospi']['investor_flow_value_unit'], 'krw')
        self.assertEqual(payload['markets']['kosdaq']['institution_net_value'], -304502000000)
        self.assertEqual(payload['status'], 'ready')
        self.assertEqual(payload['usd_krw']['pair'], 'USD/KRW')
        self.assertTrue(payload['freshness']['served_from_cache'])

    def test_market_snapshot_route_returns_partial_payload_when_fx_is_missing(self) -> None:
        routes_market.market_snapshot_service = PartialMarketSnapshotService()

        client = TestClient(self.app)
        response = client.get('/market/snapshot')

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload['status'], 'partial')
        self.assertEqual(payload['stale_reason'], 'koreaexim_quota_exhausted')
        self.assertIn('usd_krw.rate', payload['missing_fields'])
        self.assertEqual(payload['markets']['kospi']['value'], 2642.07)
        self.assertEqual(payload['usd_krw']['status'], 'missing')
        self.assertEqual(payload['usd_krw']['source'], 'unavailable')
        self.assertEqual(payload['freshness']['last_refresh_status'], 'degraded')

    def test_market_snapshot_route_maps_configuration_errors_to_503(self) -> None:
        routes_market.market_snapshot_service = UnavailableMarketSnapshotService()

        client = TestClient(self.app)
        response = client.get('/market/snapshot')

        self.assertEqual(response.status_code, 503)
        self.assertIn('MARKET_SNAPSHOT_KIS_USE_REAL', response.json()['detail'])

    def test_market_snapshot_route_maps_upstream_errors_to_502(self) -> None:
        routes_market.market_snapshot_service = BrokenMarketSnapshotService()

        client = TestClient(self.app)
        response = client.get('/market/snapshot')

        self.assertEqual(response.status_code, 502)
        self.assertIn('Market snapshot upstream failed', response.json()['detail'])


if __name__ == '__main__':
    unittest.main()
