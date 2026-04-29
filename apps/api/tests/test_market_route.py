import unittest

from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.api import routes_market
from app.market.snapshot import MarketSnapshotConfigurationError, MarketSnapshotUpstreamError


class FakeMarketSnapshotService:
    async def get_market_snapshot(self) -> dict:
        return {
            'as_of': '2026-04-29T10:05:12+09:00',
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
                    'advancers': 512,
                    'unchanged': 94,
                    'decliners': 330,
                    'foreign_net_quantity': -5290,
                    'foreign_net_value': 148656,
                    'institution_net_quantity': 720,
                    'institution_net_value': -55532,
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
                    'advancers': 828,
                    'unchanged': 94,
                    'decliners': 716,
                    'foreign_net_quantity': -18565,
                    'foreign_net_value': -597490,
                    'institution_net_quantity': -4738,
                    'institution_net_value': -150685,
                },
            },
            'usd_krw': {
                'pair': 'USD/KRW',
                'rate': 1444.5,
                'effective_date': '2026-04-28',
                'fallback_applied': True,
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
        self.assertEqual(payload['usd_krw']['pair'], 'USD/KRW')

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
