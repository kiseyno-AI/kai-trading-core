import unittest

from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.api import routes_kis


class FakeKISClient:
    async def healthcheck(self) -> dict:
        return {
            'broker': 'kis',
            'base_url': 'https://openapivts.koreainvestment.com:29443',
            'websocket_url': 'ws://ops.koreainvestment.com:31000',
            'environment': 'demo',
            'use_mock': True,
            'app_key_configured': True,
            'app_secret_configured': True,
            'hts_id_configured': True,
            'realtime_order_notice_enabled': True,
        }


class FakeRealtimeOrderNoticeService:
    def health_snapshot(self) -> dict:
        return {
            'status': 'subscribed',
            'configured': True,
            'last_message_received_at': '2026-04-28T00:01:00+00:00',
            'metrics': {
                'notices_ingested': 3,
                'duplicate_notices': 1,
            },
        }


class FakeMarketSnapshotService:
    def runtime_status(self) -> dict:
        return {
            'ready': True,
            'global_kis_environment': 'demo',
            'effective_kis_environment': 'real',
            'effective_kis_base_url': 'https://openapi.koreainvestment.com:9443',
            'uses_real_domain_override': True,
            'market_snapshot_kis_use_real': True,
            'kis_app_key_configured': True,
            'kis_app_secret_configured': True,
            'koreaexim_api_key_configured': True,
            'blocking_reasons': [],
        }


class FakePortfolioSummaryService:
    def runtime_status(self) -> dict:
        return {
            'ready': True,
            'app_env': 'production',
            'kis_environment': 'real',
            'kis_base_url': 'https://openapi.koreainvestment.com:9443',
            'kis_use_mock': False,
            'kis_app_key_configured': True,
            'kis_app_secret_configured': True,
            'portfolio_account_references_configured': True,
            'portfolio_account_references_valid': True,
            'configured_account_count': 2,
            'blocking_reasons': [],
        }


class FakeKISIntradaySnapshotService:
    def runtime_status(self) -> dict:
        return {
            'enabled': True,
            'ready': True,
            'credentials_configured': True,
            'kis_use_mock': True,
            'market_code': 'J',
            'max_ranking_pages': 2,
            'news_limit': 5,
            'blocking_reasons': [],
        }

    async def collect_snapshots(self, **kwargs) -> dict:
        return {
            'status': 'ok',
            'requested_symbols': kwargs.get('symbols') or [],
            'snapshots_persisted': 1,
            'items': [{'symbol': '005930', 'availability_status': 'available'}],
        }

    def inspect(self, **kwargs) -> dict:
        return {
            'summary': {'entries': 1, 'available_count': 1, 'partial_count': 0, 'unavailable_count': 0},
            'items': [{'symbol': '005930', 'availability_status': 'available', 'freshness_status': 'fresh'}],
        }


class KISRouteTests(unittest.TestCase):
    def setUp(self) -> None:
        self.app = FastAPI()
        self.app.include_router(routes_kis.router)
        self.original_client = routes_kis.kis_client
        self.original_realtime_service = routes_kis.kis_realtime_order_notice_service
        self.original_market_snapshot_service = routes_kis.market_snapshot_service
        self.original_portfolio_summary_service = routes_kis.portfolio_summary_service
        self.original_intraday_snapshot_service = routes_kis.kis_intraday_snapshot_service
        routes_kis.kis_client = FakeKISClient()
        routes_kis.kis_realtime_order_notice_service = FakeRealtimeOrderNoticeService()
        routes_kis.market_snapshot_service = FakeMarketSnapshotService()
        routes_kis.portfolio_summary_service = FakePortfolioSummaryService()
        routes_kis.kis_intraday_snapshot_service = FakeKISIntradaySnapshotService()

    def tearDown(self) -> None:
        routes_kis.kis_client = self.original_client
        routes_kis.kis_realtime_order_notice_service = self.original_realtime_service
        routes_kis.market_snapshot_service = self.original_market_snapshot_service
        routes_kis.portfolio_summary_service = self.original_portfolio_summary_service
        routes_kis.kis_intraday_snapshot_service = self.original_intraday_snapshot_service

    def test_health_route_includes_realtime_order_notice_snapshot(self) -> None:
        client = TestClient(self.app)
        response = client.get('/broker/kis/health')

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload['environment'], 'demo')
        self.assertEqual(payload['realtime_order_notices']['status'], 'subscribed')
        self.assertEqual(payload['realtime_order_notices']['metrics']['notices_ingested'], 3)
        self.assertTrue(payload['market_snapshot_runtime']['ready'])
        self.assertEqual(payload['market_snapshot_runtime']['effective_kis_environment'], 'real')
        self.assertTrue(payload['portfolio_summary_runtime']['ready'])
        self.assertEqual(payload['portfolio_summary_runtime']['configured_account_count'], 2)
        self.assertEqual(payload['portfolio_summary_runtime']['kis_environment'], 'real')
        self.assertTrue(payload['intraday_snapshot_runtime']['ready'])
        self.assertEqual(payload['intraday_snapshot_runtime']['market_code'], 'J')

    def test_intraday_snapshot_collect_route_passes_scope_through(self) -> None:
        client = TestClient(self.app)
        response = client.post('/broker/kis/intraday-snapshots/collect?symbols=005930,000660&max_news_items=2')

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload['status'], 'ok')
        self.assertEqual(payload['requested_symbols'], ['005930', '000660'])
        self.assertEqual(payload['snapshots_persisted'], 1)

    def test_intraday_snapshot_read_route_returns_summary(self) -> None:
        client = TestClient(self.app)
        response = client.get('/broker/kis/intraday-snapshots?symbols=005930&limit=5')

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload['summary']['entries'], 1)
        self.assertEqual(payload['items'][0]['symbol'], '005930')
        self.assertEqual(payload['items'][0]['freshness_status'], 'fresh')


if __name__ == '__main__':
    unittest.main()
