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


class KISRouteTests(unittest.TestCase):
    def setUp(self) -> None:
        self.app = FastAPI()
        self.app.include_router(routes_kis.router)
        self.original_client = routes_kis.kis_client
        self.original_realtime_service = routes_kis.kis_realtime_order_notice_service
        routes_kis.kis_client = FakeKISClient()
        routes_kis.kis_realtime_order_notice_service = FakeRealtimeOrderNoticeService()

    def tearDown(self) -> None:
        routes_kis.kis_client = self.original_client
        routes_kis.kis_realtime_order_notice_service = self.original_realtime_service

    def test_health_route_includes_realtime_order_notice_snapshot(self) -> None:
        client = TestClient(self.app)
        response = client.get('/broker/kis/health')

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload['environment'], 'demo')
        self.assertEqual(payload['realtime_order_notices']['status'], 'subscribed')
        self.assertEqual(payload['realtime_order_notices']['metrics']['notices_ingested'], 3)


if __name__ == '__main__':
    unittest.main()
