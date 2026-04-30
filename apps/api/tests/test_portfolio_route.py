import unittest

from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.api import routes_portfolio


class FakePortfolioSummaryService:
    def __init__(self) -> None:
        self.calls: list[dict] = []
        self.exception: Exception | None = None

    async def get_portfolio_summary(self, *, refresh: bool = False, trigger: str = 'route_read'):
        self.calls.append({'refresh': refresh, 'trigger': trigger})
        if self.exception is not None:
            raise self.exception
        return {
            'asOf': '2026-04-29T01:23:45+00:00',
            'source': 'kis_portfolio_summary_v1',
            'sync': {'status': 'synced'},
            'freshness': {'servedFromCache': True},
            'accounts': [{'id': 'KIS:12345678-01', 'label': 'Main'}],
            'notices': [],
        }


class PortfolioRouteTests(unittest.TestCase):
    def setUp(self) -> None:
        self.app = FastAPI()
        self.app.include_router(routes_portfolio.router)
        self.service = FakePortfolioSummaryService()
        self.original_service = routes_portfolio.portfolio_summary_service
        routes_portfolio.portfolio_summary_service = self.service

    def tearDown(self) -> None:
        routes_portfolio.portfolio_summary_service = self.original_service

    def test_summary_route_returns_service_payload(self) -> None:
        client = TestClient(self.app)

        response = client.get('/portfolio/summary')

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()['sync']['status'], 'synced')
        self.assertEqual(response.json()['accounts'][0]['label'], 'Main')
        self.assertEqual(self.service.calls, [{'refresh': False, 'trigger': 'route_read'}])

    def test_summary_route_passes_refresh_flag(self) -> None:
        client = TestClient(self.app)

        response = client.get('/portfolio/summary?refresh=true')

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()['freshness']['servedFromCache'], True)
        self.assertEqual(self.service.calls, [{'refresh': True, 'trigger': 'route_read'}])

    def test_summary_route_maps_unexpected_errors_to_500(self) -> None:
        client = TestClient(self.app)
        self.service.exception = RuntimeError('unexpected failure')

        response = client.get('/portfolio/summary')

        self.assertEqual(response.status_code, 500)
        self.assertIn('Portfolio summary failed', response.json()['detail'])


if __name__ == '__main__':
    unittest.main()
