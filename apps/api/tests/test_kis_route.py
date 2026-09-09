import unittest
from unittest.mock import patch

from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.api import routes_kis
from app.core.settings import settings


class FakeKISClient:
    def __init__(self) -> None:
        self.token_force_refresh_calls: list[bool] = []
        self.token_error: Exception | None = None
        self.quote_error: Exception | None = None

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

    async def issue_access_token(self, force_refresh: bool = False) -> dict:
        self.token_force_refresh_calls.append(force_refresh)
        if self.token_error is not None:
            raise self.token_error
        return {
            'access_token': 'raw-secret-access-token',
            'token_type': 'Bearer',
            'expires_in': 3_300,
            'cached': True,
            'refresh_deferred': True,
            'defer_reason': 'minimum_issue_interval',
            'unexpected_upstream_field': 'must-not-be-returned',
        }

    async def fetch_domestic_quote(self, **kwargs) -> dict:
        if self.quote_error is not None:
            raise self.quote_error
        return {'symbol': kwargs['symbol'], 'price': '70000'}


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
    def __init__(self) -> None:
        self.collect_error: Exception | None = None
        self.inspect_error: Exception | None = None

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
        if self.collect_error is not None:
            raise self.collect_error
        return {
            'status': 'ok',
            'requested_symbols': kwargs.get('symbols') or [],
            'snapshots_persisted': 1,
            'items': [{'symbol': '005930', 'availability_status': 'available'}],
        }

    def inspect(self, **kwargs) -> dict:
        if self.inspect_error is not None:
            raise self.inspect_error
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
        self.original_upstream_bearer_token = settings.upstream_bearer_token
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
        settings.upstream_bearer_token = self.original_upstream_bearer_token

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

    def test_token_route_is_disabled_without_configured_bearer_token(self) -> None:
        settings.upstream_bearer_token = None
        client = TestClient(self.app)

        response = client.post('/broker/kis/token')

        self.assertEqual(response.status_code, 503)
        self.assertEqual(response.json(), {'detail': 'KIS token endpoint is disabled'})
        self.assertEqual(routes_kis.kis_client.token_force_refresh_calls, [])

    def test_token_route_requires_exact_bearer_authorization(self) -> None:
        settings.upstream_bearer_token = 'internal-secret'
        client = TestClient(self.app)

        missing = client.post('/broker/kis/token')
        wrong = client.post(
            '/broker/kis/token',
            headers={'Authorization': 'Bearer wrong-secret'},
        )
        wrong_scheme = client.post(
            '/broker/kis/token',
            headers={'Authorization': 'bearer internal-secret'},
        )

        for response in (missing, wrong, wrong_scheme):
            self.assertEqual(response.status_code, 401)
            self.assertEqual(response.json(), {'detail': 'Unauthorized'})
            self.assertEqual(response.headers['www-authenticate'], 'Bearer')
        self.assertEqual(routes_kis.kis_client.token_force_refresh_calls, [])

    def test_token_route_returns_only_safe_metadata_and_forwards_force(self) -> None:
        settings.upstream_bearer_token = 'internal-secret'
        client = TestClient(self.app)

        response = client.post(
            '/broker/kis/token?force_refresh=true',
            headers={'Authorization': 'Bearer internal-secret'},
        )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            response.json(),
            {
                'access_token_present': True,
                'token_type': 'Bearer',
                'expires_in': 3_300,
                'cached': True,
                'refresh_deferred': True,
                'defer_reason': 'minimum_issue_interval',
            },
        )
        self.assertNotIn('raw-secret-access-token', response.text)
        self.assertNotIn('unexpected_upstream_field', response.text)
        self.assertEqual(routes_kis.kis_client.token_force_refresh_calls, [True])

    def test_token_route_failure_does_not_expose_exception_or_token(self) -> None:
        settings.upstream_bearer_token = 'internal-secret'
        routes_kis.kis_client.token_error = RuntimeError(
            'upstream rejected raw-secret-access-token'
        )
        client = TestClient(self.app)

        response = client.post(
            '/broker/kis/token',
            headers={'Authorization': 'Bearer internal-secret'},
        )

        self.assertEqual(response.status_code, 502)
        self.assertEqual(response.json(), {'detail': 'KIS token issue failed'})
        self.assertNotIn('raw-secret-access-token', response.text)

    def test_quote_failure_does_not_expose_upstream_payload(self) -> None:
        routes_kis.kis_client.quote_error = RuntimeError(
            'raw-token account=12345678-01 upstream failure'
        )
        client = TestClient(self.app)

        response = client.get('/broker/kis/quote?symbol=005930')

        self.assertEqual(response.status_code, 502)
        self.assertEqual(response.json(), {'detail': 'KIS quote failed'})
        self.assertNotIn('raw-token', response.text)
        self.assertNotIn('12345678-01', response.text)

    def test_intraday_route_errors_return_bounded_messages(self) -> None:
        client = TestClient(self.app)
        secret_text = 'raw-token account=12345678-01'

        routes_kis.kis_intraday_snapshot_service.collect_error = ValueError(secret_text)
        collect_invalid = client.post('/broker/kis/intraday-snapshots/collect')
        routes_kis.kis_intraday_snapshot_service.collect_error = RuntimeError(secret_text)
        collect_failed = client.post('/broker/kis/intraday-snapshots/collect')
        routes_kis.kis_intraday_snapshot_service.inspect_error = ValueError(secret_text)
        inspect_invalid = client.get('/broker/kis/intraday-snapshots')
        routes_kis.kis_intraday_snapshot_service.inspect_error = RuntimeError(secret_text)
        inspect_failed = client.get('/broker/kis/intraday-snapshots')

        self.assertEqual(
            (collect_invalid.status_code, collect_invalid.json()),
            (400, {'detail': 'Invalid KIS intraday snapshot request'}),
        )
        self.assertEqual(
            (collect_failed.status_code, collect_failed.json()),
            (502, {'detail': 'KIS intraday snapshot collection failed'}),
        )
        self.assertEqual(
            (inspect_invalid.status_code, inspect_invalid.json()),
            (400, {'detail': 'Invalid KIS intraday snapshot request'}),
        )
        self.assertEqual(
            (inspect_failed.status_code, inspect_failed.json()),
            (500, {'detail': 'KIS intraday snapshot read failed'}),
        )
        for response in (
            collect_invalid,
            collect_failed,
            inspect_invalid,
            inspect_failed,
        ):
            self.assertNotIn('raw-token', response.text)
            self.assertNotIn('12345678-01', response.text)


if __name__ == '__main__':
    unittest.main()
