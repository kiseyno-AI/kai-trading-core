from __future__ import annotations

import unittest
from unittest.mock import AsyncMock, patch

import httpx

from app.core.settings import settings
from app.integrations.fred.client import FREDClient
from app.integrations.world_bank.client import WorldBankAPIError, WorldBankClient


class MacroProviderClientRetryTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.original_fred_api_key = settings.fred_api_key
        settings.fred_api_key = 'test-fred-key'

    def tearDown(self) -> None:
        settings.fred_api_key = self.original_fred_api_key

    async def test_world_bank_retries_read_timeout_then_returns_rows(self) -> None:
        attempts = 0

        def handler(request: httpx.Request) -> httpx.Response:
            nonlocal attempts
            attempts += 1
            if attempts == 1:
                raise httpx.ReadTimeout('', request=request)
            return httpx.Response(
                200,
                json=[{'page': 1}, [{'date': '2025', 'value': 2.1}]],
                request=request,
            )

        client = WorldBankClient(transport=httpx.MockTransport(handler))
        with patch('app.core.audit_logging.asyncio.sleep', new_callable=AsyncMock):
            result = await client.fetch_indicator(
                country='KR',
                indicator='FP.CPI.TOTL.ZG',
            )

        self.assertEqual(attempts, 2)
        self.assertEqual(result['rows'], [{'date': '2025', 'value': 2.1}])

    async def test_world_bank_empty_read_timeout_has_nonblank_transport_error(self) -> None:
        attempts = 0

        def handler(request: httpx.Request) -> httpx.Response:
            nonlocal attempts
            attempts += 1
            raise httpx.ReadTimeout('', request=request)

        client = WorldBankClient(transport=httpx.MockTransport(handler))
        with (
            patch('app.core.audit_logging.asyncio.sleep', new_callable=AsyncMock),
            self.assertRaises(WorldBankAPIError) as raised,
        ):
            await client.fetch_indicator(
                country='KR',
                indicator='FP.CPI.TOTL.ZG',
            )

        self.assertEqual(attempts, 3)
        self.assertEqual(
            str(raised.exception),
            'World Bank transport error: ReadTimeout',
        )

    async def test_world_bank_does_not_retry_invalid_json(self) -> None:
        attempts = 0

        def handler(request: httpx.Request) -> httpx.Response:
            nonlocal attempts
            attempts += 1
            return httpx.Response(200, content=b'{', request=request)

        client = WorldBankClient(transport=httpx.MockTransport(handler))
        with self.assertRaises(ValueError):
            await client.fetch_indicator(
                country='KR',
                indicator='FP.CPI.TOTL.ZG',
            )

        self.assertEqual(attempts, 1)

    async def test_fred_retries_service_unavailable_then_returns_observations(self) -> None:
        attempts = 0

        def handler(request: httpx.Request) -> httpx.Response:
            nonlocal attempts
            attempts += 1
            if attempts == 1:
                return httpx.Response(503, headers={'Retry-After': '0'}, request=request)
            return httpx.Response(
                200,
                json={'observations': [{'date': '2025-01-01', 'value': '2.1'}]},
                request=request,
            )

        client = FREDClient(transport=httpx.MockTransport(handler))
        with patch('app.core.audit_logging.asyncio.sleep', new_callable=AsyncMock):
            result = await client.fetch_latest_observations(series_id='CPIAUCSL')

        self.assertEqual(attempts, 2)
        self.assertEqual(
            result['observations'],
            [{'date': '2025-01-01', 'value': '2.1'}],
        )
