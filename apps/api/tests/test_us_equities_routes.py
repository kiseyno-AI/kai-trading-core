import unittest
from unittest.mock import AsyncMock, patch

import httpx

from app.main import app


class USEquitiesRouteTests(unittest.IsolatedAsyncioTestCase):
    async def test_refresh_route_returns_service_payload(self):
        payload = {'status': 'completed', 'counts': {'integrated': 1}, 'persisted_count': 1, 'preview': []}
        with patch(
            'app.api.routes_us_equities.us_universe_refresh_service.refresh',
            new=AsyncMock(return_value=payload),
        ) as refresh:
            transport = httpx.ASGITransport(app=app)
            async with httpx.AsyncClient(transport=transport, base_url='http://test') as client:
                response = await client.post('/us-equities/universe/refresh', json={'persist': True})

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json(), payload)
        refresh.assert_awaited_once_with(persist=True, preview_limit=20)

    async def test_read_route_scopes_to_us_exchanges(self):
        with patch('app.api.routes_us_equities.load_symbols', return_value=[{'symbol': 'AAPL'}]) as load:
            transport = httpx.ASGITransport(app=app)
            async with httpx.AsyncClient(transport=transport, base_url='http://test') as client:
                response = await client.get('/us-equities/universe?limit=25')

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()['items'], [{'symbol': 'AAPL'}])
        load.assert_called_once_with(
            is_active=True,
            source='us_integrated_master',
            markets=['NASDAQ', 'NYSE', 'NYSE_AMERICAN'],
            limit=25,
        )

    async def test_price_collection_route_is_explicitly_dated(self):
        payload = {'status': 'completed', 'market_date': '2026-08-19', 'counts': {'price_selected': 2}}
        with patch(
            'app.api.routes_us_equities.us_price_collection_service.collect_and_screen',
            new=AsyncMock(return_value=payload),
        ) as collect:
            transport = httpx.ASGITransport(app=app)
            async with httpx.AsyncClient(transport=transport, base_url='http://test') as client:
                response = await client.post(
                    '/us-equities/prices/collect-and-screen',
                    json={'market_date': '2026-08-19', 'persist': False, 'prefilter_cap': 50},
                )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json(), payload)
        collect.assert_awaited_once()
        self.assertEqual(collect.await_args.kwargs['market_date'].isoformat(), '2026-08-19')
        self.assertFalse(collect.await_args.kwargs['persist'])
        self.assertEqual(collect.await_args.kwargs['prefilter_cap'], 50)

    async def test_fundamental_collection_route_is_explicitly_dated(self):
        payload = {'status': 'completed', 'market_date': '2026-08-19', 'counts': {'agent_candidates': 2}}
        with patch(
            'app.api.routes_us_equities.us_fundamental_collection_service.collect_and_screen',
            new=AsyncMock(return_value=payload),
        ) as collect:
            transport = httpx.ASGITransport(app=app)
            async with httpx.AsyncClient(transport=transport, base_url='http://test') as client:
                response = await client.post(
                    '/us-equities/fundamentals/collect-and-screen',
                    json={'market_date': '2026-08-19', 'persist': False},
                )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json(), payload)
        collect.assert_awaited_once()

    async def test_execution_plan_route_defaults_to_dry_run(self):
        payload = {'status': 'ready', 'dry_run': True, 'payload': {'items': []}}
        with patch(
            'app.api.routes_us_equities.us_execution_plan_service.dispatch',
            new=AsyncMock(return_value=payload),
        ) as dispatch:
            transport = httpx.ASGITransport(app=app)
            async with httpx.AsyncClient(transport=transport, base_url='http://test') as client:
                response = await client.post(
                    '/us-equities/execution-plans/dispatch',
                    json={'source_market_date': '2026-08-19'},
                )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json(), payload)
        dispatch.assert_awaited_once()
        self.assertTrue(dispatch.await_args.kwargs['dry_run'])
        self.assertEqual(dispatch.await_args.kwargs['source_market_date'].isoformat(), '2026-08-19')


if __name__ == '__main__':
    unittest.main()
