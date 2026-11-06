"""Core's direct broker-order routes are closed unless explicitly enabled."""

from __future__ import annotations

import unittest
from typing import Any
from unittest.mock import patch

from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.api import routes_trading
from app.core.settings import Settings, settings


class _OrderService:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict[str, Any]]] = []

    async def submit_buy_order(self, **kwargs: Any) -> dict[str, Any]:
        self.calls.append(('buy', kwargs))
        return {'created': True, 'client_order_id': 'X'}

    async def submit_sell_order(self, **kwargs: Any) -> dict[str, Any]:
        self.calls.append(('sell', kwargs))
        return {'created': True, 'client_order_id': 'X'}

    async def amend_order(self, **kwargs: Any) -> dict[str, Any]:
        self.calls.append(('amend', kwargs))
        return {'client_order_id': 'X'}

    async def cancel_order(self, **kwargs: Any) -> dict[str, Any]:
        self.calls.append(('cancel', kwargs))
        return {'client_order_id': 'X', 'cancelled': True}


ORDER_BODY = {
    'account_number': '12345678',
    'account_product_code': '01',
    'symbol': '005930',
    'quantity': '1',
    'price': '70000',
}


class DirectOrderApiGateTests(unittest.TestCase):
    def setUp(self) -> None:
        self.service = _OrderService()
        app = FastAPI()
        app.include_router(routes_trading.router)
        self.client = TestClient(app)
        self._patch = patch.object(routes_trading, 'trading_order_service', self.service)
        self._patch.start()

    def tearDown(self) -> None:
        self._patch.stop()

    def test_the_setting_defaults_to_closed(self) -> None:
        self.assertFalse(Settings.model_fields['trading_direct_order_api_enabled'].default)

    def test_buy_sell_and_amend_are_rejected_without_reaching_the_broker_service(self) -> None:
        with patch.object(settings, 'trading_direct_order_api_enabled', False):
            buy = self.client.post('/trading/orders/buy', json=ORDER_BODY)
            sell = self.client.post('/trading/orders/sell', json=ORDER_BODY)
            amend = self.client.post(
                '/trading/orders/ORDER-1/amend',
                json={'account_reference': 'KIS:12345678-01', 'price': '71000'},
            )
            # Gate applies before body validation, so even an empty body is a 403.
            empty = self.client.post('/trading/orders/buy', json={})

        for response in (buy, sell, amend, empty):
            self.assertEqual(response.status_code, 403)
            self.assertEqual(response.json()['detail']['error'], 'direct_order_api_disabled')
        self.assertEqual(self.service.calls, [])

    def test_cancel_stays_available_because_it_only_reduces_risk(self) -> None:
        with patch.object(settings, 'trading_direct_order_api_enabled', False):
            response = self.client.post(
                '/trading/orders/ORDER-1/cancel',
                json={'account_reference': 'KIS:12345678-01', 'all_remaining': True},
            )

        self.assertEqual(response.status_code, 200)
        self.assertEqual([name for name, _ in self.service.calls], ['cancel'])

    def test_routes_work_when_an_operator_enables_them(self) -> None:
        with patch.object(settings, 'trading_direct_order_api_enabled', True):
            buy = self.client.post('/trading/orders/buy', json=ORDER_BODY)
            sell = self.client.post('/trading/orders/sell', json=ORDER_BODY)

        self.assertEqual(buy.status_code, 201)
        self.assertEqual(sell.status_code, 201)
        self.assertEqual([name for name, _ in self.service.calls], ['buy', 'sell'])


if __name__ == '__main__':
    unittest.main()
