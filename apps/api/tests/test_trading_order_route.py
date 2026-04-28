import unittest

from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.api import routes_trading
from app.orders.service import (
    TradingOrderRejectedError,
)


class FakeTradingOrderService:
    def __init__(self) -> None:
        self.buy_calls: list[dict] = []
        self.sell_calls: list[dict] = []
        self.cancel_calls: list[dict] = []
        self.amend_calls: list[dict] = []
        self.get_calls: list[dict] = []
        self.list_calls: list[dict] = []

    async def submit_buy_order(self, **kwargs):
        self.buy_calls.append(kwargs)
        return {
            'created': True,
            'idempotent_replay': False,
            'client_order_id': kwargs.get('client_order_id') or 'generated-buy',
        }

    async def submit_sell_order(self, **kwargs):
        self.sell_calls.append(kwargs)
        raise TradingOrderRejectedError(
            'sell rejected',
            payload={
                'operation': 'submit',
                'client_order_id': kwargs.get('client_order_id') or 'generated-sell',
                'request': {'submission_status': 'rejected'},
                'order': {'current_status': 'rejected'},
            },
        )

    async def cancel_order(self, **kwargs):
        self.cancel_calls.append(kwargs)
        return {'operation': 'cancel', 'order': {'current_status': 'cancel_pending'}}

    async def amend_order(self, **kwargs):
        self.amend_calls.append(kwargs)
        return {'operation': 'amend', 'order': {'current_status': 'replace_pending'}}

    async def get_order(self, **kwargs):
        self.get_calls.append(kwargs)
        return {'operation': 'get', 'order': {'client_order_id': kwargs['client_order_id']}}

    def list_orders(self, **kwargs):
        self.list_calls.append(kwargs)
        return {'count': 1, 'items': [{'client_order_id': 'TEST-ORDER-1'}]}


class TradingOrderRouteTests(unittest.TestCase):
    def setUp(self) -> None:
        self.app = FastAPI()
        self.app.include_router(routes_trading.router)
        self.service = FakeTradingOrderService()
        self.original_service = routes_trading.trading_order_service
        routes_trading.trading_order_service = self.service

    def tearDown(self) -> None:
        routes_trading.trading_order_service = self.original_service

    def test_buy_route_returns_created_payload(self) -> None:
        client = TestClient(self.app)
        response = client.post(
            '/trading/orders/buy',
            json={
                'client_order_id': 'ROUTE-BUY-1',
                'account_number': '12345678',
                'account_product_code': '01',
                'symbol': '005930',
                'quantity': '10',
                'price': '70000',
            },
        )

        self.assertEqual(response.status_code, 201)
        self.assertEqual(response.json()['client_order_id'], 'ROUTE-BUY-1')
        self.assertEqual(self.service.buy_calls[0]['symbol'], '005930')

    def test_sell_route_maps_rejected_order_to_409(self) -> None:
        client = TestClient(self.app)
        response = client.post(
            '/trading/orders/sell',
            json={
                'client_order_id': 'ROUTE-SELL-1',
                'account_number': '12345678',
                'account_product_code': '01',
                'symbol': '005930',
                'quantity': '10',
                'price': '70000',
            },
        )

        self.assertEqual(response.status_code, 409)
        self.assertEqual(response.json()['detail']['order']['current_status'], 'rejected')

    def test_cancel_amend_get_and_list_routes_use_service(self) -> None:
        client = TestClient(self.app)

        cancel_response = client.post(
            '/trading/orders/ROUTE-ORDER-1/cancel',
            json={'account_reference': 'KIS:12345678-01', 'all_remaining': True},
        )
        amend_response = client.post(
            '/trading/orders/ROUTE-ORDER-1/amend',
            json={'account_reference': 'KIS:12345678-01', 'price': '71000'},
        )
        get_response = client.get(
            '/trading/orders/ROUTE-ORDER-1',
            params={'account_reference': 'KIS:12345678-01', 'refresh': 'true'},
        )
        list_response = client.get(
            '/trading/orders',
            params={'account_reference': 'KIS:12345678-01', 'status': 'submitted'},
        )

        self.assertEqual(cancel_response.status_code, 200)
        self.assertEqual(cancel_response.json()['order']['current_status'], 'cancel_pending')
        self.assertEqual(amend_response.status_code, 200)
        self.assertEqual(amend_response.json()['order']['current_status'], 'replace_pending')
        self.assertEqual(get_response.status_code, 200)
        self.assertEqual(get_response.json()['order']['client_order_id'], 'ROUTE-ORDER-1')
        self.assertEqual(list_response.status_code, 200)
        self.assertEqual(list_response.json()['count'], 1)
        self.assertTrue(self.service.cancel_calls)
        self.assertTrue(self.service.amend_calls)
        self.assertTrue(self.service.get_calls)
        self.assertTrue(self.service.list_calls)


if __name__ == '__main__':
    unittest.main()
