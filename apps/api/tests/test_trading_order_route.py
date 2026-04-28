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


class FakeTradingOrderMonitoringService:
    def __init__(self) -> None:
        self.lifecycle_calls: list[dict] = []
        self.mismatch_calls: list[dict] = []
        self.stale_calls: list[dict] = []
        self.timeline_calls: list[dict] = []
        self.realtime_health_calls = 0

    def lifecycle_view(self, **kwargs):
        self.lifecycle_calls.append(kwargs)
        return {
            'summary': {'today_orders': 1, 'open_orders': 1},
            'buckets': {'today_orders': {'count': 1, 'items': [{'client_order_id': 'LIFE-ORDER-1'}]}},
        }

    def mismatches_view(self, **kwargs):
        self.mismatch_calls.append(kwargs)
        return {'count': 1, 'items': [{'client_order_id': 'MISMATCH-ORDER-1'}]}

    def stale_pending_view(self, **kwargs):
        self.stale_calls.append(kwargs)
        return {'count': 1, 'items': [{'client_order_id': 'STALE-ORDER-1'}]}

    def realtime_health(self):
        self.realtime_health_calls += 1
        return {'summary': {'healthy': True, 'status': 'subscribed'}}

    async def order_timeline_view(self, **kwargs):
        self.timeline_calls.append(kwargs)
        return {
            'summary': {'client_order_id': kwargs['client_order_id']},
            'timeline': {'count': 2, 'items': [{'event_type': 'submit_request'}, {'event_type': 'fill'}]},
        }


class FakeTradingOrderReconciliationService:
    def __init__(self) -> None:
        self.bulk_calls: list[dict] = []
        self.inspect_calls: list[dict] = []
        self.single_calls: list[dict] = []

    async def reconcile_recent_orders(self, **kwargs):
        self.bulk_calls.append(kwargs)
        return {
            'triggered': True,
            'trigger': kwargs.get('trigger'),
            'selected_count': 1,
            'orders': [{'client_order_id': 'ROUTE-ORDER-1'}] if kwargs.get('include_orders') else [],
        }

    def inspect_orders(self, **kwargs):
        self.inspect_calls.append(kwargs)
        return {
            'reconciliation': {'last_trigger': 'manual'},
            'mismatches': {'count': 1, 'items': [{'client_order_id': 'ROUTE-ORDER-1'}]},
            'stale_pending_orders': {'count': 0, 'items': []},
            'missing_fill_detail_orders': {'count': 0, 'items': []},
        }

    async def reconcile_single_order(self, **kwargs):
        self.single_calls.append(kwargs)
        return {
            'operation': 'reconcile',
            'order': {'client_order_id': kwargs['client_order_id'], 'current_status': 'filled'},
            'reconcile': {'reconcile_status': 'repaired'},
        }


class TradingOrderRouteTests(unittest.TestCase):
    def setUp(self) -> None:
        self.app = FastAPI()
        self.app.include_router(routes_trading.router)
        self.service = FakeTradingOrderService()
        self.monitoring_service = FakeTradingOrderMonitoringService()
        self.reconcile_service = FakeTradingOrderReconciliationService()
        self.original_service = routes_trading.trading_order_service
        self.original_monitoring_service = routes_trading.trading_order_monitoring_service
        self.original_reconcile_service = routes_trading.trading_order_reconciliation_service
        routes_trading.trading_order_service = self.service
        routes_trading.trading_order_monitoring_service = self.monitoring_service
        routes_trading.trading_order_reconciliation_service = self.reconcile_service

    def tearDown(self) -> None:
        routes_trading.trading_order_service = self.original_service
        routes_trading.trading_order_monitoring_service = self.original_monitoring_service
        routes_trading.trading_order_reconciliation_service = self.original_reconcile_service

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

    def test_reconcile_and_inspect_routes_use_reconciliation_service(self) -> None:
        client = TestClient(self.app)

        bulk_response = client.post(
            '/trading/orders/reconcile',
            json={
                'account_reference': 'KIS:12345678-01',
                'symbol': '005930',
                'limit': 25,
                'reason': 'manual-audit',
                'include_orders': True,
            },
        )
        inspect_response = client.get(
            '/trading/orders/inspect',
            params={
                'account_reference': 'KIS:12345678-01',
                'limit': 25,
                'stale_after_minutes': 30,
            },
        )
        single_response = client.post(
            '/trading/orders/ROUTE-ORDER-1/reconcile',
            json={'account_reference': 'KIS:12345678-01', 'reason': 'single-repair'},
        )

        self.assertEqual(bulk_response.status_code, 200)
        self.assertEqual(bulk_response.json()['selected_count'], 1)
        self.assertEqual(inspect_response.status_code, 200)
        self.assertEqual(inspect_response.json()['mismatches']['count'], 1)
        self.assertEqual(single_response.status_code, 200)
        self.assertEqual(single_response.json()['order']['current_status'], 'filled')
        self.assertEqual(self.reconcile_service.bulk_calls[0]['trigger'], 'manual')
        self.assertEqual(self.reconcile_service.single_calls[0]['trigger'], 'manual_single')
        self.assertEqual(self.reconcile_service.inspect_calls[0]['stale_after_minutes'], 30)

    def test_monitoring_routes_use_monitoring_service(self) -> None:
        client = TestClient(self.app)

        lifecycle_response = client.get(
            '/trading/orders/lifecycle',
            params={'account_reference': 'KIS:12345678-01', 'limit': 20, 'stale_after_minutes': 30},
        )
        mismatch_response = client.get(
            '/trading/orders/mismatches',
            params={'account_reference': 'KIS:12345678-01', 'symbol': '005930', 'limit': 10},
        )
        stale_response = client.get(
            '/trading/orders/stale-pending',
            params={'account_reference': 'KIS:12345678-01', 'limit': 10},
        )
        timeline_response = client.get(
            '/trading/orders/ROUTE-ORDER-1/timeline',
            params={'account_reference': 'KIS:12345678-01', 'refresh': 'true'},
        )
        realtime_response = client.get('/trading/orders/realtime-health')

        self.assertEqual(lifecycle_response.status_code, 200)
        self.assertEqual(lifecycle_response.json()['summary']['today_orders'], 1)
        self.assertEqual(mismatch_response.status_code, 200)
        self.assertEqual(mismatch_response.json()['items'][0]['client_order_id'], 'MISMATCH-ORDER-1')
        self.assertEqual(stale_response.status_code, 200)
        self.assertEqual(stale_response.json()['items'][0]['client_order_id'], 'STALE-ORDER-1')
        self.assertEqual(timeline_response.status_code, 200)
        self.assertEqual(timeline_response.json()['timeline']['count'], 2)
        self.assertEqual(realtime_response.status_code, 200)
        self.assertTrue(realtime_response.json()['summary']['healthy'])
        self.assertEqual(self.monitoring_service.lifecycle_calls[0]['stale_after_minutes'], 30)
        self.assertEqual(self.monitoring_service.timeline_calls[0]['refresh'], True)
        self.assertEqual(self.monitoring_service.realtime_health_calls, 1)


if __name__ == '__main__':
    unittest.main()
