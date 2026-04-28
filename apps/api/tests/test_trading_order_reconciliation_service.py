import unittest
from uuid import uuid4

from sqlalchemy import delete

from app.db import engine, session_scope
from app.db_models import BrokerOrder, BrokerOrderEvent, OrderRequest
from app.orders.reconciliation import TradingOrderReconciliationService
from app.orders.store import upsert_broker_order, upsert_order_request


class TradingOrderReconciliationInspectTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        OrderRequest.__table__.create(bind=engine, checkfirst=True)
        BrokerOrder.__table__.create(bind=engine, checkfirst=True)
        BrokerOrderEvent.__table__.create(bind=engine, checkfirst=True)

    def setUp(self) -> None:
        self.service = TradingOrderReconciliationService()
        self.account_reference = f'KIS:88{uuid4().hex[:6].upper()}-01'
        self.symbol = '005930'

    def tearDown(self) -> None:
        with session_scope() as session:
            session.execute(delete(BrokerOrderEvent).where(BrokerOrderEvent.account_reference == self.account_reference))
            session.execute(delete(BrokerOrder).where(BrokerOrder.account_reference == self.account_reference))
            session.execute(delete(OrderRequest).where(OrderRequest.account_reference == self.account_reference))

    def _seed_order(
        self,
        *,
        client_order_id: str,
        current_status: str,
        last_broker_update_at: str,
        filled_quantity: str = '0',
        remaining_quantity: str = '10',
        average_fill_price: str | None = '70000',
        last_reconcile_status: str | None = None,
        last_reconcile_payload: dict | None = None,
    ) -> None:
        request_row = upsert_order_request(
            {
                'broker_name': 'kis',
                'client_order_id': client_order_id,
                'source': 'api',
                'account_reference': self.account_reference,
                'symbol': self.symbol,
                'side': 'buy',
                'order_type': 'limit',
                'requested_quantity': '10',
                'requested_price': '70000',
                'request_payload': {'source': 'inspect-test'},
                'validation_status': 'valid',
                'submission_status': 'submitted',
            }
        )
        upsert_broker_order(
            {
                'order_request_id': request_row['id'],
                'broker_name': 'kis',
                'broker_order_id': f'BROKER-{client_order_id}',
                'client_order_id': client_order_id,
                'account_reference': self.account_reference,
                'symbol': self.symbol,
                'side': 'buy',
                'order_type': 'limit',
                'requested_quantity': '10',
                'requested_price': '70000',
                'accepted_quantity': '10',
                'accepted_price': '70000',
                'filled_quantity': filled_quantity,
                'remaining_quantity': remaining_quantity,
                'average_fill_price': average_fill_price,
                'current_status': current_status,
                'last_broker_update_at': last_broker_update_at,
                'last_reconciled_at': '2026-04-28T00:30:00+00:00' if last_reconcile_status else None,
                'last_reconcile_status': last_reconcile_status,
                'last_reconcile_trigger': 'manual' if last_reconcile_status else None,
                'last_reconcile_payload': last_reconcile_payload,
                'raw_latest_payload': {'status': current_status},
                'snapshot_payload': {'status': current_status},
            }
        )

    def test_inspect_orders_separates_mismatch_stale_and_missing_fill_detail(self) -> None:
        self._seed_order(
            client_order_id='REPAIRED-1',
            current_status='filled',
            filled_quantity='10',
            remaining_quantity='0',
            average_fill_price='70120',
            last_broker_update_at='2026-04-28T00:20:00+00:00',
            last_reconcile_status='repaired',
            last_reconcile_payload={
                'mismatch_reasons': ['status', 'filled_quantity'],
                'repair_applied': True,
                'missing_fill_detail': False,
                'stale_pending': False,
            },
        )
        self._seed_order(
            client_order_id='STALE-1',
            current_status='submitted',
            filled_quantity='0',
            remaining_quantity='10',
            average_fill_price=None,
            last_broker_update_at='2026-04-27T23:00:00+00:00',
        )
        self._seed_order(
            client_order_id='MISSING-FILL-1',
            current_status='partial_fill',
            filled_quantity='4',
            remaining_quantity='6',
            average_fill_price=None,
            last_broker_update_at='2026-04-28T00:25:00+00:00',
            last_reconcile_status='missing_fill_detail',
            last_reconcile_payload={
                'mismatch_reasons': [],
                'repair_applied': False,
                'missing_fill_detail': True,
                'stale_pending': False,
            },
        )

        result = self.service.inspect_orders(
            account_reference=self.account_reference,
            limit=10,
            stale_after_minutes=15,
        )

        self.assertEqual(result['mismatches']['count'], 1)
        self.assertEqual(result['mismatches']['items'][0]['client_order_id'], 'REPAIRED-1')
        self.assertEqual(result['stale_pending_orders']['count'], 1)
        self.assertEqual(result['stale_pending_orders']['items'][0]['client_order_id'], 'STALE-1')
        self.assertEqual(result['missing_fill_detail_orders']['count'], 1)
        self.assertEqual(result['missing_fill_detail_orders']['items'][0]['client_order_id'], 'MISSING-FILL-1')


if __name__ == '__main__':
    unittest.main()
