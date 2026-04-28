import unittest
from decimal import Decimal
from uuid import uuid4

from sqlalchemy import delete, func, select

from app.db import engine, session_scope
from app.db_models import BrokerOrder, BrokerOrderEvent, OrderRequest
from app.orders.store import (
    append_broker_order_event,
    list_broker_order_events,
    list_broker_orders,
    load_broker_order,
    upsert_broker_order,
    upsert_order_request,
)


class OrderStorePersistenceTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        OrderRequest.__table__.create(bind=engine, checkfirst=True)
        BrokerOrder.__table__.create(bind=engine, checkfirst=True)
        BrokerOrderEvent.__table__.create(bind=engine, checkfirst=True)

    def setUp(self) -> None:
        suffix = uuid4().hex[:10].upper()
        self.account_reference = f'TEST-ACCOUNT-{suffix}'
        self.client_order_id = f'TEST-CLIENT-{suffix}'
        self.broker_order_id = f'TEST-BROKER-{suffix}'

    def tearDown(self) -> None:
        with session_scope() as session:
            session.execute(
                delete(BrokerOrderEvent).where(
                    BrokerOrderEvent.account_reference == self.account_reference
                )
            )
            session.execute(
                delete(BrokerOrder).where(
                    BrokerOrder.account_reference == self.account_reference
                )
            )
            session.execute(
                delete(OrderRequest).where(
                    OrderRequest.account_reference == self.account_reference
                )
            )

    def test_upsert_order_request_is_idempotent_by_client_order_scope(self) -> None:
        created = upsert_order_request(
            {
                'broker_name': 'kis',
                'client_order_id': self.client_order_id,
                'source': 'strategy',
                'account_reference': self.account_reference,
                'symbol': '005930',
                'side': 'buy',
                'order_type': 'limit',
                'requested_quantity': '10',
                'requested_price': '70000',
                'time_in_force': 'day',
                'request_payload': {'requested_by': 'test'},
                'validation_status': 'valid',
                'submission_status': 'pending_submit',
                'submission_attempts': 0,
            }
        )
        updated = upsert_order_request(
            {
                'broker_name': 'kis',
                'client_order_id': self.client_order_id,
                'source': 'strategy',
                'account_reference': self.account_reference,
                'symbol': '005930',
                'side': 'buy',
                'order_type': 'limit',
                'requested_quantity': '10',
                'requested_price': '70000',
                'time_in_force': 'day',
                'request_payload': {'requested_by': 'test', 'retry': 1},
                'validation_status': 'valid',
                'submission_status': 'submitted',
                'submission_payload': {'attempt': 1, 'result': 'submitted'},
                'submission_attempts': 1,
                'submitted_at': '2026-04-28T00:01:00+00:00',
            }
        )

        with session_scope() as session:
            row_count = session.execute(
                select(func.count()).select_from(OrderRequest).where(
                    OrderRequest.account_reference == self.account_reference
                )
            ).scalar_one()

        self.assertEqual(created['id'], updated['id'])
        self.assertEqual(row_count, 1)
        self.assertEqual(updated['submission_status'], 'submitted')
        self.assertEqual(updated['submission_attempts'], 1)
        self.assertEqual(updated['request_payload']['retry'], 1)

    def test_upsert_broker_order_updates_current_snapshot_without_duplication(self) -> None:
        request_row = upsert_order_request(
            {
                'broker_name': 'kis',
                'client_order_id': self.client_order_id,
                'source': 'agent',
                'account_reference': self.account_reference,
                'symbol': '005930',
                'side': 'buy',
                'order_type': 'limit',
                'requested_quantity': '10',
                'requested_price': '70000',
                'request_payload': {'requested_by': 'snapshot-test'},
                'validation_status': 'valid',
            }
        )

        first_snapshot = upsert_broker_order(
            {
                'order_request_id': request_row['id'],
                'broker_name': 'kis',
                'client_order_id': self.client_order_id,
                'account_reference': self.account_reference,
                'symbol': '005930',
                'side': 'buy',
                'order_type': 'limit',
                'requested_quantity': '10',
                'requested_price': '70000',
                'current_status': 'submitted',
                'first_submitted_at': '2026-04-28T00:02:00+00:00',
                'last_broker_update_at': '2026-04-28T00:02:05+00:00',
                'raw_latest_payload': {'step': 'submit'},
                'snapshot_payload': {'status': 'submitted'},
            }
        )
        second_snapshot = upsert_broker_order(
            {
                'order_request_id': request_row['id'],
                'broker_name': 'kis',
                'broker_order_id': self.broker_order_id,
                'client_order_id': self.client_order_id,
                'account_reference': self.account_reference,
                'symbol': '005930',
                'side': 'buy',
                'order_type': 'limit',
                'requested_quantity': '10',
                'requested_price': '70000',
                'accepted_quantity': '10',
                'accepted_price': '70000',
                'filled_quantity': '4',
                'remaining_quantity': '6',
                'average_fill_price': '70010',
                'current_status': 'partial_fill',
                'last_broker_update_at': '2026-04-28T00:03:00+00:00',
                'raw_latest_payload': {'step': 'partial_fill'},
                'snapshot_payload': {'status': 'partial_fill'},
            }
        )

        with session_scope() as session:
            row_count = session.execute(
                select(func.count()).select_from(BrokerOrder).where(
                    BrokerOrder.account_reference == self.account_reference
                )
            ).scalar_one()

        loaded = load_broker_order(
            account_reference=self.account_reference,
            client_order_id=self.client_order_id,
        )
        partial_fill_rows = list_broker_orders(
            account_reference=self.account_reference,
            statuses=['partial_fill'],
        )

        self.assertEqual(first_snapshot['id'], second_snapshot['id'])
        self.assertEqual(row_count, 1)
        self.assertIsNotNone(loaded)
        self.assertEqual(loaded['broker_order_id'], self.broker_order_id)
        self.assertEqual(loaded['current_status'], 'partial_fill')
        self.assertEqual(loaded['filled_quantity'], Decimal('4'))
        self.assertEqual(loaded['remaining_quantity'], Decimal('6'))
        self.assertEqual(len(partial_fill_rows), 1)

    def test_append_broker_order_event_deduplicates_repeated_notifications(self) -> None:
        request_row = upsert_order_request(
            {
                'broker_name': 'kis',
                'client_order_id': self.client_order_id,
                'source': 'api',
                'account_reference': self.account_reference,
                'symbol': '005930',
                'side': 'buy',
                'order_type': 'limit',
                'requested_quantity': '10',
                'requested_price': '70000',
                'request_payload': {'requested_by': 'event-test'},
                'validation_status': 'valid',
            }
        )
        upsert_broker_order(
            {
                'order_request_id': request_row['id'],
                'broker_name': 'kis',
                'broker_order_id': self.broker_order_id,
                'client_order_id': self.client_order_id,
                'account_reference': self.account_reference,
                'symbol': '005930',
                'side': 'buy',
                'order_type': 'limit',
                'requested_quantity': '10',
                'requested_price': '70000',
                'accepted_quantity': '10',
                'accepted_price': '70000',
                'filled_quantity': '0',
                'remaining_quantity': '10',
                'current_status': 'accepted',
                'last_broker_update_at': '2026-04-28T00:04:00+00:00',
                'raw_latest_payload': {'step': 'accept'},
                'snapshot_payload': {'status': 'accepted'},
            }
        )

        first_event = append_broker_order_event(
            {
                'broker_name': 'kis',
                'account_reference': self.account_reference,
                'client_order_id': self.client_order_id,
                'broker_order_id': self.broker_order_id,
                'event_type': 'partial_fill',
                'event_sequence': '1',
                'event_occurred_at': '2026-04-28T00:04:30+00:00',
                'quantity_delta': '4',
                'cumulative_filled_quantity': '4',
                'remaining_quantity': '6',
                'average_fill_price': '70010',
                'fill_price': '70010',
                'resulting_status': 'partial_fill',
                'source': 'ws_notice',
                'event_dedup_key': 'ws:fill-1',
                'raw_payload': {'trade_id': 'fill-1'},
                'normalized_payload': {'status': 'partial_fill'},
            }
        )
        duplicate_event = append_broker_order_event(
            {
                'broker_name': 'kis',
                'account_reference': self.account_reference,
                'client_order_id': self.client_order_id,
                'broker_order_id': self.broker_order_id,
                'event_type': 'partial_fill',
                'event_sequence': '1',
                'event_occurred_at': '2026-04-28T00:04:30+00:00',
                'quantity_delta': '4',
                'cumulative_filled_quantity': '4',
                'remaining_quantity': '6',
                'average_fill_price': '70010',
                'fill_price': '70010',
                'resulting_status': 'partial_fill',
                'source': 'ws_notice',
                'event_dedup_key': 'ws:fill-1',
                'raw_payload': {'trade_id': 'fill-1'},
                'normalized_payload': {'status': 'partial_fill'},
            }
        )
        append_broker_order_event(
            {
                'broker_name': 'kis',
                'account_reference': self.account_reference,
                'client_order_id': self.client_order_id,
                'broker_order_id': self.broker_order_id,
                'event_type': 'reconcile_snapshot',
                'event_sequence': '2',
                'event_occurred_at': '2026-04-28T00:05:00+00:00',
                'cumulative_filled_quantity': '4',
                'remaining_quantity': '6',
                'resulting_status': 'partial_fill',
                'source': 'poll_reconcile',
                'event_dedup_key': 'poll:reconcile-1',
                'raw_payload': {'snapshot_id': 'reconcile-1'},
                'normalized_payload': {'status': 'partial_fill'},
            }
        )

        loaded = load_broker_order(
            account_reference=self.account_reference,
            broker_order_id=self.broker_order_id,
        )
        events = list_broker_order_events(
            account_reference=self.account_reference,
            client_order_id=self.client_order_id,
        )

        self.assertTrue(first_event['inserted'])
        self.assertFalse(duplicate_event['inserted'])
        self.assertEqual([item['event_dedup_key'] for item in events], ['ws:fill-1', 'poll:reconcile-1'])
        self.assertIsNotNone(loaded)
        self.assertEqual(loaded['current_status'], 'partial_fill')
        self.assertEqual(loaded['filled_quantity'], Decimal('4'))
        self.assertEqual(loaded['remaining_quantity'], Decimal('6'))


if __name__ == '__main__':
    unittest.main()
