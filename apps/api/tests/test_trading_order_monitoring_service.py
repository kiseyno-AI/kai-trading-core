import unittest
from datetime import datetime, timedelta, timezone
from uuid import uuid4

from sqlalchemy import delete

from app.db import engine, session_scope
from app.db_models import BrokerOrder, BrokerOrderEvent, OrderRequest
from app.market.clock import KST
from app.orders.monitoring import TradingOrderMonitoringService
from app.orders.reconciliation import TradingOrderReconciliationService
from app.orders.store import upsert_broker_order, upsert_order_request


class FakeRealtimeNoticeService:
    def __init__(self, payload: dict | None = None) -> None:
        self.payload = payload or {
            'enabled': True,
            'configured': True,
            'status': 'subscribed',
            'last_message_received_at': '2026-04-28T00:00:00+00:00',
            'last_message_age_seconds': 2.5,
            'reconnect_attempts': 0,
            'consecutive_failures': 0,
            'metrics': {
                'messages_received': 5,
                'parse_errors': 0,
            },
        }

    def health_snapshot(self) -> dict:
        return self.payload


class FakeOrderService:
    def __init__(self) -> None:
        self.calls: list[dict] = []

    async def get_order(self, **kwargs):
        self.calls.append(kwargs)
        return {
            'operation': 'get',
            'broker_name': 'kis',
            'account_reference': kwargs['account_reference'],
            'client_order_id': kwargs['client_order_id'],
            'request': {
                'source': 'api',
                'submitted_at': '2026-04-28T00:00:00+00:00',
                'submission_status': 'submitted',
                'validation_status': 'valid',
            },
            'order': {
                'client_order_id': kwargs['client_order_id'],
                'broker_order_id': '0000123456',
                'account_reference': kwargs['account_reference'],
                'symbol': '005930',
                'side': 'buy',
                'order_type': 'limit',
                'requested_quantity': '10',
                'requested_price': '70000',
                'filled_quantity': '10',
                'remaining_quantity': '0',
                'average_fill_price': '70120',
                'current_status': 'filled',
                'first_submitted_at': '2026-04-28T00:00:00+00:00',
                'last_broker_update_at': '2026-04-28T00:05:00+00:00',
                'updated_at': '2026-04-28T00:05:00+00:00',
                'last_fill_at': '2026-04-28T00:05:00+00:00',
                'last_reconciled_at': '2026-04-28T00:06:00+00:00',
                'last_reconcile_status': 'matched',
                'last_reconcile_trigger': 'read_refresh',
                'last_reconcile_payload': {'mismatch_reasons': []},
            },
            'events': [
                {
                    'id': 1,
                    'event_type': 'submit_request',
                    'source': 'local_request',
                    'event_occurred_at': '2026-04-28T00:00:00+00:00',
                    'normalized_payload': {'status': 'pending_submit'},
                    'raw_payload': {'step': 'request'},
                },
                {
                    'id': 2,
                    'event_type': 'fill',
                    'source': 'poll_reconcile',
                    'event_occurred_at': '2026-04-28T00:05:00+00:00',
                    'resulting_status': 'filled',
                    'cumulative_filled_quantity': '10',
                    'fill_price': '70120',
                    'normalized_payload': {'status': 'filled'},
                    'raw_payload': {'step': 'fill'},
                },
            ],
        }


class TradingOrderMonitoringLifecycleTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        OrderRequest.__table__.create(bind=engine, checkfirst=True)
        BrokerOrder.__table__.create(bind=engine, checkfirst=True)
        BrokerOrderEvent.__table__.create(bind=engine, checkfirst=True)

    def setUp(self) -> None:
        self.account_reference = f'KIS:77{uuid4().hex[:6].upper()}-01'
        self.symbol = '005930'
        self.service = TradingOrderMonitoringService(
            reconciliation_service=TradingOrderReconciliationService(),
            realtime_notice_service=FakeRealtimeNoticeService(),
        )
        self.now = datetime.now(timezone.utc)
        local_today = self.now.astimezone(KST).replace(hour=9, minute=0, second=0, microsecond=0)
        self.today_submitted_at = local_today.astimezone(timezone.utc)
        self.yesterday_submitted_at = (local_today - timedelta(days=1)).astimezone(timezone.utc)

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
        first_submitted_at: datetime,
        last_broker_update_at: datetime,
        filled_quantity: str = '0',
        remaining_quantity: str = '10',
        average_fill_price: str | None = '70000',
        last_reconcile_status: str | None = None,
        last_reconcile_payload: dict | None = None,
        status_reason: str | None = None,
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
                'request_payload': {'source': 'monitoring-test'},
                'validation_status': 'valid',
                'submission_status': 'submitted',
                'submitted_at': first_submitted_at.isoformat(),
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
                'status_reason': status_reason,
                'first_submitted_at': first_submitted_at.isoformat(),
                'last_broker_update_at': last_broker_update_at.isoformat(),
                'last_reconciled_at': self.now.isoformat() if last_reconcile_status else None,
                'last_reconcile_status': last_reconcile_status,
                'last_reconcile_trigger': 'manual' if last_reconcile_status else None,
                'last_reconcile_payload': last_reconcile_payload,
                'raw_latest_payload': {'status': current_status},
                'snapshot_payload': {'status': current_status},
            }
        )

    def test_lifecycle_view_groups_orders_into_web_facing_buckets(self) -> None:
        self._seed_order(
            client_order_id='OPEN-TODAY',
            current_status='accepted',
            first_submitted_at=self.today_submitted_at,
            last_broker_update_at=self.now - timedelta(minutes=5),
        )
        self._seed_order(
            client_order_id='PARTIAL-TODAY',
            current_status='partial_fill',
            first_submitted_at=self.today_submitted_at + timedelta(minutes=1),
            last_broker_update_at=self.now - timedelta(minutes=3),
            filled_quantity='4',
            remaining_quantity='6',
            average_fill_price='70100',
            last_reconcile_status='matched',
            last_reconcile_payload={'mismatch_reasons': [], 'missing_fill_detail': False, 'stale_pending': False},
        )
        self._seed_order(
            client_order_id='FILLED-REPAIRED',
            current_status='filled',
            first_submitted_at=self.today_submitted_at + timedelta(minutes=2),
            last_broker_update_at=self.now - timedelta(minutes=2),
            filled_quantity='10',
            remaining_quantity='0',
            average_fill_price='70120',
            last_reconcile_status='repaired',
            last_reconcile_payload={'mismatch_reasons': ['status'], 'missing_fill_detail': False, 'stale_pending': False},
        )
        self._seed_order(
            client_order_id='STALE-TODAY',
            current_status='submitted',
            first_submitted_at=self.today_submitted_at + timedelta(minutes=3),
            last_broker_update_at=self.now - timedelta(minutes=60),
        )
        self._seed_order(
            client_order_id='REJECTED-TODAY',
            current_status='rejected',
            first_submitted_at=self.today_submitted_at + timedelta(minutes=4),
            last_broker_update_at=self.now - timedelta(minutes=1),
            status_reason='insufficient balance',
        )
        self._seed_order(
            client_order_id='REPLACED-TODAY',
            current_status='replaced',
            first_submitted_at=self.today_submitted_at + timedelta(minutes=5),
            last_broker_update_at=self.now - timedelta(minutes=1),
        )
        self._seed_order(
            client_order_id='YESTERDAY-ORDER',
            current_status='accepted',
            first_submitted_at=self.yesterday_submitted_at,
            last_broker_update_at=self.now - timedelta(minutes=5),
        )

        result = self.service.lifecycle_view(
            account_reference=self.account_reference,
            limit=10,
            stale_after_minutes=15,
        )

        self.assertEqual(result['summary']['today_orders'], 6)
        self.assertEqual(result['summary']['open_orders'], 4)
        self.assertEqual(result['summary']['partially_filled_orders'], 1)
        self.assertEqual(result['summary']['filled_orders'], 1)
        self.assertEqual(result['summary']['canceled_or_rejected_orders'], 1)
        self.assertEqual(result['summary']['replaced_orders'], 1)
        self.assertEqual(result['summary']['mismatch_count'], 1)
        self.assertEqual(result['summary']['stale_pending_count'], 1)
        self.assertTrue(result['summary']['realtime_healthy'])

        today_ids = {item['client_order_id'] for item in result['buckets']['today_orders']['items']}
        self.assertNotIn('YESTERDAY-ORDER', today_ids)
        self.assertIn('STALE-TODAY', {item['client_order_id'] for item in result['buckets']['open_orders']['items']})

        filled_item = result['buckets']['filled_orders']['items'][0]
        self.assertEqual(filled_item['client_order_id'], 'FILLED-REPAIRED')
        self.assertIn('reconcile_mismatch', filled_item['attention_flags'])

        stale_open_item = next(
            item for item in result['buckets']['open_orders']['items']
            if item['client_order_id'] == 'STALE-TODAY'
        )
        self.assertTrue(stale_open_item['is_stale_pending'])
        self.assertIn('stale_pending', stale_open_item['attention_flags'])

    def test_mismatch_and_stale_views_return_monitoring_items(self) -> None:
        self._seed_order(
            client_order_id='REPAIRED-1',
            current_status='filled',
            first_submitted_at=self.today_submitted_at,
            last_broker_update_at=self.now - timedelta(minutes=10),
            filled_quantity='10',
            remaining_quantity='0',
            average_fill_price='70120',
            last_reconcile_status='repaired',
            last_reconcile_payload={'mismatch_reasons': ['status'], 'missing_fill_detail': False, 'stale_pending': False},
        )
        self._seed_order(
            client_order_id='STALE-1',
            current_status='submitted',
            first_submitted_at=self.today_submitted_at,
            last_broker_update_at=self.now - timedelta(minutes=45),
        )

        mismatch_result = self.service.mismatches_view(
            account_reference=self.account_reference,
            limit=10,
            stale_after_minutes=15,
        )
        stale_result = self.service.stale_pending_view(
            account_reference=self.account_reference,
            limit=10,
            stale_after_minutes=15,
        )

        self.assertEqual(mismatch_result['count'], 1)
        self.assertEqual(mismatch_result['items'][0]['client_order_id'], 'REPAIRED-1')
        self.assertIn('reconcile_mismatch', mismatch_result['items'][0]['attention_flags'])
        self.assertEqual(stale_result['count'], 1)
        self.assertEqual(stale_result['items'][0]['client_order_id'], 'STALE-1')
        self.assertTrue(stale_result['items'][0]['is_stale_pending'])


class TradingOrderMonitoringAsyncTests(unittest.IsolatedAsyncioTestCase):
    async def test_order_timeline_view_normalizes_labels_and_latest_event(self) -> None:
        fake_order_service = FakeOrderService()
        service = TradingOrderMonitoringService(
            order_service=fake_order_service,
            realtime_notice_service=FakeRealtimeNoticeService(),
        )

        result = await service.order_timeline_view(
            account_reference='KIS:12345678-01',
            client_order_id='TIMELINE-ORDER-1',
            refresh=True,
        )

        self.assertEqual(result['summary']['current_status'], 'filled')
        self.assertEqual(result['timeline']['count'], 2)
        self.assertEqual(result['timeline']['latest_event']['event_type'], 'fill')
        self.assertTrue(result['timeline']['latest_event']['is_execution_event'])
        self.assertTrue(result['timeline']['latest_event']['is_reconciliation_event'])
        self.assertEqual(fake_order_service.calls[0]['refresh'], True)

    def test_realtime_health_summary_marks_attention_flags(self) -> None:
        service = TradingOrderMonitoringService(
            realtime_notice_service=FakeRealtimeNoticeService(
                {
                    'enabled': True,
                    'configured': False,
                    'status': 'reconnecting',
                    'reconnect_attempts': 3,
                    'consecutive_failures': 2,
                    'last_message_age_seconds': None,
                    'metrics': {'parse_errors': 1},
                }
            ),
        )

        result = service.realtime_health()

        self.assertFalse(result['summary']['healthy'])
        self.assertIn('configuration_incomplete', result['summary']['attention_flags'])
        self.assertIn('reconnecting', result['summary']['attention_flags'])
        self.assertIn('parse_errors_seen', result['summary']['attention_flags'])


if __name__ == '__main__':
    unittest.main()
