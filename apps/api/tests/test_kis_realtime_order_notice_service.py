import unittest
from datetime import datetime, timezone
from decimal import Decimal
from uuid import uuid4

from sqlalchemy import delete

from app.db import engine, session_scope
from app.db_models import BrokerOrder, BrokerOrderEvent, OrderRequest
from app.orders.kis_realtime_order_notices import KISRealtimeOrderNoticeService
from app.orders.store import (
    append_broker_order_event,
    list_broker_order_events,
    load_broker_order,
    upsert_broker_order,
    upsert_order_request,
)


class FakeKISRealtimeClient:
    def _environment(self) -> str:
        return 'demo'

    def websocket_url(self) -> str:
        return 'ws://example.test:31000'


class KISRealtimeOrderNoticeServiceTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        OrderRequest.__table__.create(bind=engine, checkfirst=True)
        BrokerOrder.__table__.create(bind=engine, checkfirst=True)
        BrokerOrderEvent.__table__.create(bind=engine, checkfirst=True)

    def setUp(self) -> None:
        suffix = uuid4().int % 10_000_000
        self.account_number = f'1{suffix:07d}'
        self.account_product_code = '01'
        self.account_reference = f'KIS:{self.account_number}-{self.account_product_code}'
        self.account_notice_number = f'{self.account_number}{self.account_product_code}'
        self.client_order_id = f'TEST-RT-{uuid4().hex[:8].upper()}'
        self.broker_order_id = f'0000{uuid4().int % 1_000_000:06d}'
        self.service = KISRealtimeOrderNoticeService(client=FakeKISRealtimeClient())

    def tearDown(self) -> None:
        with session_scope() as session:
            session.execute(
                delete(BrokerOrderEvent).where(BrokerOrderEvent.account_reference == self.account_reference)
            )
            session.execute(
                delete(BrokerOrder).where(BrokerOrder.account_reference == self.account_reference)
            )
            session.execute(
                delete(OrderRequest).where(OrderRequest.account_reference == self.account_reference)
            )

    def _seed_order(self) -> None:
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
                'request_payload': {'requested_by': 'realtime-test'},
                'validation_status': 'valid',
                'submission_status': 'submitted',
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
                'current_status': 'submitted',
                'last_broker_update_at': '2026-04-28T00:00:00+00:00',
                'raw_latest_payload': {'step': 'submitted'},
                'snapshot_payload': {'status': 'submitted'},
            }
        )

    def _build_notice(
        self,
        *,
        cntg_yn: str,
        stock_cntg_hour: str,
        cntg_qty: str,
        cntg_unpr: str,
        rfus_yn: str = '0',
        acpt_yn: str = '2',
        rctf_cls: str = '00',
    ) -> dict[str, str]:
        return {
            'CUST_ID': 'HTS-USER-1',
            'ACNT_NO': self.account_notice_number,
            'ODER_NO': self.broker_order_id,
            'OODER_NO': '',
            'SELN_BYOV_CLS': '02',
            'RCTF_CLS': rctf_cls,
            'ODER_KIND': '00',
            'ODER_COND': '00',
            'STCK_SHRN_ISCD': '005930',
            'CNTG_QTY': cntg_qty,
            'CNTG_UNPR': cntg_unpr,
            'STCK_CNTG_HOUR': stock_cntg_hour,
            'RFUS_YN': rfus_yn,
            'CNTG_YN': cntg_yn,
            'ACPT_YN': acpt_yn,
            'BRNC_NO': '06010',
            'ODER_QTY': '10',
            'ACNT_NAME': 'Realtime Test',
            'ORD_COND_PRC': '',
            'ORD_EXG_GB': '1',
            'POPUP_YN': 'N',
            'FILLER': '',
            'CRDT_CLS': '00',
            'CRDT_LOAN_DATE': '',
            'CNTG_ISNM40': 'Samsung Electronics',
            'ODER_PRC': '70000',
        }

    def test_ingest_accept_then_fill_notice_updates_snapshot_and_events(self) -> None:
        self._seed_order()

        accept_result = self.service.ingest_notice_fields(
            self._build_notice(
                cntg_yn='1',
                stock_cntg_hour='090001',
                cntg_qty='10',
                cntg_unpr='70000',
            ),
        )
        fill_result = self.service.ingest_notice_fields(
            self._build_notice(
                cntg_yn='2',
                stock_cntg_hour='090500',
                cntg_qty='4',
                cntg_unpr='70010',
            ),
        )

        order_row = load_broker_order(
            account_reference=self.account_reference,
            broker_order_id=self.broker_order_id,
        )
        events = list_broker_order_events(
            account_reference=self.account_reference,
            client_order_id=self.client_order_id,
        )

        self.assertTrue(accept_result['inserted'])
        self.assertTrue(fill_result['inserted'])
        self.assertIsNotNone(order_row)
        self.assertEqual(order_row['current_status'], 'partial_fill')
        self.assertEqual(order_row['filled_quantity'], Decimal('4'))
        self.assertEqual(order_row['remaining_quantity'], Decimal('6'))
        self.assertEqual(order_row['average_fill_price'], Decimal('70010'))
        self.assertEqual([event['event_type'] for event in events], ['accept', 'partial_fill'])

    def test_duplicate_and_older_notice_do_not_regress_filled_snapshot(self) -> None:
        self._seed_order()
        received_at = datetime(2026, 4, 28, 0, 20, tzinfo=timezone.utc)

        fill_notice = self._build_notice(
            cntg_yn='2',
            stock_cntg_hour='091500',
            cntg_qty='10',
            cntg_unpr='70100',
        )
        first_fill = self.service.ingest_notice_fields(
            fill_notice,
            received_at=received_at,
        )
        duplicate_fill = self.service.ingest_notice_fields(
            fill_notice,
            received_at=received_at,
        )
        older_partial = self.service.ingest_notice_fields(
            self._build_notice(
                cntg_yn='2',
                stock_cntg_hour='091200',
                cntg_qty='4',
                cntg_unpr='70010',
            ),
            received_at=received_at,
        )

        order_row = load_broker_order(
            account_reference=self.account_reference,
            broker_order_id=self.broker_order_id,
        )
        events = list_broker_order_events(
            account_reference=self.account_reference,
            client_order_id=self.client_order_id,
        )

        self.assertTrue(first_fill['inserted'])
        self.assertFalse(duplicate_fill['inserted'])
        self.assertTrue(older_partial['inserted'])
        self.assertIsNotNone(order_row)
        self.assertEqual(order_row['current_status'], 'filled')
        self.assertEqual(order_row['filled_quantity'], Decimal('10'))
        self.assertEqual(order_row['remaining_quantity'], Decimal('0'))
        self.assertEqual([event['event_type'] for event in events], ['partial_fill', 'fill'])

    def test_later_ws_fill_notice_preserves_reconciled_filled_snapshot(self) -> None:
        self._seed_order()
        append_broker_order_event(
            {
                'broker_name': 'kis',
                'account_reference': self.account_reference,
                'client_order_id': self.client_order_id,
                'broker_order_id': self.broker_order_id,
                'symbol': '005930',
                'event_type': 'fill',
                'event_occurred_at': '2026-04-28T00:20:00+00:00',
                'source': 'rest_query',
                'event_dedup_key': f'reconcile:{self.client_order_id}:filled:10:0',
                'raw_payload': {'source': 'reconcile'},
                'normalized_payload': {'status': 'filled', 'action': 'reconcile'},
                'resulting_status': 'filled',
                'cumulative_filled_quantity': '10',
                'remaining_quantity': '0',
                'average_fill_price': '70080',
                'fill_price': '70080',
            }
        )

        result = self.service.ingest_notice_fields(
            self._build_notice(
                cntg_yn='2',
                stock_cntg_hour='092500',
                cntg_qty='4',
                cntg_unpr='70010',
            ),
            received_at=datetime(2026, 4, 28, 0, 25, tzinfo=timezone.utc),
        )

        order_row = load_broker_order(
            account_reference=self.account_reference,
            broker_order_id=self.broker_order_id,
        )
        events = list_broker_order_events(
            account_reference=self.account_reference,
            client_order_id=self.client_order_id,
        )

        self.assertTrue(result['inserted'])
        self.assertIsNotNone(order_row)
        self.assertEqual(order_row['current_status'], 'filled')
        self.assertEqual(order_row['filled_quantity'], Decimal('10'))
        self.assertEqual(order_row['remaining_quantity'], Decimal('0'))
        self.assertEqual(order_row['average_fill_price'], Decimal('70080'))
        self.assertEqual(events[-1]['source'], 'ws_notice')
        self.assertEqual(events[-1]['cumulative_filled_quantity'], Decimal('10'))


if __name__ == '__main__':
    unittest.main()
