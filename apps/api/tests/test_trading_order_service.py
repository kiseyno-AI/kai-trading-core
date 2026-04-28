import asyncio
import unittest
from decimal import Decimal
from uuid import uuid4

from sqlalchemy import delete

from app.brokers.kis.client import KISAPIError, KISTimeoutError
from app.db import engine, session_scope
from app.db_models import BrokerOrder, BrokerOrderEvent, OrderRequest
from app.orders.service import (
    TradingOrderPendingSubmissionError,
    TradingOrderRejectedError,
    TradingOrderValidationError,
    trading_order_service as default_service,
    TradingOrderService,
)
from app.orders.store import (
    list_broker_order_events,
    load_broker_order,
    load_order_request,
)


class FakeKISOrderClient:
    def __init__(self) -> None:
        self.submit_calls = 0
        self.cancel_replace_calls = 0
        self.daily_fill_calls = 0
        self.cancelable_calls = 0
        self.submit_started = asyncio.Event()
        self.resume_submit = asyncio.Event()
        self.resume_submit.set()
        self.submit_result = {
            'rt_cd': '0',
            'msg_cd': 'APBK0013',
            'msg1': 'success',
            'output': {
                'KRX_FWDG_ORD_ORGNO': '06010',
                'ODNO': '0000123456',
                'ORD_TMD': '091500',
            },
        }
        self.submit_exception = None
        self.cancel_replace_result = {
            'rt_cd': '0',
            'msg_cd': 'APBK0013',
            'msg1': 'success',
            'output': {
                'KRX_FWDG_ORD_ORGNO': '06010',
                'ODNO': '0000456789',
                'ORD_TMD': '093000',
            },
        }
        self.cancel_replace_exception = None
        self.daily_fill_result = {'output1': [], 'output2': {}, 'raw_pages': [], 'page_count': 0, 'max_pages_reached': False}
        self.cancelable_result = {'output': [], 'raw_pages': [], 'page_count': 0, 'max_pages_reached': False}

    def _environment(self) -> str:
        return 'demo'

    async def submit_domestic_cash_order(self, **kwargs):
        self.submit_calls += 1
        self.submit_started.set()
        await self.resume_submit.wait()
        if self.submit_exception is not None:
            raise self.submit_exception
        return self.submit_result

    async def submit_domestic_order_cancel_replace(self, **kwargs):
        self.cancel_replace_calls += 1
        if self.cancel_replace_exception is not None:
            raise self.cancel_replace_exception
        return self.cancel_replace_result

    async def fetch_domestic_daily_order_fills(self, **kwargs):
        self.daily_fill_calls += 1
        return self.daily_fill_result

    async def fetch_domestic_cancelable_orders(self, **kwargs):
        self.cancelable_calls += 1
        return self.cancelable_result


class TradingOrderServiceTests(unittest.IsolatedAsyncioTestCase):
    @classmethod
    def setUpClass(cls) -> None:
        OrderRequest.__table__.create(bind=engine, checkfirst=True)
        BrokerOrder.__table__.create(bind=engine, checkfirst=True)
        BrokerOrderEvent.__table__.create(bind=engine, checkfirst=True)

    def setUp(self) -> None:
        suffix = uuid4().hex[:8].upper()
        self.account_number = f'12{suffix[:6]}'
        self.account_product_code = '01'
        self.account_reference = f'KIS:{self.account_number}-{self.account_product_code}'
        self.client_order_id = f'TEST-ORDER-{suffix}'
        self.symbol = '005930'
        self.fake_client = FakeKISOrderClient()
        self.service = TradingOrderService(client=self.fake_client)

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

    async def _submit_buy_order(self, *, client_order_id: str | None = None) -> dict:
        return await self.service.submit_buy_order(
            client_order_id=client_order_id or self.client_order_id,
            account_number=self.account_number,
            account_product_code=self.account_product_code,
            symbol=self.symbol,
            quantity=Decimal('10'),
            price=Decimal('70000'),
            order_type='limit',
            time_in_force='day',
            source='api',
        )

    async def test_submit_buy_order_persists_request_snapshot_and_events(self) -> None:
        result = await self._submit_buy_order()

        request_row = load_order_request(account_reference=self.account_reference, client_order_id=self.client_order_id)
        order_row = load_broker_order(account_reference=self.account_reference, client_order_id=self.client_order_id)
        events = list_broker_order_events(
            account_reference=self.account_reference,
            client_order_id=self.client_order_id,
        )

        self.assertTrue(result['created'])
        self.assertFalse(result['idempotent_replay'])
        self.assertEqual(self.fake_client.submit_calls, 1)
        self.assertEqual(request_row['submission_status'], 'submitted')
        self.assertEqual(request_row['submission_attempts'], 1)
        self.assertEqual(order_row['current_status'], 'submitted')
        self.assertEqual(order_row['broker_order_id'], '0000123456')
        self.assertEqual([item['event_type'] for item in events], ['submit_request', 'submit_response'])

    async def test_submit_rejection_persists_rejected_state(self) -> None:
        self.fake_client.submit_exception = KISAPIError(
            'KIS request rejected /uapi/domestic-stock/v1/trading/order-cash: rejected',
            payload={'rt_cd': '1', 'msg_cd': 'OPSQ0002', 'msg1': 'rejected'},
            msg_code='OPSQ0002',
            result_code='1',
        )

        with self.assertRaises(TradingOrderRejectedError) as ctx:
            await self._submit_buy_order()

        request_row = load_order_request(account_reference=self.account_reference, client_order_id=self.client_order_id)
        order_row = load_broker_order(account_reference=self.account_reference, client_order_id=self.client_order_id)
        events = list_broker_order_events(
            account_reference=self.account_reference,
            client_order_id=self.client_order_id,
        )

        self.assertIn('request', ctx.exception.payload)
        self.assertEqual(request_row['submission_status'], 'rejected')
        self.assertEqual(order_row['current_status'], 'rejected')
        self.assertEqual(events[-1]['event_type'], 'reject')

    async def test_submit_timeout_blocks_replay_with_same_client_order_id(self) -> None:
        self.fake_client.submit_exception = KISTimeoutError(
            'KIS request timed out for /uapi/domestic-stock/v1/trading/order-cash',
            retryable=True,
            ambiguous_submission=True,
        )

        with self.assertRaises(TradingOrderPendingSubmissionError):
            await self._submit_buy_order()

        retry_result = await self._submit_buy_order()
        request_row = load_order_request(account_reference=self.account_reference, client_order_id=self.client_order_id)
        order_row = load_broker_order(account_reference=self.account_reference, client_order_id=self.client_order_id)

        self.assertEqual(self.fake_client.submit_calls, 1)
        self.assertTrue(retry_result['idempotent_replay'])
        self.assertEqual(request_row['submission_status'], 'submitting')
        self.assertEqual(order_row['current_status'], 'pending_submit')

    async def test_submit_replay_is_atomic_while_first_request_is_in_flight(self) -> None:
        self.fake_client.resume_submit.clear()

        first_task = asyncio.create_task(self._submit_buy_order())
        await self.fake_client.submit_started.wait()

        replay_result = await self._submit_buy_order()

        self.assertEqual(self.fake_client.submit_calls, 1)
        self.assertTrue(replay_result['idempotent_replay'])
        self.assertFalse(replay_result['created'])
        self.assertEqual(replay_result['request']['submission_status'], 'submitting')
        self.assertEqual(replay_result['order']['current_status'], 'pending_submit')

        self.fake_client.resume_submit.set()
        first_result = await first_task

        self.assertTrue(first_result['created'])
        self.assertFalse(first_result['idempotent_replay'])
        self.assertEqual(self.fake_client.submit_calls, 1)

    async def test_submit_rejects_mismatched_account_reference_scope(self) -> None:
        mismatched_account_reference = f'KIS:{self.account_number}-99'

        with self.assertRaises(TradingOrderValidationError):
            await self.service.submit_buy_order(
                client_order_id=self.client_order_id,
                account_reference=mismatched_account_reference,
                account_number=self.account_number,
                account_product_code=self.account_product_code,
                symbol=self.symbol,
                quantity=Decimal('10'),
                price=Decimal('70000'),
                order_type='limit',
                time_in_force='day',
                source='api',
            )

        self.assertEqual(self.fake_client.submit_calls, 0)
        self.assertIsNone(
            load_order_request(account_reference=mismatched_account_reference, client_order_id=self.client_order_id)
        )

    async def test_get_order_refresh_maps_partial_fill_from_broker_inquiry(self) -> None:
        await self._submit_buy_order()
        self.fake_client.daily_fill_result = {
            'output1': [
                {
                    'odno': '0000123456',
                    'pdno': self.symbol,
                    'ord_qty': '10',
                    'ord_unpr': '70000',
                    'tot_ccld_qty': '4',
                    'rmn_qty': '6',
                    'avg_prvs': '70100',
                    'cncl_yn': 'N',
                    'rjct_qty': '0',
                }
            ],
            'output2': {'tot_ord_qty': '10'},
            'raw_pages': [],
            'page_count': 1,
            'max_pages_reached': False,
        }
        self.fake_client.cancelable_result = {
            'output': [
                {
                    'odno': '0000123456',
                    'pdno': self.symbol,
                    'psbl_qty': '6',
                    'ord_dvsn_cd': '00',
                }
            ],
            'raw_pages': [],
            'page_count': 1,
            'max_pages_reached': False,
        }

        result = await self.service.get_order(
            account_reference=self.account_reference,
            client_order_id=self.client_order_id,
            refresh=True,
        )

        self.assertEqual(self.fake_client.daily_fill_calls, 1)
        self.assertEqual(self.fake_client.cancelable_calls, 1)
        self.assertEqual(result['order']['current_status'], 'partial_fill')
        self.assertEqual(result['order']['filled_quantity'], Decimal('4'))
        self.assertEqual(result['order']['remaining_quantity'], Decimal('6'))
        self.assertEqual(result['events'][-1]['event_type'], 'partial_fill')

    async def test_manual_reconcile_repairs_missed_notice_fill_state(self) -> None:
        await self._submit_buy_order()
        self.fake_client.daily_fill_result = {
            'output1': [
                {
                    'odno': '0000123456',
                    'pdno': self.symbol,
                    'ord_qty': '10',
                    'ord_unpr': '70000',
                    'tot_ccld_qty': '10',
                    'rmn_qty': '0',
                    'avg_prvs': '70120',
                    'cncl_yn': 'N',
                    'rjct_qty': '0',
                }
            ],
            'output2': {'tot_ord_qty': '10'},
            'raw_pages': [],
            'page_count': 1,
            'max_pages_reached': False,
        }
        self.fake_client.cancelable_result = {
            'output': [],
            'raw_pages': [],
            'page_count': 1,
            'max_pages_reached': False,
        }

        result = await self.service.reconcile_order(
            account_reference=self.account_reference,
            client_order_id=self.client_order_id,
            trigger='manual',
            reason='missed_notice_recovery',
        )

        self.assertEqual(result['order']['current_status'], 'filled')
        self.assertEqual(result['order']['filled_quantity'], Decimal('10'))
        self.assertEqual(result['order']['remaining_quantity'], Decimal('0'))
        self.assertEqual(result['order']['average_fill_price'], Decimal('70120'))
        self.assertEqual(result['order']['last_reconcile_status'], 'repaired')
        self.assertIn('status', result['reconcile']['mismatch_reasons'])
        self.assertTrue(result['reconcile']['repair_applied'])
        self.assertEqual(result['events'][-1]['event_type'], 'fill')
        self.assertEqual(result['events'][-1]['source'], 'poll_reconcile')

    async def test_repeated_manual_reconcile_is_idempotent_for_same_broker_truth(self) -> None:
        await self._submit_buy_order()
        self.fake_client.daily_fill_result = {
            'output1': [
                {
                    'odno': '0000123456',
                    'pdno': self.symbol,
                    'ord_qty': '10',
                    'ord_unpr': '70000',
                    'tot_ccld_qty': '10',
                    'rmn_qty': '0',
                    'avg_prvs': '70120',
                    'cncl_yn': 'N',
                    'rjct_qty': '0',
                }
            ],
            'output2': {'tot_ord_qty': '10'},
            'raw_pages': [],
            'page_count': 1,
            'max_pages_reached': False,
        }
        self.fake_client.cancelable_result = {
            'output': [],
            'raw_pages': [],
            'page_count': 1,
            'max_pages_reached': False,
        }

        first = await self.service.reconcile_order(
            account_reference=self.account_reference,
            client_order_id=self.client_order_id,
            trigger='manual',
            reason='first_pass',
        )
        second = await self.service.reconcile_order(
            account_reference=self.account_reference,
            client_order_id=self.client_order_id,
            trigger='manual',
            reason='second_pass',
        )
        events = list_broker_order_events(
            account_reference=self.account_reference,
            client_order_id=self.client_order_id,
        )

        self.assertEqual(first['order']['current_status'], 'filled')
        self.assertEqual(second['order']['current_status'], 'filled')
        self.assertTrue(first['reconcile']['event_appended'])
        self.assertFalse(second['reconcile']['event_appended'])
        self.assertEqual(second['order']['last_reconcile_status'], 'matched')
        self.assertEqual(
            len([item for item in events if item['event_type'] == 'fill' and item['source'] == 'poll_reconcile']),
            1,
        )

    async def test_cancel_order_sets_cancel_pending_and_persists_request_events(self) -> None:
        await self._submit_buy_order()
        self.fake_client.cancelable_result = {
            'output': [
                {
                    'odno': '0000123456',
                    'pdno': self.symbol,
                    'psbl_qty': '10',
                    'ord_dvsn_cd': '00',
                }
            ],
            'raw_pages': [],
            'page_count': 1,
            'max_pages_reached': False,
        }

        result = await self.service.cancel_order(
            account_reference=self.account_reference,
            client_order_id=self.client_order_id,
            all_remaining=True,
        )

        self.assertEqual(self.fake_client.cancel_replace_calls, 1)
        self.assertEqual(result['order']['current_status'], 'cancel_pending')
        self.assertEqual([item['event_type'] for item in result['events']][-2:], ['cancel_request', 'cancel_request'])
        self.assertEqual([item['source'] for item in result['events']][-2:], ['local_request', 'rest_submit'])


if __name__ == '__main__':
    unittest.main()
