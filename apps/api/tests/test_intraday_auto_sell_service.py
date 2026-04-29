from datetime import date, datetime, timezone
from decimal import Decimal
import unittest
from uuid import uuid4

from sqlalchemy import delete, select

from app.core.settings import settings
from app.db import engine, session_scope
from app.db_models import BrokerOrder, OrderRequest, SignalDeciderDecisionSnapshot, SignalDeciderExecutionPlanItem
from app.market.clock import MarketState
from app.orders.intraday_auto_sell import (
    EXECUTION_STATUS_SUBMITTED,
    SignalDeciderIntradayAutoSellService,
)
from app.orders.next_open_execution import (
    ELIGIBILITY_ELIGIBLE,
    EXECUTION_STATUS_SUBMITTED as NEXT_OPEN_EXECUTION_STATUS_SUBMITTED,
    EXECUTION_WINDOW_NEXT_OPEN,
)
from app.orders.service import TradingOrderRejectedError
from app.orders.store import upsert_broker_order, upsert_order_request


class FakeAccountInquiryService:
    def __init__(self) -> None:
        self.results: dict[str, dict] = {}
        self.exceptions: dict[str, Exception] = {}
        self.calls: list[dict] = []

    async def get_positions(self, **kwargs):
        self.calls.append(kwargs)
        account_reference = kwargs['account_reference']
        if account_reference in self.exceptions:
            raise self.exceptions[account_reference]
        return self.results[account_reference]


class FakeQuoteService:
    def __init__(self) -> None:
        self.results: dict[str, dict] = {}
        self.calls: list[str] = []

    async def get_quote(self, symbol: str) -> dict:
        self.calls.append(symbol)
        return self.results[symbol]


class FakeIntradaySellOrderService:
    def __init__(self) -> None:
        self.calls: list[dict] = []
        self.mode = 'success'

    async def submit_sell_order(self, **kwargs):
        self.calls.append(kwargs)
        if self.mode == 'reject':
            request_row = upsert_order_request(
                {
                    'broker_name': 'kis',
                    'client_order_id': kwargs['client_order_id'],
                    'source': kwargs['source'],
                    'account_reference': kwargs['account_reference'],
                    'symbol': kwargs['symbol'],
                    'side': 'sell',
                    'order_type': kwargs['order_type'],
                    'requested_quantity': kwargs['quantity'],
                    'requested_price': None,
                    'time_in_force': kwargs['time_in_force'],
                    'strategy_context_payload': kwargs['strategy_context_payload'],
                    'request_payload': {'from': 'fake-intraday-auto-sell-service'},
                    'validation_status': 'valid',
                    'submission_status': 'rejected',
                    'submitted_at': '2026-04-30T00:10:00+00:00',
                }
            )
            order_row = upsert_broker_order(
                {
                    'order_request_id': request_row['id'],
                    'broker_name': 'kis',
                    'broker_order_id': f"REJECT-{kwargs['symbol']}",
                    'client_order_id': kwargs['client_order_id'],
                    'account_reference': kwargs['account_reference'],
                    'symbol': kwargs['symbol'],
                    'side': 'sell',
                    'order_type': kwargs['order_type'],
                    'requested_quantity': kwargs['quantity'],
                    'requested_price': None,
                    'accepted_quantity': kwargs['quantity'],
                    'accepted_price': None,
                    'filled_quantity': '0',
                    'remaining_quantity': kwargs['quantity'],
                    'time_in_force': kwargs['time_in_force'],
                    'current_status': 'rejected',
                    'first_submitted_at': '2026-04-30T00:10:00+00:00',
                    'last_broker_update_at': '2026-04-30T00:10:00+00:00',
                    'raw_latest_payload': {'from': 'fake-intraday-auto-sell-service'},
                    'snapshot_payload': {'status': 'rejected'},
                }
            )
            raise TradingOrderRejectedError(
                'sell rejected',
                payload={
                    'request': request_row,
                    'order': order_row,
                },
            )

        request_row = upsert_order_request(
            {
                'broker_name': 'kis',
                'client_order_id': kwargs['client_order_id'],
                'source': kwargs['source'],
                'account_reference': kwargs['account_reference'],
                'symbol': kwargs['symbol'],
                'side': 'sell',
                'order_type': kwargs['order_type'],
                'requested_quantity': kwargs['quantity'],
                'requested_price': None,
                'time_in_force': kwargs['time_in_force'],
                'strategy_context_payload': kwargs['strategy_context_payload'],
                'request_payload': {'from': 'fake-intraday-auto-sell-service'},
                'validation_status': 'valid',
                'submission_status': 'submitted',
                'submitted_at': '2026-04-30T00:10:00+00:00',
            }
        )
        order_row = upsert_broker_order(
            {
                'order_request_id': request_row['id'],
                'broker_name': 'kis',
                'broker_order_id': f"SELL-{kwargs['symbol']}-{len(self.calls)}",
                'client_order_id': kwargs['client_order_id'],
                'account_reference': kwargs['account_reference'],
                'symbol': kwargs['symbol'],
                'side': 'sell',
                'order_type': kwargs['order_type'],
                'requested_quantity': kwargs['quantity'],
                'requested_price': None,
                'accepted_quantity': kwargs['quantity'],
                'accepted_price': None,
                'filled_quantity': '0',
                'remaining_quantity': kwargs['quantity'],
                'time_in_force': kwargs['time_in_force'],
                'current_status': 'submitted',
                'first_submitted_at': '2026-04-30T00:10:00+00:00',
                'last_broker_update_at': '2026-04-30T00:10:00+00:00',
                'raw_latest_payload': {'from': 'fake-intraday-auto-sell-service'},
                'snapshot_payload': {'status': 'submitted'},
            }
        )
        return {
            'created': True,
            'idempotent_replay': False,
            'request': request_row,
            'order': order_row,
            'events': [],
        }


class IntradayAutoSellServiceTests(unittest.IsolatedAsyncioTestCase):
    @classmethod
    def setUpClass(cls) -> None:
        OrderRequest.__table__.create(bind=engine, checkfirst=True)
        BrokerOrder.__table__.create(bind=engine, checkfirst=True)
        SignalDeciderDecisionSnapshot.__table__.create(bind=engine, checkfirst=True)
        SignalDeciderExecutionPlanItem.__table__.create(bind=engine, checkfirst=True)

    def setUp(self) -> None:
        suffix = uuid4().hex[:8].upper()
        self.account_number = f'12{suffix[:6]}'
        self.account_reference = f'KIS:{self.account_number}-01'
        self.symbol = '005930'
        self.market_date = date(2026, 4, 29)
        self.target_trade_date = date(2026, 4, 30)
        self.original_settings = {
            'signal_decider_intraday_auto_sell_enabled': settings.signal_decider_intraday_auto_sell_enabled,
            'signal_decider_intraday_auto_sell_dry_run': settings.signal_decider_intraday_auto_sell_dry_run,
            'signal_decider_intraday_auto_sell_cooldown_seconds': settings.signal_decider_intraday_auto_sell_cooldown_seconds,
        }
        settings.signal_decider_intraday_auto_sell_enabled = True
        settings.signal_decider_intraday_auto_sell_dry_run = False
        settings.signal_decider_intraday_auto_sell_cooldown_seconds = 120
        self.account_inquiry_service = FakeAccountInquiryService()
        self.quote_service = FakeQuoteService()
        self.order_service = FakeIntradaySellOrderService()

    def tearDown(self) -> None:
        for key, value in self.original_settings.items():
            setattr(settings, key, value)
        with session_scope() as session:
            session.execute(delete(SignalDeciderExecutionPlanItem).where(SignalDeciderExecutionPlanItem.account_reference == self.account_reference))
            session.execute(delete(BrokerOrder).where(BrokerOrder.account_reference == self.account_reference))
            session.execute(delete(OrderRequest).where(OrderRequest.account_reference == self.account_reference))
            session.execute(delete(SignalDeciderDecisionSnapshot).where(SignalDeciderDecisionSnapshot.symbol == self.symbol))

    def _market_state(self) -> MarketState:
        return MarketState(
            market='KRX',
            state='OPEN',
            is_trading_day=True,
            is_open=True,
            server_time='2026-04-30T10:15:00+09:00',
            session_open_at='2026-04-30T09:00:00+09:00',
            session_close_at='2026-04-30T15:30:00+09:00',
        )

    def _service(self) -> SignalDeciderIntradayAutoSellService:
        return SignalDeciderIntradayAutoSellService(
            order_service=self.order_service,
            account_inquiry_service=self.account_inquiry_service,
            quote_service=self.quote_service,
            market_state_getter=self._market_state,
        )

    def _seed_snapshot(self, *, target_price: int = 54000, stop_loss_price: int = 47000) -> int:
        with session_scope() as session:
            row = SignalDeciderDecisionSnapshot(
                feedback_id=None,
                market_date=self.market_date,
                symbol=self.symbol,
                agent_name='signal-decider',
                agent_version='test-agent',
                decision_run_id='run-1',
                feedback_received_at=datetime(2026, 4, 29, 6, 0, tzinfo=timezone.utc),
                policy_version='signal_decider_policy_test',
                decision_action='buy',
                preferred_action='buy',
                next_day_open_action='buy',
                entry_price_basis='next_open',
                target_price=target_price,
                expected_return_pct=4.5,
                stop_loss_price=stop_loss_price,
                suggested_holding_period='3-10 trading days',
                confidence_score_10=7.5,
                conviction_score_10=6.0,
                thesis_summary='Test snapshot',
                supporting_evidence_payload=['Breadth improved.'],
                decision_risks_payload=['Leadership narrow.'],
                invalidation_conditions_payload=['Close below support.'],
                data_quality_warnings_payload=[],
                specialist_disagreement_payload=[],
                traceability_payload={'gate_checks': {'missing_upstream_roles': []}},
                input_feature_snapshot_payload={'technical': {'trend_state': 'uptrend'}},
                request_payload={'signal_decider_policy': {'current_version': 'signal_decider_policy_test'}},
                response_payload={'next_day_open_action': 'buy'},
                response_summary_payload={'policy_version': 'signal_decider_policy_test'},
            )
            session.add(row)
            session.flush()
            return row.id

    def _seed_next_open_entry(self, *, filled_quantity: str = '5', planned_quantity: str = '5') -> int:
        snapshot_id = self._seed_snapshot()
        buy_client_order_id = f'BUY-{uuid4().hex[:10].upper()}'
        request_row = upsert_order_request(
            {
                'broker_name': 'kis',
                'client_order_id': buy_client_order_id,
                'source': 'signal_decider_next_open',
                'account_reference': self.account_reference,
                'symbol': self.symbol,
                'side': 'buy',
                'order_type': 'market',
                'requested_quantity': planned_quantity,
                'requested_price': None,
                'time_in_force': 'day',
                'strategy_context_payload': {
                    'signal_decider_next_open': {
                        'target_trade_date': self.target_trade_date.isoformat(),
                    }
                },
                'request_payload': {'from': 'seed-next-open-entry'},
                'validation_status': 'valid',
                'submission_status': 'submitted',
                'submitted_at': '2026-04-30T00:00:05+00:00',
            }
        )
        order_row = upsert_broker_order(
            {
                'order_request_id': request_row['id'],
                'broker_name': 'kis',
                'broker_order_id': f'B-{buy_client_order_id}',
                'client_order_id': buy_client_order_id,
                'account_reference': self.account_reference,
                'symbol': self.symbol,
                'side': 'buy',
                'order_type': 'market',
                'requested_quantity': planned_quantity,
                'requested_price': None,
                'accepted_quantity': planned_quantity,
                'accepted_price': None,
                'filled_quantity': filled_quantity,
                'remaining_quantity': '0',
                'time_in_force': 'day',
                'current_status': 'filled',
                'first_submitted_at': '2026-04-30T00:00:05+00:00',
                'last_broker_update_at': '2026-04-30T00:01:00+00:00',
                'raw_latest_payload': {'from': 'seed-next-open-entry'},
                'snapshot_payload': {'status': 'filled'},
            }
        )
        with session_scope() as session:
            row = SignalDeciderExecutionPlanItem(
                execution_window=EXECUTION_WINDOW_NEXT_OPEN,
                decision_snapshot_id=snapshot_id,
                order_request_id=request_row['id'],
                broker_order_row_id=order_row['id'],
                broker_name='kis',
                target_trade_date=self.target_trade_date,
                source_market_date=self.market_date,
                account_reference=self.account_reference,
                account_label='Main',
                symbol=self.symbol,
                policy_version='signal_decider_policy_test',
                planned_side='buy',
                planned_order_type='market',
                planned_time_in_force='day',
                allocation_budget_krw=Decimal('1500000'),
                estimated_reference_price=50000,
                planned_quantity=Decimal(planned_quantity),
                estimated_notional_krw=Decimal('250000'),
                eligibility_status=ELIGIBILITY_ELIGIBLE,
                execution_status=NEXT_OPEN_EXECUTION_STATUS_SUBMITTED,
                client_order_id=buy_client_order_id,
                validation_payload={'blocking_reasons': [], 'warnings': []},
                traceability_payload={
                    'decision_snapshot_id': snapshot_id,
                    'target_price': 54000,
                    'stop_loss_price': 47000,
                    'suggested_holding_period': '3-10 trading days',
                },
                submission_payload={'mode': 'live'},
                first_ready_at=datetime(2026, 4, 30, 0, 0, 1, tzinfo=timezone.utc),
                last_evaluated_at=datetime(2026, 4, 30, 0, 0, 1, tzinfo=timezone.utc),
                submitted_at=datetime(2026, 4, 30, 0, 0, 5, tzinfo=timezone.utc),
            )
            session.add(row)
            session.flush()
            return row.id

    def _set_live_position(self, *, quantity: str, sellable_quantity: str) -> None:
        self.account_inquiry_service.results[self.account_reference] = {
            'account_reference': self.account_reference,
            'items': [
                {
                    'symbol': self.symbol,
                    'quantity': quantity,
                    'sellable_quantity': sellable_quantity,
                    'average_price': 50000,
                    'market_value': 275000,
                }
            ],
        }

    def _set_live_quote(self, *, price: int) -> None:
        self.quote_service.results[self.symbol] = {
            'symbol': self.symbol,
            'source': 'kis',
            'available': True,
            'is_live': True,
            'price': price,
            'as_of': '101500',
            'cached_at': '2026-04-30T10:15:00+09:00',
            'fallback_reason': None,
            'unavailable_reason': None,
        }

    async def test_refresh_watch_items_creates_diagnostics_row_before_trigger(self) -> None:
        entry_plan_item_id = self._seed_next_open_entry()
        self._set_live_position(quantity='5', sellable_quantity='5')
        self._set_live_quote(price=51000)

        result = await self._service().refresh_watch_items(trigger='test_refresh')

        self.assertEqual(result['summary']['items_total'], 1)
        self.assertEqual(result['summary']['ready_count'], 0)
        item = result['items'][0]
        self.assertEqual(item['traceability']['linked_next_open_plan_item_id'], entry_plan_item_id)
        self.assertEqual(item['validation']['blocking_reasons'][0]['code'], 'trigger_not_hit')

    async def test_run_intraday_auto_sell_submits_once_when_target_hits(self) -> None:
        self._seed_next_open_entry(filled_quantity='5', planned_quantity='5')
        self._set_live_position(quantity='5', sellable_quantity='5')
        self._set_live_quote(price=55000)

        service = self._service()
        first = await service.run_intraday_auto_sell_if_due(force=True, trigger='test')
        second = await service.run_intraday_auto_sell_if_due(force=True, trigger='test')

        self.assertEqual(first['summary']['selected_count'], 1)
        self.assertEqual(first['summary']['submitted_count'], 1)
        self.assertEqual(len(self.order_service.calls), 1)
        call = self.order_service.calls[0]
        self.assertEqual(call['source'], 'signal_decider_auto_sell')
        self.assertEqual(call['quantity'], Decimal('5'))
        self.assertEqual(call['strategy_context_payload']['signal_decider_intraday_auto_sell']['trigger_rule'], 'target_hit')
        self.assertEqual(first['items'][0]['execution_status'], EXECUTION_STATUS_SUBMITTED)
        self.assertEqual(second['summary']['selected_count'], 0)
        self.assertEqual(len(self.order_service.calls), 1)

    async def test_retry_uses_new_client_order_id_after_reject_and_cooldown(self) -> None:
        self._seed_next_open_entry(filled_quantity='5', planned_quantity='5')
        self._set_live_position(quantity='5', sellable_quantity='5')
        self._set_live_quote(price=46000)
        self.order_service.mode = 'reject'

        service = self._service()
        first = await service.run_intraday_auto_sell_if_due(force=True, trigger='test')
        second = await service.run_intraday_auto_sell_if_due(force=True, trigger='test')

        self.assertEqual(first['summary']['failed_count'], 1)
        self.assertEqual(second['summary']['selected_count'], 0)
        self.assertEqual(len(self.order_service.calls), 1)

        with session_scope() as session:
            watch = session.execute(
                select(SignalDeciderExecutionPlanItem).where(
                    SignalDeciderExecutionPlanItem.execution_window == 'intraday_auto_sell',
                    SignalDeciderExecutionPlanItem.account_reference == self.account_reference,
                )
            ).scalar_one()
            rejected_order = session.execute(
                select(BrokerOrder).where(
                    BrokerOrder.account_reference == self.account_reference,
                    BrokerOrder.client_order_id == watch.client_order_id,
                )
            ).scalar_one()
            rejected_order.last_broker_update_at = datetime(2026, 4, 29, 0, 0, tzinfo=timezone.utc)
            latest = (watch.submission_payload or {}).get('latest') or {}
            attempts = (watch.submission_payload or {}).get('attempts') or []
            latest['executed_at'] = '2026-04-29T00:00:00+00:00'
            for attempt in attempts:
                attempt['executed_at'] = '2026-04-29T00:00:00+00:00'
            watch.submission_payload = {
                **(watch.submission_payload or {}),
                'latest': latest,
                'attempts': attempts,
            }

        self.order_service.mode = 'success'
        third = await service.run_intraday_auto_sell_if_due(force=True, trigger='test')

        self.assertEqual(third['summary']['selected_count'], 1)
        self.assertEqual(third['summary']['submitted_count'], 1)
        self.assertEqual(len(self.order_service.calls), 2)
        self.assertTrue(self.order_service.calls[1]['client_order_id'].endswith('-A2'))

    async def test_auto_sell_quantity_is_capped_to_linked_entry_fill(self) -> None:
        self._seed_next_open_entry(filled_quantity='3', planned_quantity='3')
        self._set_live_position(quantity='10', sellable_quantity='10')
        self._set_live_quote(price=55000)

        result = await self._service().run_intraday_auto_sell_if_due(force=True, trigger='test')

        self.assertEqual(result['summary']['submitted_count'], 1)
        self.assertEqual(self.order_service.calls[0]['quantity'], Decimal('3'))


if __name__ == '__main__':
    unittest.main()
