from copy import deepcopy
from datetime import date, datetime, timezone
from decimal import Decimal
from types import SimpleNamespace
import unittest
from uuid import uuid4

from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy import delete, select

from app.api import routes_agent_context
from app.db import engine, session_scope
from app.db_models import (
    BrokerOrder,
    BrokerOrderEvent,
    OrderRequest,
    SignalDeciderActualExecutionOutcome,
    SignalDeciderDecisionSnapshot,
    SignalDeciderExecutionPlanItem,
)
from app.orders.exec_payload_slimming import compact_exec_readback
from app.orders.signal_decider_execution_outcomes import signal_decider_execution_outcome_service
from app.orders.store import append_broker_order_event, upsert_broker_order, upsert_order_request


class SignalDeciderExecutionOutcomeServiceTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        OrderRequest.__table__.create(bind=engine, checkfirst=True)
        BrokerOrder.__table__.create(bind=engine, checkfirst=True)
        BrokerOrderEvent.__table__.create(bind=engine, checkfirst=True)
        SignalDeciderDecisionSnapshot.__table__.create(bind=engine, checkfirst=True)
        SignalDeciderExecutionPlanItem.__table__.create(bind=engine, checkfirst=True)
        SignalDeciderActualExecutionOutcome.__table__.create(bind=engine, checkfirst=True)

    def setUp(self) -> None:
        suffix = uuid4().hex[:8].upper()
        self.account_reference = f'KIS:34{suffix[:6]}-01'
        self.symbol = '005930'
        self.market_date = date(2026, 4, 29)
        self.target_trade_date = date(2026, 4, 30)
        self.policy_version = f'signal_decider_policy_exec_{suffix.lower()}'

    def tearDown(self) -> None:
        with session_scope() as session:
            session.execute(
                delete(SignalDeciderActualExecutionOutcome).where(
                    SignalDeciderActualExecutionOutcome.account_reference == self.account_reference
                )
            )
            session.execute(
                delete(SignalDeciderExecutionPlanItem).where(
                    SignalDeciderExecutionPlanItem.account_reference == self.account_reference
                )
            )
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
            session.execute(
                delete(SignalDeciderDecisionSnapshot).where(
                    SignalDeciderDecisionSnapshot.policy_version == self.policy_version
                )
            )

    def _seed_snapshot(self, *, agent_name: str = 'signal-decider') -> int:
        with session_scope() as session:
            row = SignalDeciderDecisionSnapshot(
                feedback_id=None,
                market_date=self.market_date,
                symbol=self.symbol,
                agent_name=agent_name,
                agent_version='test-agent',
                decision_run_id=f'run-{self.symbol}',
                feedback_received_at=datetime(2026, 4, 29, 6, 0, tzinfo=timezone.utc),
                policy_version=self.policy_version,
                decision_action='buy',
                preferred_action='buy',
                next_day_open_action='buy',
                entry_price_basis='next_open',
                target_price=54000,
                expected_return_pct=4.8,
                stop_loss_price=47000,
                suggested_holding_period='3-10 trading days',
                confidence_score_10=7.2,
                conviction_score_10=6.4,
                thesis_summary='Execution test snapshot',
                supporting_evidence_payload=['Breadth improved.'],
                decision_risks_payload=['Leadership remained narrow.'],
                invalidation_conditions_payload=['Close below support.'],
                data_quality_warnings_payload=[],
                specialist_disagreement_payload=[],
                traceability_payload={'schema_version': 'signal_decider_trace_v2', 'gate_checks': {'missing_upstream_roles': []}},
                input_feature_snapshot_payload={'technical': {'trend_state': 'uptrend'}},
                request_payload={'signal_decider_policy': {'current_version': self.policy_version}},
                response_payload={'next_day_open_action': 'buy'},
                response_summary_payload={'policy_version': self.policy_version},
            )
            session.add(row)
            session.flush()
            return row.id

    def _seed_order(
        self,
        *,
        client_order_id: str,
        side: str,
        source: str,
        quantity: str,
        status: str,
        average_fill_price: str | None = None,
        filled_quantity: str = '0',
        submitted_at: str,
        last_fill_at: str | None = None,
        strategy_context_payload: dict | None = None,
        broker_order_id: str | None = None,
    ) -> tuple[dict, dict]:
        request_row = upsert_order_request(
            {
                'broker_name': 'kis',
                'client_order_id': client_order_id,
                'source': source,
                'account_reference': self.account_reference,
                'symbol': self.symbol,
                'side': side,
                'order_type': 'market',
                'requested_quantity': quantity,
                'requested_price': None,
                'time_in_force': 'day',
                'strategy_context_payload': strategy_context_payload,
                'request_payload': {'seeded': True, 'side': side},
                'validation_status': 'valid',
                'submission_status': 'rejected' if status == 'rejected' else 'submitted',
                'submitted_at': submitted_at,
            }
        )
        filled_decimal = Decimal(filled_quantity)
        requested_decimal = Decimal(quantity)
        order_row = upsert_broker_order(
            {
                'order_request_id': request_row['id'],
                'broker_name': 'kis',
                'broker_order_id': broker_order_id or f'{client_order_id}-BROKER',
                'client_order_id': client_order_id,
                'account_reference': self.account_reference,
                'symbol': self.symbol,
                'side': side,
                'order_type': 'market',
                'requested_quantity': quantity,
                'requested_price': None,
                'accepted_quantity': quantity,
                'accepted_price': None,
                'filled_quantity': filled_quantity,
                'remaining_quantity': str(max(requested_decimal - filled_decimal, Decimal('0'))),
                'average_fill_price': average_fill_price,
                'time_in_force': 'day',
                'current_status': status,
                'first_submitted_at': submitted_at,
                'last_broker_update_at': last_fill_at or submitted_at,
                'last_fill_at': last_fill_at,
                'raw_latest_payload': {'seeded': True, 'status': status},
                'snapshot_payload': {'status': status},
            }
        )
        if filled_decimal > Decimal('0') and average_fill_price is not None and last_fill_at is not None:
            append_broker_order_event(
                {
                    'broker_name': 'kis',
                    'account_reference': self.account_reference,
                    'client_order_id': client_order_id,
                    'broker_order_id': order_row['broker_order_id'],
                    'symbol': self.symbol,
                    'event_type': 'fill' if status == 'filled' else 'partial_fill',
                    'event_occurred_at': last_fill_at,
                    'quantity_delta': filled_quantity,
                    'cumulative_filled_quantity': filled_quantity,
                    'remaining_quantity': str(max(requested_decimal - filled_decimal, Decimal('0'))),
                    'fill_price': average_fill_price,
                    'average_fill_price': average_fill_price,
                    'resulting_status': status,
                    'source': 'ws_notice',
                    'event_dedup_key': f'fill:{client_order_id}:{filled_quantity}:{last_fill_at}',
                    'raw_payload': {'seeded': True},
                    'normalized_payload': {'status': status},
                }
            )
        return request_row, order_row

    def _seed_plan_item(
        self,
        *,
        execution_window: str,
        snapshot_id: int,
        planned_side: str,
        client_order_id: str | None,
        order_request_id: int | None,
        broker_order_row_id: int | None,
        execution_status: str,
        decision_agent_name: str = 'signal-decider',
        validation_payload: dict | None = None,
        traceability_payload: dict | None = None,
        submission_payload: dict | None = None,
    ) -> int:
        with session_scope() as session:
            row = SignalDeciderExecutionPlanItem(
                execution_window=execution_window,
                decision_agent_name=decision_agent_name,
                decision_snapshot_id=snapshot_id,
                order_request_id=order_request_id,
                broker_order_row_id=broker_order_row_id,
                broker_name='kis',
                target_trade_date=self.target_trade_date,
                source_market_date=self.market_date,
                account_reference=self.account_reference,
                account_label='Main',
                symbol=self.symbol,
                policy_version=self.policy_version,
                planned_side=planned_side,
                planned_order_type='market',
                planned_time_in_force='day',
                allocation_budget_krw=Decimal('250000'),
                estimated_reference_price=50000,
                planned_quantity=Decimal('5'),
                estimated_notional_krw=Decimal('250000'),
                eligibility_status='eligible',
                execution_status=execution_status,
                client_order_id=client_order_id,
                validation_payload=validation_payload,
                traceability_payload=traceability_payload or {},
                submission_payload=submission_payload,
                first_ready_at=datetime(2026, 4, 29, 23, 0, tzinfo=timezone.utc),
                last_evaluated_at=datetime(2026, 4, 29, 23, 0, tzinfo=timezone.utc),
                submitted_at=datetime(2026, 4, 30, 0, 1, tzinfo=timezone.utc) if order_request_id is not None else None,
            )
            session.add(row)
            session.flush()
            return row.id

    def _attach_closed_exec_readback_submission(self, *, entry_plan_item_id: int) -> None:
        with session_scope() as session:
            row = session.get(SignalDeciderExecutionPlanItem, entry_plan_item_id)
            self.assertIsNotNone(row)
            row.submission_payload = {
                'mode': 'exec_dispatch',
                'exec_dispatch': {
                    'plan_id': f'exec-plan-{entry_plan_item_id}',
                    'dispatch_status': 'accepted',
                    'item_links': [
                        {
                            'core_item_key': f'core-plan-{entry_plan_item_id}:next_open_buy',
                            'intent': 'next_open_buy',
                            'side': 'buy',
                            'symbol': self.symbol,
                            'exec_plan_item_id': 'exec-entry-item-1',
                            'remote_status': 'filled',
                        },
                        {
                            'core_item_key': f'core-plan-{entry_plan_item_id}:target_sell',
                            'intent': 'target_sell',
                            'side': 'sell',
                            'symbol': self.symbol,
                            'exec_plan_item_id': 'exec-target-item-1',
                            'remote_status': 'filled',
                        },
                        {
                            'core_item_key': f'core-plan-{entry_plan_item_id}:stop_sell',
                            'intent': 'stop_sell',
                            'side': 'sell',
                            'symbol': self.symbol,
                            'exec_plan_item_id': 'exec-stop-item-1',
                            'remote_status': 'cancelled',
                        },
                    ],
                    'readback': {
                        'last_synced_at': '2026-04-30T03:06:00+00:00',
                        'runtime': {'status': 'ok', 'payload': {'mode': 'mock', 'ready': True}},
                        'orders': {'status': 'ok', 'count': 0, 'items': []},
                        'executions': {
                            'status': 'ok',
                            'count': 2,
                            'items': [
                                {
                                    'id': 'exec-buy-1',
                                    'execution_plan_item_id': 'exec-entry-item-1',
                                    'broker_order_id': 'exec-buy-order-1',
                                    'client_order_id': 'EXEC-BUY-1',
                                    'symbol': self.symbol,
                                    'side': 'buy',
                                    'quantity': '5',
                                    'price': '50000',
                                    'executed_at': '2026-04-30T00:01:30+00:00',
                                },
                                {
                                    'id': 'exec-sell-1',
                                    'execution_plan_item_id': 'exec-target-item-1',
                                    'broker_order_id': 'exec-sell-order-1',
                                    'client_order_id': 'EXEC-SELL-1',
                                    'symbol': self.symbol,
                                    'side': 'sell',
                                    'quantity': '5',
                                    'price': '54000',
                                    'executed_at': '2026-04-30T03:05:00+00:00',
                                },
                            ],
                        },
                        'positions': {'status': 'ok', 'count': 0, 'items': []},
                        'derived_state': {
                            'derived_status': 'closed',
                            'entry_filled_quantity': '5',
                            'exit_filled_quantity': '5',
                            'open_quantity': '0',
                            'closed_quantity': '5',
                            'has_plan_attributed_fill_evidence': True,
                            'endpoint_statuses': {
                                'plans': 'ok',
                                'orders': 'ok',
                                'executions': 'ok',
                                'positions': 'ok',
                            },
                        },
                    },
                },
            }
            row.submitted_at = datetime(2026, 4, 30, 0, 1, tzinfo=timezone.utc)

    def _attach_partial_exec_full_order_readback_submission(self, *, entry_plan_item_id: int) -> None:
        with session_scope() as session:
            row = session.get(SignalDeciderExecutionPlanItem, entry_plan_item_id)
            self.assertIsNotNone(row)
            row.submission_payload = {
                'mode': 'exec_dispatch',
                'exec_dispatch': {
                    'plan_id': f'exec-plan-{entry_plan_item_id}',
                    'dispatch_status': 'accepted',
                    'item_links': [
                        {
                            'core_item_key': f'core-plan-{entry_plan_item_id}:next_open_buy',
                            'intent': 'next_open_buy',
                            'side': 'buy',
                            'symbol': self.symbol,
                            'exec_plan_item_id': 'exec-entry-item-1',
                            'remote_status': 'filled',
                        },
                        {
                            'core_item_key': f'core-plan-{entry_plan_item_id}:target_sell',
                            'intent': 'target_sell',
                            'side': 'sell',
                            'symbol': self.symbol,
                            'exec_plan_item_id': 'exec-target-item-1',
                            'remote_status': 'filled',
                        },
                    ],
                    'readback': {
                        'last_synced_at': '2026-04-30T03:06:00+00:00',
                        'runtime': {'status': 'ok', 'payload': {'mode': 'mock', 'ready': True}},
                        'orders': {
                            'status': 'ok',
                            'count': 2,
                            'items': [
                                {
                                    'id': 'exec-buy-order-1',
                                    'execution_plan_item_id': 'exec-entry-item-1',
                                    'broker_order_id': 'exec-buy-order-1',
                                    'client_order_id': 'EXEC-BUY-1',
                                    'symbol': self.symbol,
                                    'side': 'buy',
                                    'status': 'filled',
                                    'filled_quantity': '5',
                                    'average_fill_price': '50000',
                                    'submitted_at': '2026-04-30T00:01:00+00:00',
                                    'last_filled_at': '2026-04-30T00:01:30+00:00',
                                    'updated_at': '2026-04-30T00:01:30+00:00',
                                },
                                {
                                    'id': 'exec-sell-order-1',
                                    'execution_plan_item_id': 'exec-target-item-1',
                                    'broker_order_id': 'exec-sell-order-1',
                                    'client_order_id': 'EXEC-SELL-1',
                                    'symbol': self.symbol,
                                    'side': 'sell',
                                    'status': 'filled',
                                    'filled_quantity': '5',
                                    'average_fill_price': '54000',
                                    'submitted_at': '2026-04-30T03:00:00+00:00',
                                    'last_filled_at': '2026-04-30T03:05:00+00:00',
                                    'updated_at': '2026-04-30T03:05:00+00:00',
                                },
                            ],
                        },
                        'executions': {
                            'status': 'ok',
                            'count': 6,
                            'items_omitted_count': 4,
                            'items': [
                                {
                                    'id': 'exec-buy-compact-1',
                                    'execution_plan_item_id': 'exec-entry-item-1',
                                    'broker_order_id': 'exec-buy-order-1',
                                    'client_order_id': 'EXEC-BUY-1',
                                    'symbol': self.symbol,
                                    'side': 'buy',
                                    'quantity': '2',
                                    'price': '50000',
                                    'executed_at': '2026-04-30T00:01:10+00:00',
                                },
                                {
                                    'id': 'exec-sell-compact-1',
                                    'execution_plan_item_id': 'exec-target-item-1',
                                    'broker_order_id': 'exec-sell-order-1',
                                    'client_order_id': 'EXEC-SELL-1',
                                    'symbol': self.symbol,
                                    'side': 'sell',
                                    'quantity': '2',
                                    'price': '54000',
                                    'executed_at': '2026-04-30T03:04:00+00:00',
                                },
                            ],
                        },
                        'positions': {'status': 'ok', 'count': 0, 'items': []},
                    },
                },
            }
            row.submitted_at = datetime(2026, 4, 30, 0, 1, tzinfo=timezone.utc)

    def test_list_outcomes_refresh_persists_closed_target_hit_trade(self) -> None:
        snapshot_id = self._seed_snapshot()
        entry_client_order_id = 'SDNO-20260430-005930'
        exit_client_order_id = 'SDIS-20260430-005930-TGT-A1'
        entry_request, entry_order = self._seed_order(
            client_order_id=entry_client_order_id,
            side='buy',
            source='signal_decider_next_open',
            quantity='5',
            status='filled',
            filled_quantity='5',
            average_fill_price='50000',
            submitted_at='2026-04-30T00:01:00+00:00',
            last_fill_at='2026-04-30T00:01:30+00:00',
            strategy_context_payload={'signal_decider_next_open': {'decision_snapshot_id': snapshot_id}},
        )
        entry_plan_item_id = self._seed_plan_item(
            execution_window='next_open',
            snapshot_id=snapshot_id,
            planned_side='buy',
            client_order_id=entry_client_order_id,
            order_request_id=entry_request['id'],
            broker_order_row_id=entry_order['id'],
            execution_status='submitted',
            traceability_payload={'target_price': 54000, 'stop_loss_price': 47000},
        )
        exit_request, exit_order = self._seed_order(
            client_order_id=exit_client_order_id,
            side='sell',
            source='signal_decider_auto_sell',
            quantity='5',
            status='filled',
            filled_quantity='5',
            average_fill_price='54000',
            submitted_at='2026-04-30T03:00:00+00:00',
            last_fill_at='2026-04-30T03:05:00+00:00',
            strategy_context_payload={
                'signal_decider_intraday_auto_sell': {
                    'decision_snapshot_id': snapshot_id,
                    'linked_next_open_plan_item_id': entry_plan_item_id,
                    'trigger_rule': 'target_hit',
                }
            },
        )
        exit_plan_item_id = self._seed_plan_item(
            execution_window='intraday_auto_sell',
            snapshot_id=snapshot_id,
            planned_side='sell',
            client_order_id=exit_client_order_id,
            order_request_id=exit_request['id'],
            broker_order_row_id=exit_order['id'],
            execution_status='submitted',
            validation_payload={'trigger': {'rule': 'target_hit'}},
            traceability_payload={'linked_next_open_plan_item_id': entry_plan_item_id},
        )

        payload = signal_decider_execution_outcome_service.list_outcomes(
            snapshot_id=snapshot_id,
            refresh=True,
        )

        self.assertEqual(payload['summary']['count'], 1)
        self.assertEqual(payload['summary']['closed_count'], 1)
        item = payload['items'][0]
        self.assertEqual(item['entry_plan_item_id'], entry_plan_item_id)
        self.assertEqual(item['exit_plan_item_id'], exit_plan_item_id)
        self.assertEqual(item['lifecycle_status'], 'closed')
        self.assertEqual(item['exit_reason'], 'target_hit')
        self.assertEqual(item['entry_filled_quantity'], '5.00000000')
        self.assertEqual(item['exit_filled_quantity'], '5.00000000')
        self.assertEqual(item['closed_quantity'], '5.00000000')
        self.assertEqual(item['open_quantity'], '0.00000000')
        self.assertEqual(item['entry_average_fill_price'], 50000)
        self.assertEqual(item['exit_average_fill_price'], 54000)
        self.assertEqual(item['realized_pnl_krw'], '20000.00')
        self.assertEqual(item['realized_return_pct'], 0.08)
        self.assertEqual(item['entry_filled_at'], '2026-04-30T00:01:30+00:00')
        self.assertEqual(item['exit_filled_at'], '2026-04-30T03:05:00+00:00')
        self.assertEqual(item['diagnostics']['exit']['trigger_rule'], 'target_hit')

        with session_scope() as session:
            row = session.execute(
                select(SignalDeciderActualExecutionOutcome).where(
                    SignalDeciderActualExecutionOutcome.entry_plan_item_id == entry_plan_item_id
                )
            ).scalar_one()
            self.assertEqual(row.exit_plan_item_id, exit_plan_item_id)
            self.assertEqual(row.exit_reason, 'target_hit')

    def test_fully_sold_partial_entry_stays_unresolved_until_entry_order_is_terminal(self) -> None:
        snapshot_id = self._seed_snapshot(agent_name='short-term-decider')
        entry_request, entry_order = self._seed_order(
            client_order_id='STDE-20260430-005930-PARTIAL',
            side='buy',
            source='signal_decider_next_open',
            quantity='10',
            status='partial_fill',
            filled_quantity='5',
            average_fill_price='50000',
            submitted_at='2026-04-30T00:01:00+00:00',
            last_fill_at='2026-04-30T00:01:30+00:00',
            strategy_context_payload={'signal_decider_next_open': {'decision_snapshot_id': snapshot_id}},
        )
        entry_plan_item_id = self._seed_plan_item(
            execution_window='next_open',
            snapshot_id=snapshot_id,
            planned_side='buy',
            client_order_id=entry_request['client_order_id'],
            order_request_id=entry_request['id'],
            broker_order_row_id=entry_order['id'],
            execution_status='submitted',
            decision_agent_name='short-term-decider',
        )
        exit_request, exit_order = self._seed_order(
            client_order_id='STDE-20260430-005930-PARTIAL-EXIT',
            side='sell',
            source='signal_decider_auto_sell',
            quantity='5',
            status='filled',
            filled_quantity='5',
            average_fill_price='54000',
            submitted_at='2026-04-30T03:00:00+00:00',
            last_fill_at='2026-04-30T03:05:00+00:00',
            strategy_context_payload={
                'signal_decider_intraday_auto_sell': {
                    'decision_snapshot_id': snapshot_id,
                    'linked_next_open_plan_item_id': entry_plan_item_id,
                    'trigger_rule': 'target_hit',
                }
            },
        )
        self._seed_plan_item(
            execution_window='intraday_auto_sell',
            snapshot_id=snapshot_id,
            planned_side='sell',
            client_order_id=exit_request['client_order_id'],
            order_request_id=exit_request['id'],
            broker_order_row_id=exit_order['id'],
            execution_status='submitted',
            decision_agent_name='short-term-decider',
            validation_payload={'trigger': {'rule': 'target_hit'}},
            traceability_payload={'linked_next_open_plan_item_id': entry_plan_item_id},
        )

        payload = signal_decider_execution_outcome_service.list_outcomes(
            snapshot_id=snapshot_id,
            refresh=True,
        )

        self.assertEqual(payload['summary']['closed_count'], 0)
        self.assertEqual(payload['summary']['unresolved_count'], 1)
        self.assertEqual(payload['summary']['lifecycle_status_counts'], {'unresolved': 1})
        item = payload['items'][0]
        self.assertEqual(item['lifecycle_status'], 'unresolved')
        self.assertEqual(item['exit_reason'], 'unresolved')
        self.assertEqual(item['closed_quantity'], '5.00000000')
        self.assertEqual(item['open_quantity'], '0.00000000')
        self.assertEqual(item['diagnostics']['entry']['order_status'], 'partial_fill')

    def test_list_outcomes_refresh_persists_closed_trade_from_exec_readback_fills(self) -> None:
        snapshot_id = self._seed_snapshot()
        entry_plan_item_id = self._seed_plan_item(
            execution_window='next_open',
            snapshot_id=snapshot_id,
            planned_side='buy',
            client_order_id='SDNO-20260430-005930-DISPATCH',
            order_request_id=None,
            broker_order_row_id=None,
            execution_status='submitted',
            traceability_payload={'target_price': 54000, 'stop_loss_price': 47000},
        )
        self._attach_closed_exec_readback_submission(entry_plan_item_id=entry_plan_item_id)

        payload = signal_decider_execution_outcome_service.list_outcomes(
            snapshot_id=snapshot_id,
            refresh=True,
        )

        self.assertEqual(payload['summary']['count'], 1)
        self.assertEqual(payload['summary']['closed_count'], 1)
        item = payload['items'][0]
        self.assertEqual(item['entry_plan_item_id'], entry_plan_item_id)
        self.assertIsNone(item['exit_plan_item_id'])
        self.assertEqual(item['lifecycle_status'], 'closed')
        self.assertEqual(item['exit_reason'], 'target_hit')
        self.assertEqual(item['entry_filled_quantity'], '5.00000000')
        self.assertEqual(item['exit_filled_quantity'], '5.00000000')
        self.assertEqual(item['closed_quantity'], '5.00000000')
        self.assertEqual(item['open_quantity'], '0.00000000')
        self.assertEqual(item['entry_average_fill_price'], 50000)
        self.assertEqual(item['exit_average_fill_price'], 54000)
        self.assertEqual(item['realized_pnl_krw'], '20000.00')
        self.assertEqual(item['realized_return_pct'], 0.08)
        self.assertEqual(item['entry_filled_at'], '2026-04-30T00:01:30+00:00')
        self.assertEqual(item['exit_filled_at'], '2026-04-30T03:05:00+00:00')
        self.assertEqual(item['diagnostics']['entry']['metric_source'], 'exec_readback')
        self.assertEqual(item['diagnostics']['exit']['metric_source'], 'exec_readback')
        self.assertEqual(item['diagnostics']['exit']['trigger_rule'], 'target_hit')
        self.assertEqual(item['diagnostics']['entry']['exec_readback']['remote_execution_ids'], ['exec-buy-1'])
        self.assertEqual(item['diagnostics']['exit']['exec_readback']['remote_execution_ids'], ['exec-sell-1'])

        with session_scope() as session:
            row = session.execute(
                select(SignalDeciderActualExecutionOutcome).where(
                    SignalDeciderActualExecutionOutcome.entry_plan_item_id == entry_plan_item_id
                )
            ).scalar_one()
            self.assertEqual(row.exit_reason, 'target_hit')
            self.assertEqual(row.realized_pnl_krw, Decimal('20000.00'))

    def test_exec_readback_stays_unresolved_while_any_entry_order_can_still_fill(self) -> None:
        snapshot_id = self._seed_snapshot()
        entry_plan_item_id = self._seed_plan_item(
            execution_window='next_open',
            snapshot_id=snapshot_id,
            planned_side='buy',
            client_order_id='SDNO-20260430-005930-MULTI-ENTRY',
            order_request_id=None,
            broker_order_row_id=None,
            execution_status='submitted',
            traceability_payload={'target_price': 54000, 'stop_loss_price': 47000},
        )
        self._attach_closed_exec_readback_submission(entry_plan_item_id=entry_plan_item_id)
        with session_scope() as session:
            row = session.get(SignalDeciderExecutionPlanItem, entry_plan_item_id)
            self.assertIsNotNone(row)
            submission_payload = deepcopy(row.submission_payload)
            exec_dispatch = submission_payload['exec_dispatch']
            exec_dispatch['item_links'][0]['remote_status'] = 'partial_fill'
            exec_dispatch['item_links'].insert(
                1,
                {
                    'core_item_key': f'core-plan-{entry_plan_item_id}:next_open_buy:2',
                    'intent': 'next_open_buy',
                    'side': 'buy',
                    'symbol': self.symbol,
                    'exec_plan_item_id': 'exec-entry-item-2',
                    'remote_status': 'filled',
                },
            )
            readback = exec_dispatch['readback']
            readback['orders'] = {
                'status': 'ok',
                'count': 3,
                'items': [
                    {
                        'id': 'exec-buy-order-partial',
                        'execution_plan_item_id': 'exec-entry-item-1',
                        'side': 'buy',
                        'status': 'partial_fill',
                        'requested_quantity': '4',
                        'filled_quantity': '2',
                        'remaining_quantity': '2',
                        'average_fill_price': '50000',
                        'submitted_at': '2026-04-30T00:01:00+00:00',
                        'updated_at': '2026-04-30T00:01:30+00:00',
                    },
                    {
                        'id': 'exec-buy-order-filled',
                        'execution_plan_item_id': 'exec-entry-item-2',
                        'side': 'buy',
                        'status': 'filled',
                        'requested_quantity': '3',
                        'filled_quantity': '3',
                        'remaining_quantity': '0',
                        'average_fill_price': '50000',
                        'submitted_at': '2026-04-30T00:02:00+00:00',
                        'updated_at': '2026-04-30T00:02:30+00:00',
                    },
                    {
                        'id': 'exec-sell-order-filled',
                        'execution_plan_item_id': 'exec-target-item-1',
                        'side': 'sell',
                        'status': 'filled',
                        'requested_quantity': '5',
                        'filled_quantity': '5',
                        'remaining_quantity': '0',
                        'average_fill_price': '54000',
                        'submitted_at': '2026-04-30T03:00:00+00:00',
                        'updated_at': '2026-04-30T03:05:00+00:00',
                    },
                ],
            }
            readback['executions'] = {
                'status': 'ok',
                'count': 3,
                'items': [
                    {
                        'id': 'exec-buy-partial',
                        'execution_plan_item_id': 'exec-entry-item-1',
                        'side': 'buy',
                        'quantity': '2',
                        'price': '50000',
                        'executed_at': '2026-04-30T00:01:30+00:00',
                    },
                    {
                        'id': 'exec-buy-filled',
                        'execution_plan_item_id': 'exec-entry-item-2',
                        'side': 'buy',
                        'quantity': '3',
                        'price': '50000',
                        'executed_at': '2026-04-30T00:02:30+00:00',
                    },
                    {
                        'id': 'exec-sell-filled',
                        'execution_plan_item_id': 'exec-target-item-1',
                        'side': 'sell',
                        'quantity': '5',
                        'price': '54000',
                        'executed_at': '2026-04-30T03:05:00+00:00',
                    },
                ],
            }
            row.submission_payload = submission_payload

        payload = signal_decider_execution_outcome_service.list_outcomes(
            snapshot_id=snapshot_id,
            refresh=True,
        )

        self.assertEqual(payload['summary']['closed_count'], 0)
        self.assertEqual(payload['summary']['unresolved_count'], 1)
        item = payload['items'][0]
        self.assertEqual(item['lifecycle_status'], 'unresolved')
        self.assertEqual(item['exit_reason'], 'unresolved')
        self.assertEqual(item['entry_filled_quantity'], '5.00000000')
        self.assertEqual(item['closed_quantity'], '5.00000000')
        self.assertEqual(item['open_quantity'], '0.00000000')
        self.assertEqual(item['diagnostics']['entry']['order_status'], 'filled')

    def test_exec_readback_stays_unresolved_with_omitted_active_entry_link(self) -> None:
        snapshot_id = self._seed_snapshot()
        entry_plan_item_id = self._seed_plan_item(
            execution_window='next_open',
            snapshot_id=snapshot_id,
            planned_side='buy',
            client_order_id='SDNO-20260430-005930-OMITTED-ENTRY',
            order_request_id=None,
            broker_order_row_id=None,
            execution_status='submitted',
            traceability_payload={'target_price': 54000, 'stop_loss_price': 47000},
        )
        self._attach_partial_exec_full_order_readback_submission(entry_plan_item_id=entry_plan_item_id)
        with session_scope() as session:
            row = session.get(SignalDeciderExecutionPlanItem, entry_plan_item_id)
            self.assertIsNotNone(row)
            submission_payload = deepcopy(row.submission_payload)
            exec_dispatch = submission_payload['exec_dispatch']
            exec_dispatch['item_links'].append(
                {
                    'core_item_key': f'core-plan-{entry_plan_item_id}:next_open_buy:omitted',
                    'intent': 'next_open_buy',
                    'side': 'buy',
                    'symbol': self.symbol,
                    'exec_plan_item_id': 'exec-entry-item-omitted',
                    'remote_status': 'partial_fill',
                }
            )
            raw_readback = deepcopy(exec_dispatch['readback'])
            raw_readback['orders']['items'].extend(
                {
                    'id': f'exec-sell-order-cancelled-{index}',
                    'side': 'sell',
                    'status': 'cancelled',
                    'quantity': '0',
                    'filled_quantity': '0',
                    'remaining_quantity': '0',
                }
                for index in range(8)
            )
            raw_readback['orders']['items'].append(
                {
                    'id': 'exec-buy-order-partial-omitted',
                    'execution_plan_item_id': 'exec-entry-item-omitted',
                    'side': 'buy',
                    'status': 'partial_fill',
                    'quantity': '2',
                    'filled_quantity': '1',
                    'remaining_quantity': '1',
                }
            )
            raw_readback['orders']['count'] = len(raw_readback['orders']['items'])
            exec_dispatch['readback'] = compact_exec_readback(raw_readback)
            self.assertEqual(exec_dispatch['readback']['orders']['items_omitted_count'], 1)
            row.submission_payload = submission_payload

        payload = signal_decider_execution_outcome_service.list_outcomes(
            snapshot_id=snapshot_id,
            refresh=True,
        )

        self.assertEqual(payload['summary']['closed_count'], 0)
        self.assertEqual(payload['summary']['unresolved_count'], 1)
        item = payload['items'][0]
        self.assertEqual(item['lifecycle_status'], 'unresolved')
        self.assertEqual(item['exit_reason'], 'unresolved')
        self.assertEqual(item['entry_filled_quantity'], '0.00000000')
        self.assertEqual(item['closed_quantity'], '0.00000000')
        self.assertEqual(item['open_quantity'], '0.00000000')
        self.assertIsNone(item['realized_pnl_krw'])
        self.assertEqual(
            item['diagnostics']['entry']['exec_readback']['status'],
            'unavailable',
        )
        self.assertEqual(
            item['diagnostics']['entry']['exec_readback']['reason_code'],
            'exec_readback_fact_sources_incomplete',
        )

        with session_scope() as session:
            row = session.get(SignalDeciderExecutionPlanItem, entry_plan_item_id)
            self.assertIsNotNone(row)
            submission_payload = deepcopy(row.submission_payload)
            submission_payload['exec_dispatch']['item_links'][-1]['remote_status'] = 'accepted'
            row.submission_payload = submission_payload

        accepted_payload = signal_decider_execution_outcome_service.list_outcomes(
            snapshot_id=snapshot_id,
            refresh=True,
        )

        self.assertEqual(accepted_payload['summary']['closed_count'], 0)
        self.assertEqual(accepted_payload['summary']['unresolved_count'], 1)
        self.assertEqual(accepted_payload['items'][0]['lifecycle_status'], 'unresolved')

    def test_list_outcomes_refresh_persists_short_term_exec_readback_fills(self) -> None:
        snapshot_id = self._seed_snapshot(agent_name='short-term-decider')
        entry_plan_item_id = self._seed_plan_item(
            execution_window='next_open',
            snapshot_id=snapshot_id,
            planned_side='buy',
            client_order_id='STDE-20260430-005930-DISPATCH',
            order_request_id=None,
            broker_order_row_id=None,
            execution_status='submitted',
            decision_agent_name='short-term-decider',
            traceability_payload={'target_price': 54000, 'stop_loss_price': 47000},
        )
        self._attach_closed_exec_readback_submission(entry_plan_item_id=entry_plan_item_id)

        payload = signal_decider_execution_outcome_service.list_outcomes(
            snapshot_id=snapshot_id,
            refresh=True,
        )

        self.assertEqual(payload['summary']['count'], 1)
        self.assertEqual(payload['summary']['closed_count'], 1)
        self.assertEqual(payload['items'][0]['entry_plan_item_id'], entry_plan_item_id)
        self.assertEqual(payload['items'][0]['realized_pnl_krw'], '20000.00')

    def test_list_outcomes_reconciles_capped_execution_readback_with_order_totals(self) -> None:
        snapshot_id = self._seed_snapshot()
        entry_plan_item_id = self._seed_plan_item(
            execution_window='next_open',
            snapshot_id=snapshot_id,
            planned_side='buy',
            client_order_id='SDNO-20260430-005930-PARTIAL-READBACK',
            order_request_id=None,
            broker_order_row_id=None,
            execution_status='submitted',
            traceability_payload={'target_price': 54000, 'stop_loss_price': 47000},
        )
        self._attach_partial_exec_full_order_readback_submission(entry_plan_item_id=entry_plan_item_id)

        payload = signal_decider_execution_outcome_service.list_outcomes(
            snapshot_id=snapshot_id,
            refresh=True,
        )

        self.assertEqual(payload['summary']['count'], 1)
        self.assertEqual(payload['summary']['closed_count'], 1)
        self.assertEqual(payload['summary']['total_realized_pnl_krw'], '20000.00')
        item = payload['items'][0]
        self.assertEqual(item['entry_plan_item_id'], entry_plan_item_id)
        self.assertEqual(item['lifecycle_status'], 'closed')
        self.assertEqual(item['exit_reason'], 'target_hit')
        self.assertEqual(item['entry_filled_quantity'], '5.00000000')
        self.assertEqual(item['exit_filled_quantity'], '5.00000000')
        self.assertEqual(item['closed_quantity'], '5.00000000')
        self.assertEqual(item['open_quantity'], '0.00000000')
        self.assertEqual(item['entry_average_fill_price'], 50000)
        self.assertEqual(item['exit_average_fill_price'], 54000)
        self.assertEqual(item['realized_pnl_krw'], '20000.00')
        self.assertEqual(item['diagnostics']['entry']['exec_readback']['source'], 'orders')
        self.assertEqual(item['diagnostics']['exit']['exec_readback']['source'], 'orders')
        self.assertEqual(
            item['diagnostics']['entry']['exec_readback']['remote_execution_ids'],
            [],
        )
        self.assertEqual(
            item['diagnostics']['exit']['exec_readback']['remote_execution_ids'],
            [],
        )
        self.assertEqual(
            item['diagnostics']['entry']['exec_readback']['remote_order_ids'],
            ['exec-buy-order-1'],
        )
        self.assertEqual(
            item['diagnostics']['exit']['exec_readback']['remote_order_ids'],
            ['exec-sell-order-1'],
        )

    def test_exec_readback_with_both_fact_sources_truncated_is_unavailable(self) -> None:
        snapshot_id = self._seed_snapshot()
        entry_plan_item_id = self._seed_plan_item(
            execution_window='next_open',
            snapshot_id=snapshot_id,
            planned_side='buy',
            client_order_id='SDNO-20260430-005930-LOSSY-READBACK',
            order_request_id=None,
            broker_order_row_id=None,
            execution_status='submitted',
            traceability_payload={'target_price': 54000, 'stop_loss_price': 47000},
        )
        self._attach_partial_exec_full_order_readback_submission(
            entry_plan_item_id=entry_plan_item_id
        )
        with session_scope() as session:
            row = session.get(SignalDeciderExecutionPlanItem, entry_plan_item_id)
            self.assertIsNotNone(row)
            submission_payload = deepcopy(row.submission_payload)
            readback = submission_payload['exec_dispatch']['readback']
            readback['orders']['count'] = 12
            readback['orders']['items_omitted_count'] = 10
            readback['executions']['count'] = 12
            readback['executions']['items_omitted_count'] = 10
            row.submission_payload = submission_payload

        payload = signal_decider_execution_outcome_service.list_outcomes(
            snapshot_id=snapshot_id,
            refresh=True,
        )

        self.assertEqual(payload['summary']['closed_count'], 0)
        self.assertEqual(payload['summary']['unresolved_count'], 1)
        item = payload['items'][0]
        self.assertEqual(item['lifecycle_status'], 'unresolved')
        self.assertIsNone(item['realized_pnl_krw'])
        self.assertEqual(
            item['diagnostics']['entry']['exec_readback']['status'],
            'unavailable',
        )
        self.assertEqual(
            item['diagnostics']['entry']['exec_readback']['reason_code'],
            'exec_readback_fact_sources_incomplete',
        )

    def test_exec_readback_completeness_rejects_malformed_items_container(self) -> None:
        for malformed_items in ({}, 'not-a-list', [None]):
            with self.subTest(malformed_items=malformed_items):
                self.assertFalse(
                    signal_decider_execution_outcome_service._exec_readback_section_is_complete(
                        readback={
                            'orders': {
                                'status': 'ok',
                                'count': 0,
                                'items': malformed_items,
                            }
                        },
                        key='orders',
                    )
                )

    def test_exec_exit_trigger_ignores_fills_from_incomplete_source(self) -> None:
        for incomplete_key, complete_key, intent, expected_reason in (
            ('executions', 'orders', 'stop_sell', 'stop_loss_hit'),
            ('orders', 'executions', 'target_sell', 'target_hit'),
        ):
            with self.subTest(incomplete_key=incomplete_key):
                item_id = f'{incomplete_key}-sell-item'
                readback = {
                    complete_key: {'status': 'ok', 'count': 0, 'items': []},
                    incomplete_key: {
                        'status': 'ok',
                        'count': 2,
                        'items_omitted_count': 1,
                        'items': [
                            {
                                'id': f'{incomplete_key}-sell-fill',
                                'execution_plan_item_id': item_id,
                                'symbol': self.symbol,
                                'side': 'sell',
                                'status': 'filled',
                                'filled_quantity': '5',
                                'quantity': '5',
                                'price': '54000',
                            }
                        ],
                    },
                }
                row = SimpleNamespace(
                    submission_payload={
                        'exec_dispatch': {
                            'item_links': [
                                {
                                    'exec_plan_item_id': item_id,
                                    'symbol': self.symbol,
                                    'side': 'sell',
                                    'intent': intent,
                                }
                            ],
                            'readback': readback,
                        }
                    }
                )

                trigger = signal_decider_execution_outcome_service._resolve_exec_readback_exit_trigger_rule(
                    entry_row=row
                )

                self.assertIsNone(trigger, expected_reason)

    def test_exec_readback_with_truncated_orders_and_empty_complete_executions_is_unavailable(
        self,
    ) -> None:
        snapshot_id = self._seed_snapshot()
        entry_plan_item_id = self._seed_plan_item(
            execution_window='next_open',
            snapshot_id=snapshot_id,
            planned_side='buy',
            client_order_id='SDNO-20260430-005930-LOSSY-ORDERS',
            order_request_id=None,
            broker_order_row_id=None,
            execution_status='submitted',
            traceability_payload={'target_price': 54000, 'stop_loss_price': 47000},
        )
        self._attach_partial_exec_full_order_readback_submission(
            entry_plan_item_id=entry_plan_item_id
        )
        with session_scope() as session:
            row = session.get(SignalDeciderExecutionPlanItem, entry_plan_item_id)
            self.assertIsNotNone(row)
            submission_payload = deepcopy(row.submission_payload)
            readback = submission_payload['exec_dispatch']['readback']
            readback['orders']['count'] = 12
            readback['orders']['items_omitted_count'] = 10
            readback['executions'] = {'status': 'ok', 'count': 0, 'items': []}
            row.submission_payload = submission_payload

        payload = signal_decider_execution_outcome_service.list_outcomes(
            snapshot_id=snapshot_id,
            refresh=True,
        )

        item = payload['items'][0]
        self.assertEqual(item['lifecycle_status'], 'unresolved')
        self.assertIsNone(item['realized_pnl_krw'])
        self.assertEqual(
            item['diagnostics']['entry']['exec_readback']['status'],
            'unavailable',
        )

    def test_exec_readback_with_truncated_executions_and_empty_complete_orders_is_unavailable(
        self,
    ) -> None:
        snapshot_id = self._seed_snapshot()
        entry_plan_item_id = self._seed_plan_item(
            execution_window='next_open',
            snapshot_id=snapshot_id,
            planned_side='buy',
            client_order_id='SDNO-20260430-005930-LOSSY-EXECUTIONS',
            order_request_id=None,
            broker_order_row_id=None,
            execution_status='submitted',
            traceability_payload={'target_price': 54000, 'stop_loss_price': 47000},
        )
        self._attach_partial_exec_full_order_readback_submission(
            entry_plan_item_id=entry_plan_item_id
        )
        with session_scope() as session:
            row = session.get(SignalDeciderExecutionPlanItem, entry_plan_item_id)
            self.assertIsNotNone(row)
            submission_payload = deepcopy(row.submission_payload)
            readback = submission_payload['exec_dispatch']['readback']
            readback['orders'] = {'status': 'ok', 'count': 0, 'items': []}
            row.submission_payload = submission_payload

        payload = signal_decider_execution_outcome_service.list_outcomes(
            snapshot_id=snapshot_id,
            refresh=True,
        )

        item = payload['items'][0]
        self.assertEqual(item['lifecycle_status'], 'unresolved')
        self.assertIsNone(item['realized_pnl_krw'])
        self.assertEqual(
            item['diagnostics']['entry']['exec_readback']['status'],
            'unavailable',
        )

    def test_agent_context_route_returns_persisted_outcomes_with_filters(self) -> None:
        snapshot_id = self._seed_snapshot()
        entry_plan_item_id = self._seed_plan_item(
            execution_window='next_open',
            snapshot_id=snapshot_id,
            planned_side='buy',
            client_order_id='SDNO-20260430-005930-ROUTE',
            order_request_id=None,
            broker_order_row_id=None,
            execution_status='submitted',
            traceability_payload={'target_price': 54000, 'stop_loss_price': 47000},
        )
        self._attach_closed_exec_readback_submission(entry_plan_item_id=entry_plan_item_id)
        signal_decider_execution_outcome_service.list_outcomes(
            snapshot_id=snapshot_id,
            refresh=True,
        )

        app = FastAPI()
        app.include_router(routes_agent_context.router)
        client = TestClient(app)
        response = client.get(
            '/agent-context/signal-decider/execution-outcomes',
            params=[
                ('snapshot_id', str(snapshot_id)),
                ('market_date_from', self.market_date.isoformat()),
                ('market_date_to', self.market_date.isoformat()),
                ('symbol', self.symbol),
                ('account_reference', self.account_reference),
                ('policy_version', self.policy_version),
                ('lifecycle_status', 'closed'),
                ('limit', '10'),
            ],
        )

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload['summary']['count'], 1)
        self.assertEqual(payload['summary']['total_realized_pnl_krw'], '20000.00')
        self.assertEqual(payload['items'][0]['decision_snapshot_id'], snapshot_id)
        self.assertEqual(payload['items'][0]['entry_plan_item_id'], entry_plan_item_id)
        self.assertEqual(payload['items'][0]['lifecycle_status'], 'closed')
        self.assertEqual(payload['items'][0]['symbol'], self.symbol)

    def test_list_outcomes_refresh_persists_rejected_entry_separately_from_synthetic_rows(self) -> None:
        snapshot_id = self._seed_snapshot()
        entry_client_order_id = 'SDNO-20260430-005930-REJECT'
        entry_request, entry_order = self._seed_order(
            client_order_id=entry_client_order_id,
            side='buy',
            source='signal_decider_next_open',
            quantity='5',
            status='rejected',
            filled_quantity='0',
            average_fill_price=None,
            submitted_at='2026-04-30T00:01:00+00:00',
            last_fill_at=None,
            strategy_context_payload={'signal_decider_next_open': {'decision_snapshot_id': snapshot_id}},
        )
        entry_plan_item_id = self._seed_plan_item(
            execution_window='next_open',
            snapshot_id=snapshot_id,
            planned_side='buy',
            client_order_id=entry_client_order_id,
            order_request_id=entry_request['id'],
            broker_order_row_id=entry_order['id'],
            execution_status='submit_failed',
        )

        payload = signal_decider_execution_outcome_service.list_outcomes(
            snapshot_id=snapshot_id,
            refresh=True,
        )

        self.assertEqual(payload['summary']['count'], 1)
        self.assertEqual(payload['summary']['rejected_count'], 1)
        item = payload['items'][0]
        self.assertEqual(item['entry_plan_item_id'], entry_plan_item_id)
        self.assertEqual(item['lifecycle_status'], 'entry_rejected')
        self.assertEqual(item['exit_reason'], 'entry_rejected')
        self.assertEqual(item['entry_filled_quantity'], '0.00000000')
        self.assertEqual(item['exit_filled_quantity'], '0.00000000')
        self.assertIsNone(item['realized_pnl_krw'])
        self.assertEqual(item['diagnostics']['entry']['order_status'], 'rejected')


if __name__ == '__main__':
    unittest.main()
