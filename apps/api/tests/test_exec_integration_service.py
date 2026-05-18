from datetime import date, datetime, timezone
from decimal import Decimal
import unittest
from unittest.mock import patch
from uuid import uuid4

from sqlalchemy import delete

from app.core.settings import settings
from app.db import engine, session_scope
from app.db_models import (
    AgentContextFeedback,
    BrokerOrder,
    OrderRequest,
    SignalDeciderDecisionSnapshot,
    SignalDeciderExecutionPlanItem,
    Symbol,
)
from app.orders.exec_integration import (
    EXEC_INTENT_NEXT_OPEN_BUY,
    EXEC_INTENT_STOP_SELL,
    EXEC_INTENT_TARGET_SELL,
    SignalDeciderExecClientError,
    SignalDeciderExecIntegrationService,
)
from app.orders.next_open_execution import EXECUTION_STATUS_DELEGATED_FOR_EXEC_AGENT_SIZING, EXECUTION_STATUS_SUBMITTED


class FakeExecClient:
    def __init__(self) -> None:
        self.post_calls: list[dict] = []
        self.runtime_calls = 0
        self.plan_calls: list[dict] = []
        self.order_calls: list[dict] = []
        self.execution_calls: list[dict] = []
        self.position_calls: list[dict] = []
        self.runtime_response: dict = {'ready': True, 'mode': 'mock'}
        self.plan_response: dict = {'items': []}
        self.order_response: dict = {'items': []}
        self.execution_response: dict = {'items': []}
        self.position_response: dict = {'items': []}
        self.agent_approved_quantities_by_agent: dict[str, str] = {}
        self.agent_approved_quantities_by_core_key: dict[str, str] = {}
        self.agent_sizing_payloads_by_agent: dict[str, dict] = {}
        self.agent_sizing_payloads_by_core_key: dict[str, dict] = {}

    async def get_runtime_status(self) -> dict:
        self.runtime_calls += 1
        return self.runtime_response

    async def post_execution_plan(self, payload: dict) -> dict:
        self.post_calls.append(payload)
        items = []
        for index, item in enumerate(payload['items'], start=1):
            response_item = {
                'core_item_key': item['core_item_key'],
                'execution_plan_id': payload['plan_id'],
                'execution_plan_item_id': f'exec-item-{index}',
                'status': 'accepted',
            }
            if (
                item.get('intent') == EXEC_INTENT_NEXT_OPEN_BUY
                and (item.get('quantity_policy') or {}).get('type') == 'exec_agent_sized'
            ):
                agent_sizing_payload = (
                    self.agent_sizing_payloads_by_core_key.get(item['core_item_key'])
                    or self.agent_sizing_payloads_by_agent.get(item.get('decision_agent_name'))
                )
                approved_quantity = (
                    self.agent_approved_quantities_by_core_key.get(item['core_item_key'])
                    or self.agent_approved_quantities_by_agent.get(item.get('decision_agent_name'))
                )
                if agent_sizing_payload is not None:
                    response_item['agent_sizing'] = agent_sizing_payload
                elif approved_quantity is not None:
                    response_item['agent_sizing'] = {
                        'approved': True,
                        'quantity': approved_quantity,
                    }
            items.append(response_item)
        return {
            'accepted_item_count': len(items),
            'rejected_item_count': 0,
            'items': items,
        }

    async def list_execution_plans(self, *, external_plan_id: str | None = None) -> dict:
        self.plan_calls.append({'external_plan_id': external_plan_id})
        return self.plan_response

    async def list_orders(self, *, external_plan_id: str | None = None, symbols=None) -> dict:
        self.order_calls.append({'external_plan_id': external_plan_id, 'symbols': symbols})
        return self.order_response

    async def list_executions(self, *, external_plan_id: str | None = None, symbols=None) -> dict:
        self.execution_calls.append({'external_plan_id': external_plan_id, 'symbols': symbols})
        return self.execution_response

    async def list_positions(self, *, symbols=None) -> dict:
        self.position_calls.append({'symbols': symbols})
        return self.position_response


class FailingRuntimeExecClient(FakeExecClient):
    async def get_runtime_status(self) -> dict:
        self.runtime_calls += 1
        raise SignalDeciderExecClientError(
            'exec transport failure',
            code='unavailable',
            retryable=True,
        )


class ExecIntegrationServiceTests(unittest.IsolatedAsyncioTestCase):
    @classmethod
    def setUpClass(cls) -> None:
        AgentContextFeedback.__table__.create(bind=engine, checkfirst=True)
        OrderRequest.__table__.create(bind=engine, checkfirst=True)
        BrokerOrder.__table__.create(bind=engine, checkfirst=True)
        SignalDeciderDecisionSnapshot.__table__.create(bind=engine, checkfirst=True)
        SignalDeciderExecutionPlanItem.__table__.create(bind=engine, checkfirst=True)
        Symbol.__table__.create(bind=engine, checkfirst=True)

    def setUp(self) -> None:
        suffix = uuid4().hex[:8].upper()
        self.policy_version = f'signal_decider_policy_exec_{suffix.lower()}'
        self.account_reference = f'KIS:98{suffix[:6]}-01'
        self.symbol = '005930'
        self.market_date = date(2026, 4, 29)
        self.target_trade_date = date(2026, 4, 30)
        self.original_settings = {
            'signal_decider_exec_integration_mode': settings.signal_decider_exec_integration_mode,
            'signal_decider_exec_base_url': settings.signal_decider_exec_base_url,
            'signal_decider_exec_dispatch_source': settings.signal_decider_exec_dispatch_source,
            'signal_decider_exec_runtime_mode': settings.signal_decider_exec_runtime_mode,
            'signal_decider_exec_request_timeout_seconds': settings.signal_decider_exec_request_timeout_seconds,
            'signal_decider_next_open_enabled': settings.signal_decider_next_open_enabled,
            'signal_decider_next_open_dry_run': settings.signal_decider_next_open_dry_run,
        }
        settings.signal_decider_exec_integration_mode = 'dispatch'
        settings.signal_decider_exec_base_url = 'http://exec.test'
        settings.signal_decider_exec_dispatch_source = 'kai-trading-core'
        settings.signal_decider_exec_runtime_mode = 'mock'
        settings.signal_decider_exec_request_timeout_seconds = 5.0
        settings.signal_decider_next_open_enabled = True
        settings.signal_decider_next_open_dry_run = False
        self.client = FakeExecClient()
        self.service = SignalDeciderExecIntegrationService(client=self.client)
        self._seed_symbol()
        self.snapshot_id = self._seed_snapshot()
        self.plan_item_id = self._seed_plan_item()

    def tearDown(self) -> None:
        for key, value in self.original_settings.items():
            setattr(settings, key, value)
        with session_scope() as session:
            session.execute(
                delete(SignalDeciderExecutionPlanItem).where(
                    SignalDeciderExecutionPlanItem.account_reference == self.account_reference
                )
            )
            session.execute(
                delete(SignalDeciderDecisionSnapshot).where(
                    SignalDeciderDecisionSnapshot.policy_version == self.policy_version
                )
            )
            session.execute(
                delete(SignalDeciderDecisionSnapshot).where(
                    SignalDeciderDecisionSnapshot.agent_name == 'short-term-decider',
                    SignalDeciderDecisionSnapshot.market_date == self.market_date,
                )
            )
            session.execute(delete(Symbol).where(Symbol.symbol == self.symbol))

    def _seed_snapshot(self) -> int:
        with session_scope() as session:
            row = SignalDeciderDecisionSnapshot(
                feedback_id=None,
                market_date=self.market_date,
                symbol=self.symbol,
                agent_name='signal-decider',
                agent_version='test-agent',
                decision_run_id='exec-run-1',
                feedback_received_at=datetime(2026, 4, 29, 6, 0, tzinfo=timezone.utc),
                policy_version=self.policy_version,
                decision_action='buy',
                preferred_action='buy',
                next_day_open_action='buy',
                entry_price_basis='next_open',
                target_price=54000,
                expected_return_pct=4.5,
                stop_loss_price=47000,
                suggested_holding_period='3-10 trading days',
                confidence_score_10=7.5,
                conviction_score_10=6.1,
                thesis_summary='Dispatch to exec.',
                supporting_evidence_payload=['Breadth improved.'],
                decision_risks_payload=['Leadership remained narrow.'],
                invalidation_conditions_payload=['Close below support.'],
                data_quality_warnings_payload=[],
                specialist_disagreement_payload=[],
                traceability_payload={'gate_checks': {'missing_upstream_roles': []}},
                input_feature_snapshot_payload={'technical': {'trend_state': 'uptrend'}},
                request_payload={'signal_decider_policy': {'current_version': self.policy_version}},
                response_payload={'next_day_open_action': 'buy'},
                response_summary_payload={'policy_version': self.policy_version},
            )
            session.add(row)
            session.flush()
            return row.id

    def _seed_short_term_snapshot(
        self,
        *,
        tactical_action: str = 'watch_for_trigger',
        trigger_price: int | None = 50500,
        chase_limit_price: int | None = 51500,
        target_price: int | None = 54000,
        stop_loss_price: int | None = 49200,
        request_payload: dict | None = None,
        input_feature_snapshot_payload: dict | None = None,
        response_payload_extra: dict | None = None,
    ) -> int:
        generic_action = 'buy' if tactical_action in {'scalp_buy_candidate', 'watch_for_trigger'} else tactical_action
        if generic_action not in {'buy', 'wait', 'reject'}:
            generic_action = 'wait'
        with session_scope() as session:
            row = SignalDeciderDecisionSnapshot(
                feedback_id=None,
                market_date=self.market_date,
                symbol=self.symbol,
                agent_name='short-term-decider',
                agent_version='test-agent',
                decision_run_id='short-run-1',
                feedback_received_at=datetime(2026, 4, 29, 6, 0, tzinfo=timezone.utc),
                policy_version='short_term_decider_contract_v1',
                decision_action=generic_action,
                preferred_action=generic_action,
                next_day_open_action=generic_action,
                entry_price_basis='trigger_limit',
                target_price=target_price,
                expected_return_pct=None,
                stop_loss_price=stop_loss_price,
                suggested_holding_period='1-2 trading days',
                confidence_score_10=7.3,
                conviction_score_10=6.5,
                thesis_summary='Dispatch guarded short-term setup.',
                supporting_evidence_payload=['Opening setup stayed constructive.'],
                decision_risks_payload=['Gap risk remained elevated.'],
                invalidation_conditions_payload=['Trigger reclaim failed.'],
                data_quality_warnings_payload=[],
                specialist_disagreement_payload=[],
                traceability_payload={
                    'schema_version': 'short_term_decider_trace_v1',
                    'gate_checks': {
                        'missing_upstream_roles': [],
                        'decision_support_only': True,
                        'allow_execution_logic': False,
                    },
                },
                input_feature_snapshot_payload=input_feature_snapshot_payload
                or {'meta': {'source_agent_name': 'short-term-decider'}},
                request_payload=request_payload or {},
                response_payload={
                    'role': 'short-term-decider',
                    'agent_name': 'short-term-decider',
                    'tactical_action': tactical_action,
                    'preferred_action': tactical_action,
                    'trigger_price': trigger_price,
                    'chase_limit_price': chase_limit_price,
                    'target_price': target_price,
                    'stop_loss_price': stop_loss_price,
                    'expected_holding_period': '1-2 trading days',
                    'confidence_score_10': 7.3,
                    'conviction_score_10': 6.5,
                    **(response_payload_extra or {}),
                },
                response_summary_payload={'model': 'openclaw/short-term-decider'},
            )
            session.add(row)
            session.flush()
            return row.id

    def _seed_symbol(self) -> None:
        with session_scope() as session:
            session.execute(delete(Symbol).where(Symbol.symbol == self.symbol))
            session.add(
                Symbol(
                    symbol=self.symbol,
                    standard_code=None,
                    name_ko='Samsung Electronics',
                    market='KOSPI',
                    instrument_type='equity',
                    source='test',
                    is_active=True,
                )
            )
            session.flush()

    def _seed_plan_item(self) -> int:
        with session_scope() as session:
            row = SignalDeciderExecutionPlanItem(
                execution_window='next_open',
                decision_agent_name='signal-decider',
                decision_snapshot_id=self.snapshot_id,
                order_request_id=None,
                broker_order_row_id=None,
                broker_name='kis',
                target_trade_date=self.target_trade_date,
                source_market_date=self.market_date,
                account_reference=self.account_reference,
                account_label='Main',
                symbol=self.symbol,
                policy_version=self.policy_version,
                planned_side='buy',
                planned_order_type='market',
                planned_time_in_force='day',
                allocation_budget_krw='1500000',
                estimated_reference_price=50000,
                planned_quantity='5',
                estimated_notional_krw='250000',
                eligibility_status='eligible',
                execution_status='planned',
                client_order_id='SDNO-20260430-005930',
                validation_payload={'blocking_reasons': [], 'warnings': []},
                traceability_payload={
                    'decision_snapshot_id': self.snapshot_id,
                    'decision_run_id': 'exec-run-1',
                    'policy_version': self.policy_version,
                    'target_trade_date': self.target_trade_date.isoformat(),
                    'source_market_date': self.market_date.isoformat(),
                    'account_reference': self.account_reference,
                    'account_label': 'Main',
                    'allocation_budget_krw': '1500000',
                    'target_price': 54000,
                    'stop_loss_price': 47000,
                    'suggested_holding_period': '3-10 trading days',
                    'signal_traceability': {'gate_checks': {'missing_upstream_roles': []}},
                },
                submission_payload=None,
                first_ready_at=datetime(2026, 4, 29, 23, 30, tzinfo=timezone.utc),
                last_evaluated_at=datetime(2026, 4, 29, 23, 30, tzinfo=timezone.utc),
                submitted_at=None,
            )
            session.add(row)
            session.flush()
            return row.id

    def _seed_short_term_plan_item(
        self,
        *,
        snapshot_id: int,
        trigger_price: int | None = 50500,
        chase_limit_price: int | None = 51500,
        target_price: int | None = 54000,
        stop_loss_price: int | None = 49200,
    ) -> int:
        with session_scope() as session:
            row = SignalDeciderExecutionPlanItem(
                execution_window='next_open',
                decision_agent_name='short-term-decider',
                decision_snapshot_id=snapshot_id,
                order_request_id=None,
                broker_order_row_id=None,
                broker_name='kis',
                target_trade_date=self.target_trade_date,
                source_market_date=self.market_date,
                account_reference=self.account_reference,
                account_label='Main',
                symbol=self.symbol,
                policy_version='short_term_decider_contract_v1',
                planned_side='buy',
                planned_order_type='limit',
                planned_time_in_force='day',
                allocation_budget_krw='1500000',
                estimated_reference_price=51500,
                planned_quantity='29',
                estimated_notional_krw='1493500',
                eligibility_status='eligible',
                execution_status='planned',
                client_order_id='STDE-20260430-005930',
                validation_payload={
                    'blocking_reasons': [],
                    'warnings': [],
                    'decision_agent_name': 'short-term-decider',
                    'short_term': {
                        'tactical_action': 'watch_for_trigger',
                        'trigger_price': trigger_price,
                        'chase_limit_price': chase_limit_price,
                        'target_price': target_price,
                        'stop_loss_price': stop_loss_price,
                        'expected_holding_period': '1-2 trading days',
                    },
                },
                traceability_payload={
                    'agent_name': 'short-term-decider',
                    'decision_agent_name': 'short-term-decider',
                    'decision_snapshot_id': snapshot_id,
                    'source_feedback_id': None,
                    'decision_run_id': 'short-run-1',
                    'policy_version': 'short_term_decider_contract_v1',
                    'target_trade_date': self.target_trade_date.isoformat(),
                    'source_market_date': self.market_date.isoformat(),
                    'account_reference': self.account_reference,
                    'account_label': 'Main',
                    'allocation_budget_krw': '1500000',
                    'target_price': target_price,
                    'stop_loss_price': stop_loss_price,
                    'expected_holding_period': '1-2 trading days',
                    'tactical_action': 'watch_for_trigger',
                    'trigger_price': trigger_price,
                    'chase_limit_price': chase_limit_price,
                    'entry_execution_mode': 'pending_trigger_limit',
                    'decision_traceability': {'schema_version': 'short_term_decider_trace_v1'},
                },
                submission_payload=None,
                first_ready_at=datetime(2026, 4, 29, 23, 30, tzinfo=timezone.utc),
                last_evaluated_at=datetime(2026, 4, 29, 23, 30, tzinfo=timezone.utc),
                submitted_at=None,
            )
            session.add(row)
            session.flush()
            return row.id

    async def test_dispatch_next_open_plan_translates_and_persists_exec_linkage(self) -> None:
        result = await self.service.dispatch_next_open_plan(
            target_trade_date=self.target_trade_date,
            refresh_plan=False,
            dry_run=False,
            force=True,
            trigger='test',
        )

        self.assertEqual(result['summary']['translated_item_count'], 3)
        self.assertTrue(result['summary']['delegated_to_exec'])
        self.assertEqual(len(self.client.post_calls), 1)
        payload = self.client.post_calls[0]
        self.assertEqual(payload['market_date'], '2026-04-30')
        intents = [item['intent'] for item in payload['items']]
        self.assertEqual(
            intents,
            [EXEC_INTENT_NEXT_OPEN_BUY, EXEC_INTENT_TARGET_SELL, EXEC_INTENT_STOP_SELL],
        )
        entry_item = payload['items'][0]
        self.assertEqual(entry_item['quantity_policy']['type'], 'exec_agent_sized')
        self.assertNotIn('quantity', entry_item['quantity_policy'])
        self.assertEqual(Decimal(str(entry_item['quantity_policy']['budget_krw'])), Decimal('1500000'))
        self.assertIsNone(entry_item['quantity_policy']['estimated_notional_krw'])
        self.assertEqual(entry_item['risk_context']['classification'], EXECUTION_STATUS_DELEGATED_FOR_EXEC_AGENT_SIZING)
        self.assertEqual(
            entry_item['risk_context']['max_holding_close_policy'],
            {
                'enabled': False,
                'type': 'none',
                'intent': None,
                'max_holding_trading_sessions': None,
                'reason': 'general_signal_decider_uses_decider_holding_context_no_short_term_forced_close',
            },
        )
        self.assertNotIn('max_holding_close_sell', [item['intent'] for item in payload['items']])
        self.assertEqual(
            entry_item['risk_context']['decider_handoff_context']['classification'],
            EXECUTION_STATUS_DELEGATED_FOR_EXEC_AGENT_SIZING,
        )
        self.assertNotIn('quantity', payload['items'][1]['quantity_policy'])
        self.assertNotIn('quantity', payload['items'][2]['quantity_policy'])

        with session_scope() as session:
            row = session.get(SignalDeciderExecutionPlanItem, self.plan_item_id)
            self.assertEqual(row.execution_status, EXECUTION_STATUS_DELEGATED_FOR_EXEC_AGENT_SIZING)
            exec_dispatch = row.submission_payload['exec_dispatch']
            self.assertIn('plan_id', exec_dispatch)
            self.assertEqual(exec_dispatch['dispatch_status'], 'accepted')
            self.assertEqual(len(exec_dispatch['item_links']), 3)

    async def test_dispatch_next_open_plan_signal_agent_quantity_marks_submitted(self) -> None:
        self.client.agent_approved_quantities_by_agent['signal-decider'] = '5'

        result = await self.service.dispatch_next_open_plan(
            target_trade_date=self.target_trade_date,
            refresh_plan=False,
            dry_run=False,
            force=True,
            trigger='test',
        )

        self.assertEqual(result['summary']['submitted_count'], 1)
        self.assertEqual(result['summary']['delegated_for_exec_agent_sizing_count'], 0)
        with session_scope() as session:
            row = session.get(SignalDeciderExecutionPlanItem, self.plan_item_id)
            self.assertEqual(row.execution_status, EXECUTION_STATUS_SUBMITTED)
            entry_link = row.submission_payload['exec_dispatch']['item_links'][0]
            self.assertEqual(entry_link['response_item']['agent_sizing']['quantity'], '5')

    async def test_dispatch_next_open_plan_signal_rejected_agent_quantity_stays_delegated(self) -> None:
        self.client.agent_sizing_payloads_by_agent['signal-decider'] = {
            'approved': False,
            'quantity': '5',
        }

        result = await self.service.dispatch_next_open_plan(
            target_trade_date=self.target_trade_date,
            refresh_plan=False,
            dry_run=False,
            force=True,
            trigger='test',
        )

        self.assertEqual(result['summary']['submitted_count'], 0)
        self.assertEqual(result['summary']['delegated_for_exec_agent_sizing_count'], 1)
        with session_scope() as session:
            row = session.get(SignalDeciderExecutionPlanItem, self.plan_item_id)
            self.assertEqual(row.execution_status, EXECUTION_STATUS_DELEGATED_FOR_EXEC_AGENT_SIZING)
            entry_link = row.submission_payload['exec_dispatch']['item_links'][0]
            self.assertFalse(entry_link['response_item']['agent_sizing']['approved'])
            self.assertEqual(entry_link['response_item']['agent_sizing']['quantity'], '5')

    async def test_dispatch_next_open_plan_signal_handoff_strips_decider_only_labels(self) -> None:
        with session_scope() as session:
            snapshot = session.get(SignalDeciderDecisionSnapshot, self.snapshot_id)
            snapshot.request_payload = {
                'role': 'signal-decider',
                'agent_name': 'signal-decider',
                'signal_decider_policy': {'current_version': self.policy_version},
            }
            snapshot.response_payload = {
                'role': 'signal-decider',
                'agent_name': 'signal-decider',
                'next_day_open_action': 'buy',
                'risk_flags': ['Size account-aware.'],
            }

        await self.service.dispatch_next_open_plan(
            target_trade_date=self.target_trade_date,
            refresh_plan=False,
            dry_run=False,
            force=True,
            trigger='test',
        )

        entry_item = self.client.post_calls[0]['items'][0]
        handoff = entry_item['risk_context']['decider_handoff_context']
        self.assertNotIn('role', handoff['decider_input_context']['request_payload'])
        self.assertNotIn('agent_name', handoff['decider_input_context']['request_payload'])
        self.assertNotIn('role', handoff['decider_output']['response_payload'])
        self.assertNotIn('agent_name', handoff['decider_output']['response_payload'])
        self.assertEqual(handoff['decider_output']['response_payload']['next_day_open_action'], 'buy')

    async def test_dispatch_next_open_plan_uses_next_open_dry_run_default(self) -> None:
        settings.signal_decider_next_open_enabled = False
        settings.signal_decider_next_open_dry_run = True

        result = await self.service.dispatch_next_open_plan(
            target_trade_date=self.target_trade_date,
            refresh_plan=False,
            trigger='test',
        )

        self.assertTrue(result['request']['dry_run'])
        self.assertEqual(result['summary']['preview_group_count'], 1)
        self.assertEqual(result['dispatch']['groups'][0]['dispatch_status'], 'preview')
        self.assertEqual(len(self.client.post_calls), 0)

    async def test_dispatch_next_open_plan_respects_execution_enabled_gate(self) -> None:
        settings.signal_decider_next_open_enabled = False
        settings.signal_decider_next_open_dry_run = False

        with patch('app.orders.exec_integration.signal_decider_next_open_execution_service.refresh_plan_items') as refresh_plan:
            result = await self.service.dispatch_next_open_plan(
                target_trade_date=self.target_trade_date,
                refresh_plan=True,
                trigger='test',
            )

        refresh_plan.assert_not_called()
        self.assertFalse(result['request']['dry_run'])
        self.assertEqual(result['summary']['reason'], 'execution_disabled')
        self.assertFalse(result['summary']['triggered'])
        self.assertEqual(len(self.client.post_calls), 0)

    async def test_dispatch_next_open_plan_includes_short_term_groups_with_explicit_identity(self) -> None:
        decider_request_payload = {
            'symbol_day_context': {
                'pre_decider_enrichment_context': {
                    'schema_version': 'pre_decider_enrichment_v1',
                    'quote_orderbook_snapshot': {'status': 'stale_eod'},
                },
                'liquidity_context': {'turnover': {'status': 'available'}},
            },
            'orchestration': {'run_id': 'symbol-run-1'},
        }
        decider_input_features = {
            'technical': {'trend_state': 'uptrend'},
            'snapshots': {'market_risk': {'risk_level': 'normal'}},
        }
        short_snapshot_id = self._seed_short_term_snapshot(
            request_payload=decider_request_payload,
            input_feature_snapshot_payload=decider_input_features,
            response_payload_extra={
                'trigger_conditions': ['Reclaim 50500 with volume.'],
                'risk_flags': ['Opening gap risk.'],
            },
        )
        self._seed_short_term_plan_item(snapshot_id=short_snapshot_id)

        result = await self.service.dispatch_next_open_plan(
            target_trade_date=self.target_trade_date,
            refresh_plan=False,
            dry_run=False,
            force=True,
            trigger='test',
        )

        self.assertEqual(len(self.client.post_calls), 2)
        by_agent = {
            payload['metadata']['decision_agent_name']: payload
            for payload in self.client.post_calls
        }
        self.assertEqual(set(by_agent), {'signal-decider', 'short-term-decider'})
        signal_payload = by_agent['signal-decider']
        short_payload = by_agent['short-term-decider']
        self.assertEqual(signal_payload['plan_id'], f'core-signal_decider-20260430-{self.account_reference.replace(":", "_").replace("-", "_")}')
        self.assertEqual(short_payload['metadata']['decision_agent_name'], 'short-term-decider')
        self.assertEqual(short_payload['strategy_run_id'], f'short-term-decider:2026-04-30:{self.account_reference.replace(":", "_").replace("-", "_")}')
        signal_entry = signal_payload['items'][0]
        self.assertEqual(signal_entry['quantity_policy']['type'], 'exec_agent_sized')
        self.assertNotIn('quantity', signal_entry['quantity_policy'])
        self.assertEqual(signal_entry['risk_context']['classification'], EXECUTION_STATUS_DELEGATED_FOR_EXEC_AGENT_SIZING)
        self.assertEqual(signal_entry['risk_context']['max_holding_close_policy']['enabled'], False)
        self.assertIsNone(signal_entry['risk_context']['max_holding_close_policy']['max_holding_trading_sessions'])
        self.assertNotIn('max_holding_close_sell', [item['intent'] for item in signal_payload['items']])
        short_entry = short_payload['items'][0]
        self.assertEqual(short_entry['decision_agent_name'], 'short-term-decider')
        self.assertIsNone(short_entry['trigger_price'])
        self.assertIsNone(short_entry['limit_price'])
        self.assertEqual(short_entry['risk_context']['agent_name'], 'short-term-decider')
        self.assertEqual(short_entry['risk_context']['decision_agent_name'], 'short-term-decider')
        self.assertEqual(short_entry['risk_context']['entry_execution_mode'], 'next_open_market')
        self.assertEqual(short_entry['risk_context']['original_watch_trigger_price'], 50500)
        self.assertEqual(short_entry['risk_context']['original_watch_chase_limit_price'], 51500)
        self.assertNotIn('trigger_price', short_entry['risk_context'])
        self.assertNotIn('chase_limit_price', short_entry['risk_context'])
        self.assertIn('source_feedback_id', short_entry['risk_context'])
        self.assertIn('target_trade_date', short_entry['risk_context'])
        self.assertIn('source_market_date', short_entry['risk_context'])
        self.assertEqual(short_entry['risk_context']['allocation_mode'], 'specified')
        self.assertEqual(Decimal(str(short_entry['risk_context']['allocation_budget_krw'])), Decimal('1500000'))
        self.assertEqual(short_entry['risk_context']['confidence_score_10'], 7.3)
        self.assertEqual(short_entry['risk_context']['conviction_score_10'], 6.5)
        self.assertEqual(short_entry['risk_context']['decision_run_id'], 'short-run-1')
        self.assertEqual(short_entry['quantity_policy']['type'], 'exec_agent_sized')
        self.assertEqual(Decimal(str(short_entry['quantity_policy']['budget_krw'])), Decimal('1500000'))
        self.assertEqual(short_entry['quantity_policy']['allocation_mode'], 'specified')
        self.assertEqual(short_entry['quantity_policy']['account_reference'], self.account_reference)
        self.assertEqual(short_entry['quantity_policy']['account_label'], 'Main')
        self.assertEqual(short_entry['risk_context']['classification'], EXECUTION_STATUS_DELEGATED_FOR_EXEC_AGENT_SIZING)
        self.assertEqual(
            short_entry['risk_context']['max_holding_close_policy'],
            {
                'enabled': True,
                'type': 'exec_agent_default',
                'intent': 'max_holding_close_sell',
                'max_holding_trading_sessions': 3,
                'reason': 'short_term_exec_agent_sizing_default',
            },
        )
        handoff = short_entry['risk_context']['decider_handoff_context']
        self.assertEqual(handoff['classification'], EXECUTION_STATUS_DELEGATED_FOR_EXEC_AGENT_SIZING)
        self.assertEqual(handoff['symbol'], self.symbol)
        self.assertEqual(handoff['target_trade_date'], '2026-04-30')
        self.assertEqual(handoff['source_market_date'], '2026-04-29')
        self.assertEqual(handoff['traceability']['decision_snapshot_id'], short_snapshot_id)
        self.assertEqual(handoff['traceability']['core_plan_item_id'], short_entry['risk_context']['core_plan_item_id'])
        self.assertEqual(handoff['decider_input_context']['request_payload'], decider_request_payload)
        self.assertEqual(handoff['decider_input_context']['input_feature_snapshot_payload'], decider_input_features)
        self.assertEqual(
            handoff['decider_input_context']['pre_decider_enrichment_context'],
            decider_request_payload['symbol_day_context']['pre_decider_enrichment_context'],
        )
        self.assertEqual(handoff['decider_output']['response_payload']['trigger_conditions'], ['Reclaim 50500 with volume.'])
        self.assertEqual(handoff['decider_output']['response_payload']['risk_flags'], ['Opening gap risk.'])
        self.assertEqual(result['summary']['translated_item_count'], 6)
        self.assertEqual(
            {group['decision_agent_name'] for group in result['dispatch']['groups']},
            {'signal-decider', 'short-term-decider'},
        )
        short_item = next(item for item in result['items'] if item['decision_agent_name'] == 'short-term-decider')
        self.assertEqual(short_item['execution_status'], EXECUTION_STATUS_DELEGATED_FOR_EXEC_AGENT_SIZING)
        signal_item = next(item for item in result['items'] if item['decision_agent_name'] == 'signal-decider')
        self.assertEqual(signal_item['execution_status'], EXECUTION_STATUS_DELEGATED_FOR_EXEC_AGENT_SIZING)
        self.assertEqual(result['summary']['delegated_for_exec_agent_sizing_count'], 2)

    async def test_dispatch_next_open_plan_short_term_missing_target_stop_dispatches_entry_only(self) -> None:
        short_snapshot_id = self._seed_short_term_snapshot(target_price=None, stop_loss_price=None)
        self._seed_short_term_plan_item(
            snapshot_id=short_snapshot_id,
            target_price=None,
            stop_loss_price=None,
        )

        await self.service.dispatch_next_open_plan(
            target_trade_date=self.target_trade_date,
            refresh_plan=False,
            dry_run=False,
            force=True,
            trigger='test',
        )

        short_payload = next(
            payload for payload in self.client.post_calls if payload['metadata']['decision_agent_name'] == 'short-term-decider'
        )
        self.assertEqual(len(short_payload['items']), 1)
        self.assertIsNone(short_payload['items'][0]['trigger_price'])
        self.assertIsNone(short_payload['items'][0]['limit_price'])
        self.assertEqual(short_payload['items'][0]['risk_context']['entry_execution_mode'], 'next_open_market')
        self.assertEqual(short_payload['items'][0]['quantity_policy']['type'], 'exec_agent_sized')

    async def test_dispatch_next_open_plan_short_term_idempotent_redispatch_skips_duplicate_payload(self) -> None:
        short_snapshot_id = self._seed_short_term_snapshot()
        self._seed_short_term_plan_item(snapshot_id=short_snapshot_id)

        first_result = await self.service.dispatch_next_open_plan(
            target_trade_date=self.target_trade_date,
            refresh_plan=False,
            dry_run=False,
            force=False,
            trigger='test',
        )
        self.assertEqual(first_result['summary']['translated_item_count'], 6)
        self.assertEqual(first_result['summary']['dispatched_group_count'], 2)
        first_short_group = next(
            group for group in first_result['dispatch']['groups'] if group['decision_agent_name'] == 'short-term-decider'
        )
        self.assertEqual(first_short_group['dispatch_status'], 'accepted')
        first_post_call_count = len(self.client.post_calls)

        second_result = await self.service.dispatch_next_open_plan(
            target_trade_date=self.target_trade_date,
            refresh_plan=False,
            dry_run=False,
            force=False,
            trigger='test',
        )
        self.assertEqual(
            second_result['summary']['translated_item_count'],
            first_result['summary']['translated_item_count'],
        )
        self.assertEqual(second_result['summary']['dispatched_group_count'], 0)
        self.assertEqual(second_result['summary']['failed_group_count'], 0)
        self.assertEqual(second_result['summary']['skipped_group_count'], 2)
        short_group = next(
            group for group in second_result['dispatch']['groups'] if group['decision_agent_name'] == 'short-term-decider'
        )
        self.assertEqual(short_group['dispatch_status'], 'already_dispatched')
        self.assertEqual(short_group['plan_id'], first_short_group['plan_id'])
        self.assertEqual(len(self.client.post_calls), first_post_call_count)

    async def test_exec_overview_includes_both_deciders_without_symbol_collision(self) -> None:
        short_snapshot_id = self._seed_short_term_snapshot()
        self._seed_short_term_plan_item(snapshot_id=short_snapshot_id)

        overview = await self.service.list_signal_decider_exec_overview(
            target_trade_date=self.target_trade_date,
            symbol=self.symbol,
            refresh=False,
            trigger='test',
        )

        self.assertEqual(len(overview['items']), 2)
        self.assertEqual(overview['summary']['decision_agent_counts']['short-term-decider'], 1)
        self.assertEqual(overview['summary']['decision_agent_counts']['signal-decider'], 1)
        self.assertEqual(
            {item['decision_agent_name'] for item in overview['items']},
            {'signal-decider', 'short-term-decider'},
        )

    async def test_exec_overview_refresh_persists_remote_readback(self) -> None:
        dispatch_result = await self.service.dispatch_next_open_plan(
            target_trade_date=self.target_trade_date,
            refresh_plan=False,
            dry_run=False,
            force=True,
            trigger='test',
        )
        plan_id = dispatch_result['dispatch']['groups'][0]['plan_id']
        self.client.plan_response = {'items': [{'external_plan_id': plan_id, 'status': 'accepted'}]}
        self.client.order_response = {
            'items': [
                {
                    'id': 'order-1',
                    'execution_plan_item_id': 'exec-item-1',
                    'symbol': self.symbol,
                    'status': 'submitted',
                }
            ]
        }
        self.client.execution_response = {
            'items': [
                {
                    'broker_order_id': 'order-1',
                    'symbol': self.symbol,
                    'side': 'buy',
                    'quantity': '5',
                }
            ]
        }
        self.client.position_response = {
            'items': [{'symbol': self.symbol, 'quantity': '5', 'average_price': '50000'}]
        }

        overview = await self.service.list_signal_decider_exec_overview(
            target_trade_date=self.target_trade_date,
            symbol=self.symbol,
            refresh=True,
            trigger='test',
        )

        self.assertEqual(overview['summary']['remote_open_count'], 1)
        self.assertEqual(overview['items'][0]['dispatch']['status'], 'accepted')
        self.assertEqual(overview['items'][0]['remote_state']['derived_status'], 'open')
        self.assertEqual(overview['items'][0]['remote_state']['entry_filled_quantity'], '5')
        self.assertEqual(self.client.runtime_calls, 1)
        self.assertEqual(self.client.plan_calls[0]['external_plan_id'], plan_id)

        with session_scope() as session:
            row = session.get(SignalDeciderExecutionPlanItem, self.plan_item_id)
            readback = row.submission_payload['exec_dispatch']['readback']
            self.assertEqual(readback['orders']['count'], 1)
            self.assertEqual(readback['derived_state']['derived_status'], 'open')

    async def test_exec_overview_refresh_filters_positions_to_current_account(self) -> None:
        dispatch_result = await self.service.dispatch_next_open_plan(
            target_trade_date=self.target_trade_date,
            refresh_plan=False,
            dry_run=False,
            force=True,
            trigger='test',
        )
        plan_id = dispatch_result['dispatch']['groups'][0]['plan_id']
        self.client.plan_response = {'items': [{'external_plan_id': plan_id, 'status': 'accepted'}]}
        self.client.order_response = {'items': []}
        self.client.execution_response = {'items': []}
        self.client.position_response = {
            'items': [
                {
                    'account_reference': 'KIS:00000000-01',
                    'symbol': self.symbol,
                    'quantity': '5',
                    'average_price': '50000',
                }
            ]
        }

        overview = await self.service.list_signal_decider_exec_overview(
            target_trade_date=self.target_trade_date,
            account_reference=self.account_reference,
            symbol=self.symbol,
            refresh=True,
            trigger='test',
        )

        self.assertEqual(overview['items'][0]['remote_state']['derived_status'], 'accepted')
        self.assertEqual(overview['items'][0]['remote_state']['positions'], [])

        with session_scope() as session:
            row = session.get(SignalDeciderExecutionPlanItem, self.plan_item_id)
            readback = row.submission_payload['exec_dispatch']['readback']
            self.assertEqual(readback['positions']['count'], 0)

    async def test_exec_overview_refresh_persists_short_term_readback(self) -> None:
        with session_scope() as session:
            session.execute(
                delete(
                    SignalDeciderExecutionPlanItem,
                ).where(
                    SignalDeciderExecutionPlanItem.account_reference == self.account_reference,
                    SignalDeciderExecutionPlanItem.decision_agent_name == 'signal-decider',
                )
            )

        short_snapshot_id = self._seed_short_term_snapshot()
        self._seed_short_term_plan_item(snapshot_id=short_snapshot_id)
        dispatch_result = await self.service.dispatch_next_open_plan(
            target_trade_date=self.target_trade_date,
            refresh_plan=False,
            dry_run=False,
            force=True,
            trigger='test',
        )
        short_group = next(
            group for group in dispatch_result['dispatch']['groups'] if group['decision_agent_name'] == 'short-term-decider'
        )
        plan_id = short_group['plan_id']

        self.client.plan_response = {'items': [{'external_plan_id': plan_id, 'status': 'accepted'}]}
        self.client.order_response = {
            'items': [
                {
                    'id': 'order-1',
                    'execution_plan_item_id': 'exec-item-1',
                    'symbol': self.symbol,
                    'status': 'submitted',
                }
            ]
        }
        self.client.execution_response = {
            'items': [
                {
                    'broker_order_id': 'order-1',
                    'symbol': self.symbol,
                    'side': 'buy',
                    'quantity': '5',
                }
            ]
        }
        self.client.position_response = {
            'items': [{'symbol': self.symbol, 'quantity': '5', 'average_price': '50000'}]
        }

        overview = await self.service.list_signal_decider_exec_overview(
            target_trade_date=self.target_trade_date,
            symbol=self.symbol,
            refresh=True,
            trigger='test',
        )

        self.assertEqual(len(overview['items']), 1)
        self.assertEqual(overview['summary']['remote_open_count'], 1)
        self.assertEqual(overview['items'][0]['dispatch']['status'], 'accepted')
        self.assertEqual(overview['items'][0]['remote_state']['derived_status'], 'open')
        self.assertEqual(
            len(overview['items'][0]['dispatch']['item_links']),
            3,
        )
        self.assertEqual(overview['items'][0]['remote_state']['orders'], self.client.order_response['items'])

    async def test_dashboard_runtime_status_normalizes_exec_runtime_payload(self) -> None:
        self.client.runtime_response = {
            'mode': 'mock',
            'ready': True,
            'safety': {
                'paused': False,
                'kill_switch_enabled': False,
                'automation_enabled': True,
            },
            'counts': {
                'open_orders': 2,
                'active_execution_jobs': 3,
            },
            'monitoring': {
                'alive': True,
                'last_heartbeat_at': '2026-04-30T00:05:00+00:00',
            },
            'readiness': {
                'reasons': [],
            },
        }

        result = await self.service.dashboard_runtime_status(refresh=True)

        self.assertEqual(result['status'], 'ready')
        self.assertTrue(result['exec_connected'])
        self.assertTrue(result['automation_enabled'])
        self.assertTrue(result['monitoring_alive'])
        self.assertEqual(result['open_order_count'], 2)
        self.assertEqual(result['active_execution_job_count'], 3)
        self.assertEqual(result['last_heartbeat_at'], '2026-04-30T00:05:00+00:00')

    async def test_dashboard_runtime_status_surfaces_exec_offline_state(self) -> None:
        service = SignalDeciderExecIntegrationService(client=FailingRuntimeExecClient())

        result = await service.dashboard_runtime_status(refresh=True)

        self.assertEqual(result['status'], 'offline')
        self.assertFalse(result['exec_connected'])
        self.assertEqual(result['issues'][0]['code'], 'unavailable')

    async def test_dashboard_runtime_status_refresh_false_keeps_ready_config_healthy(self) -> None:
        result = await self.service.dashboard_runtime_status(refresh=False)

        self.assertEqual(result['status'], 'degraded')
        self.assertTrue(result['exec_connected'])
        self.assertFalse(result['monitoring_alive'])
        self.assertEqual(self.client.runtime_calls, 0)

    async def test_runtime_dashboard_views_merge_core_plan_and_exec_truth(self) -> None:
        dispatch_result = await self.service.dispatch_next_open_plan(
            target_trade_date=self.target_trade_date,
            refresh_plan=False,
            dry_run=False,
            force=True,
            trigger='test',
        )
        plan_id = dispatch_result['dispatch']['groups'][0]['plan_id']
        self.client.runtime_response = {
            'mode': 'mock',
            'ready': True,
            'safety': {
                'paused': False,
                'kill_switch_enabled': False,
                'automation_enabled': True,
            },
            'counts': {
                'open_orders': 1,
                'active_execution_jobs': 1,
            },
        }
        self.client.plan_response = {'items': [{'external_plan_id': plan_id, 'status': 'accepted'}]}
        self.client.order_response = {
            'items': [
                {
                    'id': 'order-1',
                    'execution_plan_item_id': 'exec-item-1',
                    'symbol': self.symbol,
                    'status': 'submitted',
                    'updated_at': '2026-04-30T00:05:00+00:00',
                }
            ]
        }
        self.client.execution_response = {
            'items': [
                {
                    'broker_order_id': 'order-1',
                    'symbol': self.symbol,
                    'side': 'buy',
                    'quantity': '5',
                    'fill_price': '50100',
                    'executed_at': '2026-04-30T00:06:00+00:00',
                }
            ]
        }
        self.client.position_response = {
            'items': [
                {
                    'symbol': self.symbol,
                    'quantity': '5',
                    'average_price': '50100',
                    'updated_at': '2026-04-30T00:07:00+00:00',
                }
            ]
        }

        board = await self.service.get_runtime_daily_board(
            market_date=self.target_trade_date,
            refresh=True,
            trigger='test',
        )
        monitored = await self.service.list_runtime_monitored_symbols(
            market_date=self.target_trade_date,
            refresh=True,
            trigger='test',
        )
        detail = await self.service.get_runtime_symbol_detail(
            market_date=self.target_trade_date,
            symbol=self.symbol,
            refresh=True,
            trigger='test',
        )

        self.assertEqual(board['runtime']['status'], 'ready')
        self.assertEqual(board['summary']['filled'], 1)
        self.assertEqual(board['items'][0]['status'], 'filled')
        self.assertEqual(board['items'][0]['monitoring_state'], 'watching_target_stop')
        self.assertEqual(board['items'][0]['filled_quantity'], 5)
        self.assertEqual(board['items'][0]['avg_fill_price'], 50100)
        self.assertEqual(board['items'][0]['execution_plan_id'], plan_id)

        self.assertEqual(monitored['summary']['items_total'], 1)
        self.assertEqual(monitored['items'][0]['symbol'], self.symbol)
        self.assertEqual(monitored['items'][0]['monitoring_state'], 'watching_target_stop')
        self.assertEqual(monitored['items'][0]['planned_intents'], ['stop_sell', 'target_sell'])

        self.assertEqual(detail['summary']['items_total'], 1)
        self.assertEqual(detail['monitoring']['state'], 'watching_target_stop')
        self.assertEqual(detail['items'][0]['status'], 'filled')
        self.assertEqual(detail['items'][0]['remote_state']['derived_status'], 'open')

    async def test_runtime_dashboard_views_classify_cancelled_orders_before_failed(self) -> None:
        dispatch_result = await self.service.dispatch_next_open_plan(
            target_trade_date=self.target_trade_date,
            refresh_plan=False,
            dry_run=False,
            force=True,
            trigger='test',
        )
        plan_id = dispatch_result['dispatch']['groups'][0]['plan_id']
        self.client.runtime_response = {
            'mode': 'mock',
            'ready': True,
            'safety': {
                'paused': False,
                'kill_switch_enabled': False,
                'automation_enabled': True,
            },
            'monitoring': {
                'alive': True,
            },
        }
        self.client.plan_response = {'items': [{'external_plan_id': plan_id, 'status': 'accepted'}]}
        self.client.execution_response = {'items': []}
        self.client.position_response = {'items': []}

        for remote_status in ('cancelled', 'canceled'):
            with self.subTest(remote_status=remote_status):
                self.client.order_response = {
                    'items': [
                        {
                            'id': 'order-1',
                            'execution_plan_item_id': 'exec-item-1',
                            'symbol': self.symbol,
                            'status': remote_status,
                            'updated_at': '2026-04-30T00:05:00+00:00',
                        }
                    ]
                }

                board = await self.service.get_runtime_daily_board(
                    market_date=self.target_trade_date,
                    refresh=True,
                    trigger='test',
                )
                detail = await self.service.get_runtime_symbol_detail(
                    market_date=self.target_trade_date,
                    symbol=self.symbol,
                    refresh=True,
                    trigger='test',
                )

                self.assertEqual(board['summary']['cancelled'], 1)
                self.assertEqual(board['summary']['failed'], 0)
                self.assertEqual(board['items'][0]['status'], 'cancelled')
                self.assertEqual(detail['items'][0]['status'], 'cancelled')
                self.assertEqual(detail['items'][0]['remote_state']['derived_status'], 'cancelled')


if __name__ == '__main__':
    unittest.main()
