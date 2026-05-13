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
    SignalDeciderPolicyVersion,
)
from app.market.clock import MarketState
from app.orders.next_open_execution import (
    EXECUTION_STATUS_DRY_RUN,
    EXECUTION_STATUS_SUBMITTED,
    ELIGIBILITY_BLOCKED,
    ELIGIBILITY_ELIGIBLE,
    SignalDeciderNextOpenExecutionService,
    _previous_trading_day,
)
from app.orders.store import upsert_broker_order, upsert_order_request


class FakeOrderService:
    def __init__(self) -> None:
        self.calls: list[dict] = []

    async def submit_buy_order(self, **kwargs):
        self.calls.append(kwargs)
        request_row = upsert_order_request(
            {
                'broker_name': 'kis',
                'client_order_id': kwargs['client_order_id'],
                'source': kwargs['source'],
                'account_reference': kwargs['account_reference'],
                'symbol': kwargs['symbol'],
                'side': 'buy',
                'order_type': kwargs['order_type'],
                'requested_quantity': kwargs['quantity'],
                'requested_price': None,
                'time_in_force': kwargs['time_in_force'],
                'strategy_context_payload': kwargs['strategy_context_payload'],
                'request_payload': {'from': 'fake-next-open-order-service'},
                'validation_status': 'valid',
                'submission_status': 'submitted',
                'submitted_at': '2026-04-30T00:00:05+00:00',
            }
        )
        order_row = upsert_broker_order(
            {
                'order_request_id': request_row['id'],
                'broker_name': 'kis',
                'broker_order_id': f"BROKER-{kwargs['symbol']}",
                'client_order_id': kwargs['client_order_id'],
                'account_reference': kwargs['account_reference'],
                'symbol': kwargs['symbol'],
                'side': 'buy',
                'order_type': kwargs['order_type'],
                'requested_quantity': kwargs['quantity'],
                'requested_price': None,
                'accepted_quantity': kwargs['quantity'],
                'accepted_price': None,
                'filled_quantity': '0',
                'remaining_quantity': kwargs['quantity'],
                'time_in_force': kwargs['time_in_force'],
                'current_status': 'submitted',
                'first_submitted_at': '2026-04-30T00:00:05+00:00',
                'last_broker_update_at': '2026-04-30T00:00:05+00:00',
                'raw_latest_payload': {'from': 'fake-next-open-order-service'},
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


class NextOpenExecutionServiceTests(unittest.IsolatedAsyncioTestCase):
    @classmethod
    def setUpClass(cls) -> None:
        AgentContextFeedback.__table__.create(bind=engine, checkfirst=True)
        OrderRequest.__table__.create(bind=engine, checkfirst=True)
        BrokerOrder.__table__.create(bind=engine, checkfirst=True)
        SignalDeciderDecisionSnapshot.__table__.create(bind=engine, checkfirst=True)
        SignalDeciderPolicyVersion.__table__.create(bind=engine, checkfirst=True)
        SignalDeciderExecutionPlanItem.__table__.create(bind=engine, checkfirst=True)

    def setUp(self) -> None:
        suffix = uuid4().hex[:8].upper()
        self.policy_version = f'signal_decider_policy_test_{suffix.lower()}'
        self.account_reference = f'KIS:12{suffix[:6]}-01'
        self.market_date = date(2026, 4, 29)
        self.target_trade_date = date(2026, 4, 30)
        self.original_settings = {
            'signal_decider_next_open_account_targets': settings.signal_decider_next_open_account_targets,
            'signal_decider_next_open_enabled': settings.signal_decider_next_open_enabled,
            'signal_decider_next_open_dry_run': settings.signal_decider_next_open_dry_run,
            'signal_decider_next_open_min_confidence_score_10': settings.signal_decider_next_open_min_confidence_score_10,
            'signal_decider_next_open_min_expected_return_pct': settings.signal_decider_next_open_min_expected_return_pct,
            'signal_decider_next_open_require_active_policy': settings.signal_decider_next_open_require_active_policy,
            'signal_decider_next_open_block_on_data_warnings': settings.signal_decider_next_open_block_on_data_warnings,
            'signal_decider_next_open_block_on_specialist_disagreement': settings.signal_decider_next_open_block_on_specialist_disagreement,
            'signal_decider_next_open_require_target_and_stop': settings.signal_decider_next_open_require_target_and_stop,
        }
        settings.signal_decider_next_open_account_targets = f'Main={self.account_reference}@1500000'
        settings.signal_decider_next_open_enabled = True
        settings.signal_decider_next_open_dry_run = False
        settings.signal_decider_next_open_min_confidence_score_10 = 6.0
        settings.signal_decider_next_open_min_expected_return_pct = 0.0
        settings.signal_decider_next_open_require_active_policy = True
        settings.signal_decider_next_open_block_on_data_warnings = True
        settings.signal_decider_next_open_block_on_specialist_disagreement = True
        settings.signal_decider_next_open_require_target_and_stop = True
        self._seed_policy_version()

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
            session.execute(
                delete(SignalDeciderDecisionSnapshot).where(
                    SignalDeciderDecisionSnapshot.agent_name == 'short-term-decider',
                    SignalDeciderDecisionSnapshot.market_date == self.market_date,
                )
            )
            session.execute(
                delete(SignalDeciderPolicyVersion).where(
                    SignalDeciderPolicyVersion.policy_version == self.policy_version
                )
            )

    def _seed_policy_version(self) -> None:
        with session_scope() as session:
            session.add(
                SignalDeciderPolicyVersion(
                    policy_version=self.policy_version,
                    status='active',
                    change_summary='Test active policy.',
                    rule_bundle_payload={'schema_version': 'signal_decider_policy_bundle_v1'},
                    metadata_payload={
                        'schema_version': 'signal_decider_policy_metadata_v1',
                        'activation_mode': 'manual_only',
                        'candidate_activation_required': True,
                    },
                    created_by='test',
                    activated_by='test',
                    activated_at=datetime(2026, 4, 29, 0, 0, tzinfo=timezone.utc),
                )
            )

    def _seed_snapshot(
        self,
        *,
        symbol: str,
        next_day_open_action: str = 'buy',
        data_warnings: list[str] | None = None,
    ) -> int:
        with session_scope() as session:
            row = SignalDeciderDecisionSnapshot(
                feedback_id=None,
                market_date=self.market_date,
                symbol=symbol,
                agent_name='signal-decider',
                agent_version='test-agent',
                decision_run_id=f'run-{symbol}',
                feedback_received_at=datetime(2026, 4, 29, 6, 0, tzinfo=timezone.utc),
                policy_version=self.policy_version,
                decision_action=next_day_open_action,
                preferred_action=next_day_open_action,
                next_day_open_action=next_day_open_action,
                entry_price_basis='next_open',
                target_price=54000,
                expected_return_pct=4.5,
                stop_loss_price=47000,
                suggested_holding_period='3-10 trading days',
                confidence_score_10=7.5,
                conviction_score_10=6.2,
                thesis_summary=f'{symbol} summary',
                supporting_evidence_payload=['Breadth improved.'],
                decision_risks_payload=['Leadership remained narrow.'],
                invalidation_conditions_payload=['Close below support.'],
                data_quality_warnings_payload=data_warnings or [],
                specialist_disagreement_payload=[],
                traceability_payload={
                    'schema_version': 'signal_decider_trace_v2',
                    'gate_checks': {
                        'missing_upstream_roles': [],
                        'decision_support_only': True,
                        'allow_execution_logic': False,
                    },
                },
                input_feature_snapshot_payload={
                    'technical': {'trend_state': 'uptrend'},
                    'meta': {'supporting_evidence_count': 1},
                },
                request_payload={'signal_decider_policy': {'current_version': self.policy_version}},
                response_payload={'next_day_open_action': next_day_open_action},
                response_summary_payload={'policy_version': self.policy_version},
            )
            session.add(row)
            session.flush()
            return row.id

    def _seed_short_term_snapshot(
        self,
        *,
        symbol: str,
        tactical_action: str = 'watch_for_trigger',
        trigger_price: int | None = 50500,
        chase_limit_price: int | None = 51500,
        target_price: int | None = 54000,
        stop_loss_price: int | None = 49000,
        data_warnings: list[str] | None = None,
    ) -> int:
        generic_action = 'buy' if tactical_action in {'scalp_buy_candidate', 'watch_for_trigger'} else tactical_action
        if generic_action not in {'buy', 'wait', 'reject'}:
            generic_action = 'wait'
        with session_scope() as session:
            row = SignalDeciderDecisionSnapshot(
                feedback_id=None,
                market_date=self.market_date,
                symbol=symbol,
                agent_name='short-term-decider',
                agent_version='test-agent',
                decision_run_id=f'short-run-{symbol}',
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
                confidence_score_10=7.1,
                conviction_score_10=6.4,
                thesis_summary=f'{symbol} short-term summary',
                supporting_evidence_payload=['Momentum stayed constructive.'],
                decision_risks_payload=['Opening range may stay volatile.'],
                invalidation_conditions_payload=['Trigger reclaim failed.'],
                data_quality_warnings_payload=data_warnings or [],
                specialist_disagreement_payload=[],
                traceability_payload={
                    'schema_version': 'short_term_decider_trace_v1',
                    'gate_checks': {
                        'missing_upstream_roles': [],
                        'decision_support_only': True,
                        'allow_execution_logic': False,
                    },
                    'tactical_context': {
                        'tactical_action': tactical_action,
                        'trigger_price': trigger_price,
                        'chase_limit_price': chase_limit_price,
                    },
                },
                input_feature_snapshot_payload={'meta': {'source_agent_name': 'short-term-decider'}},
                request_payload={'orchestration': {'required_upstream_roles': ['technical-summarizer']}},
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
                    'confidence_score_10': 7.1,
                    'conviction_score_10': 6.4,
                    'supporting_evidence': ['Momentum stayed constructive.'],
                    'tactical_risks': ['Opening range may stay volatile.'],
                    'invalidation_conditions': ['Trigger reclaim failed.'],
                    'data_quality_warnings': data_warnings or [],
                },
                response_summary_payload={'model': 'openclaw/short-term-decider'},
            )
            session.add(row)
            session.flush()
            return row.id

    def _service(self, *, order_service=None, state='OPEN', server_time='2026-04-30T09:01:00+09:00'):
        return SignalDeciderNextOpenExecutionService(
            order_service=order_service,
            market_state_getter=lambda: MarketState(
                market='KRX',
                state=state,
                is_trading_day=True,
                is_open=state == 'OPEN',
                server_time=server_time,
                session_open_at='2026-04-30T09:00:00+09:00',
                session_close_at='2026-04-30T15:30:00+09:00',
            ),
        )

    def test_previous_trading_day_uses_exchange_calendar(self) -> None:
        self.assertEqual(_previous_trading_day(date(2026, 1, 2)), date(2025, 12, 30))

    def test_refresh_plan_items_persists_eligible_and_blocked_buy_candidates(self) -> None:
        eligible_snapshot_id = self._seed_snapshot(symbol='005930')
        blocked_snapshot_id = self._seed_snapshot(
            symbol='000660',
            data_warnings=['Missing final rank page.'],
        )
        self._seed_snapshot(symbol='035420', next_day_open_action='wait')
        service = self._service(state='PREOPEN', server_time='2026-04-30T08:40:00+09:00')

        def fake_bars(symbol: str, **_kwargs):
            if symbol == '005930':
                return [{'trade_date': '2026-04-29', 'close_price': 50000}]
            if symbol == '000660':
                return [{'trade_date': '2026-04-29', 'close_price': 250000}]
            return []

        with patch('app.orders.next_open_execution.load_daily_bars_for_symbol_range', side_effect=fake_bars):
            result = service.refresh_plan_items(target_trade_date=self.target_trade_date, trigger='test')

        self.assertEqual(result['summary']['items_total'], 2)
        self.assertEqual(result['summary']['eligible_count'], 1)
        self.assertEqual(result['summary']['blocked_count'], 1)
        items_by_symbol = {item['symbol']: item for item in result['items']}
        self.assertEqual(items_by_symbol['005930']['decision_snapshot_id'], eligible_snapshot_id)
        self.assertEqual(items_by_symbol['005930']['eligibility_status'], ELIGIBILITY_ELIGIBLE)
        self.assertEqual(items_by_symbol['005930']['planned_quantity'], '30.00000000')
        self.assertEqual(items_by_symbol['000660']['decision_snapshot_id'], blocked_snapshot_id)
        self.assertEqual(items_by_symbol['000660']['eligibility_status'], ELIGIBILITY_BLOCKED)
        self.assertEqual(
            items_by_symbol['000660']['validation']['blocking_reasons'][0]['code'],
            'data_quality_warnings_present',
        )
        self.assertNotIn('035420', items_by_symbol)

    def test_refresh_plan_items_reuses_cached_policy_context_by_policy_version(self) -> None:
        self._seed_snapshot(symbol='005930')
        self._seed_snapshot(symbol='000660')
        service = self._service(state='PREOPEN', server_time='2026-04-30T08:40:00+09:00')

        def fake_bars(symbol: str, **_kwargs):
            if symbol == '005930':
                return [{'trade_date': '2026-04-29', 'close_price': 50000}]
            if symbol == '000660':
                return [{'trade_date': '2026-04-29', 'close_price': 250000}]
            return []

        with patch(
            'app.orders.next_open_execution.signal_decider_loop_service.get_policy_prompt_context',
            return_value={'status': 'active', 'activation_mode': 'manual_only'},
        ) as get_policy_prompt_context:
            with patch('app.orders.next_open_execution.load_daily_bars_for_symbol_range', side_effect=fake_bars):
                service.refresh_plan_items(target_trade_date=self.target_trade_date, trigger='test')

        get_policy_prompt_context.assert_called_once_with(policy_version=self.policy_version)

    def test_refresh_plan_items_keeps_same_symbol_different_deciders_separate(self) -> None:
        signal_snapshot_id = self._seed_snapshot(symbol='005930')
        short_term_snapshot_id = self._seed_short_term_snapshot(symbol='005930')
        service = self._service(state='PREOPEN', server_time='2026-04-30T08:40:00+09:00')

        with patch(
            'app.orders.next_open_execution.load_daily_bars_for_symbol_range',
            return_value=[{'trade_date': '2026-04-29', 'close_price': 50000}],
        ):
            result = service.refresh_plan_items(target_trade_date=self.target_trade_date, trigger='test')

        self.assertEqual(result['summary']['items_total'], 2)
        self.assertEqual(result['summary']['decision_agent_counts']['short-term-decider'], 1)
        self.assertEqual(result['summary']['decision_agent_counts']['signal-decider'], 1)
        items = [item for item in result['items'] if item['symbol'] == '005930']
        self.assertEqual(len(items), 2)
        by_agent = {item['decision_agent_name']: item for item in items}
        self.assertEqual(by_agent['signal-decider']['decision_snapshot_id'], signal_snapshot_id)
        self.assertEqual(by_agent['signal-decider']['client_order_id'], 'SDNO-20260430-005930')
        self.assertEqual(by_agent['short-term-decider']['decision_snapshot_id'], short_term_snapshot_id)
        self.assertEqual(by_agent['short-term-decider']['client_order_id'], 'STDE-20260430-005930')
        self.assertEqual(by_agent['short-term-decider']['eligibility_status'], ELIGIBILITY_ELIGIBLE)

    def test_refresh_plan_items_short_term_rows_created_without_account_targets(self) -> None:
        self._seed_snapshot(symbol='005930')
        self._seed_short_term_snapshot(symbol='005930')
        settings.signal_decider_next_open_account_targets = ''
        service = self._service(state='PREOPEN', server_time='2026-04-30T08:40:00+09:00')

        with patch(
            'app.orders.next_open_execution.load_daily_bars_for_symbol_range',
            return_value=[{'trade_date': '2026-04-29', 'close_price': 50000}],
        ):
            result = service.refresh_plan_items(target_trade_date=self.target_trade_date, trigger='test')

        self.assertEqual(result['summary']['items_total'], 1)
        self.assertEqual(result['summary']['eligible_count'], 1)
        self.assertEqual(result['summary']['blocked_count'], 0)
        item = result['items'][0]
        self.assertEqual(item['decision_agent_name'], 'short-term-decider')
        self.assertEqual(item['account_reference'], 'KIS:00000000-00')
        self.assertEqual(item['account_label'], 'UNSPECIFIED_ROUTE')
        self.assertEqual(item['traceability']['allocation_mode'], 'unspecified_route')
        self.assertEqual(item['traceability']['allocation_budget_krw'], '0')
        self.assertIsNone(item['planned_quantity'])
        self.assertEqual(item['eligibility_status'], ELIGIBILITY_ELIGIBLE)

    def test_refresh_plan_items_skips_non_actionable_short_term_decisions(self) -> None:
        self._seed_short_term_snapshot(symbol='035420', tactical_action='wait')
        service = self._service(state='PREOPEN', server_time='2026-04-30T08:40:00+09:00')

        with patch(
            'app.orders.next_open_execution.load_daily_bars_for_symbol_range',
            return_value=[{'trade_date': '2026-04-29', 'close_price': 50000}],
        ):
            result = service.refresh_plan_items(target_trade_date=self.target_trade_date, trigger='test')

        self.assertNotIn('035420', {item['symbol'] for item in result['items']})

    def test_refresh_plan_items_blocks_short_term_rows_missing_trigger_fields(self) -> None:
        self._seed_short_term_snapshot(
            symbol='000660',
            tactical_action='watch_for_trigger',
            trigger_price=None,
            chase_limit_price=255000,
        )
        service = self._service(state='PREOPEN', server_time='2026-04-30T08:40:00+09:00')

        with patch(
            'app.orders.next_open_execution.load_daily_bars_for_symbol_range',
            return_value=[{'trade_date': '2026-04-29', 'close_price': 250000}],
        ):
            result = service.refresh_plan_items(target_trade_date=self.target_trade_date, trigger='test')

        item = next(item for item in result['items'] if item['symbol'] == '000660')
        self.assertEqual(item['decision_agent_name'], 'short-term-decider')
        self.assertEqual(item['eligibility_status'], ELIGIBILITY_BLOCKED)
        self.assertEqual(item['validation']['blocking_reasons'][0]['code'], 'trigger_price_missing')

    def test_refresh_plan_items_short_term_missing_target_and_stop_are_warnings(self) -> None:
        self._seed_short_term_snapshot(
            symbol='000660',
            tactical_action='watch_for_trigger',
            target_price=None,
            stop_loss_price=None,
        )
        service = self._service(state='PREOPEN', server_time='2026-04-30T08:40:00+09:00')

        with patch(
            'app.orders.next_open_execution.load_daily_bars_for_symbol_range',
            return_value=[{'trade_date': '2026-04-29', 'close_price': 250000}],
        ):
            result = service.refresh_plan_items(target_trade_date=self.target_trade_date, trigger='test')

        item = next(item for item in result['items'] if item['symbol'] == '000660')
        self.assertEqual(item['decision_agent_name'], 'short-term-decider')
        self.assertEqual(item['eligibility_status'], ELIGIBILITY_ELIGIBLE)
        warning_codes = {warning['code'] for warning in item['validation']['warnings']}
        self.assertIn('target_price_missing', warning_codes)
        self.assertIn('stop_loss_price_missing', warning_codes)

    async def test_run_market_open_execution_submits_and_prevents_duplicate_replays(self) -> None:
        self._seed_snapshot(symbol='005930')
        fake_order_service = FakeOrderService()
        service = self._service(order_service=fake_order_service)

        with patch(
            'app.orders.next_open_execution.load_daily_bars_for_symbol_range',
            return_value=[{'trade_date': '2026-04-29', 'close_price': 50000}],
        ):
            service.refresh_plan_items(target_trade_date=self.target_trade_date, trigger='test')
            first_result = await service.run_market_open_execution_if_due(
                target_trade_date=self.target_trade_date,
                trigger='test',
            )
            second_result = await service.run_market_open_execution_if_due(
                target_trade_date=self.target_trade_date,
                trigger='test',
            )

        self.assertEqual(first_result['summary']['selected_count'], 1)
        self.assertEqual(first_result['summary']['submitted_count'], 1)
        self.assertEqual(len(fake_order_service.calls), 1)
        call = fake_order_service.calls[0]
        self.assertEqual(call['source'], 'signal_decider_next_open')
        self.assertEqual(call['order_type'], 'market')
        self.assertEqual(call['strategy_context_payload']['signal_decider_next_open']['target_trade_date'], '2026-04-30')
        self.assertEqual(call['strategy_context_payload']['signal_decider_next_open']['target_price'], 54000)
        self.assertEqual(call['strategy_context_payload']['signal_decider_next_open']['stop_loss_price'], 47000)
        item = first_result['items'][0]
        self.assertEqual(item['execution_status'], EXECUTION_STATUS_SUBMITTED)
        self.assertIsNotNone(item['order_request_id'])
        self.assertIsNotNone(item['broker_order_row_id'])
        self.assertEqual(second_result['summary']['selected_count'], 0)
        self.assertEqual(len(fake_order_service.calls), 1)

    async def test_run_market_open_execution_dry_run_marks_rows_without_submitting(self) -> None:
        settings.signal_decider_next_open_enabled = False
        settings.signal_decider_next_open_dry_run = True
        self._seed_snapshot(symbol='005930')
        fake_order_service = FakeOrderService()
        service = self._service(order_service=fake_order_service)

        with patch(
            'app.orders.next_open_execution.load_daily_bars_for_symbol_range',
            return_value=[{'trade_date': '2026-04-29', 'close_price': 50000}],
        ):
            service.refresh_plan_items(target_trade_date=self.target_trade_date, trigger='test')
            result = await service.run_market_open_execution_if_due(
                target_trade_date=self.target_trade_date,
                trigger='test',
            )

        self.assertEqual(result['summary']['dry_run_count'], 1)
        self.assertEqual(len(fake_order_service.calls), 0)
        self.assertEqual(result['items'][0]['execution_status'], EXECUTION_STATUS_DRY_RUN)
        self.assertEqual(result['items'][0]['submission']['mode'], 'dry_run')

    async def test_refresh_plan_items_does_not_mutate_submitted_rows(self) -> None:
        original_snapshot_id = self._seed_snapshot(symbol='005930')
        fake_order_service = FakeOrderService()
        service = self._service(order_service=fake_order_service)

        with patch(
            'app.orders.next_open_execution.load_daily_bars_for_symbol_range',
            return_value=[{'trade_date': '2026-04-29', 'close_price': 50000}],
        ):
            service.refresh_plan_items(target_trade_date=self.target_trade_date, trigger='test')
            first_result = await service.run_market_open_execution_if_due(
                target_trade_date=self.target_trade_date,
                trigger='test',
            )

        submitted_item = first_result['items'][0]
        replacement_snapshot_id = self._seed_snapshot(symbol='005930')
        self.assertNotEqual(replacement_snapshot_id, original_snapshot_id)

        with patch(
            'app.orders.next_open_execution.load_daily_bars_for_symbol_range',
            return_value=[{'trade_date': '2026-04-29', 'close_price': 51000}],
        ):
            refreshed = service.refresh_plan_items(target_trade_date=self.target_trade_date, trigger='test_refresh')

        refreshed_item = next(item for item in refreshed['items'] if item['symbol'] == '005930')
        self.assertEqual(refreshed_item['execution_status'], EXECUTION_STATUS_SUBMITTED)
        self.assertEqual(refreshed_item['decision_snapshot_id'], submitted_item['decision_snapshot_id'])
        self.assertEqual(refreshed_item['traceability'], submitted_item['traceability'])
        self.assertEqual(refreshed_item['validation'], submitted_item['validation'])
        self.assertEqual(refreshed_item['estimated_reference_price'], submitted_item['estimated_reference_price'])
        self.assertEqual(refreshed_item['planned_quantity'], submitted_item['planned_quantity'])
        self.assertEqual(refreshed_item['submitted_at'], submitted_item['submitted_at'])
