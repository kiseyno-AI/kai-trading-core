import asyncio
from datetime import date, datetime, timezone
from decimal import Decimal
import json
import unittest
from unittest.mock import AsyncMock, patch
from uuid import uuid4

from sqlalchemy import delete, select

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
from app.market.session import (
    KisKrxTradingCalendarProvider,
    reset_krx_calendar_provider,
    set_krx_calendar_provider,
)
from app.orders.exec_integration import (
    EXEC_ENTRY_BUY_INTENTS,
    EXEC_INTENT_INTRADAY_WATCH_BUY,
    EXEC_INTENT_NEXT_OPEN_BUY,
    EXEC_INTENT_STOP_SELL,
    EXEC_INTENT_TARGET_SELL,
    SignalDeciderExecClientError,
    SignalDeciderExecIntegrationService,
    _build_market_regime_sizing_policy,
)
from app.orders.exec_payload_slimming import (
    SIGNAL_DECIDER_EXEC_SUBMISSION_MAX_BYTES,
    cleanup_signal_decider_exec_submission_payloads,
    json_size_bytes,
)
from app.orders.next_open_execution import (
    EXECUTION_STATUS_DELEGATED_FOR_EXEC_AGENT_SIZING,
    EXECUTION_STATUS_SUBMIT_FAILED,
    EXECUTION_STATUS_SUBMITTED,
    _short_term_entry_quality_blocks,
    signal_decider_next_open_execution_service,
)


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
                item.get('intent') in EXEC_ENTRY_BUY_INTENTS
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


def _accepted_plan_readback_for_payload(payload: dict) -> dict:
    return {
        'items': [
            {
                'external_plan_id': payload['plan_id'],
                'status': 'accepted',
                'items': [
                    {
                        'core_item_key': item['core_item_key'],
                        'execution_plan_id': payload['plan_id'],
                        'execution_plan_item_id': f"exec-item-{index}",
                        'symbol': item['symbol'],
                        'intent': item['intent'],
                        'status': 'accepted',
                    }
                    for index, item in enumerate(payload['items'], start=1)
                ],
            }
        ]
    }


class TimeoutAfterAcceptExecClient(FakeExecClient):
    async def post_execution_plan(self, payload: dict) -> dict:
        self.post_calls.append(payload)
        self.plan_response = _accepted_plan_readback_for_payload(payload)
        raise SignalDeciderExecClientError(
            'exec request timed out',
            code='unavailable',
            retryable=True,
        )


class DuplicateConflictAfterAcceptExecClient(FakeExecClient):
    async def post_execution_plan(self, payload: dict) -> dict:
        self.post_calls.append(payload)
        self.plan_response = _accepted_plan_readback_for_payload(payload)
        raise SignalDeciderExecClientError(
            'exec plan already exists',
            code='failed',
            retryable=False,
            status_code=409,
        )


class TimeoutWithStaleSamePlanReadbackExecClient(FakeExecClient):
    async def post_execution_plan(self, payload: dict) -> dict:
        self.post_calls.append(payload)
        stale_items = []
        for index, item in enumerate(payload['items'], start=1):
            stale_item = {
                'execution_plan_id': payload['plan_id'],
                'execution_plan_item_id': f"stale-exec-item-{index}",
                'symbol': item['symbol'],
                'intent': item['intent'],
                'status': 'accepted',
            }
            if index % 2 == 0:
                stale_item['core_item_key'] = f"stale-{item['core_item_key']}"
            stale_items.append(stale_item)
        self.plan_response = {
            'items': [
                {
                    'external_plan_id': payload['plan_id'],
                    'status': 'accepted',
                    'items': stale_items,
                }
            ]
        }
        raise SignalDeciderExecClientError(
            'exec request timed out',
            code='unavailable',
            retryable=True,
        )


class FakeCalendarClient:
    def __init__(self, rows: list[dict]) -> None:
        self.rows = rows
        self.calls: list[dict] = []

    async def fetch_krx_holiday_calendar(self, **kwargs):
        self.calls.append(kwargs)
        return {'output': list(self.rows)}


class FailingCalendarClient:
    async def fetch_krx_holiday_calendar(self, **_kwargs):
        raise RuntimeError('kis calendar unavailable')


class MarketRegimeSizingPolicyTests(unittest.TestCase):
    def test_preserves_zero_quantity_multiplier(self) -> None:
        for recommended_sizing_multiplier in (0, 1):
            with self.subTest(recommended_sizing_multiplier=recommended_sizing_multiplier):
                policy = _build_market_regime_sizing_policy(
                    {
                        'market_regime': 'defensive',
                        'quantity_multiplier': 0,
                        'recommended_sizing_multiplier': recommended_sizing_multiplier,
                    }
                )

                self.assertEqual(policy['quantity_multiplier'], 0)
                self.assertEqual(policy['raw_quantity_multiplier'], 0)

    def test_preserves_zero_recommended_sizing_multiplier_when_quantity_missing(self) -> None:
        policy = _build_market_regime_sizing_policy(
            {
                'market_regime': 'defensive',
                'recommended_sizing_multiplier': 0,
            }
        )

        self.assertEqual(policy['quantity_multiplier'], 0)
        self.assertEqual(policy['raw_quantity_multiplier'], 0)

    def test_defaults_missing_or_none_multiplier_hints_to_one(self) -> None:
        for context in (
            {'market_regime': 'normal'},
            {
                'market_regime': 'normal',
                'quantity_multiplier': None,
                'recommended_sizing_multiplier': None,
            },
        ):
            with self.subTest(context=context):
                policy = _build_market_regime_sizing_policy(context)

                self.assertEqual(policy['quantity_multiplier'], 1)
                self.assertEqual(policy['raw_quantity_multiplier'], 1)

    def test_caps_aggressive_multiplier_and_carries_window_and_gate_requirements(self) -> None:
        policy = _build_market_regime_sizing_policy(
            {
                'schema_version': 'market_regime_context_v1',
                'market_regime': 'aggressive_open',
                'regime_confidence': 0.84,
                'regime_reasons': ['breadth_strong', 'early_continuation_confirmed'],
                'quantity_multiplier': 3.5,
                'entry_priority_boost': True,
                'valid_until': '2026-04-30T09:20:00+09:00',
                'sizing_window': {
                    'name': 'aggressive_open',
                    'timezone': 'Asia/Seoul',
                    'valid_from': '2026-04-30T09:00:00+09:00',
                    'valid_until': '2026-04-30T09:20:00+09:00',
                    'quantity_multiplier_cap': 2.0,
                    'entry_priority_boost': True,
                    'exec_enforced': True,
                },
            }
        )

        self.assertEqual(policy['market_regime'], 'aggressive_open')
        self.assertEqual(policy['quantity_multiplier'], 2)
        self.assertEqual(policy['raw_quantity_multiplier'], 3.5)
        self.assertEqual(policy['quantity_multiplier_cap'], 2)
        self.assertEqual(policy['valid_until'], '2026-04-30T09:20:00+09:00')
        self.assertEqual(policy['sizing_window']['valid_from'], '2026-04-30T09:00:00+09:00')
        self.assertTrue(policy['entry_priority_boost'])
        self.assertTrue(policy['exec_must_enforce_window'])
        self.assertFalse(policy['core_bypasses_safety_gates'])
        self.assertEqual(
            set(policy['required_exec_gates']),
            {'kill_switch', 'account_drawdown_gate', 'symbol_caps', 'max_notional_caps', 'stop_loss_required'},
        )


class ExecIntegrationDashboardCacheTests(unittest.IsolatedAsyncioTestCase):
    async def test_dashboard_payload_build_is_singleflight_per_cache_key(self) -> None:
        service = SignalDeciderExecIntegrationService(client=FakeExecClient())
        market_date = date(2026, 8, 5)
        refresh_state = {'requested': False, 'status': 'idle'}
        build_calls = 0

        async def build() -> dict:
            nonlocal build_calls
            build_calls += 1
            await asyncio.sleep(0.01)
            payload = {
                'request': {'market_date': market_date.isoformat(), 'refresh': False},
                'freshness': {'status': 'fresh', 'is_stale': False, 'is_partial': False},
                'refresh': refresh_state,
                'items': [{'symbol': '005930'}],
            }
            service._store_dashboard_cached_payload(
                endpoint='monitored_symbols',
                market_date=market_date,
                symbol=None,
                payload=payload,
            )
            return payload

        first, second = await asyncio.gather(
            service._build_dashboard_payload_singleflight(
                endpoint='monitored_symbols',
                market_date=market_date,
                symbol=None,
                refresh_state=refresh_state,
                build=build,
            ),
            service._build_dashboard_payload_singleflight(
                endpoint='monitored_symbols',
                market_date=market_date,
                symbol=None,
                refresh_state=refresh_state,
                build=build,
            ),
        )

        self.assertEqual(build_calls, 1)
        self.assertEqual(first['items'], [{'symbol': '005930'}])
        self.assertEqual(second['freshness']['served_from_cache'], True)

    async def test_historical_daily_board_ignores_unscoped_current_runtime_activity(self) -> None:
        service = SignalDeciderExecIntegrationService(client=FakeExecClient())
        overview = {
            'integration': {
                'mode': 'dispatch',
                'enabled': True,
                'delegate_execution': True,
                'runtime_mode': 'live',
                'issues': [],
            },
            'runtime': {
                'status': 'ok',
                'payload': {
                    'mode': 'live',
                    'ready': True,
                    'counts': {'open_orders': 1, 'active_execution_jobs': 0},
                },
                'error': None,
            },
            'items': [],
        }

        with (
            patch.object(
                service,
                '_load_dashboard_read_model_overview',
                new=AsyncMock(return_value=overview),
            ),
            patch(
                'app.orders.exec_integration._utcnow',
                return_value=datetime(2026, 8, 29, 1, 0, tzinfo=timezone.utc),
            ),
        ):
            board = await service.get_runtime_daily_board(
                market_date=date(2026, 8, 28),
                refresh=False,
                trigger='test',
            )

        self.assertEqual(board['items'], [])
        self.assertEqual(board['activity_coverage']['status'], 'unavailable')
        self.assertEqual(
            board['activity_coverage']['status_reason'],
            'runtime_read_model_unavailable',
        )
        self.assertIsNone(board['activity_coverage']['exec_activity_detected'])
        self.assertEqual(board['freshness']['status'], 'unavailable')


class ExecRuntimeReadbackNormalizationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.service = SignalDeciderExecIntegrationService(client=FakeExecClient())
        self.integration = {
            'mode': 'dispatch',
            'enabled': True,
            'delegate_execution': True,
            'runtime_mode': 'live',
            'issues': [],
        }

    @staticmethod
    def _current_exec_payload() -> dict:
        return {
            'mode': 'live',
            'ready': True,
            'safety': {
                'paused': False,
                'kill_switch_enabled': False,
                'automation_enabled': True,
            },
            'liveness': {
                'monitoring': {
                    'status': 'degraded',
                    'last_heartbeat_at': '2026-08-29T00:06:53+00:00',
                },
                'autonomous': {
                    'enabled': True,
                    'monitoring': {
                        'worker_name': 'monitoring_dispatch_loop',
                        'status': 'running',
                        'poll_interval_seconds': 5,
                        'last_heartbeat_at': '2026-08-29T00:06:53+00:00',
                        'last_success_at': '2026-08-29T00:06:53+00:00',
                    },
                }
            },
            'readiness': {'reasons': []},
        }

    def test_normalizes_current_exec_autonomous_monitoring_liveness(self) -> None:
        result = self.service._build_dashboard_runtime_status(
            integration=self.integration,
            runtime={'status': 'ok', 'payload': self._current_exec_payload(), 'error': None},
        )

        self.assertEqual(result['status'], 'ready')
        self.assertTrue(result['monitoring_alive'])
        self.assertEqual(result['last_heartbeat_at'], '2026-08-29T00:06:53+00:00')

    def test_exposes_missing_pnl_coverage_reasons(self) -> None:
        result = self.service._build_dashboard_runtime_status(
            integration=self.integration,
            runtime={'status': 'ok', 'payload': self._current_exec_payload(), 'error': None},
        )

        self.assertIsNone(result['daily_pnl'])
        self.assertIsNone(result['weekly_pnl'])
        self.assertIsNone(result['monthly_pnl'])
        self.assertEqual(
            result['pnl_coverage'],
            {
                period: {
                    'status': 'unavailable',
                    'status_reason': 'not_provided_by_exec',
                    'source': 'exec_runtime_status',
                }
                for period in ('daily', 'weekly', 'monthly')
            },
        )

    def test_exposes_upstream_pnl_not_computable_reason(self) -> None:
        payload = self._current_exec_payload()
        payload['daily_pnl'] = {
            'daily_realized_pnl': None,
            'status_reason': 'fill_coverage_incomplete',
        }

        result = self.service._build_dashboard_runtime_status(
            integration=self.integration,
            runtime={'status': 'ok', 'payload': payload, 'error': None},
        )

        self.assertEqual(
            result['pnl_coverage']['daily'],
            {
                'status': 'unavailable',
                'status_reason': 'fill_coverage_incomplete',
                'source': 'exec_runtime_status',
            },
        )

    def test_exec_runtime_activity_is_unknown_when_activity_summaries_are_omitted(self) -> None:
        runtime = {
            'status': 'ok',
            'payload': self._current_exec_payload(),
            'error': None,
        }

        result = self.service._exec_runtime_activity_detected(runtime=runtime)

        self.assertIsNone(result)

    def test_daily_board_distinguishes_no_activity_from_unavailable_exec_activity(self) -> None:
        refresh_state = {
            'requested': False,
            'status': 'not_requested',
            'source': 'exec_readback',
        }
        active_payload = self._current_exec_payload()
        active_payload['symbols'] = {'filled_today': ['005930']}
        no_activity_payload = self._current_exec_payload()
        no_activity_payload['symbols'] = {
            'monitored': [],
            'scheduled_today': [],
            'filled_today': [],
            'failed_today': [],
            'positions': [],
        }
        no_activity_payload['counts'] = {'open_orders': 0, 'active_execution_jobs': 0}

        def build(payload: dict | None) -> dict:
            return self.service._build_runtime_daily_board_payload(
                market_date=date(2026, 8, 29),
                refresh=False,
                overview={
                    'integration': self.integration,
                    'runtime': {'status': 'ok', 'payload': payload, 'error': None},
                    'items': [],
                },
                refresh_state=refresh_state,
                served_from_cache=False,
            )

        with patch(
            'app.orders.exec_integration._utcnow',
            return_value=datetime(2026, 8, 29, 1, 0, tzinfo=timezone.utc),
        ):
            unavailable = build(active_payload)
            no_activity = build(no_activity_payload)
            unloaded = build(None)

        self.assertEqual(unavailable['items'], [])
        self.assertEqual(unavailable['activity_coverage']['status'], 'unavailable')
        self.assertEqual(unavailable['activity_coverage']['status_reason'], 'exec_activity_not_materialized')
        self.assertTrue(unavailable['activity_coverage']['exec_activity_detected'])
        self.assertEqual(unavailable['freshness']['status'], 'unavailable')
        self.assertEqual(no_activity['items'], [])
        self.assertEqual(no_activity['activity_coverage']['status'], 'no_activity')
        self.assertEqual(no_activity['activity_coverage']['status_reason'], 'no_exec_activity')
        self.assertFalse(no_activity['activity_coverage']['exec_activity_detected'])
        self.assertEqual(no_activity['freshness']['status'], 'fresh')
        self.assertEqual(unloaded['activity_coverage']['status'], 'unavailable')
        self.assertEqual(unloaded['activity_coverage']['status_reason'], 'runtime_read_model_unavailable')
        self.assertIsNone(unloaded['activity_coverage']['exec_activity_detected'])
        self.assertEqual(unloaded['freshness']['status'], 'unavailable')


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
        self.symbol = f'T{suffix[:5]}'
        self.seeded_symbols = {self.symbol}
        self.main_client_order_id = f'SDNO-20260430-{self.symbol}'
        self.short_term_client_order_id = f'STDE-20260430-{self.symbol}'
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
        self.enrichment_preflight_patcher = patch(
            'app.orders.next_open_execution.inspect_pykrx_enrichment_preflight',
            side_effect=lambda **kwargs: {
                'market_date': str(kwargs.get('market_date') or self.market_date),
                'status': 'ready',
                'allowed': True,
                'required_categories': ['sector_classification', 'investor_flow_symbol', 'short_selling'],
                'failed_categories': [],
                'zero_coverage_categories': [],
                'category_row_counts': {
                    'sector_classification': 1,
                    'investor_flow_symbol': 1,
                    'short_selling': 1,
                },
            },
        )
        self.enrichment_preflight_mock = self.enrichment_preflight_patcher.start()
        self.addCleanup(self.enrichment_preflight_patcher.stop)
        self.calendar_client = FakeCalendarClient(
            [
                {'bass_dt': '20260429', 'opnd_yn': 'Y'},
                {'bass_dt': '20260430', 'opnd_yn': 'Y'},
            ]
        )
        set_krx_calendar_provider(KisKrxTradingCalendarProvider(client=self.calendar_client))
        self.addCleanup(reset_krx_calendar_provider)
        self._seed_symbol()
        self.snapshot_id = self._seed_snapshot()
        self.plan_item_id = self._seed_plan_item()

    def tearDown(self) -> None:
        reset_krx_calendar_provider()
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
            session.execute(delete(Symbol).where(Symbol.symbol.in_(self.seeded_symbols)))

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
                request_payload={
                    'signal_decider_policy': {'current_version': self.policy_version},
                    'market_regime_context': self._market_regime_context(),
                },
                response_payload={'next_day_open_action': 'buy'},
                response_summary_payload={'policy_version': self.policy_version},
            )
            session.add(row)
            session.flush()
            return row.id

    def _market_regime_context(
        self,
        *,
        posture: str = 'caution',
        market_regime: str | None = None,
        quantity_multiplier: float | None = None,
    ) -> dict:
        resolved_market_regime = market_regime or ('defensive' if posture == 'caution' else 'normal')
        resolved_multiplier = (
            quantity_multiplier
            if quantity_multiplier is not None
            else 0.5
            if posture == 'caution'
            else 1.0
        )
        sizing_window = (
            {
                'name': 'aggressive_open',
                'timezone': 'Asia/Seoul',
                'valid_from': f'{self.target_trade_date.isoformat()}T09:00:00+09:00',
                'valid_until': f'{self.target_trade_date.isoformat()}T09:20:00+09:00',
                'quantity_multiplier_cap': 2.0,
                'entry_priority_boost': True,
                'exec_enforced': True,
            }
            if resolved_market_regime == 'aggressive_open'
            else None
        )
        return {
            'schema_version': 'market_regime_context_v1',
            'market_date': self.market_date.isoformat(),
            'market': 'KOSPI',
            'posture': posture,
            'market_regime': resolved_market_regime,
            'risk_mode': resolved_market_regime,
            'regime_confidence': 0.81 if resolved_market_regime == 'aggressive_open' else 0.60,
            'regime_reasons': ['breadth_strong'] if resolved_market_regime == 'aggressive_open' else ['weak_market_breadth'],
            'reason_code': 'weak_market_breadth' if posture == 'caution' else 'market_regime_normal',
            'reason_summary': 'Market breadth is weak.',
            'source_status': {
                'overall': 'available',
                'market_breadth': 'available',
                'sector_context': 'available',
                'relative_strength_context': 'available',
            },
            'freshness': {'overall_status': 'fresh'},
            'market_breadth': {'up_ratio': 0.42, 'down_ratio': 0.52},
            'sector_context': {'sector_name': 'Semiconductors', 'weak_sector': False},
            'relative_strength_context': {'return_percentile': 0.72, 'volume_percentile': 0.68},
            'recommended_exec_action': 'downsize' if posture == 'caution' else 'allow',
            'recommended_sizing_multiplier': resolved_multiplier,
            'quantity_multiplier': resolved_multiplier,
            'entry_priority_boost': resolved_market_regime == 'aggressive_open',
            'valid_until': sizing_window['valid_until'] if sizing_window else None,
            'sizing_window': sizing_window,
            'exec_guardrails': {
                'core_bypasses_safety_gates': False,
                'exec_must_enforce_window': resolved_market_regime == 'aggressive_open',
                'must_respect': ['kill_switch', 'account_drawdown_gate', 'symbol_caps', 'max_notional_caps', 'stop_loss_required'],
            },
            'missing_fields': [],
        }

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
        confidence_score_10: float = 7.3,
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
                confidence_score_10=confidence_score_10,
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
                    'confidence_score_10': confidence_score_10,
                    'conviction_score_10': 6.5,
                    **(response_payload_extra or {}),
                },
                response_summary_payload={'model': 'openclaw/short-term-decider'},
            )
            session.add(row)
            session.flush()
            return row.id

    def _seed_symbol(self, *, symbol: str | None = None, name_ko: str = 'Samsung Electronics') -> None:
        resolved_symbol = symbol or self.symbol
        self.seeded_symbols.add(resolved_symbol)
        with session_scope() as session:
            session.execute(delete(Symbol).where(Symbol.symbol == resolved_symbol))
            session.add(
                Symbol(
                    symbol=resolved_symbol,
                    standard_code=None,
                    name_ko=name_ko,
                    market='KOSPI',
                    instrument_type='equity',
                    source='test',
                    is_active=True,
                )
            )
            session.flush()

    def _seed_signal_candidate(
        self,
        *,
        symbol: str,
        name_ko: str,
        decision_run_id: str,
        client_order_id: str,
        request_payload: dict | None = None,
        input_feature_snapshot_payload: dict | None = None,
    ) -> int:
        self._seed_symbol(symbol=symbol, name_ko=name_ko)
        with session_scope() as session:
            snapshot = SignalDeciderDecisionSnapshot(
                feedback_id=None,
                market_date=self.market_date,
                symbol=symbol,
                agent_name='signal-decider',
                agent_version='test-agent',
                decision_run_id=decision_run_id,
                feedback_received_at=datetime(2026, 4, 29, 6, 0, tzinfo=timezone.utc),
                policy_version=self.policy_version,
                decision_action='buy',
                preferred_action='buy',
                next_day_open_action='buy',
                entry_price_basis='next_open',
                target_price=130000,
                expected_return_pct=3.1,
                stop_loss_price=112000,
                suggested_holding_period='3-10 trading days',
                confidence_score_10=7.1,
                conviction_score_10=6.0,
                thesis_summary='Second dispatch candidate.',
                supporting_evidence_payload=['Liquidity remained strong.'],
                decision_risks_payload=['Gap risk.'],
                invalidation_conditions_payload=['Lose support.'],
                data_quality_warnings_payload=[],
                specialist_disagreement_payload=[],
                traceability_payload={'gate_checks': {'missing_upstream_roles': []}},
                input_feature_snapshot_payload=input_feature_snapshot_payload or {'technical': {'trend_state': 'uptrend'}},
                request_payload=request_payload or {'signal_decider_policy': {'current_version': self.policy_version}},
                response_payload={'next_day_open_action': 'buy'},
                response_summary_payload={'policy_version': self.policy_version},
            )
            session.add(snapshot)
            session.flush()
            plan_item = SignalDeciderExecutionPlanItem(
                execution_window='next_open',
                decision_agent_name='signal-decider',
                decision_snapshot_id=snapshot.id,
                order_request_id=None,
                broker_order_row_id=None,
                broker_name='kis',
                target_trade_date=self.target_trade_date,
                source_market_date=self.market_date,
                account_reference=self.account_reference,
                account_label='Main',
                symbol=symbol,
                policy_version=self.policy_version,
                planned_side='buy',
                planned_order_type='market',
                planned_time_in_force='day',
                allocation_budget_krw='1500000',
                estimated_reference_price=120000,
                planned_quantity='5',
                estimated_notional_krw='600000',
                eligibility_status='eligible',
                execution_status='planned',
                client_order_id=client_order_id,
                validation_payload={'blocking_reasons': [], 'warnings': []},
                traceability_payload={
                    'decision_snapshot_id': snapshot.id,
                    'decision_run_id': decision_run_id,
                    'policy_version': self.policy_version,
                    'target_trade_date': self.target_trade_date.isoformat(),
                    'source_market_date': self.market_date.isoformat(),
                    'account_reference': self.account_reference,
                    'account_label': 'Main',
                    'allocation_budget_krw': '1500000',
                    'target_price': 130000,
                    'stop_loss_price': 112000,
                    'suggested_holding_period': '3-10 trading days',
                    'signal_traceability': {'gate_checks': {'missing_upstream_roles': []}},
                },
                submission_payload=None,
                first_ready_at=datetime(2026, 4, 29, 23, 30, tzinfo=timezone.utc),
                last_evaluated_at=datetime(2026, 4, 29, 23, 30, tzinfo=timezone.utc),
                submitted_at=None,
            )
            session.add(plan_item)
            session.flush()
            return plan_item.id

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
                client_order_id=self.main_client_order_id,
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
                client_order_id=self.short_term_client_order_id,
                validation_payload={
                    'blocking_reasons': [],
                    'warnings': [],
                    'decision_agent_name': 'short-term-decider',
                    'short_term': {
                        'raw_action': 'watch_for_trigger',
                        'tactical_action': 'watch_for_trigger',
                        'generic_action': 'buy',
                        'handoff_intent': 'intraday_watch_buy',
                        'entry_execution_mode': 'pending_trigger_limit',
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
                    'raw_action': 'watch_for_trigger',
                    'tactical_action': 'watch_for_trigger',
                    'generic_action': 'buy',
                    'handoff_intent': 'intraday_watch_buy',
                    'handoff_active': target_price is not None and stop_loss_price is not None,
                    'exec_monitoring_active': target_price is not None and stop_loss_price is not None,
                    'approval_state': 'pending_exec_agent' if target_price is not None and stop_loss_price is not None else 'blocked',
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

    def _mark_plan_item_blocked_with_readback(
        self,
        *,
        readback: dict,
        dispatch_status: str = 'accepted',
        execution_status: str = 'planned',
    ) -> None:
        with session_scope() as session:
            row = session.get(SignalDeciderExecutionPlanItem, self.plan_item_id)
            submission = dict(row.submission_payload or {})
            exec_dispatch = dict(submission.get('exec_dispatch') or {})
            exec_dispatch['dispatch_status'] = dispatch_status
            exec_dispatch['readback'] = readback
            submission['exec_dispatch'] = exec_dispatch
            row.submission_payload = submission
            row.eligibility_status = 'blocked'
            row.execution_status = execution_status
            row.validation_payload = {
                **(row.validation_payload or {}),
                'blocking_reasons': [{'code': 'stale_planning_block', 'message': 'stale planning block'}],
            }
            session.flush()

    async def _readback_stale_short_term_watch(
        self,
        *,
        exec_status: str = 'watch_monitoring_active',
        response_item_status: str | None = None,
        core_execution_status: str = 'planned',
    ) -> tuple[dict, dict, int]:
        short_snapshot_id = self._seed_short_term_snapshot()
        short_plan_item_id = self._seed_short_term_plan_item(snapshot_id=short_snapshot_id)
        dispatch_result = await self.service.dispatch_next_open_plan(
            target_trade_date=self.target_trade_date,
            account_reference=self.account_reference,
            refresh_plan=False,
            dry_run=False,
            force=True,
            trigger='test',
        )
        short_group = next(
            group
            for group in dispatch_result['dispatch']['groups']
            if group['decision_agent_name'] == 'short-term-decider'
        )
        short_plan_id = short_group['plan_id']
        short_post_call = next(
            payload
            for payload in self.client.post_calls
            if payload['metadata']['decision_agent_name'] == 'short-term-decider'
        )
        short_entry_item = next(
            item
            for item in short_post_call['items']
            if item.get('intent') == EXEC_INTENT_INTRADAY_WATCH_BUY
        )
        original_blocking_reason = {
            'code': 'manual_recovery_gate_block',
            'message': 'Original core quality gate remained blocked before exec restore.',
        }
        original_warning = {
            'code': 'original_soft_gate_warning',
            'message': 'Original soft gate warning is preserved.',
        }
        with session_scope() as session:
            row = session.get(SignalDeciderExecutionPlanItem, short_plan_item_id)
            row.eligibility_status = 'blocked'
            row.execution_status = core_execution_status
            row.validation_payload = {
                **(row.validation_payload or {}),
                'blocking_reasons': [original_blocking_reason],
                'warnings': [original_warning],
            }
            row.traceability_payload = {
                **(row.traceability_payload or {}),
                'exec_monitoring_active': False,
                'approval_state': 'blocked',
            }
            session.flush()

        self.client.runtime_response = {
            'mode': 'mock',
            'ready': True,
            'monitoring': {'alive': True},
        }
        self.client.plan_response = {
            'items': [
                {
                    'external_plan_id': short_plan_id,
                    'status': 'accepted',
                    'items': [
                        {
                            'core_item_key': short_entry_item['core_item_key'],
                            'execution_plan_item_id': 'exec-short-watch-1',
                            'symbol': self.symbol,
                            'intent': EXEC_INTENT_INTRADAY_WATCH_BUY,
                            'status': response_item_status or exec_status,
                            'monitoring_status': exec_status,
                        }
                    ],
                }
            ]
        }
        self.client.order_response = {'items': []}
        self.client.execution_response = {'items': []}
        self.client.position_response = {'items': []}

        overview = await self.service.list_signal_decider_exec_overview(
            target_trade_date=self.target_trade_date,
            account_reference=self.account_reference,
            symbol=self.symbol,
            refresh=True,
            trigger='test',
        )
        short_item = next(
            item
            for item in overview['items']
            if item['plan_item_id'] == short_plan_item_id
        )
        return overview, short_item, short_plan_item_id

    async def test_dispatch_next_open_plan_blocks_when_source_pykrx_enrichment_missing(self) -> None:
        blocked_preflight = {
            'market_date': self.market_date.isoformat(),
            'status': 'blocked',
            'allowed': False,
            'required_categories': ['sector_classification', 'investor_flow_symbol', 'short_selling'],
            'failed_categories': ['investor_flow_symbol', 'short_selling'],
            'zero_coverage_categories': ['investor_flow_symbol', 'short_selling'],
            'category_row_counts': {
                'sector_classification': 1,
                'investor_flow_symbol': 0,
                'short_selling': 0,
            },
        }
        with patch('app.orders.next_open_execution.inspect_pykrx_enrichment_preflight', return_value=blocked_preflight):
            result = await self.service.dispatch_next_open_plan(
                target_trade_date=self.target_trade_date,
                refresh_plan=False,
                dry_run=False,
                force=False,
                trigger='test',
            )

        self.assertEqual(result['summary']['reason'], 'pykrx_enrichment_preflight_blocked')
        self.assertFalse(result['summary']['triggered'])
        self.assertEqual(result['summary']['translated_item_count'], 0)
        self.assertFalse(result['summary']['enrichment_preflight_allowed'])
        self.assertEqual(result['summary']['enrichment_preflight_status'], 'blocked')
        self.assertEqual(result['enrichment_preflight']['zero_coverage_categories'], ['investor_flow_symbol', 'short_selling'])
        self.assertEqual(self.client.post_calls, [])
        self.assertEqual(result['items'][0]['eligibility_status'], 'blocked')
        blocking_codes = [entry['code'] for entry in result['items'][0]['validation']['blocking_reasons']]
        self.assertIn('pykrx_enrichment_preflight_blocked', blocking_codes)

    async def test_dispatch_next_open_plan_continues_for_candidate_enriched_short_term_rows(self) -> None:
        short_snapshot_id = self._seed_short_term_snapshot()
        self._seed_short_term_plan_item(snapshot_id=short_snapshot_id)
        with session_scope() as session:
            short_row = session.execute(
                select(SignalDeciderExecutionPlanItem).where(
                    SignalDeciderExecutionPlanItem.decision_snapshot_id == short_snapshot_id,
                    SignalDeciderExecutionPlanItem.decision_agent_name == 'short-term-decider',
                )
            ).scalar_one()
            short_validation = dict(short_row.validation_payload or {})
            short_validation['candidate_pykrx_enrichment'] = {
                'market_date': self.market_date.isoformat(),
                'required_categories': ['sector_classification', 'investor_flow_symbol', 'short_selling'],
                'present_categories': ['sector_classification', 'investor_flow_symbol', 'short_selling'],
                'missing_categories': [],
                'allowed': True,
                'source': 'candidate_symbol_pykrx_enrichment',
            }
            short_validation['warnings'] = [
                {
                    'code': 'global_pykrx_enrichment_preflight_partial',
                    'message': 'Global PyKRX enrichment coverage is partial, but this candidate is complete.',
                }
            ]
            short_row.validation_payload = short_validation
            session.flush()
        blocked_preflight = {
            'market_date': self.market_date.isoformat(),
            'status': 'blocked',
            'allowed': False,
            'required_categories': ['sector_classification', 'investor_flow_symbol', 'short_selling'],
            'failed_categories': ['sector_classification', 'investor_flow_symbol', 'short_selling'],
            'zero_coverage_categories': [],
            'category_row_counts': {
                'sector_classification': 2699,
                'investor_flow_symbol': 2672,
                'short_selling': 2806,
            },
        }

        with patch('app.orders.next_open_execution.inspect_pykrx_enrichment_preflight', return_value=blocked_preflight):
            result = await self.service.dispatch_next_open_plan(
                target_trade_date=self.target_trade_date,
                refresh_plan=False,
                dry_run=False,
                force=False,
                trigger='test',
            )

        self.assertTrue(result['summary']['triggered'])
        self.assertEqual(result['summary']['translated_item_count'], 3)
        self.assertEqual(result['summary']['dispatched_group_count'], 1)
        self.assertFalse(result['summary']['enrichment_preflight_allowed'])
        self.assertIn('dispatch_note', result['enrichment_preflight'])
        self.assertEqual(len(self.client.post_calls), 1)
        self.assertEqual(self.client.post_calls[0]['metadata']['decision_agent_name'], 'short-term-decider')
        groups_by_agent = {group['decision_agent_name']: group for group in result['dispatch']['groups']}
        self.assertEqual(groups_by_agent['signal-decider']['dispatch_status'], 'skipped')
        self.assertEqual(groups_by_agent['short-term-decider']['dispatch_status'], 'accepted')
        items_by_agent = {item['decision_agent_name']: item for item in result['items']}
        self.assertEqual(items_by_agent['signal-decider']['eligibility_status'], 'blocked')
        self.assertEqual(items_by_agent['short-term-decider']['execution_status'], EXECUTION_STATUS_DELEGATED_FOR_EXEC_AGENT_SIZING)

    async def test_short_term_entry_quality_blocks_scalp_when_signal_decider_waits(self) -> None:
        short_snapshot_id = self._seed_short_term_snapshot(
            tactical_action='scalp_buy_candidate',
            request_payload={
                'symbol_day_context': {
                    'market_relative_context': {
                        'relative_strength': {
                            'return_percentile': 0.82,
                            'volume_percentile': 0.76,
                        },
                    },
                },
            },
            input_feature_snapshot_payload={
                'liquidity_context': {
                    'turnover': {
                        'trading_value_source': 'raw',
                        'raw_trading_value_krw': 9_000_000_000,
                    },
                },
            },
            response_payload_extra={
                'thesis_summary': 'Immediate scalp setup with momentum expansion.',
            },
        )
        with session_scope() as session:
            snapshot = session.get(SignalDeciderDecisionSnapshot, short_snapshot_id)
            blocks, warnings, feature_snapshot = _short_term_entry_quality_blocks(
                snapshot=snapshot,
                signal_decider_crosscheck={
                    'action': 'wait',
                    'decision_snapshot_id': self.snapshot_id,
                    'policy_version': self.policy_version,
                },
                tactical_action='scalp_buy_candidate',
            )

        blocking_codes = {entry['code'] for entry in blocks}
        self.assertIn('signal_decider_wait_blocks_scalp_buy', blocking_codes)
        self.assertEqual(warnings, [])
        self.assertEqual(feature_snapshot['signal_decider_crosscheck']['action'], 'wait')

    async def test_short_term_entry_quality_warns_low_buy_quality_score(self) -> None:
        short_snapshot_id = self._seed_short_term_snapshot(
            request_payload={
                'symbol_day_context': {
                    'market_relative_context': {
                        'relative_strength': {
                            'return_percentile': 0.86,
                            'volume_percentile': 0.80,
                        },
                    },
                    'decision_input_quality_context': {
                        'final_judgment': {
                            'buy_quality_score': 59,
                            'decision_band': 'small_probe',
                        },
                    },
                    'liquidity_context': {
                        'turnover': {
                            'trading_value_source': 'raw',
                            'raw_trading_value_krw': 12_000_000_000,
                        },
                    },
                },
            },
            response_payload_extra={
                'trigger_conditions': ['눌림 후 VWAP 재돌파 확인.'],
            },
        )
        with session_scope() as session:
            snapshot = session.get(SignalDeciderDecisionSnapshot, short_snapshot_id)
            blocks, warnings, feature_snapshot = _short_term_entry_quality_blocks(
                snapshot=snapshot,
                signal_decider_crosscheck={'action': 'buy'},
                tactical_action='watch_for_trigger',
            )

        self.assertEqual(blocks, [])
        self.assertIn('short_term_buy_quality_score_warning', {entry['code'] for entry in warnings})
        self.assertEqual(Decimal(str(feature_snapshot['buy_quality_score'])), Decimal('59'))

    async def test_short_term_entry_quality_warns_missing_objective_selection_data(self) -> None:
        short_snapshot_id = self._seed_short_term_snapshot(
            response_payload_extra={
                'trigger_conditions': ['눌림 후 VWAP 재돌파 확인.'],
            },
        )
        with session_scope() as session:
            snapshot = session.get(SignalDeciderDecisionSnapshot, short_snapshot_id)
            blocks, warnings, feature_snapshot = _short_term_entry_quality_blocks(
                snapshot=snapshot,
                signal_decider_crosscheck={'action': 'buy'},
                tactical_action='watch_for_trigger',
            )

        self.assertEqual(blocks, [])
        codes = {entry['code'] for entry in warnings}
        self.assertIn('short_term_relative_strength_unverified_warning', codes)
        self.assertIn('short_term_relative_volume_unverified_warning', codes)
        self.assertIn('short_term_liquidity_unverified_warning', codes)
        self.assertIsNone(feature_snapshot['return_percentile'])
        self.assertIsNone(feature_snapshot['volume_percentile'])

    async def test_short_term_entry_quality_warns_weak_relative_strength(self) -> None:
        short_snapshot_id = self._seed_short_term_snapshot(
            request_payload={
                'symbol_day_context': {
                    'market_relative_context': {
                        'relative_strength': {
                            'return_percentile': 0.55,
                            'volume_percentile': 0.88,
                        },
                    },
                    'liquidity_context': {
                        'turnover': {
                            'trading_value_source': 'raw',
                            'raw_trading_value_krw': 12_000_000_000,
                        },
                    },
                },
            },
            response_payload_extra={
                'trigger_conditions': ['눌림 후 VWAP 재돌파 확인.'],
            },
        )
        with session_scope() as session:
            snapshot = session.get(SignalDeciderDecisionSnapshot, short_snapshot_id)
            blocks, warnings, feature_snapshot = _short_term_entry_quality_blocks(
                snapshot=snapshot,
                signal_decider_crosscheck={'action': 'buy'},
                tactical_action='watch_for_trigger',
            )

        self.assertEqual(blocks, [])
        self.assertIn('short_term_weak_relative_strength_warning', {entry['code'] for entry in warnings})
        self.assertEqual(Decimal(str(feature_snapshot['return_percentile'])), Decimal('0.55'))

    async def test_short_term_entry_quality_blocks_very_weak_relative_strength(self) -> None:
        short_snapshot_id = self._seed_short_term_snapshot(
            request_payload={
                'symbol_day_context': {
                    'market_relative_context': {
                        'relative_strength': {
                            'return_percentile': 0.22,
                            'volume_percentile': 0.88,
                        },
                    },
                    'liquidity_context': {
                        'turnover': {
                            'trading_value_source': 'raw',
                            'raw_trading_value_krw': 12_000_000_000,
                        },
                    },
                },
            },
            response_payload_extra={
                'trigger_conditions': ['눌림 후 VWAP 재돌파 확인.'],
            },
        )
        with session_scope() as session:
            snapshot = session.get(SignalDeciderDecisionSnapshot, short_snapshot_id)
            blocks, warnings, feature_snapshot = _short_term_entry_quality_blocks(
                snapshot=snapshot,
                signal_decider_crosscheck={'action': 'buy'},
                tactical_action='watch_for_trigger',
            )

        self.assertIn('short_term_very_weak_relative_strength_block', {entry['code'] for entry in blocks})
        self.assertEqual(warnings, [])
        self.assertEqual(Decimal(str(feature_snapshot['return_percentile'])), Decimal('0.22'))

    async def test_short_term_entry_quality_blocks_poor_reward_risk(self) -> None:
        short_snapshot_id = self._seed_short_term_snapshot(
            trigger_price=50500,
            chase_limit_price=50800,
            target_price=52000,
            stop_loss_price=49200,
            request_payload={
                'symbol_day_context': {
                    'market_relative_context': {
                        'relative_strength': {
                            'return_percentile': 0.82,
                            'volume_percentile': 0.76,
                        },
                    },
                    'liquidity_context': {
                        'turnover': {
                            'trading_value_source': 'raw',
                            'raw_trading_value_krw': 15_000_000_000,
                        },
                    },
                },
            },
            response_payload_extra={
                'trigger_conditions': ['거래량 동반 재돌파 확인.'],
            },
        )
        with session_scope() as session:
            snapshot = session.get(SignalDeciderDecisionSnapshot, short_snapshot_id)
            blocks, warnings, feature_snapshot = _short_term_entry_quality_blocks(
                snapshot=snapshot,
                signal_decider_crosscheck={'action': 'buy'},
                tactical_action='watch_for_trigger',
            )

        self.assertIn('short_term_reward_risk_ratio_block', {entry['code'] for entry in blocks})
        self.assertEqual(warnings, [])
        self.assertLess(Decimal(str(feature_snapshot['reward_risk_ratio'])), Decimal('1.80'))

    async def test_dispatch_next_open_plan_fails_closed_on_calendar_fallback(self) -> None:
        set_krx_calendar_provider(
            KisKrxTradingCalendarProvider(client=FailingCalendarClient())
        )

        result = await self.service.dispatch_next_open_plan(
            target_trade_date=self.target_trade_date,
            refresh_plan=False,
            dry_run=False,
            force=False,
            trigger='test',
        )

        self.assertFalse(result['summary']['triggered'])
        self.assertEqual(result['summary']['reason'], 'calendar_unavailable')
        self.assertEqual(result['summary']['calendar_source'], 'exchange_calendars')
        self.assertEqual(self.client.post_calls, [])

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
        for item in payload['items']:
            self.assertEqual(
                {
                    key: item[key]
                    for key in ('asset_class', 'market', 'exchange', 'currency', 'broker', 'session_policy')
                },
                {
                    'asset_class': 'stock',
                    'market': 'KRX',
                    'exchange': 'KRX',
                    'currency': 'KRW',
                    'broker': 'kis',
                    'session_policy': {
                        'session': 'regular',
                        'timezone': 'Asia/Seoul',
                        'allow_extended_hours': False,
                    },
                },
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
        entry_key = entry_item['core_item_key']
        for exit_item in payload['items'][1:]:
            self.assertEqual(exit_item['quantity_policy']['type'], 'position_exit_from_entry')
            self.assertEqual(exit_item['quantity_policy']['linked_entry_item_key'], entry_key)
            self.assertNotIn('quantity', exit_item['quantity_policy'])

        with session_scope() as session:
            row = session.get(SignalDeciderExecutionPlanItem, self.plan_item_id)
            self.assertEqual(row.execution_status, EXECUTION_STATUS_DELEGATED_FOR_EXEC_AGENT_SIZING)
            exec_dispatch = row.submission_payload['exec_dispatch']
            self.assertIn('plan_id', exec_dispatch)
            self.assertEqual(exec_dispatch['dispatch_status'], 'accepted')
            self.assertEqual(len(exec_dispatch['item_links']), 3)

    async def test_dispatch_next_open_plan_persists_compact_per_row_payload_for_multi_item_plan(self) -> None:
        bulky_decider_context = {
            'decider_input_context': {
                'items': [
                    {
                        'symbol': self.symbol,
                        'risk_context': {
                            'decider_handoff_context': {
                                'decider_input_context': {'blob': 'x' * 12000},
                            },
                        },
                    }
                ],
                'raw_request_payload': {'blob': 'y' * 12000},
            },
            'symbol_day_context': {
                'pre_decider_enrichment_context': {'quote': 'available'},
                'decision_input_quality_context': {
                    'schema_version': 'decision_input_quality_v1',
                    'execution_feasibility': {'status': 'usable', 'liquidity_tier': 'large_cap'},
                },
            },
        }
        with session_scope() as session:
            snapshot = session.get(SignalDeciderDecisionSnapshot, self.snapshot_id)
            snapshot.request_payload = {
                'signal_decider_policy': {'current_version': self.policy_version},
                **bulky_decider_context,
            }
            snapshot.input_feature_snapshot_payload = {'raw_feature_blob': 'z' * 12000}

        self._seed_signal_candidate(
            symbol='000660',
            name_ko='SK hynix',
            decision_run_id='exec-run-2',
            client_order_id='SDNO-20260430-000660',
            request_payload={
                'signal_decider_policy': {'current_version': self.policy_version},
                **bulky_decider_context,
            },
            input_feature_snapshot_payload={'raw_feature_blob': 'q' * 12000},
        )

        result = await self.service.dispatch_next_open_plan(
            target_trade_date=self.target_trade_date,
            refresh_plan=False,
            dry_run=False,
            force=True,
            trigger='test',
        )

        self.assertEqual(result['summary']['eligible_count'], 2)
        self.assertEqual(result['summary']['translated_item_count'], 6)
        with session_scope() as session:
            rows = session.execute(
                select(SignalDeciderExecutionPlanItem).where(
                    SignalDeciderExecutionPlanItem.account_reference == self.account_reference,
                    SignalDeciderExecutionPlanItem.target_trade_date == self.target_trade_date,
                )
            ).scalars().all()
            self.assertEqual(len(rows), 2)
            for row in rows:
                submission_payload = row.submission_payload
                payload_text = json.dumps(submission_payload, ensure_ascii=False, sort_keys=True)
                exec_dispatch = submission_payload['exec_dispatch']
                self.assertLess(json_size_bytes(submission_payload), SIGNAL_DECIDER_EXEC_SUBMISSION_MAX_BYTES)
                self.assertNotIn('last_request_payload', exec_dispatch)
                self.assertNotIn('last_response_payload', exec_dispatch)
                self.assertIn('last_request_summary', exec_dispatch)
                self.assertEqual(exec_dispatch['last_request_summary']['total_item_count'], 6)
                self.assertEqual(exec_dispatch['last_request_summary']['row_item_count'], 3)
                self.assertNotIn('decider_input_context', payload_text)
                self.assertNotIn('raw_request_payload', payload_text)
                self.assertNotIn('last_request_payload', payload_text)

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

    async def test_cleanup_signal_decider_exec_payloads_dry_run_preserves_rows(self) -> None:
        oversized_payload = {
            'mode': 'exec_dispatch',
            'latest': {'dispatch_status': 'failed', 'attempted_at': '2026-05-22T00:00:00+00:00'},
            'exec_dispatch': {
                'plan_id': 'core-signal-decider-20260522-KIS',
                'plan_fingerprint': 'abc123',
                'dispatch_status': 'failed',
                'last_attempt_at': '2026-05-22T00:00:00+00:00',
                'last_error': {'code': 'unavailable', 'message': 'x' * 4096},
                'item_links': [
                    {
                        'core_item_key': f'core-plan-{self.plan_item_id}:next_open_buy',
                        'intent': 'next_open_buy',
                        'side': 'buy',
                        'symbol': self.symbol,
                    }
                ],
                'last_request_payload': {
                    'plan_id': 'core-signal-decider-20260522-KIS',
                    'items': [
                        {
                            'core_item_key': f'core-plan-{self.plan_item_id}:next_open_buy',
                            'symbol': self.symbol,
                            'risk_context': {
                                'decider_handoff_context': {
                                    'decider_input_context': {'raw_blob': 'x' * 70000},
                                },
                            },
                        }
                    ],
                },
                'readback': {
                    'orders': {
                        'status': 'ok',
                        'count': 1,
                        'items': [{'symbol': self.symbol, 'status': 'rejected', 'raw_blob': 'y' * 70000}],
                    },
                    'derived_state': {'derived_status': 'failed'},
                },
            },
        }
        with session_scope() as session:
            row = session.get(SignalDeciderExecutionPlanItem, self.plan_item_id)
            row.submission_payload = oversized_payload
        with session_scope() as session:
            row = session.get(SignalDeciderExecutionPlanItem, self.plan_item_id)
            committed_payload = row.submission_payload

        dry_run = cleanup_signal_decider_exec_submission_payloads(
            dry_run=True,
            target_trade_date=self.target_trade_date,
            min_payload_bytes=1024,
            limit=10,
        )

        self.assertEqual(dry_run['would_update_count'], 1)
        self.assertEqual(dry_run['updated_count'], 0)
        with session_scope() as session:
            row = session.get(SignalDeciderExecutionPlanItem, self.plan_item_id)
            self.assertEqual(row.submission_payload, committed_payload)

        applied = cleanup_signal_decider_exec_submission_payloads(
            dry_run=False,
            target_trade_date=self.target_trade_date,
            min_payload_bytes=1024,
            limit=10,
        )

        self.assertEqual(applied['updated_count'], 1)
        with session_scope() as session:
            row = session.get(SignalDeciderExecutionPlanItem, self.plan_item_id)
            payload_text = json.dumps(row.submission_payload, ensure_ascii=False, sort_keys=True)
            self.assertLess(json_size_bytes(row.submission_payload), SIGNAL_DECIDER_EXEC_SUBMISSION_MAX_BYTES)
            self.assertNotIn('last_request_payload', row.submission_payload['exec_dispatch'])
            self.assertNotIn('decider_input_context', payload_text)

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
                    'trading_value': {
                        'status': 'available',
                        'source': 'raw',
                        'raw_trading_value_krw': 9_870_000_000,
                        'trading_value_krw': 9_870_000_000,
                    },
                },
                'market_relative_context': {
                    'relative_strength': {
                        'return_percentile': 0.82,
                        'volume_percentile': 0.76,
                    },
                },
                'liquidity_context': {
                    'turnover': {
                        'status': 'available',
                        'trading_value_source': 'raw',
                        'raw_trading_value_krw': 9_870_000_000,
                    },
                },
                'decision_input_quality_context': {
                    'schema_version': 'decision_input_quality_v1',
                    'execution_feasibility': {
                        'status': 'usable',
                        'liquidity_tier': 'medium',
                        'max_safe_notional_krw': 49350000,
                        'sizing_hints': [{'code': 'max_safe_notional', 'max_safe_notional_krw': 49350000}],
                    },
                    'final_judgment': {
                        'buy_quality_score': 66,
                        'decision_band': 'small_probe',
                    },
                },
                'market_regime_context': self._market_regime_context(),
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
            confidence_score_10=5.2,
            response_payload_extra={
                'trigger_conditions': ['Reclaim 50500 with volume.'],
                'risk_flags': ['Opening gap risk.'],
            },
        )
        short_plan_item_id = self._seed_short_term_plan_item(snapshot_id=short_snapshot_id)
        alignment_gate = {
            'schema_version': 'signal_decider_alignment_gate_v1',
            'signal_decider_alignment': 'wait_conflict_block',
            'alignment_gate_action': 'report_only_block_candidate',
            'alignment_gate_reason': 'Signal-decider is WAIT for the same symbol/date context.',
            'signal_decider_decision_snapshot_id': 991,
            'decision_snapshot_id': 991,
        }
        with session_scope() as session:
            row = session.get(SignalDeciderExecutionPlanItem, short_plan_item_id)
            row.validation_payload = {
                **(row.validation_payload or {}),
                'alignment_gate': alignment_gate,
                'signal_decider_alignment': 'wait_conflict_block',
                'alignment_gate_action': 'report_only_block_candidate',
                'alignment_gate_reason': alignment_gate['alignment_gate_reason'],
                'alignment_signal_decider_decision_snapshot_id': 991,
            }
            row.traceability_payload = {
                **(row.traceability_payload or {}),
                'alignment_gate': alignment_gate,
                'signal_decider_alignment': 'wait_conflict_block',
                'alignment_gate_action': 'report_only_block_candidate',
                'alignment_gate_reason': alignment_gate['alignment_gate_reason'],
                'alignment_signal_decider_decision_snapshot_id': 991,
            }

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
        self.assertEqual(signal_entry['risk_context']['market_regime_context']['schema_version'], 'market_regime_context_v1')
        self.assertEqual(signal_entry['risk_context']['market_regime_context']['recommended_exec_action'], 'downsize')
        self.assertEqual(signal_entry['risk_context']['max_holding_close_policy']['enabled'], False)
        self.assertIsNone(signal_entry['risk_context']['max_holding_close_policy']['max_holding_trading_sessions'])
        self.assertNotIn('max_holding_close_sell', [item['intent'] for item in signal_payload['items']])
        short_entry = short_payload['items'][0]
        self.assertEqual(short_entry['decision_agent_name'], 'short-term-decider')
        self.assertEqual(short_entry['intent'], EXEC_INTENT_INTRADAY_WATCH_BUY)
        self.assertEqual(short_entry['handoff_intent'], 'intraday_watch_buy')
        self.assertEqual(short_entry['raw_action'], 'watch_for_trigger')
        self.assertEqual(short_entry['tactical_action'], 'watch_for_trigger')
        self.assertEqual(short_entry['generic_action'], 'buy')
        self.assertEqual(short_entry['trigger_price'], 50500)
        self.assertEqual(short_entry['limit_price'], 51500)
        self.assertEqual(short_entry['risk_context']['agent_name'], 'short-term-decider')
        self.assertEqual(short_entry['risk_context']['decision_agent_name'], 'short-term-decider')
        self.assertEqual(short_entry['risk_context']['handoff_intent'], 'intraday_watch_buy')
        self.assertEqual(short_entry['risk_context']['generic_action'], 'buy')
        self.assertEqual(short_entry['risk_context']['signal_decider_alignment'], 'wait_conflict_block')
        self.assertEqual(short_entry['risk_context']['alignment_gate_action'], 'report_only_block_candidate')
        self.assertEqual(short_entry['risk_context']['alignment_signal_decider_decision_snapshot_id'], 991)
        self.assertEqual(short_entry['risk_context']['entry_execution_mode'], 'pending_trigger_limit')
        self.assertTrue(short_entry['risk_context']['monitoring_candidate'])
        self.assertEqual(short_entry['risk_context']['monitoring_intent'], EXEC_INTENT_INTRADAY_WATCH_BUY)
        self.assertEqual(short_entry['risk_context']['original_watch_trigger_price'], 50500)
        self.assertEqual(short_entry['risk_context']['original_watch_chase_limit_price'], 51500)
        self.assertNotIn('trigger_price', short_entry['risk_context'])
        self.assertNotIn('chase_limit_price', short_entry['risk_context'])
        self.assertIn('source_feedback_id', short_entry['risk_context'])
        self.assertIn('target_trade_date', short_entry['risk_context'])
        self.assertIn('source_market_date', short_entry['risk_context'])
        self.assertEqual(short_entry['risk_context']['allocation_mode'], 'specified')
        self.assertEqual(Decimal(str(short_entry['risk_context']['allocation_budget_krw'])), Decimal('1500000'))
        self.assertEqual(short_entry['risk_context']['confidence_score_10'], 5.2)
        self.assertEqual(short_entry['risk_context']['conviction_score_10'], 6.5)
        self.assertEqual(short_entry['risk_context']['decision_run_id'], 'short-run-1')
        self.assertEqual(short_entry['quantity_policy']['type'], 'exec_agent_sized')
        self.assertEqual(Decimal(str(short_entry['quantity_policy']['budget_krw'])), Decimal('1500000'))
        self.assertEqual(short_entry['quantity_policy']['allocation_mode'], 'specified')
        self.assertEqual(short_entry['quantity_policy']['account_reference'], self.account_reference)
        self.assertEqual(short_entry['quantity_policy']['account_label'], 'Main')
        self.assertEqual(short_entry['risk_context']['classification'], EXECUTION_STATUS_DELEGATED_FOR_EXEC_AGENT_SIZING)
        self.assertEqual(short_entry['risk_context']['market_regime_context']['schema_version'], 'market_regime_context_v1')
        self.assertEqual(short_entry['risk_context']['market_regime_context']['recommended_exec_action'], 'downsize')
        short_items_by_intent = {item['intent']: item for item in short_payload['items']}
        self.assertEqual(
            set(short_items_by_intent),
            {EXEC_INTENT_INTRADAY_WATCH_BUY, EXEC_INTENT_TARGET_SELL, EXEC_INTENT_STOP_SELL},
        )
        short_entry_key = short_entry['core_item_key']
        for exit_intent in (EXEC_INTENT_TARGET_SELL, EXEC_INTENT_STOP_SELL):
            exit_policy = short_items_by_intent[exit_intent]['quantity_policy']
            self.assertEqual(exit_policy['type'], 'position_exit_from_entry')
            self.assertEqual(exit_policy['linked_entry_item_key'], short_entry_key)
            self.assertNotIn('quantity', exit_policy)
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
        self.assertEqual(handoff['traceability']['handoff_intent'], 'intraday_watch_buy')
        self.assertEqual(handoff['traceability']['signal_decider_alignment'], 'wait_conflict_block')
        self.assertEqual(handoff['traceability']['alignment_gate_action'], 'report_only_block_candidate')
        self.assertEqual(handoff['decider_output']['generic_action'], 'buy')
        self.assertEqual(handoff['decider_output']['handoff_intent'], 'intraday_watch_buy')
        self.assertEqual(handoff['decider_output']['signal_decider_alignment'], 'wait_conflict_block')
        self.assertEqual(handoff['traceability']['core_plan_item_id'], short_entry['risk_context']['core_plan_item_id'])
        self.assertEqual(handoff['decider_input_context']['request_payload'], decider_request_payload)
        self.assertEqual(handoff['decider_input_context']['input_feature_snapshot_payload'], decider_input_features)
        self.assertEqual(
            handoff['decider_input_context']['pre_decider_enrichment_context'],
            decider_request_payload['symbol_day_context']['pre_decider_enrichment_context'],
        )
        self.assertEqual(
            handoff['decider_input_context']['decision_input_quality_context']['schema_version'],
            'decision_input_quality_v1',
        )
        self.assertEqual(
            handoff['decider_input_context']['market_regime_context']['schema_version'],
            'market_regime_context_v1',
        )
        self.assertEqual(handoff['market_regime_context']['recommended_exec_action'], 'downsize')
        self.assertEqual(
            short_entry['risk_context']['decision_input_quality_context']['schema_version'],
            'decision_input_quality_v1',
        )
        self.assertEqual(
            short_entry['risk_context']['execution_feasibility_context']['liquidity_tier'],
            'medium',
        )
        self.assertEqual(Decimal(str(handoff['decider_output']['confidence_score_10'])), Decimal('5.2'))
        self.assertIn('sizing_hints', short_entry['quantity_policy'])
        self.assertEqual(short_entry['quantity_policy']['max_safe_notional_krw'], 49350000)
        self.assertEqual(handoff['decider_output']['response_payload']['trigger_conditions'], ['Reclaim 50500 with volume.'])
        self.assertEqual(handoff['decider_output']['response_payload']['risk_flags'], ['Opening gap risk.'])
        self.assertEqual(Decimal(str(handoff['decider_output']['response_payload']['confidence_score_10'])), Decimal('5.2'))
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

    async def test_readback_promotes_blocked_planned_short_term_watch_to_delegated(self) -> None:
        overview, short_item, short_plan_item_id = await self._readback_stale_short_term_watch(
            exec_status='watch_monitoring_active',
        )

        self.assertTrue(short_item['exec_monitoring_active'])
        self.assertTrue(short_item['exec_agent_approved'])
        self.assertEqual(short_item['approval_state'], 'approved')
        self.assertEqual(short_item['execution_status'], EXECUTION_STATUS_DELEGATED_FOR_EXEC_AGENT_SIZING)
        self.assertEqual(short_item['eligibility_status'], 'blocked')
        self.assertEqual(short_item['validation']['blocking_reasons'][0]['code'], 'manual_recovery_gate_block')
        self.assertIn(
            'exec_watch_state_divergence',
            {warning['code'] for warning in short_item['validation']['warnings']},
        )
        self.assertEqual(short_item['remote_state']['plan_item_status_counts']['watch_monitoring_active'], 1)
        self.assertEqual(overview['summary']['core_exec_state_divergence_count'], 1)

        with session_scope() as session:
            row = session.get(SignalDeciderExecutionPlanItem, short_plan_item_id)
            self.assertEqual(row.execution_status, EXECUTION_STATUS_DELEGATED_FOR_EXEC_AGENT_SIZING)
            self.assertEqual(row.eligibility_status, 'blocked')
            self.assertEqual(row.validation_payload['blocking_reasons'][0]['code'], 'manual_recovery_gate_block')
            self.assertEqual(row.validation_payload['warnings'][0]['code'], 'original_soft_gate_warning')
            self.assertIn(
                'exec_watch_state_divergence',
                {warning['code'] for warning in row.validation_payload['warnings']},
            )
            audit = row.submission_payload['exec_dispatch']['core_state_sync']
            self.assertTrue(audit['divergence'])
            self.assertEqual(audit['core_state_before']['eligibility_status'], 'blocked')
            self.assertEqual(audit['core_state_before']['execution_status'], 'planned')
            self.assertEqual(audit['core_state_after']['execution_status'], EXECUTION_STATUS_DELEGATED_FOR_EXEC_AGENT_SIZING)

    async def test_readback_promotes_submit_failed_short_term_watch_to_delegated(self) -> None:
        overview, short_item, short_plan_item_id = await self._readback_stale_short_term_watch(
            exec_status='watch_monitoring_active',
            core_execution_status='submit_failed',
        )

        self.assertTrue(short_item['exec_monitoring_active'])
        self.assertEqual(short_item['execution_status'], EXECUTION_STATUS_DELEGATED_FOR_EXEC_AGENT_SIZING)
        self.assertEqual(overview['summary']['core_exec_state_divergence_count'], 1)

        with session_scope() as session:
            row = session.get(SignalDeciderExecutionPlanItem, short_plan_item_id)
            self.assertEqual(row.execution_status, EXECUTION_STATUS_DELEGATED_FOR_EXEC_AGENT_SIZING)
            audit = row.submission_payload['exec_dispatch']['core_state_sync']
            self.assertEqual(audit['core_state_before']['execution_status'], 'submit_failed')
            self.assertEqual(audit['core_state_after']['execution_status'], EXECUTION_STATUS_DELEGATED_FOR_EXEC_AGENT_SIZING)

    async def test_runtime_read_model_uses_exec_watch_status_without_broker_orders(self) -> None:
        _, _, short_plan_item_id = await self._readback_stale_short_term_watch(
            exec_status='watch_monitoring_pending',
        )

        board = await self.service.get_runtime_daily_board(
            market_date=self.target_trade_date,
            refresh=False,
            trigger='test',
        )
        short_board_item = next(
            item
            for item in board['items']
            if item['plan_item_id'] == short_plan_item_id
        )

        self.assertEqual(short_board_item['status'], 'scheduled')
        self.assertEqual(short_board_item['monitoring_state'], 'watching_fill')
        self.assertTrue(short_board_item['exec_monitoring_active'])
        self.assertTrue(short_board_item['exec_runtime_showable'])
        self.assertEqual(short_board_item['exec_runtime_state'], 'exec_approved_watch')
        self.assertEqual(short_board_item['core_state_divergence_warning']['code'], 'exec_watch_state_divergence')
        self.assertEqual(short_board_item['open_quantity'], 0)
        self.assertIsNone(short_board_item['broker_order_id'])

        with session_scope() as session:
            self.assertEqual(
                session.execute(
                    select(OrderRequest).where(OrderRequest.client_order_id == self.short_term_client_order_id)
                ).scalars().all(),
                [],
            )
            self.assertEqual(
                session.execute(
                    select(BrokerOrder).where(BrokerOrder.client_order_id == self.short_term_client_order_id)
                ).scalars().all(),
                [],
            )

    async def test_runtime_read_model_prioritizes_active_exec_watch_over_accepted_status(self) -> None:
        _, _, short_plan_item_id = await self._readback_stale_short_term_watch(
            exec_status='watch_monitoring_active',
            response_item_status='accepted',
        )

        board = await self.service.get_runtime_daily_board(
            market_date=self.target_trade_date,
            refresh=False,
            trigger='test',
        )
        short_board_item = next(
            item
            for item in board['items']
            if item['plan_item_id'] == short_plan_item_id
        )

        self.assertEqual(short_board_item['status'], 'accepted')
        self.assertEqual(short_board_item['monitoring_state'], 'watching_fill')
        self.assertTrue(short_board_item['exec_monitoring_active'])
        self.assertTrue(short_board_item['exec_runtime_showable'])
        self.assertEqual(short_board_item['exec_runtime_state'], 'exec_approved_watch')

    async def test_overview_warns_when_exec_watch_active_but_core_was_blocked_planned(self) -> None:
        overview, short_item, _ = await self._readback_stale_short_term_watch(
            exec_status='watch_buy_submitted',
        )

        self.assertEqual(overview['summary']['core_exec_state_divergence_count'], 1)
        self.assertEqual(overview['summary']['warnings'][0]['code'], 'exec_watch_state_divergence')
        self.assertEqual(overview['summary']['warnings'][0]['symbols'], [self.symbol])
        warning = short_item['core_state_divergence_warning']
        self.assertEqual(warning['code'], 'exec_watch_state_divergence')
        self.assertEqual(warning['core_state_before']['eligibility_status'], 'blocked')
        self.assertEqual(warning['core_state_before']['execution_status'], 'planned')
        self.assertEqual(warning['exec_statuses'], ['watch_buy_submitted'])

    async def test_dispatch_next_open_plan_short_term_missing_target_stop_skips_watch_handoff(self) -> None:
        short_snapshot_id = self._seed_short_term_snapshot(target_price=None, stop_loss_price=None)
        self._seed_short_term_plan_item(
            snapshot_id=short_snapshot_id,
            target_price=None,
            stop_loss_price=None,
        )

        result = await self.service.dispatch_next_open_plan(
            target_trade_date=self.target_trade_date,
            refresh_plan=False,
            dry_run=False,
            force=True,
            trigger='test',
        )

        self.assertFalse(
            any(payload['metadata']['decision_agent_name'] == 'short-term-decider' for payload in self.client.post_calls)
        )
        short_group = next(
            group for group in result['dispatch']['groups'] if group['decision_agent_name'] == 'short-term-decider'
        )
        self.assertEqual(short_group['dispatch_status'], 'skipped')
        self.assertEqual(short_group['eligible_count'], 0)

    async def test_dispatch_next_open_plan_recovers_retryable_timeout_from_exec_readback(self) -> None:
        short_snapshot_id = self._seed_short_term_snapshot()
        short_plan_item_id = self._seed_short_term_plan_item(snapshot_id=short_snapshot_id)
        self.client = TimeoutAfterAcceptExecClient()
        self.service = SignalDeciderExecIntegrationService(client=self.client)

        result = await self.service.dispatch_next_open_plan(
            target_trade_date=self.target_trade_date,
            account_reference=self.account_reference,
            refresh_plan=False,
            dry_run=False,
            force=True,
            trigger='test',
        )

        self.assertEqual(result['summary']['failed_group_count'], 0)
        self.assertEqual(result['summary']['dispatched_group_count'], 2)
        self.assertEqual(len(self.client.post_calls), 2)
        self.assertEqual(len(self.client.plan_calls), 2)
        short_group = next(
            group for group in result['dispatch']['groups'] if group['decision_agent_name'] == 'short-term-decider'
        )
        self.assertTrue(short_group['recovered_from_retryable_error'])
        self.assertEqual(short_group['dispatch_status'], 'accepted')

        with session_scope() as session:
            short_row = session.get(SignalDeciderExecutionPlanItem, short_plan_item_id)
            self.assertEqual(short_row.execution_status, EXECUTION_STATUS_DELEGATED_FOR_EXEC_AGENT_SIZING)
            exec_dispatch = short_row.submission_payload['exec_dispatch']
            self.assertEqual(exec_dispatch['dispatch_status'], 'accepted')
            self.assertIsNone(exec_dispatch['last_error'])

    async def test_dispatch_next_open_plan_recovers_409_duplicate_from_matching_readback(self) -> None:
        self.client = DuplicateConflictAfterAcceptExecClient()
        self.service = SignalDeciderExecIntegrationService(client=self.client)

        result = await self.service.dispatch_next_open_plan(
            target_trade_date=self.target_trade_date,
            account_reference=self.account_reference,
            refresh_plan=False,
            dry_run=False,
            force=True,
            trigger='test',
        )

        self.assertEqual(result['summary']['failed_group_count'], 0)
        self.assertEqual(result['summary']['dispatched_group_count'], 1)
        self.assertEqual(len(self.client.post_calls), 1)
        self.assertEqual(len(self.client.plan_calls), 1)
        group = result['dispatch']['groups'][0]
        self.assertTrue(group['recovered_from_duplicate'])
        self.assertFalse(group['recovered_from_retryable_error'])
        self.assertEqual(group['dispatch_status'], 'accepted')

        with session_scope() as session:
            row = session.get(SignalDeciderExecutionPlanItem, self.plan_item_id)
            exec_dispatch = row.submission_payload['exec_dispatch']
            self.assertEqual(exec_dispatch['dispatch_status'], 'accepted')
            self.assertIsNone(exec_dispatch['last_error'])
            self.assertEqual(
                {link['core_item_key'] for link in exec_dispatch['item_links']},
                {
                    f'core-plan-{self.plan_item_id}:next_open_buy',
                    f'core-plan-{self.plan_item_id}:target_sell',
                    f'core-plan-{self.plan_item_id}:stop_sell',
                },
            )

    async def test_dispatch_next_open_plan_rejects_stale_timeout_readback_without_matching_core_keys(self) -> None:
        self.client = TimeoutWithStaleSamePlanReadbackExecClient()
        self.service = SignalDeciderExecIntegrationService(client=self.client)

        result = await self.service.dispatch_next_open_plan(
            target_trade_date=self.target_trade_date,
            account_reference=self.account_reference,
            refresh_plan=False,
            dry_run=False,
            force=True,
            trigger='test',
        )

        self.assertEqual(result['summary']['failed_group_count'], 1)
        self.assertEqual(result['summary']['dispatched_group_count'], 0)
        self.assertEqual(len(self.client.post_calls), 1)
        self.assertEqual(len(self.client.plan_calls), 1)
        group = result['dispatch']['groups'][0]
        self.assertEqual(group['dispatch_status'], 'unavailable')
        self.assertNotIn('recovered_from_duplicate', group)
        self.assertNotIn('recovered_from_retryable_error', group)

        with session_scope() as session:
            row = session.get(SignalDeciderExecutionPlanItem, self.plan_item_id)
            self.assertEqual(row.execution_status, EXECUTION_STATUS_SUBMIT_FAILED)
            exec_dispatch = row.submission_payload['exec_dispatch']
            self.assertEqual(exec_dispatch['dispatch_status'], 'unavailable')
            self.assertIsNone(exec_dispatch.get('last_success_at'))
            self.assertEqual(exec_dispatch['last_error']['code'], 'unavailable')
            self.assertEqual(
                {link['core_item_key'] for link in exec_dispatch['item_links']},
                {
                    f'core-plan-{self.plan_item_id}:next_open_buy',
                    f'core-plan-{self.plan_item_id}:target_sell',
                    f'core-plan-{self.plan_item_id}:stop_sell',
                },
            )

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
        short_item = next(item for item in overview['items'] if item['decision_agent_name'] == 'short-term-decider')
        self.assertEqual(short_item['raw_action'], 'watch_for_trigger')
        self.assertEqual(short_item['tactical_action'], 'watch_for_trigger')
        self.assertEqual(short_item['generic_action'], 'buy')
        self.assertEqual(short_item['handoff_intent'], 'intraday_watch_buy')
        self.assertEqual(short_item['approval_state'], 'not_dispatched')
        self.assertFalse(short_item['exec_agent_approved'])
        self.assertFalse(short_item['exec_monitoring_active'])

    async def test_exec_overview_offloads_plan_read_to_threadpool(self) -> None:
        to_thread_calls = []

        async def run_inline(func, *args, **kwargs):
            to_thread_calls.append((func, kwargs))
            return func(*args, **kwargs)

        to_thread = AsyncMock(side_effect=run_inline)
        with patch('app.orders.exec_integration.asyncio.to_thread', new=to_thread):
            overview = await self.service.list_signal_decider_exec_overview(
                target_trade_date=self.target_trade_date,
                refresh=False,
                trigger='test',
            )

        to_thread.assert_awaited_once()
        offloaded_func, offloaded_kwargs = to_thread_calls[0]
        self.assertIs(offloaded_func.__self__, signal_decider_next_open_execution_service)
        self.assertEqual(offloaded_func.__name__, 'list_plan_items')
        self.assertEqual(offloaded_kwargs['target_trade_date'], self.target_trade_date)
        self.assertIsNone(offloaded_kwargs['account_reference'])
        self.assertFalse(offloaded_kwargs['refresh'])
        self.assertEqual(len(overview['items']), 1)

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
            'symbols': {
                'filled_today': ['003280', '010950', '161390'],
                'filled_today_by_side': {
                    'buy': ['010950', '161390'],
                    'sell': ['003280', '010950', '161390'],
                },
                'filled_today_counts': {
                    'all': {'symbols': 3, 'fills': 5, 'quantity': 306},
                    'buy': {'symbols': 2, 'fills': 2, 'quantity': 3},
                    'sell': {'symbols': 3, 'fills': 3, 'quantity': 153},
                },
            },
            'daily_pnl': {
                'daily_realized_pnl': '8800.0000',
                'matched_sell_quantity': 3,
                'unmatched_sell_quantity': 150,
                'sold_symbols': ['003280', '010950', '161390'],
                'sold_symbol_count': 3,
                'closed_symbols': ['010950', '161390'],
                'closed_symbol_count': 2,
                'unmatched_sell_symbols': ['003280'],
                'unmatched_sell_symbol_count': 1,
                'pnl_method': 'fifo_internal_fills',
            },
            'monthly_pnl': {
                'monthly_realized_pnl': '8800.0000',
                'matched_sell_quantity': 3,
                'unmatched_sell_quantity': 150,
                'pnl_method': 'fifo_internal_fills',
            },
            'weekly_pnl': {
                'weekly_realized_pnl': '8800.0000',
                'matched_sell_quantity': 3,
                'unmatched_sell_quantity': 150,
                'closed_trade_count': 2,
                'win_count': 2,
                'loss_count': 0,
                'pnl_method': 'fifo_internal_fills',
            },
            'actionable_alerts': {
                'status': 'ok',
                'alerts': [],
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
        self.assertEqual(result['symbols']['filled_today_by_side']['sell'], ['003280', '010950', '161390'])
        self.assertEqual(result['daily_pnl']['sold_symbol_count'], 3)
        self.assertEqual(result['daily_pnl']['closed_symbol_count'], 2)
        self.assertEqual(result['daily_pnl']['unmatched_sell_symbols'], ['003280'])
        self.assertEqual(result['weekly_pnl']['weekly_realized_pnl'], '8800.0000')
        self.assertEqual(result['weekly_pnl']['closed_trade_count'], 2)
        self.assertEqual(result['monthly_pnl']['monthly_realized_pnl'], '8800.0000')
        self.assertEqual(result['actionable_alerts']['status'], 'ok')

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

        with (
            patch('app.orders.exec_integration.kis_realtime_price_service') as price_service,
            patch.object(self.service, '_load_live_portfolio_holding_symbols', new=AsyncMock(return_value=None)),
        ):
            price_service.price_fields_for_symbol.return_value = {
                'current_price': 50200,
                'price_as_of': '2026-04-30T00:07:30+00:00',
                'price_received_at': '2026-04-30T00:07:31+00:00',
                'price_source': 'kis_realtime_ws',
                'price_status': 'available',
            }
            price_service.observe_monitored_symbols.return_value = {'allowed_symbols': [self.symbol]}

            await self.service.list_signal_decider_exec_overview(
                target_trade_date=self.target_trade_date,
                refresh=True,
                trigger='test',
            )
            board = await self.service.get_runtime_daily_board(
                market_date=self.target_trade_date,
                refresh=False,
                trigger='test',
            )
            monitored = await self.service.list_runtime_monitored_symbols(
                market_date=self.target_trade_date,
                refresh=False,
                trigger='test',
            )
            detail = await self.service.get_runtime_symbol_detail(
                market_date=self.target_trade_date,
                symbol=self.symbol,
                refresh=False,
                trigger='test',
            )

        self.assertEqual(board['runtime']['status'], 'ready')
        self.assertEqual(board['summary']['filled'], 1)
        self.assertEqual(board['items'][0]['status'], 'filled')
        self.assertEqual(board['items'][0]['monitoring_state'], 'watching_target_stop')
        self.assertEqual(board['items'][0]['filled_quantity'], 5)
        self.assertEqual(board['items'][0]['avg_fill_price'], 50100)
        self.assertEqual(board['items'][0]['execution_plan_id'], plan_id)
        self.assertEqual(board['items'][0]['current_price'], 50200)
        self.assertEqual(board['items'][0]['price_status'], 'available')
        today_activity = board['items'][0]['today_activity']
        self.assertEqual(today_activity['buy_activity_status'], 'available')
        self.assertEqual(today_activity['buy_activity_source'], 'executions')
        self.assertEqual(today_activity['buy_filled_quantity_today'], 5)
        self.assertEqual(today_activity['buy_avg_fill_price_today'], 50100)
        self.assertEqual(today_activity['buy_last_filled_at'], '2026-04-30T00:06:00+00:00')
        self.assertEqual(today_activity['buy_broker_order_ids'], ['order-1'])
        self.assertEqual(today_activity['sell_activity_status'], 'none')
        self.assertEqual(today_activity['sell_filled_quantity_today'], 0)
        self.assertEqual(today_activity['net_open_quantity'], 5)

        self.assertEqual(monitored['summary']['items_total'], 1)
        self.assertEqual(monitored['items'][0]['symbol'], self.symbol)
        self.assertEqual(monitored['items'][0]['monitoring_state'], 'watching_target_stop')
        self.assertEqual(monitored['items'][0]['planned_intents'], ['stop_sell', 'target_sell'])
        self.assertEqual(monitored['items'][0]['current_price'], 50200)
        self.assertTrue(monitored['items'][0]['exec_monitoring_active'])
        self.assertTrue(monitored['items'][0]['exec_runtime_showable'])
        self.assertTrue(monitored['items'][0]['exec_agent_approved'])
        self.assertTrue(monitored['items'][0]['held_position'])
        self.assertEqual(monitored['items'][0]['position_quantity'], 5)
        self.assertEqual(monitored['items'][0]['position_average_price'], 50100)
        self.assertIn('target_sell_monitoring', monitored['items'][0]['monitoring_statuses'])
        self.assertIn('stop_sell_monitoring', monitored['items'][0]['monitoring_statuses'])
        self.assertEqual(monitored['items'][0]['target_price'], 54000)
        self.assertEqual(monitored['items'][0]['stop_loss_price'], 47000)

        self.assertEqual(detail['summary']['items_total'], 1)
        self.assertEqual(detail['monitoring']['state'], 'watching_target_stop')
        self.assertEqual(detail['current_price'], 50200)
        self.assertEqual(detail['items'][0]['status'], 'filled')
        self.assertEqual(detail['items'][0]['remote_state']['derived_status'], 'open')

    async def test_runtime_monitored_symbols_flags_exec_approved_watch_candidate(self) -> None:
        self.client.agent_sizing_payloads_by_agent['signal-decider'] = {
            'approved': True,
            'quantity': '5',
            'notional_krw': '250000.0000',
            'action': 'approve',
            'reason_code': 'small_tactical_upside',
            'target_profile': 'tactical_2pct',
            'target_tag': 'target_profile:tactical_2pct',
            'special_tactical_scalp': True,
        }

        await self.service.dispatch_next_open_plan(
            target_trade_date=self.target_trade_date,
            refresh_plan=False,
            dry_run=False,
            force=True,
            trigger='test',
        )

        with (
            patch('app.orders.exec_integration.kis_realtime_price_service') as price_service,
            patch.object(self.service, '_load_live_portfolio_holding_symbols', new=AsyncMock(return_value=None)),
        ):
            price_service.price_fields_for_symbol.return_value = {
                'current_price': 50200,
                'price_as_of': '2026-04-30T00:07:30+00:00',
                'price_received_at': '2026-04-30T00:07:31+00:00',
                'price_source': 'kis_realtime_ws',
                'price_status': 'available',
                'realtime_subscribed': True,
                'realtime_subscription_reason': 'subscribed',
                'display_price_available': True,
            }
            price_service.observe_monitored_symbols.return_value = {'allowed_symbols': [self.symbol]}

            monitored = await self.service.list_runtime_monitored_symbols(
                market_date=self.target_trade_date,
                refresh=False,
                trigger='test',
            )

        self.assertEqual(monitored['summary']['items_total'], 1)
        item = monitored['items'][0]
        self.assertEqual(item['symbol'], self.symbol)
        self.assertIn(item['monitoring_state'], {'watching_fill', 'reconciling'})
        self.assertTrue(item['exec_monitoring_active'])
        self.assertTrue(item['exec_runtime_showable'])
        self.assertEqual(item['exec_runtime_state'], 'exec_approved_watch')
        self.assertTrue(item['exec_agent_approved'])
        self.assertEqual(item['exec_agent_action'], 'approve')
        self.assertEqual(item['exec_agent_reason_code'], 'small_tactical_upside')
        self.assertEqual(item['exec_agent_target_profile'], 'tactical_2pct')
        self.assertEqual(item['exec_agent_target_tag'], 'target_profile:tactical_2pct')
        self.assertTrue(item['exec_agent_special_tactical_scalp'])
        self.assertFalse(item['held_position'])
        self.assertEqual(item['approved_quantity'], 5)
        self.assertEqual(item['approved_notional'], '250000.0000')
        self.assertIn('watch_monitoring_active', item['monitoring_statuses'])

    async def test_runtime_monitored_symbols_treats_accepted_entry_link_as_exec_approved_watch(self) -> None:
        with patch('app.orders.exec_integration.kis_realtime_price_service') as price_service:
            price_service.price_fields_for_symbol.return_value = {
                'current_price': 50200,
                'price_as_of': '2026-04-30T00:07:30+00:00',
                'price_received_at': '2026-04-30T00:07:31+00:00',
                'price_source': 'kis_realtime_ws',
                'price_status': 'available',
                'realtime_subscribed': True,
                'realtime_subscription_reason': 'subscribed',
                'display_price_available': True,
            }
            price_service.observe_monitored_symbols.return_value = {
                'allowed_symbols': ['WATCHACCEPTED', 'WATCHCREATED', 'WATCHPENDING', 'WATCHQUEUED']
            }

            for entry_status in ('accepted', 'created', 'pending', 'queued'):
                with self.subTest(entry_status=entry_status):
                    symbol = f'WATCH{entry_status.upper()}'
                    accepted_watch = self.service._build_runtime_board_item(
                        item={
                            'plan_item_id': 9101,
                            'account_reference': self.account_reference,
                            'account_label': 'Main',
                            'symbol': symbol,
                            'decision_agent_name': 'short-term-decider',
                            'estimated_reference_price': 2000,
                            'planned_quantity': '10',
                            'eligibility_status': 'eligible',
                            'execution_status': 'delegated_for_exec_agent_sizing',
                            'dispatch': {
                                'status': 'accepted',
                                'plan_id': f'exec-{entry_status}-watch',
                                'item_links': [
                                    {
                                        'intent': EXEC_INTENT_INTRADAY_WATCH_BUY,
                                        'side': 'buy',
                                        'symbol': symbol,
                                        'remote_status': entry_status,
                                        'response_item': {
                                            'status': entry_status,
                                            'execution_plan_item_id': f'exec-item-watch-{entry_status}',
                                        },
                                    }
                                ],
                            },
                            'trigger_price': '2050',
                            'chase_limit_price': '2070',
                            'target_price': '2200',
                            'stop_loss_price': '1900',
                        },
                        market_date=self.target_trade_date,
                        symbol_metadata_map={symbol: {'name': 'Accepted Watch', 'market': 'KOSPI'}},
                        runtime_status='ready',
                    )

                    payload = self.service._build_runtime_monitored_symbols_payload(
                        board={
                            'market_date': self.target_trade_date.isoformat(),
                            'runtime': {'status': 'ready', 'mode': 'mock'},
                            'items': [accepted_watch],
                        },
                        refresh=False,
                        refresh_state={'requested': False, 'status': 'not_requested'},
                        served_from_cache=False,
                    )

                    item = payload['items'][0]
                    self.assertEqual(item['symbol'], symbol)
                    self.assertTrue(item['exec_runtime_showable'])
                    self.assertTrue(item['exec_monitoring_active'])
                    self.assertTrue(item['exec_agent_approved'])
                    self.assertEqual(item['exec_agent_action'], 'approve')
                    self.assertEqual(item['exec_runtime_state'], 'exec_approved_watch')
                    self.assertIsNone(item['exec_runtime_exclusion_reason'])
                    self.assertIn(entry_status, item['monitoring_statuses'])
                    self.assertIn('watch_monitoring_active', item['monitoring_statuses'])
                    self.assertNotEqual(item['exec_runtime_state'], 'core_planned_only')

    async def test_runtime_monitored_symbols_flags_core_only_and_rejected_candidates(self) -> None:
        core_only = self.service._build_runtime_board_item(
            item={
                'plan_item_id': 9001,
                'account_reference': self.account_reference,
                'account_label': 'Main',
                'symbol': 'CORE',
                'decision_agent_name': 'signal-decider',
                'estimated_reference_price': 1000,
                'planned_quantity': '10',
                'eligibility_status': 'eligible',
                'execution_status': 'planned',
                'dispatch': {},
                'target_price': '1100',
                'stop_loss_price': '950',
            },
            market_date=self.target_trade_date,
            symbol_metadata_map={'CORE': {'name': 'Core only', 'market': 'KOSPI'}},
            runtime_status='ready',
        )
        rejected = self.service._build_runtime_board_item(
            item={
                'plan_item_id': 9002,
                'account_reference': self.account_reference,
                'account_label': 'Main',
                'symbol': 'REJ',
                'decision_agent_name': 'signal-decider',
                'estimated_reference_price': 2000,
                'planned_quantity': '10',
                'eligibility_status': 'eligible',
                'execution_status': 'planned',
                'dispatch': {
                    'status': 'accepted',
                    'plan_id': 'exec-rejected',
                    'item_links': [
                        {
                            'intent': EXEC_INTENT_INTRADAY_WATCH_BUY,
                            'side': 'buy',
                            'symbol': 'REJ',
                            'remote_status': 'rejected',
                            'response_item': {
                                'status': 'rejected',
                                'agent_sizing': {
                                    'approved': False,
                                    'action': 'reject',
                                    'reason_code': 'expired_setup',
                                },
                            },
                        }
                    ],
                },
                'target_price': '2200',
                'stop_loss_price': '1900',
            },
            market_date=self.target_trade_date,
            symbol_metadata_map={'REJ': {'name': 'Rejected', 'market': 'KOSPI'}},
            runtime_status='ready',
        )
        accepted_with_false_agent_decision = self.service._build_runtime_board_item(
            item={
                'plan_item_id': 9003,
                'account_reference': self.account_reference,
                'account_label': 'Main',
                'symbol': 'RNO',
                'decision_agent_name': 'short-term-decider',
                'estimated_reference_price': 3000,
                'planned_quantity': '10',
                'eligibility_status': 'eligible',
                'execution_status': 'delegated_for_exec_agent_sizing',
                'dispatch': {
                    'status': 'accepted',
                    'plan_id': 'exec-rejected-by-agent',
                    'item_links': [
                        {
                            'intent': EXEC_INTENT_INTRADAY_WATCH_BUY,
                            'side': 'buy',
                            'symbol': 'RNO',
                            'remote_status': 'accepted',
                            'response_item': {
                                'status': 'accepted',
                                'agent_sizing': {
                                    'approved': False,
                                    'action': 'reject',
                                    'reason_code': 'liquidity_block',
                                },
                            },
                        }
                    ],
                },
                'target_price': '3300',
                'stop_loss_price': '2800',
            },
            market_date=self.target_trade_date,
            symbol_metadata_map={'RNO': {'name': 'Rejected Accepted', 'market': 'KOSPI'}},
            runtime_status='ready',
        )

        with patch('app.orders.exec_integration.kis_realtime_price_service') as price_service:
            price_service.price_fields_for_symbol.return_value = {
                'current_price': None,
                'price_as_of': None,
                'price_received_at': None,
                'price_source': None,
                'price_status': 'unavailable',
                'realtime_subscribed': False,
                'realtime_subscription_reason': 'not_monitored',
                'display_price_available': False,
            }
            price_service.observe_monitored_symbols.return_value = {'allowed_symbols': []}

            payload = self.service._build_runtime_monitored_symbols_payload(
                board={
                    'market_date': self.target_trade_date.isoformat(),
                    'runtime': {'status': 'ready', 'mode': 'mock'},
                    'items': [core_only, rejected, accepted_with_false_agent_decision],
                },
                refresh=False,
                refresh_state={'requested': False, 'status': 'not_requested'},
                served_from_cache=False,
            )

        by_symbol = {item['symbol']: item for item in payload['items']}
        self.assertFalse(by_symbol['CORE']['exec_runtime_showable'])
        self.assertFalse(by_symbol['CORE']['exec_agent_approved'])
        self.assertFalse(by_symbol['CORE']['held_position'])
        self.assertEqual(by_symbol['CORE']['exec_runtime_state'], 'core_planned_only')
        self.assertEqual(by_symbol['CORE']['exec_runtime_exclusion_reason'], 'core_planned_not_exec_approved')
        self.assertFalse(by_symbol['REJ']['exec_runtime_showable'])
        self.assertFalse(by_symbol['REJ']['exec_agent_approved'])
        self.assertEqual(by_symbol['REJ']['exec_agent_action'], 'reject')
        self.assertEqual(by_symbol['REJ']['exec_agent_reason_code'], 'expired_setup')
        self.assertEqual(by_symbol['REJ']['exec_runtime_state'], 'exec_agent_rejected')
        self.assertEqual(by_symbol['REJ']['exec_runtime_exclusion_reason'], 'exec_agent_rejected')
        self.assertIn('rejected', by_symbol['REJ']['monitoring_statuses'])
        self.assertFalse(by_symbol['RNO']['exec_runtime_showable'])
        self.assertFalse(by_symbol['RNO']['exec_agent_approved'])
        self.assertEqual(by_symbol['RNO']['exec_agent_action'], 'reject')
        self.assertEqual(by_symbol['RNO']['exec_agent_reason_code'], 'liquidity_block')
        self.assertEqual(by_symbol['RNO']['exec_runtime_state'], 'exec_agent_rejected')
        self.assertEqual(by_symbol['RNO']['exec_runtime_exclusion_reason'], 'exec_agent_rejected')

    async def test_runtime_monitored_symbols_overlay_live_exec_watch_from_runtime_snapshot(self) -> None:
        core_only = self.service._build_runtime_board_item(
            item={
                'plan_item_id': 9003,
                'account_reference': self.account_reference,
                'account_label': 'Main',
                'symbol': self.symbol,
                'decision_agent_name': 'signal-decider',
                'estimated_reference_price': 50000,
                'planned_quantity': '10',
                'eligibility_status': 'eligible',
                'execution_status': 'accepted',
                'dispatch': {
                    'status': 'accepted',
                    'plan_id': f'core-signal-decider-{self.target_trade_date:%Y%m%d}-KIS_00000000_00',
                },
                'target_price': '54000',
                'stop_loss_price': '47000',
                'trigger_price': '50500',
                'chase_limit_price': '51500',
            },
            market_date=self.target_trade_date,
            symbol_metadata_map={self.symbol: {'name': 'Samsung', 'market': 'KOSPI'}},
            runtime_status='ready',
        )

        with patch('app.orders.exec_integration.kis_realtime_price_service') as price_service:
            price_service.price_fields_for_symbol.return_value = {
                'current_price': 50200,
                'price_as_of': '2026-04-30T00:07:30+00:00',
                'price_received_at': '2026-04-30T00:07:31+00:00',
                'price_source': 'kis_realtime_ws',
                'price_status': 'available',
                'realtime_subscribed': True,
                'realtime_subscription_reason': 'subscribed',
                'display_price_available': True,
            }
            price_service.observe_monitored_symbols.return_value = {'allowed_symbols': [self.symbol]}

            payload = self.service._build_runtime_monitored_symbols_payload(
                board={
                    'market_date': self.target_trade_date.isoformat(),
                    'runtime': {'status': 'ready', 'mode': 'mock'},
                    'items': [core_only],
                },
                refresh=False,
                refresh_state={'requested': False, 'status': 'not_requested'},
                served_from_cache=False,
                live_exec_monitoring_symbols={self.symbol},
            )

        item = payload['items'][0]
        self.assertTrue(item['exec_agent_approved'])
        self.assertTrue(item['exec_monitoring_active'])
        self.assertTrue(item['exec_runtime_showable'])
        self.assertEqual(item['exec_runtime_state'], 'exec_approved_watch')
        self.assertIsNone(item['exec_runtime_exclusion_reason'])
        self.assertIn('watch_monitoring_active', item['monitoring_statuses'])

    async def test_runtime_monitored_symbols_extracts_exec_monitored_symbol_list(self) -> None:
        payload = {
            'liveness': {
                'autonomous': {
                    'monitoring': {
                        'last_cycle': {
                            'monitored_symbols': ['005930', '000660', '005930'],
                            'items': [
                                {
                                    'symbol': 'SHOULD_NOT_LIMIT_TO_ITEMS',
                                    'intent': 'intraday_watch_buy',
                                    'status': 'watch_monitoring_active',
                                }
                            ],
                        }
                    }
                }
            }
        }

        symbols = self.service._extract_live_exec_monitoring_symbols(
            payload=payload,
            market_date=self.target_trade_date,
        )

        self.assertEqual(symbols, {'005930', '000660'})

    async def test_runtime_monitored_symbols_include_historical_held_position(self) -> None:
        with session_scope() as session:
            row = session.get(SignalDeciderExecutionPlanItem, self.plan_item_id)
            row.target_trade_date = self.market_date
            row.source_market_date = date(2026, 4, 28)
            row.execution_status = EXECUTION_STATUS_SUBMITTED
            row.submitted_at = datetime(2026, 4, 29, 0, 0, tzinfo=timezone.utc)
            row.submission_payload = {
                'mode': 'exec_dispatch',
                'exec_dispatch': {
                    'plan_id': 'historical-held-plan',
                    'dispatch_status': 'accepted',
                    'last_attempt_at': '2026-04-29T00:00:00+00:00',
                    'last_success_at': '2026-04-29T00:00:00+00:00',
                    'item_links': [
                        {
                            'core_item_key': f'core-plan-{self.plan_item_id}:next_open_buy',
                            'intent': EXEC_INTENT_NEXT_OPEN_BUY,
                            'side': 'buy',
                            'symbol': self.symbol,
                            'remote_status': 'filled',
                        },
                        {
                            'core_item_key': f'core-plan-{self.plan_item_id}:target_sell',
                            'intent': EXEC_INTENT_TARGET_SELL,
                            'side': 'sell',
                            'symbol': self.symbol,
                        },
                        {
                            'core_item_key': f'core-plan-{self.plan_item_id}:stop_sell',
                            'intent': EXEC_INTENT_STOP_SELL,
                            'side': 'sell',
                            'symbol': self.symbol,
                        },
                    ],
                    'readback': {
                        'last_synced_at': '2026-04-29T00:07:00+00:00',
                        'runtime': {'status': 'ok', 'payload': {'ready': True}},
                        'orders': {'status': 'ok', 'count': 0, 'items': []},
                        'executions': {
                            'status': 'ok',
                            'count': 1,
                            'items': [
                                {
                                    'broker_order_id': 'historical-buy-order',
                                    'symbol': self.symbol,
                                    'side': 'buy',
                                    'quantity': '7',
                                    'fill_price': '50100',
                                    'executed_at': '2026-04-29T00:06:00+00:00',
                                }
                            ],
                        },
                        'positions': {
                            'status': 'ok',
                            'count': 1,
                            'items': [
                                {
                                    'account_reference': self.account_reference,
                                    'symbol': self.symbol,
                                    'quantity': '7',
                                    'average_price': '50100',
                                    'updated_at': '2026-04-29T00:07:00+00:00',
                                }
                            ],
                        },
                        'derived_state': {
                            'derived_status': 'open',
                            'entry_filled_quantity': '7',
                            'exit_filled_quantity': None,
                            'open_quantity': '7',
                            'closed_quantity': '0',
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

        with (
            patch('app.orders.exec_integration.kis_realtime_price_service') as price_service,
            patch.object(self.service, '_load_live_portfolio_holding_symbols', new=AsyncMock(return_value=None)),
        ):
            price_service.price_fields_for_symbol.return_value = {
                'current_price': 50200,
                'price_as_of': '2026-04-30T00:07:30+00:00',
                'price_received_at': '2026-04-30T00:07:31+00:00',
                'price_source': 'kis_realtime_ws',
                'price_status': 'available',
                'realtime_subscribed': True,
                'realtime_subscription_reason': 'subscribed',
                'display_price_available': True,
            }
            price_service.observe_monitored_symbols.return_value = {'allowed_symbols': [self.symbol]}

            monitored = await self.service.list_runtime_monitored_symbols(
                market_date=self.target_trade_date,
                refresh=False,
                trigger='test',
            )

        self.assertEqual(monitored['summary']['items_total'], 1)
        item = monitored['items'][0]
        self.assertEqual(item['symbol'], self.symbol)
        self.assertIn(item['monitoring_state'], {'watching_target_stop', 'reconciling'})
        self.assertTrue(item['held_position'])
        self.assertTrue(item['exec_runtime_showable'])
        self.assertEqual(item['position_quantity'], 7)
        self.assertEqual(item['position_average_price'], 50100)
        self.assertIn('target_sell_monitoring', item['monitoring_statuses'])
        self.assertIn('stop_sell_monitoring', item['monitoring_statuses'])

        with patch('app.orders.exec_integration.kis_realtime_price_service') as price_service:
            price_service.price_fields_for_symbol.return_value = {
                'current_price': None,
                'price_as_of': None,
                'price_received_at': None,
                'price_source': None,
                'price_status': 'unavailable',
                'realtime_subscribed': False,
                'realtime_subscription_reason': 'not_monitored',
                'display_price_available': False,
            }
            price_service.observe_monitored_symbols.return_value = {'allowed_symbols': []}

            monitored_without_live_holding = self.service._build_runtime_monitored_symbols_payload(
                board={
                    'market_date': self.target_trade_date.isoformat(),
                    'runtime': {'status': 'ready', 'mode': 'mock'},
                    'items': [],
                },
                refresh=False,
                refresh_state={'requested': False, 'status': 'not_requested'},
                served_from_cache=False,
                live_holding_symbols=set(),
            )

        self.assertEqual(monitored_without_live_holding['summary']['items_total'], 0)
        self.assertEqual(monitored_without_live_holding['items'], [])

    async def test_runtime_monitored_symbols_use_cached_display_fallback_when_capacity_limited(self) -> None:
        fallback_symbol = '000660'
        self._seed_signal_candidate(
            symbol=fallback_symbol,
            name_ko='SK hynix',
            decision_run_id='exec-run-2',
            client_order_id='SDNO-20260430-000660',
        )

        def realtime_fields(symbol: str) -> dict:
            if symbol == self.symbol:
                return {
                    'current_price': 50200,
                    'price_as_of': '2026-04-30T00:07:30+00:00',
                    'price_received_at': '2026-04-30T00:07:31+00:00',
                    'price_source': 'kis_realtime_ws',
                    'price_status': 'available',
                    'realtime_subscribed': True,
                    'realtime_subscription_reason': 'subscribed',
                    'display_price_available': True,
                }
            return {
                'current_price': None,
                'price_as_of': None,
                'price_received_at': None,
                'price_source': None,
                'price_status': 'limited',
                'realtime_subscribed': False,
                'realtime_subscription_reason': 'capacity_limited',
                'display_price_available': False,
            }

        def current_quote(symbol: str) -> dict | None:
            if symbol != fallback_symbol:
                return None
            return {
                'symbol': fallback_symbol,
                'price': 182500,
                'cached_at': '2026-04-30T00:06:00+00:00',
                'as_of': '2026-04-30T00:06:00+00:00',
                'trade_date': '2026-04-30',
            }

        with (
            patch('app.orders.exec_integration.kis_realtime_price_service') as price_service,
            patch('app.quotes.display_prices.load_current_quote', side_effect=current_quote),
            patch('app.quotes.display_prices.load_daily_bars_for_symbol') as load_daily_bars,
            patch('app.brokers.kis.client.KISClient.fetch_domestic_quote', new_callable=AsyncMock) as fetch_quote,
        ):
            price_service.price_fields_for_symbol.side_effect = realtime_fields
            price_service.observe_monitored_symbols.return_value = {
                'allowed_symbols': [self.symbol],
                'limited_symbols': [fallback_symbol],
            }

            monitored = await self.service.list_runtime_monitored_symbols(
                market_date=self.target_trade_date,
                refresh=False,
                trigger='test',
            )
            cached_monitored = await self.service.list_runtime_monitored_symbols(
                market_date=self.target_trade_date,
                refresh=False,
                trigger='test',
            )

        by_symbol = {item['symbol']: item for item in monitored['items']}
        fallback_item = by_symbol[fallback_symbol]
        self.assertEqual(fallback_item['current_price'], 182500)
        self.assertEqual(fallback_item['price_source'], 'cached_quote')
        self.assertEqual(fallback_item['price_status'], 'fallback')
        self.assertTrue(fallback_item['display_price_available'])
        self.assertFalse(fallback_item['realtime_subscribed'])
        self.assertEqual(fallback_item['realtime_subscription_reason'], 'capacity_limited')
        self.assertEqual(monitored['summary']['desired_symbol_count'], 2)
        self.assertEqual(monitored['summary']['realtime_subscribed_count'], 1)
        self.assertEqual(monitored['summary']['capacity_limited_count'], 1)
        self.assertEqual(monitored['summary']['cached_fallback_covered_count'], 1)
        self.assertEqual(monitored['summary']['truly_unavailable_count'], 0)
        self.assertEqual(cached_monitored['summary']['cached_fallback_covered_count'], 1)
        self.assertEqual(monitored['realtime_subscription']['desired_symbol_count'], 2)
        load_daily_bars.assert_not_called()
        fetch_quote.assert_not_called()

    async def test_runtime_symbol_detail_labels_stale_cached_display_fallback(self) -> None:
        with (
            patch('app.orders.exec_integration.kis_realtime_price_service') as price_service,
            patch(
                'app.quotes.display_prices.load_current_quote',
                return_value={
                    'symbol': self.symbol,
                    'price': 49800,
                    'cached_at': '2026-04-29T06:10:00+00:00',
                    'trade_date': '2026-04-29',
                },
            ),
            patch('app.quotes.display_prices.load_daily_bars_for_symbol') as load_daily_bars,
        ):
            price_service.price_fields_for_symbol.return_value = {
                'current_price': None,
                'price_as_of': None,
                'price_received_at': None,
                'price_source': None,
                'price_status': 'unavailable',
                'realtime_subscribed': False,
                'realtime_subscription_reason': 'not_monitored',
                'display_price_available': False,
            }

            detail = await self.service.get_runtime_symbol_detail(
                market_date=self.target_trade_date,
                symbol=self.symbol,
                refresh=False,
                trigger='test',
            )

        self.assertEqual(detail['current_price'], 49800)
        self.assertEqual(detail['price_source'], 'cached_quote')
        self.assertEqual(detail['price_status'], 'stale')
        self.assertTrue(detail['display_price_available'])
        self.assertEqual(detail['items'][0]['price_status'], 'stale')
        load_daily_bars.assert_not_called()

    async def test_runtime_monitored_symbols_observe_high_priority_symbols_first(self) -> None:
        board = {
            'market_date': self.target_trade_date.isoformat(),
            'runtime': {'status': 'ready', 'mode': 'mock'},
            'items': [
                {
                    'symbol': 'LOW',
                    'name': 'Low priority',
                    'market': 'KOSPI',
                    'status': 'planned',
                    'monitoring_state': 'idle',
                    'planned_intents': ['target_sell'],
                    'decision_agent_name': 'signal-decider',
                },
                {
                    'symbol': 'HIGH',
                    'name': 'Held position',
                    'market': 'KOSPI',
                    'status': 'filled',
                    'monitoring_state': 'watching_target_stop',
                    'planned_intents': ['stop_sell', 'target_sell'],
                    'open_quantity': 5,
                    'decision_agent_name': 'signal-decider',
                },
                {
                    'symbol': 'MID',
                    'name': 'Intraday watch',
                    'market': 'KOSPI',
                    'intent': 'intraday_watch_buy',
                    'status': 'accepted',
                    'monitoring_state': 'watching_fill',
                    'trigger_price': 50500,
                    'chase_limit_price': 51500,
                    'decision_agent_name': 'short-term-decider',
                },
            ],
        }
        with patch('app.orders.exec_integration.kis_realtime_price_service') as price_service:
            price_service.price_fields_for_symbol.return_value = {
                'current_price': 100,
                'price_as_of': '2026-04-30T00:00:00+00:00',
                'price_received_at': '2026-04-30T00:00:01+00:00',
                'price_source': 'kis_realtime_ws',
                'price_status': 'available',
                'realtime_subscribed': True,
                'realtime_subscription_reason': 'subscribed',
                'display_price_available': True,
            }

            payload = self.service._build_runtime_monitored_symbols_payload(
                board=board,
                refresh=False,
                refresh_state={'requested': False, 'status': 'not_requested'},
                served_from_cache=False,
            )

        observed_symbols = list(price_service.observe_monitored_symbols.call_args.args[0])
        self.assertEqual(observed_symbols, ['HIGH', 'MID', 'LOW'])
        self.assertTrue(price_service.observe_monitored_symbols.call_args.kwargs['preserve_order'])
        self.assertEqual(payload['realtime_subscription']['desired_symbols'], ['HIGH', 'MID', 'LOW'])

    async def test_runtime_dashboard_readback_facts_override_blocked_eligibility(self) -> None:
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
            'monitoring': {
                'alive': True,
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

        await self.service.list_signal_decider_exec_overview(
            target_trade_date=self.target_trade_date,
            refresh=True,
            trigger='test',
        )
        with session_scope() as session:
            row = session.get(SignalDeciderExecutionPlanItem, self.plan_item_id)
            row.eligibility_status = 'blocked'
            row.validation_payload = {
                **(row.validation_payload or {}),
                'blocking_reasons': [{'code': 'stale_planning_block', 'message': 'stale planning block'}],
            }
            session.flush()

        remote_call_counts = (
            self.client.runtime_calls,
            len(self.client.plan_calls),
            len(self.client.order_calls),
            len(self.client.execution_calls),
            len(self.client.position_calls),
        )
        with patch.object(self.service, '_load_live_exec_monitoring_symbols', new=AsyncMock(return_value=None)):
            board = await self.service.get_runtime_daily_board(
                market_date=self.target_trade_date,
                refresh=False,
                trigger='test',
            )
            monitored = await self.service.list_runtime_monitored_symbols(
                market_date=self.target_trade_date,
                refresh=False,
                trigger='test',
            )

        self.assertEqual(board['summary']['filled'], 1)
        self.assertEqual(board['summary']['failed'], 0)
        self.assertEqual(board['items'][0]['status'], 'filled')
        self.assertEqual(board['items'][0]['monitoring_state'], 'watching_target_stop')
        self.assertEqual(board['items'][0]['filled_quantity'], 5)
        self.assertEqual(board['items'][0]['open_quantity'], 5)
        self.assertIsNone(board['items'][0]['failure_reason'])
        self.assertEqual(monitored['summary']['items_total'], 1)
        self.assertEqual(monitored['items'][0]['monitoring_state'], 'watching_target_stop')
        self.assertEqual(
            remote_call_counts,
            (
                self.client.runtime_calls,
                len(self.client.plan_calls),
                len(self.client.order_calls),
                len(self.client.execution_calls),
                len(self.client.position_calls),
            ),
        )

    async def test_runtime_dashboard_filled_order_position_overrides_blocked_eligibility_without_execution(self) -> None:
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
                    'side': 'buy',
                    'status': 'filled',
                    'quantity': '5',
                    'updated_at': '2026-04-30T00:05:00+00:00',
                }
            ]
        }
        self.client.execution_response = {'items': []}
        self.client.position_response = {
            'items': [
                {
                    'account_reference': self.account_reference,
                    'symbol': self.symbol,
                    'quantity': '5',
                    'average_price': '50100',
                    'updated_at': '2026-04-30T00:07:00+00:00',
                }
            ]
        }

        await self.service.list_signal_decider_exec_overview(
            target_trade_date=self.target_trade_date,
            refresh=True,
            trigger='test',
        )
        with session_scope() as session:
            row = session.get(SignalDeciderExecutionPlanItem, self.plan_item_id)
            row.eligibility_status = 'blocked'
            row.validation_payload = {
                **(row.validation_payload or {}),
                'blocking_reasons': [{'code': 'stale_planning_block', 'message': 'stale planning block'}],
            }
            session.flush()

        board = await self.service.get_runtime_daily_board(
            market_date=self.target_trade_date,
            refresh=False,
            trigger='test',
        )

        self.assertEqual(board['summary']['filled'], 1)
        self.assertEqual(board['summary']['failed'], 0)
        self.assertEqual(board['items'][0]['status'], 'filled')
        self.assertEqual(board['items'][0]['filled_quantity'], 5)
        self.assertEqual(board['items'][0]['open_quantity'], 5)
        self.assertIsNone(board['items'][0]['failure_reason'])

    async def test_runtime_dashboard_position_without_plan_fill_evidence_does_not_mark_filled(self) -> None:
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
                    'account_reference': self.account_reference,
                    'symbol': self.symbol,
                    'quantity': '5',
                    'average_price': '50100',
                    'updated_at': '2026-04-30T00:07:00+00:00',
                }
            ]
        }

        await self.service.list_signal_decider_exec_overview(
            target_trade_date=self.target_trade_date,
            refresh=True,
            trigger='test',
        )
        board = await self.service.get_runtime_daily_board(
            market_date=self.target_trade_date,
            refresh=False,
            trigger='test',
        )

        self.assertEqual(board['summary']['filled'], 0)
        self.assertEqual(board['summary']['accepted'], 1)
        self.assertEqual(board['items'][0]['status'], 'accepted')
        self.assertEqual(board['items'][0]['open_quantity'], 0)

        with session_scope() as session:
            row = session.get(SignalDeciderExecutionPlanItem, self.plan_item_id)
            row.eligibility_status = 'blocked'
            row.validation_payload = {
                **(row.validation_payload or {}),
                'blocking_reasons': [{'code': 'stale_planning_block', 'message': 'stale planning block'}],
            }
            session.flush()
        self.service._dashboard_cache.clear()

        blocked_board = await self.service.get_runtime_daily_board(
            market_date=self.target_trade_date,
            refresh=False,
            trigger='test',
        )

        self.assertEqual(blocked_board['summary']['filled'], 0)
        self.assertEqual(blocked_board['summary']['failed'], 1)
        self.assertEqual(blocked_board['items'][0]['status'], 'failed')
        self.assertEqual(blocked_board['items'][0]['open_quantity'], 0)

    async def test_runtime_dashboard_sell_fill_overrides_blocked_eligibility(self) -> None:
        await self.service.dispatch_next_open_plan(
            target_trade_date=self.target_trade_date,
            refresh_plan=False,
            dry_run=False,
            force=True,
            trigger='test',
        )
        self._mark_plan_item_blocked_with_readback(
            dispatch_status='failed',
            execution_status='submit_failed',
            readback={
                'last_synced_at': '2026-04-30T05:31:00+00:00',
                'runtime': {
                    'status': 'ok',
                    'payload': {
                        'mode': 'mock',
                        'ready': True,
                        'monitoring_alive': True,
                    },
                },
                'derived_state': {
                    'derived_status': 'closed',
                    'entry_filled_quantity': '5',
                    'exit_filled_quantity': '5',
                    'open_quantity': '0',
                    'closed_quantity': '5',
                },
                'orders': {
                    'items': [
                        {
                            'id': 'sell-order-1',
                            'broker_order_id': 'sell-order-1',
                            'symbol': self.symbol,
                            'side': 'sell',
                            'status': 'filled',
                            'updated_at': '2026-04-30T05:31:00+00:00',
                        }
                    ]
                },
                'executions': {
                    'items': [
                        {
                            'broker_order_id': 'sell-order-1',
                            'symbol': self.symbol,
                            'side': 'sell',
                            'quantity': '5',
                            'price': '51000',
                            'executed_at': '2026-04-30T05:31:00+00:00',
                        }
                    ]
                },
                'positions': {'items': []},
            },
        )

        board = await self.service.get_runtime_daily_board(
            market_date=self.target_trade_date,
            refresh=False,
            trigger='test',
        )

        self.assertEqual(board['summary']['filled'], 1)
        self.assertEqual(board['summary']['failed'], 0)
        self.assertEqual(board['items'][0]['status'], 'filled')
        self.assertEqual(board['items'][0]['filled_quantity'], 5)
        self.assertEqual(board['items'][0]['exit_filled_quantity'], 5)
        self.assertEqual(board['items'][0]['open_quantity'], 0)
        self.assertEqual(board['items'][0]['broker_order_id'], 'sell-order-1')
        today_activity = board['items'][0]['today_activity']
        self.assertEqual(today_activity['buy_activity_status'], 'none')
        self.assertEqual(today_activity['buy_filled_quantity_today'], 0)
        self.assertIsNone(today_activity['buy_avg_fill_price_today'])
        self.assertIsNone(today_activity['buy_last_filled_at'])
        self.assertEqual(today_activity['buy_broker_order_ids'], [])
        self.assertEqual(today_activity['sell_activity_status'], 'available')
        self.assertEqual(today_activity['sell_activity_source'], 'executions')
        self.assertEqual(today_activity['sell_filled_quantity_today'], 5)
        self.assertEqual(today_activity['sell_avg_fill_price_today'], 51000)
        self.assertEqual(today_activity['sell_last_filled_at'], '2026-04-30T05:31:00+00:00')
        self.assertEqual(today_activity['sell_broker_order_ids'], ['sell-order-1'])
        self.assertEqual(today_activity['net_open_quantity'], 0)

    async def test_runtime_dashboard_today_activity_exposes_mixed_buy_and_sell_fills(self) -> None:
        await self.service.dispatch_next_open_plan(
            target_trade_date=self.target_trade_date,
            refresh_plan=False,
            dry_run=False,
            force=True,
            trigger='test',
        )
        self._mark_plan_item_blocked_with_readback(
            dispatch_status='failed',
            execution_status='submit_failed',
            readback={
                'last_synced_at': '2026-04-30T05:31:00+00:00',
                'runtime': {
                    'status': 'ok',
                    'payload': {
                        'mode': 'mock',
                        'ready': True,
                        'monitoring_alive': True,
                    },
                },
                'derived_state': {
                    'derived_status': 'open',
                    'entry_filled_quantity': '5',
                    'exit_filled_quantity': '2',
                    'open_quantity': '3',
                    'closed_quantity': '2',
                },
                'orders': {
                    'items': [
                        {
                            'id': 'buy-order-1',
                            'broker_order_id': 'buy-order-1',
                            'symbol': self.symbol,
                            'side': 'buy',
                            'status': 'filled',
                            'updated_at': '2026-04-30T00:06:30+00:00',
                        },
                        {
                            'id': 'sell-order-1',
                            'broker_order_id': 'sell-order-1',
                            'symbol': self.symbol,
                            'side': 'sell',
                            'status': 'filled',
                            'updated_at': '2026-04-30T05:31:30+00:00',
                        },
                    ]
                },
                'executions': {
                    'items': [
                        {
                            'broker_order_id': 'buy-order-1',
                            'symbol': self.symbol,
                            'side': 'buy',
                            'quantity': '5',
                            'price': '50100',
                            'executed_at': '2026-04-30T00:06:00+00:00',
                        },
                        {
                            'broker_order_id': 'sell-order-1',
                            'symbol': self.symbol,
                            'side': 'sell',
                            'quantity': '2',
                            'price': '51200',
                            'executed_at': '2026-04-30T05:31:00+00:00',
                        },
                    ]
                },
                'positions': {'items': []},
            },
        )

        board = await self.service.get_runtime_daily_board(
            market_date=self.target_trade_date,
            refresh=False,
            trigger='test',
        )

        self.assertEqual(board['summary']['partial'], 1)
        item = board['items'][0]
        self.assertEqual(item['status'], 'partial')
        self.assertEqual(item['filled_quantity'], 5)
        self.assertEqual(item['exit_filled_quantity'], 2)
        self.assertEqual(item['open_quantity'], 3)
        today_activity = item['today_activity']
        self.assertEqual(today_activity['buy_filled_quantity_today'], 5)
        self.assertEqual(today_activity['buy_avg_fill_price_today'], 50100)
        self.assertEqual(today_activity['buy_last_filled_at'], '2026-04-30T00:06:00+00:00')
        self.assertEqual(today_activity['buy_broker_order_ids'], ['buy-order-1'])
        self.assertEqual(today_activity['sell_filled_quantity_today'], 2)
        self.assertEqual(today_activity['sell_avg_fill_price_today'], 51200)
        self.assertEqual(today_activity['sell_last_filled_at'], '2026-04-30T05:31:00+00:00')
        self.assertEqual(today_activity['sell_broker_order_ids'], ['sell-order-1'])
        self.assertEqual(today_activity['net_open_quantity'], 3)

    async def test_runtime_dashboard_uses_derived_open_quantity_for_held_position_contract(self) -> None:
        await self.service.dispatch_next_open_plan(
            target_trade_date=self.target_trade_date,
            refresh_plan=False,
            dry_run=False,
            force=True,
            trigger='test',
        )
        self._mark_plan_item_blocked_with_readback(
            readback={
                'last_synced_at': '2026-04-30T05:31:00+00:00',
                'runtime': {'status': 'ok', 'payload': {'mode': 'mock', 'ready': True}},
                'derived_state': {
                    'derived_status': 'open',
                    'entry_filled_quantity': '5',
                    'exit_filled_quantity': '2',
                    'open_quantity': '3',
                    'closed_quantity': '2',
                },
                'orders': {'items': []},
                'executions': {
                    'items': [
                        {
                            'broker_order_id': 'buy-order-1',
                            'symbol': self.symbol,
                            'side': 'buy',
                            'quantity': '5',
                            'price': '50100',
                            'executed_at': '2026-04-30T00:06:00+00:00',
                        },
                        {
                            'broker_order_id': 'sell-order-1',
                            'symbol': self.symbol,
                            'side': 'sell',
                            'quantity': '2',
                            'price': '51200',
                            'executed_at': '2026-04-30T05:31:00+00:00',
                        },
                    ]
                },
                'positions': {'items': []},
            },
        )

        board = await self.service.get_runtime_daily_board(
            market_date=self.target_trade_date,
            refresh=False,
            trigger='test',
        )

        item = board['items'][0]
        self.assertEqual(item['open_quantity'], 3)
        self.assertTrue(item['held_position'])
        self.assertTrue(item['exec_monitoring_active'])
        self.assertTrue(item['exec_runtime_showable'])
        self.assertEqual(item['exec_runtime_state'], 'held_position')
        self.assertEqual(item['position_quantity'], 3)
        self.assertIsNone(item['position_average_price'])
        self.assertIn('target_sell_monitoring', item['monitoring_statuses'])
        self.assertIn('stop_sell_monitoring', item['monitoring_statuses'])

    async def test_runtime_dashboard_today_activity_marks_untimestamped_side_unavailable(self) -> None:
        await self.service.dispatch_next_open_plan(
            target_trade_date=self.target_trade_date,
            refresh_plan=False,
            dry_run=False,
            force=True,
            trigger='test',
        )
        self._mark_plan_item_blocked_with_readback(
            readback={
                'last_synced_at': '2026-04-30T00:07:00+00:00',
                'runtime': {'status': 'ok', 'payload': {'mode': 'mock', 'ready': True}},
                'derived_state': {
                    'derived_status': 'open',
                    'entry_filled_quantity': '5',
                    'exit_filled_quantity': None,
                    'open_quantity': '5',
                    'closed_quantity': '0',
                },
                'orders': {'items': []},
                'executions': {
                    'items': [
                        {
                            'broker_order_id': 'buy-order-no-time',
                            'symbol': self.symbol,
                            'side': 'buy',
                            'quantity': '5',
                            'price': '50100',
                        }
                    ]
                },
                'positions': {'items': []},
            },
        )

        board = await self.service.get_runtime_daily_board(
            market_date=self.target_trade_date,
            refresh=False,
            trigger='test',
        )

        today_activity = board['items'][0]['today_activity']
        self.assertEqual(today_activity['buy_activity_status'], 'unavailable')
        self.assertIsNone(today_activity['buy_filled_quantity_today'])
        self.assertIsNone(today_activity['buy_avg_fill_price_today'])
        self.assertIsNone(today_activity['buy_last_filled_at'])
        self.assertEqual(today_activity['buy_broker_order_ids'], [])
        self.assertEqual(today_activity['sell_activity_status'], 'none')

    async def test_runtime_dashboard_today_activity_caps_ids_and_omits_raw_readback_blobs(self) -> None:
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
                    'id': f'buy-order-{index}',
                    'broker_order_id': f'buy-order-{index}',
                    'execution_plan_item_id': 'exec-item-1',
                    'symbol': self.symbol,
                    'side': 'buy',
                    'status': 'filled',
                    'quantity': '1',
                    'filled_quantity': '1',
                    'avg_fill_price': str(50100 + index),
                    'updated_at': '2026-04-30T00:06:30+00:00',
                    'raw_blob': 'x' * 4096,
                }
                for index in range(7)
            ]
        }
        self.client.execution_response = {
            'items': [
                {
                    'broker_order_id': f'buy-order-{index}',
                    'execution_plan_item_id': 'exec-item-1',
                    'symbol': self.symbol,
                    'side': 'buy',
                    'quantity': '1',
                    'fill_price': str(50100 + index),
                    'executed_at': f'2026-04-30T00:0{index}:00+00:00',
                    'raw_blob': 'y' * 4096,
                }
                for index in range(7)
            ]
        }
        self.client.position_response = {
            'items': [
                {
                    'account_reference': self.account_reference,
                    'symbol': self.symbol,
                    'quantity': '7',
                    'average_price': '50103',
                    'updated_at': '2026-04-30T00:07:00+00:00',
                    'raw_blob': 'z' * 4096,
                }
            ]
        }

        await self.service.list_signal_decider_exec_overview(
            target_trade_date=self.target_trade_date,
            refresh=True,
            trigger='test',
        )
        board = await self.service.get_runtime_daily_board(
            market_date=self.target_trade_date,
            refresh=False,
            trigger='test',
        )

        today_activity = board['items'][0]['today_activity']
        self.assertEqual(today_activity['buy_filled_quantity_today'], 7)
        self.assertEqual(today_activity['buy_avg_fill_price_today'], 50103)
        self.assertEqual(
            today_activity['buy_broker_order_ids'],
            ['buy-order-0', 'buy-order-1', 'buy-order-2', 'buy-order-3', 'buy-order-4'],
        )
        self.assertEqual(today_activity['buy_broker_order_ids_omitted_count'], 2)
        board_payload = json.dumps(board, ensure_ascii=False, sort_keys=True)
        self.assertNotIn('raw_blob', board_payload)
        self.assertNotIn('xxxx', board_payload)
        self.assertLess(len(board_payload.encode('utf-8')), 20000)

    async def test_runtime_monitored_symbols_include_target_stop_watch_from_partial_read_model(self) -> None:
        with session_scope() as session:
            row = session.get(SignalDeciderExecutionPlanItem, self.plan_item_id)
            row.eligibility_status = 'blocked'
            row.validation_payload = {
                **(row.validation_payload or {}),
                'blocking_reasons': [{'code': 'stale_planning_block', 'message': 'stale planning block'}],
            }
            session.flush()

        with (
            patch('app.orders.exec_integration.kis_realtime_price_service') as price_service,
            patch.object(self.service, '_load_live_exec_monitoring_symbols', new=AsyncMock(return_value=None)),
        ):
            price_service.price_fields_for_symbol.return_value = {
                'current_price': None,
                'price_as_of': None,
                'price_received_at': None,
                'price_source': None,
                'price_status': 'unavailable',
            }
            price_service.observe_monitored_symbols.return_value = {'allowed_symbols': [self.symbol]}

            monitored = await self.service.list_runtime_monitored_symbols(
                market_date=self.target_trade_date,
                refresh=False,
                trigger='test',
            )

        self.assertEqual(monitored['summary']['items_total'], 1)
        self.assertEqual(monitored['summary']['watching_fill'], 1)
        self.assertEqual(monitored['freshness']['status'], 'partial')
        self.assertEqual(monitored['freshness']['readback_missing_count'], 1)
        self.assertEqual(monitored['items'][0]['symbol'], self.symbol)
        self.assertEqual(monitored['items'][0]['monitoring_state'], 'watching_fill')
        self.assertEqual(monitored['items'][0]['planned_intents'], ['stop_sell', 'target_sell'])
        self.assertEqual(self.client.runtime_calls, 0)
        self.assertEqual(self.client.order_calls, [])
        self.assertEqual(self.client.execution_calls, [])
        self.assertEqual(self.client.position_calls, [])

    async def test_runtime_dashboard_refresh_true_returns_local_read_model_while_refresh_runs(self) -> None:
        await self.service.dispatch_next_open_plan(
            target_trade_date=self.target_trade_date,
            refresh_plan=False,
            dry_run=False,
            force=True,
            trigger='test',
        )
        original_overview = self.service.list_signal_decider_exec_overview
        refresh_started = asyncio.Event()
        release_refresh = asyncio.Event()
        refresh_call_count = 0

        async def slow_refresh_overview(**kwargs):
            nonlocal refresh_call_count
            if kwargs.get('refresh'):
                refresh_call_count += 1
                refresh_started.set()
                await release_refresh.wait()
            return await original_overview(**kwargs)

        self.service.list_signal_decider_exec_overview = slow_refresh_overview
        try:
            board = await asyncio.wait_for(
                self.service.get_runtime_daily_board(
                    market_date=self.target_trade_date,
                    refresh=True,
                    trigger='test',
                ),
                timeout=1.0,
            )

            self.assertEqual(board['refresh']['status'], 'scheduled')
            self.assertEqual(board['freshness']['status'], 'refreshing')
            self.assertEqual(board['freshness']['read_model_source'], 'persisted_exec_read_model')
            await asyncio.wait_for(refresh_started.wait(), timeout=1.0)

            monitored = await asyncio.wait_for(
                self.service.list_runtime_monitored_symbols(
                    market_date=self.target_trade_date,
                    refresh=True,
                    trigger='test',
                ),
                timeout=1.0,
            )

            self.assertEqual(refresh_call_count, 1)
            self.assertEqual(monitored['refresh']['status'], 'running')
            self.assertEqual(monitored['freshness']['status'], 'refreshing')
        finally:
            release_refresh.set()
            tasks = list(self.service._dashboard_refresh_tasks.values())
            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)
            self.service.list_signal_decider_exec_overview = original_overview

    async def test_dashboard_background_refresh_stores_monitored_cache_with_live_holding_filter(self) -> None:
        key = self.service._dashboard_refresh_key(
            target_trade_date=self.target_trade_date,
            account_reference=None,
            symbol=None,
        )

        with (
            patch.object(
                self.service,
                'list_signal_decider_exec_overview',
                new=AsyncMock(return_value={'items': [], 'summary': {}}),
            ) as overview,
            patch.object(
                self.service,
                '_load_live_portfolio_holding_symbols',
                new=AsyncMock(return_value={self.symbol}),
            ) as load_live_holdings,
            patch.object(self.service, '_store_dashboard_caches_from_overview') as store_caches,
        ):
            await self.service._refresh_dashboard_read_model(
                key=key,
                target_trade_date=self.target_trade_date,
                account_reference=None,
                symbol=None,
                trigger='test',
                timeout_seconds=1.0,
            )

        overview.assert_awaited_once()
        load_live_holdings.assert_awaited_once_with(trigger='test:dashboard_background_refresh')
        store_caches.assert_called_once()
        self.assertEqual(store_caches.call_args.kwargs['live_holding_symbols'], {self.symbol})

    async def test_dashboard_background_refresh_stores_monitored_cache_with_live_exec_overlay(self) -> None:
        key = self.service._dashboard_refresh_key(
            target_trade_date=self.target_trade_date,
            account_reference=None,
            symbol=None,
        )
        self.client.runtime_response = {
            'ready': True,
            'mode': 'mock',
            'safety': {
                'paused': False,
                'kill_switch_enabled': False,
                'automation_enabled': True,
            },
            'liveness': {
                'autonomous': {
                    'monitoring': {
                        'last_cycle': {
                            'items': [
                                {
                                    'symbol': self.symbol,
                                    'intent': EXEC_INTENT_NEXT_OPEN_BUY,
                                    'status': 'watch_monitoring_active',
                                    'source_identity': {
                                        'source_plan_id': f'live-watch-{self.target_trade_date:%Y%m%d}',
                                        'strategy_run_id': f'core:{self.target_trade_date.isoformat()}:runtime',
                                    },
                                }
                            ]
                        }
                    }
                }
            },
        }

        with (
            patch('app.orders.exec_integration.kis_realtime_price_service') as price_service,
            patch.object(
                self.service,
                '_load_live_portfolio_holding_symbols',
                new=AsyncMock(return_value=None),
            ) as load_live_holdings,
        ):
            price_service.price_fields_for_symbol.return_value = {
                'current_price': 50200,
                'price_as_of': '2026-04-30T00:07:30+00:00',
                'price_received_at': '2026-04-30T00:07:31+00:00',
                'price_source': 'kis_realtime_ws',
                'price_status': 'available',
                'realtime_subscribed': True,
                'realtime_subscription_reason': 'subscribed',
                'display_price_available': True,
            }
            price_service.observe_monitored_symbols.return_value = {'allowed_symbols': [self.symbol]}

            await self.service._refresh_dashboard_read_model(
                key=key,
                target_trade_date=self.target_trade_date,
                account_reference=None,
                symbol=None,
                trigger='test',
                timeout_seconds=1.0,
            )
            monitored = await self.service.list_runtime_monitored_symbols(
                market_date=self.target_trade_date,
                refresh=False,
                trigger='test',
            )

        load_live_holdings.assert_awaited_once_with(trigger='test:dashboard_background_refresh')
        self.assertTrue(monitored['freshness']['served_from_cache'])
        item = next(candidate for candidate in monitored['items'] if candidate['symbol'] == self.symbol)
        self.assertTrue(item['exec_monitoring_active'])
        self.assertTrue(item['exec_runtime_showable'])
        self.assertTrue(item['exec_agent_approved'])
        self.assertEqual(item['exec_runtime_state'], 'exec_approved_watch')
        self.assertIsNone(item['exec_runtime_exclusion_reason'])
        self.assertIn('watch_monitoring_active', item['monitoring_statuses'])

    async def test_scheduled_exec_readback_refresh_reuses_dashboard_background_state(self) -> None:
        await self.service.dispatch_next_open_plan(
            target_trade_date=self.target_trade_date,
            refresh_plan=False,
            dry_run=False,
            force=True,
            trigger='test',
        )
        original_overview = self.service.list_signal_decider_exec_overview
        refresh_started = asyncio.Event()
        release_refresh = asyncio.Event()
        refresh_call_count = 0

        async def slow_refresh_overview(**kwargs):
            nonlocal refresh_call_count
            if kwargs.get('refresh'):
                refresh_call_count += 1
                refresh_started.set()
                await release_refresh.wait()
            return await original_overview(**kwargs)

        self.service.list_signal_decider_exec_overview = slow_refresh_overview
        try:
            scheduled = self.service.schedule_signal_decider_exec_readback_refresh(
                target_trade_date=self.target_trade_date,
                trigger='market_open_scheduler',
            )

            self.assertEqual(scheduled['refresh']['status'], 'scheduled')
            self.assertTrue(scheduled['refresh']['background'])
            self.assertEqual(scheduled['request']['target_trade_date'], self.target_trade_date.isoformat())
            await asyncio.wait_for(refresh_started.wait(), timeout=1.0)

            running = self.service.schedule_signal_decider_exec_readback_refresh(
                target_trade_date=self.target_trade_date,
                trigger='market_open_scheduler',
            )

            self.assertEqual(refresh_call_count, 1)
            self.assertEqual(running['refresh']['status'], 'running')
            self.assertTrue(running['refresh']['in_flight'])
        finally:
            release_refresh.set()
            tasks = list(self.service._dashboard_refresh_tasks.values())
            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)
            self.service.list_signal_decider_exec_overview = original_overview

    async def test_scheduled_exec_readback_refresh_uses_supplied_timeout(self) -> None:
        await self.service.dispatch_next_open_plan(
            target_trade_date=self.target_trade_date,
            refresh_plan=False,
            dry_run=False,
            force=True,
            trigger='test',
        )
        original_overview = self.service.list_signal_decider_exec_overview
        self.service.dashboard_refresh_timeout_seconds = 0.001
        refresh_started = asyncio.Event()
        release_refresh = asyncio.Event()
        refresh_cancelled = False

        async def slow_refresh_overview(**kwargs):
            nonlocal refresh_cancelled
            if kwargs.get('refresh'):
                refresh_started.set()
                try:
                    await release_refresh.wait()
                except asyncio.CancelledError:
                    refresh_cancelled = True
                    raise
            return await original_overview(**kwargs)

        self.service.list_signal_decider_exec_overview = slow_refresh_overview
        try:
            scheduled = self.service.schedule_signal_decider_exec_readback_refresh(
                target_trade_date=self.target_trade_date,
                trigger='market_open_scheduler',
                timeout_seconds=0.2,
            )

            self.assertEqual(scheduled['refresh']['status'], 'scheduled')
            self.assertAlmostEqual(scheduled['refresh']['timeout_seconds'], 0.2)
            await asyncio.wait_for(refresh_started.wait(), timeout=1.0)
            tasks = list(self.service._dashboard_refresh_tasks.values())
            self.assertEqual(len(tasks), 1)
            task = tasks[0]

            await asyncio.sleep(0.03)

            self.assertFalse(refresh_cancelled)
            self.assertFalse(task.done())

            release_refresh.set()
            await asyncio.wait_for(task, timeout=1.0)
            key = self.service._dashboard_refresh_key(
                target_trade_date=self.target_trade_date,
                account_reference=None,
                symbol=None,
            )
            refresh_state = self.service._dashboard_refresh_payload(
                key,
                requested=True,
            )
            self.assertEqual(refresh_state['status'], 'succeeded')
            self.assertAlmostEqual(refresh_state['timeout_seconds'], 0.2)
            self.assertIsNone(refresh_state['last_error'])
        finally:
            release_refresh.set()
            tasks = list(self.service._dashboard_refresh_tasks.values())
            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)
            self.service.list_signal_decider_exec_overview = original_overview

    async def test_runtime_monitored_symbols_empty_response_does_not_wait_for_refresh(self) -> None:
        empty_market_date = date(2027, 5, 21)
        original_overview = self.service.list_signal_decider_exec_overview
        refresh_started = asyncio.Event()
        release_refresh = asyncio.Event()
        refresh_call_count = 0

        async def slow_refresh_overview(**kwargs):
            nonlocal refresh_call_count
            if kwargs.get('refresh'):
                refresh_call_count += 1
                refresh_started.set()
                await release_refresh.wait()
                runtime_result = await self.service.runtime_status(refresh=False)
                return {
                    'request': {
                        'target_trade_date': str(kwargs.get('target_trade_date')),
                        'account_reference': kwargs.get('account_reference'),
                        'symbol': kwargs.get('symbol'),
                        'refresh': True,
                    },
                    'integration': runtime_result['integration'],
                    'runtime': runtime_result['runtime'],
                    'summary': {},
                    'items': [],
                }
            return await original_overview(**kwargs)

        async def run_inline(func, *args, **kwargs):
            return func(*args, **kwargs)

        self.service.list_signal_decider_exec_overview = slow_refresh_overview
        try:
            with patch('app.orders.exec_integration.asyncio.to_thread', new=AsyncMock(side_effect=run_inline)):
                monitored = await asyncio.wait_for(
                    self.service.list_runtime_monitored_symbols(
                        market_date=empty_market_date,
                        refresh=True,
                        trigger='test',
                    ),
                    timeout=1.0,
                )

            self.assertEqual(monitored['summary']['items_total'], 0)
            self.assertEqual(monitored['items'], [])
            self.assertEqual(monitored['freshness']['status'], 'refreshing')
            self.assertEqual(monitored['refresh']['status'], 'scheduled')
            await asyncio.wait_for(refresh_started.wait(), timeout=1.0)
            self.assertEqual(refresh_call_count, 1)
        finally:
            release_refresh.set()
            tasks = list(self.service._dashboard_refresh_tasks.values())
            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)
            self.service.list_signal_decider_exec_overview = original_overview

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

                await self.service.list_signal_decider_exec_overview(
                    target_trade_date=self.target_trade_date,
                    refresh=True,
                    trigger='test',
                )
                board = await self.service.get_runtime_daily_board(
                    market_date=self.target_trade_date,
                    refresh=False,
                    trigger='test',
                )
                detail = await self.service.get_runtime_symbol_detail(
                    market_date=self.target_trade_date,
                    symbol=self.symbol,
                    refresh=False,
                    trigger='test',
                )

                self.assertEqual(board['summary']['cancelled'], 1)
                self.assertEqual(board['summary']['failed'], 0)
                self.assertEqual(board['items'][0]['status'], 'cancelled')
                self.assertEqual(detail['items'][0]['status'], 'cancelled')
                self.assertEqual(detail['items'][0]['remote_state']['derived_status'], 'cancelled')


if __name__ == '__main__':
    unittest.main()
