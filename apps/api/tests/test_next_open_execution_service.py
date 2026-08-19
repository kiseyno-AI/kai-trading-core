from datetime import date, datetime, timezone
from decimal import Decimal
import json
from types import SimpleNamespace
import unittest
from unittest.mock import MagicMock, patch
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
from app.market.session import (
    KisKrxTradingCalendarProvider,
    reset_krx_calendar_provider,
    set_krx_calendar_provider,
)
from app.orders.next_open_execution import (
    EXECUTION_STATUS_DRY_RUN,
    EXECUTION_STATUS_DELEGATED_FOR_EXEC_AGENT_SIZING,
    EXECUTION_STATUS_SUBMITTED,
    ELIGIBILITY_BLOCKED,
    ELIGIBILITY_ELIGIBLE,
    SignalDeciderNextOpenExecutionService,
    NextOpenAccountTarget,
    _preopen_plan_covers_latest_snapshots,
    _previous_trading_day,
)
from app.orders.store import upsert_broker_order, upsert_order_request


def _preopen_snapshot(
    snapshot_id: int,
    symbol: str,
    action: str = 'buy',
    updated_at: datetime | None = None,
) -> SimpleNamespace:
    return SimpleNamespace(
        id=snapshot_id,
        symbol=symbol,
        agent_name='signal-decider',
        next_day_open_action=action,
        decision_action=action,
        updated_at=updated_at,
    )


def _preopen_plan_row(
    snapshot_id: int,
    symbol: str,
    updated_at: datetime | None = None,
) -> SimpleNamespace:
    return SimpleNamespace(
        account_reference='KIS:12345678-01',
        symbol=symbol,
        decision_agent_name='signal-decider',
        decision_snapshot_id=snapshot_id,
        execution_status='planned',
        updated_at=updated_at,
    )


def test_preopen_plan_refresh_detects_late_actionable_feedback() -> None:
    target = NextOpenAccountTarget(
        label='Main',
        account_reference='KIS:12345678-01',
        budget_krw=Decimal('1000000'),
    )

    covered = _preopen_plan_covers_latest_snapshots(
        existing_rows=[_preopen_plan_row(1, '005930')],
        snapshots=[_preopen_snapshot(1, '005930'), _preopen_snapshot(2, '000660')],
        account_targets=[target],
    )

    assert covered is False


def test_preopen_plan_reuses_only_exact_latest_feedback_set() -> None:
    target = NextOpenAccountTarget(
        label='Main',
        account_reference='KIS:12345678-01',
        budget_krw=Decimal('1000000'),
    )
    snapshots = [_preopen_snapshot(1, '005930'), _preopen_snapshot(2, '000660')]

    assert _preopen_plan_covers_latest_snapshots(
        existing_rows=[_preopen_plan_row(1, '005930'), _preopen_plan_row(2, '000660')],
        snapshots=snapshots,
        account_targets=[target],
    ) is True
    assert _preopen_plan_covers_latest_snapshots(
        existing_rows=[_preopen_plan_row(1, '005930'), _preopen_plan_row(2, '000660')],
        snapshots=[_preopen_snapshot(3, '005930', action='wait'), _preopen_snapshot(2, '000660')],
        account_targets=[target],
    ) is False


def test_preopen_plan_detects_unrouted_short_term_watch_feedback() -> None:
    snapshot = SimpleNamespace(
        id=7,
        symbol='035420',
        agent_name='short-term-decider',
        response_payload={'tactical_action': 'watch_for_trigger'},
    )
    row = SimpleNamespace(
        account_reference='KIS:00000000-00',
        symbol='035420',
        decision_agent_name='short-term-decider',
        decision_snapshot_id=7,
        execution_status='planned',
    )

    assert _preopen_plan_covers_latest_snapshots(
        existing_rows=[],
        snapshots=[snapshot],
        account_targets=[],
    ) is False
    assert _preopen_plan_covers_latest_snapshots(
        existing_rows=[row],
        snapshots=[snapshot],
        account_targets=[],
    ) is True


def test_preopen_plan_refreshes_when_same_snapshot_row_was_updated() -> None:
    target = NextOpenAccountTarget(
        label='Main',
        account_reference='KIS:12345678-01',
        budget_krw=Decimal('1000000'),
    )
    row_updated_at = datetime(2026, 4, 29, 20, 0, tzinfo=timezone.utc)
    snapshot_updated_at = datetime(2026, 4, 29, 20, 5, tzinfo=timezone.utc)

    assert _preopen_plan_covers_latest_snapshots(
        existing_rows=[_preopen_plan_row(1, '005930', updated_at=row_updated_at)],
        snapshots=[_preopen_snapshot(1, '005930', updated_at=snapshot_updated_at)],
        account_targets=[target],
    ) is False


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


class FakeCalendarClient:
    def __init__(self, rows: list[dict]) -> None:
        self.rows = rows
        self.calls: list[dict] = []

    async def fetch_krx_holiday_calendar(self, **kwargs):
        self.calls.append(kwargs)
        return {'output': list(self.rows)}


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
            'signal_decider_wait_conflict_gate_mode': settings.signal_decider_wait_conflict_gate_mode,
            'signal_decider_exec_integration_mode': settings.signal_decider_exec_integration_mode,
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
        settings.signal_decider_wait_conflict_gate_mode = 'report_only'
        settings.signal_decider_exec_integration_mode = 'disabled'
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
        self._seed_policy_version()

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

    def _quality_context(self, *, trading_value_source: str = 'raw', trading_value_krw: int | None = 8_000_000_000) -> dict:
        return {
            'status': 'available',
            'blocks': {
                'liquidity_turnover': {
                    'status': 'available' if trading_value_krw is not None else 'unavailable',
                    'turnover': {
                        'status': 'available' if trading_value_krw is not None else 'missing',
                        'trading_value_source': trading_value_source,
                        'raw_trading_value_krw': trading_value_krw if trading_value_source == 'raw' else None,
                        'estimated_trading_value_krw': trading_value_krw if trading_value_source == 'estimated' else None,
                        'freshness': {'freshness_status': 'fresh', 'provenance_status': 'provider_sourced'},
                    },
                    'liquidity_ranking': {
                        'status': 'available',
                        'avg_trading_value_20d': trading_value_krw or 8_000_000_000,
                        'freshness': {'freshness_status': 'fresh', 'provenance_status': 'provider_sourced'},
                    },
                    'decision_guidance': {'category': 'liquidity_turnover_slippage', 'decision_tier': 'core_execution'},
                },
                'intraday_realtime': {
                    'status': 'available',
                    'quote': {'status': 'available', 'freshness': {'freshness_status': 'fresh', 'provenance_status': 'provider_sourced'}},
                    'decision_guidance': {'category': 'after_hours_intraday_realtime', 'decision_tier': 'core_execution'},
                },
                'technical_evidence': {
                    'status': 'available',
                    'decision_guidance': {'category': 'technical_evidence_support_resistance', 'decision_tier': 'supporting_context'},
                },
                'disclosures_news': {
                    'status': 'empty',
                    'decision_guidance': {'category': 'disclosures_news_events', 'decision_tier': 'auxiliary_context'},
                },
            },
        }

    def _pre_decider_context(self, *, trading_value_source: str = 'raw', trading_value_krw: int = 8_000_000_000) -> dict:
        return {
            'schema_version': 'pre_decider_enrichment_v1',
            'quote_orderbook_snapshot': {'status': 'available', 'spread_bps': 15},
            'trading_value': {
                'status': 'available',
                'source': trading_value_source,
                'raw_trading_value_krw': trading_value_krw if trading_value_source == 'raw' else None,
                'estimated_trading_value_krw': trading_value_krw if trading_value_source == 'estimated' else None,
                'trading_value_krw': trading_value_krw,
            },
            'flow_short_sector_context': {'status': 'available'},
        }

    def _market_regime_context(self, *, posture: str = 'caution') -> dict:
        return {
            'schema_version': 'market_regime_context_v1',
            'market_date': self.market_date.isoformat(),
            'market': 'KOSPI',
            'posture': posture,
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
            'recommended_sizing_multiplier': 0.5 if posture == 'caution' else 1.0,
            'missing_fields': [],
        }

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
                request_payload={
                    'signal_decider_policy': {'current_version': self.policy_version},
                    'symbol_day_context': {
                        'decider_data_quality_context': self._quality_context(),
                        'pre_decider_enrichment_context': self._pre_decider_context(),
                        'market_regime_context': self._market_regime_context(),
                    },
                },
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
        pre_decider_enrichment_context: dict[str, object] | None = None,
        decider_data_quality_context: dict[str, object] | None = None,
        include_decider_data_quality_context: bool = True,
        confidence_score_10: float = 7.1,
        input_feature_snapshot_payload: dict[str, object] | None = None,
    ) -> int:
        generic_action = 'buy' if tactical_action in {'scalp_buy_candidate', 'watch_for_trigger'} else tactical_action
        if generic_action not in {'buy', 'wait', 'reject'}:
            generic_action = 'wait'
        request_payload = {
            'orchestration': {'required_upstream_roles': ['technical-summarizer']},
            'pre_decider_enrichment_context': (
                self._pre_decider_context()
                if pre_decider_enrichment_context is None
                else pre_decider_enrichment_context
            ),
            'market_regime_context': self._market_regime_context(),
        }
        if include_decider_data_quality_context:
            request_payload['decider_data_quality_context'] = (
                self._quality_context()
                if decider_data_quality_context is None
                else decider_data_quality_context
            )
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
                confidence_score_10=confidence_score_10,
                conviction_score_10=6.4,
                thesis_summary=f'{symbol} short-term summary',
                supporting_evidence_payload=['Support held with relative strength.'],
                decision_risks_payload=['Opening range may stay volatile.'],
                invalidation_conditions_payload=['Stop below floor.'],
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
                input_feature_snapshot_payload=(
                    input_feature_snapshot_payload
                    if input_feature_snapshot_payload is not None
                    else {'meta': {'source_agent_name': 'short-term-decider'}}
                ),
                request_payload=request_payload,
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
                    'conviction_score_10': 6.4,
                    'supporting_evidence': ['Support held with relative strength.'],
                    'tactical_risks': ['Opening range may stay volatile.'],
                    'invalidation_conditions': ['Stop below floor.'],
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

    def test_previous_trading_day_uses_kis_calendar_helper(self) -> None:
        set_krx_calendar_provider(
            KisKrxTradingCalendarProvider(
                client=FakeCalendarClient(
                    [
                        {'bass_dt': '20260101', 'opnd_yn': 'N'},
                        {'bass_dt': '20251231', 'opnd_yn': 'N'},
                        {'bass_dt': '20251230', 'opnd_yn': 'Y'},
                    ]
                )
            )
        )

        self.assertEqual(_previous_trading_day(date(2026, 1, 2)), date(2025, 12, 30))

    def test_list_plan_items_rolls_requested_non_open_date_forward(self) -> None:
        set_krx_calendar_provider(
            KisKrxTradingCalendarProvider(
                client=FakeCalendarClient(
                    [
                        {'bass_dt': '20260602', 'opnd_yn': 'Y'},
                        {'bass_dt': '20260603', 'opnd_yn': 'N'},
                        {'bass_dt': '20260604', 'opnd_yn': 'Y'},
                    ]
                )
            )
        )
        service = self._service(state='PREOPEN', server_time='2026-06-03T08:40:00+09:00')

        result = service.list_plan_items(target_trade_date=date(2026, 6, 3))

        self.assertEqual(result['request']['target_trade_date'], '2026-06-04')
        self.assertEqual(result['summary']['target_trade_date'], '2026-06-04')
        self.assertEqual(result['summary']['source_market_date'], '2026-06-02')

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

    def test_refresh_plan_items_marks_incomplete_agent_context_candidate_set(self) -> None:
        self.market_date = date(2026, 8, 3)
        self.target_trade_date = date(2026, 8, 4)
        self.calendar_client = FakeCalendarClient(
            [
                {'bass_dt': '20260803', 'opnd_yn': 'Y'},
                {'bass_dt': '20260804', 'opnd_yn': 'Y'},
            ]
        )
        set_krx_calendar_provider(KisKrxTradingCalendarProvider(client=self.calendar_client))
        self._seed_snapshot(symbol='005930')
        run_id = f'agent-context-test-{uuid4().hex[:10]}'
        service = self._service(state='PREOPEN', server_time='2026-04-30T08:40:00+09:00')
        latest_run = {
            'run_id': run_id,
            'status': 'partial',
            'symbols_total': 1,
            'completed_count': 0,
            'failed_count': 1,
            'started_at': datetime(2026, 4, 29, 5, 45, tzinfo=timezone.utc),
            'completed_at': datetime(2026, 4, 29, 5, 46, tzinfo=timezone.utc),
        }
        failure_summary = {
            'summary': {
                'failure_count': 1,
                'group_count': 1,
                'by_failure_kind': {'openclaw_auth_rejected': 1},
            },
            'groups': [
                {
                    'market_date': '2026-08-03',
                    'run_id': run_id,
                    'agent_name': 'openclaw-gateway',
                    'failure_kind': 'openclaw_auth_rejected',
                    'failure_status': 'auth/rejected',
                    'category': 'dependency',
                    'failure_count': 1,
                }
            ],
        }

        def fake_find_latest_run(*, market_date: date, requested_symbols: list[str] | None = None):
            if requested_symbols == ['005930']:
                return latest_run
            return None

        with (
            patch(
                'app.orders.next_open_execution.agent_context_feedback_run_state_service.find_latest_run',
                side_effect=fake_find_latest_run,
            ) as find_latest_run,
            patch(
                'app.orders.next_open_execution.agent_context_feedback_failure_service.get_grouped_summary',
                return_value=failure_summary,
            ) as get_grouped_summary,
            patch(
                'app.orders.next_open_execution.load_daily_bars_for_symbol_range',
                return_value=[{'trade_date': '2026-08-03', 'close_price': 50000}],
            ),
        ):
            result = service.refresh_plan_items(target_trade_date=self.target_trade_date, trigger='test')

        find_latest_run.assert_any_call(
            market_date=self.market_date,
            requested_symbols=['005930'],
        )
        get_grouped_summary.assert_called_once_with(
            market_date=self.market_date,
            run_id=run_id,
            limit=20,
        )
        self.assertEqual(result['summary']['agent_context_completeness_status'], 'incomplete')
        self.assertTrue(result['summary']['agent_context_incomplete'])
        self.assertEqual(result['summary']['agent_context_failure_count'], 1)
        item = result['items'][0]
        completeness = item['agent_context_completeness']
        self.assertEqual(completeness['status'], 'incomplete')
        self.assertTrue(completeness['has_incomplete_agent_context'])
        self.assertEqual(completeness['latest_run']['run_id'], run_id)
        self.assertEqual(completeness['failure_groups'][0]['failure_kind'], 'openclaw_auth_rejected')
        warning_codes = {warning['code'] for warning in item['validation']['warnings']}
        self.assertIn('agent_context_incomplete_candidate_set', warning_codes)
        self.assertEqual(item['traceability']['agent_context_completeness']['status'], 'incomplete')

    def test_refresh_plan_items_blocks_existing_rows_when_pykrx_enrichment_missing(self) -> None:
        self._seed_snapshot(symbol='005930')
        service = self._service(state='PREOPEN', server_time='2026-04-30T08:40:00+09:00')

        with patch(
            'app.orders.next_open_execution.load_daily_bars_for_symbol_range',
            return_value=[{'trade_date': '2026-04-29', 'close_price': 50000}],
        ):
            initial = service.refresh_plan_items(target_trade_date=self.target_trade_date, trigger='test')

        self.assertEqual(initial['summary']['eligible_count'], 1)
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
            result = service.refresh_plan_items(target_trade_date=self.target_trade_date, trigger='test_retry')

        self.assertEqual(result['summary']['reason'], 'pykrx_enrichment_preflight_blocked')
        self.assertFalse(result['summary']['enrichment_preflight_allowed'])
        self.assertEqual(result['summary']['enrichment_preflight_status'], 'blocked')
        self.assertEqual(result['summary']['blocked_count'], 1)
        self.assertEqual(result['enrichment_preflight']['failed_categories'], ['investor_flow_symbol', 'short_selling'])
        item = result['items'][0]
        self.assertEqual(item['eligibility_status'], ELIGIBILITY_BLOCKED)
        blocking_codes = [entry['code'] for entry in item['validation']['blocking_reasons']]
        self.assertIn('pykrx_enrichment_preflight_blocked', blocking_codes)
        self.assertEqual(
            item['validation']['pykrx_enrichment_preflight']['zero_coverage_categories'],
            ['investor_flow_symbol', 'short_selling'],
        )

    def test_refresh_plan_items_keeps_short_term_watch_rows_when_pykrx_preflight_blocks_signal_buys(self) -> None:
        self._seed_snapshot(symbol='900001')
        for index in range(36):
            self._seed_short_term_snapshot(symbol=f'{index + 1:06d}', tactical_action='watch_for_trigger')
        service = self._service(state='PREOPEN', server_time='2026-04-30T08:40:00+09:00')
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
        required_categories = ['sector_classification', 'investor_flow_symbol', 'short_selling']
        candidate_rows = [
            {'symbol': f'{index + 1:06d}', 'category': category}
            for index in range(36)
            for category in required_categories
        ]

        with (
            patch('app.orders.next_open_execution.inspect_pykrx_enrichment_preflight', return_value=blocked_preflight),
            patch('app.orders.next_open_execution.load_daily_pykrx_enrichment_rows', return_value=candidate_rows),
            patch(
                'app.orders.next_open_execution.load_daily_bars_for_symbol_range',
                return_value=[{'trade_date': '2026-04-29', 'close_price': 50000}],
            ),
        ):
            result = service.refresh_plan_items(target_trade_date=self.target_trade_date, trigger='test_retry')

        self.assertEqual(result['summary']['reason'], 'pykrx_enrichment_preflight_blocked')
        self.assertFalse(result['summary']['enrichment_preflight_allowed'])
        self.assertEqual(result['summary']['items_total'], 37)
        self.assertEqual(result['summary']['short_term_candidates_total'], 36)
        self.assertEqual(result['summary']['short_term_watch_active_count'], 36)
        self.assertEqual(result['summary']['short_term_blocked_or_degraded_count'], 36)
        self.assertEqual(result['summary']['signal_decider_global_preflight_blocked_count'], 1)
        items_by_agent = {}
        for item in result['items']:
            items_by_agent.setdefault(item['decision_agent_name'], []).append(item)
        self.assertEqual(len(items_by_agent['short-term-decider']), 36)
        self.assertEqual(len(items_by_agent['signal-decider']), 1)

        signal_item = items_by_agent['signal-decider'][0]
        self.assertEqual(signal_item['eligibility_status'], ELIGIBILITY_BLOCKED)
        signal_block_codes = {reason['code'] for reason in signal_item['validation']['blocking_reasons']}
        self.assertIn('pykrx_enrichment_preflight_blocked', signal_block_codes)

        short_item = items_by_agent['short-term-decider'][0]
        self.assertEqual(short_item['eligibility_status'], ELIGIBILITY_ELIGIBLE)
        self.assertEqual(short_item['validation']['blocking_reasons'], [])
        self.assertEqual(short_item['validation']['candidate_pykrx_enrichment']['missing_categories'], [])
        short_warning_codes = {warning['code'] for warning in short_item['validation']['warnings']}
        self.assertIn('global_pykrx_enrichment_preflight_partial', short_warning_codes)
        self.assertIn('pykrx_enrichment_preflight_blocked', short_warning_codes)
        self.assertEqual(short_item['validation']['short_term']['tactical_action'], 'watch_for_trigger')
        self.assertEqual(short_item['validation']['short_term']['trigger_price'], 50500)
        self.assertEqual(short_item['validation']['short_term']['chase_limit_price'], 51500)
        self.assertEqual(short_item['validation']['pykrx_enrichment_preflight']['status'], 'blocked')

    def test_refresh_plan_items_blocks_only_short_term_candidate_missing_symbol_enrichment(self) -> None:
        self._seed_short_term_snapshot(symbol='005930')
        service = self._service(state='PREOPEN', server_time='2026-04-30T08:40:00+09:00')
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
        candidate_rows = [
            {'symbol': '005930', 'category': 'sector_classification'},
            {'symbol': '005930', 'category': 'short_selling'},
        ]

        with (
            patch('app.orders.next_open_execution.inspect_pykrx_enrichment_preflight', return_value=blocked_preflight),
            patch('app.orders.next_open_execution.load_daily_pykrx_enrichment_rows', return_value=candidate_rows),
            patch(
                'app.orders.next_open_execution.load_daily_bars_for_symbol_range',
                return_value=[{'trade_date': '2026-04-29', 'close_price': 50000}],
            ),
        ):
            result = service.refresh_plan_items(target_trade_date=self.target_trade_date, trigger='test')

        self.assertEqual(result['summary']['items_total'], 1)
        self.assertEqual(result['summary']['blocked_count'], 1)
        item = result['items'][0]
        self.assertEqual(item['eligibility_status'], ELIGIBILITY_BLOCKED)
        self.assertEqual(item['validation']['candidate_pykrx_enrichment']['missing_categories'], ['investor_flow_symbol'])
        self.assertIn(
            'candidate_pykrx_enrichment_missing',
            {reason['code'] for reason in item['validation']['blocking_reasons']},
        )

    def test_refresh_plan_items_blocks_short_term_candidate_missing_symbol_enrichment_when_global_partial_allowed(
        self,
    ) -> None:
        self._seed_short_term_snapshot(symbol='005930')
        service = self._service(state='PREOPEN', server_time='2026-04-30T08:40:00+09:00')
        required_categories = ['sector_classification', 'investor_flow_symbol', 'short_selling']
        partial_preflight = {
            'market_date': self.market_date.isoformat(),
            'status': 'ready',
            'allowed': True,
            'required_categories': required_categories,
            'failed_categories': [],
            'zero_coverage_categories': [],
            'category_row_counts': {
                'sector_classification': 2699,
                'investor_flow_symbol': 2672,
                'short_selling': 2806,
            },
            'warning_categories': [
                {
                    'category': 'investor_flow_symbol',
                    'gate_status': 'ready',
                    'coverage_status': 'partial',
                    'expected_rows': 2700,
                    'present_rows': 2672,
                },
            ],
        }
        candidate_rows = [
            {'symbol': '005930', 'category': 'sector_classification'},
            {'symbol': '005930', 'category': 'short_selling'},
        ]

        with (
            patch('app.orders.next_open_execution.inspect_pykrx_enrichment_preflight', return_value=partial_preflight),
            patch(
                'app.orders.next_open_execution.load_daily_pykrx_enrichment_rows',
                return_value=candidate_rows,
            ) as load_candidate_rows,
            patch(
                'app.orders.next_open_execution.load_daily_bars_for_symbol_range',
                return_value=[{'trade_date': '2026-04-29', 'close_price': 50000}],
            ),
        ):
            result = service.refresh_plan_items(target_trade_date=self.target_trade_date, trigger='test')

        load_candidate_rows.assert_called_once()
        self.assertEqual(load_candidate_rows.call_args.kwargs['symbols'], ['005930'])
        self.assertEqual(load_candidate_rows.call_args.kwargs['categories'], required_categories)
        self.assertTrue(result['summary']['enrichment_preflight_allowed'])
        self.assertEqual(result['summary']['enrichment_preflight_status'], 'ready')
        self.assertEqual(result['summary']['blocked_count'], 1)
        item = result['items'][0]
        self.assertEqual(item['eligibility_status'], ELIGIBILITY_BLOCKED)
        self.assertEqual(item['validation']['candidate_pykrx_enrichment']['missing_categories'], ['investor_flow_symbol'])
        self.assertIn(
            'candidate_pykrx_enrichment_missing',
            {reason['code'] for reason in item['validation']['blocking_reasons']},
        )

    def test_refresh_plan_items_signal_dispatch_does_not_budget_size_quantity(self) -> None:
        settings.signal_decider_exec_integration_mode = 'dispatch'
        self._seed_snapshot(symbol='005930')
        service = self._service(state='PREOPEN', server_time='2026-04-30T08:40:00+09:00')

        with patch(
            'app.orders.next_open_execution.load_daily_bars_for_symbol_range',
            return_value=[{'trade_date': '2026-04-29', 'close_price': 50000}],
        ):
            result = service.refresh_plan_items(target_trade_date=self.target_trade_date, trigger='test')

        item = result['items'][0]
        self.assertEqual(item['decision_agent_name'], 'signal-decider')
        self.assertEqual(item['eligibility_status'], ELIGIBILITY_ELIGIBLE)
        self.assertIsNone(item['planned_quantity'])
        self.assertIsNone(item['estimated_notional_krw'])
        self.assertEqual(item['estimated_reference_price'], 50000)
        self.assertIsNone(item['validation']['pricing']['planned_quantity'])
        self.assertIsNone(item['validation']['pricing']['estimated_notional_krw'])
        self.assertEqual(
            item['validation']['pricing_note'],
            'Sizing for signal-decider dispatch is delegated to exec-agent.',
        )
        self.assertEqual(item['traceability']['core_execution_classification'], EXECUTION_STATUS_DELEGATED_FOR_EXEC_AGENT_SIZING)

    def test_refresh_plan_items_blocks_decision_input_quality_hard_blocks(self) -> None:
        snapshot_id = self._seed_snapshot(symbol='005930')
        with session_scope() as session:
            snapshot = session.get(SignalDeciderDecisionSnapshot, snapshot_id)
            snapshot.request_payload = {
                'signal_decider_policy': {'current_version': self.policy_version},
                'symbol_day_context': {
                    'decider_data_quality_context': self._quality_context(
                        trading_value_source='missing',
                        trading_value_krw=None,
                    ),
                    'pre_decider_enrichment_context': {
                        'schema_version': 'pre_decider_enrichment_v1',
                        'quote_orderbook_snapshot': {
                            'status': 'stale_eod',
                            'snapshot_type': 'eod_stale',
                            'quote_freshness_status': 'stale',
                        },
                        'trading_value': {'status': 'unavailable', 'source': 'missing'},
                    },
                },
            }
        service = self._service(state='PREOPEN', server_time='2026-04-30T08:40:00+09:00')

        with patch(
            'app.orders.next_open_execution.load_daily_bars_for_symbol_range',
            return_value=[{'trade_date': '2026-04-29', 'close_price': 50000}],
        ):
            result = service.refresh_plan_items(target_trade_date=self.target_trade_date, trigger='test')

        item = result['items'][0]
        self.assertEqual(item['eligibility_status'], ELIGIBILITY_BLOCKED)
        self.assertEqual(item['validation']['decision_input_quality_context']['final_judgment']['decision_band'], 'block')
        block_codes = {reason['code'] for reason in item['validation']['blocking_reasons']}
        self.assertIn('decision_input_quality_liquidity_turnover_hard_block', block_codes)
        self.assertIn('decision_input_quality_source_turnover_missing', block_codes)

    def test_refresh_plan_items_blocks_buy_when_performance_reward_risk_bucket_is_weak(self) -> None:
        snapshot_id = self._seed_snapshot(symbol='005930')
        performance_criteria = {
            'schema_version': 'signal_decider_performance_criteria_v1',
            'status': 'ready',
            'sample_count': 12,
            'overall': {
                'positive_outcome_rate': 0.30,
                'negative_outcome_rate': 0.65,
            },
            'performance_buckets': [
                {
                    'bucket_key': 'decision.reward_risk_bucket',
                    'bucket_value': 'reward_risk_lt_1_3',
                    'sample_count': 5,
                    'bad_buy_rate': 0.80,
                    'negative_rate': 0.80,
                    'avg_close_return_pct': -1.7,
                }
            ],
            'rule_candidates': [],
        }
        with session_scope() as session:
            snapshot = session.get(SignalDeciderDecisionSnapshot, snapshot_id)
            snapshot.target_price = 51000
            snapshot.stop_loss_price = 49000
            snapshot.request_payload = {
                **(snapshot.request_payload or {}),
                'signal_decider_policy': {
                    'current_version': self.policy_version,
                    'performance_criteria': performance_criteria,
                },
            }
        service = self._service(state='PREOPEN', server_time='2026-04-30T08:40:00+09:00')

        with patch(
            'app.orders.next_open_execution.load_daily_bars_for_symbol_range',
            return_value=[{'trade_date': '2026-04-29', 'close_price': 50000}],
        ):
            result = service.refresh_plan_items(target_trade_date=self.target_trade_date, trigger='test')

        item = result['items'][0]
        self.assertEqual(item['eligibility_status'], ELIGIBILITY_BLOCKED)
        self.assertEqual(item['validation']['performance_gate']['feature_values']['decision.reward_risk_bucket'], 'reward_risk_lt_1_3')
        block_codes = {reason['code'] for reason in item['validation']['blocking_reasons']}
        self.assertIn('performance_reward_risk_gate_blocked', block_codes)
        self.assertIn('performance_adjusted_confidence_below_threshold', block_codes)

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
        self.assertEqual(by_agent['short-term-decider']['raw_action'], 'watch_for_trigger')
        self.assertEqual(by_agent['short-term-decider']['tactical_action'], 'watch_for_trigger')
        self.assertEqual(by_agent['short-term-decider']['generic_action'], 'buy')
        self.assertEqual(by_agent['short-term-decider']['handoff_intent'], 'intraday_watch_buy')
        self.assertTrue(by_agent['short-term-decider']['handoff_active'])
        self.assertTrue(by_agent['short-term-decider']['exec_monitoring_active'])
        self.assertEqual(by_agent['short-term-decider']['approval_state'], 'pending_exec_agent')
        self.assertEqual(by_agent['short-term-decider']['trigger_price'], 50500)
        self.assertEqual(by_agent['short-term-decider']['chase_limit_price'], 51500)
        self.assertEqual(by_agent['short-term-decider']['target_price'], 54000)
        self.assertEqual(by_agent['short-term-decider']['stop_loss_price'], 49000)

    def test_refresh_plan_items_audits_wait_conflict_block_report_only(self) -> None:
        signal_snapshot_id = self._seed_snapshot(symbol='005930', next_day_open_action='wait')
        self._seed_short_term_snapshot(symbol='005930')
        service = self._service(state='PREOPEN', server_time='2026-04-30T08:40:00+09:00')

        with patch(
            'app.orders.next_open_execution.load_daily_bars_for_symbol_range',
            return_value=[{'trade_date': '2026-04-29', 'close_price': 50000}],
        ):
            result = service.refresh_plan_items(target_trade_date=self.target_trade_date, trigger='test')

        short_item = next(item for item in result['items'] if item['decision_agent_name'] == 'short-term-decider')
        self.assertEqual(short_item['eligibility_status'], ELIGIBILITY_ELIGIBLE)
        self.assertEqual(short_item['signal_decider_alignment'], 'wait_conflict_block')
        self.assertEqual(short_item['alignment_gate_action'], 'report_only_block_candidate')
        self.assertEqual(short_item['alignment_signal_decider_decision_snapshot_id'], signal_snapshot_id)
        self.assertEqual(short_item['validation']['signal_decider_alignment'], 'wait_conflict_block')
        self.assertEqual(short_item['traceability']['signal_decider_alignment'], 'wait_conflict_block')
        warning_codes = {warning['code'] for warning in short_item['validation']['warnings']}
        block_codes = {reason['code'] for reason in short_item['validation']['blocking_reasons']}
        self.assertIn('signal_decider_wait_conflict_gate_report_only', warning_codes)
        self.assertNotIn('signal_decider_wait_conflict_gate', block_codes)
        self.assertEqual(result['summary']['short_term_wait_conflict_block_count'], 1)
        self.assertEqual(result['summary']['short_term_wait_conflict_override_count'], 0)

    def test_refresh_plan_items_allows_wait_conflict_override_for_strong_watch_setup(self) -> None:
        signal_snapshot_id = self._seed_snapshot(symbol='005930', next_day_open_action='wait')
        self._seed_short_term_snapshot(
            symbol='005930',
            input_feature_snapshot_payload={
                'meta': {'source_agent_name': 'short-term-decider'},
                'quality': {
                    'buy_quality_score': 76,
                    'return_percentile': 0.82,
                    'volume_percentile': 0.68,
                    'raw_trading_value_krw': 9000000000,
                },
            },
        )
        service = self._service(state='PREOPEN', server_time='2026-04-30T08:40:00+09:00')

        with patch(
            'app.orders.next_open_execution.load_daily_bars_for_symbol_range',
            return_value=[{'trade_date': '2026-04-29', 'close_price': 50000}],
        ):
            result = service.refresh_plan_items(target_trade_date=self.target_trade_date, trigger='test')

        short_item = next(item for item in result['items'] if item['decision_agent_name'] == 'short-term-decider')
        self.assertEqual(short_item['eligibility_status'], ELIGIBILITY_ELIGIBLE)
        self.assertEqual(short_item['signal_decider_alignment'], 'wait_conflict_override')
        self.assertEqual(short_item['alignment_gate_action'], 'allow_by_override')
        self.assertEqual(short_item['alignment_signal_decider_decision_snapshot_id'], signal_snapshot_id)
        self.assertTrue(short_item['alignment_gate']['override']['allowed'])
        warning_codes = {warning['code'] for warning in short_item['validation']['warnings']}
        self.assertIn('signal_decider_wait_conflict_override', warning_codes)
        self.assertEqual(result['summary']['short_term_wait_conflict_block_count'], 0)
        self.assertEqual(result['summary']['short_term_wait_conflict_override_count'], 1)

    def test_refresh_plan_items_marks_short_term_buy_aligned_with_signal_buy(self) -> None:
        signal_snapshot_id = self._seed_snapshot(symbol='005930', next_day_open_action='buy')
        self._seed_short_term_snapshot(symbol='005930')
        service = self._service(state='PREOPEN', server_time='2026-04-30T08:40:00+09:00')

        with patch(
            'app.orders.next_open_execution.load_daily_bars_for_symbol_range',
            return_value=[{'trade_date': '2026-04-29', 'close_price': 50000}],
        ):
            result = service.refresh_plan_items(target_trade_date=self.target_trade_date, trigger='test')

        short_item = next(item for item in result['items'] if item['decision_agent_name'] == 'short-term-decider')
        self.assertEqual(short_item['eligibility_status'], ELIGIBILITY_ELIGIBLE)
        self.assertEqual(short_item['signal_decider_alignment'], 'aligned')
        self.assertEqual(short_item['alignment_gate_action'], 'allow')
        self.assertEqual(short_item['alignment_signal_decider_decision_snapshot_id'], signal_snapshot_id)
        self.assertEqual(result['summary']['short_term_wait_conflict_block_count'], 0)
        self.assertEqual(result['summary']['short_term_wait_conflict_override_count'], 0)

    def test_refresh_plan_items_dispatches_low_confidence_short_term_watchers_for_monitoring(self) -> None:
        settings.signal_decider_exec_integration_mode = 'dispatch'
        self._seed_short_term_snapshot(symbol='005930', confidence_score_10=6.0)
        self._seed_short_term_snapshot(
            symbol='000660',
            confidence_score_10=5.9,
            trigger_price=252000,
            chase_limit_price=255000,
            target_price=270000,
            stop_loss_price=245000,
        )
        service = self._service(state='PREOPEN', server_time='2026-04-30T08:40:00+09:00')

        def fake_bars(symbol: str, **_kwargs):
            if symbol == '005930':
                return [{'trade_date': '2026-04-29', 'close_price': 50000}]
            if symbol == '000660':
                return [{'trade_date': '2026-04-29', 'close_price': 250000}]
            return []

        with patch('app.orders.next_open_execution.load_daily_bars_for_symbol_range', side_effect=fake_bars):
            result = service.refresh_plan_items(target_trade_date=self.target_trade_date, trigger='test')

        items_by_symbol = {item['symbol']: item for item in result['items']}
        self.assertEqual(result['summary']['eligible_count'], 2)
        self.assertEqual(items_by_symbol['005930']['eligibility_status'], ELIGIBILITY_ELIGIBLE)
        self.assertEqual(items_by_symbol['000660']['eligibility_status'], ELIGIBILITY_ELIGIBLE)
        self.assertEqual(items_by_symbol['000660']['validation']['blocking_reasons'], [])
        self.assertIn(
            'confidence_below_threshold',
            {warning['code'] for warning in items_by_symbol['000660']['validation']['warnings']},
        )
        self.assertEqual(
            Decimal(str(items_by_symbol['000660']['validation']['short_term']['confidence_score_10'])),
            Decimal('5.9'),
        )
        self.assertEqual(items_by_symbol['000660']['validation']['short_term']['tactical_action'], 'watch_for_trigger')
        self.assertEqual(items_by_symbol['000660']['validation']['short_term']['handoff_intent'], 'intraday_watch_buy')
        self.assertEqual(items_by_symbol['000660']['validation']['handoff']['approval_state'], 'pending_exec_agent')
        self.assertEqual(items_by_symbol['000660']['validation']['short_term']['trigger_price'], 252000)
        self.assertEqual(items_by_symbol['000660']['validation']['short_term']['chase_limit_price'], 255000)
        self.assertEqual(
            Decimal(str(items_by_symbol['000660']['traceability']['confidence_score_10'])),
            Decimal('5.9'),
        )
        self.assertEqual(
            items_by_symbol['000660']['traceability']['core_execution_classification'],
            EXECUTION_STATUS_DELEGATED_FOR_EXEC_AGENT_SIZING,
        )

    def test_refresh_plan_items_attaches_pre_decider_enrichment_to_short_term_validation(self) -> None:
        pre_decider_context = {
            'schema_version': 'pre_decider_enrichment_v1',
            'quote_orderbook_snapshot': {
                'status': 'stale_eod',
                'snapshot_type': 'eod_stale',
                'quote_freshness_timestamp': '2026-04-29T15:30:00+09:00',
            },
            'trading_value': {'status': 'available', 'source': 'raw', 'trading_value_krw': 9870000000},
            'flow_short_sector_context': {'status': 'available'},
        }
        self._seed_short_term_snapshot(
            symbol='005930',
            pre_decider_enrichment_context=pre_decider_context,
        )
        service = self._service(state='PREOPEN', server_time='2026-04-30T08:40:00+09:00')

        with patch(
            'app.orders.next_open_execution.load_daily_bars_for_symbol_range',
            return_value=[{'trade_date': '2026-04-29', 'close_price': 50000}],
        ):
            result = service.refresh_plan_items(target_trade_date=self.target_trade_date, trigger='test')

        item = result['items'][0]
        self.assertEqual(item['decision_agent_name'], 'short-term-decider')
        self.assertEqual(item['validation']['pre_decider_enrichment_context'], pre_decider_context)
        self.assertEqual(item['validation']['short_term']['pre_decider_enrichment_context'], pre_decider_context)
        self.assertEqual(item['traceability']['pre_decider_enrichment_context'], pre_decider_context)
        self.assertEqual(item['validation']['market_regime_context']['schema_version'], 'market_regime_context_v1')
        self.assertEqual(item['validation']['market_regime_context']['posture'], 'caution')
        self.assertEqual(item['validation']['short_term']['market_regime_context']['recommended_exec_action'], 'downsize')
        self.assertEqual(item['traceability']['market_regime_context']['schema_version'], 'market_regime_context_v1')
        quality_context = item['validation']['decision_input_quality_context']
        self.assertEqual(quality_context['schema_version'], 'decision_input_quality_v1')
        self.assertEqual(quality_context['execution_feasibility']['liquidity_tier'], 'medium')
        self.assertEqual(
            item['validation']['short_term']['decision_input_quality_context']['schema_version'],
            'decision_input_quality_v1',
        )
        self.assertEqual(item['traceability']['decision_input_quality_context']['schema_version'], 'decision_input_quality_v1')

    def test_refresh_plan_items_delegates_short_term_data_warning_candidates_in_dispatch_mode(self) -> None:
        settings.signal_decider_exec_integration_mode = 'dispatch'
        self._seed_short_term_snapshot(
            symbol='005930',
            data_warnings=['Opening quote snapshot stale.'],
        )
        service = self._service(state='PREOPEN', server_time='2026-04-30T08:40:00+09:00')

        with patch(
            'app.orders.next_open_execution.load_daily_bars_for_symbol_range',
            return_value=[{'trade_date': '2026-04-29', 'close_price': 50000}],
        ):
            result = service.refresh_plan_items(target_trade_date=self.target_trade_date, trigger='test')

        item = result['items'][0]
        self.assertEqual(item['decision_agent_name'], 'short-term-decider')
        self.assertEqual(item['eligibility_status'], ELIGIBILITY_ELIGIBLE)
        self.assertEqual(item['execution_status'], 'planned')
        self.assertEqual(item['traceability']['core_execution_classification'], EXECUTION_STATUS_DELEGATED_FOR_EXEC_AGENT_SIZING)
        self.assertEqual(item['validation']['blocking_reasons'], [])
        warning_codes = {warning['code'] for warning in item['validation']['warnings']}
        self.assertIn('data_quality_warnings_present', warning_codes)

    def test_refresh_plan_items_keeps_short_term_missing_liquidity_as_warning(self) -> None:
        self._seed_short_term_snapshot(
            symbol='005930',
            include_decider_data_quality_context=False,
            pre_decider_enrichment_context={'schema_version': 'pre_decider_enrichment_v1'},
        )
        service = self._service(state='PREOPEN', server_time='2026-04-30T08:40:00+09:00')

        with patch(
            'app.orders.next_open_execution.load_daily_bars_for_symbol_range',
            return_value=[{'trade_date': '2026-04-29', 'close_price': 50000}],
        ):
            result = service.refresh_plan_items(target_trade_date=self.target_trade_date, trigger='test')

        item = result['items'][0]
        self.assertEqual(item['decision_agent_name'], 'short-term-decider')
        self.assertEqual(item['eligibility_status'], ELIGIBILITY_ELIGIBLE)
        self.assertEqual(item['validation']['blocking_reasons'], [])
        warning_codes = {warning['code'] for warning in item['validation']['warnings']}
        self.assertIn('short_term_liquidity_unverified_warning', warning_codes)
        quality_context = item['validation']['decision_input_quality_context']
        quality_hard_codes = {block['code'] for block in quality_context['final_judgment']['hard_blocks']}
        self.assertIn('source_turnover_missing', quality_hard_codes)
        block_codes = {reason['code'] for reason in item['validation']['blocking_reasons']}
        self.assertNotIn('decision_input_quality_source_turnover_missing', block_codes)
        self.assertEqual(item['validation']['handoff']['approval_state'], 'pending_exec_agent')

    def test_refresh_plan_items_still_blocks_short_term_explicit_quality_hard_blocks(self) -> None:
        self._seed_short_term_snapshot(
            symbol='005930',
            decider_data_quality_context=self._quality_context(
                trading_value_source='missing',
                trading_value_krw=None,
            ),
            pre_decider_enrichment_context={'schema_version': 'pre_decider_enrichment_v1'},
        )
        service = self._service(state='PREOPEN', server_time='2026-04-30T08:40:00+09:00')

        with patch(
            'app.orders.next_open_execution.load_daily_bars_for_symbol_range',
            return_value=[{'trade_date': '2026-04-29', 'close_price': 50000}],
        ):
            result = service.refresh_plan_items(target_trade_date=self.target_trade_date, trigger='test')

        item = result['items'][0]
        self.assertEqual(item['decision_agent_name'], 'short-term-decider')
        self.assertEqual(item['eligibility_status'], ELIGIBILITY_BLOCKED)
        warning_codes = {warning['code'] for warning in item['validation']['warnings']}
        self.assertIn('short_term_liquidity_unverified_warning', warning_codes)
        block_codes = {reason['code'] for reason in item['validation']['blocking_reasons']}
        self.assertIn('decision_input_quality_liquidity_turnover_hard_block', block_codes)
        self.assertNotIn('decision_input_quality_source_turnover_missing', block_codes)

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

    def test_refresh_plan_items_blocks_short_term_rows_with_invalid_account_targets(self) -> None:
        self._seed_short_term_snapshot(symbol='005930')
        settings.signal_decider_next_open_account_targets = 'Main=not-a-kis-account@1500000'
        service = self._service(state='PREOPEN', server_time='2026-04-30T08:40:00+09:00')

        with patch(
            'app.orders.next_open_execution.load_daily_bars_for_symbol_range',
            return_value=[{'trade_date': '2026-04-29', 'close_price': 50000}],
        ):
            result = service.refresh_plan_items(target_trade_date=self.target_trade_date, trigger='test')

        self.assertEqual(result['summary']['items_total'], 1)
        self.assertEqual(result['summary']['eligible_count'], 0)
        self.assertEqual(result['summary']['blocked_count'], 1)
        item = result['items'][0]
        self.assertEqual(item['decision_agent_name'], 'short-term-decider')
        self.assertEqual(item['account_reference'], 'KIS:00000000-00')
        self.assertEqual(item['account_label'], 'UNSPECIFIED_ROUTE')
        self.assertEqual(item['eligibility_status'], ELIGIBILITY_BLOCKED)
        self.assertIn(
            'account_targets_invalid',
            {reason['code'] for reason in item['validation']['blocking_reasons']},
        )

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

    def test_refresh_plan_items_delegates_soft_watch_quality_blocks_to_exec_monitoring(self) -> None:
        self._seed_short_term_snapshot(
            symbol='000660',
            tactical_action='watch_for_trigger',
            input_feature_snapshot_payload={
                'buy_quality_score': 45,
                'raw_trading_value_krw': 250000000,
                'trigger_price': 250000,
                'target_price': 260000,
                'stop_loss_price': 245000,
                'meta': {'source_agent_name': 'short-term-decider'},
            },
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
        self.assertEqual(item['validation']['blocking_reasons'], [])
        self.assertTrue(item['handoff_active'])
        self.assertTrue(item['exec_monitoring_active'])
        warning_codes = {warning['code'] for warning in item['validation']['warnings']}
        self.assertIn('short_term_buy_quality_score_block_delegated_to_exec_watch', warning_codes)
        self.assertIn('short_term_liquidity_floor_block_delegated_to_exec_watch', warning_codes)

    def test_refresh_plan_items_blocks_short_term_rows_above_chase_limit(self) -> None:
        self._seed_short_term_snapshot(
            symbol='000660',
            tactical_action='watch_for_trigger',
            trigger_price=250000,
            chase_limit_price=255000,
            target_price=275000,
            stop_loss_price=240000,
        )
        service = self._service(state='PREOPEN', server_time='2026-04-30T08:40:00+09:00')

        with patch(
            'app.orders.next_open_execution.load_daily_bars_for_symbol_range',
            return_value=[{'trade_date': '2026-04-29', 'close_price': 260000}],
        ):
            result = service.refresh_plan_items(target_trade_date=self.target_trade_date, trigger='test')

        item = next(item for item in result['items'] if item['symbol'] == '000660')
        self.assertEqual(item['decision_agent_name'], 'short-term-decider')
        self.assertEqual(item['eligibility_status'], ELIGIBILITY_BLOCKED)
        self.assertFalse(item['handoff_active'])
        self.assertEqual(item['blocker_reason']['code'], 'reference_price_above_chase_limit')

    def test_refresh_plan_items_short_term_missing_target_and_stop_are_blocked(self) -> None:
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
        self.assertEqual(item['eligibility_status'], ELIGIBILITY_BLOCKED)
        self.assertFalse(item['handoff_active'])
        self.assertFalse(item['exec_monitoring_active'])
        self.assertEqual(item['approval_state'], 'blocked')
        blocking_codes = {reason['code'] for reason in item['validation']['blocking_reasons']}
        self.assertIn('target_price_missing', blocking_codes)
        self.assertIn('stop_loss_price_missing', blocking_codes)
        self.assertEqual(item['blocker_reason']['code'], 'target_price_missing')

    def test_refresh_plan_items_maps_scalp_candidate_to_tactical_buy_handoff(self) -> None:
        self._seed_short_term_snapshot(symbol='005930', tactical_action='scalp_buy_candidate')
        service = self._service(state='PREOPEN', server_time='2026-04-30T08:40:00+09:00')

        with patch(
            'app.orders.next_open_execution.load_daily_bars_for_symbol_range',
            return_value=[{'trade_date': '2026-04-29', 'close_price': 50000}],
        ):
            result = service.refresh_plan_items(target_trade_date=self.target_trade_date, trigger='test')

        item = result['items'][0]
        self.assertEqual(item['decision_agent_name'], 'short-term-decider')
        self.assertEqual(item['eligibility_status'], ELIGIBILITY_ELIGIBLE)
        self.assertEqual(item['raw_action'], 'scalp_buy_candidate')
        self.assertEqual(item['tactical_action'], 'scalp_buy_candidate')
        self.assertEqual(item['generic_action'], 'buy')
        self.assertEqual(item['handoff_intent'], 'tactical_buy')
        self.assertEqual(item['entry_execution_mode'], 'guarded_tactical_buy')
        self.assertTrue(item['handoff_active'])
        self.assertTrue(item['exec_monitoring_active'])

    def test_refresh_plan_items_reuses_short_term_candidate_for_duplicate_refreshes(self) -> None:
        self._seed_short_term_snapshot(symbol='005930', tactical_action='watch_for_trigger')
        service = self._service(state='PREOPEN', server_time='2026-04-30T08:40:00+09:00')

        with patch(
            'app.orders.next_open_execution.load_daily_bars_for_symbol_range',
            return_value=[{'trade_date': '2026-04-29', 'close_price': 50000}],
        ) as load_bars:
            first = service.refresh_plan_items(target_trade_date=self.target_trade_date, trigger='test')
            second = service.refresh_plan_items(target_trade_date=self.target_trade_date, trigger='test_retry')

        self.assertEqual(load_bars.call_count, 2)
        self.assertEqual(first['summary']['items_total'], 1)
        self.assertEqual(second['summary']['items_total'], 1)
        self.assertEqual(first['items'][0]['plan_item_id'], second['items'][0]['plan_item_id'])

    def test_refresh_plan_items_reuses_short_term_candidate_with_string_json_payloads(self) -> None:
        snapshot_id = self._seed_short_term_snapshot(symbol='005930', tactical_action='watch_for_trigger')
        with session_scope() as session:
            snapshot = session.get(SignalDeciderDecisionSnapshot, snapshot_id)
            self.assertIsNotNone(snapshot)
            snapshot.response_payload = json.dumps(snapshot.response_payload)
            snapshot.request_payload = json.dumps(snapshot.request_payload)
            snapshot.traceability_payload = json.dumps(snapshot.traceability_payload)
            snapshot.input_feature_snapshot_payload = json.dumps(snapshot.input_feature_snapshot_payload)
            snapshot.response_summary_payload = json.dumps(snapshot.response_summary_payload)
        service = self._service(state='PREOPEN', server_time='2026-04-30T08:40:00+09:00')

        with patch(
            'app.orders.next_open_execution.load_daily_bars_for_symbol_range',
            return_value=[{'trade_date': '2026-04-29', 'close_price': 50000}],
        ):
            first = service.refresh_plan_items(target_trade_date=self.target_trade_date, trigger='test')
            second = service.refresh_plan_items(target_trade_date=self.target_trade_date, trigger='test_retry')

        self.assertEqual(first['summary']['items_total'], 1)
        self.assertEqual(first['summary']['ready_count'], 1)
        self.assertEqual(second['summary']['items_total'], 1)
        self.assertEqual(second['summary']['ready_count'], 1)
        self.assertEqual(first['items'][0]['plan_item_id'], second['items'][0]['plan_item_id'])
        self.assertEqual(second['items'][0]['eligibility_status'], ELIGIBILITY_ELIGIBLE)
        self.assertEqual(second['items'][0]['validation']['short_term']['tactical_action'], 'watch_for_trigger')

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

    async def test_run_market_open_execution_fails_closed_on_calendar_fallback(self) -> None:
        fake_order_service = FakeOrderService()
        fallback_market_state = MarketState(
            market='KRX',
            state='OPEN',
            is_trading_day=True,
            is_open=True,
            server_time='2026-04-30T09:01:00+09:00',
            session_open_at='2026-04-30T09:00:00+09:00',
            session_close_at='2026-04-30T15:30:00+09:00',
            calendar_source='exchange_calendars',
            calendar_source_status='fallback',
            calendar_source_error='kis calendar unavailable',
        )
        service = SignalDeciderNextOpenExecutionService(
            order_service=fake_order_service,
            market_state_getter=lambda: fallback_market_state,
        )

        result = await service.run_market_open_execution_if_due(
            target_trade_date=self.target_trade_date,
            trigger='test',
        )

        self.assertFalse(result['summary']['triggered'])
        self.assertEqual(result['summary']['reason'], 'calendar_unavailable')
        self.assertEqual(result['summary']['calendar_source'], 'exchange_calendars')
        self.assertEqual(len(fake_order_service.calls), 0)

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


class CandidateEnrichmentRepairTests(unittest.TestCase):
    @patch('app.eod.pykrx_enrichment.pykrx_enrichment_service')
    def test_repairs_only_missing_candidate_categories_then_reinspects(self, enrichment_service):
        service = SignalDeciderNextOpenExecutionService.__new__(SignalDeciderNextOpenExecutionService)
        service._inspect_candidate_pykrx_enrichment = MagicMock(side_effect=[
            {
                '004370': {
                    'missing_categories': ['investor_flow_symbol', 'short_selling'],
                },
                '005930': {'missing_categories': []},
            },
            {
                '004370': {'missing_categories': []},
                '005930': {'missing_categories': []},
            },
        ])

        result = service._repair_and_inspect_candidate_pykrx_enrichment(
            source_market_date=date(2026, 8, 19),
            symbols=['004370', '005930'],
            categories=['sector_classification', 'investor_flow_symbol', 'short_selling'],
        )

        enrichment_service.collect_for_market_date.assert_called_once_with(
            market_date=date(2026, 8, 19),
            markets=['KOSPI', 'KOSDAQ', 'KONEX'],
            symbols=['004370'],
            categories=['investor_flow_symbol', 'short_selling'],
            allow_kis_candidate_fallback=True,
        )
        self.assertEqual(result['004370']['missing_categories'], [])
