from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
import json
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

from sqlalchemy import delete

from app.core.settings import Settings, settings
from app.core.test_database_safety import (
    assert_safe_test_database_settings,
    assert_single_active_policy_fixture_state,
)

assert_safe_test_database_settings(settings)

from app.db import engine, session_scope
from app.db_models import (
    AgentContextFeedback,
    AgentContextFeedbackRun,
    BrokerOrder,
    CurrentMarketSnapshotCache,
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
from app.orders.exec_integration import (
    EXEC_INTENT_INTRADAY_WATCH_BUY,
    SignalDeciderExecIntegrationService,
    _delta_dispatch_batch_key,
    _partition_exec_dispatch_rows,
)
from app.orders.execution_handoff_contract import (
    EXECUTION_HANDOFF_ACK_SCHEMA_VERSION,
    EXECUTION_HANDOFF_CAPABILITY,
    EXECUTION_HANDOFF_CONTRACT_VERSION,
)
from app.orders.next_open_execution import (
    EXECUTION_STATUS_DISPATCH_PENDING_REMOTE_ACK,
    EXECUTION_STATUS_DRY_RUN,
    EXECUTION_STATUS_DELEGATED_FOR_EXEC_AGENT_SIZING,
    EXECUTION_STATUS_PLANNED,
    EXECUTION_STATUS_REVOCATION_PENDING,
    EXECUTION_STATUS_REVOCATION_MANUAL_REVIEW,
    EXECUTION_STATUS_REVOKED,
    EXECUTION_STATUS_SUBMITTED,
    ELIGIBILITY_BLOCKED,
    ELIGIBILITY_ELIGIBLE,
    ELIGIBILITY_MONITOR_CANDIDATE,
    SignalDeciderNextOpenExecutionService,
    NextOpenAccountTarget,
    _delegate_short_term_watch_entry_quality_blocks_to_warnings,
    _is_canonical_closed_market_cost_absence,
    _lock_dispatch_authority_source_tables,
    _next_open_performance_cost_evidence,
    _next_open_policy_fingerprint,
    _normalize_objective_percentile,
    _preopen_plan_covers_latest_snapshots,
    _previous_trading_day,
    _to_decimal,
    _to_int,
    _to_json_safe,
)
from app.orders.store import upsert_broker_order, upsert_order_request


def test_normalize_objective_percentile_requires_finite_bounded_units() -> None:
    assert _normalize_objective_percentile(Decimal('0.82')) == Decimal('0.82')
    assert _normalize_objective_percentile(Decimal('82'), unit='percent') == Decimal('0.82')
    for value, unit in (
        (Decimal('82'), None),
        (Decimal('-0.01'), None),
        (Decimal('101'), 'percent'),
        ('NaN', None),
        ('Infinity', None),
        ('-Infinity', 'percent'),
    ):
        assert _normalize_objective_percentile(value, unit=unit) is None


def test_dispatch_authority_source_locks_use_one_deadlock_safe_order() -> None:
    session = MagicMock()
    bind = MagicMock()
    bind.dialect.name = 'postgresql'
    bind.dialect.identifier_preparer.format_table.side_effect = (
        lambda table: table.name
    )
    session.get_bind.return_value = bind

    _lock_dispatch_authority_source_tables(session)

    statements = [str(call.args[0]) for call in session.execute.call_args_list]
    assert statements == [
        'LOCK TABLE agent_context_feedback_runs IN SHARE MODE',
        'LOCK TABLE signal_decider_decision_snapshots IN SHARE MODE',
    ]


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
        request_payload={'symbol_day_context': {'company': {'market': 'KOSPI'}}},
        updated_at=updated_at,
    )


def _preopen_plan_row(
    snapshot_id: int,
    symbol: str,
    updated_at: datetime | None = None,
    execution_status: str = 'planned',
) -> SimpleNamespace:
    target = NextOpenAccountTarget(
        label='Main',
        account_reference='KIS:12345678-01',
        budget_krw=Decimal('1000000'),
    )
    return SimpleNamespace(
        account_reference='KIS:12345678-01',
        symbol=symbol,
        decision_agent_name='signal-decider',
        decision_snapshot_id=snapshot_id,
        execution_status=execution_status,
        validation_payload={
            'execution_cost_evidence_policy_version': 'next_open_cost_evidence_v1',
            'preopen_policy_fingerprint': _next_open_policy_fingerprint([target]),
        },
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


def test_next_open_numeric_normalizers_reject_nonfinite_values() -> None:
    for value in (Decimal('NaN'), Decimal('Infinity'), float('nan'), float('inf')):
        assert _to_decimal(value) is None
        assert _to_int(value) is None

    assert _to_json_safe(
        {
            'decimal_nan': Decimal('NaN'),
            'decimal_infinity': Decimal('Infinity'),
            'float_nan': float('nan'),
            'float_infinity': float('inf'),
        }
    ) == {
        'decimal_nan': None,
        'decimal_infinity': None,
        'float_nan': None,
        'float_infinity': None,
    }


def test_pending_live_cost_accepts_only_canonical_closed_market_non_executable_spread() -> None:
    execution_feasibility = {
        'expected_slippage_bps': None,
        'expected_slippage_basis': 'unavailable',
        'expected_slippage_status': 'missing',
        'expected_slippage_reason': 'quote_spread_missing',
    }
    pre_decider_context = {
        'market_data_phase_contract': {
            'schema_version': 'market_data_phase_contract_v1',
            'recommendation_basis': 'latest_completed_eod',
            'realtime_quote_orderbook_required': False,
            'eod_reference_price_accepted': True,
            'not_applicable_realtime_is_expected': True,
        },
        'quote_orderbook_snapshot': {
            'status': 'stale_eod',
            'snapshot_type': 'eod_stale',
            'quote_freshness_status': 'stale',
            'spread_bps': None,
        },
    }

    assert _is_canonical_closed_market_cost_absence(
        pre_decider_enrichment_context=pre_decider_context,
        execution_feasibility_context=execution_feasibility,
    )
    pre_decider_context['quote_orderbook_snapshot']['spread_bps'] = 17.5
    execution_feasibility['expected_slippage_status'] = 'unavailable'
    execution_feasibility['expected_slippage_reason'] = 'quote_spread_not_executable_eod'
    assert _is_canonical_closed_market_cost_absence(
        pre_decider_enrichment_context=pre_decider_context,
        execution_feasibility_context=execution_feasibility,
    )
    pre_decider_context['quote_orderbook_snapshot']['spread_bps'] = ''
    execution_feasibility['expected_slippage_status'] = 'invalid'
    execution_feasibility['expected_slippage_reason'] = 'quote_spread_nonfinite_or_negative'
    assert not _is_canonical_closed_market_cost_absence(
        pre_decider_enrichment_context=pre_decider_context,
        execution_feasibility_context=execution_feasibility,
    )


def test_preopen_plan_reuses_only_exact_latest_feedback_set() -> None:
    target = NextOpenAccountTarget(
        label='Main',
        account_reference='KIS:12345678-01',
        budget_krw=Decimal('1000000'),
    )
    snapshots = [_preopen_snapshot(1, '005930'), _preopen_snapshot(2, '000660')]

    assert _preopen_plan_covers_latest_snapshots(
        existing_rows=[
            _preopen_plan_row(1, '005930', execution_status=EXECUTION_STATUS_SUBMITTED),
            _preopen_plan_row(2, '000660', execution_status=EXECUTION_STATUS_SUBMITTED),
        ],
        snapshots=snapshots,
        account_targets=[target],
    ) is True
    assert _preopen_plan_covers_latest_snapshots(
        existing_rows=[
            _preopen_plan_row(1, '005930', execution_status=EXECUTION_STATUS_SUBMITTED),
            _preopen_plan_row(2, '000660', execution_status=EXECUTION_STATUS_SUBMITTED),
        ],
        snapshots=[_preopen_snapshot(3, '005930'), _preopen_snapshot(2, '000660')],
        account_targets=[target],
    ) is False


def test_terminal_handoff_is_not_reused_when_latest_signal_decision_is_wait() -> None:
    target = NextOpenAccountTarget(
        label='Main',
        account_reference='KIS:12345678-01',
        budget_krw=Decimal('1000000'),
    )

    assert _preopen_plan_covers_latest_snapshots(
        existing_rows=[
            _preopen_plan_row(
                1,
                '005930',
                execution_status=EXECUTION_STATUS_DELEGATED_FOR_EXEC_AGENT_SIZING,
            )
        ],
        snapshots=[_preopen_snapshot(2, '005930', action='wait')],
        account_targets=[target],
    ) is False


def test_terminal_short_term_handoff_is_not_reused_when_latest_tactical_decision_is_wait() -> None:
    target = NextOpenAccountTarget(
        label='Main',
        account_reference='KIS:12345678-01',
        budget_krw=Decimal('1000000'),
    )
    prior = SimpleNamespace(
        account_reference=target.account_reference,
        symbol='005930',
        decision_agent_name='short-term-decider',
        decision_snapshot_id=1,
        execution_status=EXECUTION_STATUS_DELEGATED_FOR_EXEC_AGENT_SIZING,
        validation_payload={},
        updated_at=None,
    )
    latest = SimpleNamespace(
        id=2,
        symbol='005930',
        agent_name='short-term-decider',
        decision_action='wait',
        next_day_open_action='wait',
        response_payload={'tactical_action': 'wait'},
        request_payload={'symbol_day_context': {'company': {'market': 'KOSPI'}}},
    )

    assert _preopen_plan_covers_latest_snapshots(
        existing_rows=[prior],
        snapshots=[latest],
        account_targets=[target],
    ) is False


def test_preopen_plan_refreshes_legacy_signal_row_without_current_cost_contract() -> None:
    target = NextOpenAccountTarget(
        label='Main',
        account_reference='KIS:12345678-01',
        budget_krw=Decimal('1000000'),
    )
    row = _preopen_plan_row(1, '005930')
    row.validation_payload = {}

    assert _preopen_plan_covers_latest_snapshots(
        existing_rows=[row],
        snapshots=[_preopen_snapshot(1, '005930')],
        account_targets=[target],
    ) is False


def test_preopen_policy_fingerprint_binds_mode_cost_thresholds_and_account_budget() -> None:
    target = NextOpenAccountTarget(
        label='Main',
        account_reference='KIS:12345678-01',
        budget_krw=Decimal('1000000'),
    )
    baseline_fingerprint = _next_open_policy_fingerprint([target])

    changed_settings = {
        'signal_decider_exec_integration_mode': (
            'dispatch' if settings.signal_decider_exec_integration_mode != 'dispatch' else 'disabled'
        ),
        'signal_decider_next_open_non_spread_round_trip_cost_bps': (
            settings.signal_decider_next_open_non_spread_round_trip_cost_bps + 1
        ),
        'signal_decider_next_open_min_confidence_score_10': (
            settings.signal_decider_next_open_min_confidence_score_10 + 0.1
        ),
        'signal_decider_next_open_min_expected_return_pct': (
            settings.signal_decider_next_open_min_expected_return_pct + 0.1
        ),
        'signal_decider_next_open_min_reward_risk_ratio': (
            settings.signal_decider_next_open_min_reward_risk_ratio + 0.1
        ),
        'signal_decider_next_open_require_active_policy': (
            not settings.signal_decider_next_open_require_active_policy
        ),
        'signal_decider_next_open_block_on_data_warnings': (
            not settings.signal_decider_next_open_block_on_data_warnings
        ),
        'signal_decider_next_open_block_on_specialist_disagreement': (
            not settings.signal_decider_next_open_block_on_specialist_disagreement
        ),
        'signal_decider_next_open_require_target_and_stop': (
            not settings.signal_decider_next_open_require_target_and_stop
        ),
        'signal_decider_wait_conflict_gate_mode': (
            'hard_block'
            if settings.signal_decider_wait_conflict_gate_mode != 'hard_block'
            else 'report_only'
        ),
        'signal_decider_wait_conflict_probe_quantity': (
            settings.signal_decider_wait_conflict_probe_quantity + 1
        ),
    }
    for setting_name, changed_value in changed_settings.items():
        with patch.object(settings, setting_name, changed_value):
            assert _next_open_policy_fingerprint([target]) != baseline_fingerprint, setting_name

    changed_budget_target = NextOpenAccountTarget(
        label=target.label,
        account_reference=target.account_reference,
        budget_krw=target.budget_krw + Decimal('1'),
    )
    assert _next_open_policy_fingerprint([changed_budget_target]) != baseline_fingerprint


def test_preopen_plan_never_reuses_nonterminal_row_even_with_matching_fingerprint() -> None:
    target = NextOpenAccountTarget(
        label='Main',
        account_reference='KIS:12345678-01',
        budget_krw=Decimal('1000000'),
    )

    assert _preopen_plan_covers_latest_snapshots(
        existing_rows=[_preopen_plan_row(1, '005930')],
        snapshots=[_preopen_snapshot(1, '005930')],
        account_targets=[target],
    ) is False


def test_exec_dispatch_rejects_legacy_signal_row_without_raw_cost_provenance() -> None:
    row = SimpleNamespace(
        decision_agent_name='signal-decider',
        planned_side='buy',
        eligibility_status=ELIGIBILITY_ELIGIBLE,
        source_market_date=date(2026, 4, 29),
        validation_payload={
            'decision_input_quality_context': {
                'execution_feasibility': {
                    'expected_slippage_bps': 0,
                    'expected_slippage_basis': 'quote_spread_bps',
                },
            },
            'performance_gate': {
                'expected_return_economics': {
                    'status': 'ready',
                    'expected_slippage_bps': '0',
                    'expected_slippage_basis': 'quote_spread_bps',
                },
            },
        },
    )

    assert SignalDeciderExecIntegrationService._row_is_dispatchable(row) is False


def test_exec_dispatch_requires_consistent_pending_cost_contract() -> None:
    pre_decider_context = {
        'schema_version': 'pre_decider_enrichment_v1',
        'market_data_phase_contract': {
            'schema_version': 'market_data_phase_contract_v1',
            'recommendation_basis': 'latest_completed_eod',
            'realtime_quote_orderbook_required': False,
            'eod_reference_price_accepted': True,
            'not_applicable_realtime_is_expected': True,
        },
        'quote_orderbook_snapshot': {
            'status': 'stale_eod',
            'snapshot_type': 'eod_stale',
            'quote_freshness_status': 'stale',
            'spread_bps': None,
        },
    }

    def validation(*, warnings: list[dict] | None = None, slippage_bps=None) -> dict:
        return {
            'execution_cost_evidence_policy_version': 'next_open_cost_evidence_v1',
            'blocking_reasons': [],
            'warnings': warnings if warnings is not None else [
                {'code': 'transaction_cost_evidence_pending_live_delegated_to_exec_watch'}
            ],
            'pre_decider_enrichment_context': pre_decider_context,
            'performance_gate': {
                'expected_return_economics': {
                    'status': 'pending_live_cost_confirmation',
                    'expected_slippage_bps': slippage_bps,
                    'expected_slippage_basis': 'unavailable',
                    'server_non_spread_round_trip_cost_basis': 'deferred_to_exec_final_full_cost_check',
                    'estimated_total_transaction_cost_bps': None,
                },
            },
        }

    row = SimpleNamespace(
        decision_agent_name='signal-decider',
        planned_side='buy',
        eligibility_status=ELIGIBILITY_ELIGIBLE,
        source_market_date=date(2026, 4, 29),
        validation_payload=validation(),
    )
    assert SignalDeciderExecIntegrationService._row_is_dispatchable(row) is True
    invalid_schema = validation()
    invalid_schema['pre_decider_enrichment_context'] = {
        **pre_decider_context,
        'schema_version': 'legacy',
    }
    for candidate in (validation(warnings=[]), validation(slippage_bps='0'), invalid_schema):
        row.validation_payload = candidate
        assert SignalDeciderExecIntegrationService._row_is_dispatchable(row) is False


def test_next_open_cost_gate_never_trusts_precomputed_cost_without_raw_quote() -> None:
    execution_feasibility = {
        'expected_slippage_bps': 0,
        'expected_slippage_basis': 'quote_spread_bps',
    }

    assert _next_open_performance_cost_evidence(
        dispatches_to_exec=True,
        execution_feasibility_context=execution_feasibility,
        pre_decider_enrichment_context={},
    ) == (None, 'unavailable')
    assert _next_open_performance_cost_evidence(
        dispatches_to_exec=False,
        execution_feasibility_context=execution_feasibility,
        pre_decider_enrichment_context={'quote_orderbook_snapshot': {'status': 'available'}},
    ) == (None, 'unavailable')


def test_preopen_plan_revalidates_unrouted_short_term_watch_feedback() -> None:
    snapshot = SimpleNamespace(
        id=7,
        symbol='035420',
        agent_name='short-term-decider',
        response_payload={'tactical_action': 'watch_for_trigger'},
        request_payload={'symbol_day_context': {'company': {'market': 'KOSPI'}}},
    )
    synthetic_target = NextOpenAccountTarget(
        label='UNSPECIFIED_ROUTE',
        account_reference='KIS:00000000-00',
        budget_krw=Decimal('0'),
    )
    row = SimpleNamespace(
        account_reference='KIS:00000000-00',
        symbol='035420',
        decision_agent_name='short-term-decider',
        decision_snapshot_id=7,
        execution_status='planned',
        validation_payload={
            'preopen_policy_fingerprint': _next_open_policy_fingerprint([synthetic_target]),
            'alignment_gate': {
                'enforcement_mode': settings.signal_decider_wait_conflict_gate_mode,
                'probe_quantity': settings.signal_decider_wait_conflict_probe_quantity,
            }
        },
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
    ) is False
    changed_mode = (
        'dispatch'
        if settings.signal_decider_exec_integration_mode != 'dispatch'
        else 'disabled'
    )
    with patch.object(settings, 'signal_decider_exec_integration_mode', changed_mode):
        assert _preopen_plan_covers_latest_snapshots(
            existing_rows=[row],
            snapshots=[snapshot],
            account_targets=[],
        ) is False


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


def test_preopen_plan_revalidates_current_gate_settings_for_terminal_rows() -> None:
    snapshot = SimpleNamespace(
        id=8,
        symbol='035420',
        agent_name='short-term-decider',
        response_payload={'tactical_action': 'watch_for_trigger'},
        request_payload={'symbol_day_context': {'company': {'market': 'KOSPI'}}},
        updated_at=None,
    )
    target = NextOpenAccountTarget(
        label='Main',
        account_reference='KIS:12345678-01',
        budget_krw=Decimal('1000000'),
    )

    stored_fingerprint = _next_open_policy_fingerprint([target])

    def row(execution_status: str) -> SimpleNamespace:
        return SimpleNamespace(
            account_reference=target.account_reference,
            symbol=snapshot.symbol,
            decision_agent_name='short-term-decider',
            decision_snapshot_id=snapshot.id,
            execution_status=execution_status,
            validation_payload={
                'preopen_policy_fingerprint': stored_fingerprint,
                'alignment_gate': {
                    'enforcement_mode': 'report_only',
                    'probe_quantity': 1,
                }
            },
            updated_at=None,
        )

    with (
        patch.object(settings, 'signal_decider_wait_conflict_gate_mode', 'downsize_probe'),
        patch.object(settings, 'signal_decider_wait_conflict_probe_quantity', 3),
    ):
        for terminal_status in (
            EXECUTION_STATUS_SUBMITTED,
            EXECUTION_STATUS_DELEGATED_FOR_EXEC_AGENT_SIZING,
        ):
            assert _preopen_plan_covers_latest_snapshots(
                existing_rows=[row(terminal_status)],
                snapshots=[snapshot],
                account_targets=[target],
            ) is False, terminal_status

        for active_status in ('planned', EXECUTION_STATUS_DRY_RUN, 'submit_failed'):
            assert _preopen_plan_covers_latest_snapshots(
                existing_rows=[row(active_status)],
                snapshots=[snapshot],
                account_targets=[target],
            ) is False, active_status


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
        AgentContextFeedbackRun.__table__.create(bind=engine, checkfirst=True)
        OrderRequest.__table__.create(bind=engine, checkfirst=True)
        BrokerOrder.__table__.create(bind=engine, checkfirst=True)
        CurrentMarketSnapshotCache.__table__.create(bind=engine, checkfirst=True)
        SignalDeciderDecisionSnapshot.__table__.create(bind=engine, checkfirst=True)
        SignalDeciderPolicyVersion.__table__.create(bind=engine, checkfirst=True)
        SignalDeciderExecutionPlanItem.__table__.create(bind=engine, checkfirst=True)

    def setUp(self) -> None:
        suffix = uuid4().hex[:8].upper()
        self.policy_version = f'signal_decider_policy_test_{suffix.lower()}'
        self.account_reference = f'KIS:12{suffix[:6]}-01'
        self.market_date = date(2026, 4, 29)
        self.target_trade_date = date(2026, 4, 30)
        self.feedback_run_ids: list[str] = []
        self.original_settings = {
            'signal_decider_next_open_account_targets': settings.signal_decider_next_open_account_targets,
            'signal_decider_next_open_enabled': settings.signal_decider_next_open_enabled,
            'signal_decider_next_open_dry_run': settings.signal_decider_next_open_dry_run,
            'signal_decider_next_open_min_confidence_score_10': settings.signal_decider_next_open_min_confidence_score_10,
            'signal_decider_next_open_min_expected_return_pct': settings.signal_decider_next_open_min_expected_return_pct,
            'signal_decider_next_open_non_spread_round_trip_cost_bps': settings.signal_decider_next_open_non_spread_round_trip_cost_bps,
            'signal_decider_next_open_require_active_policy': settings.signal_decider_next_open_require_active_policy,
            'signal_decider_next_open_block_on_data_warnings': settings.signal_decider_next_open_block_on_data_warnings,
            'signal_decider_next_open_block_on_specialist_disagreement': settings.signal_decider_next_open_block_on_specialist_disagreement,
            'signal_decider_next_open_require_target_and_stop': settings.signal_decider_next_open_require_target_and_stop,
            'signal_decider_wait_conflict_gate_mode': settings.signal_decider_wait_conflict_gate_mode,
            'signal_decider_wait_conflict_probe_quantity': settings.signal_decider_wait_conflict_probe_quantity,
            'signal_decider_exec_integration_mode': settings.signal_decider_exec_integration_mode,
            'signal_decider_exec_runtime_mode': settings.signal_decider_exec_runtime_mode,
        }
        self._previous_active_policy_states: list[tuple[str, datetime | None]] = []
        self.addCleanup(self._restore_fixture_state)
        settings.signal_decider_next_open_account_targets = f'Main={self.account_reference}@1500000'
        settings.signal_decider_next_open_enabled = True
        settings.signal_decider_next_open_dry_run = False
        settings.signal_decider_next_open_min_confidence_score_10 = 6.0
        settings.signal_decider_next_open_min_expected_return_pct = 0.0
        self.decision_quality_clock_patcher = patch(
            'app.agent_context.decision_input_quality._utcnow',
            return_value=datetime(2026, 4, 29, 0, 57, 20, tzinfo=timezone.utc),
        )
        self.decision_quality_clock_patcher.start()
        self.addCleanup(self.decision_quality_clock_patcher.stop)
        settings.signal_decider_next_open_non_spread_round_trip_cost_bps = 0.0
        settings.signal_decider_next_open_require_active_policy = True
        settings.signal_decider_next_open_block_on_data_warnings = True
        settings.signal_decider_next_open_block_on_specialist_disagreement = True
        settings.signal_decider_next_open_require_target_and_stop = True
        settings.signal_decider_wait_conflict_gate_mode = 'report_only'
        settings.signal_decider_wait_conflict_probe_quantity = 1
        settings.signal_decider_exec_integration_mode = 'disabled'
        settings.signal_decider_exec_runtime_mode = 'mock'
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

    def _restore_fixture_state(self) -> None:
        reset_krx_calendar_provider()
        for key, value in self.original_settings.items():
            setattr(settings, key, value)
        with session_scope() as session:
            session.execute(
                delete(AgentContextFeedbackRun).where(
                    AgentContextFeedbackRun.run_id.in_(self.feedback_run_ids)
                )
            )
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

            session.flush()
            for policy_version, retired_at in self._previous_active_policy_states:
                previous_policy = (
                    session.query(SignalDeciderPolicyVersion)
                    .filter(SignalDeciderPolicyVersion.policy_version == policy_version)
                    .one_or_none()
                )
                if previous_policy is not None:
                    previous_policy.status = 'active'
                    previous_policy.retired_at = retired_at

    def test_dispatch_generation_samples_market_clock_after_authority_locks(
        self,
    ) -> None:
        authority_locked = False
        market_state_call_count = 0

        def mark_authority_locked(_session) -> None:
            nonlocal authority_locked
            authority_locked = True

        def get_market_state() -> MarketState:
            nonlocal market_state_call_count
            market_state_call_count += 1
            if market_state_call_count > 1:
                self.assertTrue(authority_locked)
            return MarketState(
                market='KRX',
                state='PREOPEN',
                is_trading_day=True,
                is_open=False,
                server_time='2026-04-30T08:59:59+09:00',
                session_open_at='2026-04-30T09:00:00+09:00',
                session_close_at='2026-04-30T15:30:00+09:00',
            )

        service = SignalDeciderNextOpenExecutionService(
            market_state_getter=get_market_state
        )
        with patch(
            'app.orders.next_open_execution._lock_dispatch_authority_source_tables',
            side_effect=mark_authority_locked,
        ):
            generation = service.inspect_dispatch_authority_generation(
                target_trade_date=self.target_trade_date,
                expected_row_bindings={},
            )

        self.assertEqual(generation['status'], 'valid')
        self.assertEqual(market_state_call_count, 2)

    def _seed_policy_version(self) -> None:
        with session_scope() as session:
            active_policies = (
                session.query(SignalDeciderPolicyVersion)
                .filter(SignalDeciderPolicyVersion.status == 'active')
                .all()
            )
            assert_single_active_policy_fixture_state(
                [active_policy.policy_version for active_policy in active_policies]
            )
            for active_policy in active_policies:
                self._previous_active_policy_states.append(
                    (active_policy.policy_version, active_policy.retired_at)
                )
                active_policy.status = 'retired'
                active_policy.retired_at = datetime.now(timezone.utc)
            session.flush()
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
            'quote_orderbook_snapshot': {
                'status': 'available',
                'snapshot_type': 'current_quote_cache',
                'market_date': '2026-04-29',
                'quote_freshness_status': 'fresh',
                'quote_freshness_timestamp': '2026-04-29T09:57:00+09:00',
                'as_of_market_date': '2026-04-29',
                'current_price': 50_000,
                'bid_price': 49_975,
                'ask_price': 50_000,
                'spread_points': 25,
                'spread_bps': 5,
                'spread_source_context': 'quote_context',
                'spread_component_source_context': {
                    'current_price': 'quote_context',
                    'bid_price': 'quote_context',
                    'ask_price': 'quote_context',
                    'spread_points': 'quote_context',
                    'spread_bps': 'quote_context',
                },
                'freshness': {
                    'quote_context': {
                        'freshness_status': 'fresh',
                        'provenance_status': 'provider_sourced',
                        'as_of_market_date': '2026-04-29',
                        'as_of_timestamp': '2026-04-29T09:57:00+09:00',
                        'source_tables': ['current_quote_cache', 'quote_snapshots'],
                        'source_provider': 'kis',
                    },
                },
                'source': {'quote_context': 'kis'},
            },
            'trading_value': {
                'status': 'available',
                'source': trading_value_source,
                'raw_trading_value_krw': trading_value_krw if trading_value_source == 'raw' else None,
                'estimated_trading_value_krw': trading_value_krw if trading_value_source == 'estimated' else None,
                'trading_value_krw': trading_value_krw,
            },
            'flow_short_sector_context': {'status': 'available'},
        }

    def _closed_market_pre_decider_context(self, *, spread_bps: float | None = None) -> dict:
        return {
            'schema_version': 'pre_decider_enrichment_v1',
            'market_data_phase_contract': {
                'schema_version': 'market_data_phase_contract_v1',
                'recommendation_basis': 'latest_completed_eod',
                'realtime_quote_orderbook_required': False,
                'eod_reference_price_accepted': True,
                'not_applicable_realtime_is_expected': True,
            },
            'quote_orderbook_snapshot': {
                'status': 'stale_eod',
                'reason': 'closed_market_eod_snapshot',
                'snapshot_type': 'eod_stale',
                'market_date': '2026-04-29',
                'quote_freshness_status': 'stale',
                'as_of_market_date': '2026-04-29',
                'as_of_timestamp': '2026-04-29T15:30:00+09:00',
                'spread_bps': spread_bps,
            },
            'trading_value': {
                'status': 'available',
                'source': 'raw',
                'raw_trading_value_krw': 8_000_000_000,
                'trading_value_krw': 8_000_000_000,
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

    def _live_market_snapshot_context(
        self,
        *,
        observed_at: datetime,
        advancers: int = 250,
        unchanged: int = 20,
        decliners: int = 680,
        partial: bool = False,
    ) -> dict[str, object]:
        symbol_count = advancers + unchanged + decliners
        observed_at_text = observed_at.isoformat()
        snapshot_status = 'partial' if partial else 'ready'
        return {
            'source': 'kis_market_snapshot_v1',
            'source_table': 'current_market_snapshot_cache',
            'snapshot_status': snapshot_status,
            'last_refresh_status': 'degraded' if partial else 'success',
            'as_of': observed_at_text,
            'payload': {
                'status': snapshot_status,
                'as_of': observed_at_text,
                'missing_fields': ['markets.investor_flow'] if partial else [],
                'legs': {
                    'index': {'status': 'ready'},
                    'flow': {'status': 'partial' if partial else 'ready'},
                    'fx': {'status': 'ready'},
                },
                'markets': {
                    'kospi': {
                        'label': 'KOSPI',
                        'index_code': '0001',
                        'value': 2673.0,
                        'change': -27.0,
                        'change_percent': -1.0,
                        'open': 2690.0,
                        'trading_value': 3_000_000,
                        'trading_value_unit': 'million_krw',
                        'prior_trading_value': None,
                        'prior_trading_value_unit': None,
                        'upper_limit_count': 0,
                        'advancers': advancers,
                        'unchanged': unchanged,
                        'decliners': decliners,
                        'lower_limit_count': 0,
                        'breadth_raw_buckets': {
                            'upper_limit_count': 0,
                            'advancers': advancers,
                            'unchanged': unchanged,
                            'decliners': decliners,
                            'lower_limit_count': 0,
                        },
                        'breadth_completeness': {
                            'status': 'verified',
                            'expected_symbol_count': symbol_count,
                            'observed_symbol_count': symbol_count,
                            'limit_bucket_semantics': 'included_in_direction_counts',
                            'expected_universe': {
                                'provider': 'kis_master',
                                'market_date': observed_at.date().isoformat(),
                                'completed_at': (
                                    observed_at.replace(hour=8, minute=0, second=0)
                                ).isoformat(),
                                'symbol_count': symbol_count,
                                'membership_sha256': 'a' * 64,
                            },
                        },
                        'provider_business_date': observed_at.date().isoformat(),
                        'provider_session_time': observed_at.strftime('%H:%M:%S'),
                        'provider_observed_at': observed_at_text,
                        'index_leg': {
                            'status': 'ready',
                            'observation_source': 'kis_index_intraday_bar',
                            'provider_observed_at': observed_at_text,
                        },
                    },
                },
            },
        }

    def _seed_snapshot(
        self,
        *,
        symbol: str,
        market: str | None = 'KOSPI',
        next_day_open_action: str = 'buy',
        data_warnings: list[str] | None = None,
        target_price: int = 54000,
        expected_return_pct: float = 4.5,
        stop_loss_price: int = 47000,
        pre_decider_enrichment_context: dict[str, object] | None = None,
        decision_run_id: str | None = None,
    ) -> int:
        symbol_day_context = {
            'decider_data_quality_context': self._quality_context(),
            'pre_decider_enrichment_context': (
                self._pre_decider_context()
                if pre_decider_enrichment_context is None
                else pre_decider_enrichment_context
            ),
            'market_regime_context': self._market_regime_context(),
        }
        if market is not None:
            symbol_day_context['company'] = {'market': market}
        with session_scope() as session:
            row = SignalDeciderDecisionSnapshot(
                feedback_id=None,
                market_date=self.market_date,
                symbol=symbol,
                agent_name='signal-decider',
                agent_version='test-agent',
                decision_run_id=decision_run_id or f'run-{symbol}',
                feedback_received_at=datetime(2026, 4, 29, 6, 0, tzinfo=timezone.utc),
                policy_version=self.policy_version,
                decision_action=next_day_open_action,
                preferred_action=next_day_open_action,
                next_day_open_action=next_day_open_action,
                entry_price_basis='next_open',
                target_price=target_price,
                expected_return_pct=expected_return_pct,
                stop_loss_price=stop_loss_price,
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
                    'symbol_day_context': symbol_day_context,
                },
                response_payload={'next_day_open_action': next_day_open_action},
                response_summary_payload={'policy_version': self.policy_version},
            )
            session.add(row)
            session.flush()
            return row.id

    def _seed_feedback_run(
        self,
        *,
        run_id: str,
        symbols: list[str],
        roles: list[str],
        role_symbol_scopes: dict[str, list[str]] | None = None,
        started_at: datetime,
        status: str = 'completed',
        completed_count: int | None = None,
        failed_count: int = 0,
    ) -> None:
        self.feedback_run_ids.append(run_id)
        with session_scope() as session:
            session.add(
                AgentContextFeedbackRun(
                    run_id=run_id,
                    market_date=self.market_date,
                    status=status,
                    symbols_total=len(symbols),
                    completed_count=(len(symbols) if completed_count is None else completed_count),
                    failed_count=failed_count,
                    request_payload={
                        'symbols': symbols,
                        'requested_symbols': symbols,
                        'roles': roles,
                        'role_symbol_scopes': role_symbol_scopes
                        or {role: symbols for role in roles},
                    },
                    summary_payload={},
                    started_at=started_at,
                    completed_at=started_at,
                )
            )

    def _seed_short_term_snapshot(
        self,
        *,
        symbol: str,
        market: str | None = 'KOSPI',
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
        objective_technical_snapshot: dict[str, object] | None = None,
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
        if market is not None or objective_technical_snapshot is not None:
            request_payload['symbol_day_context'] = {}
            if market is not None:
                request_payload['symbol_day_context']['company'] = {'market': market}
            if objective_technical_snapshot is not None:
                request_payload['symbol_day_context']['technical_snapshot'] = objective_technical_snapshot
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

    def _mark_row_as_handed_off(self, *, symbol: str = '005930') -> int:
        submitted_at = datetime(2026, 4, 30, 0, 1, tzinfo=timezone.utc)
        with session_scope() as session:
            row = (
                session.query(SignalDeciderExecutionPlanItem)
                .filter(
                    SignalDeciderExecutionPlanItem.account_reference == self.account_reference,
                    SignalDeciderExecutionPlanItem.symbol == symbol,
                )
                .one()
            )
            row.execution_status = EXECUTION_STATUS_SUBMITTED
            row.submitted_at = submitted_at
            row.submission_payload = {
                'mode': 'exec_dispatch',
                'exec_dispatch': {
                    'plan_id': f'old-plan-{row.id}',
                    'plan_fingerprint': 'old-fingerprint',
                    'dispatch_status': 'accepted',
                    'last_success_at': submitted_at.isoformat(),
                    'item_links': [
                        {
                            'core_item_key': f'core-plan-{row.id}:{intent}',
                            'intent': intent,
                            'side': 'buy' if intent == 'next_open_buy' else 'sell',
                            'exec_plan_id': f'exec-old-plan-{row.id}',
                            'exec_plan_item_id': f'exec-{row.id}-{intent}',
                            'remote_status': 'accepted',
                        }
                        for intent in ('next_open_buy', 'target_sell', 'stop_sell')
                    ],
                },
            }
            return row.id

    def _ack_pending_revocation(
        self,
        *,
        service: SignalDeciderNextOpenExecutionService,
        item: dict,
    ) -> None:
        revocation = item['submission']['revocation']
        request_payload = {
            'plan_id': revocation['plan_id'],
            'replacement_decision_snapshot_id': revocation['replacement_decision_snapshot_id'],
            'items': [
                {
                    'core_item_key': link['core_item_key'],
                    'execution_plan_item_id': link['exec_plan_item_id'],
                }
                for link in revocation['item_links']
            ],
        }
        response_payload = {
            'contract_ack': {'status': 'revoked'},
            'item_acks': [
                {**request_item, 'status': 'revoked'}
                for request_item in request_payload['items']
            ],
        }
        service.apply_exec_revocation_ack(
            row_id=item['plan_item_id'],
            request_payload=request_payload,
            response_payload=response_payload,
            ack_status='revoked',
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
        settings.signal_decider_exec_integration_mode = 'dispatch'
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
        self.assertIsNone(items_by_symbol['005930']['planned_quantity'])
        self.assertEqual(items_by_symbol['000660']['decision_snapshot_id'], blocked_snapshot_id)
        self.assertEqual(items_by_symbol['000660']['eligibility_status'], ELIGIBILITY_BLOCKED)
        self.assertEqual(
            items_by_symbol['000660']['validation']['blocking_reasons'][0]['code'],
            'data_quality_warnings_present',
        )
        self.assertNotIn('035420', items_by_symbol)

    def test_refresh_plan_items_accepts_only_explicit_krx_market_snapshots(self) -> None:
        settings.signal_decider_exec_integration_mode = 'dispatch'
        domestic_snapshot_id = self._seed_snapshot(symbol='005930', market='KOSPI')
        self._seed_snapshot(symbol='AAPL', market='US')
        self._seed_snapshot(symbol='000660', market=None)
        conflicting_snapshot_id = self._seed_snapshot(symbol='CONFLICT', market='US')
        domestic_conflict_id = self._seed_snapshot(
            symbol='DOMESTIC-CONFLICT',
            market='KOSDAQ',
        )
        with session_scope() as session:
            conflicting_snapshot = session.get(
                SignalDeciderDecisionSnapshot,
                conflicting_snapshot_id,
            )
            conflicting_snapshot.request_payload = {
                **(conflicting_snapshot.request_payload or {}),
                'symbol_day_context': {
                    **((conflicting_snapshot.request_payload or {}).get('symbol_day_context') or {}),
                    'market': 'KOSPI',
                    'company': {'market': 'US'},
                },
            }
            domestic_conflict = session.get(
                SignalDeciderDecisionSnapshot,
                domestic_conflict_id,
            )
            domestic_conflict.request_payload = {
                **(domestic_conflict.request_payload or {}),
                'symbol_day_context': {
                    **((domestic_conflict.request_payload or {}).get('symbol_day_context') or {}),
                    'market': 'KOSPI',
                    'company': {'market': 'KOSDAQ'},
                },
            }
        service = self._service(state='PREOPEN', server_time='2026-04-30T08:40:00+09:00')

        with patch(
            'app.orders.next_open_execution.load_daily_bars_for_symbol_range',
            return_value=[{'trade_date': '2026-04-29', 'close_price': 50000}],
        ):
            result = service.refresh_plan_items(
                target_trade_date=self.target_trade_date,
                trigger='test',
            )

        self.assertEqual(result['summary']['items_total'], 1)
        self.assertEqual(result['items'][0]['symbol'], '005930')
        self.assertEqual(result['items'][0]['decision_snapshot_id'], domestic_snapshot_id)

    def test_next_open_threshold_settings_reject_nonfinite_or_unsafe_values(self) -> None:
        invalid_by_field = {
            'signal_decider_next_open_min_confidence_score_10': (
                float('nan'),
                float('inf'),
                float('-inf'),
                -0.1,
                10.1,
            ),
            'signal_decider_next_open_min_expected_return_pct': (
                float('nan'),
                float('inf'),
                float('-inf'),
                -0.1,
            ),
        }
        for field_name, invalid_values in invalid_by_field.items():
            for invalid in invalid_values:
                with self.subTest(field_name=field_name, invalid=invalid), self.assertRaises(ValueError):
                    Settings(_env_file=None, **{field_name: invalid})

    def test_refresh_plan_items_fails_closed_if_threshold_is_mutated_nonfinite(self) -> None:
        settings.signal_decider_exec_integration_mode = 'dispatch'
        self._seed_snapshot(symbol='005930')
        service = self._service(state='PREOPEN', server_time='2026-04-30T08:40:00+09:00')

        for field_name, invalid in (
            ('signal_decider_next_open_min_confidence_score_10', float('nan')),
            ('signal_decider_next_open_min_expected_return_pct', float('-inf')),
        ):
            with self.subTest(field_name=field_name), patch.object(settings, field_name, invalid), patch(
                'app.orders.next_open_execution.load_daily_bars_for_symbol_range',
                return_value=[{'trade_date': '2026-04-29', 'close_price': 50000}],
            ):
                result = service.refresh_plan_items(
                    target_trade_date=self.target_trade_date,
                    trigger='test',
                    force=True,
                )

            block_codes = {
                reason['code']
                for reason in result['items'][0]['validation']['blocking_reasons']
            }
            self.assertIn('next_open_threshold_configuration_invalid', block_codes)

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
        settings.signal_decider_exec_integration_mode = 'dispatch'
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

    def test_enrichment_blocker_preserves_revocation_and_remote_ack_states(self) -> None:
        settings.signal_decider_exec_integration_mode = 'dispatch'
        self._seed_snapshot(symbol='005930')
        service = self._service(state='PREOPEN', server_time='2026-04-30T08:40:00+09:00')
        with patch(
            'app.orders.next_open_execution.load_daily_bars_for_symbol_range',
            return_value=[{'trade_date': '2026-04-29', 'close_price': 50000}],
        ):
            initial = service.refresh_plan_items(
                target_trade_date=self.target_trade_date,
                trigger='seed_guarded_row',
            )
        row_id = initial['items'][0]['plan_item_id']
        blocked_preflight = {
            'market_date': self.market_date.isoformat(),
            'status': 'blocked',
            'allowed': False,
            'failed_categories': ['investor_flow_symbol'],
        }

        for execution_status in (
            EXECUTION_STATUS_DISPATCH_PENDING_REMOTE_ACK,
            EXECUTION_STATUS_REVOCATION_PENDING,
            EXECUTION_STATUS_REVOCATION_MANUAL_REVIEW,
            EXECUTION_STATUS_REVOKED,
        ):
            with self.subTest(execution_status=execution_status):
                guarded_payload = {
                    'sentinel': execution_status,
                    'exec_dispatch': {'dispatch_status': 'accepted'},
                    'revocation': {'status': 'pending'},
                }
                with session_scope() as session:
                    row = session.get(SignalDeciderExecutionPlanItem, row_id)
                    row.execution_status = execution_status
                    row.eligibility_status = ELIGIBILITY_BLOCKED
                    row.submission_payload = guarded_payload
                    prior_last_evaluated_at = row.last_evaluated_at

                service.block_existing_plan_rows_for_enrichment_preflight(
                    target_trade_date=self.target_trade_date,
                    source_market_date=self.market_date,
                    preflight=blocked_preflight,
                )

                with session_scope() as session:
                    row = session.get(SignalDeciderExecutionPlanItem, row_id)
                    self.assertEqual(row.execution_status, execution_status)
                    self.assertEqual(row.eligibility_status, ELIGIBILITY_BLOCKED)
                    self.assertEqual(row.submission_payload, guarded_payload)
                    self.assertEqual(row.last_evaluated_at, prior_last_evaluated_at)

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
        self.assertEqual(short_item['eligibility_status'], ELIGIBILITY_MONITOR_CANDIDATE)
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
                'app.eod.pykrx_enrichment.pykrx_enrichment_service.collect_for_market_date',
                return_value={'status': 'unavailable'},
            ),
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
                'app.eod.pykrx_enrichment.pykrx_enrichment_service.collect_for_market_date',
                return_value={'status': 'partial'},
            ) as repair_candidate_rows,
            patch(
                'app.orders.next_open_execution.load_daily_bars_for_symbol_range',
                return_value=[{'trade_date': '2026-04-29', 'close_price': 50000}],
            ),
        ):
            result = service.refresh_plan_items(target_trade_date=self.target_trade_date, trigger='test')

        self.assertEqual(load_candidate_rows.call_count, 2)
        self.assertEqual(load_candidate_rows.call_args.kwargs['symbols'], ['005930'])
        self.assertEqual(load_candidate_rows.call_args.kwargs['categories'], required_categories)
        repair_candidate_rows.assert_called_once()
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
                    'company': {'market': 'KOSPI'},
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
            'usage_policy': {
                'deterministic_gate_eligible': True,
                'manual_review_only': True,
                'allow_buy_authorization': False,
            },
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

    def test_refresh_plan_items_applies_quote_slippage_to_signal_buy_economics(self) -> None:
        settings.signal_decider_exec_integration_mode = 'dispatch'
        pre_decider_context = self._pre_decider_context()
        pre_decider_context['quote_orderbook_snapshot'].update(
            {
                'bid_price': 49_875,
                'ask_price': 50_125,
                'spread_points': 250,
                'spread_bps': 50,
            }
        )
        self._seed_snapshot(
            symbol='005930',
            target_price=50100,
            expected_return_pct=3.0,
            stop_loss_price=49000,
            pre_decider_enrichment_context=pre_decider_context,
        )
        service = self._service(state='PREOPEN', server_time='2026-04-30T08:40:00+09:00')

        with patch(
            'app.orders.next_open_execution.load_daily_bars_for_symbol_range',
            return_value=[{'trade_date': '2026-04-29', 'close_price': 50000}],
        ):
            result = service.refresh_plan_items(target_trade_date=self.target_trade_date, trigger='test')

        item = result['items'][0]
        economics = item['validation']['performance_gate']['expected_return_economics']
        self.assertEqual(economics['expected_slippage_bps'], '50')
        self.assertEqual(
            economics['server_non_spread_round_trip_cost_basis'],
            'deferred_to_exec_final_full_cost_check',
        )
        self.assertIsNone(economics['estimated_total_transaction_cost_bps'])
        self.assertEqual(economics['status'], 'no_positive_edge')
        block_codes = {reason['code'] for reason in item['validation']['blocking_reasons']}
        self.assertIn('expected_return_edge_erased_by_slippage', block_codes)

    def test_refresh_plan_items_direct_mode_requires_execution_time_spread_in_addition_to_server_cost(self) -> None:
        settings.signal_decider_next_open_non_spread_round_trip_cost_bps = 50.0
        self._seed_snapshot(
            symbol='005930',
            target_price=51_500,
            expected_return_pct=3.0,
            stop_loss_price=48_900,
            pre_decider_enrichment_context=self._pre_decider_context(),
        )
        service = self._service(state='PREOPEN', server_time='2026-04-30T08:40:00+09:00')

        with patch(
            'app.orders.next_open_execution.load_daily_bars_for_symbol_range',
            return_value=[{'trade_date': '2026-04-29', 'close_price': 50_000}],
        ):
            result = service.refresh_plan_items(target_trade_date=self.target_trade_date, trigger='test')

        item = result['items'][0]
        economics = item['validation']['performance_gate']['expected_return_economics']
        self.assertEqual(economics['server_non_spread_round_trip_cost_bps'], '50')
        self.assertIsNone(economics['estimated_total_transaction_cost_bps'])
        self.assertEqual(economics['status'], 'unproven')
        self.assertEqual(item['eligibility_status'], ELIGIBILITY_BLOCKED)
        self.assertIn(
            'transaction_cost_evidence_missing',
            {reason['code'] for reason in item['validation']['blocking_reasons']},
        )

    def test_refresh_plan_items_dispatch_prescreens_production_closed_market_eod_without_live_cost(self) -> None:
        settings.signal_decider_exec_integration_mode = 'dispatch'
        for spread_bps in (None, 17.5):
            with self.subTest(spread_bps=spread_bps):
                self._seed_snapshot(
                    symbol='005930',
                    target_price=52_000,
                    expected_return_pct=5.0,
                    stop_loss_price=49_000,
                    pre_decider_enrichment_context=self._closed_market_pre_decider_context(
                        spread_bps=spread_bps
                    ),
                )
                service = self._service(state='PREOPEN', server_time='2026-04-30T08:40:00+09:00')

                with patch(
                    'app.orders.next_open_execution.load_daily_bars_for_symbol_range',
                    return_value=[{'trade_date': '2026-04-29', 'close_price': 50_000}],
                ):
                    result = service.refresh_plan_items(
                        target_trade_date=self.target_trade_date,
                        trigger='test',
                    )

                item = result['items'][0]
                economics = item['validation']['performance_gate']['expected_return_economics']
                self.assertEqual(item['eligibility_status'], ELIGIBILITY_ELIGIBLE)
                self.assertEqual(economics['status'], 'pending_live_cost_confirmation')
                self.assertIsNone(economics['expected_slippage_bps'])
                self.assertEqual(economics['expected_slippage_basis'], 'unavailable')
                self.assertEqual(item['validation']['blocking_reasons'], [])
                self.assertIn(
                    'transaction_cost_evidence_pending_live_delegated_to_exec_watch',
                    {warning['code'] for warning in item['validation']['warnings']},
                )

    def test_refresh_plan_items_direct_mode_blocks_production_closed_market_eod_without_live_cost(self) -> None:
        self._seed_snapshot(
            symbol='005930',
            target_price=52_000,
            expected_return_pct=5.0,
            stop_loss_price=49_000,
            pre_decider_enrichment_context=self._closed_market_pre_decider_context(),
        )
        service = self._service(state='PREOPEN', server_time='2026-04-30T08:40:00+09:00')

        with patch(
            'app.orders.next_open_execution.load_daily_bars_for_symbol_range',
            return_value=[{'trade_date': '2026-04-29', 'close_price': 50_000}],
        ):
            result = service.refresh_plan_items(target_trade_date=self.target_trade_date, trigger='test')

        item = result['items'][0]
        self.assertEqual(item['eligibility_status'], ELIGIBILITY_BLOCKED)
        self.assertIn(
            'transaction_cost_evidence_missing',
            {reason['code'] for reason in item['validation']['blocking_reasons']},
        )

    def test_refresh_plan_items_ignores_manual_review_performance_without_explicit_eligibility(self) -> None:
        snapshot_id = self._seed_snapshot(symbol='005930')
        with session_scope() as session:
            snapshot = session.get(SignalDeciderDecisionSnapshot, snapshot_id)
            snapshot.request_payload = {
                **(snapshot.request_payload or {}),
                'signal_decider_policy': {
                    'current_version': self.policy_version,
                    'performance_criteria': {
                        'schema_version': 'signal_decider_performance_criteria_v1',
                        'status': 'ready',
                        'sample_count': 12,
                        'overall': {
                            'positive_outcome_rate': 0.2,
                            'negative_outcome_rate': 0.8,
                        },
                        'performance_buckets': [
                            {
                                'bucket_key': 'decision.reward_risk_bucket',
                                'bucket_value': 'reward_risk_1_3_to_2_0',
                                'sample_count': 8,
                                'bad_buy_rate': 0.8,
                                'negative_rate': 0.8,
                                'avg_close_return_pct': -1.7,
                            }
                        ],
                        'rule_candidates': [],
                        'usage_policy': {
                            'manual_review_only': True,
                            'allow_buy_authorization': False,
                        },
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
        self.assertEqual(item['validation']['performance_gate']['status'], 'not_eligible')
        block_codes = {reason['code'] for reason in item['validation']['blocking_reasons']}
        self.assertNotIn('performance_reward_risk_gate_blocked', block_codes)
        self.assertNotIn('performance_rule_candidate_blocked', block_codes)
        self.assertIn('transaction_cost_evidence_missing', block_codes)

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

    def test_preopen_scheduler_revalidates_policy_status_before_reusing_plan(self) -> None:
        settings.signal_decider_exec_integration_mode = 'dispatch'
        self._seed_snapshot(symbol='005930')
        service = self._service(state='PREOPEN', server_time='2026-04-30T08:40:00+09:00')

        with patch(
            'app.orders.next_open_execution.load_daily_bars_for_symbol_range',
            return_value=[{'trade_date': '2026-04-29', 'close_price': 50000}],
        ), patch(
            'app.orders.next_open_execution.signal_decider_loop_service.get_policy_prompt_context',
            return_value={'status': 'active'},
        ):
            first = service.refresh_plan_items(
                target_trade_date=self.target_trade_date,
                trigger='test',
            )

        self.assertEqual(first['items'][0]['eligibility_status'], ELIGIBILITY_ELIGIBLE)

        with patch(
            'app.orders.next_open_execution.load_daily_bars_for_symbol_range',
            return_value=[{'trade_date': '2026-04-29', 'close_price': 50000}],
        ), patch(
            'app.orders.next_open_execution.signal_decider_loop_service.get_policy_prompt_context',
            return_value={'status': 'retired'},
        ) as get_policy_prompt_context:
            refreshed = service.refresh_plan_items(
                target_trade_date=self.target_trade_date,
                trigger='preopen_scheduler',
            )

        self.assertNotEqual(refreshed['summary'].get('reason'), 'existing_next_open_plan_reused')
        self.assertGreaterEqual(get_policy_prompt_context.call_count, 1)
        self.assertEqual(refreshed['items'][0]['eligibility_status'], ELIGIBILITY_BLOCKED)
        self.assertIn(
            'policy_not_active',
            {
                reason['code']
                for reason in refreshed['items'][0]['validation']['blocking_reasons']
            },
        )

    def test_preopen_scheduler_revalidates_corrected_market_data_before_reusing_plan(self) -> None:
        settings.signal_decider_exec_integration_mode = 'dispatch'
        self._seed_snapshot(
            symbol='005930',
            target_price=54000,
            expected_return_pct=8.0,
            stop_loss_price=47000,
        )
        service = self._service(state='PREOPEN', server_time='2026-04-30T08:40:00+09:00')

        with patch(
            'app.orders.next_open_execution.load_daily_bars_for_symbol_range',
            side_effect=[
                [{'trade_date': '2026-04-29', 'close_price': 50000}],
                [{'trade_date': '2026-04-29', 'close_price': 55000}],
            ],
        ) as load_bars:
            first = service.refresh_plan_items(
                target_trade_date=self.target_trade_date,
                trigger='test',
            )
            refreshed = service.refresh_plan_items(
                target_trade_date=self.target_trade_date,
                trigger='preopen_scheduler',
            )

        self.assertEqual(first['items'][0]['eligibility_status'], ELIGIBILITY_ELIGIBLE)
        self.assertEqual(load_bars.call_count, 2)
        self.assertNotEqual(refreshed['summary'].get('reason'), 'existing_next_open_plan_reused')
        self.assertEqual(refreshed['items'][0]['estimated_reference_price'], 55000)
        self.assertEqual(refreshed['items'][0]['eligibility_status'], ELIGIBILITY_BLOCKED)
        self.assertIn(
            'expected_return_net_edge_not_positive',
            {
                reason['code']
                for reason in refreshed['items'][0]['validation']['blocking_reasons']
            },
        )

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
        self.assertEqual(by_agent['short-term-decider']['eligibility_status'], ELIGIBILITY_MONITOR_CANDIDATE)
        self.assertEqual(by_agent['short-term-decider']['raw_action'], 'watch_for_trigger')
        self.assertEqual(by_agent['short-term-decider']['tactical_action'], 'watch_for_trigger')
        self.assertEqual(by_agent['short-term-decider']['generic_action'], 'monitor_candidate')
        self.assertEqual(by_agent['short-term-decider']['handoff_intent'], 'intraday_watch_buy')
        self.assertTrue(by_agent['short-term-decider']['handoff_active'])
        self.assertTrue(by_agent['short-term-decider']['exec_monitoring_active'])
        self.assertEqual(by_agent['short-term-decider']['approval_state'], 'pending_exec_agent')
        self.assertEqual(by_agent['short-term-decider']['trigger_price'], 50500)
        self.assertEqual(by_agent['short-term-decider']['chase_limit_price'], 51500)
        self.assertEqual(by_agent['short-term-decider']['target_price'], 54000)
        self.assertEqual(by_agent['short-term-decider']['stop_loss_price'], 49000)

    def test_watch_for_trigger_is_a_monitor_candidate_not_a_buy_eligible_planned_order(self) -> None:
        settings.signal_decider_exec_integration_mode = 'dispatch'
        self._seed_short_term_snapshot(symbol='005930', tactical_action='watch_for_trigger')
        service = self._service(state='PREOPEN', server_time='2026-04-30T08:40:00+09:00')

        with patch(
            'app.orders.next_open_execution.load_daily_bars_for_symbol_range',
            return_value=[{'trade_date': '2026-04-29', 'close_price': 50000}],
        ):
            result = service.refresh_plan_items(target_trade_date=self.target_trade_date, trigger='test')

        item = result['items'][0]
        self.assertEqual(item['tactical_action'], 'watch_for_trigger')
        self.assertEqual(item['generic_action'], 'monitor_candidate')
        self.assertEqual(item['eligibility_status'], 'monitor_candidate')
        self.assertEqual(item['execution_status'], 'awaiting_trigger')
        self.assertEqual(item['approval_state'], 'pending_exec_agent')
        self.assertEqual(result['summary']['eligible_count'], 0)
        self.assertEqual(result['summary']['planned_count'], 0)
        self.assertEqual(result['summary']['monitor_candidate_count'], 1)
        self.assertEqual(result['summary']['awaiting_trigger_count'], 1)

        with session_scope() as session:
            row = session.get(SignalDeciderExecutionPlanItem, item['plan_item_id'])
            session.expunge(row)
        self.assertTrue(SignalDeciderExecIntegrationService._row_is_dispatchable(row))

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
        self.assertEqual(short_item['eligibility_status'], ELIGIBILITY_MONITOR_CANDIDATE)
        self.assertEqual(short_item['signal_decider_alignment'], 'wait_conflict_block')
        self.assertEqual(short_item['alignment_gate_action'], 'report_only_block_candidate')
        self.assertEqual(short_item['alignment_signal_decider_decision_snapshot_id'], signal_snapshot_id)
        self.assertEqual(short_item['validation']['signal_decider_alignment'], 'wait_conflict_block')
        self.assertEqual(short_item['traceability']['signal_decider_alignment'], 'wait_conflict_block')
        self.assertEqual(short_item['final_enforcement_decision'], 'allowed_report_only')
        self.assertEqual(short_item['requested_quantity'], short_item['final_quantity'])
        warning_codes = {warning['code'] for warning in short_item['validation']['warnings']}
        block_codes = {reason['code'] for reason in short_item['validation']['blocking_reasons']}
        self.assertIn('signal_decider_wait_conflict_gate_report_only', warning_codes)
        self.assertNotIn('signal_decider_wait_conflict_gate', block_codes)
        self.assertEqual(result['summary']['short_term_wait_conflict_block_count'], 1)
        self.assertEqual(result['summary']['short_term_wait_conflict_override_count'], 0)

    def test_refresh_plan_items_allows_wait_conflict_override_for_strong_watch_setup(self) -> None:
        settings.signal_decider_wait_conflict_gate_mode = 'hard_block'
        signal_snapshot_id = self._seed_snapshot(symbol='005930', next_day_open_action='wait')
        short_snapshot_id = self._seed_short_term_snapshot(
            symbol='005930',
            objective_technical_snapshot={
                'trend_state': 'uptrend',
                'breakout_20d': True,
                'breakdown_20d': False,
            },
        )
        with session_scope() as session:
            snapshot = session.get(SignalDeciderDecisionSnapshot, short_snapshot_id)
            request_payload = dict(snapshot.request_payload or {})
            symbol_day_context = dict(request_payload.get('symbol_day_context') or {})
            symbol_day_context['market_relative_context'] = {
                'relative_strength': {
                    'return_percentile': 0.82,
                    'volume_percentile': 0.68,
                },
            }
            request_payload['symbol_day_context'] = symbol_day_context
            request_payload['decision_input_quality_context'] = {
                'final_judgment': {
                    'buy_quality_score': 76,
                    'decision_band': 'normal_buy',
                },
            }
            request_payload['liquidity_context'] = {
                'turnover': {
                    'trading_value_source': 'raw',
                    'raw_trading_value_krw': 9_000_000_000,
                },
            }
            snapshot.request_payload = request_payload
        service = self._service(state='PREOPEN', server_time='2026-04-30T08:40:00+09:00')

        with patch(
            'app.orders.next_open_execution.load_daily_bars_for_symbol_range',
            return_value=[{'trade_date': '2026-04-29', 'close_price': 50000}],
        ):
            result = service.refresh_plan_items(target_trade_date=self.target_trade_date, trigger='test')

        short_item = next(item for item in result['items'] if item['decision_agent_name'] == 'short-term-decider')
        self.assertEqual(short_item['eligibility_status'], ELIGIBILITY_MONITOR_CANDIDATE)
        self.assertEqual(short_item['signal_decider_alignment'], 'wait_conflict_override')
        self.assertEqual(short_item['alignment_gate_action'], 'allow_by_override')
        self.assertEqual(short_item['alignment_signal_decider_decision_snapshot_id'], signal_snapshot_id)
        self.assertTrue(short_item['alignment_gate']['override']['allowed'])
        self.assertEqual(short_item['final_enforcement_decision'], 'allowed_by_override')
        self.assertEqual(short_item['requested_quantity'], short_item['final_quantity'])
        warning_codes = {warning['code'] for warning in short_item['validation']['warnings']}
        self.assertIn('signal_decider_wait_conflict_override', warning_codes)
        self.assertEqual(result['summary']['short_term_wait_conflict_block_count'], 0)
        self.assertEqual(result['summary']['short_term_wait_conflict_override_count'], 1)

    def test_refresh_plan_items_hard_blocks_wait_conflict(self) -> None:
        settings.signal_decider_wait_conflict_gate_mode = 'hard_block'
        self._seed_snapshot(symbol='005930', next_day_open_action='wait')
        self._seed_short_term_snapshot(symbol='005930')
        service = self._service(state='PREOPEN', server_time='2026-04-30T08:40:00+09:00')

        with patch(
            'app.orders.next_open_execution.load_daily_bars_for_symbol_range',
            return_value=[{'trade_date': '2026-04-29', 'close_price': 50000}],
        ):
            result = service.refresh_plan_items(target_trade_date=self.target_trade_date, trigger='test')

        item = next(item for item in result['items'] if item['decision_agent_name'] == 'short-term-decider')
        self.assertEqual(item['eligibility_status'], ELIGIBILITY_BLOCKED)
        self.assertEqual(item['final_enforcement_decision'], 'blocked')
        self.assertGreater(item['requested_quantity'], 0)
        self.assertEqual(item['final_quantity'], 0)
        self.assertEqual(Decimal(item['planned_quantity']), Decimal('0'))
        self.assertIn(
            'signal_decider_wait_conflict_gate',
            {reason['code'] for reason in item['validation']['blocking_reasons']},
        )

    def test_refresh_plan_items_downsizes_wait_conflict_to_configured_probe(self) -> None:
        settings.signal_decider_wait_conflict_gate_mode = 'downsize_probe'
        settings.signal_decider_wait_conflict_probe_quantity = 2
        self._seed_snapshot(symbol='005930', next_day_open_action='wait')
        self._seed_short_term_snapshot(symbol='005930')
        service = self._service(state='PREOPEN', server_time='2026-04-30T08:40:00+09:00')

        with patch(
            'app.orders.next_open_execution.load_daily_bars_for_symbol_range',
            return_value=[{'trade_date': '2026-04-29', 'close_price': 50000}],
        ):
            result = service.refresh_plan_items(target_trade_date=self.target_trade_date, trigger='test')

        item = next(item for item in result['items'] if item['decision_agent_name'] == 'short-term-decider')
        self.assertEqual(item['eligibility_status'], ELIGIBILITY_MONITOR_CANDIDATE)
        self.assertEqual(item['alignment_gate_action'], 'downsize_probe')
        self.assertEqual(item['final_enforcement_decision'], 'downsized')
        self.assertGreater(item['requested_quantity'], 2)
        self.assertEqual(item['final_quantity'], 2)
        self.assertEqual(Decimal(item['planned_quantity']), Decimal('2'))

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
        self.assertEqual(short_item['eligibility_status'], ELIGIBILITY_MONITOR_CANDIDATE)
        self.assertEqual(short_item['signal_decider_alignment'], 'aligned')
        self.assertEqual(short_item['alignment_gate_action'], 'allow')
        self.assertEqual(short_item['alignment_signal_decider_decision_snapshot_id'], signal_snapshot_id)
        self.assertEqual(result['summary']['short_term_wait_conflict_block_count'], 0)
        self.assertEqual(result['summary']['short_term_wait_conflict_override_count'], 0)

    def test_refresh_plan_items_hard_blocks_low_confidence_short_term_buy_authority(self) -> None:
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
        self.assertEqual(result['summary']['eligible_count'], 0)
        self.assertEqual(result['summary']['monitor_candidate_count'], 1)
        self.assertEqual(items_by_symbol['005930']['eligibility_status'], ELIGIBILITY_MONITOR_CANDIDATE)
        self.assertEqual(items_by_symbol['000660']['eligibility_status'], ELIGIBILITY_BLOCKED)
        self.assertIn(
            'confidence_below_threshold',
            {reason['code'] for reason in items_by_symbol['000660']['validation']['blocking_reasons']},
        )
        self.assertEqual(
            Decimal(str(items_by_symbol['000660']['validation']['short_term']['confidence_score_10'])),
            Decimal('5.9'),
        )
        self.assertEqual(items_by_symbol['000660']['validation']['short_term']['tactical_action'], 'watch_for_trigger')
        self.assertEqual(items_by_symbol['000660']['validation']['short_term']['handoff_intent'], 'intraday_watch_buy')
        self.assertNotEqual(items_by_symbol['000660']['validation']['handoff']['approval_state'], 'pending_exec_agent')
        self.assertEqual(items_by_symbol['000660']['validation']['short_term']['trigger_price'], 252000)
        self.assertEqual(items_by_symbol['000660']['validation']['short_term']['chase_limit_price'], 255000)
        self.assertEqual(
            Decimal(str(items_by_symbol['000660']['traceability']['confidence_score_10'])),
            Decimal('5.9'),
        )
        self.assertEqual(
            items_by_symbol['000660']['traceability']['core_execution_classification'],
            'blocked',
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

    def test_refresh_plan_items_hard_blocks_non_recheckable_short_term_data_warnings(self) -> None:
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
        self.assertEqual(item['eligibility_status'], ELIGIBILITY_BLOCKED)
        self.assertEqual(item['execution_status'], 'planned')
        self.assertEqual(item['traceability']['core_execution_classification'], 'blocked')
        block_codes = {reason['code'] for reason in item['validation']['blocking_reasons']}
        self.assertIn('data_quality_warnings_present', block_codes)

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
        self.assertEqual(item['eligibility_status'], ELIGIBILITY_MONITOR_CANDIDATE)
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
        self.assertEqual(result['summary']['eligible_count'], 0)
        self.assertEqual(result['summary']['monitor_candidate_count'], 1)
        self.assertEqual(result['summary']['blocked_count'], 0)
        item = result['items'][0]
        self.assertEqual(item['decision_agent_name'], 'short-term-decider')
        self.assertEqual(item['account_reference'], 'KIS:00000000-00')
        self.assertEqual(item['account_label'], 'UNSPECIFIED_ROUTE')
        self.assertEqual(item['traceability']['allocation_mode'], 'unspecified_route')
        self.assertEqual(item['traceability']['allocation_budget_krw'], '0')
        self.assertIsNone(item['planned_quantity'])
        self.assertEqual(item['eligibility_status'], ELIGIBILITY_MONITOR_CANDIDATE)

    async def test_paper_unspecified_route_wait_conflict_dispatches_configured_probe_quantity(self) -> None:
        settings.signal_decider_wait_conflict_gate_mode = 'downsize_probe'
        settings.signal_decider_wait_conflict_probe_quantity = 2
        settings.signal_decider_exec_integration_mode = 'dispatch'
        settings.signal_decider_exec_runtime_mode = 'paper'
        self._seed_snapshot(symbol='005930', next_day_open_action='wait')
        self._seed_short_term_snapshot(symbol='005930')
        settings.signal_decider_next_open_account_targets = ''
        service = self._service(state='PREOPEN', server_time='2026-04-30T08:40:00+09:00')

        with patch(
            'app.orders.next_open_execution.load_daily_bars_for_symbol_range',
            return_value=[{'trade_date': '2026-04-29', 'close_price': 50000}],
        ):
            result = service.refresh_plan_items(target_trade_date=self.target_trade_date, trigger='test')

        self.assertEqual(result['summary']['items_total'], 1)
        self.assertEqual(result['summary']['eligible_count'], 0)
        self.assertEqual(result['summary']['monitor_candidate_count'], 1)
        self.assertEqual(result['summary']['blocked_count'], 0)
        item = result['items'][0]
        self.assertEqual(item['decision_agent_name'], 'short-term-decider')
        self.assertEqual(item['account_reference'], 'KIS:00000000-00')
        self.assertEqual(item['account_label'], 'UNSPECIFIED_ROUTE')
        self.assertEqual(item['traceability']['allocation_mode'], 'unspecified_route')
        self.assertEqual(item['traceability']['allocation_budget_krw'], '0')
        self.assertEqual(item['eligibility_status'], ELIGIBILITY_MONITOR_CANDIDATE)
        self.assertEqual(item['alignment_gate_action'], 'downsize_probe')
        self.assertEqual(item['final_enforcement_decision'], 'downsized')
        self.assertEqual(item['requested_quantity'], 2)
        self.assertEqual(item['final_quantity'], 2)
        self.assertEqual(Decimal(item['planned_quantity']), Decimal('2'))

        dispatched_payloads = []

        async def capture_dispatch(payload):
            dispatched_payloads.append(payload)
            return {
                'plan_id': payload['plan_id'],
                'execution_plan_id': payload['plan_id'],
                'contract_ack': {
                    'schema_version': EXECUTION_HANDOFF_ACK_SCHEMA_VERSION,
                    'contract_version': EXECUTION_HANDOFF_CONTRACT_VERSION,
                    'capability': EXECUTION_HANDOFF_CAPABILITY,
                    'plan_id': payload['plan_id'],
                    'status': 'accepted',
                    'dispatch_material_fingerprint': payload['metadata'][
                        'dispatch_material_fingerprint'
                    ],
                },
                'accepted_item_count': len(payload['items']),
                'rejected_item_count': 0,
                'item_acks': [
                    {
                        'core_item_key': dispatched_item['core_item_key'],
                        'execution_plan_id': payload['plan_id'],
                        'execution_plan_item_id': f'exec-item-{index}',
                        'status': 'accepted',
                    }
                    for index, dispatched_item in enumerate(payload['items'], start=1)
                ],
            }

        exec_client = AsyncMock()
        exec_client.post_execution_plan.side_effect = capture_dispatch
        dispatch_result = await SignalDeciderExecIntegrationService(client=exec_client).dispatch_next_open_plan(
            target_trade_date=self.target_trade_date,
            account_reference='KIS:00000000-00',
            refresh_plan=False,
            dry_run=False,
            force=True,
            trigger='test',
        )

        self.assertTrue(
            dispatch_result['summary']['triggered'],
            dispatch_result,
        )
        self.assertEqual(len(dispatched_payloads), 1)
        self.assertEqual(dispatched_payloads[0]['metadata']['account_label'], 'UNSPECIFIED_ROUTE')
        entry = next(
            dispatched_item
            for dispatched_item in dispatched_payloads[0]['items']
            if dispatched_item['intent'] == EXEC_INTENT_INTRADAY_WATCH_BUY
        )
        self.assertEqual(entry['quantity_policy']['type'], 'exec_agent_sized')
        self.assertEqual(entry['quantity_policy']['max_quantity'], '2')
        self.assertEqual(entry['quantity_policy']['enforcement_source'], 'core_wait_conflict_alignment_gate')

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

    def test_preopen_scheduler_does_not_reuse_unspecified_route_after_account_target_parse_error(
        self,
    ) -> None:
        self._seed_short_term_snapshot(symbol='005930')
        settings.signal_decider_next_open_account_targets = ''
        service = self._service(state='PREOPEN', server_time='2026-04-30T08:40:00+09:00')

        with patch(
            'app.orders.next_open_execution.load_daily_bars_for_symbol_range',
            return_value=[{'trade_date': '2026-04-29', 'close_price': 50000}],
        ):
            initial = service.refresh_plan_items(
                target_trade_date=self.target_trade_date,
                trigger='test',
            )
            self.assertEqual(initial['summary']['blocked_count'], 0)

            settings.signal_decider_next_open_account_targets = (
                'Main=not-a-kis-account@1500000'
            )
            refreshed = service.refresh_plan_items(
                target_trade_date=self.target_trade_date,
                trigger='preopen_scheduler',
            )

        self.assertNotEqual(
            refreshed['summary'].get('reason'),
            'existing_next_open_plan_reused',
        )
        self.assertEqual(refreshed['summary']['blocked_count'], 1)
        self.assertIn(
            'account_targets_invalid',
            {
                reason['code']
                for reason in refreshed['items'][0]['validation']['blocking_reasons']
            },
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

    def test_latest_snapshot_selection_ranks_before_short_term_actionability_filter(self) -> None:
        actionable_id = self._seed_short_term_snapshot(
            symbol='035420',
            tactical_action='watch_for_trigger',
        )
        wait_id = self._seed_short_term_snapshot(
            symbol='035420',
            tactical_action='wait',
        )
        self.assertGreater(wait_id, actionable_id)

        snapshots = self._service()._load_latest_snapshots(
            source_market_date=self.market_date,
        )

        matching = [
            snapshot
            for snapshot in snapshots
            if snapshot.symbol == '035420'
            and snapshot.agent_name == 'short-term-decider'
        ]
        self.assertEqual([snapshot.id for snapshot in matching], [wait_id])
        self.assertEqual(matching[0].decision_action, 'wait')

    def test_latest_run_cohort_missing_symbol_revokes_prior_delegated_buy(self) -> None:
        run_a = f'cohort-a-{uuid4().hex[:10]}'
        run_b = f'cohort-b-{uuid4().hex[:10]}'
        symbols = ['005930', '000660']
        self._seed_feedback_run(
            run_id=run_a,
            symbols=symbols,
            roles=['signal-decider'],
            started_at=datetime(2026, 4, 29, 5, 0, tzinfo=timezone.utc),
        )
        original_x = self._seed_snapshot(symbol='005930', decision_run_id=run_a)
        self._seed_snapshot(symbol='000660', decision_run_id=run_a)
        service = self._service()
        with patch(
            'app.orders.next_open_execution.load_daily_bars_for_symbol_range',
            return_value=[{'trade_date': '2026-04-29', 'close_price': 50000}],
        ):
            service.refresh_plan_items(target_trade_date=self.target_trade_date, trigger='cohort_a')
        self._mark_row_as_handed_off(symbol='005930')

        self._seed_feedback_run(
            run_id=run_b,
            symbols=symbols,
            roles=['signal-decider'],
            started_at=datetime(2026, 4, 29, 6, 0, tzinfo=timezone.utc),
            status='completed_with_failures',
            completed_count=1,
            failed_count=1,
        )
        self._seed_snapshot(symbol='000660', decision_run_id=run_b)
        with patch(
            'app.orders.next_open_execution.load_daily_bars_for_symbol_range',
            return_value=[{'trade_date': '2026-04-29', 'close_price': 50000}],
        ):
            refreshed = service.refresh_plan_items(
                target_trade_date=self.target_trade_date,
                trigger='cohort_b_partial',
            )

        x_item = next(item for item in refreshed['items'] if item['symbol'] == '005930')
        self.assertEqual(x_item['decision_snapshot_id'], original_x)
        self.assertEqual(x_item['execution_status'], EXECUTION_STATUS_REVOCATION_PENDING)
        self.assertEqual(x_item['submission']['revocation']['reason_code'], 'candidate_removed')

    def test_latest_run_cohort_narrower_scope_does_not_remove_unrequested_symbol(self) -> None:
        run_a = f'cohort-full-{uuid4().hex[:10]}'
        run_b = f'cohort-narrow-{uuid4().hex[:10]}'
        self._seed_feedback_run(
            run_id=run_a,
            symbols=['005930', '000660'],
            roles=['signal-decider'],
            started_at=datetime(2026, 4, 29, 5, 0, tzinfo=timezone.utc),
        )
        x_id = self._seed_snapshot(symbol='005930', decision_run_id=run_a)
        self._seed_snapshot(symbol='000660', decision_run_id=run_a)
        self._seed_feedback_run(
            run_id=run_b,
            symbols=['000660'],
            roles=['signal-decider'],
            started_at=datetime(2026, 4, 29, 6, 0, tzinfo=timezone.utc),
        )
        y_id = self._seed_snapshot(symbol='000660', decision_run_id=run_b)

        selected = self._service()._load_latest_snapshots(source_market_date=self.market_date)
        by_symbol = {
            snapshot.symbol: snapshot.id
            for snapshot in selected
            if snapshot.symbol in {'005930', '000660'}
        }

        self.assertEqual(by_symbol['005930'], x_id)
        self.assertEqual(by_symbol['000660'], y_id)

    def test_latest_complete_run_cohort_replaces_every_requested_symbol(self) -> None:
        run_a = f'cohort-old-{uuid4().hex[:10]}'
        run_b = f'cohort-new-{uuid4().hex[:10]}'
        symbols = ['005930', '000660']
        self._seed_feedback_run(
            run_id=run_a,
            symbols=symbols,
            roles=['signal-decider'],
            started_at=datetime(2026, 4, 29, 5, 0, tzinfo=timezone.utc),
        )
        self._seed_snapshot(symbol='005930', decision_run_id=run_a)
        self._seed_snapshot(symbol='000660', decision_run_id=run_a)
        self._seed_feedback_run(
            run_id=run_b,
            symbols=symbols,
            roles=['signal-decider'],
            started_at=datetime(2026, 4, 29, 6, 0, tzinfo=timezone.utc),
        )
        new_x = self._seed_snapshot(symbol='005930', decision_run_id=run_b)
        new_y = self._seed_snapshot(symbol='000660', decision_run_id=run_b)

        selected = self._service()._load_latest_snapshots(source_market_date=self.market_date)
        by_symbol = {
            snapshot.symbol: snapshot.id
            for snapshot in selected
            if snapshot.symbol in {'005930', '000660'}
        }

        self.assertEqual(by_symbol, {'005930': new_x, '000660': new_y})

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

    def test_refresh_plan_items_keeps_non_recheckable_watch_quality_blocks_in_core(self) -> None:
        snapshot_id = self._seed_short_term_snapshot(
            symbol='000660',
            tactical_action='watch_for_trigger',
            trigger_price=250000,
            chase_limit_price=255000,
            target_price=260000,
            stop_loss_price=245000,
        )
        with session_scope() as session:
            snapshot = session.get(SignalDeciderDecisionSnapshot, snapshot_id)
            snapshot.request_payload = {
                **(snapshot.request_payload or {}),
                'decision_input_quality_context': {
                    'final_judgment': {
                        'buy_quality_score': 45,
                        'decision_band': 'watch',
                    },
                },
                'liquidity_context': {
                    'turnover': {
                        'trading_value_source': 'raw',
                        'raw_trading_value_krw': 250_000_000,
                    },
                },
            }
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
        block_codes = {reason['code'] for reason in item['validation']['blocking_reasons']}
        self.assertIn('short_term_buy_quality_score_block', block_codes)
        warning_codes = {warning['code'] for warning in item['validation']['warnings']}
        self.assertIn('short_term_liquidity_floor_block_delegated_to_exec_watch', warning_codes)

    def test_watch_delegation_set_contains_only_dynamic_liquidity_and_reward_risk(self) -> None:
        blocks = [
            {'code': code, 'message': code}
            for code in (
                'short_term_buy_quality_score_block',
                'short_term_high_atr_chase_block',
                'short_term_high_atr_weak_trend_block',
                'short_term_very_weak_relative_strength_block',
                'short_term_liquidity_floor_block',
                'short_term_reward_risk_ratio_block',
            )
        ]

        remaining, warnings = _delegate_short_term_watch_entry_quality_blocks_to_warnings(
            tactical_action='watch_for_trigger',
            handoff_intent='intraday_watch_buy',
            blocks=blocks,
            warnings=[],
        )

        self.assertEqual(
            {block['code'] for block in remaining},
            {
                'short_term_buy_quality_score_block',
                'short_term_high_atr_chase_block',
                'short_term_high_atr_weak_trend_block',
                'short_term_very_weak_relative_strength_block',
            },
        )
        self.assertEqual(
            {warning['code'] for warning in warnings},
            {
                'short_term_liquidity_floor_block_delegated_to_exec_watch',
                'short_term_reward_risk_ratio_block_delegated_to_exec_watch',
            },
        )

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
        self._seed_short_term_snapshot(
            symbol='005930',
            tactical_action='scalp_buy_candidate',
            objective_technical_snapshot={
                'trend_state': 'uptrend',
                'breakout_20d': True,
                'breakdown_20d': False,
            },
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
        self.assertEqual(item['raw_action'], 'scalp_buy_candidate')
        self.assertEqual(item['tactical_action'], 'scalp_buy_candidate')
        self.assertEqual(item['generic_action'], 'buy')
        self.assertEqual(item['handoff_intent'], 'tactical_buy')
        self.assertEqual(item['entry_execution_mode'], 'guarded_tactical_buy')
        self.assertTrue(item['handoff_active'])
        self.assertTrue(item['exec_monitoring_active'])

    def test_scalp_gate_treats_llm_breakout_text_as_audit_only(self) -> None:
        snapshot_id = self._seed_short_term_snapshot(
            symbol='005930',
            tactical_action='scalp_buy_candidate',
            objective_technical_snapshot={
                'trend_state': 'sideways',
                'breakout_20d': False,
                'breakdown_20d': False,
            },
        )
        with session_scope() as session:
            snapshot = session.get(SignalDeciderDecisionSnapshot, snapshot_id)
            snapshot.thesis_summary = 'Breakout failed.'
            snapshot.supporting_evidence_payload = ['No breakout was observed.']
            snapshot.decision_risks_payload = ['Breakout failure remains possible.']
            snapshot.invalidation_conditions_payload = ['No breakout confirmation.']
            snapshot.response_payload = {
                **(snapshot.response_payload or {}),
                'thesis_summary': 'Breakout failed.',
                'supporting_evidence': ['No breakout was observed.'],
                'tactical_risks': ['Breakout failure remains possible.'],
                'invalidation_conditions': ['No breakout confirmation.'],
            }
        service = self._service(state='PREOPEN', server_time='2026-04-30T08:40:00+09:00')

        with patch(
            'app.orders.next_open_execution.load_daily_bars_for_symbol_range',
            return_value=[{'trade_date': '2026-04-29', 'close_price': 50000}],
        ):
            result = service.refresh_plan_items(target_trade_date=self.target_trade_date, trigger='test')

        item = result['items'][0]
        self.assertEqual(item['eligibility_status'], ELIGIBILITY_BLOCKED)
        quality = item['validation']['short_term_entry_quality']
        self.assertFalse(quality['continuation_setup_detected'])
        self.assertEqual(quality['continuation_evidence_source'], 'objective_request_payload')
        self.assertIn(
            'short_term_scalp_requires_continuation_setup',
            {reason['code'] for reason in item['validation']['blocking_reasons']},
        )

    def test_short_term_gate_ignores_response_and_feature_snapshot_quality_spoofing(self) -> None:
        snapshot_id = self._seed_short_term_snapshot(
            symbol='005930',
            input_feature_snapshot_payload={
                'buy_quality_score': 99,
                'return_percentile': 0.99,
                'volume_percentile': 0.99,
                'raw_trading_value_krw': 99_000_000_000,
            },
        )
        with session_scope() as session:
            snapshot = session.get(SignalDeciderDecisionSnapshot, snapshot_id)
            request_payload = dict(snapshot.request_payload or {})
            market_regime = dict(request_payload.get('market_regime_context') or {})
            market_regime['relative_strength_context'] = {
                'return_percentile': 0.20,
                'volume_percentile': 0.20,
            }
            request_payload['market_regime_context'] = market_regime
            snapshot.request_payload = request_payload
            snapshot.response_payload = {
                **(snapshot.response_payload or {}),
                'buy_quality_score': 99,
                'return_percentile': 0.99,
                'volume_percentile': 0.99,
                'raw_trading_value_krw': 99_000_000_000,
            }
        service = self._service(state='PREOPEN', server_time='2026-04-30T08:40:00+09:00')

        with patch(
            'app.orders.next_open_execution.load_daily_bars_for_symbol_range',
            return_value=[{'trade_date': '2026-04-29', 'close_price': 50000}],
        ):
            result = service.refresh_plan_items(target_trade_date=self.target_trade_date, trigger='test')

        item = result['items'][0]
        quality = item['validation']['short_term_entry_quality']
        self.assertEqual(quality['return_percentile'], '0.2')
        self.assertEqual(quality['volume_percentile'], '0.2')
        self.assertIn(
            'short_term_very_weak_relative_strength_block',
            {reason['code'] for reason in item['validation']['blocking_reasons']},
        )

    def test_short_term_gate_blocks_invalid_objective_percentiles(self) -> None:
        snapshot_id = self._seed_short_term_snapshot(symbol='005930')
        with session_scope() as session:
            snapshot = session.get(SignalDeciderDecisionSnapshot, snapshot_id)
            request_payload = dict(snapshot.request_payload or {})
            symbol_day_context = dict(request_payload.get('symbol_day_context') or {})
            symbol_day_context['market_relative_context'] = {
                'relative_strength': {
                    'return_percentile': 82,
                    'volume_percentile': 'NaN',
                },
            }
            request_payload['symbol_day_context'] = symbol_day_context
            snapshot.request_payload = request_payload
        service = self._service(state='PREOPEN', server_time='2026-04-30T08:40:00+09:00')

        with patch(
            'app.orders.next_open_execution.load_daily_bars_for_symbol_range',
            return_value=[{'trade_date': '2026-04-29', 'close_price': 50000}],
        ):
            result = service.refresh_plan_items(target_trade_date=self.target_trade_date, trigger='test')

        item = result['items'][0]
        quality = item['validation']['short_term_entry_quality']
        self.assertEqual(quality['return_percentile_status'], 'invalid')
        self.assertEqual(quality['volume_percentile_status'], 'invalid')
        self.assertIsNone(quality['return_percentile'])
        self.assertIsNone(quality['volume_percentile'])
        self.assertEqual(item['eligibility_status'], ELIGIBILITY_BLOCKED)
        self.assertTrue(
            {
                'short_term_return_percentile_invalid_block',
                'short_term_volume_percentile_invalid_block',
            }.issubset({reason['code'] for reason in item['validation']['blocking_reasons']})
        )

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
        self.assertEqual(second['items'][0]['eligibility_status'], ELIGIBILITY_MONITOR_CANDIDATE)
        self.assertEqual(second['items'][0]['validation']['short_term']['tactical_action'], 'watch_for_trigger')

    async def test_run_market_open_execution_blocks_source_day_quote_without_execution_time_recheck(self) -> None:
        self._seed_snapshot(symbol='005930')
        fake_order_service = FakeOrderService()
        service = self._service(order_service=fake_order_service)

        with patch(
            'app.orders.next_open_execution.load_daily_bars_for_symbol_range',
            return_value=[{'trade_date': '2026-04-29', 'close_price': 50000}],
        ):
            refresh_result = service.refresh_plan_items(target_trade_date=self.target_trade_date, trigger='test')

        with session_scope() as session:
            row = (
                session.query(SignalDeciderExecutionPlanItem)
                .filter(SignalDeciderExecutionPlanItem.account_reference == self.account_reference)
                .one()
            )
            row.eligibility_status = ELIGIBILITY_ELIGIBLE
            row.planned_quantity = 5

        settings.signal_decider_exec_integration_mode = 'dispatch'
        with patch.object(service, 'refresh_plan_items', return_value={'enrichment_preflight': None}):
            result = await service.run_market_open_execution_if_due(
                target_trade_date=self.target_trade_date,
                trigger='test',
            )

        item = refresh_result['items'][0]
        self.assertEqual(item['eligibility_status'], ELIGIBILITY_BLOCKED)
        self.assertIn(
            'transaction_cost_evidence_missing',
            {reason['code'] for reason in item['validation']['blocking_reasons']},
        )
        self.assertEqual(result['summary']['selected_count'], 0)
        self.assertEqual(result['summary']['submitted_count'], 0)
        self.assertEqual(result['summary']['reason'], 'exec_dispatch_owns_submission')
        self.assertEqual(len(fake_order_service.calls), 0)

    async def test_run_market_open_execution_dry_run_does_not_bypass_execution_time_cost_gate(self) -> None:
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

        self.assertEqual(result['summary']['selected_count'], 0)
        self.assertEqual(result['summary']['dry_run_count'], 0)
        self.assertEqual(len(fake_order_service.calls), 0)
        self.assertEqual(result['items'], [])

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

    async def test_refresh_plan_items_revokes_old_buy_before_rebinding_new_buy(self) -> None:
        original_snapshot_id = self._seed_snapshot(symbol='005930')
        service = self._service()

        with patch(
            'app.orders.next_open_execution.load_daily_bars_for_symbol_range',
            return_value=[{'trade_date': '2026-04-29', 'close_price': 50000}],
        ):
            service.refresh_plan_items(target_trade_date=self.target_trade_date, trigger='test')

        submitted_at = datetime(2026, 4, 30, 0, 1, tzinfo=timezone.utc)
        with session_scope() as session:
            row = (
                session.query(SignalDeciderExecutionPlanItem)
                .filter(
                    SignalDeciderExecutionPlanItem.account_reference == self.account_reference,
                    SignalDeciderExecutionPlanItem.symbol == '005930',
                )
                .one()
            )
            row.execution_status = EXECUTION_STATUS_SUBMITTED
            row.submitted_at = submitted_at
            row.submission_payload = {
                'mode': 'exec_dispatch',
                'exec_dispatch': {
                    'plan_id': 'old-plan',
                    'plan_fingerprint': 'old-fingerprint',
                    'dispatch_status': 'accepted',
                    'last_success_at': submitted_at.isoformat(),
                    'item_links': [
                        {
                            'core_item_key': f'core-plan-{row.id}:{intent}',
                            'intent': intent,
                            'exec_plan_id': 'exec-old-plan',
                            'exec_plan_item_id': f'exec-{intent}',
                            'remote_status': 'accepted',
                        }
                        for intent in ('next_open_buy', 'target_sell', 'stop_sell')
                    ],
                },
            }
            row_id = row.id

        submitted_item = service.list_plan_items(target_trade_date=self.target_trade_date)['items'][0]
        replacement_snapshot_id = self._seed_snapshot(symbol='005930')
        self.assertNotEqual(replacement_snapshot_id, original_snapshot_id)

        with patch(
            'app.orders.next_open_execution.load_daily_bars_for_symbol_range',
            return_value=[{'trade_date': '2026-04-29', 'close_price': 51000}],
        ):
            refreshed = service.refresh_plan_items(target_trade_date=self.target_trade_date, trigger='test_refresh')

        refreshed_item = next(
            item
            for item in refreshed['items']
            if item['symbol'] == '005930'
            and item['account_reference'] == self.account_reference
        )
        self.assertEqual(refreshed_item['execution_status'], EXECUTION_STATUS_REVOCATION_PENDING)
        self.assertEqual(refreshed_item['decision_snapshot_id'], submitted_item['decision_snapshot_id'])
        self.assertEqual(refreshed_item['traceability'], submitted_item['traceability'])
        self.assertEqual(refreshed_item['estimated_reference_price'], submitted_item['estimated_reference_price'])
        self.assertEqual(refreshed_item['planned_quantity'], submitted_item['planned_quantity'])
        self.assertEqual(refreshed_item['submitted_at'], submitted_item['submitted_at'])
        revocation = refreshed_item['submission']['revocation']
        self.assertEqual(revocation['plan_id'], 'old-plan')
        self.assertEqual(revocation['replacement_decision_snapshot_id'], str(replacement_snapshot_id))
        self.assertTrue(revocation['replacement_actionable'])
        self.assertEqual(
            refreshed_item['submission']['revision_history'][0]['validation_payload'],
            submitted_item['validation'],
        )

        request_payload = {
            'plan_id': 'old-plan',
            'replacement_decision_snapshot_id': str(replacement_snapshot_id),
            'items': [
                {
                    'core_item_key': link['core_item_key'],
                    'execution_plan_item_id': link['exec_plan_item_id'],
                }
                for link in revocation['item_links']
            ],
        }
        response_payload = {
            'contract_ack': {'status': 'revoked'},
            'item_acks': [
                {
                    'core_item_key': item['core_item_key'],
                    'execution_plan_item_id': item['execution_plan_item_id'],
                    'status': 'revoked',
                }
                for item in request_payload['items']
            ],
        }
        service.apply_exec_revocation_ack(
            row_id=row_id,
            request_payload=request_payload,
            response_payload=response_payload,
            ack_status='revoked',
        )

        acknowledged = service.list_plan_items(target_trade_date=self.target_trade_date)['items'][0]
        self.assertEqual(acknowledged['execution_status'], EXECUTION_STATUS_PLANNED)
        self.assertIsNone(acknowledged['submitted_at'])
        self.assertNotIn('exec_dispatch', acknowledged['submission'])
        self.assertEqual(len(acknowledged['submission']['revision_history']), 1)
        self.assertEqual(len(acknowledged['submission']['revocation_history']), 1)

        with patch(
            'app.orders.next_open_execution.load_daily_bars_for_symbol_range',
            return_value=[{'trade_date': '2026-04-29', 'close_price': 51000}],
        ):
            rebound = service.refresh_plan_items(
                target_trade_date=self.target_trade_date,
                trigger='revocation_ack',
            )['items'][0]
        self.assertEqual(rebound['decision_snapshot_id'], replacement_snapshot_id)
        self.assertNotIn('exec_dispatch', rebound['submission'])
        self.assertEqual(len(rebound['submission']['revision_history']), 1)

    def test_stale_buy_refresh_cannot_overwrite_newer_wait_refresh(self) -> None:
        self._seed_snapshot(symbol='005930', next_day_open_action='buy')
        stale_service = self._service(
            state='PREOPEN',
            server_time='2026-04-30T08:45:00+09:00',
        )
        latest_service = self._service(
            state='PREOPEN',
            server_time='2026-04-30T08:45:00+09:00',
        )

        with patch(
            'app.orders.next_open_execution.load_daily_bars_for_symbol_range',
            return_value=[{'trade_date': '2026-04-29', 'close_price': 50000}],
        ):
            stale_service.refresh_plan_items(
                target_trade_date=self.target_trade_date,
                trigger='seed_buy_plan',
            )
            stale_snapshot_cohort = stale_service._load_latest_snapshots(
                source_market_date=self.market_date
            )
            load_latest = stale_service._load_latest_snapshots
            load_count = 0

            def expose_newer_wait_after_stale_read(*, source_market_date: date):
                nonlocal load_count
                load_count += 1
                if load_count == 1:
                    self._seed_snapshot(
                        symbol='005930',
                        next_day_open_action='wait',
                    )
                    latest_service.refresh_plan_items(
                        target_trade_date=self.target_trade_date,
                        trigger='newer_wait_refresh',
                    )
                    return stale_snapshot_cohort
                return load_latest(source_market_date=source_market_date)

            with patch.object(
                stale_service,
                '_load_latest_snapshots',
                side_effect=expose_newer_wait_after_stale_read,
            ):
                stale_result = stale_service.refresh_plan_items(
                    target_trade_date=self.target_trade_date,
                    trigger='stale_buy_refresh',
                )

        item = next(
            row
            for row in stale_result['items']
            if row['account_reference'] == self.account_reference
            and row['symbol'] == '005930'
        )
        self.assertGreaterEqual(load_count, 2)
        self.assertEqual(item['eligibility_status'], ELIGIBILITY_BLOCKED)
        self.assertTrue(
            {
                'latest_snapshot_not_actionable',
                'candidate_removed',
            }
            & {reason['code'] for reason in item['validation']['blocking_reasons']},
        )

    def test_stale_policy_refresh_cannot_overwrite_newer_account_budget(self) -> None:
        self._seed_snapshot(symbol='005930', next_day_open_action='buy')
        stale_service = self._service(
            state='PREOPEN',
            server_time='2026-04-30T08:45:00+09:00',
        )
        latest_service = self._service(
            state='PREOPEN',
            server_time='2026-04-30T08:45:00+09:00',
        )

        with patch(
            'app.orders.next_open_execution.load_daily_bars_for_symbol_range',
            return_value=[{'trade_date': '2026-04-29', 'close_price': 50000}],
        ):
            stale_service.refresh_plan_items(
                target_trade_date=self.target_trade_date,
                trigger='seed_original_budget',
            )
            old_targets = stale_service._parse_account_targets()
            parse_account_targets = stale_service._parse_account_targets
            parse_count = 0

            def expose_newer_budget_after_stale_read():
                nonlocal parse_count
                parse_count += 1
                if parse_count == 2:
                    settings.signal_decider_next_open_account_targets = (
                        f'Main={self.account_reference}@900000'
                    )
                    latest_service.refresh_plan_items(
                        target_trade_date=self.target_trade_date,
                        trigger='newer_budget_refresh',
                    )
                    return old_targets
                return parse_account_targets()

            with patch.object(
                stale_service,
                '_parse_account_targets',
                side_effect=expose_newer_budget_after_stale_read,
            ):
                stale_result = stale_service.refresh_plan_items(
                    target_trade_date=self.target_trade_date,
                    trigger='stale_budget_refresh',
                )

        item = next(
            row
            for row in stale_result['items']
            if row['account_reference'] == self.account_reference
            and row['symbol'] == '005930'
        )
        self.assertGreaterEqual(parse_count, 3)
        self.assertEqual(Decimal(item['allocation_budget_krw']), Decimal('900000'))
        self.assertEqual(
            item['validation']['preopen_policy_fingerprint'],
            _next_open_policy_fingerprint(
                [
                    NextOpenAccountTarget(
                        label='Main',
                        account_reference=self.account_reference,
                        budget_krw=Decimal('900000'),
                    )
                ]
            ),
        )

    def test_open_refresh_revokes_preopen_authority_on_fresh_severe_live_breadth(self) -> None:
        snapshot_id = self._seed_snapshot(symbol='005930')
        preopen_service = self._service(
            state='PREOPEN',
            server_time='2026-04-30T08:40:00+09:00',
        )
        with patch(
            'app.orders.next_open_execution.load_daily_bars_for_symbol_range',
            return_value=[{'trade_date': '2026-04-29', 'close_price': 50000}],
        ):
            preopen_service.refresh_plan_items(
                target_trade_date=self.target_trade_date,
                trigger='preopen_scheduler',
            )
        self._mark_row_as_handed_off()
        with session_scope() as session:
            immutable_request_before = json.loads(
                json.dumps(session.get(SignalDeciderDecisionSnapshot, snapshot_id).request_payload)
            )

        open_service = self._service(
            state='OPEN',
            server_time='2026-04-30T09:10:00+09:00',
        )
        live_snapshot = self._live_market_snapshot_context(
            observed_at=datetime(
                2026,
                4,
                30,
                9,
                9,
                30,
                tzinfo=timezone(timedelta(hours=9)),
            )
        )
        with (
            patch(
                'app.orders.next_open_execution.load_current_market_snapshot_context',
                return_value=live_snapshot,
            ),
            patch(
                'app.orders.next_open_execution.load_daily_bars_for_symbol_range',
                return_value=[{'trade_date': '2026-04-29', 'close_price': 50000}],
            ),
        ):
            refreshed = open_service.refresh_plan_items(
                target_trade_date=self.target_trade_date,
                trigger='market_open_scheduler',
            )

        item = refreshed['items'][0]
        self.assertEqual(item['execution_status'], EXECUTION_STATUS_REVOCATION_PENDING)
        self.assertEqual(
            item['submission']['revocation']['reason_code'],
            'current_live_market_risk_off',
        )
        self.assertEqual(item['submission']['revocation']['replacement_action'], 'buy')
        self.assertTrue(item['submission']['revocation']['replacement_actionable'])
        self.assertEqual(
            item['validation']['current_live_market_authority']['posture'],
            'risk_off',
        )
        with session_scope() as session:
            self.assertEqual(
                session.get(SignalDeciderDecisionSnapshot, snapshot_id).request_payload,
                immutable_request_before,
            )

        self._ack_pending_revocation(service=open_service, item=item)
        acknowledged = open_service.list_plan_items(
            target_trade_date=self.target_trade_date,
        )['items'][0]
        self.assertEqual(acknowledged['execution_status'], EXECUTION_STATUS_PLANNED)
        self.assertNotIn('exec_dispatch', acknowledged['submission'])

        with (
            patch(
                'app.orders.next_open_execution.load_current_market_snapshot_context',
                return_value=live_snapshot,
            ),
            patch(
                'app.orders.next_open_execution.load_daily_bars_for_symbol_range',
                return_value=[{'trade_date': '2026-04-29', 'close_price': 50000}],
            ),
        ):
            still_risk_off = open_service.refresh_plan_items(
                target_trade_date=self.target_trade_date,
                trigger='revocation_ack_revalidation',
            )['items'][0]
        self.assertEqual(still_risk_off['eligibility_status'], ELIGIBILITY_BLOCKED)
        self.assertNotIn('exec_dispatch', still_risk_off['submission'])

        healthy_snapshot = self._live_market_snapshot_context(
            observed_at=datetime(
                2026,
                4,
                30,
                9,
                10,
                tzinfo=timezone(timedelta(hours=9)),
            ),
            advancers=650,
            unchanged=50,
            decliners=250,
        )
        with (
            patch(
                'app.orders.next_open_execution.load_current_market_snapshot_context',
                return_value=healthy_snapshot,
            ),
            patch(
                'app.orders.next_open_execution.load_daily_bars_for_symbol_range',
                return_value=[{'trade_date': '2026-04-29', 'close_price': 50000}],
            ),
        ):
            recovered = open_service.refresh_plan_items(
                target_trade_date=self.target_trade_date,
                trigger='live_market_recovered',
            )['items'][0]
        self.assertEqual(recovered['execution_status'], EXECUTION_STATUS_PLANNED)
        self.assertTrue(
            {
                'current_live_market_risk_off',
                'current_live_market_evidence_unavailable',
            }.isdisjoint(
                {reason['code'] for reason in recovered['validation']['blocking_reasons']}
            )
        )
        self.assertEqual(
            recovered['validation']['current_live_market_authority']['action'],
            'allow',
        )
        self.assertNotIn('exec_dispatch', recovered['submission'])

    def test_healthy_live_refresh_retires_acked_transient_revocation_authority(self) -> None:
        settings.signal_decider_exec_integration_mode = 'dispatch'
        self._seed_snapshot(symbol='005930')
        preopen_service = self._service(
            state='PREOPEN',
            server_time='2026-04-30T08:40:00+09:00',
        )
        with patch(
            'app.orders.next_open_execution.load_daily_bars_for_symbol_range',
            return_value=[{'trade_date': '2026-04-29', 'close_price': 50000}],
        ):
            preopen_service.refresh_plan_items(
                target_trade_date=self.target_trade_date,
                trigger='preopen_scheduler',
            )
        row_id = self._mark_row_as_handed_off()

        staged = preopen_service.stage_market_evidence_unavailable_revocations(
            target_trade_date=self.target_trade_date,
            failure_source='market_snapshot_refresh_if_due_failed',
        )
        self.assertEqual(staged, {'staged_count': 1, 'pending_count': 1})
        pending = preopen_service.list_plan_items(
            target_trade_date=self.target_trade_date,
        )['items'][0]
        self._ack_pending_revocation(service=preopen_service, item=pending)

        acknowledged = preopen_service.list_plan_items(
            target_trade_date=self.target_trade_date,
        )['items'][0]
        self.assertEqual(acknowledged['execution_status'], EXECUTION_STATUS_REVOKED)
        self.assertIn('exec_dispatch', acknowledged['submission'])

        healthy_snapshot = self._live_market_snapshot_context(
            observed_at=datetime(
                2026,
                4,
                30,
                9,
                10,
                tzinfo=timezone(timedelta(hours=9)),
            ),
            advancers=650,
            unchanged=50,
            decliners=250,
        )
        open_service = self._service(
            state='OPEN',
            server_time='2026-04-30T09:10:30+09:00',
        )
        with (
            patch(
                'app.orders.next_open_execution.load_current_market_snapshot_context',
                return_value=healthy_snapshot,
            ),
            patch(
                'app.orders.next_open_execution.load_daily_bars_for_symbol_range',
                return_value=[{'trade_date': '2026-04-29', 'close_price': 50000}],
            ),
        ):
            recovered = open_service.refresh_plan_items(
                target_trade_date=self.target_trade_date,
                trigger='healthy_authority_recovery',
            )['items'][0]

        self.assertEqual(recovered['eligibility_status'], ELIGIBILITY_ELIGIBLE)
        self.assertEqual(recovered['execution_status'], EXECUTION_STATUS_PLANNED)
        self.assertIsNone(recovered['submitted_at'])
        self.assertNotIn('exec_dispatch', recovered['submission'])
        self.assertEqual(
            recovered['submission']['revocation']['recovery']['status'],
            'authority_revalidated',
        )
        archived_dispatch = recovered['submission']['revision_history'][-1][
            'dispatch_evidence'
        ]
        self.assertEqual(archived_dispatch['plan_id'], f'old-plan-{row_id}')
        self.assertEqual(archived_dispatch['dispatch_status'], 'accepted')

        with session_scope() as session:
            row = session.get(SignalDeciderExecutionPlanItem, row_id)
            guarded, pending_rows = _partition_exec_dispatch_rows([row], force=False)
            guarded_ids = [item.id for item in guarded]
            pending_ids = [item.id for item in pending_rows]
            first_generation = _delta_dispatch_batch_key(pending_rows)
            second_generation = _delta_dispatch_batch_key(pending_rows)

        self.assertEqual(guarded_ids, [])
        self.assertEqual(pending_ids, [row_id])
        self.assertEqual(first_generation, second_generation)

    def test_transient_revocation_recovery_refuses_changed_row_authority(self) -> None:
        settings.signal_decider_exec_integration_mode = 'dispatch'
        self._seed_snapshot(symbol='005930')
        service = self._service(
            state='PREOPEN',
            server_time='2026-04-30T08:40:00+09:00',
        )
        with patch(
            'app.orders.next_open_execution.load_daily_bars_for_symbol_range',
            return_value=[{'trade_date': '2026-04-29', 'close_price': 50000}],
        ):
            service.refresh_plan_items(
                target_trade_date=self.target_trade_date,
                trigger='preopen_scheduler',
            )
        row_id = self._mark_row_as_handed_off()
        service.stage_market_evidence_unavailable_revocations(
            target_trade_date=self.target_trade_date,
            failure_source='decision_plan_refresh_failed',
        )
        pending = service.list_plan_items(
            target_trade_date=self.target_trade_date,
        )['items'][0]
        self._ack_pending_revocation(service=service, item=pending)

        with session_scope() as session:
            row = session.get(SignalDeciderExecutionPlanItem, row_id)
            row.planned_order_type = 'limit'

        healthy_snapshot = self._live_market_snapshot_context(
            observed_at=datetime(
                2026,
                4,
                30,
                9,
                10,
                tzinfo=timezone(timedelta(hours=9)),
            ),
            advancers=650,
            unchanged=50,
            decliners=250,
        )
        open_service = self._service(
            state='OPEN',
            server_time='2026-04-30T09:10:30+09:00',
        )
        with (
            patch(
                'app.orders.next_open_execution.load_current_market_snapshot_context',
                return_value=healthy_snapshot,
            ),
            patch(
                'app.orders.next_open_execution.load_daily_bars_for_symbol_range',
                return_value=[{'trade_date': '2026-04-29', 'close_price': 50000}],
            ),
        ):
            unchanged = open_service.refresh_plan_items(
                target_trade_date=self.target_trade_date,
                trigger='tampered_authority_recovery',
            )['items'][0]

        self.assertEqual(unchanged['execution_status'], EXECUTION_STATUS_REVOKED)
        self.assertEqual(unchanged['eligibility_status'], ELIGIBILITY_BLOCKED)
        self.assertIn('exec_dispatch', unchanged['submission'])
        self.assertNotIn('recovery', unchanged['submission']['revocation'])

    def test_open_refresh_retries_when_live_market_snapshot_changes_before_plan_lock(self) -> None:
        self._seed_snapshot(symbol='005930')
        preopen_service = self._service(
            state='PREOPEN',
            server_time='2026-04-30T08:40:00+09:00',
        )
        with patch(
            'app.orders.next_open_execution.load_daily_bars_for_symbol_range',
            return_value=[{'trade_date': '2026-04-29', 'close_price': 50000}],
        ):
            preopen_service.refresh_plan_items(
                target_trade_date=self.target_trade_date,
                trigger='preopen_scheduler',
            )
        self._mark_row_as_handed_off()

        healthy_snapshot = self._live_market_snapshot_context(
            observed_at=datetime(
                2026,
                4,
                30,
                9,
                9,
                20,
                tzinfo=timezone(timedelta(hours=9)),
            ),
            advancers=650,
            unchanged=50,
            decliners=250,
        )
        severe_snapshot = self._live_market_snapshot_context(
            observed_at=datetime(
                2026,
                4,
                30,
                9,
                9,
                30,
                tzinfo=timezone(timedelta(hours=9)),
            )
        )
        live_snapshot_loader = MagicMock(
            side_effect=[
                healthy_snapshot,
                severe_snapshot,
                severe_snapshot,
                severe_snapshot,
            ]
        )
        open_service = self._service(
            state='OPEN',
            server_time='2026-04-30T09:10:00+09:00',
        )
        with (
            patch(
                'app.orders.next_open_execution.load_current_market_snapshot_context',
                live_snapshot_loader,
            ),
            patch(
                'app.orders.next_open_execution.load_daily_bars_for_symbol_range',
                return_value=[{'trade_date': '2026-04-29', 'close_price': 50000}],
            ),
        ):
            item = open_service.refresh_plan_items(
                target_trade_date=self.target_trade_date,
                trigger='market_snapshot_revision_race',
            )['items'][0]

        self.assertEqual(live_snapshot_loader.call_count, 4)
        self.assertEqual(item['execution_status'], EXECUTION_STATUS_REVOCATION_PENDING)
        self.assertEqual(
            item['submission']['revocation']['reason_code'],
            'current_live_market_risk_off',
        )

    def test_open_refresh_revokes_accepted_authority_on_stale_live_breadth(self) -> None:
        self._seed_snapshot(symbol='005930')
        preopen_service = self._service(
            state='PREOPEN',
            server_time='2026-04-30T08:40:00+09:00',
        )
        with patch(
            'app.orders.next_open_execution.load_daily_bars_for_symbol_range',
            return_value=[{'trade_date': '2026-04-29', 'close_price': 50000}],
        ):
            preopen_service.refresh_plan_items(
                target_trade_date=self.target_trade_date,
                trigger='preopen_scheduler',
            )
        self._mark_row_as_handed_off()

        stale_snapshot = self._live_market_snapshot_context(
            observed_at=datetime(
                2026,
                4,
                30,
                9,
                0,
                tzinfo=timezone(timedelta(hours=9)),
            )
        )
        open_service = self._service(
            state='OPEN',
            server_time='2026-04-30T09:10:00+09:00',
        )
        with (
            patch(
                'app.orders.next_open_execution.load_current_market_snapshot_context',
                return_value=stale_snapshot,
            ),
            patch(
                'app.orders.next_open_execution.load_daily_bars_for_symbol_range',
                return_value=[{'trade_date': '2026-04-29', 'close_price': 50000}],
            ),
        ):
            refreshed = open_service.refresh_plan_items(
                target_trade_date=self.target_trade_date,
                trigger='market_open_scheduler',
            )

        item = refreshed['items'][0]
        self.assertEqual(item['execution_status'], EXECUTION_STATUS_REVOCATION_PENDING)
        self.assertEqual(
            item['submission']['revocation']['reason_code'],
            'current_live_market_evidence_unavailable',
        )
        self.assertEqual(
            item['validation']['current_live_market_authority']['action'],
            'block_new_buy',
        )

    def test_open_refresh_revokes_accepted_authority_on_severe_breadth_when_flow_is_missing(self) -> None:
        self._seed_snapshot(symbol='005930')
        preopen_service = self._service(
            state='PREOPEN',
            server_time='2026-04-30T08:40:00+09:00',
        )
        with patch(
            'app.orders.next_open_execution.load_daily_bars_for_symbol_range',
            return_value=[{'trade_date': '2026-04-29', 'close_price': 50000}],
        ):
            preopen_service.refresh_plan_items(
                target_trade_date=self.target_trade_date,
                trigger='preopen_scheduler',
            )
        self._mark_row_as_handed_off()

        partial_snapshot = self._live_market_snapshot_context(
            observed_at=datetime(
                2026,
                4,
                30,
                9,
                9,
                30,
                tzinfo=timezone(timedelta(hours=9)),
            ),
            partial=True,
        )
        open_service = self._service(
            state='OPEN',
            server_time='2026-04-30T09:10:00+09:00',
        )
        with (
            patch(
                'app.orders.next_open_execution.load_current_market_snapshot_context',
                return_value=partial_snapshot,
            ),
            patch(
                'app.orders.next_open_execution.load_daily_bars_for_symbol_range',
                return_value=[{'trade_date': '2026-04-29', 'close_price': 50000}],
            ),
        ):
            refreshed = open_service.refresh_plan_items(
                target_trade_date=self.target_trade_date,
                trigger='market_open_scheduler',
            )

        item = refreshed['items'][0]
        self.assertEqual(item['execution_status'], EXECUTION_STATUS_REVOCATION_PENDING)
        self.assertEqual(
            item['submission']['revocation']['reason_code'],
            'current_live_market_risk_off',
        )
        self.assertEqual(
            item['validation']['current_live_market_authority']['action'],
            'reject',
        )

    def test_open_refresh_preserves_accepted_authority_on_healthy_breadth_when_fx_is_missing(self) -> None:
        self._seed_snapshot(symbol='005930')
        preopen_service = self._service(
            state='PREOPEN',
            server_time='2026-04-30T08:40:00+09:00',
        )
        with patch(
            'app.orders.next_open_execution.load_daily_bars_for_symbol_range',
            return_value=[{'trade_date': '2026-04-29', 'close_price': 50000}],
        ):
            preopen_service.refresh_plan_items(
                target_trade_date=self.target_trade_date,
                trigger='preopen_scheduler',
            )
        self._mark_row_as_handed_off()

        partial_snapshot = self._live_market_snapshot_context(
            observed_at=datetime(
                2026,
                4,
                30,
                9,
                9,
                30,
                tzinfo=timezone(timedelta(hours=9)),
            ),
            advancers=650,
            unchanged=50,
            decliners=250,
            partial=True,
        )
        partial_snapshot['payload']['missing_fields'] = [
            'usd_krw.rate',
            'usd_krw.effective_date',
        ]
        partial_snapshot['payload']['legs']['flow']['status'] = 'ready'
        partial_snapshot['payload']['legs']['fx']['status'] = 'missing'
        open_service = self._service(
            state='OPEN',
            server_time='2026-04-30T09:10:00+09:00',
        )
        with (
            patch(
                'app.orders.next_open_execution.load_current_market_snapshot_context',
                return_value=partial_snapshot,
            ),
            patch(
                'app.orders.next_open_execution.load_daily_bars_for_symbol_range',
                return_value=[{'trade_date': '2026-04-29', 'close_price': 50000}],
            ),
        ):
            refreshed = open_service.refresh_plan_items(
                target_trade_date=self.target_trade_date,
                trigger='market_open_scheduler',
            )

        item = refreshed['items'][0]
        self.assertEqual(item['execution_status'], EXECUTION_STATUS_SUBMITTED)
        self.assertNotIn('revocation', item['submission'])
        self.assertEqual(
            item['validation']['current_live_market_authority']['action'],
            'allow',
        )

    def test_open_refresh_revokes_accepted_authority_for_ambiguous_krx_market(self) -> None:
        self._seed_snapshot(symbol='005930', market='KRX')
        preopen_service = self._service(
            state='PREOPEN',
            server_time='2026-04-30T08:40:00+09:00',
        )
        with patch(
            'app.orders.next_open_execution.load_daily_bars_for_symbol_range',
            return_value=[{'trade_date': '2026-04-29', 'close_price': 50000}],
        ):
            preopen_service.refresh_plan_items(
                target_trade_date=self.target_trade_date,
                trigger='preopen_scheduler',
            )
        self._mark_row_as_handed_off()

        fresh_snapshot = self._live_market_snapshot_context(
            observed_at=datetime(
                2026,
                4,
                30,
                9,
                9,
                30,
                tzinfo=timezone(timedelta(hours=9)),
            )
        )
        open_service = self._service(
            state='OPEN',
            server_time='2026-04-30T09:10:00+09:00',
        )
        with (
            patch(
                'app.orders.next_open_execution.load_current_market_snapshot_context',
                return_value=fresh_snapshot,
            ),
            patch(
                'app.orders.next_open_execution.load_daily_bars_for_symbol_range',
                return_value=[{'trade_date': '2026-04-29', 'close_price': 50000}],
            ),
        ):
            refreshed = open_service.refresh_plan_items(
                target_trade_date=self.target_trade_date,
                trigger='market_open_scheduler',
            )

        item = refreshed['items'][0]
        self.assertEqual(item['execution_status'], EXECUTION_STATUS_REVOCATION_PENDING)
        self.assertEqual(
            item['submission']['revocation']['reason_code'],
            'current_live_market_evidence_unavailable',
        )
        self.assertEqual(
            item['validation']['current_live_market_authority']['reason_code'],
            'market_identity_missing_or_ambiguous',
        )

    def test_open_refresh_revokes_accepted_authority_when_live_snapshot_is_missing(self) -> None:
        self._seed_snapshot(symbol='005930')
        preopen_service = self._service(
            state='PREOPEN',
            server_time='2026-04-30T08:40:00+09:00',
        )
        with patch(
            'app.orders.next_open_execution.load_daily_bars_for_symbol_range',
            return_value=[{'trade_date': '2026-04-29', 'close_price': 50000}],
        ):
            preopen_service.refresh_plan_items(
                target_trade_date=self.target_trade_date,
                trigger='preopen_scheduler',
            )
        self._mark_row_as_handed_off()

        open_service = self._service(
            state='OPEN',
            server_time='2026-04-30T09:10:00+09:00',
        )
        with (
            patch(
                'app.orders.next_open_execution.load_current_market_snapshot_context',
                return_value=None,
            ),
            patch(
                'app.orders.next_open_execution.load_daily_bars_for_symbol_range',
                return_value=[{'trade_date': '2026-04-29', 'close_price': 50000}],
            ),
        ):
            refreshed = open_service.refresh_plan_items(
                target_trade_date=self.target_trade_date,
                trigger='market_open_scheduler',
            )

        item = refreshed['items'][0]
        self.assertEqual(item['execution_status'], EXECUTION_STATUS_REVOCATION_PENDING)
        self.assertEqual(
            item['submission']['revocation']['reason_code'],
            'current_live_market_evidence_unavailable',
        )
        self.assertEqual(
            item['validation']['current_live_market_authority']['action'],
            'block_new_buy',
        )

    def test_open_refresh_fails_closed_for_new_buy_without_live_market_evidence(self) -> None:
        self._seed_snapshot(symbol='005930')
        open_service = self._service(
            state='OPEN',
            server_time='2026-04-30T09:10:00+09:00',
        )
        with (
            patch(
                'app.orders.next_open_execution.load_current_market_snapshot_context',
                return_value=None,
            ),
            patch(
                'app.orders.next_open_execution.load_daily_bars_for_symbol_range',
                return_value=[{'trade_date': '2026-04-29', 'close_price': 50000}],
            ),
        ):
            refreshed = open_service.refresh_plan_items(
                target_trade_date=self.target_trade_date,
                trigger='market_open_scheduler',
            )

        item = refreshed['items'][0]
        self.assertEqual(item['eligibility_status'], ELIGIBILITY_BLOCKED)
        self.assertIn(
            'current_live_market_evidence_unavailable',
            {reason['code'] for reason in item['validation']['blocking_reasons']},
        )
        self.assertEqual(
            item['validation']['current_live_market_authority']['action'],
            'block_new_buy',
        )

    def test_refresh_revokes_terminal_buy_when_latest_candidate_disappears(self) -> None:
        self._seed_snapshot(symbol='005930')
        service = self._service()
        with patch(
            'app.orders.next_open_execution.load_daily_bars_for_symbol_range',
            return_value=[{'trade_date': '2026-04-29', 'close_price': 50000}],
        ):
            service.refresh_plan_items(target_trade_date=self.target_trade_date, trigger='test')
        self._mark_row_as_handed_off()

        with patch.object(service, '_load_latest_snapshots', return_value=[]):
            refreshed = service.refresh_plan_items(
                target_trade_date=self.target_trade_date,
                trigger='candidate_removed',
            )
        item = next(
            row for row in refreshed['items']
            if row['account_reference'] == self.account_reference and row['symbol'] == '005930'
        )
        self.assertEqual(item['execution_status'], EXECUTION_STATUS_REVOCATION_PENDING)
        self.assertEqual(item['submission']['revocation']['reason_code'], 'candidate_removed')
        self.assertTrue(
            item['submission']['revocation']['replacement_decision_snapshot_id'].startswith(
                'observation:'
            )
        )

        self._ack_pending_revocation(service=service, item=item)
        revoked = service.list_plan_items(target_trade_date=self.target_trade_date)['items'][0]
        self.assertEqual(revoked['execution_status'], EXECUTION_STATUS_REVOKED)

    def test_refresh_revokes_terminal_buy_when_account_route_is_removed(self) -> None:
        self._seed_snapshot(symbol='005930')
        service = self._service()
        with patch(
            'app.orders.next_open_execution.load_daily_bars_for_symbol_range',
            return_value=[{'trade_date': '2026-04-29', 'close_price': 50000}],
        ):
            service.refresh_plan_items(target_trade_date=self.target_trade_date, trigger='test')
        self._mark_row_as_handed_off()
        settings.signal_decider_next_open_account_targets = ''

        refreshed = service.refresh_plan_items(
            target_trade_date=self.target_trade_date,
            trigger='account_route_removed',
        )
        item = next(
            row for row in refreshed['items']
            if row['account_reference'] == self.account_reference and row['symbol'] == '005930'
        )
        self.assertEqual(item['execution_status'], EXECUTION_STATUS_REVOCATION_PENDING)
        self.assertEqual(item['submission']['revocation']['reason_code'], 'account_route_removed')
        self.assertTrue(
            item['submission']['revocation']['replacement_decision_snapshot_id'].startswith(
                'observation:'
            )
        )

        self._ack_pending_revocation(service=service, item=item)
        revoked = service.list_plan_items(target_trade_date=self.target_trade_date)['items'][0]
        self.assertEqual(revoked['execution_status'], EXECUTION_STATUS_REVOKED)

    def test_refresh_revokes_and_replans_terminal_buy_when_policy_budget_changes(self) -> None:
        self._seed_snapshot(symbol='005930')
        service = self._service()
        with patch(
            'app.orders.next_open_execution.load_daily_bars_for_symbol_range',
            return_value=[{'trade_date': '2026-04-29', 'close_price': 50000}],
        ):
            service.refresh_plan_items(target_trade_date=self.target_trade_date, trigger='test')
        self._mark_row_as_handed_off()
        settings.signal_decider_next_open_account_targets = (
            f'Main={self.account_reference}@900000'
        )

        with patch(
            'app.orders.next_open_execution.load_daily_bars_for_symbol_range',
            return_value=[{'trade_date': '2026-04-29', 'close_price': 50000}],
        ):
            refreshed = service.refresh_plan_items(
                target_trade_date=self.target_trade_date,
                trigger='policy_budget_changed',
            )
        item = next(
            row for row in refreshed['items']
            if row['account_reference'] == self.account_reference and row['symbol'] == '005930'
        )
        self.assertEqual(item['execution_status'], EXECUTION_STATUS_REVOCATION_PENDING)
        revocation = item['submission']['revocation']
        self.assertEqual(revocation['reason_code'], 'policy_scope_changed')
        self.assertTrue(revocation['replacement_actionable'])
        self.assertTrue(
            revocation['replacement_decision_snapshot_id'].startswith(
                'observation:policy_scope_changed:'
            )
        )

        self._ack_pending_revocation(service=service, item=item)
        acknowledged = service.list_plan_items(
            target_trade_date=self.target_trade_date,
        )['items'][0]
        self.assertEqual(acknowledged['execution_status'], EXECUTION_STATUS_PLANNED)

        with patch(
            'app.orders.next_open_execution.load_daily_bars_for_symbol_range',
            return_value=[{'trade_date': '2026-04-29', 'close_price': 50000}],
        ):
            rebound = service.refresh_plan_items(
                target_trade_date=self.target_trade_date,
                trigger='policy_revocation_ack',
            )['items'][0]
        self.assertEqual(Decimal(rebound['allocation_budget_krw']), Decimal('900000'))
        self.assertNotIn('exec_dispatch', rebound['submission'])


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


class NextOpenSetupCleanupSafetyTests(unittest.TestCase):
    def test_failed_setup_cleanup_restores_mutated_runtime_settings(self) -> None:
        original_targets = settings.signal_decider_next_open_account_targets
        case = NextOpenExecutionServiceTests(
            methodName='test_previous_trading_day_uses_kis_calendar_helper'
        )

        with patch.object(
            case,
            '_seed_policy_version',
            side_effect=RuntimeError('fixture seed failed'),
        ):
            result = unittest.TestResult()
            case.run(result)

        self.assertEqual(len(result.errors), 1)
        self.assertIn('fixture seed failed', result.errors[0][1])
        self.assertEqual(
            settings.signal_decider_next_open_account_targets,
            original_targets,
        )
