import copy
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
import json
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

import pytest
from sqlalchemy import delete, event, select, text
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import Session

from app.core.settings import Settings, settings
from app.core.test_database_safety import (
    assert_safe_test_database_settings,
    assert_single_active_policy_fixture_state,
)

assert_safe_test_database_settings(settings)

from app.db import engine, session_scope
from app.db_models import (
    AgentContextFeedback,
    AgentContextFeedbackFailure,
    AgentContextFeedbackRun,
    BrokerOrder,
    CurrentMarketSnapshotCache,
    OrderRequest,
    SignalDeciderDecisionSnapshot,
    SignalDeciderExecutionPlanItem,
    SignalDeciderPolicyVersion,
)
from app.market.clock import MarketState
from app.agent_context.signal_decider_loop_service import (
    SignalDeciderLoopRepository,
)
from app.agent_context.eligibility import _load_active_watch_reasons
from app.market.session import (
    KisKrxTradingCalendarProvider,
    reset_krx_calendar_provider,
    set_krx_calendar_provider,
)
from app.orders.exec_integration import (
    EXECUTION_ACTIVATION_CAPABILITY,
    EXECUTION_ACTIVATION_SCHEMA_VERSION,
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
    EXECUTION_STATUS_AWAITING_TRIGGER,
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
    _PlanRefreshRevisionConflict,
    _acquire_plan_refresh_advisory_lock,
    _actionable_signal_policy_versions,
    _delegate_short_term_watch_entry_quality_blocks_to_warnings,
    _execution_plan_row_authority_fingerprint,
    _is_canonical_closed_market_cost_absence,
    _lock_dispatch_authority_source_tables,
    _next_open_performance_cost_evidence,
    _next_open_policy_fingerprint,
    _normalize_objective_percentile,
    _snapshot_generation_authority_changes,
    _snapshot_generation_authority_fingerprint,
    _set_plan_refresh_transaction_timeouts,
    _preopen_plan_covers_latest_snapshots,
    _previous_trading_day,
    _to_decimal,
    _to_int,
    _to_json_safe,
)
from app.orders.store import upsert_broker_order, upsert_order_request


def _usable_candidate_pykrx_rows(
    symbol: str,
    *,
    categories: tuple[str, ...] = (
        'sector_classification',
        'investor_flow_symbol',
        'short_selling',
    ),
    market: str = 'KOSPI',
) -> list[dict]:
    investor_metrics = {
        'sell_volume': 1,
        'buy_volume': 2,
        'net_volume': 1,
        'sell_value': 10,
        'buy_value': 20,
        'net_value': 10,
    }
    payloads = {
        'sector_classification': {'sector_name': '반도체'},
        'investor_flow_symbol': {
            'investor_groups': {
                group: dict(investor_metrics)
                for group in ('개인', '외국인', '기관합계')
            }
        },
        'short_selling': {
            'short_volume': {
                'short_volume': 1,
                'total_volume': 10,
                'short_volume_ratio_pct': 10.0,
            }
        },
    }
    return [
        {
            'symbol': symbol,
            'market': market,
            'category': category,
            'source': 'pykrx',
            'payload': payloads[category],
        }
        for category in categories
    ]


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
    expected_source_tables = [
        'agent_context_feedback',
        'agent_context_feedback_failures',
        'agent_context_feedback_runs',
        'broker_orders',
        'current_market_snapshot_cache',
        'daily_action_summaries',
        'daily_advanced_value_factor_results',
        'daily_beta_factor_results',
        'daily_composite_factor_results',
        'daily_low_risk_factor_results',
        'daily_market_risk_filter_results',
        'daily_momentum_factor_results',
        'daily_piotroski_filter_results',
        'daily_price_bars',
        'daily_price_contexts',
        'daily_pykrx_enrichment_rows',
        'daily_quality_factor_results',
        'daily_signal_features',
        'daily_technical_snapshots',
        'daily_unified_value_factor_results',
        'daily_value_factor_results',
        'dart_disclosure_events',
        'dart_fundamentals_highlights',
        'dart_fundamentals_rows',
        'dart_fundamentals_snapshots',
        'eod_collection_runs',
        'external_news_item_classifications',
        'external_news_item_symbols',
        'external_news_items',
        'macro_context_observations',
        'order_requests',
        'signal_decider_decision_snapshots',
        'signal_decider_evaluations',
        'signal_decider_policy_versions',
        'signal_decider_realized_outcomes',
        'symbols',
    ]
    assert statements == [
        (
            'LOCK TABLE signal_decider_execution_plan_items '
            'IN SHARE ROW EXCLUSIVE MODE NOWAIT'
        ),
        (
            f"LOCK TABLE {', '.join(expected_source_tables)} "
            'IN SHARE MODE NOWAIT'
        ),
    ]


def test_plan_refresh_sets_bounded_transaction_timeouts_before_locks() -> None:
    session = MagicMock()
    bind = MagicMock()
    bind.dialect.name = 'postgresql'
    session.get_bind.return_value = bind

    with (
        patch.object(settings, 'signal_decider_db_statement_timeout_ms', 300_000),
        patch.object(settings, 'signal_decider_db_lock_timeout_ms', 30_000),
    ):
        _set_plan_refresh_transaction_timeouts(session)

    statements = [str(call.args[0]) for call in session.execute.call_args_list]
    assert statements == [
        'SET LOCAL statement_timeout = 300000',
        'SET LOCAL lock_timeout = 30000',
        'SET LOCAL idle_in_transaction_session_timeout = 930000'
    ]


def test_plan_refresh_idle_timeout_is_bounded_for_postgres_only() -> None:
    postgres_session = MagicMock()
    postgres_bind = MagicMock()
    postgres_bind.dialect.name = 'postgresql'
    postgres_session.get_bind.return_value = postgres_bind

    with (
        patch.object(settings, 'signal_decider_db_statement_timeout_ms', 2_000_000),
        patch.object(settings, 'signal_decider_db_lock_timeout_ms', 1_000_000),
    ):
        _set_plan_refresh_transaction_timeouts(postgres_session)

    assert str(postgres_session.execute.call_args_list[-1].args[0]) == (
        'SET LOCAL idle_in_transaction_session_timeout = 1200000'
    )

    sqlite_session = MagicMock()
    sqlite_bind = MagicMock()
    sqlite_bind.dialect.name = 'sqlite'
    sqlite_session.get_bind.return_value = sqlite_bind

    _set_plan_refresh_transaction_timeouts(sqlite_session)

    sqlite_session.execute.assert_not_called()


def test_generation_authority_ignores_diagnostics_but_not_validity() -> None:
    baseline = {
        'schema_version': 'persisted_decider_generation_validation_v1',
        'market_date': '2026-09-23',
        'valid_snapshot_ids': [],
        'invalid_snapshot_ids': [28741],
        'validations': [
            {
                'snapshot_id': 28741,
                'symbol': '019010',
                'agent_name': 'short-term-decider',
                'status': 'invalid',
                'reason': 'durable_decider_inputs_changed',
                'details': {'error_type': 'OperationalError'},
            }
        ],
    }
    concrete_mismatch = copy.deepcopy(baseline)
    concrete_mismatch['validations'][0]['details'] = {
        'reasons': [{'code': 'source_context_changed'}]
    }
    now_valid = copy.deepcopy(baseline)
    now_valid['valid_snapshot_ids'] = [28741]
    now_valid['invalid_snapshot_ids'] = []
    now_valid['validations'][0].update(
        status='valid',
        reason='durable_decider_inputs_unchanged',
    )

    assert _snapshot_generation_authority_fingerprint(
        baseline
    ) == _snapshot_generation_authority_fingerprint(concrete_mismatch)
    assert _snapshot_generation_authority_fingerprint(
        baseline
    ) != _snapshot_generation_authority_fingerprint(now_valid)
    assert _snapshot_generation_authority_changes(
        baseline,
        concrete_mismatch,
    ) == {}
    assert _snapshot_generation_authority_changes(
        baseline,
        now_valid,
    ) == {
        '28741': {
            'initial': {
                'snapshot_id': 28741,
                'symbol': '019010',
                'agent_name': 'short-term-decider',
                'status': 'invalid',
                'reason': 'durable_decider_inputs_changed',
            },
            'locked': {
                'snapshot_id': 28741,
                'symbol': '019010',
                'agent_name': 'short-term-decider',
                'status': 'valid',
                'reason': 'durable_decider_inputs_unchanged',
            },
        }
    }


def test_dispatch_authority_source_lock_contention_is_a_retryable_revision_conflict(
) -> None:
    class LockNotAvailableError(Exception):
        sqlstate = '55P03'

    session = MagicMock()
    bind = MagicMock()
    bind.dialect.name = 'postgresql'
    bind.dialect.identifier_preparer.format_table.side_effect = (
        lambda table: table.name
    )
    session.get_bind.return_value = bind
    lock_error = OperationalError(
        'LOCK TABLE source IN SHARE MODE NOWAIT',
        {},
        LockNotAvailableError('source writer is active'),
    )
    session.execute.side_effect = [MagicMock(), lock_error]

    try:
        _lock_dispatch_authority_source_tables(session)
    except _PlanRefreshRevisionConflict as exc:
        assert exc.__cause__ is lock_error
    else:  # pragma: no cover - assertion branch
        raise AssertionError('lock contention must use bounded revision retry')


def test_plan_refresh_retries_transient_revision_conflicts_with_backoff() -> None:
    service = SignalDeciderNextOpenExecutionService()
    expected = {'status': 'completed'}
    refresh_once = MagicMock(
        side_effect=[
            _PlanRefreshRevisionConflict('writer one'),
            _PlanRefreshRevisionConflict('writer two'),
            expected,
        ]
    )
    service._refresh_plan_items_once = refresh_once

    with patch('app.orders.next_open_execution.time.sleep') as sleep:
        result = service.refresh_plan_items(
            target_trade_date='2026-04-30',
            trigger='test',
        )

    assert result == expected
    assert refresh_once.call_count == 3
    assert all(
        call.kwargs['_compact_response'] is False
        for call in refresh_once.call_args_list
    )
    assert [entry.args[0] for entry in sleep.call_args_list] == [0.025, 0.05]


def test_scheduler_refresh_requests_compact_response() -> None:
    service = SignalDeciderNextOpenExecutionService()
    expected = {'summary': {'items_total': 0}, 'items': []}

    with patch.object(
        service,
        '_refresh_plan_items_once',
        return_value=expected,
    ) as refresh_plan_items_once:
        result = service.refresh_plan_if_due(
            target_trade_date='2026-04-30'
        )

    assert result == expected
    refresh_plan_items_once.assert_called_once_with(
        target_trade_date='2026-04-30',
        trigger='preopen_scheduler',
        force=False,
        _compact_response=True,
    )


def test_plan_refresh_fails_closed_after_bounded_revision_conflicts() -> None:
    service = SignalDeciderNextOpenExecutionService()
    refresh_once = MagicMock(
        side_effect=_PlanRefreshRevisionConflict('writer remains active')
    )
    service._refresh_plan_items_once = refresh_once

    with (
        patch('app.orders.next_open_execution.time.sleep') as sleep,
        pytest.raises(
            RuntimeError,
            match='could not bind stable decision inputs',
        ),
    ):
        service.refresh_plan_items(
            target_trade_date='2026-04-30',
            trigger='test',
        )

    assert refresh_once.call_count == 6
    assert [entry.args[0] for entry in sleep.call_args_list] == [
        0.025,
        0.05,
        0.1,
        0.2,
        0.4,
    ]


def test_dispatch_authority_source_locks_fence_real_postgres_writers_and_proofs(
) -> None:
    if engine.dialect.name != 'postgresql':
        return

    writer = Session(bind=engine)
    first_proof = Session(bind=engine)
    competing_proof = Session(bind=engine)
    try:
        writer.execute(
            text('LOCK TABLE eod_collection_runs IN ROW EXCLUSIVE MODE')
        )
        try:
            _lock_dispatch_authority_source_tables(first_proof)
        except _PlanRefreshRevisionConflict:
            first_proof.rollback()
        else:  # pragma: no cover - assertion branch
            raise AssertionError('an active durable source writer must fence proof')

        writer.rollback()
        _lock_dispatch_authority_source_tables(first_proof)

        try:
            _lock_dispatch_authority_source_tables(competing_proof)
        except _PlanRefreshRevisionConflict:
            competing_proof.rollback()
        else:  # pragma: no cover - assertion branch
            raise AssertionError('activation proofs must serialize before ACK writes')

        first_proof.rollback()
        _lock_dispatch_authority_source_tables(competing_proof)
    finally:
        writer.rollback()
        first_proof.rollback()
        competing_proof.rollback()
        writer.close()
        first_proof.close()
        competing_proof.close()


def test_plan_refresh_lock_fails_fast_instead_of_holding_a_pool_waiter() -> None:
    session = MagicMock()
    bind = MagicMock()
    bind.dialect.name = 'postgresql'
    session.get_bind.return_value = bind
    session.execute.return_value.scalar_one.return_value = False

    try:
        _acquire_plan_refresh_advisory_lock(
            session,
            target_trade_date=date(2026, 9, 14),
        )
    except _PlanRefreshRevisionConflict:
        pass
    else:
        raise AssertionError('a busy plan refresh lock must fail fast')

    statement = str(session.execute.call_args.args[0])
    assert 'pg_try_advisory_xact_lock' in statement


def test_next_open_summary_reports_distinct_symbols_beside_account_rows() -> None:
    def row(
        *,
        account_reference: str,
        symbol: str,
        eligibility_status: str,
        execution_status: str,
        decision_agent_name: str = 'signal-decider',
    ) -> SimpleNamespace:
        return SimpleNamespace(
            account_reference=account_reference,
            symbol=symbol,
            eligibility_status=eligibility_status,
            execution_status=execution_status,
            decision_agent_name=decision_agent_name,
            validation_payload={},
        )

    rows = [
        row(
            account_reference=account_reference,
            symbol=symbol,
            eligibility_status=eligibility_status,
            execution_status=execution_status,
            decision_agent_name=decision_agent_name,
        )
        for account_reference in ('KIS:12345678-01', 'KIS:87654321-01')
        for symbol, eligibility_status, execution_status, decision_agent_name in (
            ('005930', ELIGIBILITY_ELIGIBLE, EXECUTION_STATUS_PLANNED, 'signal-decider'),
            (
                '000660',
                ELIGIBILITY_MONITOR_CANDIDATE,
                EXECUTION_STATUS_AWAITING_TRIGGER,
                'short-term-decider',
            ),
            ('035420', ELIGIBILITY_BLOCKED, EXECUTION_STATUS_PLANNED, 'signal-decider'),
        )
    ]

    summary = SignalDeciderNextOpenExecutionService.__new__(
        SignalDeciderNextOpenExecutionService
    )._build_summary(
        rows=rows,
        target_trade_date=date(2026, 4, 30),
        runtime={
            'execution_enabled': True,
            'dry_run': False,
            'account_targets_configured': True,
            'account_targets_valid': True,
            'account_target_errors': [],
        },
    )

    assert {
        key: summary[key]
        for key in (
            'items_total',
            'symbol_count',
            'eligible_count',
            'eligible_symbol_count',
            'monitor_candidate_count',
            'monitor_candidate_symbol_count',
            'blocked_count',
            'blocked_symbol_count',
        )
    } == {
        'items_total': 6,
        'symbol_count': 3,
        'eligible_count': 2,
        'eligible_symbol_count': 1,
        'monitor_candidate_count': 2,
        'monitor_candidate_symbol_count': 1,
        'blocked_count': 2,
        'blocked_symbol_count': 1,
    }


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


class NextOpenRefreshUnitTests(unittest.TestCase):
    def test_scope_state_reads_use_signal_decider_db_budget(self) -> None:
        service = SignalDeciderNextOpenExecutionService()
        session = MagicMock()
        session.execute.return_value.scalars.return_value.all.return_value = []
        session.execute.return_value.all.return_value = []
        session_context = MagicMock()
        session_context.__enter__.return_value = session
        session_context.__exit__.return_value = False

        with (
            patch.object(settings, 'signal_decider_db_statement_timeout_ms', 300_000),
            patch.object(settings, 'signal_decider_db_lock_timeout_ms', 30_000),
            patch(
                'app.orders.next_open_execution.session_scope',
                return_value=session_context,
            ) as scoped_session,
        ):
            result = service._load_latest_decider_scope_state(
                source_market_date=date(2026, 9, 23),
                target_trade_date=date(2026, 9, 28),
            )

        scoped_session.assert_called_once_with(
            statement_timeout_ms=300_000,
            lock_timeout_ms=30_000,
        )
        self.assertEqual(result['selected_by_key'], {})

    def test_policy_context_versions_include_only_actionable_signals(self) -> None:
        def snapshot(action: str, policy_version: str) -> SimpleNamespace:
            return SimpleNamespace(
                agent_name='signal-decider',
                policy_version=policy_version,
                next_day_open_action=action,
                decision_action=action,
                request_payload={
                    'symbol_day_context': {'market': 'KOSPI'},
                },
            )

        self.assertEqual(
            _actionable_signal_policy_versions(
                [
                    snapshot('wait', 'wait-policy'),
                    snapshot('reject', 'reject-policy'),
                    snapshot('buy', 'buy-policy'),
                ]
            ),
            ['buy-policy'],
        )

    def test_compact_refresh_response_skips_detailed_serialization(self) -> None:
        target_trade_date = date(2026, 4, 30)
        rows = [SimpleNamespace(id=1)]
        service = SignalDeciderNextOpenExecutionService()

        with (
            patch.object(
                service,
                '_load_plan_rows',
                return_value=rows,
            ) as load_plan_rows,
            patch.object(
                service,
                '_serialize_row',
                return_value={'plan_item_id': 1},
            ) as serialize_row,
        ):
            compact_rows, compact_items = (
                service._load_plan_refresh_response(
                    target_trade_date=target_trade_date,
                    compact_response=True,
                )
            )
            load_plan_rows.assert_called_once_with(
                target_trade_date=target_trade_date,
                account_reference=None,
                include_payloads=False,
                include_validation_payload=True,
            )
            serialize_row.assert_not_called()

            load_plan_rows.reset_mock()
            detailed_rows, detailed_items = (
                service._load_plan_refresh_response(
                    target_trade_date=target_trade_date,
                    compact_response=False,
                )
            )

        self.assertIs(compact_rows, rows)
        self.assertEqual(compact_items, [])
        self.assertIs(detailed_rows, rows)
        self.assertEqual(detailed_items, [{'plan_item_id': 1}])
        load_plan_rows.assert_called_once_with(
            target_trade_date=target_trade_date,
            account_reference=None,
        )
        serialize_row.assert_called_once_with(rows[0])


class AgentContextCompletenessUnitTests(unittest.TestCase):
    def test_empty_candidate_scope_uses_source_decision_cohort(self) -> None:
        source_market_date = date(2026, 9, 11)
        latest_run = {
            'run_id': 'feedback-cycle-2026-09-11-decision',
            'status': 'partial',
            'symbols_total': 160,
            'completed_count': 60,
            'failed_count': 100,
            'started_at': datetime(2026, 9, 12, 13, 36, tzinfo=timezone.utc),
            'completed_at': datetime(2026, 9, 12, 14, 57, tzinfo=timezone.utc),
            'request_payload': {
                'mode': 'daily_retry_cycle_role_sequenced',
                'roles': ['short-term-decider', 'signal-decider'],
                'symbols': ['005930', '000660'],
                'requested_symbols': ['005930', '000660'],
                'role_symbol_scopes': {
                    'short-term-decider': ['005930', '000660'],
                    'signal-decider': ['005930', '000660'],
                },
            },
        }
        failure = {
            'symbol': '000660',
            'agent_name': 'short-term-decider',
            'run_id': 'repair-short-term',
            'failure_kind': 'response_contract_invalid',
            'retryable': True,
            'summary_payload': {},
            'context_payload': {},
        }
        scope_state = {
            'selected_by_key': {
                ('005930', 'short-term-decider'): object(),
                ('005930', 'signal-decider'): object(),
                ('000660', 'short-term-decider'): None,
                ('000660', 'signal-decider'): object(),
            },
            'authoritative_run_id_by_key': {
                ('005930', 'short-term-decider'): 'initial-short-term',
                ('005930', 'signal-decider'): 'initial-signal',
                ('000660', 'short-term-decider'): 'repair-short-term',
                ('000660', 'signal-decider'): 'initial-signal',
            },
            'failures_by_key': {
                ('000660', 'short-term-decider'): failure,
            },
            'run_snapshots': [],
        }
        service = SignalDeciderNextOpenExecutionService()

        with (
            patch(
                'app.orders.next_open_execution.agent_context_feedback_run_state_service.find_latest_run',
                return_value=latest_run,
            ) as find_latest_run,
            patch.object(
                service,
                '_load_latest_decider_scope_state',
                return_value=scope_state,
            ),
        ):
            result = service._build_agent_context_completeness_context(
                source_market_date=source_market_date,
                candidate_symbols=[],
            )

        find_latest_run.assert_called_once_with(
            market_date=source_market_date,
            requested_symbols=None,
            requested_roles=['short-term-decider', 'signal-decider'],
            request_modes=['daily_retry_cycle_role_sequenced'],
        )
        self.assertEqual(result['candidate_set_basis'], 'source_decision_run')
        self.assertEqual(result['status'], 'incomplete')
        self.assertEqual(result['cohort_status'], 'incomplete')
        self.assertTrue(result['has_incomplete_agent_context'])
        self.assertEqual(result['evaluation_scope_count'], 4)
        self.assertEqual(result['completed_scope_count'], 3)
        self.assertEqual(result['failure_count'], 1)
        self.assertEqual(result['pending_scope_count'], 0)
        self.assertEqual(result['latest_run']['completed_count'], 60)
        self.assertEqual(result['latest_run']['failed_count'], 100)

    def test_complete_candidate_scope_reports_incomplete_source_cohort(self) -> None:
        source_market_date = date(2026, 9, 11)
        source_run = {
            'run_id': 'feedback-cycle-source',
            'status': 'partial',
            'symbols_total': 4,
            'completed_count': 3,
            'failed_count': 1,
            'request_payload': {
                'mode': 'daily_retry_cycle_role_sequenced',
                'roles': ['short-term-decider', 'signal-decider'],
                'role_symbol_scopes': {
                    'short-term-decider': ['005930', '000660'],
                    'signal-decider': ['005930', '000660'],
                },
            },
        }
        failed_scope = ('000660', 'short-term-decider')
        service = SignalDeciderNextOpenExecutionService()
        scope_state = {
            'selected_by_key': {
                ('005930', 'short-term-decider'): object(),
                ('005930', 'signal-decider'): object(),
                failed_scope: None,
                ('000660', 'signal-decider'): object(),
            },
            'authoritative_run_id_by_key': {failed_scope: 'failed-child'},
            'failures_by_key': {
                failed_scope: {
                    'symbol': '000660',
                    'agent_name': 'short-term-decider',
                    'run_id': 'failed-child',
                    'failure_kind': 'response_contract_invalid',
                    'retryable': True,
                    'summary_payload': {},
                    'context_payload': {},
                }
            },
            'run_snapshots': [],
        }

        with (
            patch(
                'app.orders.next_open_execution.agent_context_feedback_run_state_service.find_latest_run',
                return_value=source_run,
            ),
            patch.object(
                service,
                '_load_latest_decider_scope_state',
                return_value=scope_state,
            ),
        ):
            result = service._build_agent_context_completeness_context(
                source_market_date=source_market_date,
                candidate_symbols=['005930'],
            )

        self.assertEqual(result['status'], 'complete')
        self.assertEqual(result['cohort_status'], 'incomplete')
        self.assertFalse(result['candidate_set_definitive'])
        self.assertFalse(result['has_incomplete_agent_context'])
        self.assertEqual(result['failure_count'], 0)
        self.assertEqual(result['cohort_failure_count'], 1)

    def test_candidate_without_source_cohort_is_unknown(self) -> None:
        service = SignalDeciderNextOpenExecutionService()
        with (
            patch(
                'app.orders.next_open_execution.agent_context_feedback_run_state_service.find_latest_run',
                return_value=None,
            ),
            patch.object(
                service,
                '_load_latest_decider_scope_state',
                return_value={
                    'selected_by_key': {},
                    'authoritative_run_id_by_key': {},
                    'failures_by_key': {},
                    'run_snapshots': [],
                },
            ),
        ):
            result = service._build_agent_context_completeness_context(
                source_market_date=date(2026, 9, 11),
                candidate_symbols=['005930'],
            )

        self.assertEqual(result['status'], 'unknown')
        self.assertIsNone(result['has_incomplete_agent_context'])
        self.assertEqual(result['reason'], 'source_decision_cohort_missing')


class NextOpenExecutionServiceTests(unittest.IsolatedAsyncioTestCase):
    @classmethod
    def setUpClass(cls) -> None:
        AgentContextFeedback.__table__.create(bind=engine, checkfirst=True)
        AgentContextFeedbackFailure.__table__.create(bind=engine, checkfirst=True)
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
        self.feedback_failure_symbols: list[str] = []
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

        def usable_candidate_enrichment_rows(*, symbols=None, categories=None, **_kwargs):
            return [
                row
                for symbol in (symbols or [])
                for row in _usable_candidate_pykrx_rows(
                    symbol,
                    categories=tuple(
                        categories
                        or (
                            'sector_classification',
                            'investor_flow_symbol',
                            'short_selling',
                        )
                    ),
                )
            ]

        self.candidate_enrichment_rows_patcher = patch(
            'app.orders.next_open_execution.load_daily_pykrx_enrichment_rows',
            side_effect=usable_candidate_enrichment_rows,
        )
        self.candidate_enrichment_rows_patcher.start()
        self.addCleanup(self.candidate_enrichment_rows_patcher.stop)

        def complete_agent_context(
            *,
            source_market_date,
            candidate_symbols=None,
            **_kwargs,
        ):
            return {
                'schema_version': 'agent_context_completeness_v1',
                'source_market_date': source_market_date.isoformat(),
                'candidate_symbols': sorted(set(candidate_symbols or [])),
                'candidate_set_basis': (
                    'latest_signal_decider_snapshots'
                    if candidate_symbols
                    else 'source_decision_run'
                ),
                'status': 'complete',
                'cohort_status': 'complete',
                'has_incomplete_agent_context': False,
                'candidate_set_definitive': True,
                'authority_fingerprint': 'test-agent-context-authority',
                'latest_run': None,
                'failure_count': 0,
                'failure_groups': [],
            }

        self.agent_context_completeness_patcher = patch.object(
            SignalDeciderNextOpenExecutionService,
            '_build_agent_context_completeness_context',
            side_effect=complete_agent_context,
        )
        self.agent_context_completeness_mock = (
            self.agent_context_completeness_patcher.start()
        )
        self.addCleanup(self.agent_context_completeness_patcher.stop)
        self.agent_context_from_state_patcher = patch.object(
            SignalDeciderNextOpenExecutionService,
            '_build_agent_context_completeness_from_state',
            side_effect=complete_agent_context,
        )
        self.agent_context_from_state_mock = (
            self.agent_context_from_state_patcher.start()
        )
        self.addCleanup(self.agent_context_from_state_patcher.stop)
        def valid_snapshot_generations(
            *,
            market_date,
            snapshots,
            full_source_revalidation_actionable_only=False,
        ):
            snapshot_ids = sorted(int(snapshot.id) for snapshot in snapshots)
            identity_by_id = {
                int(snapshot.id): {
                    'symbol': str(snapshot.symbol).strip().upper(),
                    'agent_name': str(snapshot.agent_name).strip(),
                }
                for snapshot in snapshots
            }
            return {
                'schema_version': 'persisted_decider_generation_validation_v1',
                'market_date': str(market_date),
                'valid_snapshot_ids': snapshot_ids,
                'invalid_snapshot_ids': [],
                'validations': [
                    {
                        'snapshot_id': snapshot_id,
                        **identity_by_id[snapshot_id],
                        'status': 'valid',
                        'reason': 'test_generation_unchanged',
                    }
                    for snapshot_id in snapshot_ids
                ],
                'generation_fingerprint': (
                    'test-snapshot-generation:'
                    + ','.join(str(snapshot_id) for snapshot_id in snapshot_ids)
                ),
            }

        self.snapshot_generation_patcher = patch(
            'app.orders.next_open_execution.agent_feedback_orchestrator.revalidate_persisted_decider_snapshots',
            side_effect=valid_snapshot_generations,
        )
        self.snapshot_generation_mock = self.snapshot_generation_patcher.start()
        self.addCleanup(self.snapshot_generation_patcher.stop)
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
            if self.feedback_failure_symbols:
                session.execute(
                    delete(AgentContextFeedbackFailure).where(
                        AgentContextFeedbackFailure.market_date == self.market_date,
                        AgentContextFeedbackFailure.symbol.in_(
                            self.feedback_failure_symbols
                        ),
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
            'app.orders.next_open_execution._set_plan_refresh_transaction_timeouts',
        ) as set_transaction_timeouts, patch(
            'app.orders.next_open_execution._lock_dispatch_authority_source_tables',
            side_effect=mark_authority_locked,
        ):
            generation = service.inspect_dispatch_authority_generation(
                target_trade_date=self.target_trade_date,
                expected_row_bindings={},
            )

        self.assertEqual(generation['status'], 'valid')
        self.assertEqual(market_state_call_count, 2)
        set_transaction_timeouts.assert_called_once()

    def test_refresh_revalidates_pykrx_authority_after_source_tables_are_locked(
        self,
    ) -> None:
        settings.signal_decider_exec_integration_mode = 'dispatch'
        self._seed_snapshot(symbol='005930')
        service = self._service(
            state='PREOPEN',
            server_time='2026-04-30T08:40:00+09:00',
        )
        authority_locked = False
        preflight_lock_states: list[bool] = []
        candidate_lock_states: list[bool] = []

        def mark_authority_locked(_session) -> None:
            nonlocal authority_locked
            authority_locked = True

        def inspect_preflight(**_kwargs) -> dict:
            preflight_lock_states.append(authority_locked)
            return {
                'market_date': self.market_date.isoformat(),
                'status': 'blocked' if authority_locked else 'ready',
                'allowed': not authority_locked,
                'required_categories': [
                    'sector_classification',
                    'investor_flow_symbol',
                    'short_selling',
                ],
                'failed_categories': (
                    ['short_selling'] if authority_locked else []
                ),
                'zero_coverage_categories': (
                    ['short_selling'] if authority_locked else []
                ),
                'category_row_counts': {
                    'sector_classification': 1,
                    'investor_flow_symbol': 1,
                    'short_selling': 0 if authority_locked else 1,
                },
            }

        def load_candidate_rows(*, symbols=None, categories=None, **_kwargs):
            candidate_lock_states.append(authority_locked)
            if authority_locked:
                return []
            return [
                row
                for symbol in (symbols or [])
                for row in _usable_candidate_pykrx_rows(
                    symbol,
                    categories=tuple(categories or ()),
                )
            ]

        with (
            patch(
                'app.orders.next_open_execution._lock_dispatch_authority_source_tables',
                side_effect=mark_authority_locked,
            ),
            patch(
                'app.orders.next_open_execution.inspect_pykrx_enrichment_preflight',
                side_effect=inspect_preflight,
            ),
            patch(
                'app.orders.next_open_execution.load_daily_pykrx_enrichment_rows',
                side_effect=load_candidate_rows,
            ),
            patch(
                'app.orders.next_open_execution.load_daily_bars_for_symbol_range',
                return_value=[
                    {'trade_date': '2026-04-29', 'close_price': 50000}
                ],
            ),
        ):
            result = service.refresh_plan_items(
                target_trade_date=self.target_trade_date,
                trigger='test',
            )

        self.assertEqual(preflight_lock_states, [False, True])
        self.assertEqual(candidate_lock_states, [False, True])
        self.assertFalse(result['summary']['enrichment_preflight_allowed'])
        self.assertEqual(
            result['summary']['enrichment_preflight_status'],
            'blocked',
        )
        self.assertEqual(result['summary']['eligible_count'], 0)
        self.assertEqual(result['summary']['blocked_count'], 1)
        item = next(
            entry
            for entry in result['items']
            if entry['account_reference'] == self.account_reference
            and entry['symbol'] == '005930'
            and entry['decision_agent_name'] == 'signal-decider'
        )
        self.assertEqual(item['eligibility_status'], ELIGIBILITY_BLOCKED)
        self.assertEqual(
            item['validation']['candidate_pykrx_enrichment'][
                'missing_categories'
            ],
            [
                'sector_classification',
                'investor_flow_symbol',
                'short_selling',
            ],
        )
        blocking_codes = {
            reason['code']
            for reason in item['validation']['blocking_reasons']
        }
        self.assertIn('pykrx_enrichment_preflight_blocked', blocking_codes)
        self.assertIn('candidate_pykrx_enrichment_missing', blocking_codes)

    def test_refresh_compares_agent_context_over_same_raw_snapshot_cohort(
        self,
    ) -> None:
        settings.signal_decider_exec_integration_mode = 'dispatch'
        self._seed_snapshot(symbol='005930')
        self._seed_snapshot(symbol='000660')
        service = self._service(
            state='PREOPEN',
            server_time='2026-04-30T08:40:00+09:00',
        )

        def reject_one_generation(*, market_date, snapshots, **_kwargs):
            valid_ids = sorted(
                int(snapshot.id)
                for snapshot in snapshots
                if snapshot.symbol == '005930'
            )
            invalid_ids = sorted(
                int(snapshot.id)
                for snapshot in snapshots
                if snapshot.symbol == '000660'
            )
            return {
                'schema_version': (
                    'persisted_decider_generation_validation_v1'
                ),
                'market_date': str(market_date),
                'valid_snapshot_ids': valid_ids,
                'invalid_snapshot_ids': invalid_ids,
                'validations': [
                    {
                        'snapshot_id': int(snapshot.id),
                        'symbol': snapshot.symbol,
                        'agent_name': snapshot.agent_name,
                        'status': (
                            'valid'
                            if snapshot.symbol == '005930'
                            else 'invalid'
                        ),
                        'reason': (
                            'test_generation_unchanged'
                            if snapshot.symbol == '005930'
                            else 'test_generation_rejected'
                        ),
                    }
                    for snapshot in snapshots
                ],
                'generation_fingerprint': 'test-mixed-generation',
            }

        self.snapshot_generation_mock.side_effect = reject_one_generation
        with patch(
            'app.orders.next_open_execution.load_daily_bars_for_symbol_range',
            return_value=[
                {'trade_date': '2026-04-29', 'close_price': 50000}
            ],
        ):
            result = service.refresh_plan_items(
                target_trade_date=self.target_trade_date,
                trigger='test',
            )

        initial_symbols = sorted(
            self.agent_context_completeness_mock.call_args.kwargs[
                'candidate_symbols'
            ]
        )
        locked_symbol_sets = [
            sorted(call.kwargs['candidate_symbols'])
            for call in self.agent_context_from_state_mock.call_args_list
        ]
        self.assertEqual(initial_symbols, ['000660', '005930'])
        self.assertEqual(locked_symbol_sets, [['000660', '005930']])
        self.assertEqual(
            {
                item['symbol']
                for item in result['items']
                if item['account_reference'] == self.account_reference
            },
            {'005930'},
        )

    def test_refresh_retries_when_source_parent_scope_changes_without_snapshot_change(
        self,
    ) -> None:
        self._seed_snapshot(symbol='005930')
        service = self._service(
            state='PREOPEN',
            server_time='2026-04-30T08:40:00+09:00',
        )
        scope_state = service._load_latest_decider_scope_state(
            source_market_date=self.market_date
        )

        def parent_run(*, run_id: str, symbols: list[str], updated_minute: int):
            return {
                'run_id': run_id,
                'status': 'completed',
                'updated_at': datetime(
                    2026, 4, 29, 5, updated_minute, tzinfo=timezone.utc
                ),
                'request_payload': {
                    'mode': 'daily_retry_cycle_role_sequenced',
                    'roles': ['short-term-decider', 'signal-decider'],
                    'role_symbol_scopes': {
                        'short-term-decider': symbols,
                        'signal-decider': symbols,
                    },
                },
            }

        initial_parent = parent_run(
            run_id='source-parent-before',
            symbols=['005930'],
            updated_minute=0,
        )
        changed_parent = parent_run(
            run_id='source-parent-after',
            symbols=['005930', '000660'],
            updated_minute=1,
        )
        initial_fingerprint = service._agent_context_authority_fingerprint(
            source_run=initial_parent,
            scope_state=scope_state,
            candidate_symbols=['005930'],
        )
        initial_context = {
            'schema_version': 'agent_context_completeness_v1',
            'source_market_date': self.market_date.isoformat(),
            'candidate_symbols': ['005930'],
            'candidate_set_basis': 'latest_signal_decider_snapshots',
            'status': 'complete',
            'cohort_status': 'complete',
            'has_incomplete_agent_context': False,
            'candidate_set_definitive': True,
            'authority_fingerprint': initial_fingerprint,
            'latest_run': {'run_id': initial_parent['run_id'], 'status': 'completed'},
            'failure_count': 0,
            'failure_groups': [],
        }
        changed_context = {
            **initial_context,
            'authority_fingerprint': service._agent_context_authority_fingerprint(
                source_run=changed_parent,
                scope_state=scope_state,
                candidate_symbols=['005930'],
            ),
            'latest_run': {
                'run_id': changed_parent['run_id'],
                'status': 'completed',
            },
        }

        with (
            patch.object(
                service,
                '_build_agent_context_completeness_context',
                return_value=initial_context,
            ),
            patch.object(
                service,
                '_build_agent_context_completeness_from_state',
                return_value=changed_context,
            ) as build_current_context,
            patch(
                'app.orders.next_open_execution.load_daily_bars_for_symbol_range',
                return_value=[{'trade_date': '2026-04-29', 'close_price': 50000}],
            ),
            patch('app.orders.next_open_execution.time.sleep'),
        ):
            with self.assertRaisesRegex(
                RuntimeError,
                'could not bind stable decision inputs',
            ):
                service.refresh_plan_items(
                    target_trade_date=self.target_trade_date,
                    trigger='test',
                )

        self.assertEqual(build_current_context.call_count, 6)

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
        feedback_received_at: datetime | None = None,
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
                feedback_received_at=feedback_received_at
                or datetime(2026, 4, 29, 6, 0, tzinfo=timezone.utc),
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
                    'signal_decider_policy': {
                        'current_version': self.policy_version,
                        'rule_bundle': {
                            'schema_version': (
                                'signal_decider_policy_bundle_v1'
                            ),
                        },
                    },
                    'symbol_day_context': symbol_day_context,
                },
                response_payload={'next_day_open_action': next_day_open_action},
                response_summary_payload={'policy_version': self.policy_version},
            )
            session.add(row)
            session.flush()
            return row.id

    @staticmethod
    def _reuse_evidence(
        *,
        symbol: str,
        feedback_run_id: str,
        feedback_received_at: datetime,
        decision_snapshot_id: int,
        feedback_id: int | None = None,
        decision_snapshot_fingerprint: str | None = None,
        decision_snapshot_observed_at: datetime | None = None,
        role: str = 'signal-decider',
    ) -> dict[str, dict[str, dict[str, object]]]:
        return {
            role: {
                symbol: {
                    'schema_version': 'agent_feedback_reuse_evidence_v1',
                    'feedback_id': feedback_id,
                    'feedback_run_id': feedback_run_id,
                    'feedback_received_at': feedback_received_at.isoformat(),
                    'decision_snapshot_id': decision_snapshot_id,
                    'decision_snapshot_fingerprint': (
                        decision_snapshot_fingerprint
                    ),
                    'decision_snapshot_observed_at': (
                        decision_snapshot_observed_at or feedback_received_at
                    ).isoformat(),
                }
            }
        }

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
        mode: str = 'role_sequential_runtime_buffered',
        reuse_existing: bool = True,
        force_recompute: bool = False,
        parent_run_id: str | None = None,
        child_run_lineage: bool = False,
        reused_symbols_by_role: dict[str, list[str]] | None = None,
        reused_feedback_evidence_by_role: (
            dict[str, dict[str, dict[str, object]]] | None
        ) = None,
        scope: str | None = None,
    ) -> None:
        self.feedback_run_ids.append(run_id)
        request_payload = {
            'symbols': symbols,
            'requested_symbols': symbols,
            'roles': roles,
            'mode': mode,
            'reuse_existing': reuse_existing,
            'force_recompute': force_recompute,
            'role_symbol_scopes': role_symbol_scopes
            or {role: symbols for role in roles},
        }
        if scope is not None:
            request_payload['scope'] = scope
        if parent_run_id is not None:
            request_payload['parent_run_id'] = parent_run_id
        if child_run_lineage:
            request_payload['child_run_lineage'] = {
                'schema_version': 'agent_feedback_child_run_lineage_v1',
                'parent_run_id': run_id,
                'child_link_field': 'parent_run_id',
            }
        with session_scope() as session:
            session.add(
                AgentContextFeedbackRun(
                    run_id=run_id,
                    market_date=self.market_date,
                    status=status,
                    symbols_total=len(symbols),
                    completed_count=(len(symbols) if completed_count is None else completed_count),
                    failed_count=failed_count,
                    request_payload=request_payload,
                    summary_payload={
                        'reused_symbols_by_role': reused_symbols_by_role or {},
                        'reused_feedback_evidence_by_role': (
                            reused_feedback_evidence_by_role or {}
                        ),
                    },
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
        decision_run_id: str | None = None,
        feedback_received_at: datetime | None = None,
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
                decision_run_id=decision_run_id or f'short-run-{symbol}',
                feedback_received_at=feedback_received_at
                or datetime(2026, 4, 29, 6, 0, tzinfo=timezone.utc),
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
        self.assertTrue(self.snapshot_generation_mock.call_args_list)
        self.assertTrue(
            all(
                call.kwargs.get(
                    'full_source_revalidation_actionable_only'
                )
                is True
                for call in self.snapshot_generation_mock.call_args_list
            )
        )

    def test_scheduler_refresh_reuses_initial_scope_state_before_locked_reread(
        self,
    ) -> None:
        self.agent_context_completeness_patcher.stop()
        self._seed_snapshot(symbol='005930')
        service = self._service(
            state='PREOPEN',
            server_time='2026-04-30T08:40:00+09:00',
        )

        with (
            patch.object(
                service,
                '_load_latest_decider_scope_state',
                wraps=service._load_latest_decider_scope_state,
            ) as load_scope_state,
            patch(
                'app.orders.next_open_execution.load_daily_bars_for_symbol_range',
                return_value=[
                    {'trade_date': '2026-04-29', 'close_price': 50000}
                ],
            ),
        ):
            result = service.refresh_plan_if_due(
                target_trade_date=self.target_trade_date,
            )

        self.assertIn('summary', result)
        self.assertEqual(result['items'], [])
        self.assertEqual(load_scope_state.call_count, 2)
        self.assertIsNone(
            load_scope_state.call_args_list[0].kwargs.get('_session')
        )
        self.assertIsNotNone(
            load_scope_state.call_args_list[1].kwargs.get('_session')
        )

    def test_refresh_revokes_decision_observed_at_target_session_open(
        self,
    ) -> None:
        settings.signal_decider_exec_integration_mode = 'dispatch'
        snapshot_id = self._seed_snapshot(
            symbol='005930',
            feedback_received_at=datetime(
                2026,
                4,
                29,
                23,
                59,
                59,
                tzinfo=timezone.utc,
            ),
        )
        service = self._service(
            state='PREOPEN',
            server_time='2026-04-30T08:40:00+09:00',
        )

        with patch(
            'app.orders.next_open_execution.load_daily_bars_for_symbol_range',
            return_value=[
                {'trade_date': '2026-04-29', 'close_price': 50000}
            ],
        ):
            initial = service.refresh_plan_items(
                target_trade_date=self.target_trade_date,
                trigger='before_target_open_cutoff',
            )
            with session_scope() as session:
                snapshot = session.get(
                    SignalDeciderDecisionSnapshot,
                    snapshot_id,
                )
                snapshot.feedback_received_at = datetime(
                    2026,
                    4,
                    30,
                    0,
                    0,
                    tzinfo=timezone.utc,
                )
            refreshed = service.refresh_plan_items(
                target_trade_date=self.target_trade_date,
                trigger='at_target_open_cutoff',
            )

        self.assertEqual(initial['summary']['eligible_count'], 1)
        item = next(
            row for row in refreshed['items'] if row['symbol'] == '005930'
        )
        self.assertEqual(item['eligibility_status'], ELIGIBILITY_BLOCKED)
        self.assertIn(
            'decision_snapshot_after_target_open',
            {
                reason['code']
                for reason in item['validation']['blocking_reasons']
            },
        )

    def test_scope_tombstones_late_latest_snapshot_without_older_fallback(
        self,
    ) -> None:
        older_snapshot_id = self._seed_snapshot(
            symbol='005930',
            feedback_received_at=datetime(
                2026,
                4,
                29,
                23,
                59,
                59,
                tzinfo=timezone.utc,
            ),
        )
        late_snapshot_id = self._seed_snapshot(
            symbol='005930',
            feedback_received_at=datetime(
                2026,
                4,
                30,
                0,
                0,
                tzinfo=timezone.utc,
            ),
        )

        state = self._service()._load_latest_decider_scope_state(
            source_market_date=self.market_date,
            target_trade_date=self.target_trade_date,
        )

        key = ('005930', 'signal-decider')
        self.assertIsNone(state['selected_by_key'][key])
        self.assertEqual(
            state['expired_snapshot_by_key'][key]['snapshot_id'],
            late_snapshot_id,
        )
        self.assertNotEqual(older_snapshot_id, late_snapshot_id)

    def test_refresh_stages_revocation_for_handed_off_late_snapshot(
        self,
    ) -> None:
        settings.signal_decider_exec_integration_mode = 'dispatch'
        snapshot_id = self._seed_snapshot(
            symbol='005930',
            feedback_received_at=datetime(
                2026,
                4,
                29,
                23,
                59,
                59,
                tzinfo=timezone.utc,
            ),
        )
        service = self._service(
            state='PREOPEN',
            server_time='2026-04-30T08:40:00+09:00',
        )
        with patch(
            'app.orders.next_open_execution.load_daily_bars_for_symbol_range',
            return_value=[
                {'trade_date': '2026-04-29', 'close_price': 50000}
            ],
        ):
            service.refresh_plan_items(
                target_trade_date=self.target_trade_date,
                trigger='before_target_open_cutoff',
            )
            self._mark_row_as_handed_off()
            with session_scope() as session:
                snapshot = session.get(
                    SignalDeciderDecisionSnapshot,
                    snapshot_id,
                )
                snapshot.feedback_received_at = datetime(
                    2026,
                    4,
                    30,
                    0,
                    0,
                    tzinfo=timezone.utc,
                )
            refreshed = service.refresh_plan_items(
                target_trade_date=self.target_trade_date,
                trigger='late_snapshot_after_handoff',
            )

        item = next(
            row for row in refreshed['items'] if row['symbol'] == '005930'
        )
        self.assertEqual(
            item['execution_status'],
            EXECUTION_STATUS_REVOCATION_PENDING,
        )
        self.assertEqual(
            item['submission']['revocation']['reason_code'],
            'decision_snapshot_after_target_open',
        )

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
        incomplete_context = {
            'schema_version': 'agent_context_completeness_v1',
            'source_market_date': self.market_date.isoformat(),
            'candidate_symbols': ['005930'],
            'candidate_set_basis': 'latest_signal_decider_snapshots',
            'status': 'incomplete',
            'cohort_status': 'incomplete',
            'has_incomplete_agent_context': True,
            'candidate_set_definitive': False,
            'latest_run': {
                **latest_run,
                'started_at': latest_run['started_at'].isoformat(),
                'completed_at': latest_run['completed_at'].isoformat(),
            },
            'failure_count': 1,
            'failure_groups': [
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

        with (
            patch.object(
                service,
                '_build_agent_context_completeness_context',
                return_value=incomplete_context,
            ),
            patch(
                'app.orders.next_open_execution.load_daily_bars_for_symbol_range',
                return_value=[{'trade_date': '2026-08-03', 'close_price': 50000}],
            ),
        ):
            result = service.refresh_plan_items(target_trade_date=self.target_trade_date, trigger='test')

        self.assertEqual(result['summary']['agent_context_completeness_status'], 'incomplete')
        self.assertTrue(result['summary']['agent_context_incomplete'])
        self.assertEqual(result['summary']['agent_context_failure_count'], 1)
        item = result['items'][0]
        completeness = item['agent_context_completeness']
        self.assertEqual(completeness['status'], 'incomplete')
        self.assertTrue(completeness['has_incomplete_agent_context'])
        self.assertEqual(completeness['latest_run']['run_id'], run_id)
        self.assertEqual(completeness['failure_groups'][0]['failure_kind'], 'openclaw_auth_rejected')
        blocking_codes = {
            reason['code'] for reason in item['validation']['blocking_reasons']
        }
        self.assertIn('agent_context_incomplete_candidate_set', blocking_codes)
        self.assertEqual(item['eligibility_status'], ELIGIBILITY_BLOCKED)
        self.assertEqual(item['traceability']['agent_context_completeness']['status'], 'incomplete')

    def test_refresh_plan_items_blocks_unverified_agent_context_candidate(self) -> None:
        self._seed_snapshot(symbol='005930')
        service = self._service(state='PREOPEN', server_time='2026-04-30T08:40:00+09:00')

        for status in ('unknown', 'unavailable', None):
            with (
                self.subTest(status=status),
                patch.object(
                    service,
                    '_build_agent_context_completeness_context',
                    return_value={
                        'schema_version': 'agent_context_completeness_v1',
                        'source_market_date': self.market_date.isoformat(),
                        'candidate_symbols': ['005930'],
                        'candidate_set_basis': 'latest_signal_decider_snapshots',
                        'status': status,
                        'has_incomplete_agent_context': None,
                        'reason': 'source_decision_cohort_missing',
                        'latest_run': None,
                        'failure_count': 0,
                    },
                ),
                patch(
                    'app.orders.next_open_execution.load_daily_bars_for_symbol_range',
                    return_value=[
                        {'trade_date': '2026-04-29', 'close_price': 50000}
                    ],
                ),
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
            self.assertIn('agent_context_candidate_set_unverified', block_codes)

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
            row
            for index in range(36)
            for row in _usable_candidate_pykrx_rows(f'{index + 1:06d}')
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
        target_short_symbols = {f'{index + 1:06d}' for index in range(36)}
        short_items = [
            item
            for item in result['items']
            if item['decision_agent_name'] == 'short-term-decider'
            and item['symbol'] in target_short_symbols
            and item['account_reference'] == self.account_reference
        ]
        self.assertEqual(len(short_items), 36)
        signal_item = next(
            item
            for item in result['items']
            if item['decision_agent_name'] == 'signal-decider'
            and item['symbol'] == '900001'
            and item['account_reference'] == self.account_reference
        )
        self.assertEqual(signal_item['eligibility_status'], ELIGIBILITY_BLOCKED)
        signal_block_codes = {reason['code'] for reason in signal_item['validation']['blocking_reasons']}
        self.assertIn('pykrx_enrichment_preflight_blocked', signal_block_codes)

        short_item = short_items[0]
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
        candidate_rows = _usable_candidate_pykrx_rows(
            '005930',
            categories=('sector_classification', 'short_selling'),
        )

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

        self.assertGreaterEqual(result['summary']['blocked_count'], 1)
        item = next(
            entry
            for entry in result['items']
            if entry['symbol'] == '005930'
            and entry['decision_agent_name'] == 'short-term-decider'
            and entry['account_reference'] == self.account_reference
        )
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
        candidate_rows = _usable_candidate_pykrx_rows(
            '005930',
            categories=('sector_classification', 'short_selling'),
        )

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

        target_calls = [
            call
            for call in load_candidate_rows.call_args_list
            if call.kwargs.get('symbols') == ['005930']
        ]
        self.assertEqual(len(target_calls), 3)
        self.assertEqual(target_calls[-1].kwargs['categories'], required_categories)
        repair_candidate_rows.assert_called_once()
        self.assertTrue(result['summary']['enrichment_preflight_allowed'])
        self.assertEqual(result['summary']['enrichment_preflight_status'], 'ready')
        self.assertGreaterEqual(result['summary']['blocked_count'], 1)
        item = next(
            entry
            for entry in result['items']
            if entry['symbol'] == '005930'
            and entry['decision_agent_name'] == 'short-term-decider'
            and entry['account_reference'] == self.account_reference
        )
        self.assertEqual(item['eligibility_status'], ELIGIBILITY_BLOCKED)
        self.assertEqual(item['validation']['candidate_pykrx_enrichment']['missing_categories'], ['investor_flow_symbol'])
        self.assertIn(
            'candidate_pykrx_enrichment_missing',
            {reason['code'] for reason in item['validation']['blocking_reasons']},
        )

    def test_refresh_plan_items_blocks_signal_candidate_missing_from_globally_ready_coverage(
        self,
    ) -> None:
        self._seed_snapshot(symbol='005930')
        service = self._service(state='PREOPEN', server_time='2026-04-30T08:40:00+09:00')
        required_categories = ['sector_classification', 'investor_flow_symbol', 'short_selling']
        ready_preflight = {
            'market_date': self.market_date.isoformat(),
            'status': 'ready',
            'allowed': True,
            'required_categories': required_categories,
            'failed_categories': [],
            'zero_coverage_categories': [],
            'warning_categories': [],
            'category_row_counts': {
                'sector_classification': 2762,
                'investor_flow_symbol': 2735,
                'short_selling': 2762,
            },
        }
        candidate_rows = _usable_candidate_pykrx_rows(
            '005930',
            categories=('sector_classification', 'short_selling'),
        )

        with (
            patch(
                'app.orders.next_open_execution.inspect_pykrx_enrichment_preflight',
                return_value=ready_preflight,
            ),
            patch(
                'app.orders.next_open_execution.load_daily_pykrx_enrichment_rows',
                return_value=candidate_rows,
            ) as load_candidate_rows,
            patch(
                'app.orders.next_open_execution.load_daily_bars_for_symbol_range',
                return_value=[{'trade_date': '2026-04-29', 'close_price': 50000}],
            ),
        ):
            result = service.refresh_plan_items(
                target_trade_date=self.target_trade_date,
                trigger='test',
            )

        self.assertEqual(load_candidate_rows.call_count, 2)
        self.assertTrue(result['summary']['enrichment_preflight_allowed'])
        self.assertGreaterEqual(result['summary']['blocked_count'], 1)
        item = next(
            entry
            for entry in result['items']
            if entry['symbol'] == '005930'
            and entry['decision_agent_name'] == 'signal-decider'
            and entry['account_reference'] == self.account_reference
        )
        self.assertEqual(
            item['validation']['candidate_pykrx_enrichment']['missing_categories'],
            ['investor_flow_symbol'],
        )
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

    def test_locked_active_policy_status_overrides_stale_draft_cache(self) -> None:
        self._seed_snapshot(symbol='005930')
        service = self._service(
            state='PREOPEN',
            server_time='2026-04-30T08:40:00+09:00',
        )

        with patch(
            'app.orders.next_open_execution.signal_decider_loop_service.get_policy_prompt_context',
            return_value={
                'status': 'draft',
                'rule_bundle': {
                    'schema_version': 'signal_decider_policy_bundle_v1'
                },
            },
        ), patch(
            'app.orders.next_open_execution.load_daily_bars_for_symbol_range',
            return_value=[
                {'trade_date': '2026-04-29', 'close_price': 50000}
            ],
        ):
            result = service.refresh_plan_items(
                target_trade_date=self.target_trade_date,
                trigger='stale_policy_cache',
            )

        self.assertEqual(
            result['items'][0]['validation']['policy_context']['status'],
            'active',
        )
        self.assertNotIn(
            'policy_not_active',
            {
                reason['code']
                for reason in result['items'][0]['validation'][
                    'blocking_reasons'
                ]
            },
        )

    def test_active_policy_rule_bundle_is_immutable_during_upsert(self) -> None:
        replacement_bundle = {
            'schema_version': 'signal_decider_policy_bundle_v1',
            'change_set': [{'rule': 'minimum_confidence', 'value': 9}],
        }

        SignalDeciderLoopRepository().bulk_upsert_policy_versions(
            items=[
                {
                    'policy_version': self.policy_version,
                    'status': 'candidate',
                    'parent_policy_version': 'unexpected-parent',
                    'source_candidate_version': 'unexpected-candidate',
                    'change_summary': 'Unexpected regenerated policy.',
                    'rule_bundle_payload': replacement_bundle,
                    'metadata_payload': {
                        'schema_version': 'signal_decider_policy_metadata_v1'
                    },
                    'created_by': 'test-regenerator',
                    'activated_by': None,
                    'activated_at': None,
                    'retired_at': None,
                    'created_at': datetime(
                        2026, 4, 29, 7, 0, tzinfo=timezone.utc
                    ),
                    'updated_at': datetime(
                        2026, 4, 29, 7, 0, tzinfo=timezone.utc
                    ),
                }
            ]
        )

        with session_scope() as session:
            policy = session.execute(
                select(SignalDeciderPolicyVersion).where(
                    SignalDeciderPolicyVersion.policy_version
                    == self.policy_version
                )
            ).scalar_one()
            self.assertEqual(policy.status, 'active')
            self.assertEqual(
                policy.change_summary,
                'Test active policy.',
            )
            self.assertEqual(
                policy.rule_bundle_payload,
                {'schema_version': 'signal_decider_policy_bundle_v1'},
            )

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

        with session_scope() as session:
            policy = session.execute(
                select(SignalDeciderPolicyVersion).where(
                    SignalDeciderPolicyVersion.policy_version
                    == self.policy_version
                )
            ).scalar_one()
            policy.status = 'retired'
            policy.retired_at = datetime(
                2026, 4, 29, 7, 0, tzinfo=timezone.utc
            )

        with patch(
            'app.orders.next_open_execution.load_daily_bars_for_symbol_range',
            return_value=[{'trade_date': '2026-04-29', 'close_price': 50000}],
        ), patch(
            'app.orders.next_open_execution.signal_decider_loop_service.get_policy_prompt_context',
            return_value={'status': 'active'},
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
        self.assertEqual(result['summary']['symbol_count'], 1)
        self.assertEqual(result['summary']['eligible_symbol_count'], 0)
        self.assertEqual(result['summary']['monitor_candidate_symbol_count'], 1)
        self.assertEqual(result['summary']['blocked_symbol_count'], 1)
        self.assertEqual(result['summary']['monitor_candidate_count'], 1)
        self.assertEqual(result['summary']['blocked_count'], 1)
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

        async def activate_dispatch(payload):
            return {
                'plan_id': payload['plan_id'],
                'execution_plan_id': payload['execution_plan_id'],
                'contract_ack': {
                    'schema_version': EXECUTION_ACTIVATION_SCHEMA_VERSION,
                    'contract_version': EXECUTION_HANDOFF_CONTRACT_VERSION,
                    'capability': EXECUTION_ACTIVATION_CAPABILITY,
                    'plan_id': payload['plan_id'],
                    'status': 'activated',
                    'dispatch_material_fingerprint': payload[
                        'dispatch_material_fingerprint'
                    ],
                    'authority_generation_fingerprint': payload[
                        'authority_generation_fingerprint'
                    ],
                },
                'activated_item_count': len(payload['items']),
                'idempotent_replay': False,
                'item_acks': [
                    {
                        **item,
                        'execution_plan_id': payload['execution_plan_id'],
                        'status': 'activated',
                    }
                    for item in payload['items']
                ],
            }

        exec_client.post_execution_plan_activation.side_effect = (
            activate_dispatch
        )
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

    def test_latest_snapshot_selection_ignores_reuse_only_retry_parent_scope(self) -> None:
        child_run = f'child-success-{uuid4().hex[:10]}'
        parent_run = f'retry-parent-{uuid4().hex[:10]}'
        self._seed_feedback_run(
            run_id=child_run,
            symbols=['005930'],
            roles=['signal-decider'],
            started_at=datetime(2026, 4, 29, 5, 0, tzinfo=timezone.utc),
        )
        snapshot_id = self._seed_snapshot(
            symbol='005930',
            decision_run_id=child_run,
        )
        self._seed_feedback_run(
            run_id=parent_run,
            symbols=['005930'],
            roles=['short-term-decider', 'signal-decider'],
            started_at=datetime(2026, 4, 29, 6, 0, tzinfo=timezone.utc),
            mode='daily_retry_cycle_role_sequenced',
        )

        selected = self._service()._load_latest_snapshots(
            source_market_date=self.market_date,
        )

        self.assertEqual(
            [
                snapshot.id
                for snapshot in selected
                if snapshot.symbol == '005930'
                and snapshot.agent_name == 'signal-decider'
            ],
            [snapshot_id],
        )

    def test_latest_snapshot_selection_tombstones_forced_retry_parent_scope(self) -> None:
        child_run = f'child-success-{uuid4().hex[:10]}'
        parent_run = f'forced-retry-parent-{uuid4().hex[:10]}'
        self._seed_feedback_run(
            run_id=child_run,
            symbols=['005930'],
            roles=['signal-decider'],
            started_at=datetime(2026, 4, 29, 5, 0, tzinfo=timezone.utc),
        )
        self._seed_snapshot(
            symbol='005930',
            decision_run_id=child_run,
        )
        self._seed_feedback_run(
            run_id=parent_run,
            symbols=['005930'],
            roles=['signal-decider'],
            started_at=datetime(2026, 4, 29, 6, 0, tzinfo=timezone.utc),
            mode='daily_retry_cycle_role_sequenced',
            force_recompute=True,
        )

        selected = self._service()._load_latest_snapshots(
            source_market_date=self.market_date,
        )

        self.assertFalse(
            any(
                snapshot.symbol == '005930'
                and snapshot.agent_name == 'signal-decider'
                for snapshot in selected
            )
        )

    def test_latest_snapshot_selection_uses_only_latest_parent_lineage(self) -> None:
        parent_a = f'parent-a-{uuid4().hex[:10]}'
        parent_b = f'parent-b-{uuid4().hex[:10]}'
        child_a = f'child-a-{uuid4().hex[:10]}'
        child_b = f'child-b-{uuid4().hex[:10]}'
        late_child_a = f'late-child-a-{uuid4().hex[:10]}'
        self._seed_feedback_run(
            run_id=parent_a,
            symbols=['005930'],
            roles=['short-term-decider', 'signal-decider'],
            started_at=datetime(2026, 4, 29, 4, 0, tzinfo=timezone.utc),
            mode='daily_retry_cycle_role_sequenced',
            child_run_lineage=True,
        )
        self._seed_feedback_run(
            run_id=child_a,
            symbols=['005930'],
            roles=['signal-decider'],
            started_at=datetime(2026, 4, 29, 4, 10, tzinfo=timezone.utc),
            parent_run_id=parent_a,
        )
        self._seed_snapshot(symbol='005930', decision_run_id=child_a)
        self._seed_feedback_run(
            run_id=parent_b,
            symbols=['005930'],
            roles=['short-term-decider', 'signal-decider'],
            started_at=datetime(2026, 4, 29, 5, 0, tzinfo=timezone.utc),
            mode='daily_retry_cycle_role_sequenced',
            child_run_lineage=True,
        )
        self._seed_feedback_run(
            run_id=child_b,
            symbols=['005930'],
            roles=['signal-decider'],
            started_at=datetime(2026, 4, 29, 5, 10, tzinfo=timezone.utc),
            parent_run_id=parent_b,
        )
        expected_snapshot_id = self._seed_snapshot(
            symbol='005930',
            decision_run_id=child_b,
        )
        self._seed_feedback_run(
            run_id=late_child_a,
            symbols=['005930'],
            roles=['signal-decider'],
            started_at=datetime(2026, 4, 29, 6, 0, tzinfo=timezone.utc),
            parent_run_id=parent_a,
        )
        self._seed_snapshot(symbol='005930', decision_run_id=late_child_a)

        selected = self._service()._load_latest_snapshots(
            source_market_date=self.market_date,
        )

        self.assertEqual(
            [
                snapshot.id
                for snapshot in selected
                if snapshot.symbol == '005930'
                and snapshot.agent_name == 'signal-decider'
            ],
            [expected_snapshot_id],
        )

    def test_latest_full_workflow_parent_supersedes_older_decider_only_parent(self) -> None:
        old_parent = f'old-decider-parent-{uuid4().hex[:8]}'
        old_child = f'old-decider-child-{uuid4().hex[:8]}'
        new_parent = f'new-full-parent-{uuid4().hex[:8]}'
        new_child = f'new-full-child-{uuid4().hex[:8]}'
        self._seed_feedback_run(
            run_id=old_parent,
            symbols=['005930'],
            roles=['short-term-decider', 'signal-decider'],
            started_at=datetime(2026, 4, 29, 4, 0, tzinfo=timezone.utc),
            mode='daily_retry_cycle_role_sequenced',
            child_run_lineage=True,
        )
        self._seed_feedback_run(
            run_id=old_child,
            symbols=['005930'],
            roles=['signal-decider'],
            started_at=datetime(2026, 4, 29, 4, 10, tzinfo=timezone.utc),
            parent_run_id=old_parent,
        )
        self._seed_snapshot(symbol='005930', decision_run_id=old_child)
        full_roles = [
            'eod-summarizer',
            'technical-summarizer',
            'fundamental-summarizer',
            'short-term-decider',
            'signal-decider',
        ]
        self._seed_feedback_run(
            run_id=new_parent,
            symbols=['005930'],
            roles=full_roles,
            started_at=datetime(2026, 4, 29, 5, 0, tzinfo=timezone.utc),
            mode='daily_retry_cycle_role_sequenced',
            child_run_lineage=True,
        )
        self._seed_feedback_run(
            run_id=new_child,
            symbols=['005930'],
            roles=['signal-decider'],
            started_at=datetime(2026, 4, 29, 5, 10, tzinfo=timezone.utc),
            parent_run_id=new_parent,
        )
        expected_snapshot_id = self._seed_snapshot(
            symbol='005930',
            decision_run_id=new_child,
        )

        state = self._service()._load_latest_decider_scope_state(
            source_market_date=self.market_date,
        )

        self.assertEqual(state['source_run']['run_id'], new_parent)
        self.assertEqual(
            state['selected_by_key'][('005930', 'signal-decider')].id,
            expected_snapshot_id,
        )

    def test_scope_state_projects_only_authority_run_metadata(self) -> None:
        parent_run_id = f'projected-parent-{uuid4().hex[:8]}'
        self._seed_feedback_run(
            run_id=parent_run_id,
            symbols=['005930'],
            roles=['short-term-decider', 'signal-decider'],
            started_at=datetime(2026, 4, 29, 5, 0, tzinfo=timezone.utc),
            mode='daily_retry_cycle_role_sequenced',
            child_run_lineage=True,
        )
        with session_scope() as session:
            row = session.execute(
                select(AgentContextFeedbackRun).where(
                    AgentContextFeedbackRun.run_id == parent_run_id
                )
            ).scalar_one()
            row.request_payload = {
                **dict(row.request_payload or {}),
                'reuse_existing': None,
                'large_unrelated_request_context': 'x' * 100_000,
            }
            row.summary_payload = {
                **dict(row.summary_payload or {}),
                'failed_symbols_by_role': {},
                'progress_summary': {
                    'failed_symbols_by_role': {},
                    'reused_symbols_by_role': {},
                    'reused_feedback_evidence_by_role': {},
                    'large_unrelated_progress_context': 'y' * 100_000,
                },
                'request': {
                    'symbols': ['005930'],
                    'requested_symbols': ['005930'],
                    'large_unrelated_summary_request_context': 'z' * 100_000,
                },
                'large_unrelated_summary_context': 'w' * 100_000,
            }

        statements: list[str] = []

        def record_statement(
            _connection,
            _cursor,
            statement,
            _parameters,
            _context,
            _executemany,
        ) -> None:
            if 'FROM agent_context_feedback_runs' in statement:
                statements.append(statement)

        event.listen(engine, 'before_cursor_execute', record_statement)
        try:
            state = self._service()._load_latest_decider_scope_state(
                source_market_date=self.market_date,
            )
        finally:
            event.remove(engine, 'before_cursor_execute', record_statement)

        projected = next(
            run
            for run in state['run_snapshots']
            if run['run_id'] == parent_run_id
        )
        self.assertEqual(
            projected['request_payload']['child_run_lineage'][
                'parent_run_id'
            ],
            parent_run_id,
        )
        self.assertNotIn(
            'large_unrelated_request_context',
            projected['request_payload'],
        )
        self.assertIn('reuse_existing', projected['request_payload'])
        self.assertIsNone(projected['request_payload']['reuse_existing'])
        self.assertNotIn(
            'large_unrelated_summary_context',
            projected['summary_payload'],
        )
        self.assertNotIn(
            'large_unrelated_progress_context',
            projected['summary_payload']['progress_summary'],
        )
        self.assertNotIn(
            'large_unrelated_summary_request_context',
            projected['summary_payload']['request'],
        )
        self.assertEqual(len(statements), 1)
        run_query = statements[0]
        self.assertEqual(
            run_query.count('agent_context_feedback_runs.request_payload'),
            1,
        )
        self.assertEqual(
            run_query.count('agent_context_feedback_runs.summary_payload'),
            1,
        )
        self.assertNotIn('json_typeof', run_query.lower())
        self.assertNotIn('->', run_query)

    def test_scope_state_preserves_legacy_summary_request_scope_fallback(
        self,
    ) -> None:
        parent_run_id = f'legacy-summary-scope-{uuid4().hex[:8]}'
        self._seed_feedback_run(
            run_id=parent_run_id,
            symbols=['005930'],
            roles=['short-term-decider', 'signal-decider'],
            started_at=datetime(2026, 4, 29, 5, 0, tzinfo=timezone.utc),
            mode='daily_retry_cycle_role_sequenced',
        )
        with session_scope() as session:
            row = session.execute(
                select(AgentContextFeedbackRun).where(
                    AgentContextFeedbackRun.run_id == parent_run_id
                )
            ).scalar_one()
            request_payload = dict(row.request_payload or {})
            request_payload.pop('role_symbol_scopes', None)
            request_payload.pop('symbols', None)
            request_payload.pop('requested_symbols', None)
            row.request_payload = request_payload
            row.summary_payload = {
                'request': {
                    'symbols': ['005930'],
                    'requested_symbols': ['005930'],
                }
            }

        state = self._service()._load_latest_decider_scope_state(
            source_market_date=self.market_date,
        )

        self.assertEqual(
            state['source_role_symbol_scopes'],
            {
                'short-term-decider': ['005930'],
                'signal-decider': ['005930'],
            },
        )

    def test_targeted_parent_overlays_only_its_symbols_on_latest_full_cohort(self) -> None:
        base_parent = f'base-parent-{uuid4().hex[:8]}'
        base_short = f'base-short-{uuid4().hex[:8]}'
        base_signal = f'base-signal-{uuid4().hex[:8]}'
        targeted_parent = f'targeted-parent-{uuid4().hex[:8]}'
        targeted_short = f'targeted-short-{uuid4().hex[:8]}'
        targeted_signal = f'targeted-signal-{uuid4().hex[:8]}'
        symbols = ['005930', '000660']
        self._seed_feedback_run(
            run_id=base_parent,
            symbols=symbols,
            roles=['short-term-decider', 'signal-decider'],
            started_at=datetime(2026, 4, 29, 4, 0, tzinfo=timezone.utc),
            mode='daily_retry_cycle_role_sequenced',
            child_run_lineage=True,
            scope='active_strategy_aware_prefilter_scoring',
        )
        self._seed_feedback_run(
            run_id=base_short,
            symbols=symbols,
            roles=['short-term-decider'],
            started_at=datetime(2026, 4, 29, 4, 10, tzinfo=timezone.utc),
            parent_run_id=base_parent,
        )
        self._seed_feedback_run(
            run_id=base_signal,
            symbols=symbols,
            roles=['signal-decider'],
            started_at=datetime(2026, 4, 29, 4, 11, tzinfo=timezone.utc),
            parent_run_id=base_parent,
        )
        base_short_b = self._seed_short_term_snapshot(
            symbol='000660', decision_run_id=base_short
        )
        base_signal_b = self._seed_snapshot(
            symbol='000660', decision_run_id=base_signal
        )
        self._seed_short_term_snapshot(symbol='005930', decision_run_id=base_short)
        self._seed_snapshot(symbol='005930', decision_run_id=base_signal)
        self._seed_feedback_run(
            run_id=targeted_parent,
            symbols=['005930'],
            roles=['short-term-decider', 'signal-decider'],
            started_at=datetime(2026, 4, 29, 5, 0, tzinfo=timezone.utc),
            mode='daily_retry_cycle_role_sequenced',
            child_run_lineage=True,
            scope='requested_symbols_scored_by_strategy_aware_prefilter',
        )
        self._seed_feedback_run(
            run_id=targeted_short,
            symbols=['005930'],
            roles=['short-term-decider'],
            started_at=datetime(2026, 4, 29, 5, 10, tzinfo=timezone.utc),
            parent_run_id=targeted_parent,
        )
        self._seed_feedback_run(
            run_id=targeted_signal,
            symbols=['005930'],
            roles=['signal-decider'],
            started_at=datetime(2026, 4, 29, 5, 11, tzinfo=timezone.utc),
            parent_run_id=targeted_parent,
        )
        targeted_short_a = self._seed_short_term_snapshot(
            symbol='005930', decision_run_id=targeted_short
        )
        targeted_signal_a = self._seed_snapshot(
            symbol='005930', decision_run_id=targeted_signal
        )

        service = self._service()
        scope_state = service._load_latest_decider_scope_state(
            source_market_date=self.market_date,
        )
        result = self.agent_context_from_state_patcher.temp_original(
            service,
            source_market_date=self.market_date,
            candidate_symbols=symbols,
            source_run=scope_state['source_run'],
            scope_state=scope_state,
        )

        self.assertEqual(scope_state['source_run']['run_id'], targeted_parent)
        self.assertEqual(
            scope_state['source_parent_run_id_by_key'][
                ('005930', 'signal-decider')
            ],
            targeted_parent,
        )
        self.assertEqual(
            scope_state['source_parent_run_id_by_key'][
                ('000660', 'signal-decider')
            ],
            base_parent,
        )
        self.assertEqual(
            scope_state['selected_by_key'][('005930', 'short-term-decider')].id,
            targeted_short_a,
        )
        self.assertEqual(
            scope_state['selected_by_key'][('005930', 'signal-decider')].id,
            targeted_signal_a,
        )
        self.assertEqual(
            scope_state['selected_by_key'][('000660', 'short-term-decider')].id,
            base_short_b,
        )
        self.assertEqual(
            scope_state['selected_by_key'][('000660', 'signal-decider')].id,
            base_signal_b,
        )
        self.assertEqual(
            result['_candidate_contexts_by_symbol']['005930']['status'],
            'complete',
        )
        self.assertEqual(
            result['_candidate_contexts_by_symbol']['000660']['status'],
            'complete',
        )

    def test_abandoned_targeted_parent_blocks_only_its_overlay_symbols(self) -> None:
        base_parent = f'base-parent-{uuid4().hex[:8]}'
        base_short = f'base-short-{uuid4().hex[:8]}'
        base_signal = f'base-signal-{uuid4().hex[:8]}'
        targeted_parent = f'abandoned-targeted-{uuid4().hex[:8]}'
        symbols = ['005930', '000660']
        self._seed_feedback_run(
            run_id=base_parent,
            symbols=symbols,
            roles=['short-term-decider', 'signal-decider'],
            started_at=datetime(2026, 4, 29, 4, 0, tzinfo=timezone.utc),
            mode='daily_retry_cycle_role_sequenced',
            child_run_lineage=True,
            scope='active_strategy_aware_prefilter_scoring',
        )
        self._seed_feedback_run(
            run_id=base_short,
            symbols=symbols,
            roles=['short-term-decider'],
            started_at=datetime(2026, 4, 29, 4, 10, tzinfo=timezone.utc),
            parent_run_id=base_parent,
        )
        self._seed_feedback_run(
            run_id=base_signal,
            symbols=symbols,
            roles=['signal-decider'],
            started_at=datetime(2026, 4, 29, 4, 11, tzinfo=timezone.utc),
            parent_run_id=base_parent,
        )
        for symbol in symbols:
            self._seed_short_term_snapshot(symbol=symbol, decision_run_id=base_short)
            self._seed_snapshot(symbol=symbol, decision_run_id=base_signal)
        self._seed_feedback_run(
            run_id=targeted_parent,
            symbols=['005930'],
            roles=['short-term-decider', 'signal-decider'],
            started_at=datetime(2026, 4, 29, 5, 0, tzinfo=timezone.utc),
            status='abandoned',
            completed_count=0,
            mode='daily_retry_cycle_role_sequenced',
            child_run_lineage=True,
            scope='requested_symbols_scored_by_strategy_aware_prefilter',
        )

        service = self._service()
        scope_state = service._load_latest_decider_scope_state(
            source_market_date=self.market_date,
        )
        result = self.agent_context_from_state_patcher.temp_original(
            service,
            source_market_date=self.market_date,
            candidate_symbols=symbols,
            source_run=scope_state['source_run'],
            scope_state=scope_state,
        )

        self.assertIsNone(
            scope_state['selected_by_key'][('005930', 'signal-decider')]
        )
        self.assertIsNotNone(
            scope_state['selected_by_key'][('000660', 'signal-decider')]
        )
        self.assertEqual(
            result['_candidate_contexts_by_symbol']['005930']['status'],
            'incomplete',
        )
        self.assertEqual(
            result['_candidate_contexts_by_symbol']['000660']['status'],
            'complete',
        )

    def test_single_role_retry_parent_overlays_only_that_decider_role(self) -> None:
        base_parent = f'base-parent-{uuid4().hex[:8]}'
        base_short = f'base-short-{uuid4().hex[:8]}'
        base_signal = f'base-signal-{uuid4().hex[:8]}'
        signal_parent = f'signal-parent-{uuid4().hex[:8]}'
        signal_child = f'signal-child-{uuid4().hex[:8]}'
        symbol = '005930'
        base_symbols = [symbol, '000660']
        self._seed_feedback_run(
            run_id=base_parent,
            symbols=base_symbols,
            roles=['short-term-decider', 'signal-decider'],
            started_at=datetime(2026, 4, 29, 4, 0, tzinfo=timezone.utc),
            mode='daily_retry_cycle_role_sequenced',
            child_run_lineage=True,
            scope='active_strategy_aware_prefilter_scoring',
        )
        self._seed_feedback_run(
            run_id=base_short,
            symbols=base_symbols,
            roles=['short-term-decider'],
            started_at=datetime(2026, 4, 29, 4, 10, tzinfo=timezone.utc),
            parent_run_id=base_parent,
        )
        self._seed_feedback_run(
            run_id=base_signal,
            symbols=base_symbols,
            roles=['signal-decider'],
            started_at=datetime(2026, 4, 29, 4, 11, tzinfo=timezone.utc),
            parent_run_id=base_parent,
        )
        base_short_snapshot = self._seed_short_term_snapshot(
            symbol=symbol,
            decision_run_id=base_short,
        )
        self._seed_snapshot(symbol=symbol, decision_run_id=base_signal)
        self._seed_short_term_snapshot(
            symbol='000660',
            decision_run_id=base_short,
        )
        self._seed_snapshot(symbol='000660', decision_run_id=base_signal)
        self._seed_feedback_run(
            run_id=signal_parent,
            symbols=[symbol],
            roles=['signal-decider'],
            started_at=datetime(2026, 4, 29, 5, 0, tzinfo=timezone.utc),
            mode='daily_retry_cycle_role_sequenced',
            child_run_lineage=True,
            scope='requested_symbols_scored_by_strategy_aware_prefilter',
        )
        self._seed_feedback_run(
            run_id=signal_child,
            symbols=[symbol],
            roles=['signal-decider'],
            started_at=datetime(2026, 4, 29, 5, 10, tzinfo=timezone.utc),
            parent_run_id=signal_parent,
        )
        new_signal_snapshot = self._seed_snapshot(
            symbol=symbol,
            decision_run_id=signal_child,
        )

        service = self._service()
        state = service._load_latest_decider_scope_state(
            source_market_date=self.market_date,
        )
        result = self.agent_context_from_state_patcher.temp_original(
            service,
            source_market_date=self.market_date,
            candidate_symbols=[symbol],
            source_run=state['source_run'],
            scope_state=state,
        )

        self.assertEqual(
            state['source_parent_run_id_by_key'][
                (symbol, 'signal-decider')
            ],
            signal_parent,
        )
        self.assertEqual(
            state['source_parent_run_id_by_key'][
                (symbol, 'short-term-decider')
            ],
            base_parent,
        )
        self.assertEqual(
            state['selected_by_key'][(symbol, 'signal-decider')].id,
            new_signal_snapshot,
        )
        self.assertEqual(
            state['selected_by_key'][(symbol, 'short-term-decider')].id,
            base_short_snapshot,
        )
        self.assertEqual(result['status'], 'complete')
        mixed_parent_fingerprint = result['authority_fingerprint']

        unrelated_parent = f'unrelated-parent-{uuid4().hex[:8]}'
        unrelated_short = f'unrelated-short-{uuid4().hex[:8]}'
        unrelated_signal = f'unrelated-signal-{uuid4().hex[:8]}'
        self._seed_feedback_run(
            run_id=unrelated_parent,
            symbols=['000660'],
            roles=['short-term-decider', 'signal-decider'],
            started_at=datetime(2026, 4, 29, 6, 0, tzinfo=timezone.utc),
            mode='daily_retry_cycle_role_sequenced',
            child_run_lineage=True,
            scope='requested_symbols_scored_by_strategy_aware_prefilter',
        )
        self._seed_feedback_run(
            run_id=unrelated_short,
            symbols=['000660'],
            roles=['short-term-decider'],
            started_at=datetime(2026, 4, 29, 6, 10, tzinfo=timezone.utc),
            parent_run_id=unrelated_parent,
        )
        self._seed_feedback_run(
            run_id=unrelated_signal,
            symbols=['000660'],
            roles=['signal-decider'],
            started_at=datetime(2026, 4, 29, 6, 11, tzinfo=timezone.utc),
            parent_run_id=unrelated_parent,
        )
        self._seed_short_term_snapshot(
            symbol='000660',
            decision_run_id=unrelated_short,
        )
        self._seed_snapshot(
            symbol='000660',
            decision_run_id=unrelated_signal,
        )
        after_unrelated_state = service._load_latest_decider_scope_state(
            source_market_date=self.market_date,
        )
        after_unrelated = self.agent_context_from_state_patcher.temp_original(
            service,
            source_market_date=self.market_date,
            candidate_symbols=[symbol],
            source_run=after_unrelated_state['source_run'],
            scope_state=after_unrelated_state,
        )
        self.assertEqual(
            after_unrelated['authority_fingerprint'],
            mixed_parent_fingerprint,
        )

    def test_targeted_retry_preserves_unrelated_terminal_buy_authority(self) -> None:
        settings.signal_decider_exec_integration_mode = 'dispatch'
        base_parent = f'base-parent-{uuid4().hex[:8]}'
        base_short = f'base-short-{uuid4().hex[:8]}'
        base_signal = f'base-signal-{uuid4().hex[:8]}'
        symbols = ['005930', '000660']
        self._seed_feedback_run(
            run_id=base_parent,
            symbols=symbols,
            roles=['short-term-decider', 'signal-decider'],
            started_at=datetime(2026, 4, 29, 4, 0, tzinfo=timezone.utc),
            mode='daily_retry_cycle_role_sequenced',
            child_run_lineage=True,
            scope='active_strategy_aware_prefilter_scoring',
        )
        self._seed_feedback_run(
            run_id=base_short,
            symbols=symbols,
            roles=['short-term-decider'],
            started_at=datetime(2026, 4, 29, 4, 10, tzinfo=timezone.utc),
            parent_run_id=base_parent,
        )
        self._seed_feedback_run(
            run_id=base_signal,
            symbols=symbols,
            roles=['signal-decider'],
            started_at=datetime(2026, 4, 29, 4, 11, tzinfo=timezone.utc),
            parent_run_id=base_parent,
        )
        for symbol in symbols:
            self._seed_short_term_snapshot(
                symbol=symbol,
                tactical_action='wait',
                decision_run_id=base_short,
            )
            self._seed_snapshot(symbol=symbol, decision_run_id=base_signal)
        service = self._service(
            state='PREOPEN',
            server_time='2026-04-30T08:40:00+09:00',
        )
        before_state = service._load_latest_decider_scope_state(
            source_market_date=self.market_date,
        )
        before_context = self.agent_context_from_state_patcher.temp_original(
            service,
            source_market_date=self.market_date,
            candidate_symbols=['005930'],
            source_run=before_state['source_run'],
            scope_state=before_state,
        )
        with patch(
            'app.orders.next_open_execution.load_daily_bars_for_symbol_range',
            return_value=[{'trade_date': '2026-04-29', 'close_price': 50000}],
        ):
            service.refresh_plan_items(
                target_trade_date=self.target_trade_date,
                trigger='base_cohort',
            )
        self._mark_row_as_handed_off(symbol='005930')

        targeted_parent = f'targeted-parent-{uuid4().hex[:8]}'
        targeted_short = f'targeted-short-{uuid4().hex[:8]}'
        targeted_signal = f'targeted-signal-{uuid4().hex[:8]}'
        self._seed_feedback_run(
            run_id=targeted_parent,
            symbols=['000660'],
            roles=['short-term-decider', 'signal-decider'],
            started_at=datetime(2026, 4, 29, 5, 0, tzinfo=timezone.utc),
            mode='daily_retry_cycle_role_sequenced',
            child_run_lineage=True,
            scope='requested_symbols_scored_by_strategy_aware_prefilter',
        )
        self._seed_feedback_run(
            run_id=targeted_short,
            symbols=['000660'],
            roles=['short-term-decider'],
            started_at=datetime(2026, 4, 29, 5, 10, tzinfo=timezone.utc),
            parent_run_id=targeted_parent,
        )
        self._seed_feedback_run(
            run_id=targeted_signal,
            symbols=['000660'],
            roles=['signal-decider'],
            started_at=datetime(2026, 4, 29, 5, 11, tzinfo=timezone.utc),
            parent_run_id=targeted_parent,
        )
        self._seed_short_term_snapshot(
            symbol='000660',
            tactical_action='wait',
            decision_run_id=targeted_short,
        )
        self._seed_snapshot(symbol='000660', decision_run_id=targeted_signal)
        after_state = service._load_latest_decider_scope_state(
            source_market_date=self.market_date,
        )
        after_context = self.agent_context_from_state_patcher.temp_original(
            service,
            source_market_date=self.market_date,
            candidate_symbols=['005930'],
            source_run=after_state['source_run'],
            scope_state=after_state,
        )

        self.assertEqual(
            after_context['authority_fingerprint'],
            before_context['authority_fingerprint'],
        )
        with patch(
            'app.orders.next_open_execution.load_daily_bars_for_symbol_range',
            return_value=[{'trade_date': '2026-04-29', 'close_price': 50000}],
        ):
            refreshed = service.refresh_plan_items(
                target_trade_date=self.target_trade_date,
                trigger='targeted_other_symbol',
            )
        unrelated = next(
            item
            for item in refreshed['items']
            if item['symbol'] == '005930'
            and item['decision_agent_name'] == 'signal-decider'
        )
        self.assertEqual(unrelated['execution_status'], EXECUTION_STATUS_SUBMITTED)
        self.assertNotIn('revocation', unrelated['submission'])

    def test_abandoned_newer_parent_blocks_older_buy_authority(self) -> None:
        old_parent = f'old-buy-parent-{uuid4().hex[:8]}'
        old_child = f'old-buy-child-{uuid4().hex[:8]}'
        abandoned_parent = f'abandoned-parent-{uuid4().hex[:8]}'
        self._seed_feedback_run(
            run_id=old_parent,
            symbols=['005930'],
            roles=['short-term-decider', 'signal-decider'],
            started_at=datetime(2026, 4, 29, 4, 0, tzinfo=timezone.utc),
            mode='daily_retry_cycle_role_sequenced',
            child_run_lineage=True,
        )
        self._seed_feedback_run(
            run_id=old_child,
            symbols=['005930'],
            roles=['signal-decider'],
            started_at=datetime(2026, 4, 29, 4, 10, tzinfo=timezone.utc),
            parent_run_id=old_parent,
        )
        self._seed_snapshot(symbol='005930', decision_run_id=old_child)
        self._seed_feedback_run(
            run_id=abandoned_parent,
            symbols=['005930'],
            roles=['short-term-decider', 'signal-decider'],
            started_at=datetime(2026, 4, 29, 5, 0, tzinfo=timezone.utc),
            status='abandoned',
            completed_count=0,
            failed_count=0,
            mode='daily_retry_cycle_role_sequenced',
            child_run_lineage=True,
        )

        state = self._service()._load_latest_decider_scope_state(
            source_market_date=self.market_date,
        )

        self.assertEqual(state['source_run']['run_id'], abandoned_parent)
        self.assertIsNone(
            state['selected_by_key'][('005930', 'signal-decider')],
        )
        self.assertNotIn(
            ('005930', 'signal-decider'),
            state['evidenced_scope_keys'],
        )

    def test_latest_snapshot_selection_preserves_explicit_child_reuse(self) -> None:
        old_run = f'old-child-{uuid4().hex[:10]}'
        parent_run = f'parent-{uuid4().hex[:10]}'
        reuse_child = f'reuse-child-{uuid4().hex[:10]}'
        self._seed_feedback_run(
            run_id=old_run,
            symbols=['005930'],
            roles=['signal-decider'],
            started_at=datetime(2026, 4, 29, 4, 0, tzinfo=timezone.utc),
        )
        expected_snapshot_id = self._seed_snapshot(
            symbol='005930',
            decision_run_id=old_run,
            feedback_received_at=datetime(2026, 4, 29, 4, 10, tzinfo=timezone.utc),
        )
        self._seed_feedback_run(
            run_id=parent_run,
            symbols=['005930'],
            roles=['short-term-decider', 'signal-decider'],
            started_at=datetime(2026, 4, 29, 5, 0, tzinfo=timezone.utc),
            mode='daily_retry_cycle_role_sequenced',
            child_run_lineage=True,
        )
        self._seed_feedback_run(
            run_id=reuse_child,
            symbols=['005930'],
            roles=['signal-decider'],
            started_at=datetime(2026, 4, 29, 5, 10, tzinfo=timezone.utc),
            parent_run_id=parent_run,
            reused_symbols_by_role={'signal-decider': ['005930']},
            reused_feedback_evidence_by_role=self._reuse_evidence(
                symbol='005930',
                feedback_run_id=old_run,
                feedback_received_at=datetime(
                    2026, 4, 29, 4, 10, tzinfo=timezone.utc
                ),
                decision_snapshot_id=expected_snapshot_id,
            ),
        )
        with session_scope() as session:
            row = session.execute(
                select(AgentContextFeedbackRun).where(
                    AgentContextFeedbackRun.run_id == reuse_child
                )
            ).scalar_one()
            request_payload = dict(row.request_payload or {})
            request_payload.pop('reuse_existing', None)
            row.request_payload = request_payload

        state = self._service()._load_latest_decider_scope_state(
            source_market_date=self.market_date,
        )

        self.assertEqual(
            state['selected_by_key'][('005930', 'signal-decider')].id,
            expected_snapshot_id,
        )
        self.assertEqual(
            state['authoritative_run_id_by_key'][('005930', 'signal-decider')],
            reuse_child,
        )
        self.assertIn(
            ('005930', 'signal-decider'),
            state['evidenced_scope_keys'],
        )

    def test_latest_snapshot_selection_reuses_prior_parent_child_snapshot(self) -> None:
        parent_a = f'parent-a-{uuid4().hex[:10]}'
        child_a = f'child-a-{uuid4().hex[:10]}'
        parent_b = f'parent-b-{uuid4().hex[:10]}'
        child_b = f'child-b-{uuid4().hex[:10]}'
        self._seed_feedback_run(
            run_id=parent_a,
            symbols=['005930'],
            roles=['short-term-decider', 'signal-decider'],
            started_at=datetime(2026, 4, 29, 4, 0, tzinfo=timezone.utc),
            mode='daily_retry_cycle_role_sequenced',
            child_run_lineage=True,
        )
        self._seed_feedback_run(
            run_id=child_a,
            symbols=['005930'],
            roles=['signal-decider'],
            started_at=datetime(2026, 4, 29, 4, 10, tzinfo=timezone.utc),
            parent_run_id=parent_a,
        )
        expected_snapshot_id = self._seed_snapshot(
            symbol='005930',
            decision_run_id=child_a,
            feedback_received_at=datetime(2026, 4, 29, 4, 20, tzinfo=timezone.utc),
        )
        self._seed_feedback_run(
            run_id=parent_b,
            symbols=['005930'],
            roles=['short-term-decider', 'signal-decider'],
            started_at=datetime(2026, 4, 29, 5, 0, tzinfo=timezone.utc),
            mode='daily_retry_cycle_role_sequenced',
            child_run_lineage=True,
        )
        self._seed_feedback_run(
            run_id=child_b,
            symbols=['005930'],
            roles=['signal-decider'],
            started_at=datetime(2026, 4, 29, 5, 10, tzinfo=timezone.utc),
            parent_run_id=parent_b,
            reused_symbols_by_role={'signal-decider': ['005930']},
            reused_feedback_evidence_by_role=self._reuse_evidence(
                symbol='005930',
                feedback_run_id=child_a,
                feedback_received_at=datetime(
                    2026, 4, 29, 4, 20, tzinfo=timezone.utc
                ),
                decision_snapshot_id=expected_snapshot_id,
            ),
        )

        state = self._service()._load_latest_decider_scope_state(
            source_market_date=self.market_date,
        )

        self.assertEqual(
            state['selected_by_key'][('005930', 'signal-decider')].id,
            expected_snapshot_id,
        )
        self.assertEqual(
            state['authoritative_run_id_by_key'][('005930', 'signal-decider')],
            child_b,
        )
        self.assertIn(
            ('005930', 'signal-decider'),
            state['evidenced_scope_keys'],
        )

    def test_exact_reuse_survives_identical_duplicate_feedback_receipt_update(self) -> None:
        symbol = f'D{uuid4().hex[:10].upper()}'
        source_run = f'duplicate-source-{uuid4().hex[:8]}'
        parent_run = f'duplicate-parent-{uuid4().hex[:8]}'
        snapshot_observed_at = datetime(
            2026, 4, 29, 4, 10, tzinfo=timezone.utc
        )
        self._seed_feedback_run(
            run_id=source_run,
            symbols=[symbol],
            roles=['signal-decider'],
            started_at=datetime(2026, 4, 29, 4, 0, tzinfo=timezone.utc),
        )
        expected_snapshot_id = self._seed_snapshot(
            symbol=symbol,
            decision_run_id=source_run,
            feedback_received_at=snapshot_observed_at,
        )
        self._seed_feedback_run(
            run_id=parent_run,
            symbols=[symbol],
            roles=['short-term-decider', 'signal-decider'],
            started_at=datetime(2026, 4, 29, 5, 0, tzinfo=timezone.utc),
            mode='daily_retry_cycle_role_sequenced',
            child_run_lineage=True,
            reused_symbols_by_role={'signal-decider': [symbol]},
            reused_feedback_evidence_by_role=self._reuse_evidence(
                symbol=symbol,
                feedback_run_id=source_run,
                feedback_received_at=datetime(
                    2026, 4, 29, 4, 30, tzinfo=timezone.utc
                ),
                decision_snapshot_id=expected_snapshot_id,
                decision_snapshot_observed_at=snapshot_observed_at,
            ),
        )

        state = self._service()._load_latest_decider_scope_state(
            source_market_date=self.market_date,
        )

        self.assertEqual(
            state['selected_by_key'][(symbol, 'signal-decider')].id,
            expected_snapshot_id,
        )
        self.assertIn(
            (symbol, 'signal-decider'),
            state['evidenced_scope_keys'],
        )

    def test_reuse_does_not_admit_slow_old_lineage_result_received_after_parent(self) -> None:
        parent_a = f'slow-parent-a-{uuid4().hex[:8]}'
        slow_child_a = f'slow-child-a-{uuid4().hex[:8]}'
        parent_b = f'slow-parent-b-{uuid4().hex[:8]}'
        reuse_child_b = f'reuse-child-b-{uuid4().hex[:8]}'
        self._seed_feedback_run(
            run_id=parent_a,
            symbols=['005930'],
            roles=['short-term-decider', 'signal-decider'],
            started_at=datetime(2026, 4, 29, 4, 0, tzinfo=timezone.utc),
            mode='daily_retry_cycle_role_sequenced',
            child_run_lineage=True,
        )
        self._seed_feedback_run(
            run_id=slow_child_a,
            symbols=['005930'],
            roles=['signal-decider'],
            started_at=datetime(2026, 4, 29, 4, 50, tzinfo=timezone.utc),
            parent_run_id=parent_a,
        )
        slow_snapshot_id = self._seed_snapshot(
            symbol='005930',
            decision_run_id=slow_child_a,
            feedback_received_at=datetime(2026, 4, 29, 5, 10, tzinfo=timezone.utc),
        )
        self._seed_feedback_run(
            run_id=parent_b,
            symbols=['005930'],
            roles=['short-term-decider', 'signal-decider'],
            started_at=datetime(2026, 4, 29, 5, 0, tzinfo=timezone.utc),
            mode='daily_retry_cycle_role_sequenced',
            child_run_lineage=True,
        )
        self._seed_feedback_run(
            run_id=reuse_child_b,
            symbols=['005930'],
            roles=['signal-decider'],
            started_at=datetime(2026, 4, 29, 5, 20, tzinfo=timezone.utc),
            parent_run_id=parent_b,
            reused_symbols_by_role={'signal-decider': ['005930']},
            reused_feedback_evidence_by_role=self._reuse_evidence(
                symbol='005930',
                feedback_run_id=slow_child_a,
                feedback_received_at=datetime(
                    2026, 4, 29, 5, 10, tzinfo=timezone.utc
                ),
                decision_snapshot_id=slow_snapshot_id,
            ),
        )

        state = self._service()._load_latest_decider_scope_state(
            source_market_date=self.market_date,
        )

        self.assertIsNone(
            state['selected_by_key'][('005930', 'signal-decider')],
        )
        self.assertNotIn(
            ('005930', 'signal-decider'),
            state['evidenced_scope_keys'],
        )

    def test_third_parent_reuse_keeps_exact_second_parent_authority_after_late_first_child(
        self,
    ) -> None:
        symbol = f'P{uuid4().hex[:10].upper()}'
        parent_one = f'parent-one-{uuid4().hex[:8]}'
        child_one = f'child-one-{uuid4().hex[:8]}'
        parent_two = f'parent-two-{uuid4().hex[:8]}'
        child_two = f'child-two-{uuid4().hex[:8]}'
        late_child_one = f'late-child-one-{uuid4().hex[:8]}'
        parent_three = f'parent-three-{uuid4().hex[:8]}'

        for run_id, started_at in (
            (parent_one, datetime(2026, 4, 29, 4, 0, tzinfo=timezone.utc)),
            (parent_two, datetime(2026, 4, 29, 5, 0, tzinfo=timezone.utc)),
        ):
            self._seed_feedback_run(
                run_id=run_id,
                symbols=[symbol],
                roles=['short-term-decider', 'signal-decider'],
                started_at=started_at,
                mode='daily_retry_cycle_role_sequenced',
                child_run_lineage=True,
            )
        self._seed_feedback_run(
            run_id=child_one,
            symbols=[symbol],
            roles=['signal-decider'],
            started_at=datetime(2026, 4, 29, 4, 10, tzinfo=timezone.utc),
            parent_run_id=parent_one,
        )
        child_one_snapshot_id = self._seed_snapshot(
            symbol=symbol,
            decision_run_id=child_one,
            feedback_received_at=datetime(2026, 4, 29, 4, 20, tzinfo=timezone.utc),
        )
        self._seed_feedback_run(
            run_id=child_two,
            symbols=[symbol],
            roles=['signal-decider'],
            started_at=datetime(2026, 4, 29, 5, 10, tzinfo=timezone.utc),
            parent_run_id=parent_two,
        )
        expected_snapshot_id = self._seed_snapshot(
            symbol=symbol,
            decision_run_id=child_two,
            feedback_received_at=datetime(2026, 4, 29, 5, 20, tzinfo=timezone.utc),
        )
        self._seed_feedback_run(
            run_id=late_child_one,
            symbols=[symbol],
            roles=['signal-decider'],
            started_at=datetime(2026, 4, 29, 6, 0, tzinfo=timezone.utc),
            parent_run_id=parent_one,
        )
        late_snapshot_id = self._seed_snapshot(
            symbol=symbol,
            decision_run_id=late_child_one,
            feedback_received_at=datetime(2026, 4, 29, 6, 10, tzinfo=timezone.utc),
        )
        self._seed_feedback_run(
            run_id=parent_three,
            symbols=[symbol],
            roles=['short-term-decider', 'signal-decider'],
            started_at=datetime(2026, 4, 29, 7, 0, tzinfo=timezone.utc),
            mode='daily_retry_cycle_role_sequenced',
            child_run_lineage=True,
            reused_symbols_by_role={'signal-decider': [symbol]},
            reused_feedback_evidence_by_role=self._reuse_evidence(
                symbol=symbol,
                feedback_run_id=child_two,
                feedback_received_at=datetime(
                    2026, 4, 29, 5, 20, tzinfo=timezone.utc
                ),
                decision_snapshot_id=expected_snapshot_id,
            ),
        )

        state = self._service()._load_latest_decider_scope_state(
            source_market_date=self.market_date,
        )
        selected = state['selected_by_key'][(symbol, 'signal-decider')]

        self.assertEqual(selected.id, expected_snapshot_id)
        self.assertNotEqual(selected.id, late_snapshot_id)
        self.assertEqual(
            state['authoritative_run_id_by_key'][(symbol, 'signal-decider')],
            parent_three,
        )

    def test_third_parent_reuse_rejects_evidence_from_older_parent_lineage(
        self,
    ) -> None:
        symbol = f'Q{uuid4().hex[:10].upper()}'
        parent_one = f'parent-one-{uuid4().hex[:8]}'
        child_one = f'child-one-{uuid4().hex[:8]}'
        parent_two = f'parent-two-{uuid4().hex[:8]}'
        parent_three = f'parent-three-{uuid4().hex[:8]}'
        self._seed_feedback_run(
            run_id=parent_one,
            symbols=[symbol],
            roles=['short-term-decider', 'signal-decider'],
            started_at=datetime(2026, 4, 29, 4, 0, tzinfo=timezone.utc),
            mode='daily_retry_cycle_role_sequenced',
            child_run_lineage=True,
        )
        self._seed_feedback_run(
            run_id=child_one,
            symbols=[symbol],
            roles=['signal-decider'],
            started_at=datetime(2026, 4, 29, 4, 10, tzinfo=timezone.utc),
            parent_run_id=parent_one,
        )
        child_one_snapshot_id = self._seed_snapshot(
            symbol=symbol,
            decision_run_id=child_one,
            feedback_received_at=datetime(2026, 4, 29, 4, 20, tzinfo=timezone.utc),
        )
        self._seed_feedback_run(
            run_id=parent_two,
            symbols=[symbol],
            roles=['short-term-decider', 'signal-decider'],
            started_at=datetime(2026, 4, 29, 5, 0, tzinfo=timezone.utc),
            mode='daily_retry_cycle_role_sequenced',
            child_run_lineage=True,
        )
        self._seed_feedback_run(
            run_id=parent_three,
            symbols=[symbol],
            roles=['short-term-decider', 'signal-decider'],
            started_at=datetime(2026, 4, 29, 7, 0, tzinfo=timezone.utc),
            mode='daily_retry_cycle_role_sequenced',
            child_run_lineage=True,
            reused_symbols_by_role={'signal-decider': [symbol]},
            reused_feedback_evidence_by_role=self._reuse_evidence(
                symbol=symbol,
                feedback_run_id=child_one,
                feedback_received_at=datetime(
                    2026, 4, 29, 4, 20, tzinfo=timezone.utc
                ),
                decision_snapshot_id=child_one_snapshot_id,
            ),
        )

        state = self._service()._load_latest_decider_scope_state(
            source_market_date=self.market_date,
        )

        self.assertIsNone(
            state['selected_by_key'][(symbol, 'signal-decider')]
        )
        self.assertNotIn(
            (symbol, 'signal-decider'),
            state['evidenced_scope_keys'],
        )

    def test_reuse_rejects_late_older_sibling_from_immediate_parent(self) -> None:
        symbol = f'S{uuid4().hex[:10].upper()}'
        parent_two = f'parent-two-{uuid4().hex[:8]}'
        child_a = f'child-a-{uuid4().hex[:8]}'
        child_b = f'child-b-{uuid4().hex[:8]}'
        parent_three = f'parent-three-{uuid4().hex[:8]}'
        self._seed_feedback_run(
            run_id=parent_two,
            symbols=[symbol],
            roles=['short-term-decider', 'signal-decider'],
            started_at=datetime(2026, 4, 29, 5, 0, tzinfo=timezone.utc),
            mode='daily_retry_cycle_role_sequenced',
            child_run_lineage=True,
        )
        self._seed_feedback_run(
            run_id=child_a,
            symbols=[symbol],
            roles=['signal-decider'],
            started_at=datetime(2026, 4, 29, 5, 10, tzinfo=timezone.utc),
            parent_run_id=parent_two,
        )
        self._seed_feedback_run(
            run_id=child_b,
            symbols=[symbol],
            roles=['signal-decider'],
            started_at=datetime(2026, 4, 29, 5, 20, tzinfo=timezone.utc),
            parent_run_id=parent_two,
        )
        self._seed_snapshot(
            symbol=symbol,
            decision_run_id=child_b,
            feedback_received_at=datetime(2026, 4, 29, 5, 25, tzinfo=timezone.utc),
        )
        child_a_snapshot_id = self._seed_snapshot(
            symbol=symbol,
            decision_run_id=child_a,
            feedback_received_at=datetime(2026, 4, 29, 5, 30, tzinfo=timezone.utc),
        )
        self._seed_feedback_run(
            run_id=parent_three,
            symbols=[symbol],
            roles=['short-term-decider', 'signal-decider'],
            started_at=datetime(2026, 4, 29, 7, 0, tzinfo=timezone.utc),
            mode='daily_retry_cycle_role_sequenced',
            child_run_lineage=True,
            reused_symbols_by_role={'signal-decider': [symbol]},
            reused_feedback_evidence_by_role=self._reuse_evidence(
                symbol=symbol,
                feedback_run_id=child_a,
                feedback_received_at=datetime(
                    2026, 4, 29, 5, 30, tzinfo=timezone.utc
                ),
                decision_snapshot_id=child_a_snapshot_id,
            ),
        )

        state = self._service()._load_latest_decider_scope_state(
            source_market_date=self.market_date,
        )

        self.assertIsNone(
            state['selected_by_key'][(symbol, 'signal-decider')]
        )
        self.assertNotIn(
            (symbol, 'signal-decider'),
            state['evidenced_scope_keys'],
        )

    def test_newer_abandoned_sibling_blocks_older_success_from_same_parent(
        self,
    ) -> None:
        symbol = f'A{uuid4().hex[:10].upper()}'
        parent_run = f'parent-{uuid4().hex[:8]}'
        older_child = f'older-child-{uuid4().hex[:8]}'
        abandoned_child = f'abandoned-child-{uuid4().hex[:8]}'
        self._seed_feedback_run(
            run_id=parent_run,
            symbols=[symbol],
            roles=['short-term-decider', 'signal-decider'],
            started_at=datetime(2026, 4, 29, 5, 0, tzinfo=timezone.utc),
            mode='daily_retry_cycle_role_sequenced',
            child_run_lineage=True,
        )
        self._seed_feedback_run(
            run_id=older_child,
            symbols=[symbol],
            roles=['signal-decider'],
            started_at=datetime(2026, 4, 29, 5, 10, tzinfo=timezone.utc),
            parent_run_id=parent_run,
        )
        self._seed_snapshot(
            symbol=symbol,
            decision_run_id=older_child,
            feedback_received_at=datetime(
                2026, 4, 29, 5, 11, tzinfo=timezone.utc
            ),
        )
        self._seed_feedback_run(
            run_id=abandoned_child,
            symbols=[symbol],
            roles=['signal-decider'],
            started_at=datetime(2026, 4, 29, 5, 20, tzinfo=timezone.utc),
            status='abandoned',
            completed_count=0,
            parent_run_id=parent_run,
        )

        state = self._service()._load_latest_decider_scope_state(
            source_market_date=self.market_date,
        )

        key = (symbol, 'signal-decider')
        self.assertIsNone(state['selected_by_key'][key])
        self.assertEqual(
            state['authoritative_run_id_by_key'][key],
            abandoned_child,
        )
        self.assertNotIn(key, state['evidenced_scope_keys'])

    def test_newer_running_child_with_snapshot_blocks_older_terminal_authority(
        self,
    ) -> None:
        symbol = f'R{uuid4().hex[:10].upper()}'
        parent_run = f'parent-{uuid4().hex[:8]}'
        older_child = f'older-child-{uuid4().hex[:8]}'
        running_child = f'running-child-{uuid4().hex[:8]}'
        self._seed_feedback_run(
            run_id=parent_run,
            symbols=[symbol],
            roles=['short-term-decider', 'signal-decider'],
            started_at=datetime(2026, 4, 29, 5, 0, tzinfo=timezone.utc),
            mode='daily_retry_cycle_role_sequenced',
            child_run_lineage=True,
        )
        self._seed_feedback_run(
            run_id=older_child,
            symbols=[symbol],
            roles=['signal-decider'],
            started_at=datetime(2026, 4, 29, 5, 10, tzinfo=timezone.utc),
            parent_run_id=parent_run,
        )
        self._seed_snapshot(
            symbol=symbol,
            decision_run_id=older_child,
            feedback_received_at=datetime(
                2026, 4, 29, 5, 11, tzinfo=timezone.utc
            ),
        )
        self._seed_feedback_run(
            run_id=running_child,
            symbols=[symbol],
            roles=['signal-decider'],
            started_at=datetime(2026, 4, 29, 5, 20, tzinfo=timezone.utc),
            status='running',
            completed_count=0,
            parent_run_id=parent_run,
        )
        self._seed_snapshot(
            symbol=symbol,
            decision_run_id=running_child,
            feedback_received_at=datetime(
                2026, 4, 29, 5, 21, tzinfo=timezone.utc
            ),
        )

        state = self._service()._load_latest_decider_scope_state(
            source_market_date=self.market_date,
        )

        key = (symbol, 'signal-decider')
        self.assertIsNone(state['selected_by_key'][key])
        self.assertEqual(
            state['authoritative_run_id_by_key'][key],
            running_child,
        )
        self.assertNotIn(key, state['evidenced_scope_keys'])

    def test_equal_parent_start_time_uses_row_id_to_find_running_barrier(
        self,
    ) -> None:
        symbol = f'T{uuid4().hex[:10].upper()}'
        older_parent = f'equal-parent-a-{uuid4().hex[:8]}'
        newer_parent = f'equal-parent-b-{uuid4().hex[:8]}'
        shared_started_at = datetime(
            2026, 4, 29, 5, 0, tzinfo=timezone.utc
        )
        self._seed_feedback_run(
            run_id=older_parent,
            symbols=[symbol],
            roles=['short-term-decider', 'signal-decider'],
            started_at=shared_started_at,
            status='running',
            completed_count=0,
            mode='daily_retry_cycle_role_sequenced',
            child_run_lineage=True,
        )
        snapshot_observed_at = datetime(
            2026, 4, 29, 4, 59, tzinfo=timezone.utc
        )
        older_snapshot_id = self._seed_snapshot(
            symbol=symbol,
            decision_run_id=older_parent,
            feedback_received_at=snapshot_observed_at,
        )
        self._seed_feedback_run(
            run_id=newer_parent,
            symbols=[symbol],
            roles=['short-term-decider', 'signal-decider'],
            started_at=shared_started_at,
            mode='daily_retry_cycle_role_sequenced',
            child_run_lineage=True,
            reused_symbols_by_role={'signal-decider': [symbol]},
            reused_feedback_evidence_by_role=self._reuse_evidence(
                symbol=symbol,
                feedback_run_id=older_parent,
                feedback_received_at=snapshot_observed_at,
                decision_snapshot_id=older_snapshot_id,
            ),
        )

        state = self._service()._load_latest_decider_scope_state(
            source_market_date=self.market_date,
        )

        key = (symbol, 'signal-decider')
        self.assertEqual(state['source_run']['run_id'], newer_parent)
        self.assertIsNone(state['selected_by_key'][key])
        self.assertNotIn(key, state['evidenced_scope_keys'])

    def test_reuse_uses_snapshot_created_at_when_feedback_receipt_is_missing(self) -> None:
        symbol = f'N{uuid4().hex[:10].upper()}'
        old_parent = f'null-receipt-parent-a-{uuid4().hex[:8]}'
        slow_child = f'null-receipt-child-a-{uuid4().hex[:8]}'
        new_parent = f'null-receipt-parent-b-{uuid4().hex[:8]}'
        reuse_child = f'null-receipt-child-b-{uuid4().hex[:8]}'
        self._seed_feedback_run(
            run_id=old_parent,
            symbols=[symbol],
            roles=['short-term-decider', 'signal-decider'],
            started_at=datetime(2026, 4, 29, 4, 0, tzinfo=timezone.utc),
            mode='daily_retry_cycle_role_sequenced',
            child_run_lineage=True,
        )
        self._seed_feedback_run(
            run_id=slow_child,
            symbols=[symbol],
            roles=['signal-decider'],
            started_at=datetime(2026, 4, 29, 4, 50, tzinfo=timezone.utc),
            parent_run_id=old_parent,
        )
        snapshot_id = self._seed_snapshot(
            symbol=symbol,
            decision_run_id=slow_child,
        )
        with session_scope() as session:
            snapshot = session.get(SignalDeciderDecisionSnapshot, snapshot_id)
            snapshot.feedback_received_at = None
            snapshot.created_at = datetime(
                2026, 4, 29, 5, 10, tzinfo=timezone.utc
            )
        self._seed_feedback_run(
            run_id=new_parent,
            symbols=[symbol],
            roles=['short-term-decider', 'signal-decider'],
            started_at=datetime(2026, 4, 29, 5, 0, tzinfo=timezone.utc),
            mode='daily_retry_cycle_role_sequenced',
            child_run_lineage=True,
        )
        self._seed_feedback_run(
            run_id=reuse_child,
            symbols=[symbol],
            roles=['signal-decider'],
            started_at=datetime(2026, 4, 29, 5, 20, tzinfo=timezone.utc),
            parent_run_id=new_parent,
            reused_symbols_by_role={'signal-decider': [symbol]},
        )

        state = self._service()._load_latest_decider_scope_state(
            source_market_date=self.market_date,
        )

        self.assertIsNone(
            state['selected_by_key'][(symbol, 'signal-decider')]
        )
        self.assertNotIn(
            (symbol, 'signal-decider'),
            state['evidenced_scope_keys'],
        )

    def test_late_old_failure_does_not_change_current_parent_authority(self) -> None:
        symbol = f'F{uuid4().hex[:10].upper()}'
        parent_run = f'current-parent-{uuid4().hex[:8]}'
        short_child = f'current-short-{uuid4().hex[:8]}'
        signal_child = f'current-signal-{uuid4().hex[:8]}'
        self._seed_feedback_run(
            run_id=parent_run,
            symbols=[symbol],
            roles=['short-term-decider', 'signal-decider'],
            started_at=datetime(2026, 4, 29, 5, 0, tzinfo=timezone.utc),
            mode='daily_retry_cycle_role_sequenced',
            child_run_lineage=True,
        )
        self._seed_feedback_run(
            run_id=short_child,
            symbols=[symbol],
            roles=['short-term-decider'],
            started_at=datetime(2026, 4, 29, 5, 10, tzinfo=timezone.utc),
            parent_run_id=parent_run,
        )
        self._seed_feedback_run(
            run_id=signal_child,
            symbols=[symbol],
            roles=['signal-decider'],
            started_at=datetime(2026, 4, 29, 5, 11, tzinfo=timezone.utc),
            parent_run_id=parent_run,
        )
        self._seed_short_term_snapshot(symbol=symbol, decision_run_id=short_child)
        self._seed_snapshot(symbol=symbol, decision_run_id=signal_child)
        service = self._service()
        before_state = service._load_latest_decider_scope_state(
            source_market_date=self.market_date,
        )
        before = self.agent_context_from_state_patcher.temp_original(
            service,
            source_market_date=self.market_date,
            candidate_symbols=[symbol],
            source_run=before_state['source_run'],
            scope_state=before_state,
        )
        self.feedback_failure_symbols.append(symbol)
        with session_scope() as session:
            session.add(
                AgentContextFeedbackFailure(
                    market_date=self.market_date,
                    symbol=symbol,
                    agent_name='signal-decider',
                    run_id=f'older-lineage-{uuid4().hex[:8]}',
                    failure_kind='dispatch_failed',
                    retryable=True,
                    summary_payload={},
                    context_payload={},
                    received_at=datetime(
                        2026, 4, 29, 7, 0, tzinfo=timezone.utc
                    ),
                )
            )
        after_state = service._load_latest_decider_scope_state(
            source_market_date=self.market_date,
        )
        after = self.agent_context_from_state_patcher.temp_original(
            service,
            source_market_date=self.market_date,
            candidate_symbols=[symbol],
            source_run=after_state['source_run'],
            scope_state=after_state,
        )

        self.assertEqual(after['status'], 'complete')
        self.assertEqual(after['failure_count'], 0)
        self.assertEqual(
            after['authority_fingerprint'],
            before['authority_fingerprint'],
        )

    def test_later_failure_in_same_child_run_invalidates_snapshot(self) -> None:
        symbol = f'F{uuid4().hex[:10].upper()}'
        parent_run = f'failure-parent-{uuid4().hex[:8]}'
        short_child = f'failure-short-{uuid4().hex[:8]}'
        signal_child = f'failure-signal-{uuid4().hex[:8]}'
        self._seed_feedback_run(
            run_id=parent_run,
            symbols=[symbol],
            roles=['short-term-decider', 'signal-decider'],
            started_at=datetime(2026, 4, 29, 5, 0, tzinfo=timezone.utc),
            mode='daily_retry_cycle_role_sequenced',
            child_run_lineage=True,
        )
        self._seed_feedback_run(
            run_id=short_child,
            symbols=[symbol],
            roles=['short-term-decider'],
            started_at=datetime(2026, 4, 29, 5, 10, tzinfo=timezone.utc),
            parent_run_id=parent_run,
        )
        self._seed_feedback_run(
            run_id=signal_child,
            symbols=[symbol],
            roles=['signal-decider'],
            started_at=datetime(2026, 4, 29, 5, 11, tzinfo=timezone.utc),
            parent_run_id=parent_run,
        )
        self._seed_short_term_snapshot(
            symbol=symbol,
            decision_run_id=short_child,
            feedback_received_at=datetime(
                2026, 4, 29, 6, 0, tzinfo=timezone.utc
            ),
        )
        self._seed_snapshot(
            symbol=symbol,
            decision_run_id=signal_child,
            feedback_received_at=datetime(
                2026, 4, 29, 6, 0, tzinfo=timezone.utc
            ),
        )
        self.feedback_failure_symbols.append(symbol)
        with session_scope() as session:
            session.add(
                AgentContextFeedbackFailure(
                    market_date=self.market_date,
                    symbol=symbol,
                    agent_name='signal-decider',
                    run_id=signal_child,
                    failure_kind='upstream_changed_during_dispatch',
                    retryable=True,
                    summary_payload={},
                    context_payload={},
                    received_at=datetime(
                        2026, 4, 29, 6, 1, tzinfo=timezone.utc
                    ),
                )
            )

        service = self._service()
        scope_state = service._load_latest_decider_scope_state(
            source_market_date=self.market_date,
        )
        completeness = self.agent_context_from_state_patcher.temp_original(
            service,
            source_market_date=self.market_date,
            candidate_symbols=[symbol],
            source_run=scope_state['source_run'],
            scope_state=scope_state,
        )

        key = (symbol, 'signal-decider')
        self.assertIsNone(scope_state['selected_by_key'][key])
        self.assertNotIn(key, scope_state['evidenced_scope_keys'])
        self.assertEqual(completeness['status'], 'incomplete')
        self.assertEqual(completeness['failure_count'], 1)

    def test_failed_child_run_prevents_snapshot_resurrection_after_late_old_failure(
        self,
    ) -> None:
        symbol = f'F{uuid4().hex[:10].upper()}'
        old_parent = f'old-parent-{uuid4().hex[:8]}'
        old_child = f'old-child-{uuid4().hex[:8]}'
        current_parent = f'current-parent-{uuid4().hex[:8]}'
        current_child = f'current-child-{uuid4().hex[:8]}'
        self._seed_feedback_run(
            run_id=old_parent,
            symbols=[symbol],
            roles=['signal-decider'],
            started_at=datetime(2026, 4, 29, 4, 0, tzinfo=timezone.utc),
            mode='daily_retry_cycle_role_sequenced',
            child_run_lineage=True,
        )
        self._seed_feedback_run(
            run_id=old_child,
            symbols=[symbol],
            roles=['signal-decider'],
            started_at=datetime(2026, 4, 29, 4, 10, tzinfo=timezone.utc),
            status='failed',
            completed_count=0,
            failed_count=1,
            parent_run_id=old_parent,
        )
        self._seed_feedback_run(
            run_id=current_parent,
            symbols=[symbol],
            roles=['signal-decider'],
            started_at=datetime(2026, 4, 29, 5, 0, tzinfo=timezone.utc),
            mode='daily_retry_cycle_role_sequenced',
            child_run_lineage=True,
        )
        self._seed_feedback_run(
            run_id=current_child,
            symbols=[symbol],
            roles=['signal-decider'],
            started_at=datetime(2026, 4, 29, 5, 10, tzinfo=timezone.utc),
            status='completed_with_failures',
            completed_count=0,
            failed_count=1,
            parent_run_id=current_parent,
        )
        self._seed_snapshot(
            symbol=symbol,
            decision_run_id=current_child,
            feedback_received_at=datetime(
                2026, 4, 29, 6, 0, tzinfo=timezone.utc
            ),
        )
        self.feedback_failure_symbols.append(symbol)
        with session_scope() as session:
            session.add(
                AgentContextFeedbackFailure(
                    market_date=self.market_date,
                    symbol=symbol,
                    agent_name='signal-decider',
                    run_id=current_child,
                    failure_kind='upstream_changed_during_dispatch',
                    retryable=True,
                    summary_payload={},
                    context_payload={},
                    received_at=datetime(
                        2026, 4, 29, 6, 1, tzinfo=timezone.utc
                    ),
                )
            )

        service = self._service()
        key = (symbol, 'signal-decider')
        before = service._load_latest_decider_scope_state(
            source_market_date=self.market_date,
        )
        self.assertIsNone(before['selected_by_key'][key])

        with session_scope() as session:
            failure = session.execute(
                select(AgentContextFeedbackFailure).where(
                    AgentContextFeedbackFailure.market_date
                    == self.market_date,
                    AgentContextFeedbackFailure.symbol == symbol,
                    AgentContextFeedbackFailure.agent_name
                    == 'signal-decider',
                )
            ).scalar_one()
            failure.run_id = old_child
            failure.received_at = datetime(
                2026, 4, 29, 6, 2, tzinfo=timezone.utc
            )

        after = service._load_latest_decider_scope_state(
            source_market_date=self.market_date,
        )

        self.assertIsNone(after['selected_by_key'][key])
        self.assertNotIn(key, after['evidenced_scope_keys'])

    def test_partial_lineage_parent_does_not_accept_unattested_old_snapshots(self) -> None:
        symbol = f'P{uuid4().hex[:10].upper()}'
        old_short = f'old-short-{uuid4().hex[:8]}'
        old_signal = f'old-signal-{uuid4().hex[:8]}'
        parent_run = f'partial-parent-{uuid4().hex[:8]}'
        self._seed_feedback_run(
            run_id=old_short,
            symbols=[symbol],
            roles=['short-term-decider'],
            started_at=datetime(2026, 4, 29, 4, 0, tzinfo=timezone.utc),
        )
        self._seed_short_term_snapshot(symbol=symbol, decision_run_id=old_short)
        self._seed_feedback_run(
            run_id=old_signal,
            symbols=[symbol],
            roles=['signal-decider'],
            started_at=datetime(2026, 4, 29, 4, 1, tzinfo=timezone.utc),
        )
        self._seed_snapshot(symbol=symbol, decision_run_id=old_signal)
        self._seed_feedback_run(
            run_id=parent_run,
            symbols=[symbol],
            roles=['short-term-decider', 'signal-decider'],
            started_at=datetime(2026, 4, 29, 5, 0, tzinfo=timezone.utc),
            status='partial',
            completed_count=0,
            failed_count=2,
            mode='daily_retry_cycle_role_sequenced',
            child_run_lineage=True,
        )

        service = self._service()
        scope_state = service._load_latest_decider_scope_state(
            source_market_date=self.market_date,
        )
        real_builder = self.agent_context_from_state_patcher.temp_original
        result = real_builder(
            service,
            source_market_date=self.market_date,
            candidate_symbols=[symbol],
            source_run=scope_state['source_run'],
            scope_state=scope_state,
        )

        self.assertEqual(result['status'], 'incomplete')
        self.assertEqual(result['completed_scope_count'], 0)
        self.assertEqual(result['pending_scope_count'], 2)

    def test_legacy_partial_parent_does_not_accept_unattested_old_snapshots(self) -> None:
        symbol = f'L{uuid4().hex[:10].upper()}'
        old_short = f'legacy-short-{uuid4().hex[:8]}'
        old_signal = f'legacy-signal-{uuid4().hex[:8]}'
        parent_run = f'legacy-partial-{uuid4().hex[:8]}'
        self._seed_feedback_run(
            run_id=old_short,
            symbols=[symbol],
            roles=['short-term-decider'],
            started_at=datetime(2026, 4, 29, 4, 0, tzinfo=timezone.utc),
        )
        self._seed_short_term_snapshot(symbol=symbol, decision_run_id=old_short)
        self._seed_feedback_run(
            run_id=old_signal,
            symbols=[symbol],
            roles=['signal-decider'],
            started_at=datetime(2026, 4, 29, 4, 1, tzinfo=timezone.utc),
        )
        self._seed_snapshot(symbol=symbol, decision_run_id=old_signal)
        self._seed_feedback_run(
            run_id=parent_run,
            symbols=[symbol],
            roles=['short-term-decider', 'signal-decider'],
            started_at=datetime(2026, 4, 29, 5, 0, tzinfo=timezone.utc),
            status='partial',
            completed_count=0,
            failed_count=2,
            mode='daily_retry_cycle_role_sequenced',
        )
        service = self._service()
        scope_state = service._load_latest_decider_scope_state(
            source_market_date=self.market_date,
        )

        result = self.agent_context_from_state_patcher.temp_original(
            service,
            source_market_date=self.market_date,
            candidate_symbols=[symbol],
            source_run=scope_state['source_run'],
            scope_state=scope_state,
        )

        self.assertFalse(scope_state['strict_source_lineage'])
        self.assertEqual(result['status'], 'incomplete')
        self.assertEqual(result['completed_scope_count'], 0)
        self.assertEqual(result['pending_scope_count'], 2)

    def test_legacy_completed_parent_does_not_accept_unattested_old_snapshots(self) -> None:
        symbol = f'C{uuid4().hex[:10].upper()}'
        old_short = f'legacy-complete-short-{uuid4().hex[:8]}'
        old_signal = f'legacy-complete-signal-{uuid4().hex[:8]}'
        parent_run = f'legacy-complete-parent-{uuid4().hex[:8]}'
        self._seed_feedback_run(
            run_id=old_short,
            symbols=[symbol],
            roles=['short-term-decider'],
            started_at=datetime(2026, 4, 29, 4, 0, tzinfo=timezone.utc),
        )
        self._seed_short_term_snapshot(symbol=symbol, decision_run_id=old_short)
        self._seed_feedback_run(
            run_id=old_signal,
            symbols=[symbol],
            roles=['signal-decider'],
            started_at=datetime(2026, 4, 29, 4, 1, tzinfo=timezone.utc),
        )
        self._seed_snapshot(symbol=symbol, decision_run_id=old_signal)
        self._seed_feedback_run(
            run_id=parent_run,
            symbols=[symbol],
            roles=['short-term-decider', 'signal-decider'],
            started_at=datetime(2026, 4, 29, 5, 0, tzinfo=timezone.utc),
            status='completed',
            completed_count=1,
            failed_count=0,
            mode='daily_retry_cycle_role_sequenced',
        )
        service = self._service()
        scope_state = service._load_latest_decider_scope_state(
            source_market_date=self.market_date,
        )

        result = self.agent_context_from_state_patcher.temp_original(
            service,
            source_market_date=self.market_date,
            candidate_symbols=[symbol],
            source_run=scope_state['source_run'],
            scope_state=scope_state,
        )

        self.assertFalse(scope_state['strict_source_lineage'])
        self.assertEqual(result['status'], 'incomplete')
        self.assertEqual(result['completed_scope_count'], 0)
        self.assertEqual(result['pending_scope_count'], 2)

    def test_lineage_parent_exact_reuse_attestation_is_complete(self) -> None:
        symbol = f'R{uuid4().hex[:10].upper()}'
        old_short = f'reused-short-{uuid4().hex[:8]}'
        old_signal = f'reused-signal-{uuid4().hex[:8]}'
        parent_run = f'reuse-parent-{uuid4().hex[:8]}'
        self._seed_feedback_run(
            run_id=old_short,
            symbols=[symbol],
            roles=['short-term-decider'],
            started_at=datetime(2026, 4, 29, 4, 0, tzinfo=timezone.utc),
        )
        old_short_snapshot_id = self._seed_short_term_snapshot(
            symbol=symbol,
            decision_run_id=old_short,
            feedback_received_at=datetime(2026, 4, 29, 4, 10, tzinfo=timezone.utc),
        )
        self._seed_feedback_run(
            run_id=old_signal,
            symbols=[symbol],
            roles=['signal-decider'],
            started_at=datetime(2026, 4, 29, 4, 1, tzinfo=timezone.utc),
        )
        old_signal_snapshot_id = self._seed_snapshot(
            symbol=symbol,
            decision_run_id=old_signal,
            feedback_received_at=datetime(2026, 4, 29, 4, 10, tzinfo=timezone.utc),
        )
        self._seed_feedback_run(
            run_id=parent_run,
            symbols=[symbol],
            roles=['short-term-decider', 'signal-decider'],
            started_at=datetime(2026, 4, 29, 5, 0, tzinfo=timezone.utc),
            mode='daily_retry_cycle_role_sequenced',
            child_run_lineage=True,
            reused_symbols_by_role={
                'short-term-decider': [symbol],
                'signal-decider': [symbol],
            },
            reused_feedback_evidence_by_role={
                **self._reuse_evidence(
                    symbol=symbol,
                    role='short-term-decider',
                    feedback_run_id=old_short,
                    feedback_received_at=datetime(
                        2026, 4, 29, 4, 10, tzinfo=timezone.utc
                    ),
                    decision_snapshot_id=old_short_snapshot_id,
                ),
                **self._reuse_evidence(
                    symbol=symbol,
                    feedback_run_id=old_signal,
                    feedback_received_at=datetime(
                        2026, 4, 29, 4, 10, tzinfo=timezone.utc
                    ),
                    decision_snapshot_id=old_signal_snapshot_id,
                ),
            },
        )
        service = self._service()
        scope_state = service._load_latest_decider_scope_state(
            source_market_date=self.market_date,
        )

        result = self.agent_context_from_state_patcher.temp_original(
            service,
            source_market_date=self.market_date,
            candidate_symbols=[symbol],
            source_run=scope_state['source_run'],
            scope_state=scope_state,
        )

        self.assertEqual(result['status'], 'complete')
        self.assertEqual(result['completed_scope_count'], 2)
        self.assertEqual(
            {
                scope_state['selected_by_key'][(symbol, agent_name)].id
                for agent_name in ('short-term-decider', 'signal-decider')
            },
            {old_short_snapshot_id, old_signal_snapshot_id},
        )
        self.assertEqual(
            scope_state['reused_scope_keys'],
            {
                (symbol, 'short-term-decider'),
                (symbol, 'signal-decider'),
            },
        )

    def test_agent_context_completeness_is_assessed_per_candidate_symbol(self) -> None:
        source_run = {
            'run_id': 'per-symbol-source',
            'status': 'partial',
            'request_payload': {
                'mode': 'daily_retry_cycle_role_sequenced',
                'roles': ['short-term-decider', 'signal-decider'],
                'role_symbol_scopes': {
                    'short-term-decider': ['005930', '000660'],
                    'signal-decider': ['005930', '000660'],
                },
            },
        }
        failed_key = ('000660', 'short-term-decider')
        scope_state = {
            'source_run': source_run,
            'selected_by_key': {
                ('005930', 'short-term-decider'): object(),
                ('005930', 'signal-decider'): object(),
                failed_key: None,
                ('000660', 'signal-decider'): object(),
            },
            'authoritative_run_id_by_key': {
                ('005930', 'short-term-decider'): 'short-a',
                ('005930', 'signal-decider'): 'signal-a',
                failed_key: 'short-b-failed',
                ('000660', 'signal-decider'): 'signal-b',
            },
            'evidenced_scope_keys': {
                ('005930', 'short-term-decider'),
                ('005930', 'signal-decider'),
                ('000660', 'signal-decider'),
            },
            'failures_by_key': {
                failed_key: {
                    'symbol': '000660',
                    'agent_name': 'short-term-decider',
                    'run_id': 'short-b-failed',
                    'failure_kind': 'response_contract_invalid',
                    'retryable': True,
                },
            },
            'run_snapshots': [],
        }
        real_builder = self.agent_context_from_state_patcher.temp_original
        service = self._service()
        result = real_builder(
            service,
            source_market_date=self.market_date,
            candidate_symbols=['005930', '000660'],
            source_run=source_run,
            scope_state=scope_state,
        )

        by_symbol = result['_candidate_contexts_by_symbol']
        self.assertEqual(result['status'], 'incomplete')
        self.assertEqual(by_symbol['005930']['status'], 'complete')
        self.assertFalse(by_symbol['005930']['candidate_set_definitive'])
        self.assertEqual(by_symbol['000660']['status'], 'incomplete')
        self.assertEqual(by_symbol['000660']['failure_count'], 1)

    def test_completeness_uses_latest_actual_child_and_ignores_stale_failure(self) -> None:
        symbol = f'T{uuid4().hex[:10].upper()}'
        source_parent = f'source-parent-{uuid4().hex[:8]}'
        short_success = f'short-success-{uuid4().hex[:8]}'
        signal_success = f'signal-success-{uuid4().hex[:8]}'
        signal_failure = f'signal-failure-{uuid4().hex[:8]}'
        signal_repair = f'signal-repair-{uuid4().hex[:8]}'
        self._seed_feedback_run(
            run_id=source_parent,
            symbols=[symbol],
            roles=['short-term-decider', 'signal-decider'],
            started_at=datetime(2026, 4, 29, 4, 0, tzinfo=timezone.utc),
            mode='daily_retry_cycle_role_sequenced',
        )
        self._seed_feedback_run(
            run_id=short_success,
            symbols=[symbol],
            roles=['short-term-decider'],
            started_at=datetime(2026, 4, 29, 5, 0, tzinfo=timezone.utc),
        )
        self._seed_short_term_snapshot(
            symbol=symbol,
            decision_run_id=short_success,
        )
        self._seed_feedback_run(
            run_id=signal_success,
            symbols=[symbol],
            roles=['signal-decider'],
            started_at=datetime(2026, 4, 29, 5, 1, tzinfo=timezone.utc),
        )
        self._seed_snapshot(symbol=symbol, decision_run_id=signal_success)
        self._seed_feedback_run(
            run_id=signal_failure,
            symbols=[symbol],
            roles=['signal-decider'],
            started_at=datetime(2026, 4, 29, 6, 0, tzinfo=timezone.utc),
            status='completed_with_failures',
            completed_count=0,
            failed_count=1,
        )
        self.feedback_failure_symbols.append(symbol)
        with session_scope() as session:
            session.add(
                AgentContextFeedbackFailure(
                    market_date=self.market_date,
                    symbol=symbol,
                    agent_name='signal-decider',
                    run_id=signal_failure,
                    failure_kind='response_contract_invalid',
                    retryable=True,
                    summary_payload={},
                    context_payload={},
                    received_at=datetime(2026, 4, 29, 6, 1, tzinfo=timezone.utc),
                )
            )

        real_builder = self.agent_context_from_state_patcher.temp_original
        service = self._service()
        failed_scope_state = service._load_latest_decider_scope_state(
            source_market_date=self.market_date,
        )
        failed_result = real_builder(
            service,
            source_market_date=self.market_date,
            candidate_symbols=[symbol],
            source_run=failed_scope_state['source_run'],
            scope_state=failed_scope_state,
        )

        self.assertEqual(failed_result['status'], 'incomplete')
        self.assertEqual(failed_result['completed_scope_count'], 1)
        self.assertEqual(failed_result['failure_count'], 1)
        self.assertEqual(failed_result['pending_scope_count'], 0)
        self.assertEqual(
            failed_result['by_failure_kind'],
            {'response_contract_invalid': 1},
        )

        self._seed_feedback_run(
            run_id=signal_repair,
            symbols=[symbol],
            roles=['signal-decider'],
            started_at=datetime(2026, 4, 29, 7, 0, tzinfo=timezone.utc),
        )
        self._seed_snapshot(symbol=symbol, decision_run_id=signal_repair)
        repaired_scope_state = service._load_latest_decider_scope_state(
            source_market_date=self.market_date,
        )
        repaired_result = real_builder(
            service,
            source_market_date=self.market_date,
            candidate_symbols=[symbol],
            source_run=repaired_scope_state['source_run'],
            scope_state=repaired_scope_state,
        )

        self.assertEqual(repaired_result['status'], 'complete')
        self.assertEqual(repaired_result['completed_scope_count'], 2)
        self.assertEqual(repaired_result['failure_count'], 0)
        self.assertEqual(repaired_result['pending_scope_count'], 0)

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
            stale_snapshot_generation_state = (
                stale_service._load_latest_snapshot_generation_state(
                    source_market_date=self.market_date
                )
            )
            load_latest_generation_state = (
                stale_service._load_latest_snapshot_generation_state
            )
            load_count = 0

            def expose_newer_wait_after_stale_read(
                *,
                source_market_date: date,
                target_trade_date: date | None = None,
            ):
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
                    return stale_snapshot_generation_state
                return load_latest_generation_state(
                    source_market_date=source_market_date,
                    target_trade_date=target_trade_date,
                )

            with patch.object(
                stale_service,
                '_load_latest_snapshot_generation_state',
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

    def test_refresh_retries_when_snapshot_crosses_target_open_cutoff(
        self,
    ) -> None:
        settings.signal_decider_exec_integration_mode = 'dispatch'
        snapshot_id = self._seed_snapshot(
            symbol='005930',
            feedback_received_at=datetime(
                2026,
                4,
                29,
                23,
                59,
                59,
                tzinfo=timezone.utc,
            ),
        )
        service = self._service(
            state='PREOPEN',
            server_time='2026-04-30T08:40:00+09:00',
        )

        with patch(
            'app.orders.next_open_execution.load_daily_bars_for_symbol_range',
            return_value=[
                {'trade_date': '2026-04-29', 'close_price': 50000}
            ],
        ):
            service.refresh_plan_items(
                target_trade_date=self.target_trade_date,
                trigger='seed_preopen_plan',
            )
            stale_state = service._load_latest_snapshot_generation_state(
                source_market_date=self.market_date,
                target_trade_date=self.target_trade_date,
            )
            load_current_state = service._load_latest_snapshot_generation_state
            load_count = 0

            def cross_cutoff_after_initial_read(
                *,
                source_market_date: date,
                target_trade_date: date | None = None,
            ):
                nonlocal load_count
                load_count += 1
                if load_count == 1:
                    with session_scope() as session:
                        snapshot = session.get(
                            SignalDeciderDecisionSnapshot,
                            snapshot_id,
                        )
                        snapshot.feedback_received_at = datetime(
                            2026,
                            4,
                            30,
                            0,
                            0,
                            tzinfo=timezone.utc,
                        )
                    return stale_state
                return load_current_state(
                    source_market_date=source_market_date,
                    target_trade_date=target_trade_date,
                )

            with patch.object(
                service,
                '_load_latest_snapshot_generation_state',
                side_effect=cross_cutoff_after_initial_read,
            ):
                refreshed = service.refresh_plan_items(
                    target_trade_date=self.target_trade_date,
                    trigger='cross_target_open_cutoff',
                )

        item = next(
            row for row in refreshed['items'] if row['symbol'] == '005930'
        )
        self.assertGreaterEqual(load_count, 2)
        self.assertEqual(item['eligibility_status'], ELIGIBILITY_BLOCKED)
        self.assertIn(
            'decision_snapshot_after_target_open',
            {
                reason['code']
                for reason in item['validation']['blocking_reasons']
            },
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

    def test_refresh_resamples_preopen_to_open_transition_under_plan_lock(self) -> None:
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

        preopen_state = MarketState(
            market='KRX',
            state='PREOPEN',
            is_trading_day=True,
            is_open=False,
            server_time='2026-04-30T08:59:59+09:00',
            session_open_at='2026-04-30T09:00:00+09:00',
            session_close_at='2026-04-30T15:30:00+09:00',
        )
        open_state = MarketState(
            market='KRX',
            state='OPEN',
            is_trading_day=True,
            is_open=True,
            server_time='2026-04-30T09:00:01+09:00',
            session_open_at='2026-04-30T09:00:00+09:00',
            session_close_at='2026-04-30T15:30:00+09:00',
        )
        state_reads = 0

        def transitioning_market_state() -> MarketState:
            nonlocal state_reads
            state_reads += 1
            return preopen_state if state_reads == 1 else open_state

        transition_service = SignalDeciderNextOpenExecutionService(
            market_state_getter=transitioning_market_state,
        )
        severe_snapshot = self._live_market_snapshot_context(
            observed_at=datetime(
                2026,
                4,
                30,
                9,
                0,
                tzinfo=timezone(timedelta(hours=9)),
            )
        )
        with (
            patch(
                'app.orders.next_open_execution.load_current_market_snapshot_context',
                return_value=severe_snapshot,
            ) as load_snapshot,
            patch(
                'app.orders.next_open_execution.load_daily_bars_for_symbol_range',
                return_value=[{'trade_date': '2026-04-29', 'close_price': 50000}],
            ),
        ):
            item = transition_service.refresh_plan_items(
                target_trade_date=self.target_trade_date,
                trigger='preopen_open_transition',
            )['items'][0]

        self.assertGreaterEqual(state_reads, 2)
        self.assertGreaterEqual(load_snapshot.call_count, 1)
        self.assertEqual(item['execution_status'], EXECUTION_STATUS_REVOCATION_PENDING)
        self.assertEqual(
            item['submission']['revocation']['reason_code'],
            'current_live_market_risk_off',
        )

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

        with (
            patch.object(service, '_load_latest_snapshots', return_value=[]),
            patch.object(
                service,
                '_load_latest_decider_scope_state',
                return_value={
                    'selected_by_key': {},
                    'authoritative_run_id_by_key': {},
                    'failures_by_key': {},
                    'run_snapshots': [],
                },
            ),
        ):
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

    def test_refresh_reports_invalid_generation_instead_of_candidate_removed(self) -> None:
        snapshot_id = self._seed_snapshot(symbol='005930')
        service = self._service()
        with patch(
            'app.orders.next_open_execution.load_daily_bars_for_symbol_range',
            return_value=[{'trade_date': '2026-04-29', 'close_price': 50000}],
        ):
            service.refresh_plan_items(
                target_trade_date=self.target_trade_date,
                trigger='store_valid_generation',
            )

        def reject_snapshot_generation(*, market_date, snapshots, **_kwargs):
            return {
                'schema_version': 'persisted_decider_generation_validation_v1',
                'market_date': str(market_date),
                'valid_snapshot_ids': [],
                'invalid_snapshot_ids': [int(snapshot.id) for snapshot in snapshots],
                'validations': [
                    {
                        'snapshot_id': int(snapshot.id),
                        'symbol': snapshot.symbol,
                        'agent_name': snapshot.agent_name,
                        'status': 'invalid',
                        'reason': 'durable_decider_inputs_changed',
                    }
                    for snapshot in snapshots
                ],
                'generation_fingerprint': f'test-snapshot-generation:invalid:{snapshot_id}',
            }

        self.snapshot_generation_mock.side_effect = reject_snapshot_generation
        with patch(
            'app.orders.next_open_execution.load_daily_bars_for_symbol_range',
            return_value=[{'trade_date': '2026-04-29', 'close_price': 50000}],
        ):
            refreshed = service.refresh_plan_items(
                target_trade_date=self.target_trade_date,
                trigger='generation_changed',
            )

        item = next(
            row
            for row in refreshed['items']
            if row['account_reference'] == self.account_reference
            and row['symbol'] == '005930'
        )
        self.assertEqual(item['eligibility_status'], ELIGIBILITY_BLOCKED)
        self.assertEqual(item['execution_status'], EXECUTION_STATUS_PLANNED)
        self.assertEqual(item['decision_snapshot_id'], snapshot_id)
        self.assertEqual(
            [reason['code'] for reason in item['validation']['blocking_reasons']],
            ['decision_input_generation_changed'],
        )

    def test_feedback_active_watch_excludes_same_generation_and_blocked_rows(self) -> None:
        self._seed_snapshot(symbol='005930')
        service = self._service()
        with patch(
            'app.orders.next_open_execution.load_daily_bars_for_symbol_range',
            return_value=[{'trade_date': '2026-04-29', 'close_price': 50000}],
        ):
            service.refresh_plan_items(
                target_trade_date=self.target_trade_date,
                trigger='store_plan_row',
            )

        with session_scope() as session:
            row = session.execute(
                select(SignalDeciderExecutionPlanItem).where(
                    SignalDeciderExecutionPlanItem.account_reference
                    == self.account_reference,
                    SignalDeciderExecutionPlanItem.symbol == '005930',
                )
            ).scalar_one()
            row.eligibility_status = ELIGIBILITY_MONITOR_CANDIDATE
            row.execution_status = EXECUTION_STATUS_AWAITING_TRIGGER
            row.source_market_date = self.market_date

        same_generation_reasons: dict[str, list[dict]] = {}
        same_generation = _load_active_watch_reasons(
            market_date=self.market_date,
            symbols=['005930'],
            reasons_by_symbol=same_generation_reasons,
        )
        self.assertEqual(same_generation['matched_rows'], 0)
        self.assertEqual(same_generation_reasons, {})

        with session_scope() as session:
            row = session.execute(
                select(SignalDeciderExecutionPlanItem).where(
                    SignalDeciderExecutionPlanItem.account_reference
                    == self.account_reference,
                    SignalDeciderExecutionPlanItem.symbol == '005930',
                )
            ).scalar_one()
            row.source_market_date = self.market_date - timedelta(days=1)

        prior_generation_reasons: dict[str, list[dict]] = {}
        prior_generation = _load_active_watch_reasons(
            market_date=self.market_date,
            symbols=['005930'],
            reasons_by_symbol=prior_generation_reasons,
        )
        self.assertEqual(prior_generation['matched_rows'], 1)
        self.assertEqual(
            prior_generation_reasons['005930'][0]['code'],
            'active_exec_watch_item',
        )

        with session_scope() as session:
            row = session.execute(
                select(SignalDeciderExecutionPlanItem).where(
                    SignalDeciderExecutionPlanItem.account_reference
                    == self.account_reference,
                    SignalDeciderExecutionPlanItem.symbol == '005930',
                )
            ).scalar_one()
            row.eligibility_status = ELIGIBILITY_BLOCKED

        blocked_reasons: dict[str, list[dict]] = {}
        blocked = _load_active_watch_reasons(
            market_date=self.market_date,
            symbols=['005930'],
            reasons_by_symbol=blocked_reasons,
        )
        self.assertEqual(blocked['matched_rows'], 0)
        self.assertEqual(blocked_reasons, {})

    def test_snapshot_scope_selection_hydrates_only_selected_payloads(self) -> None:
        older_snapshot_id = self._seed_snapshot(
            symbol='005930',
            decision_run_id='scope-projection-older',
            feedback_received_at=datetime(
                2026, 4, 29, 5, 0, tzinfo=timezone.utc
            ),
        )
        snapshot_id = self._seed_snapshot(
            symbol='005930',
            decision_run_id='scope-projection-selected',
            feedback_received_at=datetime(
                2026, 4, 29, 6, 0, tzinfo=timezone.utc
            ),
        )
        service = self._service()
        statements: list[tuple[str, object]] = []

        def record_statement(
            _connection,
            _cursor,
            statement,
            _parameters,
            _context,
            _executemany,
        ) -> None:
            if 'FROM signal_decider_decision_snapshots' in statement:
                statements.append((statement, _parameters))

        event.listen(engine, 'before_cursor_execute', record_statement)
        try:
            state = service._load_latest_decider_scope_state(
                source_market_date=self.market_date,
                target_trade_date=self.target_trade_date,
            )
        finally:
            event.remove(engine, 'before_cursor_execute', record_statement)

        selected = state['selected_by_key'][('005930', 'signal-decider')]
        self.assertEqual(selected.id, snapshot_id)
        self.assertIsInstance(selected.supporting_evidence_payload, (dict, list))
        metadata_queries = [
            (statement, parameters)
            for statement, parameters in statements
            if 'market_date' in statement and 'id IN' not in statement
        ]
        hydration_queries = [
            (statement, parameters)
            for statement, parameters in statements
            if 'id IN' in statement
        ]
        self.assertEqual(len(metadata_queries), 1)
        self.assertNotIn(
            'supporting_evidence_payload',
            metadata_queries[0][0],
        )
        self.assertEqual(len(hydration_queries), 1)
        self.assertIn(
            'supporting_evidence_payload',
            hydration_queries[0][0],
        )
        hydration_parameters = hydration_queries[0][1]
        self.assertIsInstance(hydration_parameters, dict)
        hydrated_ids = {
            int(value)
            for value in hydration_parameters.values()
            if isinstance(value, int)
        }
        self.assertIn(snapshot_id, hydrated_ids)
        self.assertNotIn(older_snapshot_id, hydrated_ids)

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

    def test_dispatch_rejects_regressed_agent_context_authority(self) -> None:
        self._seed_snapshot(symbol='005930')
        service = self._service(
            state='PREOPEN',
            server_time='2026-04-30T08:40:00+09:00',
        )
        stored_context = {
            'schema_version': 'agent_context_completeness_v1',
            'source_market_date': self.market_date.isoformat(),
            'candidate_symbols': ['005930'],
            'candidate_set_basis': 'latest_signal_decider_snapshots',
            'status': 'complete',
            'cohort_status': 'complete',
            'has_incomplete_agent_context': False,
            'candidate_set_definitive': True,
            'authority_fingerprint': 'stored-complete-authority',
            'latest_run': {'run_id': 'stored-parent', 'status': 'completed'},
            'failure_count': 0,
            'failure_groups': [],
        }
        with (
            patch.object(
                service,
                '_build_agent_context_completeness_context',
                return_value=stored_context,
            ),
            patch.object(
                service,
                '_build_agent_context_completeness_from_state',
                return_value=stored_context,
            ),
            patch(
                'app.orders.next_open_execution.load_daily_bars_for_symbol_range',
                return_value=[{'trade_date': '2026-04-29', 'close_price': 50000}],
            ),
        ):
            service.refresh_plan_items(
                target_trade_date=self.target_trade_date,
                trigger='store_complete_agent_context',
            )
        self._mark_row_as_handed_off()

        with session_scope() as session:
            row = (
                session.query(SignalDeciderExecutionPlanItem)
                .filter(
                    SignalDeciderExecutionPlanItem.account_reference
                    == self.account_reference,
                    SignalDeciderExecutionPlanItem.symbol == '005930',
                )
                .one()
            )
            expected_row_bindings = {
                int(row.id): {
                    'decision_snapshot_id': int(row.decision_snapshot_id),
                    'row_revision_fingerprint': (
                        _execution_plan_row_authority_fingerprint(row)
                    ),
                }
            }

        current_contexts = [
            {
                **stored_context,
                'authority_fingerprint': 'new-complete-authority',
                'latest_run': {'run_id': 'new-parent', 'status': 'completed'},
            },
            {
                **stored_context,
                'status': 'incomplete',
                'cohort_status': 'incomplete',
                'has_incomplete_agent_context': True,
                'candidate_set_definitive': False,
                'authority_fingerprint': 'new-running-authority',
                'latest_run': {'run_id': 'new-parent', 'status': 'running'},
            },
        ]
        for current_context in current_contexts:
            with self.subTest(
                status=current_context['status'],
                authority_fingerprint=current_context['authority_fingerprint'],
            ):
                with (
                    patch.object(
                        service,
                        '_build_agent_context_completeness_context',
                        return_value=current_context,
                    ),
                    patch.object(
                        service,
                        '_build_agent_context_completeness_from_state',
                        return_value=current_context,
                    ),
                ):
                    generation = service.inspect_dispatch_authority_generation(
                        target_trade_date=self.target_trade_date,
                        expected_row_bindings=expected_row_bindings,
                    )

                self.assertEqual(generation['status'], 'invalid')
                self.assertTrue(generation['issues'])
                self.assertEqual(
                    generation['issues'][0]['reason_code'],
                    'agent_context_authority_changed',
                )
                self.assertEqual(
                    generation['issues'][0]['replacement_action'],
                    'reject',
                )

    def test_terminal_buy_revokes_when_agent_context_authority_regresses(self) -> None:
        self._seed_snapshot(symbol='005930')
        service = self._service(
            state='PREOPEN',
            server_time='2026-04-30T08:40:00+09:00',
        )
        stored_context = {
            'schema_version': 'agent_context_completeness_v1',
            'source_market_date': self.market_date.isoformat(),
            'candidate_symbols': ['005930'],
            'candidate_set_basis': 'latest_signal_decider_snapshots',
            'status': 'complete',
            'cohort_status': 'complete',
            'has_incomplete_agent_context': False,
            'candidate_set_definitive': True,
            'authority_fingerprint': 'stored-complete-authority',
            'latest_run': {'run_id': 'stored-parent', 'status': 'completed'},
            'failure_count': 0,
            'failure_groups': [],
        }
        with (
            patch.object(
                service,
                '_build_agent_context_completeness_context',
                return_value=stored_context,
            ),
            patch.object(
                service,
                '_build_agent_context_completeness_from_state',
                return_value=stored_context,
            ),
            patch(
                'app.orders.next_open_execution.load_daily_bars_for_symbol_range',
                return_value=[{'trade_date': '2026-04-29', 'close_price': 50000}],
            ),
        ):
            service.refresh_plan_items(
                target_trade_date=self.target_trade_date,
                trigger='store_complete_agent_context',
            )
        self._mark_row_as_handed_off()

        regressed_context = {
            **stored_context,
            'status': 'incomplete',
            'cohort_status': 'incomplete',
            'has_incomplete_agent_context': True,
            'candidate_set_definitive': False,
            'authority_fingerprint': 'new-running-authority',
            'latest_run': {'run_id': 'new-parent', 'status': 'running'},
        }
        with (
            patch.object(
                service,
                '_build_agent_context_completeness_context',
                return_value=regressed_context,
            ),
            patch.object(
                service,
                '_build_agent_context_completeness_from_state',
                return_value=regressed_context,
            ),
            patch(
                'app.orders.next_open_execution.load_daily_bars_for_symbol_range',
                return_value=[{'trade_date': '2026-04-29', 'close_price': 50000}],
            ),
        ):
            refreshed = service.refresh_plan_items(
                target_trade_date=self.target_trade_date,
                trigger='agent_context_regressed',
            )

        item = next(
            row
            for row in refreshed['items']
            if row['account_reference'] == self.account_reference
            and row['symbol'] == '005930'
        )
        self.assertEqual(
            item['execution_status'],
            EXECUTION_STATUS_REVOCATION_PENDING,
        )
        self.assertEqual(
            item['submission']['revocation']['reason_code'],
            'agent_context_authority_changed',
        )

    def test_terminal_buy_revokes_when_bound_decision_policy_is_retired(self) -> None:
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
                trigger='store_active_policy_authority',
            )
        self._mark_row_as_handed_off()
        with session_scope() as session:
            policy = session.execute(
                select(SignalDeciderPolicyVersion).where(
                    SignalDeciderPolicyVersion.policy_version
                    == self.policy_version
                )
            ).scalar_one()
            policy.status = 'retired'
            policy.retired_at = datetime(
                2026, 4, 29, 7, 0, tzinfo=timezone.utc
            )

        with patch(
            'app.orders.next_open_execution.load_daily_bars_for_symbol_range',
            return_value=[{'trade_date': '2026-04-29', 'close_price': 50000}],
        ):
            refreshed = service.refresh_plan_items(
                target_trade_date=self.target_trade_date,
                trigger='policy_retired_after_handoff',
            )

        item = refreshed['items'][0]
        self.assertEqual(
            item['execution_status'],
            EXECUTION_STATUS_REVOCATION_PENDING,
        )
        self.assertEqual(
            item['submission']['revocation']['reason_code'],
            'policy_scope_changed',
        )
        self.assertEqual(item['eligibility_status'], ELIGIBILITY_BLOCKED)

    def test_terminal_buy_revokes_when_active_policy_bundle_changes(self) -> None:
        settings.signal_decider_exec_integration_mode = 'dispatch'
        self._seed_snapshot(symbol='005930')
        service = self._service(
            state='PREOPEN',
            server_time='2026-04-30T08:40:00+09:00',
        )
        with patch(
            'app.orders.next_open_execution.load_daily_bars_for_symbol_range',
            return_value=[
                {'trade_date': '2026-04-29', 'close_price': 50000}
            ],
        ):
            service.refresh_plan_items(
                target_trade_date=self.target_trade_date,
                trigger='store_policy_bundle_authority',
            )
        self._mark_row_as_handed_off()

        with session_scope() as session:
            policy = session.execute(
                select(SignalDeciderPolicyVersion).where(
                    SignalDeciderPolicyVersion.policy_version
                    == self.policy_version
                )
            ).scalar_one()
            policy.rule_bundle_payload = {
                'schema_version': 'signal_decider_policy_bundle_v1',
                'change_set': [{'rule': 'minimum_confidence', 'value': 8}],
            }

        with patch(
            'app.orders.next_open_execution.load_daily_bars_for_symbol_range',
            return_value=[
                {'trade_date': '2026-04-29', 'close_price': 50000}
            ],
        ):
            refreshed = service.refresh_plan_items(
                target_trade_date=self.target_trade_date,
                trigger='policy_bundle_changed_after_handoff',
            )

        item = refreshed['items'][0]
        self.assertEqual(
            item['execution_status'],
            EXECUTION_STATUS_REVOCATION_PENDING,
        )
        self.assertEqual(
            item['submission']['revocation']['reason_code'],
            'policy_scope_changed',
        )
        self.assertEqual(item['eligibility_status'], ELIGIBILITY_BLOCKED)

    def test_dispatch_generation_rejects_retired_decision_policy(self) -> None:
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
                trigger='store_active_policy_generation',
            )
        with session_scope() as session:
            row = session.execute(
                select(SignalDeciderExecutionPlanItem).where(
                    SignalDeciderExecutionPlanItem.account_reference
                    == self.account_reference,
                    SignalDeciderExecutionPlanItem.symbol == '005930',
                )
            ).scalar_one()
            expected_row_bindings = {
                int(row.id): {
                    'decision_snapshot_id': int(row.decision_snapshot_id),
                    'row_revision_fingerprint': (
                        _execution_plan_row_authority_fingerprint(row)
                    ),
                }
            }
            policy = session.execute(
                select(SignalDeciderPolicyVersion).where(
                    SignalDeciderPolicyVersion.policy_version
                    == self.policy_version
                )
            ).scalar_one()
            policy.status = 'retired'
            policy.retired_at = datetime(
                2026, 4, 29, 7, 0, tzinfo=timezone.utc
            )

        generation = service.inspect_dispatch_authority_generation(
            target_trade_date=self.target_trade_date,
            expected_row_bindings=expected_row_bindings,
        )

        self.assertEqual(generation['status'], 'invalid')
        self.assertEqual(generation['reason_code'], 'policy_scope_changed')
        self.assertEqual(
            generation['issues'][0]['reason_code'],
            'policy_scope_changed',
        )

    def test_dispatch_generation_rejects_changed_durable_decision_inputs(self) -> None:
        settings.signal_decider_exec_integration_mode = 'dispatch'
        snapshot_id = self._seed_snapshot(symbol='005930')
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
                trigger='store_durable_generation',
            )
        with session_scope() as session:
            row = session.execute(
                select(SignalDeciderExecutionPlanItem).where(
                    SignalDeciderExecutionPlanItem.account_reference
                    == self.account_reference,
                    SignalDeciderExecutionPlanItem.symbol == '005930',
                )
            ).scalar_one()
            expected_row_bindings = {
                int(row.id): {
                    'decision_snapshot_id': int(row.decision_snapshot_id),
                    'row_revision_fingerprint': (
                        _execution_plan_row_authority_fingerprint(row)
                    ),
                }
            }

        self.snapshot_generation_mock.side_effect = None
        self.snapshot_generation_mock.return_value = {
            'schema_version': 'persisted_decider_generation_validation_v1',
            'market_date': self.market_date.isoformat(),
            'valid_snapshot_ids': [],
            'invalid_snapshot_ids': [snapshot_id],
            'validations': [
                {
                    'snapshot_id': snapshot_id,
                    'symbol': '005930',
                    'agent_name': 'signal-decider',
                    'status': 'invalid',
                    'reason': 'durable_decider_inputs_changed',
                }
            ],
            'generation_fingerprint': 'test-snapshot-generation:changed',
        }

        generation = service.inspect_dispatch_authority_generation(
            target_trade_date=self.target_trade_date,
            expected_row_bindings=expected_row_bindings,
        )

        self.assertEqual(generation['status'], 'invalid')
        self.assertEqual(
            generation['reason_code'],
            'decision_input_generation_changed',
        )
        self.assertFalse(generation['replacement_actionable'])
        self.assertIs(
            self.snapshot_generation_mock.call_args.kwargs.get(
                'full_source_revalidation_actionable_only'
            ),
            True,
        )

    def test_dispatch_generation_rejects_snapshot_observed_at_target_open(
        self,
    ) -> None:
        settings.signal_decider_exec_integration_mode = 'dispatch'
        snapshot_id = self._seed_snapshot(
            symbol='005930',
            feedback_received_at=datetime(
                2026,
                4,
                29,
                23,
                59,
                59,
                tzinfo=timezone.utc,
            ),
        )
        service = self._service(
            state='PREOPEN',
            server_time='2026-04-30T08:40:00+09:00',
        )
        with patch(
            'app.orders.next_open_execution.load_daily_bars_for_symbol_range',
            return_value=[
                {'trade_date': '2026-04-29', 'close_price': 50000}
            ],
        ):
            service.refresh_plan_items(
                target_trade_date=self.target_trade_date,
                trigger='store_preopen_generation',
            )
        with session_scope() as session:
            row = session.execute(
                select(SignalDeciderExecutionPlanItem).where(
                    SignalDeciderExecutionPlanItem.account_reference
                    == self.account_reference,
                    SignalDeciderExecutionPlanItem.symbol == '005930',
                )
            ).scalar_one()
            expected_row_bindings = {
                int(row.id): {
                    'decision_snapshot_id': int(row.decision_snapshot_id),
                    'row_revision_fingerprint': (
                        _execution_plan_row_authority_fingerprint(row)
                    ),
                }
            }
            snapshot = session.get(
                SignalDeciderDecisionSnapshot,
                snapshot_id,
            )
            snapshot.feedback_received_at = datetime(
                2026,
                4,
                30,
                0,
                0,
                tzinfo=timezone.utc,
            )

        generation = service.inspect_dispatch_authority_generation(
            target_trade_date=self.target_trade_date,
            expected_row_bindings=expected_row_bindings,
        )

        self.assertEqual(generation['status'], 'invalid')
        self.assertEqual(
            generation['reason_code'],
            'decision_snapshot_after_target_open',
        )
        self.assertFalse(generation['replacement_actionable'])

    def test_revoked_buy_recovers_after_agent_context_becomes_complete_again(self) -> None:
        settings.signal_decider_exec_integration_mode = 'dispatch'
        self._seed_snapshot(symbol='005930')
        service = self._service(
            state='PREOPEN',
            server_time='2026-04-30T08:40:00+09:00',
        )
        initial_context = {
            'schema_version': 'agent_context_completeness_v1',
            'source_market_date': self.market_date.isoformat(),
            'candidate_symbols': ['005930'],
            'candidate_set_basis': 'latest_signal_decider_snapshots',
            'status': 'complete',
            'cohort_status': 'complete',
            'has_incomplete_agent_context': False,
            'candidate_set_definitive': True,
            'authority_fingerprint': 'initial-complete-authority',
            'latest_run': {'run_id': 'initial-parent', 'status': 'completed'},
            'failure_count': 0,
            'failure_groups': [],
        }

        def refresh_with_context(context: dict, trigger: str) -> dict:
            with (
                patch.object(
                    service,
                    '_build_agent_context_completeness_context',
                    return_value=context,
                ),
                patch.object(
                    service,
                    '_build_agent_context_completeness_from_state',
                    return_value=context,
                ),
                patch(
                    'app.orders.next_open_execution.load_daily_bars_for_symbol_range',
                    return_value=[
                        {'trade_date': '2026-04-29', 'close_price': 50000}
                    ],
                ),
            ):
                return service.refresh_plan_items(
                    target_trade_date=self.target_trade_date,
                    trigger=trigger,
                )['items'][0]

        refresh_with_context(initial_context, 'store_initial_context')
        self._mark_row_as_handed_off()
        regressed_context = {
            **initial_context,
            'status': 'incomplete',
            'cohort_status': 'incomplete',
            'has_incomplete_agent_context': True,
            'candidate_set_definitive': False,
            'authority_fingerprint': 'running-authority',
            'latest_run': {'run_id': 'running-parent', 'status': 'running'},
        }
        pending = refresh_with_context(
            regressed_context,
            'agent_context_temporarily_incomplete',
        )
        self.assertEqual(
            pending['execution_status'],
            EXECUTION_STATUS_REVOCATION_PENDING,
        )
        self._ack_pending_revocation(service=service, item=pending)
        revoked = service.list_plan_items(
            target_trade_date=self.target_trade_date,
        )['items'][0]
        self.assertEqual(revoked['execution_status'], EXECUTION_STATUS_REVOKED)

        recovered_context = {
            **initial_context,
            'authority_fingerprint': 'recovered-complete-authority',
            'latest_run': {'run_id': 'recovered-parent', 'status': 'completed'},
        }
        recovered = refresh_with_context(
            recovered_context,
            'agent_context_recovered',
        )

        self.assertEqual(recovered['eligibility_status'], ELIGIBILITY_ELIGIBLE)
        self.assertEqual(recovered['execution_status'], EXECUTION_STATUS_PLANNED)
        self.assertNotIn('exec_dispatch', recovered['submission'])
        self.assertEqual(
            recovered['submission']['revocation']['recovery']['status'],
            'authority_revalidated',
        )

    def test_blocked_pykrx_still_revokes_terminal_buy_removed_by_child_tombstone(
        self,
    ) -> None:
        initial_run = f'pykrx-candidate-{uuid4().hex[:10]}'
        tombstone_run = f'pykrx-tombstone-{uuid4().hex[:10]}'
        self._seed_feedback_run(
            run_id=initial_run,
            symbols=['005930'],
            roles=['signal-decider'],
            started_at=datetime(2026, 4, 29, 5, 0, tzinfo=timezone.utc),
        )
        self._seed_snapshot(symbol='005930', decision_run_id=initial_run)
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
                trigger='seed_terminal_buy',
            )
        self._mark_row_as_handed_off()
        self._seed_feedback_run(
            run_id=tombstone_run,
            symbols=['005930'],
            roles=['signal-decider'],
            started_at=datetime(2026, 4, 29, 6, 0, tzinfo=timezone.utc),
            force_recompute=True,
        )
        blocked_preflight = {
            'market_date': self.market_date.isoformat(),
            'status': 'blocked',
            'allowed': False,
            'required_categories': [
                'sector_classification',
                'investor_flow_symbol',
                'short_selling',
            ],
            'failed_categories': ['investor_flow_symbol'],
            'zero_coverage_categories': ['investor_flow_symbol'],
            'category_row_counts': {
                'sector_classification': 1,
                'investor_flow_symbol': 0,
                'short_selling': 1,
            },
        }

        with patch(
            'app.orders.next_open_execution.inspect_pykrx_enrichment_preflight',
            return_value=blocked_preflight,
        ):
            refreshed = service.refresh_plan_items(
                target_trade_date=self.target_trade_date,
                trigger='blocked_pykrx_after_tombstone',
            )

        item = next(
            row
            for row in refreshed['items']
            if row['account_reference'] == self.account_reference
            and row['symbol'] == '005930'
        )
        self.assertEqual(
            item['execution_status'],
            EXECUTION_STATUS_REVOCATION_PENDING,
        )
        self.assertEqual(
            item['submission']['revocation']['reason_code'],
            'candidate_removed',
        )
        self.assertEqual(
            refreshed['summary']['reason'],
            'pykrx_enrichment_preflight_blocked',
        )


class CandidateEnrichmentRepairTests(unittest.TestCase):
    @patch('app.orders.next_open_execution.load_daily_pykrx_enrichment_rows')
    def test_candidate_inspection_rejects_all_zero_investor_payload(
        self,
        load_rows_mock,
    ):
        rows = _usable_candidate_pykrx_rows('005930')
        investor_row = next(
            row for row in rows if row['category'] == 'investor_flow_symbol'
        )
        for metrics in investor_row['payload']['investor_groups'].values():
            metrics.update(
                sell_volume=0,
                buy_volume=0,
                net_volume=0,
                sell_value=0,
                buy_value=0,
                net_value=0,
            )
        load_rows_mock.return_value = rows
        service = SignalDeciderNextOpenExecutionService.__new__(
            SignalDeciderNextOpenExecutionService
        )

        result = service._inspect_candidate_pykrx_enrichment(
            source_market_date=date(2026, 9, 18),
            symbols=['005930'],
            categories=['sector_classification', 'investor_flow_symbol', 'short_selling'],
        )

        self.assertFalse(result['005930']['allowed'])
        self.assertEqual(
            result['005930']['missing_categories'],
            ['investor_flow_symbol'],
        )

    @patch('app.orders.next_open_execution.load_daily_pykrx_enrichment_rows')
    def test_candidate_inspection_rejects_time_sensitive_carry_forward_rows(
        self,
        load_rows_mock,
    ):
        load_rows_mock.return_value = [
            {
                'symbol': '004370',
                'category': 'sector_classification',
                'source': 'pykrx_carry_forward',
                'payload': _usable_candidate_pykrx_rows('004370')[0]['payload'],
            },
            {
                'symbol': '004370',
                'category': 'investor_flow_symbol',
                'source': 'pykrx_carry_forward',
                'payload': _usable_candidate_pykrx_rows('004370')[1]['payload'],
            },
            {
                'symbol': '004370',
                'market': 'KOSPI',
                'category': 'short_selling',
                'source': 'pykrx',
                'payload': {
                    'short_volume': {
                        'short_volume': 1,
                        'total_volume': 10,
                        'short_volume_ratio_pct': 10.0,
                    }
                },
            },
        ]
        service = SignalDeciderNextOpenExecutionService.__new__(
            SignalDeciderNextOpenExecutionService
        )

        result = service._inspect_candidate_pykrx_enrichment(
            source_market_date=date(2026, 9, 18),
            symbols=['004370'],
            categories=[
                'sector_classification',
                'investor_flow_symbol',
                'short_selling',
            ],
        )

        self.assertEqual(
            result['004370']['missing_categories'],
            ['sector_classification', 'investor_flow_symbol'],
        )
        load_rows_mock.assert_called_once_with(
            market_date=date(2026, 9, 18),
            categories=['sector_classification', 'investor_flow_symbol', 'short_selling'],
            symbols=['004370'],
            limit=None,
        )

    @patch('app.orders.next_open_execution.load_daily_pykrx_enrichment_rows')
    def test_candidate_inspection_rejects_unsupported_konex_market(
        self,
        load_rows_mock,
    ):
        investor_group = {
            'sell_volume': 1,
            'buy_volume': 2,
            'net_volume': 1,
            'sell_value': 10,
            'buy_value': 20,
            'net_value': 10,
        }
        load_rows_mock.return_value = [
            {
                'symbol': '900001',
                'market': 'KONEX',
                'category': 'sector_classification',
                'source': 'pykrx',
                'payload': {'sector_name': '테스트'},
            },
            {
                'symbol': '900001',
                'market': 'KONEX',
                'category': 'investor_flow_symbol',
                'source': 'pykrx',
                'payload': {
                    'investor_groups': {
                        group: dict(investor_group)
                        for group in ('개인', '외국인', '기관합계')
                    }
                },
            },
            {
                'symbol': '900001',
                'market': 'KONEX',
                'category': 'short_selling',
                'source': 'pykrx',
                'payload': {
                    'short_volume': {
                        'short_volume': 1,
                        'total_volume': 10,
                        'short_volume_ratio_pct': 10.0,
                    }
                },
            },
        ]
        service = SignalDeciderNextOpenExecutionService.__new__(
            SignalDeciderNextOpenExecutionService
        )

        result = service._inspect_candidate_pykrx_enrichment(
            source_market_date=date(2026, 9, 18),
            symbols=['900001'],
            categories=['sector_classification', 'investor_flow_symbol', 'short_selling'],
        )

        self.assertFalse(result['900001']['allowed'])
        self.assertEqual(result['900001']['market'], 'KONEX')
        self.assertEqual(result['900001']['reason'], 'unsupported_decision_market')

    @patch('app.orders.next_open_execution.load_daily_pykrx_enrichment_rows')
    def test_candidate_inspection_rejects_short_row_without_core_metrics(
        self,
        load_rows_mock,
    ):
        investor_group = {
            'sell_volume': 1,
            'buy_volume': 2,
            'net_volume': 1,
            'sell_value': 10,
            'buy_value': 20,
            'net_value': 10,
        }
        load_rows_mock.return_value = [
            {
                'symbol': '005930',
                'market': 'KOSPI',
                'category': 'sector_classification',
                'source': 'pykrx',
                'payload': {'sector_name': '반도체'},
            },
            {
                'symbol': '005930',
                'market': 'KOSPI',
                'category': 'investor_flow_symbol',
                'source': 'pykrx',
                'payload': {
                    'investor_groups': {
                        group: dict(investor_group)
                        for group in ('개인', '외국인', '기관합계')
                    }
                },
            },
            {
                'symbol': '005930',
                'market': 'KOSPI',
                'category': 'short_selling',
                'source': 'pykrx',
                'payload': {
                    'short_volume': {
                        'short_volume': None,
                        'total_volume': None,
                        'short_volume_ratio_pct': None,
                    }
                },
            },
        ]
        service = SignalDeciderNextOpenExecutionService.__new__(
            SignalDeciderNextOpenExecutionService
        )

        result = service._inspect_candidate_pykrx_enrichment(
            source_market_date=date(2026, 9, 18),
            symbols=['005930'],
            categories=['sector_classification', 'investor_flow_symbol', 'short_selling'],
        )

        self.assertFalse(result['005930']['allowed'])
        self.assertEqual(result['005930']['missing_categories'], ['short_selling'])

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
            point_in_time_recovery=True,
        )
        self.assertEqual(result['004370']['missing_categories'], [])

    @patch('app.eod.pykrx_enrichment.pykrx_enrichment_service')
    def test_prior_date_repair_remains_blocked_without_historical_enrichment(
        self,
        enrichment_service,
    ):
        service = SignalDeciderNextOpenExecutionService.__new__(
            SignalDeciderNextOpenExecutionService
        )
        missing = {
            '004370': {
                'missing_categories': [
                    'investor_flow_symbol',
                    'short_selling',
                ],
                'allowed': False,
            }
        }
        service._inspect_candidate_pykrx_enrichment = MagicMock(
            side_effect=[missing, missing]
        )

        result = service._repair_and_inspect_candidate_pykrx_enrichment(
            source_market_date=date(2026, 9, 11),
            symbols=['004370'],
            categories=['investor_flow_symbol', 'short_selling'],
        )

        enrichment_service.collect_for_market_date.assert_called_once_with(
            market_date=date(2026, 9, 11),
            markets=['KOSPI', 'KOSDAQ', 'KONEX'],
            symbols=['004370'],
            categories=['investor_flow_symbol', 'short_selling'],
            allow_kis_candidate_fallback=True,
            point_in_time_recovery=True,
        )
        self.assertFalse(result['004370']['allowed'])
        self.assertEqual(
            result['004370']['missing_categories'],
            ['investor_flow_symbol', 'short_selling'],
        )


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
