from contextlib import contextmanager
from datetime import date, datetime, timezone
from decimal import Decimal
import json
from pathlib import Path
import unittest
from unittest.mock import patch

from sqlalchemy import create_engine, event
from sqlalchemy.orm import sessionmaker

from app.agent_context.decider_replay_service import (
    DeciderReplayCostAssumptions,
    DeciderReplayDataset,
    DeciderReplayRepository,
    DeciderReplayService,
    DeciderReplayTrade,
    _actual_short_term_outcome_to_wait_conflict_trade,
    _build_wait_conflict_markdown_summary,
    _post_decision_eval_to_trade,
    _replay_alignment_gate,
)
from app.agent_context.roles import SHORT_TERM_DECIDER_AGENT_NAME, SIGNAL_DECIDER_AGENT_NAME
from app.agent_context.model_identity import build_observed_model_identity
from app.db_models import (
    AgentContextFeedback,
    Base,
    BrokerOrder,
    DeciderPostDecisionEvaluation,
    OrderRequest,
    SignalDeciderActualExecutionOutcome,
    SignalDeciderDecisionSnapshot,
    SignalDeciderExecutionPlanItem,
    SignalDeciderRealizedOutcome,
)


DEFAULT_NOTIONAL = Decimal('1000000')


def _enable_sqlite_foreign_keys(dbapi_connection, _connection_record) -> None:
    cursor = dbapi_connection.cursor()
    cursor.execute('PRAGMA foreign_keys=ON')
    cursor.close()


class FakeDeciderReplayRepository:
    def __init__(
        self,
        trades: list[DeciderReplayTrade],
        wait_conflict_trades: list[DeciderReplayTrade] | None = None,
        replay_dataset: DeciderReplayDataset | None = None,
    ) -> None:
        self.trades = list(trades)
        self.wait_conflict_trades = list(wait_conflict_trades or [])
        self.replay_dataset = replay_dataset
        self.calls: list[dict] = []
        self.wait_conflict_calls: list[dict] = []

    def list_replay_trades(
        self,
        **kwargs,
    ) -> DeciderReplayDataset | list[DeciderReplayTrade]:
        self.calls.append(kwargs)
        if self.replay_dataset is not None:
            return self.replay_dataset
        return list(self.trades)

    def list_wait_conflict_gate_trades(self, **kwargs) -> list[DeciderReplayTrade]:
        self.wait_conflict_calls.append(kwargs)
        return list(self.wait_conflict_trades)


def _trade(
    *,
    snapshot_id: int,
    symbol: str,
    gross_pnl_krw: int | float,
    trigger_price_drift_pct: float | None,
    confidence_score_10: float = 7.0,
    market_date: str = '2026-06-16',
    alignment_gate: dict | None = None,
    outcome_source: str = 'decider_post_decision_evaluation',
    outcome_resolved: bool = True,
    lifecycle_status: str | None = None,
    entry_filled_quantity: int = 10,
    closed_quantity: int | None = 10,
    open_quantity: int | None = 0,
) -> DeciderReplayTrade:
    gross = Decimal(str(gross_pnl_krw))
    return DeciderReplayTrade(
        replay_trade_id=f'post_decision_eval:{snapshot_id}',
        snapshot_id=snapshot_id,
        agent_name=SHORT_TERM_DECIDER_AGENT_NAME,
        symbol=symbol,
        market_date=date.fromisoformat(market_date),
        policy_version='short_term_decider_policy_test',
        baseline_gross_pnl_krw=gross,
        baseline_return_pct=float(gross / DEFAULT_NOTIONAL),
        baseline_notional_krw=DEFAULT_NOTIONAL,
        outcome_source=outcome_source,
        point_in_time={
            'snapshot_id': snapshot_id,
            'agent_name': SHORT_TERM_DECIDER_AGENT_NAME,
            'symbol': symbol,
            'market_date': market_date,
            'decision_action': 'buy',
            'policy_version': 'short_term_decider_policy_test',
            'confidence_score_10': confidence_score_10,
            'trigger_price_drift_pct': trigger_price_drift_pct,
            'risk_flags': [],
            'payload_context': {
                'response_payload': {'tactical_action': 'watch_for_trigger'},
                'request_payload': {'symbol_day_context': {'symbol': symbol}},
            },
            'alignment_gate': alignment_gate or {},
        },
        decision_timestamp=datetime(2026, 6, 16, 6, 0, tzinfo=timezone.utc),
        alignment_gate=alignment_gate,
        outcome_resolved=outcome_resolved,
        lifecycle_status=lifecycle_status,
        entry_filled_quantity=Decimal(entry_filled_quantity),
        closed_quantity=Decimal(closed_quantity) if closed_quantity is not None else None,
        open_quantity=Decimal(open_quantity) if open_quantity is not None else None,
    )


def _stale_watch_rule() -> dict:
    return {
        'rules': [
            {
                'name': 'stale_watch_trigger_drift',
                'predicates': [
                    {
                        'type': 'trigger_price_drift',
                        'max_below_trigger_pct': 0.03,
                    }
                ],
            }
        ]
    }


def _alignment_gate(alignment: str, action: str) -> dict:
    override = {'allowed': False}
    if alignment == 'wait_conflict_override':
        override = {
            'allowed': True,
            'metrics': {
                'buy_quality_score': 76,
                'return_percentile': 0.82,
                'return_percentile_status': 'available',
                'volume_percentile': 0.68,
                'volume_percentile_status': 'available',
                'reward_risk_ratio': 2.5,
                'raw_trading_value_krw': 9_000_000_000,
                'continuation_setup_detected': True,
            },
        }
    return {
        'schema_version': 'signal_decider_alignment_gate_v1',
        'signal_decider_alignment': alignment,
        'alignment_gate_action': action,
        'alignment_gate_reason': f'{alignment} test reason',
        'signal_decider_decision_snapshot_id': 900,
        'override': override,
    }


def _actual_wait_conflict_trade(
    *,
    closed_quantity: int,
    open_quantity: int,
    realized_pnl_krw: int,
    lifecycle_status: str | None = None,
    gate_mode: str = 'hard_block',
    probe_quantity: int = 1,
) -> DeciderReplayTrade:
    snapshot = SignalDeciderDecisionSnapshot(
        id=700,
        market_date=date(2026, 9, 1),
        symbol='004690',
        agent_name=SHORT_TERM_DECIDER_AGENT_NAME,
        policy_version='short_term_decider_policy_test',
        decision_action='buy',
    )
    plan_row = SignalDeciderExecutionPlanItem(
        id=701,
        source_market_date=date(2026, 9, 1),
        validation_payload={
            'alignment_gate': _alignment_gate(
                'wait_conflict_block',
                'report_only_block_candidate',
            )
        },
        traceability_payload={},
    )
    outcome = SignalDeciderActualExecutionOutcome(
        id=702,
        target_trade_date=date(2026, 9, 2),
        lifecycle_status=lifecycle_status or ('closed' if open_quantity == 0 else 'partially_closed'),
        entry_filled_quantity=Decimal('10'),
        entry_average_fill_price=Decimal('10000'),
        entry_notional_krw=Decimal('100000'),
        exit_filled_quantity=Decimal(closed_quantity),
        exit_average_fill_price=Decimal('10000'),
        exit_notional_krw=Decimal(closed_quantity) * Decimal('10000'),
        closed_quantity=Decimal(closed_quantity),
        open_quantity=Decimal(open_quantity),
        realized_pnl_krw=Decimal(realized_pnl_krw),
        realized_return_pct=Decimal(realized_pnl_krw) / Decimal('100000'),
    )
    return _actual_short_term_outcome_to_wait_conflict_trade(
        snapshot=snapshot,
        plan_row=plan_row,
        outcome=outcome,
        signal_decider_context=None,
        cost_assumptions=DeciderReplayCostAssumptions(),
        gate_mode=gate_mode,
        probe_quantity=probe_quantity,
        alignment_context_source='persisted_plan_alignment_gate',
    )


def _snapshot_object(*, snapshot_id: int = 1, symbol: str = '005930') -> SignalDeciderDecisionSnapshot:
    return SignalDeciderDecisionSnapshot(
        id=snapshot_id,
        feedback_id=None,
        market_date=date(2026, 6, 16),
        symbol=symbol,
        agent_name=SHORT_TERM_DECIDER_AGENT_NAME,
        agent_version='test-agent',
        decision_run_id='test-run',
        feedback_received_at=datetime(2026, 6, 16, 6, 0, tzinfo=timezone.utc),
        policy_version='short_term_decider_policy_test',
        decision_action='buy',
        preferred_action='buy',
        next_day_open_action='buy',
        entry_price_basis='next_open',
        target_price=110,
        expected_return_pct=0.10,
        stop_loss_price=90,
        suggested_holding_period='5 trading days',
        confidence_score_10=7,
        conviction_score_10=6,
        thesis_summary='Replay test snapshot.',
        supporting_evidence_payload=[],
        decision_risks_payload=[],
        invalidation_conditions_payload=[],
        data_quality_warnings_payload=[],
        specialist_disagreement_payload=[],
        traceability_payload={},
        input_feature_snapshot_payload={},
        request_payload={},
        response_payload={},
        response_summary_payload={
            'decision_provenance': {
                'model_identity': build_observed_model_identity(
                    role=SHORT_TERM_DECIDER_AGENT_NAME,
                    expected_model='openai/gpt-5.6-sol',
                    actual_provider='openai',
                    actual_model='gpt-5.6-sol',
                )
            }
        },
    )


class DeciderReplayServiceTests(unittest.TestCase):
    def test_replay_reinterprets_persisted_gate_without_recomputing_context(self) -> None:
        persisted = _alignment_gate('wait_conflict_block', 'report_only_block_candidate')

        hard_block = _replay_alignment_gate(persisted, gate_mode='hard_block')
        downsize = _replay_alignment_gate(persisted, gate_mode='downsize_probe')
        override = _replay_alignment_gate(
            _alignment_gate('wait_conflict_override', 'allow_by_override'),
            gate_mode='hard_block',
        )
        missing = _replay_alignment_gate(None, gate_mode='hard_block')

        self.assertEqual(hard_block['alignment_gate_action'], 'block')
        self.assertEqual(downsize['alignment_gate_action'], 'downsize_probe')
        self.assertEqual(override['alignment_gate_action'], 'allow_by_override')
        self.assertEqual(missing['signal_decider_alignment'], 'missing_alignment_evidence')
        self.assertEqual(missing['alignment_gate_action'], 'block')

    def test_replay_rejects_legacy_override_without_current_liquidity_evidence(self) -> None:
        persisted = _alignment_gate('wait_conflict_override', 'allow_by_override')
        persisted['override']['metrics'].pop('raw_trading_value_krw')

        for gate_mode, expected_action in (
            ('hard_block', 'block'),
            ('downsize_probe', 'downsize_probe'),
        ):
            with self.subTest(gate_mode=gate_mode):
                replayed = _replay_alignment_gate(persisted, gate_mode=gate_mode)

                self.assertEqual(replayed['signal_decider_alignment'], 'wait_conflict_block')
                self.assertEqual(replayed['alignment_gate_action'], expected_action)
                self.assertFalse(replayed['override']['allowed'])
                self.assertIn('liquidity_floor_ok', replayed['override']['failed_checks'])

    def test_wait_conflict_markdown_distinguishes_manual_approval_from_activation(self) -> None:
        markdown = _build_wait_conflict_markdown_summary(
            request={
                'market_date_from': '2026-08-03',
                'market_date_to': '2026-08-28',
                'date_window_basis': 'source_market_date',
                'candidate_gate_mode': 'hard_block',
                'probe_quantity': 1,
                'cost_assumptions': {
                    'fee_bps': 10,
                    'slippage_bps': 5,
                    'notional_krw': 1_000_000,
                },
            },
            metadata={
                'profitability_basis': 'estimated_net',
                'promotion_cost_gate': {'status': 'passed', 'reason': None},
            },
            summary={
                'baseline_trade_count': 20,
                'candidate_trade_count': 12,
                'blocked_candidate_count': 8,
                'allowed_by_override_count': 0,
                'skipped_losses': 6,
                'skipped_winners': 2,
                'gross_pnl_delta_krw': 500_000,
                'estimated_net_pnl_delta_krw': 450_000,
                'unresolved_open_candidate_count': 0,
                'missing_alignment_evidence_count': 0,
            },
            examples=[],
            weeks=[],
            unresolved_open_candidates=[],
            promotion_readiness={
                'ready_for_manual_approval': True,
                'ready': False,
            },
        )

        self.assertIn('- Manual approval may now be requested: true', markdown)
        self.assertIn('- Final activation ready: false', markdown)

    def test_rule_skips_losing_buy_and_reports_avoided_loser(self) -> None:
        repository = FakeDeciderReplayRepository(
            [
                _trade(
                    snapshot_id=1,
                    symbol='005930',
                    gross_pnl_krw=-50_000,
                    trigger_price_drift_pct=-0.04,
                ),
                _trade(
                    snapshot_id=2,
                    symbol='000660',
                    gross_pnl_krw=100_000,
                    trigger_price_drift_pct=-0.01,
                ),
            ]
        )
        service = DeciderReplayService(repository=repository)

        result = service.replay(
            market_date_from='2026-06-15',
            market_date_to='2026-06-19',
            agent_name=SHORT_TERM_DECIDER_AGENT_NAME,
            rule_config=_stale_watch_rule(),
        )

        self.assertEqual(result['summary']['baseline_trade_count'], 2)
        self.assertEqual(result['summary']['candidate_trade_count'], 1)
        self.assertEqual(result['summary']['skipped_buys'], 1)
        self.assertEqual(result['summary']['avoided_losers'], 1)
        self.assertEqual(result['summary']['skipped_winners'], 0)
        self.assertEqual(result['summary']['baseline']['gross_pnl_krw'], 50_000)
        self.assertEqual(result['summary']['candidate']['gross_pnl_krw'], 100_000)
        self.assertEqual(result['summary']['gross_pnl_delta_krw'], 50_000)
        self.assertEqual(result['examples'][0]['symbol'], '005930')
        self.assertIn('stale_watch_trigger_drift', result['examples'][0]['reason'])
        self.assertIn('Baseline trades: 2', result['markdown_summary'])
        self.assertEqual(repository.calls[0]['agent_name'], SHORT_TERM_DECIDER_AGENT_NAME)

    def test_rule_skips_winning_buy(self) -> None:
        repository = FakeDeciderReplayRepository(
            [
                _trade(
                    snapshot_id=10,
                    symbol='035420',
                    gross_pnl_krw=80_000,
                    trigger_price_drift_pct=-0.05,
                ),
                _trade(
                    snapshot_id=11,
                    symbol='068270',
                    gross_pnl_krw=-20_000,
                    trigger_price_drift_pct=-0.01,
                ),
            ]
        )
        service = DeciderReplayService(repository=repository)

        result = service.replay(
            market_date_from='2026-06-15',
            market_date_to='2026-06-19',
            agent_name=SHORT_TERM_DECIDER_AGENT_NAME,
            rule_config=_stale_watch_rule(),
        )

        self.assertEqual(result['summary']['skipped_buys'], 1)
        self.assertEqual(result['summary']['skipped_winners'], 1)
        self.assertEqual(result['summary']['avoided_losers'], 0)
        self.assertEqual(result['summary']['baseline']['gross_pnl_krw'], 60_000)
        self.assertEqual(result['summary']['candidate']['gross_pnl_krw'], -20_000)
        self.assertEqual(result['summary']['gross_pnl_delta_krw'], -80_000)
        self.assertEqual(result['examples'][0]['symbol'], '035420')

    def test_rule_keeps_baseline_when_no_predicate_matches(self) -> None:
        repository = FakeDeciderReplayRepository(
            [
                _trade(
                    snapshot_id=20,
                    symbol='005930',
                    gross_pnl_krw=30_000,
                    trigger_price_drift_pct=-0.01,
                ),
                _trade(
                    snapshot_id=21,
                    symbol='000660',
                    gross_pnl_krw=-10_000,
                    trigger_price_drift_pct=0.02,
                ),
            ]
        )
        service = DeciderReplayService(repository=repository)

        result = service.replay(
            market_date_from='2026-06-15',
            market_date_to='2026-06-19',
            agent_name=SHORT_TERM_DECIDER_AGENT_NAME,
            rule_config=_stale_watch_rule(),
        )

        self.assertEqual(result['summary']['skipped_buys'], 0)
        self.assertEqual(result['summary']['baseline'], result['summary']['candidate'])
        self.assertEqual(result['examples'], [])

    def test_fee_slippage_changes_net_pl_but_not_gross_pl(self) -> None:
        repository = FakeDeciderReplayRepository(
            [
                _trade(
                    snapshot_id=30,
                    symbol='005930',
                    gross_pnl_krw=10_000,
                    trigger_price_drift_pct=None,
                )
            ]
        )
        service = DeciderReplayService(repository=repository)

        zero_cost = service.replay(
            market_date_from='2026-06-15',
            market_date_to='2026-06-19',
            agent_name=SHORT_TERM_DECIDER_AGENT_NAME,
            rule_config=None,
            fee_bps=0,
            slippage_bps=0,
        )
        with_cost = service.replay(
            market_date_from='2026-06-15',
            market_date_to='2026-06-19',
            agent_name=SHORT_TERM_DECIDER_AGENT_NAME,
            rule_config=None,
            fee_bps=10,
            slippage_bps=5,
        )
        unrealistically_low_cost = service.replay(
            market_date_from='2026-06-15',
            market_date_to='2026-06-19',
            agent_name=SHORT_TERM_DECIDER_AGENT_NAME,
            rule_config=None,
            fee_bps='0.000001',
            slippage_bps='0.000001',
        )

        self.assertEqual(
            zero_cost['summary']['baseline']['gross_pnl_krw'],
            with_cost['summary']['baseline']['gross_pnl_krw'],
        )
        self.assertEqual(zero_cost['summary']['baseline']['gross_pnl_krw'], 10_000)
        self.assertEqual(zero_cost['summary']['baseline']['estimated_net_pnl_krw'], 10_000)
        self.assertEqual(with_cost['summary']['baseline']['estimated_net_pnl_krw'], 7_000)
        self.assertEqual(zero_cost['metadata']['profitability_basis'], 'gross_only')
        self.assertEqual(
            zero_cost['metadata']['promotion_cost_gate'],
            {
                'eligible': False,
                'status': 'ineligible',
                'reason': 'zero_fee_and_slippage_assumptions',
                'minimum_assumptions': {
                    'fee_bps': 10,
                    'slippage_bps': 5,
                },
                'effective_assumptions': {
                    'fee_bps': 0,
                    'slippage_bps': 0,
                },
            },
        )
        self.assertEqual(with_cost['metadata']['profitability_basis'], 'estimated_net')
        self.assertEqual(with_cost['metadata']['promotion_cost_gate']['status'], 'passed')
        self.assertEqual(
            with_cost['metadata']['promotion_cost_gate']['minimum_assumptions'],
            {'fee_bps': 10, 'slippage_bps': 5},
        )
        self.assertEqual(
            with_cost['metadata']['promotion_cost_gate']['effective_assumptions'],
            {'fee_bps': 10, 'slippage_bps': 5},
        )
        self.assertEqual(
            unrealistically_low_cost['metadata']['promotion_cost_gate']['status'],
            'ineligible',
        )
        self.assertEqual(
            unrealistically_low_cost['metadata']['promotion_cost_gate']['reason'],
            'below_minimum_fee_and_slippage_assumptions',
        )
        self.assertIn('- Profitability basis: gross_only', zero_cost['markdown_summary'])
        self.assertIn(
            '- Per-side cost assumptions: fee_bps=0, slippage_bps=0',
            zero_cost['markdown_summary'],
        )
        self.assertIn(
            '- Fallback notional assumption KRW: 1000000',
            zero_cost['markdown_summary'],
        )
        self.assertIn(
            '- Promotion cost gate: ineligible (zero_fee_and_slippage_assumptions)',
            zero_cost['markdown_summary'],
        )
        self.assertIn('- Profitability basis: estimated_net', with_cost['markdown_summary'])
        self.assertIn('- Promotion cost gate: passed', with_cost['markdown_summary'])

    def test_non_finite_cost_inputs_are_rejected(self) -> None:
        service = DeciderReplayService(repository=FakeDeciderReplayRepository([]))

        for field_name, value in (
            ('fee_bps', Decimal('NaN')),
            ('fee_bps', float('inf')),
            ('slippage_bps', Decimal('-Infinity')),
            ('notional_krw', 'NaN'),
        ):
            kwargs = {
                'fee_bps': 1,
                'slippage_bps': 1,
                'notional_krw': DEFAULT_NOTIONAL,
            }
            kwargs[field_name] = value
            with self.subTest(field_name=field_name, value=str(value)):
                with self.assertRaisesRegex(ValueError, f'{field_name} must be numeric'):
                    service.replay(
                        market_date_from='2026-06-15',
                        market_date_to='2026-06-19',
                        agent_name=SHORT_TERM_DECIDER_AGENT_NAME,
                        rule_config=None,
                        **kwargs,
                    )

    def test_invalid_rule_config_fails_closed_before_repository_access(self) -> None:
        invalid_configs = (
            {'rules': 'not-a-list'},
            {'rules': [{'name': 'typo', 'predicates': [{'type': 'confidnce', 'value': 5}]}]},
            {'rules': [{'name': 'bad-op', 'predicates': [{'field': 'symbol', 'op': 'approx', 'value': '005930'}]}]},
            {'rules': [{'name': 'ignored-operator', 'predicates': [{'field': 'symbol', 'operator': '!=', 'value': '005930'}]}]},
            {'rules': [{'name': 'wrong-op-type', 'predicates': [{'type': 'confidence', 'op': 'contains', 'value': 5}]}]},
            {'rules': [{'name': 'numeric-nan', 'predicates': [{'field': 'confidence_score_10', 'op': '>', 'value': 'Infinity'}]}]},
            {'rules': [{'name': 'ignored-rule-field', 'enabled': False, 'predicates': [{'field': 'symbol', 'value': '005930'}]}]},
            {'rules': [{'name': 'nan', 'predicates': [{'type': 'confidence', 'value': 'NaN'}]}]},
        )
        for rule_config in invalid_configs:
            repository = FakeDeciderReplayRepository([])
            service = DeciderReplayService(repository=repository)

            with self.subTest(rule_config=rule_config):
                with self.assertRaises(ValueError):
                    service.replay(
                        market_date_from='2026-06-15',
                        market_date_to='2026-06-19',
                        agent_name=SHORT_TERM_DECIDER_AGENT_NAME,
                        rule_config=rule_config,
                    )
                self.assertEqual(repository.calls, [])

    def test_replay_reports_censored_coverage_without_charging_synthetic_trades(self) -> None:
        economic_trade = _trade(
            snapshot_id=31,
            symbol='005930',
            gross_pnl_krw=10_000,
            trigger_price_drift_pct=None,
        )
        dataset = DeciderReplayDataset(
            trades=(economic_trade,),
            source_decision_count=3,
            excluded_reason_counts={'missing_fixed_horizon_outcome': 2},
            cohort_basis='fixed_horizon_counterfactual',
            diagnostics={'actual_lifecycle_outcomes_used_for_promotion': False},
        )
        service = DeciderReplayService(
            repository=FakeDeciderReplayRepository([], replay_dataset=dataset)
        )

        result = service.replay(
            market_date_from='2026-06-15',
            market_date_to='2026-06-19',
            agent_name=SHORT_TERM_DECIDER_AGENT_NAME,
            rule_config=None,
            fee_bps=10,
            slippage_bps=5,
        )

        self.assertEqual(result['summary']['baseline_trade_count'], 1)
        self.assertEqual(result['summary']['baseline']['estimated_net_pnl_krw'], 7_000)
        self.assertEqual(result['summary']['coverage']['source_decision_count'], 3)
        self.assertEqual(result['summary']['coverage']['censored_or_excluded_count'], 2)
        self.assertEqual(result['summary']['coverage']['economic_coverage_ratio'], 0.333333)
        self.assertFalse(
            result['summary']['coverage']['actual_lifecycle_outcomes_used_for_promotion']
        )

    def test_profitability_metrics_separate_gross_and_estimated_net_winners(self) -> None:
        service = DeciderReplayService(
            repository=FakeDeciderReplayRepository(
                [
                    _trade(
                        snapshot_id=32,
                        symbol='005930',
                        gross_pnl_krw=1_000,
                        trigger_price_drift_pct=-0.04,
                    )
                ]
            )
        )

        result = service.replay(
            market_date_from='2026-06-15',
            market_date_to='2026-06-19',
            agent_name=SHORT_TERM_DECIDER_AGENT_NAME,
            rule_config=_stale_watch_rule(),
            fee_bps=10,
            slippage_bps=5,
        )

        self.assertEqual(result['summary']['baseline']['gross_win_rate'], 1.0)
        self.assertEqual(result['summary']['baseline']['estimated_net_win_rate'], 0.0)
        self.assertEqual(result['summary']['skipped_winners'], 1)
        self.assertEqual(result['summary']['avoided_net_losers'], 1)
        self.assertEqual(result['summary']['promotion_metrics']['basis'], 'estimated_net')

    def test_neither_hit_uses_last_observed_close_instead_of_zero_return(self) -> None:
        evaluation = DeciderPostDecisionEvaluation(
            id=70,
            feedback_id=1,
            decision_snapshot_id=1,
            agent_name=SHORT_TERM_DECIDER_AGENT_NAME,
            market_date=date(2026, 6, 16),
            symbol='005930',
            window_days=5,
            decision_action='buy',
            entry_price=100,
            target_price=110,
            stop_loss_price=90,
            outcome_label='neither_hit_within_5d',
            outcome_reason='Neither threshold was touched.',
            data_quality_warnings_payload=[],
            metadata_payload={
                'observed_price_path': [
                    {'trade_date': '2026-06-17', 'close_price': 101},
                    {'trade_date': '2026-06-23', 'close_price': 104},
                ]
            },
        )

        trade = _post_decision_eval_to_trade(
            snapshot=_snapshot_object(),
            evaluation=evaluation,
            cost_assumptions=DeciderReplayCostAssumptions(),
        )

        self.assertIsNotNone(trade)
        self.assertEqual(trade.baseline_return_pct, 0.04)
        self.assertEqual(trade.baseline_gross_pnl_krw, Decimal('40000.00'))

    def test_neither_hit_without_observed_close_is_excluded(self) -> None:
        evaluation = DeciderPostDecisionEvaluation(
            id=71,
            feedback_id=1,
            decision_snapshot_id=1,
            agent_name=SHORT_TERM_DECIDER_AGENT_NAME,
            market_date=date(2026, 6, 16),
            symbol='005930',
            window_days=5,
            decision_action='buy',
            entry_price=100,
            target_price=110,
            stop_loss_price=90,
            outcome_label='neither_hit_within_5d',
            outcome_reason='Neither threshold was touched.',
            data_quality_warnings_payload=[],
            metadata_payload={'observed_price_path': []},
        )

        trade = _post_decision_eval_to_trade(
            snapshot=_snapshot_object(),
            evaluation=evaluation,
            cost_assumptions=DeciderReplayCostAssumptions(),
        )

        self.assertIsNone(trade)

    def test_wait_conflict_gate_replay_reports_tradeoff_counts(self) -> None:
        repository = FakeDeciderReplayRepository(
            [],
            wait_conflict_trades=[
                _trade(
                    snapshot_id=100,
                    symbol='005960',
                    gross_pnl_krw=-50_000,
                    trigger_price_drift_pct=None,
                    market_date='2026-07-01',
                    outcome_source='actual_short_term_execution_outcome',
                    alignment_gate=_alignment_gate('wait_conflict_block', 'block'),
                ),
                _trade(
                    snapshot_id=101,
                    symbol='013580',
                    gross_pnl_krw=80_000,
                    trigger_price_drift_pct=None,
                    market_date='2026-07-02',
                    outcome_source='actual_short_term_execution_outcome',
                    alignment_gate=_alignment_gate('wait_conflict_override', 'allow_by_override'),
                ),
                _trade(
                    snapshot_id=102,
                    symbol='005930',
                    gross_pnl_krw=10_000,
                    trigger_price_drift_pct=None,
                    market_date='2026-07-03',
                    outcome_source='actual_short_term_execution_outcome',
                    alignment_gate=_alignment_gate('aligned', 'allow'),
                ),
            ],
        )
        service = DeciderReplayService(repository=repository)

        result = service.replay_wait_conflict_gate(
            market_date_from='2026-06-29',
            market_date_to='2026-07-03',
        )

        self.assertEqual(result['schema_version'], 'short_term_wait_conflict_gate_replay_v1')
        self.assertEqual(result['metadata']['promotion_cost_gate']['status'], 'ineligible')
        self.assertIn('- Profitability basis: gross_only', result['markdown_summary'])
        self.assertIn('- Fallback notional assumption KRW: 1000000', result['markdown_summary'])
        self.assertIn('- Promotion cost gate: ineligible', result['markdown_summary'])
        self.assertEqual(result['summary']['baseline_trade_count'], 3)
        self.assertEqual(result['summary']['candidate_trade_count'], 2)
        self.assertEqual(result['summary']['blocked_candidate_count'], 1)
        self.assertEqual(result['summary']['allowed_by_override_count'], 1)
        self.assertEqual(result['summary']['skipped_losses'], 1)
        self.assertEqual(result['summary']['skipped_winners'], 0)
        self.assertEqual(result['summary']['gross_pnl_delta_krw'], 50_000)
        self.assertEqual(result['summary']['estimated_net_pnl_delta_krw'], 50_000)
        self.assertEqual(result['examples'][0]['symbol'], '005960')
        self.assertEqual(repository.wait_conflict_calls[0]['gate_mode'], 'hard_block')

    def test_wait_conflict_downsize_tradeoffs_use_removed_quantity_share(self) -> None:
        downsized_loss_gate = {
            **_alignment_gate('wait_conflict_block', 'downsize_probe'),
            'requested_quantity': 10,
            'final_quantity': 2,
        }
        downsized_winner_gate = {
            **_alignment_gate('wait_conflict_block', 'downsize_probe'),
            'requested_quantity': 5,
            'final_quantity': 2,
        }
        repository = FakeDeciderReplayRepository(
            [],
            wait_conflict_trades=[
                _trade(
                    snapshot_id=110,
                    symbol='005930',
                    gross_pnl_krw=-10_000,
                    trigger_price_drift_pct=None,
                    market_date='2026-07-01',
                    outcome_source='actual_short_term_execution_outcome',
                    alignment_gate=downsized_loss_gate,
                ),
                _trade(
                    snapshot_id=111,
                    symbol='000660',
                    gross_pnl_krw=5_000,
                    trigger_price_drift_pct=None,
                    market_date='2026-07-02',
                    outcome_source='actual_short_term_execution_outcome',
                    alignment_gate=downsized_winner_gate,
                ),
            ],
        )
        result = DeciderReplayService(repository=repository).replay_wait_conflict_gate(
            market_date_from='2026-06-29',
            market_date_to='2026-07-03',
            gate_mode='downsize_probe',
            probe_quantity=2,
        )

        self.assertEqual(result['summary']['baseline_realized_pnl_krw'], -5_000)
        self.assertEqual(result['summary']['counterfactual_realized_pnl_krw'], 0)
        self.assertEqual(result['summary']['net_pnl_delta_krw'], 5_000)
        self.assertEqual(result['summary']['skipped_losses'], 1)
        self.assertEqual(result['summary']['skipped_losses_avoided_krw'], 8_000)
        self.assertEqual(result['summary']['skipped_winners'], 1)
        self.assertEqual(result['summary']['skipped_winners_gross_pnl_krw'], 3_000)
        self.assertEqual(result['request']['probe_quantity'], 2)
        self.assertEqual(repository.wait_conflict_calls[0]['probe_quantity'], 2)
        self.assertIn('- Replay probe quantity: 2', result['markdown_summary'])
        self.assertTrue(
            result['promotion_readiness']['checks']['avoided_losses_exceed_skipped_winners']
        )

    def test_wait_conflict_gate_rejects_invalid_probe_quantities(self) -> None:
        service = DeciderReplayService(repository=FakeDeciderReplayRepository([]))

        for probe_quantity in (0, 101, '1.5', 'not-a-number', True):
            with self.subTest(probe_quantity=probe_quantity):
                with self.assertRaisesRegex(
                    ValueError,
                    'probe_quantity must be an integer from 1 through 100',
                ):
                    service.replay_wait_conflict_gate(
                        market_date_from='2026-06-29',
                        market_date_to='2026-07-03',
                        probe_quantity=probe_quantity,
                    )

    def test_wait_conflict_promotion_uses_estimated_net_delta_when_costs_reverse_gross(self) -> None:
        blocked_loss = _trade(
            snapshot_id=112,
            symbol='005930',
            gross_pnl_krw=-100,
            trigger_price_drift_pct=None,
            market_date='2026-07-01',
            outcome_source='actual_short_term_execution_outcome',
            alignment_gate=_alignment_gate('wait_conflict_block', 'block'),
        )

        def metric_block(trades, *, cost_assumptions):
            self.assertGreater(cost_assumptions.fee_bps + cost_assumptions.slippage_bps, 0)
            has_baseline_trade = bool(trades)
            return {
                'trade_count': len(trades),
                'gross_pnl_krw': -100 if has_baseline_trade else 0,
                'estimated_net_pnl_krw': 100 if has_baseline_trade else 0,
                'win_rate': 0 if has_baseline_trade else None,
                'average_return_pct': -0.0001 if has_baseline_trade else None,
                'max_drawdown_proxy_krw': None,
            }

        with patch('app.agent_context.decider_replay_service._metric_block', side_effect=metric_block):
            result = DeciderReplayService(
                repository=FakeDeciderReplayRepository([], wait_conflict_trades=[blocked_loss])
            ).replay_wait_conflict_gate(
                market_date_from='2026-06-29',
                market_date_to='2026-07-03',
                fee_bps=10,
                slippage_bps=5,
            )

        self.assertGreater(result['summary']['gross_pnl_delta_krw'], 0)
        self.assertLessEqual(result['summary']['estimated_net_pnl_delta_krw'], 0)
        self.assertEqual(
            result['summary']['net_pnl_delta_krw'],
            result['summary']['estimated_net_pnl_delta_krw'],
        )
        self.assertFalse(result['promotion_readiness']['checks']['positive_net_pnl_delta'])

    def test_wait_conflict_gate_replay_returns_empty_data_reason(self) -> None:
        service = DeciderReplayService(repository=FakeDeciderReplayRepository([]))

        result = service.replay_wait_conflict_gate(
            market_date_from='2026-06-29',
            market_date_to='2026-07-03',
        )

        self.assertEqual(result['summary']['baseline_trade_count'], 0)
        self.assertEqual(result['summary']['empty_data_reason'], 'no_actual_short_term_entry_fills_in_range')
        self.assertIn('actual short-term execution outcomes only', result['markdown_summary'])

    def test_wait_conflict_gate_rolling_window_covers_four_completed_weeks(self) -> None:
        repository = FakeDeciderReplayRepository([])
        result = DeciderReplayService(repository=repository).replay_wait_conflict_gate(
            as_of_date_kst='2026-09-06',
        )

        self.assertEqual(result['request']['market_date_from'], '2026-08-10')
        self.assertEqual(result['request']['market_date_to'], '2026-09-04')
        self.assertEqual(result['request']['window_mode'], 'rolling_completed_weeks')
        self.assertEqual(result['request']['completed_weeks'], 4)
        self.assertEqual(result['request']['probe_quantity'], 1)
        self.assertEqual(result['summary']['completed_week_count'], 0)
        self.assertEqual(len(result['weeks']), 4)
        self.assertEqual(repository.wait_conflict_calls[0]['market_date_from'], date(2026, 8, 10))
        self.assertEqual(repository.wait_conflict_calls[0]['market_date_to'], date(2026, 9, 4))
        self.assertEqual(repository.wait_conflict_calls[0]['probe_quantity'], 1)

    def test_wait_conflict_gate_excludes_empty_weeks_from_promotion_count(self) -> None:
        concentrated_conflicts = [
            _trade(
                snapshot_id=120 + index,
                symbol=f'{index:06d}',
                gross_pnl_krw=-1_000,
                trigger_price_drift_pct=None,
                market_date='2026-08-10',
                outcome_source='actual_short_term_execution_outcome',
                alignment_gate=_alignment_gate('wait_conflict_block', 'block'),
            )
            for index in range(20)
        ]
        result = DeciderReplayService(
            repository=FakeDeciderReplayRepository(
                [],
                wait_conflict_trades=concentrated_conflicts,
            )
        ).replay_wait_conflict_gate(
            completed_weeks=6,
            as_of_date_kst='2026-09-06',
        )

        self.assertEqual(result['summary']['resolved_wait_conflict_count'], 20)
        self.assertTrue(result['promotion_readiness']['checks']['resolved_wait_conflicts_met'])
        self.assertEqual(result['request']['completed_weeks'], 6)
        self.assertEqual(result['summary']['completed_week_count'], 1)
        self.assertFalse(result['promotion_readiness']['checks']['completed_weeks_met'])

    def test_wait_conflict_gate_separates_unresolved_open_candidates(self) -> None:
        open_trade = _trade(
            snapshot_id=200,
            symbol='003550',
            gross_pnl_krw=0,
            trigger_price_drift_pct=None,
            market_date='2026-09-01',
            outcome_source='actual_short_term_execution_outcome',
            alignment_gate=_alignment_gate('wait_conflict_block', 'block'),
            outcome_resolved=False,
            lifecycle_status='open',
            closed_quantity=0,
            open_quantity=8,
        )
        result = DeciderReplayService(
            repository=FakeDeciderReplayRepository([], wait_conflict_trades=[open_trade])
        ).replay_wait_conflict_gate(
            market_date_from='2026-08-31',
            market_date_to='2026-09-04',
        )

        self.assertEqual(result['summary']['baseline_trade_count'], 0)
        self.assertEqual(result['summary']['unresolved_open_candidate_count'], 1)
        self.assertEqual(result['unresolved_open_candidates'][0]['symbol'], '003550')
        self.assertIsNone(result['unresolved_open_candidates'][0]['realized_pnl_krw'])

    def test_partial_exit_with_realized_pnl_stays_out_of_resolved_replay_metrics(self) -> None:
        partial_trade = _actual_wait_conflict_trade(
            closed_quantity=4,
            open_quantity=6,
            realized_pnl_krw=-4000,
        )

        result = DeciderReplayService(
            repository=FakeDeciderReplayRepository([], wait_conflict_trades=[partial_trade])
        ).replay_wait_conflict_gate(
            market_date_from='2026-08-31',
            market_date_to='2026-09-04',
        )

        self.assertFalse(partial_trade.outcome_resolved)
        self.assertFalse(partial_trade.point_in_time['realized_outcome_available'])
        self.assertEqual(result['summary']['baseline_trade_count'], 0)
        self.assertEqual(result['summary']['candidate_trade_count'], 0)
        self.assertEqual(result['summary']['baseline_realized_pnl_krw'], 0)
        self.assertEqual(result['summary']['counterfactual_realized_pnl_krw'], 0)
        self.assertEqual(result['summary']['skipped_losses'], 0)
        self.assertEqual(result['summary']['resolved_wait_conflict_count'], 0)
        self.assertEqual(result['summary']['unresolved_wait_conflict_count'], 1)
        self.assertEqual(result['summary']['unresolved_open_candidate_count'], 1)
        self.assertEqual(result['unresolved_open_candidates'][0]['symbol'], '004690')
        self.assertIsNone(result['unresolved_open_candidates'][0]['realized_pnl_krw'])
        self.assertFalse(result['promotion_readiness']['checks']['resolved_wait_conflicts_met'])

    def test_fully_sold_active_partial_entry_stays_out_of_resolved_replay_metrics(self) -> None:
        partial_entry_trade = _actual_wait_conflict_trade(
            closed_quantity=5,
            open_quantity=0,
            realized_pnl_krw=20000,
            lifecycle_status='unresolved',
        )

        result = DeciderReplayService(
            repository=FakeDeciderReplayRepository([], wait_conflict_trades=[partial_entry_trade])
        ).replay_wait_conflict_gate(
            market_date_from='2026-08-31',
            market_date_to='2026-09-04',
        )

        self.assertFalse(partial_entry_trade.outcome_resolved)
        self.assertFalse(partial_entry_trade.point_in_time['realized_outcome_available'])
        self.assertEqual(result['summary']['baseline_trade_count'], 0)
        self.assertEqual(result['summary']['candidate_trade_count'], 0)
        self.assertEqual(result['summary']['baseline_realized_pnl_krw'], 0)
        self.assertEqual(result['summary']['counterfactual_realized_pnl_krw'], 0)
        self.assertEqual(result['summary']['resolved_wait_conflict_count'], 0)
        self.assertEqual(result['summary']['unresolved_wait_conflict_count'], 1)
        self.assertEqual(result['summary']['unresolved_open_candidate_count'], 1)
        self.assertIsNone(result['unresolved_open_candidates'][0]['realized_pnl_krw'])
        self.assertFalse(result['promotion_readiness']['checks']['resolved_wait_conflicts_met'])

    def test_fully_closed_exit_contributes_to_resolved_replay_metrics(self) -> None:
        closed_trade = _actual_wait_conflict_trade(
            closed_quantity=10,
            open_quantity=0,
            realized_pnl_krw=-4000,
        )

        result = DeciderReplayService(
            repository=FakeDeciderReplayRepository([], wait_conflict_trades=[closed_trade])
        ).replay_wait_conflict_gate(
            market_date_from='2026-08-31',
            market_date_to='2026-09-04',
        )

        self.assertTrue(closed_trade.outcome_resolved)
        self.assertTrue(closed_trade.point_in_time['realized_outcome_available'])
        self.assertEqual(result['summary']['baseline_trade_count'], 1)
        self.assertEqual(result['summary']['candidate_trade_count'], 0)
        self.assertEqual(result['summary']['baseline_realized_pnl_krw'], -4000)
        self.assertEqual(result['summary']['counterfactual_realized_pnl_krw'], 0)
        self.assertEqual(result['summary']['skipped_losses'], 1)
        self.assertEqual(result['summary']['skipped_losses_avoided_krw'], 4000)
        self.assertEqual(result['summary']['resolved_wait_conflict_count'], 1)
        self.assertEqual(result['summary']['unresolved_wait_conflict_count'], 0)
        self.assertEqual(result['summary']['unresolved_open_candidate_count'], 0)
        self.assertEqual(result['unresolved_open_candidates'], [])

    def test_issue_547_week_regression_matches_reported_counterfactual(self) -> None:
        fixture_path = Path(__file__).parent / 'fixtures' / 'wait_conflict_week_2026-08-31.json'
        fixture = json.loads(fixture_path.read_text())
        conflicts = [
            _trade(
                snapshot_id=300 + index,
                symbol=row['symbol'],
                gross_pnl_krw=row['realized_pnl_krw'],
                trigger_price_drift_pct=None,
                market_date='2026-09-04',
                outcome_source='actual_short_term_execution_outcome',
                alignment_gate=_alignment_gate('wait_conflict_block', 'block'),
            )
            for index, row in enumerate(fixture['resolved_wait_conflicts'])
        ]
        non_blocked_balance = _trade(
            snapshot_id=399,
            symbol='__NON_BLOCKED_REALIZED__',
            gross_pnl_krw=fixture['expected']['counterfactual_realized_pnl_krw'],
            trigger_price_drift_pct=None,
            market_date='2026-09-04',
            outcome_source='actual_short_term_execution_outcome',
            alignment_gate=_alignment_gate('aligned', 'allow'),
        )
        result = DeciderReplayService(
            repository=FakeDeciderReplayRepository(
                [],
                wait_conflict_trades=[*conflicts, non_blocked_balance],
            )
        ).replay_wait_conflict_gate(
            market_date_from=fixture['window_kst']['market_date_from'],
            market_date_to=fixture['window_kst']['market_date_to'],
        )

        expected = fixture['expected']
        self.assertEqual(result['summary']['skipped_losses'], expected['skipped_losses'])
        self.assertEqual(
            result['summary']['skipped_losses_avoided_krw'],
            expected['skipped_losses_avoided_krw'],
        )
        self.assertEqual(result['summary']['skipped_winners'], expected['skipped_winners'])
        self.assertEqual(
            result['summary']['skipped_winners_gross_pnl_krw'],
            expected['skipped_winners_gross_pnl_krw'],
        )
        self.assertEqual(result['summary']['baseline_realized_pnl_krw'], expected['baseline_realized_pnl_krw'])
        self.assertEqual(
            result['summary']['counterfactual_realized_pnl_krw'],
            expected['counterfactual_realized_pnl_krw'],
        )
        self.assertEqual(result['summary']['net_pnl_delta_krw'], expected['net_pnl_delta_krw'])


class DeciderReplayRepositoryTests(unittest.TestCase):
    def test_replay_dataset_excludes_other_and_unverified_model_cohorts(self) -> None:
        engine = create_engine('sqlite+pysqlite:///:memory:')
        tables = [
            SignalDeciderDecisionSnapshot.__table__,
            SignalDeciderRealizedOutcome.__table__,
            DeciderPostDecisionEvaluation.__table__,
            SignalDeciderActualExecutionOutcome.__table__,
        ]
        Base.metadata.create_all(engine, tables=tables)
        TestSessionLocal = sessionmaker(bind=engine, future=True)

        @contextmanager
        def test_session_scope():
            with TestSessionLocal() as session:
                yield session

        expected_identity = build_observed_model_identity(
            role=SHORT_TERM_DECIDER_AGENT_NAME,
            expected_model='openai/gpt-5.6-sol',
            actual_provider='openai',
            actual_model='gpt-5.6-sol',
        )
        other_identity = {
            **expected_identity,
            'expected_model': 'openai/gpt-5.7',
            'actual_model': 'gpt-5.7',
            'actual_model_ref': 'openai/gpt-5.7',
            'cohort': f'{SHORT_TERM_DECIDER_AGENT_NAME}|openai/gpt-5.7',
        }
        try:
            with TestSessionLocal() as session:
                for snapshot_id, symbol, identity in (
                    (1, '111111', expected_identity),
                    (2, '222222', other_identity),
                    (3, '333333', None),
                ):
                    snapshot = _snapshot_object(snapshot_id=snapshot_id, symbol=symbol)
                    snapshot.response_payload = {'tactical_action': 'scalp_buy_candidate'}
                    snapshot.response_summary_payload = {
                        'decision_provenance': {'model_identity': identity}
                        if identity is not None
                        else {}
                    }
                    session.add(snapshot)
                    session.flush()
                    session.add(
                        SignalDeciderRealizedOutcome(
                            decision_snapshot_id=snapshot.id,
                            market_date=snapshot.market_date,
                            symbol=symbol,
                            policy_version=snapshot.policy_version,
                            decision_action='buy',
                            horizon_days=5,
                            outcome_status='complete',
                            bars_observed=5,
                            close_return_pct=Decimal('0.01'),
                        )
                    )
                session.commit()

            with patch('app.agent_context.decider_replay_service.session_scope', test_session_scope):
                dataset = DeciderReplayRepository().list_replay_trades(
                    market_date_from=date(2026, 6, 16),
                    market_date_to=date(2026, 6, 16),
                    agent_name=SHORT_TERM_DECIDER_AGENT_NAME,
                    symbols=None,
                    horizon_days=5,
                    cost_assumptions=DeciderReplayCostAssumptions(),
                )

            self.assertEqual([trade.snapshot_id for trade in dataset], [1])
            self.assertEqual(dataset.source_decision_count, 3)
            self.assertEqual(
                dataset.excluded_reason_counts,
                {
                    'model_identity_cohort_mismatch': 1,
                    'model_identity_unverified': 1,
                },
            )
            self.assertEqual(
                dataset.diagnostics['model_identity_cohort'],
                f'{SHORT_TERM_DECIDER_AGENT_NAME}|openai/gpt-5.6-sol',
            )
        finally:
            Base.metadata.drop_all(engine, tables=reversed(tables))
            engine.dispose()

    def test_short_term_replay_uses_tactical_action_instead_of_legacy_generic_buy(self) -> None:
        engine = create_engine('sqlite+pysqlite:///:memory:')
        event.listen(engine, 'connect', _enable_sqlite_foreign_keys)
        tables = [
            AgentContextFeedback.__table__,
            SignalDeciderDecisionSnapshot.__table__,
            SignalDeciderRealizedOutcome.__table__,
            DeciderPostDecisionEvaluation.__table__,
            SignalDeciderActualExecutionOutcome.__table__,
        ]
        Base.metadata.create_all(engine, tables=tables)
        TestSessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)

        @contextmanager
        def test_session_scope():
            session = TestSessionLocal()
            try:
                yield session
                session.commit()
            except Exception:
                session.rollback()
                raise
            finally:
                session.close()

        try:
            with TestSessionLocal() as session:
                snapshot_specs = [
                    (1, SHORT_TERM_DECIDER_AGENT_NAME, '111111', {'tactical_action': 'watch_for_trigger'}),
                    (2, SHORT_TERM_DECIDER_AGENT_NAME, '222222', {'tactical_action': 'reject'}),
                    (3, SHORT_TERM_DECIDER_AGENT_NAME, '333333', {'tactical_action': 'scalp_buy_candidate'}),
                    (4, SHORT_TERM_DECIDER_AGENT_NAME, '444444', {}),
                    (5, SIGNAL_DECIDER_AGENT_NAME, '555555', {}),
                ]
                for snapshot_id, agent_name, symbol, response_payload in snapshot_specs:
                    snapshot = _snapshot_object(snapshot_id=snapshot_id, symbol=symbol)
                    snapshot.agent_name = agent_name
                    snapshot.response_summary_payload = {
                        'decision_provenance': {
                            'model_identity': build_observed_model_identity(
                                role=agent_name,
                                expected_model='openai/gpt-5.6-sol',
                                actual_provider='openai',
                                actual_model='gpt-5.6-sol',
                            )
                        }
                    }
                    snapshot.response_payload = response_payload
                    session.add(snapshot)
                    session.flush()
                    session.add(
                        SignalDeciderRealizedOutcome(
                            decision_snapshot_id=snapshot.id,
                            feedback_id=None,
                            market_date=snapshot.market_date,
                            symbol=symbol,
                            policy_version=snapshot.policy_version,
                            decision_action='buy',
                            horizon_days=5,
                            outcome_status='complete',
                            bars_observed=5,
                            close_return_pct=Decimal('0.01'),
                        )
                    )
                session.commit()

            with patch('app.agent_context.decider_replay_service.session_scope', test_session_scope):
                repository = DeciderReplayRepository()
                short_term_dataset = repository.list_replay_trades(
                    market_date_from=date(2026, 6, 16),
                    market_date_to=date(2026, 6, 16),
                    agent_name=SHORT_TERM_DECIDER_AGENT_NAME,
                    symbols=None,
                    horizon_days=5,
                    cost_assumptions=DeciderReplayCostAssumptions(),
                )
                signal_dataset = repository.list_replay_trades(
                    market_date_from=date(2026, 6, 16),
                    market_date_to=date(2026, 6, 16),
                    agent_name=SIGNAL_DECIDER_AGENT_NAME,
                    symbols=None,
                    horizon_days=5,
                    cost_assumptions=DeciderReplayCostAssumptions(),
                )

            self.assertEqual(short_term_dataset.source_decision_count, 4)
            self.assertEqual([trade.snapshot_id for trade in short_term_dataset], [3])
            self.assertEqual(
                short_term_dataset.excluded_reason_counts,
                {
                    'invalid_short_term_tactical_action': 1,
                    'short_term_tactical_action_not_buy': 2,
                },
            )
            self.assertEqual(signal_dataset.source_decision_count, 1)
            self.assertEqual([trade.snapshot_id for trade in signal_dataset], [5])
            self.assertEqual(signal_dataset.excluded_reason_counts, {})
        finally:
            Base.metadata.drop_all(engine, tables=reversed(tables))
            engine.dispose()

    def test_replay_uses_latest_snapshot_per_feedback_and_keeps_null_feedback_independent(self) -> None:
        engine = create_engine('sqlite+pysqlite:///:memory:')
        event.listen(engine, 'connect', _enable_sqlite_foreign_keys)
        tables = [
            AgentContextFeedback.__table__,
            SignalDeciderDecisionSnapshot.__table__,
            SignalDeciderRealizedOutcome.__table__,
            DeciderPostDecisionEvaluation.__table__,
            SignalDeciderActualExecutionOutcome.__table__,
        ]
        Base.metadata.create_all(engine, tables=tables)
        TestSessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)

        @contextmanager
        def test_session_scope():
            session = TestSessionLocal()
            try:
                yield session
                session.commit()
            except Exception:
                session.rollback()
                raise
            finally:
                session.close()

        try:
            with TestSessionLocal() as session:
                feedback = AgentContextFeedback(
                    market_date=date(2026, 6, 16),
                    symbol='005930',
                    agent_name=SIGNAL_DECIDER_AGENT_NAME,
                    summary_payload={'summary': 'same semantic feedback'},
                    feedback_payload={'preferred_action': 'buy'},
                    received_at=datetime(2026, 6, 16, 6, 0, tzinfo=timezone.utc),
                )
                session.add(feedback)
                session.flush()

                snapshot_specs = [
                    (
                        11,
                        feedback.id,
                        date(2026, 6, 16),
                        '005930',
                        'buy',
                        'short_term_decider_policy_test',
                        datetime(2026, 6, 16, 6, 0, tzinfo=timezone.utc),
                    ),
                    (
                        12,
                        feedback.id,
                        date(2026, 6, 15),
                        '005930',
                        'wait',
                        'unrelated_policy_v9',
                        datetime(2026, 6, 17, 7, 0, tzinfo=timezone.utc),
                    ),
                    (
                        13,
                        None,
                        date(2026, 6, 16),
                        '000660',
                        'buy',
                        'short_term_decider_policy_test',
                        datetime(2026, 6, 16, 8, 0, tzinfo=timezone.utc),
                    ),
                    (
                        14,
                        None,
                        date(2026, 6, 16),
                        '000660',
                        'buy',
                        'short_term_decider_policy_test',
                        datetime(2026, 6, 16, 9, 0, tzinfo=timezone.utc),
                    ),
                    (
                        15,
                        None,
                        date(2026, 6, 16),
                        '035420',
                        'buy',
                        'unrelated_policy_v9',
                        datetime(2026, 6, 16, 10, 0, tzinfo=timezone.utc),
                    ),
                ]
                for (
                    snapshot_id,
                    feedback_id,
                    market_date,
                    symbol,
                    action,
                    policy_version,
                    created_at,
                ) in snapshot_specs:
                    snapshot = _snapshot_object(snapshot_id=snapshot_id, symbol=symbol)
                    snapshot.feedback_id = feedback_id
                    snapshot.market_date = market_date
                    snapshot.agent_name = SIGNAL_DECIDER_AGENT_NAME
                    snapshot.response_summary_payload = {
                        'decision_provenance': {
                            'model_identity': build_observed_model_identity(
                                role=SIGNAL_DECIDER_AGENT_NAME,
                                expected_model='openai/gpt-5.6-sol',
                                actual_provider='openai',
                                actual_model='gpt-5.6-sol',
                            )
                        }
                    }
                    snapshot.decision_run_id = f'run-{snapshot_id}'
                    snapshot.decision_action = action
                    snapshot.policy_version = policy_version
                    snapshot.created_at = created_at
                    snapshot.updated_at = created_at
                    session.add(snapshot)
                    session.flush()
                    session.add(
                        SignalDeciderRealizedOutcome(
                            decision_snapshot_id=snapshot.id,
                            feedback_id=feedback_id,
                            market_date=market_date,
                            symbol=symbol,
                            policy_version=snapshot.policy_version,
                            decision_action='buy',
                            horizon_days=5,
                            outcome_status='complete',
                            bars_observed=5,
                            close_return_pct=Decimal('0.01'),
                        )
                    )
                session.commit()

            with patch('app.agent_context.decider_replay_service.session_scope', test_session_scope):
                dataset = DeciderReplayRepository().list_replay_trades(
                    market_date_from=date(2026, 6, 15),
                    market_date_to=date(2026, 6, 16),
                    agent_name=SIGNAL_DECIDER_AGENT_NAME,
                    symbols=None,
                    policy_versions=['short_term_decider_policy_test'],
                    horizon_days=5,
                    cost_assumptions=DeciderReplayCostAssumptions(),
                )

            self.assertEqual(dataset.source_decision_count, 2)
            self.assertEqual({trade.snapshot_id for trade in dataset}, {13, 14})
        finally:
            Base.metadata.drop_all(engine, tables=reversed(tables))
            engine.dispose()

    def test_wait_conflict_gate_filters_source_date_and_applies_one_replay_probe(self) -> None:
        engine = create_engine('sqlite+pysqlite:///:memory:')
        event.listen(engine, 'connect', _enable_sqlite_foreign_keys)
        tables = [
            AgentContextFeedback.__table__,
            OrderRequest.__table__,
            BrokerOrder.__table__,
            SignalDeciderDecisionSnapshot.__table__,
            SignalDeciderRealizedOutcome.__table__,
            DeciderPostDecisionEvaluation.__table__,
            SignalDeciderExecutionPlanItem.__table__,
            SignalDeciderActualExecutionOutcome.__table__,
        ]
        Base.metadata.create_all(engine, tables=tables)
        TestSessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)

        @contextmanager
        def test_session_scope():
            session = TestSessionLocal()
            try:
                yield session
                session.commit()
            except Exception:
                session.rollback()
                raise
            finally:
                session.close()

        def seed_outcome(
            *,
            session,
            symbol: str,
            source_market_date: date,
            target_trade_date: date,
            realized_pnl_krw: Decimal,
            alignment_gate: dict | None = None,
            open_quantity: Decimal = Decimal('0'),
        ) -> None:
            snapshot = SignalDeciderDecisionSnapshot(
                feedback_id=None,
                market_date=source_market_date,
                symbol=symbol,
                agent_name=SHORT_TERM_DECIDER_AGENT_NAME,
                agent_version='test-agent',
                decision_run_id=f'run-{symbol}-{source_market_date.isoformat()}',
                feedback_received_at=datetime.combine(
                    source_market_date,
                    datetime.min.time(),
                    tzinfo=timezone.utc,
                ),
                policy_version='short_term_decider_policy_test',
                decision_action='buy',
                preferred_action='buy',
                next_day_open_action='buy',
                entry_price_basis='next_open',
                target_price=11000,
                expected_return_pct=0.05,
                stop_loss_price=9500,
                suggested_holding_period='1-3 trading days',
                confidence_score_10=7,
                conviction_score_10=6,
                thesis_summary='Repository replay regression snapshot.',
                supporting_evidence_payload=[],
                decision_risks_payload=[],
                invalidation_conditions_payload=[],
                data_quality_warnings_payload=[],
                specialist_disagreement_payload=[],
                traceability_payload={},
                input_feature_snapshot_payload={},
                request_payload={'symbol_day_context': {'symbol': symbol}},
                response_payload={'tactical_action': 'watch_for_trigger'},
                response_summary_payload={
                    'decision_provenance': {
                        'model_identity': build_observed_model_identity(
                            role=SHORT_TERM_DECIDER_AGENT_NAME,
                            expected_model='openai/gpt-5.6-sol',
                            actual_provider='openai',
                            actual_model='gpt-5.6-sol',
                        )
                    }
                },
            )
            session.add(snapshot)
            session.flush()
            validation_payload = {'short_term': {'tactical_action': 'watch_for_trigger'}}
            traceability_payload = {'tactical_action': 'watch_for_trigger'}
            if alignment_gate is not None:
                validation_payload['alignment_gate'] = alignment_gate
                traceability_payload['alignment_gate'] = alignment_gate
            plan_row = SignalDeciderExecutionPlanItem(
                execution_window='next_open',
                decision_agent_name=SHORT_TERM_DECIDER_AGENT_NAME,
                decision_snapshot_id=snapshot.id,
                broker_name='kis',
                target_trade_date=target_trade_date,
                source_market_date=source_market_date,
                account_reference='KIS:WAIT-CONFLICT-01',
                account_label='Regression',
                symbol=symbol,
                policy_version='short_term_decider_policy_test',
                planned_side='buy',
                planned_order_type='market',
                planned_time_in_force='day',
                allocation_budget_krw=Decimal('100000'),
                estimated_reference_price=10000,
                planned_quantity=Decimal('10'),
                estimated_notional_krw=Decimal('100000'),
                eligibility_status='eligible',
                execution_status='filled',
                validation_payload=validation_payload,
                traceability_payload=traceability_payload,
            )
            session.add(plan_row)
            session.flush()
            session.add(
                SignalDeciderActualExecutionOutcome(
                    decision_snapshot_id=snapshot.id,
                    entry_plan_item_id=plan_row.id,
                    broker_name='kis',
                    market_date=target_trade_date,
                    source_market_date=source_market_date,
                    target_trade_date=target_trade_date,
                    account_reference='KIS:WAIT-CONFLICT-01',
                    symbol=symbol,
                    policy_version='short_term_decider_policy_test',
                    lifecycle_status='closed',
                    exit_reason='time_exit',
                    entry_filled_quantity=Decimal('10'),
                    exit_filled_quantity=Decimal('10'),
                    closed_quantity=Decimal('10'),
                    open_quantity=open_quantity,
                    entry_average_fill_price=Decimal('10000'),
                    exit_average_fill_price=Decimal('10100'),
                    entry_notional_krw=Decimal('100000'),
                    exit_notional_krw=Decimal('101000'),
                    realized_pnl_krw=realized_pnl_krw,
                    realized_return_pct=Decimal('0.01'),
                    diagnostics_payload={},
                )
            )

        try:
            with TestSessionLocal() as session:
                seed_outcome(
                    session=session,
                    symbol='111111',
                    source_market_date=date(2026, 7, 3),
                    target_trade_date=date(2026, 7, 6),
                    realized_pnl_krw=Decimal('1000'),
                    alignment_gate={
                        **_alignment_gate('wait_conflict_block', 'downsize_probe'),
                        'probe_quantity': 1,
                        'requested_quantity': 10,
                        'final_quantity': 1,
                    },
                )
                seed_outcome(
                    session=session,
                    symbol='666666',
                    source_market_date=date(2026, 7, 2),
                    target_trade_date=date(2026, 7, 3),
                    realized_pnl_krw=Decimal('-3000'),
                    alignment_gate={
                        **_alignment_gate('wait_conflict_block', 'downsize_probe'),
                        'probe_quantity': 7,
                        'requested_quantity': 10,
                        'final_quantity': 7,
                    },
                )
                seed_outcome(
                    session=session,
                    symbol='555555',
                    source_market_date=date(2026, 7, 1),
                    target_trade_date=date(2026, 7, 2),
                    realized_pnl_krw=Decimal('2000'),
                )
                seed_outcome(
                    session=session,
                    symbol='222222',
                    source_market_date=date(2026, 6, 26),
                    target_trade_date=date(2026, 6, 29),
                    realized_pnl_krw=Decimal('-2000'),
                )
                seed_outcome(
                    session=session,
                    symbol='333333',
                    source_market_date=date(2026, 7, 6),
                    target_trade_date=date(2026, 7, 7),
                    realized_pnl_krw=Decimal('3000'),
                )
                seed_outcome(
                    session=session,
                    symbol='444444',
                    source_market_date=date(2026, 7, 2),
                    target_trade_date=date(2026, 7, 3),
                    realized_pnl_krw=Decimal('500'),
                    open_quantity=Decimal('1'),
                )
                session.commit()

            with patch('app.agent_context.decider_replay_service.session_scope', test_session_scope):
                repository = DeciderReplayRepository()
                trades = repository.list_wait_conflict_gate_trades(
                    market_date_from=date(2026, 6, 29),
                    market_date_to=date(2026, 7, 3),
                    symbols=None,
                    cost_assumptions=DeciderReplayCostAssumptions(),
                    gate_mode='downsize_probe',
                    probe_quantity=3,
                )
                fixed_horizon_dataset = repository.list_replay_trades(
                    market_date_from=date(2026, 6, 29),
                    market_date_to=date(2026, 7, 3),
                    agent_name=SHORT_TERM_DECIDER_AGENT_NAME,
                    symbols=None,
                    horizon_days=5,
                    cost_assumptions=DeciderReplayCostAssumptions(),
                )

            self.assertEqual(
                [trade.symbol for trade in trades],
                ['555555', '444444', '666666', '111111'],
            )
            for trade in trades:
                with self.subTest(symbol=trade.symbol):
                    self.assertEqual(trade.alignment_gate['requested_quantity'], 10)
                    self.assertEqual(trade.alignment_gate['probe_quantity'], 3)
                    self.assertEqual(trade.alignment_gate['final_quantity'], 3)
                    self.assertEqual(trade.point_in_time['alignment_gate']['probe_quantity'], 3)
            self.assertEqual(trades[0].alignment_gate['evidence_status'], 'missing')
            self.assertEqual(trades[1].alignment_gate['evidence_status'], 'missing')
            self.assertEqual(trades[2].alignment_gate['evidence_status'], 'persisted')
            self.assertEqual(trades[3].alignment_gate['evidence_status'], 'persisted')
            self.assertEqual(trades[3].market_date, date(2026, 7, 3))
            self.assertEqual(trades[3].point_in_time['market_date'], '2026-07-03')
            self.assertEqual(trades[3].point_in_time['source_market_date'], '2026-07-03')
            self.assertEqual(trades[3].point_in_time['target_trade_date'], '2026-07-06')
            self.assertEqual(
                trades[3].point_in_time['alignment_context_source'],
                'persisted_plan_alignment_gate',
            )
            self.assertEqual(trades[3].alignment_gate['alignment_gate_action'], 'downsize_probe')
            self.assertEqual(trades.source_decision_count, 4)
            self.assertEqual(
                trades.excluded_reason_counts,
                {'actual_position_still_open': 1},
            )
            self.assertEqual(trades.diagnostics['actual_lifecycle_eligible_count'], 3)
            self.assertEqual(sum(trade.outcome_resolved for trade in trades), 3)
            self.assertEqual(len(fixed_horizon_dataset), 0)
            self.assertEqual(fixed_horizon_dataset.source_decision_count, 4)
            self.assertEqual(
                fixed_horizon_dataset.excluded_reason_counts,
                {'short_term_tactical_action_not_buy': 4},
            )
            self.assertEqual(
                fixed_horizon_dataset.diagnostics['actual_lifecycle_outcomes_used_for_promotion'],
                False,
            )
            self.assertEqual(
                fixed_horizon_dataset.diagnostics['actual_lifecycle_eligible_count'],
                3,
            )
        finally:
            Base.metadata.drop_all(engine, tables=list(reversed(tables)))


if __name__ == '__main__':
    unittest.main()
