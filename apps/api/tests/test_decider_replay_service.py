from contextlib import contextmanager
from datetime import date, datetime, timezone
from decimal import Decimal
import unittest
from unittest.mock import patch

from sqlalchemy import create_engine, event
from sqlalchemy.orm import sessionmaker

from app.agent_context.decider_replay_service import (
    DeciderReplayCostAssumptions,
    DeciderReplayRepository,
    DeciderReplayService,
    DeciderReplayTrade,
)
from app.agent_context.roles import SHORT_TERM_DECIDER_AGENT_NAME
from app.db_models import (
    AgentContextFeedback,
    Base,
    BrokerOrder,
    OrderRequest,
    SignalDeciderActualExecutionOutcome,
    SignalDeciderDecisionSnapshot,
    SignalDeciderExecutionPlanItem,
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
    ) -> None:
        self.trades = list(trades)
        self.wait_conflict_trades = list(wait_conflict_trades or [])
        self.calls: list[dict] = []
        self.wait_conflict_calls: list[dict] = []

    def list_replay_trades(self, **kwargs) -> list[DeciderReplayTrade]:
        self.calls.append(kwargs)
        return list(self.trades)

    def list_wait_conflict_gate_trades(self, **kwargs) -> list[DeciderReplayTrade]:
        self.wait_conflict_calls.append(kwargs)
        return list(self.wait_conflict_trades)


def _trade(
    *,
    snapshot_id: int,
    symbol: str,
    gross_pnl_krw: int,
    trigger_price_drift_pct: float | None,
    confidence_score_10: float = 7.0,
    market_date: str = '2026-06-16',
    alignment_gate: dict | None = None,
    outcome_source: str = 'decider_post_decision_evaluation',
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
    return {
        'schema_version': 'signal_decider_alignment_gate_v1',
        'signal_decider_alignment': alignment,
        'alignment_gate_action': action,
        'alignment_gate_reason': f'{alignment} test reason',
        'signal_decider_decision_snapshot_id': 900,
    }


class DeciderReplayServiceTests(unittest.TestCase):
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

        self.assertEqual(
            zero_cost['summary']['baseline']['gross_pnl_krw'],
            with_cost['summary']['baseline']['gross_pnl_krw'],
        )
        self.assertEqual(zero_cost['summary']['baseline']['gross_pnl_krw'], 10_000)
        self.assertEqual(zero_cost['summary']['baseline']['estimated_net_pnl_krw'], 10_000)
        self.assertEqual(with_cost['summary']['baseline']['estimated_net_pnl_krw'], 7_000)

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

    def test_wait_conflict_gate_replay_returns_empty_data_reason(self) -> None:
        service = DeciderReplayService(repository=FakeDeciderReplayRepository([]))

        result = service.replay_wait_conflict_gate(
            market_date_from='2026-06-29',
            market_date_to='2026-07-03',
        )

        self.assertEqual(result['summary']['baseline_trade_count'], 0)
        self.assertEqual(result['summary']['empty_data_reason'], 'no_actual_short_term_execution_outcomes_in_range')
        self.assertIn('actual short-term execution outcomes only', result['markdown_summary'])


class DeciderReplayRepositoryTests(unittest.TestCase):
    def test_wait_conflict_gate_filters_source_market_date_not_target_trade_date(self) -> None:
        engine = create_engine('sqlite+pysqlite:///:memory:')
        event.listen(engine, 'connect', _enable_sqlite_foreign_keys)
        tables = [
            AgentContextFeedback.__table__,
            OrderRequest.__table__,
            BrokerOrder.__table__,
            SignalDeciderDecisionSnapshot.__table__,
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
                response_summary_payload={},
            )
            session.add(snapshot)
            session.flush()
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
                validation_payload={'short_term': {'tactical_action': 'watch_for_trigger'}},
                traceability_payload={'tactical_action': 'watch_for_trigger'},
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
                    open_quantity=Decimal('0'),
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
                session.commit()

            with patch('app.agent_context.decider_replay_service.session_scope', test_session_scope):
                trades = DeciderReplayRepository().list_wait_conflict_gate_trades(
                    market_date_from=date(2026, 6, 29),
                    market_date_to=date(2026, 7, 3),
                    symbols=None,
                    cost_assumptions=DeciderReplayCostAssumptions(),
                    gate_mode='hard_block',
                )

            self.assertEqual([trade.symbol for trade in trades], ['111111'])
            self.assertEqual(trades[0].market_date, date(2026, 7, 3))
            self.assertEqual(trades[0].point_in_time['market_date'], '2026-07-03')
            self.assertEqual(trades[0].point_in_time['source_market_date'], '2026-07-03')
            self.assertEqual(trades[0].point_in_time['target_trade_date'], '2026-07-06')
        finally:
            Base.metadata.drop_all(engine, tables=list(reversed(tables)))


if __name__ == '__main__':
    unittest.main()
