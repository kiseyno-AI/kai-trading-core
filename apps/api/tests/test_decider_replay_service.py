from datetime import date, datetime, timezone
from decimal import Decimal
import unittest

from app.agent_context.decider_replay_service import DeciderReplayService, DeciderReplayTrade
from app.agent_context.roles import SHORT_TERM_DECIDER_AGENT_NAME


DEFAULT_NOTIONAL = Decimal('1000000')


class FakeDeciderReplayRepository:
    def __init__(self, trades: list[DeciderReplayTrade]) -> None:
        self.trades = list(trades)
        self.calls: list[dict] = []

    def list_replay_trades(self, **kwargs) -> list[DeciderReplayTrade]:
        self.calls.append(kwargs)
        return list(self.trades)


def _trade(
    *,
    snapshot_id: int,
    symbol: str,
    gross_pnl_krw: int,
    trigger_price_drift_pct: float | None,
    confidence_score_10: float = 7.0,
    market_date: str = '2026-06-16',
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
        outcome_source='decider_post_decision_evaluation',
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
        },
        decision_timestamp=datetime(2026, 6, 16, 6, 0, tzinfo=timezone.utc),
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


if __name__ == '__main__':
    unittest.main()
