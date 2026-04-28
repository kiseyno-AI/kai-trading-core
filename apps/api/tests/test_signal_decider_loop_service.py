from datetime import date, datetime, timezone
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from app.agent_context.signal_decider_loop_service import (
    DEFAULT_SIGNAL_DECIDER_POLICY_VERSION,
    SignalDeciderLoopService,
)


def _feedback_row(
    *,
    feedback_id: int,
    market_date: str,
    symbol: str,
    action: str,
    signal_score: int = 78,
    breakout_20d: bool = True,
) -> SimpleNamespace:
    context_payload = {
        'symbol_day_context': {
            'technical_snapshot': {
                'trend_state': 'uptrend',
                'ma_alignment': 'bullish',
                'breakout_20d': breakout_20d,
                'breakdown_20d': False,
                'return_5d': 2.4,
                'return_20d': 8.1,
                'volume_ratio_20d': 1.6,
                'atr_pct_14': 2.2,
            },
            'action_summary': {
                'action_label': 'long_breakout',
                'bias_direction': 'bullish',
                'signal_score': signal_score,
                'confidence_tier': 'high',
                'risk_tier': 'low',
            },
            'signal_features': {
                'signal_count': 3,
                'volume_power_rank': 2,
                'overtime_volume_rank': 3,
                'overtime_fluctuation_rank': 1,
            },
            'price_context': {
                'close_location_ratio': 0.82,
                'gap_vs_prev_close_pct': 0.3,
                'range_pct': 4.1,
            },
        }
    }
    feedback_payload = {
        'role': 'signal-decider',
        'agent_name': 'signal-decider',
        'symbol': symbol,
        'market_date': market_date,
        'preferred_action': action,
        'next_day_open_action': action,
        'entry_price_basis': 'next_open',
        'target_price': 105 if action == 'buy' else None,
        'expected_return_pct': 4.0,
        'stop_loss_price': 96 if action == 'buy' else None,
        'suggested_holding_period': '3-10 trading days',
        'confidence_score_10': 7,
        'conviction_score_10': 6,
        'thesis_summary': f'{action} decision for {symbol}',
        'supporting_evidence': ['Breadth improved into the close.'],
        'decision_risks': ['Leadership remained narrow.'],
        'data_quality_warnings': [],
        'specialist_disagreement': [],
        'traceability': {'agent_name': 'signal-decider', 'symbol': symbol, 'market_date': market_date},
    }
    return SimpleNamespace(
        id=feedback_id,
        market_date=date.fromisoformat(market_date),
        symbol=symbol,
        agent_name='signal-decider',
        summary_payload={'model': 'openclaw/signal-decider'},
        feedback_payload=feedback_payload,
        normalized_feedback_payload=feedback_payload,
        context_payload=context_payload,
        received_at=datetime(2026, 4, 23, 6, 0, tzinfo=timezone.utc),
    )


class FakeSignalDeciderLoopRepository:
    def __init__(self, feedback_rows):
        self.feedback_rows = list(feedback_rows)
        self.outcomes = {}
        self.evaluations = {}
        self.candidates = {}
        self.next_outcome_id = 1
        self.next_evaluation_id = 1
        self.next_candidate_id = 1

    def list_signal_decider_feedback(
        self,
        *,
        market_date=None,
        market_date_from=None,
        market_date_to=None,
        symbols=None,
        limit=None,
    ):
        rows = list(self.feedback_rows)
        if market_date is not None:
            rows = [row for row in rows if row.market_date == market_date]
        if market_date_from is not None:
            rows = [row for row in rows if row.market_date >= market_date_from]
        if market_date_to is not None:
            rows = [row for row in rows if row.market_date <= market_date_to]
        normalized_symbols = set(symbols or [])
        if normalized_symbols:
            rows = [row for row in rows if row.symbol in normalized_symbols]
        rows.sort(key=lambda row: (row.market_date, row.symbol), reverse=True)
        return rows[:limit] if limit is not None else rows

    def list_realized_outcomes(self, *, feedback_ids, horizons=None):
        rows = [
            item
            for item in self.outcomes.values()
            if item['feedback_id'] in set(feedback_ids)
            and (horizons is None or item['horizon_days'] in set(horizons))
        ]
        rows.sort(key=lambda item: (item['feedback_id'], item['horizon_days']))
        return rows

    def list_evaluations(self, *, feedback_ids, horizons=None, evaluation_version=None):
        rows = [
            item
            for item in self.evaluations.values()
            if item['feedback_id'] in set(feedback_ids)
            and (horizons is None or item['horizon_days'] in set(horizons))
            and (evaluation_version is None or item['evaluation_version'] == evaluation_version)
        ]
        rows.sort(key=lambda item: (item['feedback_id'], item['horizon_days']))
        return rows

    def list_evaluations_by_range(
        self,
        *,
        market_date_from,
        market_date_to,
        symbols=None,
        horizons=None,
        evaluation_version=None,
    ):
        rows = [
            item
            for item in self.evaluations.values()
            if market_date_from <= date.fromisoformat(item['market_date']) <= market_date_to
            and (not symbols or item['symbol'] in set(symbols))
            and (horizons is None or item['horizon_days'] in set(horizons))
            and (evaluation_version is None or item['evaluation_version'] == evaluation_version)
        ]
        rows.sort(key=lambda item: (item['market_date'], item['symbol'], item['horizon_days']))
        return rows

    def list_policy_candidates(self, *, status=None, evaluation_version=None, base_policy_version=None, limit=50):
        rows = list(self.candidates.values())
        if status is not None:
            rows = [row for row in rows if row['status'] == status]
        if evaluation_version is not None:
            rows = [row for row in rows if row['evaluation_version'] == evaluation_version]
        if base_policy_version is not None:
            rows = [row for row in rows if row['base_policy_version'] == base_policy_version]
        rows.sort(key=lambda row: row['candidate_version'])
        return rows[:limit]

    def bulk_upsert_realized_outcomes(self, *, items):
        persisted = []
        for item in items:
            key = (item['feedback_id'], item['horizon_days'])
            stored = dict(item)
            stored.setdefault('id', self.outcomes.get(key, {}).get('id', self.next_outcome_id))
            if key not in self.outcomes:
                self.next_outcome_id += 1
            stored['market_date'] = stored['market_date'].isoformat()
            stored['entry_trade_date'] = stored['entry_trade_date'].isoformat() if stored['entry_trade_date'] else None
            stored['exit_trade_date'] = stored['exit_trade_date'].isoformat() if stored['exit_trade_date'] else None
            stored['computed_at'] = stored['computed_at'].isoformat()
            self.outcomes[key] = stored
            persisted.append(stored)
        return persisted

    def bulk_upsert_evaluations(self, *, items):
        persisted = []
        for item in items:
            key = (item['feedback_id'], item['horizon_days'], item['evaluation_version'])
            stored = dict(item)
            stored.setdefault('id', self.evaluations.get(key, {}).get('id', self.next_evaluation_id))
            if key not in self.evaluations:
                self.next_evaluation_id += 1
            stored['market_date'] = stored['market_date'].isoformat()
            stored['evaluated_at'] = stored['evaluated_at'].isoformat()
            self.evaluations[key] = stored
            persisted.append(stored)
        return persisted

    def bulk_upsert_policy_candidates(self, *, items):
        persisted = []
        for item in items:
            key = item['candidate_version']
            stored = dict(item)
            stored.setdefault('id', self.candidates.get(key, {}).get('id', self.next_candidate_id))
            if key not in self.candidates:
                self.next_candidate_id += 1
            stored['market_date_from'] = stored['market_date_from'].isoformat()
            stored['market_date_to'] = stored['market_date_to'].isoformat()
            stored['generated_at'] = stored['generated_at'].isoformat()
            self.candidates[key] = stored
            persisted.append(stored)
        return persisted


class SignalDeciderLoopServiceTests(unittest.TestCase):
    def test_prepare_feedback_persistence_adds_policy_and_feature_snapshot(self):
        service = SignalDeciderLoopService(repository=FakeSignalDeciderLoopRepository([]))

        summary, feedback, context = service.prepare_feedback_persistence(
            agent_name='signal-decider',
            summary_payload={'model': 'openclaw/signal-decider'},
            feedback_payload={
                'next_day_open_action': 'wait',
                'traceability': {'symbol': '005930', 'market_date': '2026-04-23'},
            },
            context_payload={
                'symbol_day_context': {
                    'technical_snapshot': {'breakout_20d': True},
                    'action_summary': {'signal_score': 78, 'confidence_tier': 'high'},
                    'signal_features': {'signal_count': 3},
                    'price_context': {'close_location_ratio': 0.8},
                }
            },
        )

        self.assertEqual(summary['policy_version'], DEFAULT_SIGNAL_DECIDER_POLICY_VERSION)
        self.assertEqual(feedback['policy_version'], DEFAULT_SIGNAL_DECIDER_POLICY_VERSION)
        self.assertEqual(context['signal_decider_policy']['activation_mode'], 'manual_only')
        self.assertTrue(context['signal_decider_trace']['feature_snapshot']['technical']['breakout_20d'])
        self.assertEqual(
            context['signal_decider_trace']['feature_snapshot']['action_summary']['signal_score_bucket'],
            'high',
        )

    def test_compute_evaluations_grades_buy_and_wait_across_horizons(self):
        repository = FakeSignalDeciderLoopRepository(
            [
                _feedback_row(feedback_id=1, market_date='2026-04-20', symbol='005930', action='buy'),
                _feedback_row(feedback_id=2, market_date='2026-04-20', symbol='000660', action='wait'),
            ]
        )
        service = SignalDeciderLoopService(repository=repository)

        bars_by_symbol = {
            '005930': [
                {'trade_date': '2026-04-20', 'open_price': 98, 'high_price': 99, 'low_price': 97, 'close_price': 98, 'volume': 1000},
                {'trade_date': '2026-04-21', 'open_price': 100, 'high_price': 104, 'low_price': 99, 'close_price': 103, 'volume': 1000},
                {'trade_date': '2026-04-22', 'open_price': 103, 'high_price': 106, 'low_price': 102, 'close_price': 105, 'volume': 1000},
                {'trade_date': '2026-04-23', 'open_price': 105, 'high_price': 108, 'low_price': 104, 'close_price': 107, 'volume': 1000},
                {'trade_date': '2026-04-24', 'open_price': 107, 'high_price': 109, 'low_price': 106, 'close_price': 108, 'volume': 1000},
                {'trade_date': '2026-04-27', 'open_price': 108, 'high_price': 112, 'low_price': 107, 'close_price': 111, 'volume': 1000},
            ],
            '000660': [
                {'trade_date': '2026-04-20', 'open_price': 48, 'high_price': 49, 'low_price': 47, 'close_price': 48, 'volume': 900},
                {'trade_date': '2026-04-21', 'open_price': 50, 'high_price': 53, 'low_price': 49, 'close_price': 52, 'volume': 900},
                {'trade_date': '2026-04-22', 'open_price': 52, 'high_price': 54, 'low_price': 51, 'close_price': 53, 'volume': 900},
                {'trade_date': '2026-04-23', 'open_price': 53, 'high_price': 55, 'low_price': 52, 'close_price': 54, 'volume': 900},
                {'trade_date': '2026-04-24', 'open_price': 54, 'high_price': 56, 'low_price': 53, 'close_price': 55, 'volume': 900},
                {'trade_date': '2026-04-27', 'open_price': 55, 'high_price': 58, 'low_price': 54, 'close_price': 57, 'volume': 900},
            ],
        }

        with patch(
            'app.agent_context.signal_decider_loop_service.load_daily_bars_for_symbol_range',
            side_effect=lambda symbol, **kwargs: bars_by_symbol[symbol],
        ):
            result = service.compute_evaluations(
                market_date_from='2026-04-20',
                market_date_to='2026-04-20',
                horizons=[1, 5],
                evaluation_version='signal_decider_eval_test',
            )

        self.assertEqual(result['summary']['decisions_considered'], 2)
        self.assertEqual(result['summary']['outcomes_upserted'], 4)
        self.assertEqual(result['summary']['evaluations_upserted'], 4)
        items_by_symbol = {item['symbol']: item for item in result['items']}
        buy_evaluations = {item['horizon_days']: item for item in items_by_symbol['005930']['evaluations']}
        wait_evaluations = {item['horizon_days']: item for item in items_by_symbol['000660']['evaluations']}
        self.assertEqual(buy_evaluations[1]['grade_label'], 'good_buy')
        self.assertEqual(buy_evaluations[5]['grade_label'], 'good_buy')
        self.assertEqual(wait_evaluations[5]['grade_label'], 'missed_opportunity_after_wait')
        buy_outcomes = {item['horizon_days']: item for item in items_by_symbol['005930']['outcomes']}
        self.assertEqual(buy_outcomes[1]['entry_trade_date'], '2026-04-21')
        self.assertEqual(buy_outcomes[5]['exit_trade_date'], '2026-04-27')

    def test_generate_policy_candidates_builds_manual_relax_wait_candidates(self):
        repository = FakeSignalDeciderLoopRepository(
            [
                _feedback_row(feedback_id=2, market_date='2026-04-20', symbol='000660', action='wait'),
                _feedback_row(feedback_id=3, market_date='2026-04-20', symbol='035420', action='wait'),
                _feedback_row(feedback_id=4, market_date='2026-04-20', symbol='051910', action='wait'),
            ]
        )
        service = SignalDeciderLoopService(repository=repository)

        bars_by_symbol = {
            symbol: [
                {'trade_date': '2026-04-20', 'open_price': 48, 'high_price': 49, 'low_price': 47, 'close_price': 48, 'volume': 900},
                {'trade_date': '2026-04-21', 'open_price': 50, 'high_price': 53, 'low_price': 49, 'close_price': 52, 'volume': 900},
                {'trade_date': '2026-04-22', 'open_price': 52, 'high_price': 55, 'low_price': 51, 'close_price': 54, 'volume': 900},
                {'trade_date': '2026-04-23', 'open_price': 54, 'high_price': 57, 'low_price': 53, 'close_price': 56, 'volume': 900},
                {'trade_date': '2026-04-24', 'open_price': 56, 'high_price': 60, 'low_price': 55, 'close_price': 59, 'volume': 900},
                {'trade_date': '2026-04-27', 'open_price': 59, 'high_price': 63, 'low_price': 58, 'close_price': 62, 'volume': 900},
            ]
            for symbol in ('000660', '035420', '051910')
        }

        with patch(
            'app.agent_context.signal_decider_loop_service.load_daily_bars_for_symbol_range',
            side_effect=lambda symbol, **kwargs: bars_by_symbol[symbol],
        ):
            result = service.generate_policy_candidates(
                market_date_from='2026-04-20',
                market_date_to='2026-04-20',
                horizons=[5],
                primary_horizon_days=5,
                evaluation_version='signal_decider_eval_test',
                min_sample_size=2,
            )

        self.assertGreaterEqual(result['summary']['candidates_generated'], 1)
        self.assertTrue(result['summary']['manual_activation_required'])
        self.assertTrue(any(item['candidate_kind'] == 'relax_wait_gate' for item in result['items']))
        self.assertTrue(all(item['activation_mode'] == 'manual_only' for item in result['items']))


if __name__ == '__main__':
    unittest.main()
