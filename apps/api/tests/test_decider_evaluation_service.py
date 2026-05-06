from datetime import date, datetime, timezone
import unittest

from app.agent_context.decider_evaluation_service import (
    DeciderPostDecisionEvaluationService,
    OUTCOME_AMBIGUOUS,
    OUTCOME_INVALID_MISSING_DATA,
    OUTCOME_NEITHER_HIT,
    OUTCOME_NOT_APPLICABLE,
    OUTCOME_STOP_HIT,
    OUTCOME_TARGET_HIT,
)


def _feedback_source(
    *,
    feedback_id: int = 11,
    decision_snapshot_id: int | None = 84,
    agent_name: str = 'signal-decider',
    action: str = 'buy',
    market_date: str = '2026-04-23',
    symbol: str = '005930',
    target_price: int | None = 105,
    stop_loss_price: int | None = 95,
) -> dict:
    normalized_feedback_payload = {
        'preferred_action': action,
        'target_price': target_price,
        'stop_loss_price': stop_loss_price,
        'data_quality_warnings': [],
    }
    if agent_name == 'signal-decider':
        normalized_feedback_payload['next_day_open_action'] = action
        normalized_feedback_payload['entry_price_basis'] = 'next_open'
    else:
        normalized_feedback_payload['tactical_action'] = action
        normalized_feedback_payload['trigger_price'] = 101
        normalized_feedback_payload['chase_limit_price'] = 103
    return {
        'feedback_id': feedback_id,
        'decision_snapshot_id': decision_snapshot_id,
        'market_date': date.fromisoformat(market_date),
        'symbol': symbol,
        'agent_name': agent_name,
        'agent_version': 'openclaw-v1',
        'run_id': f'run-{feedback_id}',
        'received_at': datetime(2026, 4, 23, 6, 0, tzinfo=timezone.utc),
        'normalized_feedback_payload': normalized_feedback_payload,
        'feedback_payload': normalized_feedback_payload,
    }


def _bar(trade_date: str, open_price: int, high_price: int, low_price: int, close_price: int) -> dict:
    return {
        'trade_date': trade_date,
        'open_price': open_price,
        'high_price': high_price,
        'low_price': low_price,
        'close_price': close_price,
        'source': 'daily_price_bars',
    }


def _serialize_item(index: int, item: dict) -> dict:
    return {
        'evaluation_id': index,
        'source': {
            'feedback_id': item['feedback_id'],
            'decision_snapshot_id': item['decision_snapshot_id'],
        },
        'agent_name': item['agent_name'],
        'symbol': item['symbol'],
        'market_date': item['market_date'].isoformat(),
        'window_days': item['window_days'],
        'action': item['decision_action'],
        'entry_trade_date': item['entry_trade_date'].isoformat() if item['entry_trade_date'] is not None else None,
        'entry_price': item['entry_price'],
        'target_price': item['target_price'],
        'stop_loss_price': item['stop_loss_price'],
        'first_target_hit_trade_date': (
            item['first_target_hit_trade_date'].isoformat()
            if item['first_target_hit_trade_date'] is not None
            else None
        ),
        'first_stop_hit_trade_date': (
            item['first_stop_hit_trade_date'].isoformat()
            if item['first_stop_hit_trade_date'] is not None
            else None
        ),
        'mfe_pct': item['mfe_pct'],
        'mae_pct': item['mae_pct'],
        'outcome_label': item['outcome_label'],
        'outcome_reason': item['outcome_reason'],
        'data_quality_warnings': item['data_quality_warnings_payload'],
        'metadata': item['metadata_payload'],
        'computed_at': item['computed_at'].isoformat(),
    }


class FakeDeciderEvaluationRepository:
    def __init__(self, source_rows: list[dict]):
        self.source_rows = list(source_rows)
        self.list_feedback_args = None
        self.upserted_items = None

    def list_feedback_sources(self, **kwargs):
        self.list_feedback_args = kwargs
        return list(self.source_rows)

    def bulk_upsert_evaluations(self, *, items):
        self.upserted_items = list(items)
        return [_serialize_item(index, item) for index, item in enumerate(self.upserted_items, start=1)]


class DeciderPostDecisionEvaluationServiceTests(unittest.TestCase):
    def test_compute_evaluations_marks_target_hit_before_stop(self):
        repository = FakeDeciderEvaluationRepository([_feedback_source()])
        bars_by_symbol = {
            '005930': [
                _bar('2026-04-24', 100, 104, 98, 103),
                _bar('2026-04-25', 103, 106, 101, 105),
                _bar('2026-04-28', 105, 109, 102, 108),
                _bar('2026-04-29', 108, 108, 101, 102),
                _bar('2026-04-30', 102, 103, 100, 101),
            ]
        }
        service = DeciderPostDecisionEvaluationService(
            repository=repository,
            daily_bar_loader=lambda symbol, **kwargs: bars_by_symbol[symbol],
        )

        result = service.compute_evaluations(
            market_date_from='2026-04-23',
            market_date_to='2026-04-23',
        )

        self.assertEqual(result['summary']['outcome_label_counts'][OUTCOME_TARGET_HIT], 1)
        item = result['items'][0]
        self.assertEqual(item['outcome_label'], OUTCOME_TARGET_HIT)
        self.assertEqual(item['first_target_hit_trade_date'], '2026-04-25')
        self.assertIsNone(item['first_stop_hit_trade_date'])
        self.assertAlmostEqual(item['mfe_pct'], 0.09, places=6)
        self.assertAlmostEqual(item['mae_pct'], -0.02, places=6)

    def test_compute_evaluations_marks_stop_hit_before_target(self):
        repository = FakeDeciderEvaluationRepository([_feedback_source()])
        bars_by_symbol = {
            '005930': [
                _bar('2026-04-24', 100, 101, 99, 100),
                _bar('2026-04-25', 100, 103, 94, 96),
                _bar('2026-04-28', 96, 98, 93, 95),
                _bar('2026-04-29', 95, 100, 94, 99),
                _bar('2026-04-30', 99, 101, 98, 100),
            ]
        }
        service = DeciderPostDecisionEvaluationService(
            repository=repository,
            daily_bar_loader=lambda symbol, **kwargs: bars_by_symbol[symbol],
        )

        result = service.compute_evaluations(
            market_date_from='2026-04-23',
            market_date_to='2026-04-23',
        )

        item = result['items'][0]
        self.assertEqual(item['outcome_label'], OUTCOME_STOP_HIT)
        self.assertEqual(item['first_stop_hit_trade_date'], '2026-04-25')
        self.assertIsNone(item['first_target_hit_trade_date'])

    def test_compute_evaluations_marks_same_day_target_stop_collision_ambiguous(self):
        repository = FakeDeciderEvaluationRepository([_feedback_source()])
        bars_by_symbol = {
            '005930': [
                _bar('2026-04-24', 100, 106, 94, 101),
                _bar('2026-04-25', 101, 102, 99, 100),
                _bar('2026-04-28', 100, 101, 99, 100),
                _bar('2026-04-29', 100, 101, 99, 100),
                _bar('2026-04-30', 100, 101, 99, 100),
            ]
        }
        service = DeciderPostDecisionEvaluationService(
            repository=repository,
            daily_bar_loader=lambda symbol, **kwargs: bars_by_symbol[symbol],
        )

        result = service.compute_evaluations(
            market_date_from='2026-04-23',
            market_date_to='2026-04-23',
        )

        item = result['items'][0]
        self.assertEqual(item['outcome_label'], OUTCOME_AMBIGUOUS)
        self.assertEqual(item['first_target_hit_trade_date'], '2026-04-24')
        self.assertEqual(item['first_stop_hit_trade_date'], '2026-04-24')

    def test_compute_evaluations_marks_neither_hit_within_5d(self):
        repository = FakeDeciderEvaluationRepository([_feedback_source()])
        bars_by_symbol = {
            '005930': [
                _bar('2026-04-24', 100, 103, 97, 101),
                _bar('2026-04-25', 101, 104, 98, 102),
                _bar('2026-04-28', 102, 104, 98, 103),
                _bar('2026-04-29', 103, 104, 98, 100),
                _bar('2026-04-30', 100, 104, 97, 99),
            ]
        }
        service = DeciderPostDecisionEvaluationService(
            repository=repository,
            daily_bar_loader=lambda symbol, **kwargs: bars_by_symbol[symbol],
        )

        result = service.compute_evaluations(
            market_date_from='2026-04-23',
            market_date_to='2026-04-23',
        )

        self.assertEqual(result['items'][0]['outcome_label'], OUTCOME_NEITHER_HIT)

    def test_compute_evaluations_marks_missing_price_data_when_window_is_incomplete(self):
        repository = FakeDeciderEvaluationRepository([_feedback_source()])
        bars_by_symbol = {
            '005930': [
                _bar('2026-04-24', 100, 103, 99, 102),
                _bar('2026-04-25', 102, 104, 100, 103),
                _bar('2026-04-28', 103, 104, 101, 102),
            ]
        }
        service = DeciderPostDecisionEvaluationService(
            repository=repository,
            daily_bar_loader=lambda symbol, **kwargs: bars_by_symbol[symbol],
        )

        result = service.compute_evaluations(
            market_date_from='2026-04-23',
            market_date_to='2026-04-23',
        )

        item = result['items'][0]
        self.assertEqual(item['outcome_label'], OUTCOME_INVALID_MISSING_DATA)
        self.assertIn('incomplete_next_5_trading_days', item['data_quality_warnings'])

    def test_compute_evaluations_marks_wait_actions_not_applicable(self):
        repository = FakeDeciderEvaluationRepository([
            _feedback_source(action='wait', target_price=None, stop_loss_price=None)
        ])
        service = DeciderPostDecisionEvaluationService(
            repository=repository,
            daily_bar_loader=lambda symbol, **kwargs: [],
        )

        result = service.compute_evaluations(
            market_date_from='2026-04-23',
            market_date_to='2026-04-23',
        )

        item = result['items'][0]
        self.assertEqual(item['outcome_label'], OUTCOME_NOT_APPLICABLE)
        self.assertEqual(item['action'], 'wait')

    def test_compute_evaluations_supports_short_term_decider_as_not_yet_mapped_tactical_action(self):
        repository = FakeDeciderEvaluationRepository([
            _feedback_source(
                feedback_id=22,
                decision_snapshot_id=None,
                agent_name='short-term-decider',
                action='watch_for_trigger',
            )
        ])
        bars_by_symbol = {
            '005930': [
                _bar('2026-04-24', 100, 103, 99, 102),
                _bar('2026-04-25', 102, 104, 100, 103),
                _bar('2026-04-28', 103, 105, 101, 104),
                _bar('2026-04-29', 104, 106, 102, 105),
                _bar('2026-04-30', 105, 107, 103, 106),
            ]
        }
        service = DeciderPostDecisionEvaluationService(
            repository=repository,
            daily_bar_loader=lambda symbol, **kwargs: bars_by_symbol[symbol],
        )

        result = service.compute_evaluations(
            market_date_from='2026-04-23',
            market_date_to='2026-04-23',
            agent_names=['short-term-decider'],
        )

        item = result['items'][0]
        self.assertEqual(item['outcome_label'], OUTCOME_NOT_APPLICABLE)
        self.assertEqual(item['source']['decision_snapshot_id'], None)
        self.assertIn('unsupported_tactical_action_for_generic_5d_evaluation', item['data_quality_warnings'])

