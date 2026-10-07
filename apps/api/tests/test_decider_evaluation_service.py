from datetime import date, datetime, timezone
import unittest
from contextlib import contextmanager
from types import SimpleNamespace
from unittest.mock import patch

from app.agent_context.decider_evaluation_service import (
    DeciderPostDecisionEvaluationRepository,
    DeciderPostDecisionEvaluationService,
    OUTCOME_AMBIGUOUS,
    OUTCOME_INVALID_LEVELS,
    OUTCOME_INVALID_MISSING_DATA,
    OUTCOME_NEITHER_HIT,
    OUTCOME_NOT_APPLICABLE,
    OUTCOME_STOP_HIT,
    OUTCOME_TARGET_HIT,
)


class _ScalarResult:
    def __init__(self, values):
        self._values = values

    def scalars(self):
        return self

    def all(self):
        return list(self._values)


class _EvaluationSession:
    def __init__(self, scalar_results):
        self.scalar_results = iter(scalar_results)
        self.statements = []

    def execute(self, statement):
        self.statements.append(statement)
        return _ScalarResult(next(self.scalar_results))


class DeciderPostDecisionEvaluationRepositoryTests(unittest.TestCase):
    def test_bulk_upsert_serializes_rows_before_session_exit(self):
        record = SimpleNamespace(id=91)
        session = _EvaluationSession([[91], [record]])
        session_active = False

        @contextmanager
        def session_scope():
            nonlocal session_active
            session_active = True
            try:
                yield session
            finally:
                session_active = False

        def serialize(item):
            self.assertTrue(session_active)
            return {'evaluation_id': item.id}

        with (
            patch(
                'app.agent_context.decider_evaluation_service.session_scope',
                session_scope,
            ),
            patch(
                'app.agent_context.decider_evaluation_service._serialize_evaluation',
                side_effect=serialize,
            ),
        ):
            result = DeciderPostDecisionEvaluationRepository().bulk_upsert_evaluations(
                items=[{'feedback_id': 81}]
            )

        self.assertEqual(result[0]['evaluation_id'], 91)

    def test_list_serializes_rows_before_session_exit(self):
        record = SimpleNamespace(id=91)
        session = _EvaluationSession([[record]])
        session_active = False

        @contextmanager
        def session_scope():
            nonlocal session_active
            session_active = True
            try:
                yield session
            finally:
                session_active = False

        def serialize(item):
            self.assertTrue(session_active)
            return {'evaluation_id': item.id}

        with (
            patch(
                'app.agent_context.decider_evaluation_service.session_scope',
                session_scope,
            ),
            patch(
                'app.agent_context.decider_evaluation_service._serialize_evaluation',
                side_effect=serialize,
            ),
        ):
            result = DeciderPostDecisionEvaluationRepository().list_evaluations(
                market_date=date(2026, 9, 23)
            )

        self.assertEqual(result[0]['evaluation_id'], 91)
        self.assertIn('agent_context_feedback', str(session.statements[0]))
        self.assertIn('received_at DESC', str(session.statements[0]))

    def test_list_include_history_preserves_all_durable_revisions(self):
        session = _EvaluationSession([[]])

        @contextmanager
        def session_scope():
            yield session

        with patch(
            'app.agent_context.decider_evaluation_service.session_scope',
            session_scope,
        ):
            result = DeciderPostDecisionEvaluationRepository().list_evaluations(
                market_date=date(2026, 9, 23),
                include_history=True,
            )

        self.assertEqual(result, [])
        self.assertNotIn('agent_context_feedback', str(session.statements[0]))


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
        'feedback_received_at': (
            datetime(2026, 4, 23, 6, 0, tzinfo=timezone.utc)
            if decision_snapshot_id is not None
            else None
        ),
        'created_at': (
            datetime(2026, 4, 23, 6, 0, tzinfo=timezone.utc)
            if decision_snapshot_id is not None
            else None
        ),
        'normalized_feedback_payload': normalized_feedback_payload,
        'feedback_payload': normalized_feedback_payload,
    }


def _bar(
    trade_date: str,
    open_price: int,
    high_price: int,
    low_price: int,
    close_price: int | None,
) -> dict:
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
    def test_compute_evaluations_quarantines_feedback_with_late_decision_snapshot(self):
        source = _feedback_source(market_date='2026-04-23')
        source['feedback_received_at'] = datetime(
            2026,
            4,
            24,
            0,
            0,
            tzinfo=timezone.utc,
        )
        source['created_at'] = datetime(
            2026,
            4,
            23,
            23,
            59,
            tzinfo=timezone.utc,
        )
        repository = FakeDeciderEvaluationRepository([source])
        service = DeciderPostDecisionEvaluationService(
            repository=repository,
            daily_bar_loader=lambda _symbol, **_kwargs: [],
        )

        with patch(
            'app.agent_context.signal_decider_snapshot_validity.next_krx_trading_date',
            return_value=date(2026, 4, 24),
        ):
            result = service.compute_evaluations(
                market_date_from='2026-04-23',
                market_date_to='2026-04-23',
            )

        self.assertEqual(result['summary']['feedback_rows_considered'], 0)
        self.assertEqual(result['summary']['evaluations_upserted'], 0)
        self.assertEqual(repository.upserted_items, [])

    def test_compute_evaluations_does_not_revive_older_revision_when_latest_is_invalid(self):
        older = _feedback_source(feedback_id=12, decision_snapshot_id=85)
        latest = _feedback_source(feedback_id=13, decision_snapshot_id=86)
        latest['received_at'] = datetime(2026, 4, 24, 0, 0, tzinfo=timezone.utc)
        latest['feedback_received_at'] = latest['received_at']
        latest['created_at'] = latest['received_at']
        repository = FakeDeciderEvaluationRepository([older, latest])
        service = DeciderPostDecisionEvaluationService(
            repository=repository,
            daily_bar_loader=lambda _symbol, **_kwargs: [],
        )

        with patch(
            'app.agent_context.signal_decider_snapshot_validity.next_krx_trading_date',
            return_value=date(2026, 4, 24),
        ):
            result = service.compute_evaluations(
                market_date_from='2026-04-23',
                market_date_to='2026-04-23',
            )

        self.assertEqual(result['summary']['feedback_rows_loaded'], 2)
        self.assertEqual(result['summary']['duplicate_revisions_ignored'], 1)
        self.assertEqual(result['summary']['latest_authority_rows_excluded_invalid'], 1)
        self.assertEqual(result['summary']['feedback_rows_considered'], 0)
        self.assertEqual(result['items'], [])

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

    def test_compute_evaluations_maps_short_term_watch_to_trigger_gated_paper_outcome(self):
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
        self.assertEqual(item['outcome_label'], OUTCOME_TARGET_HIT)
        self.assertEqual(item['source']['decision_snapshot_id'], None)
        self.assertEqual(item['entry_price'], 101)
        self.assertEqual(item['metadata']['paper_ledger']['mode'], 'paper_report_only')
        self.assertFalse(item['metadata']['paper_ledger']['costs_explicit'])
        self.assertFalse(item['metadata']['paper_ledger']['costs_nonzero'])
        self.assertIn(
            'zero_cost_assumption_may_overstate_paper_return',
            item['metadata']['paper_ledger']['cost_warnings'],
        )
        self.assertNotIn('unsupported_tactical_action_for_generic_5d_evaluation', item['data_quality_warnings'])

    def test_short_term_watch_paper_ledger_uses_trigger_chase_stop_first_and_costs(self):
        source = _feedback_source(
            feedback_id=23,
            agent_name='short-term-decider',
            action='watch_for_trigger',
        )
        repository = FakeDeciderEvaluationRepository([source])
        bars = [
            _bar('2026-04-24', 100, 106, 94, 102),
            _bar('2026-04-25', 102, 104, 98, 103),
            _bar('2026-04-28', 103, 104, 100, 102),
            _bar('2026-04-29', 102, 103, 99, 101),
            _bar('2026-04-30', 101, 102, 100, 101),
        ]
        service = DeciderPostDecisionEvaluationService(
            repository=repository,
            daily_bar_loader=lambda _symbol, **_kwargs: bars,
        )

        result = service.compute_evaluations(
            market_date_from='2026-04-23',
            market_date_to='2026-04-23',
            agent_names=['short-term-decider'],
            fee_bps=10,
            slippage_bps=20,
            tax_bps=18,
        )

        item = result['items'][0]
        ledger = item['metadata']['paper_ledger']
        self.assertEqual(item['outcome_label'], OUTCOME_STOP_HIT)
        self.assertEqual(item['entry_trade_date'], '2026-04-24')
        self.assertEqual(item['entry_price'], 101)
        self.assertEqual(item['first_stop_hit_trade_date'], '2026-04-24')
        self.assertIsNone(item['first_target_hit_trade_date'])
        self.assertEqual(ledger['entry_rule'], 'trigger_touched_within_chase_limit')
        self.assertEqual(
            ledger['levels'],
            {'trigger_price': 101, 'chase_limit_price': 103, 'target_price': 105, 'stop_loss_price': 95},
        )
        self.assertEqual(ledger['same_bar_policy'], 'stop_first')
        self.assertEqual(ledger['exit_reason'], 'stop_loss_hit')
        self.assertEqual(ledger['cost_assumptions_bps'], {'fee': 10.0, 'slippage': 20.0, 'tax': 18.0})
        self.assertTrue(ledger['costs_explicit'])
        self.assertTrue(ledger['costs_nonzero'])
        self.assertEqual(ledger['cost_warnings'], [])
        self.assertLess(ledger['net_return_pct'], ledger['gross_return_pct'])

    def test_short_term_watch_paper_ledger_warns_when_cost_inputs_are_partial(self):
        source = _feedback_source(
            feedback_id=26,
            agent_name='short-term-decider',
            action='watch_for_trigger',
        )
        repository = FakeDeciderEvaluationRepository([source])
        bars = [
            _bar('2026-04-24', 100, 104, 98, 103),
            _bar('2026-04-25', 103, 106, 101, 105),
            _bar('2026-04-28', 105, 106, 102, 104),
            _bar('2026-04-29', 104, 105, 102, 103),
            _bar('2026-04-30', 103, 104, 101, 102),
        ]
        service = DeciderPostDecisionEvaluationService(
            repository=repository,
            daily_bar_loader=lambda _symbol, **_kwargs: bars,
        )

        result = service.compute_evaluations(
            market_date_from='2026-04-23',
            market_date_to='2026-04-23',
            agent_names=['short-term-decider'],
            fee_bps=10,
        )

        ledger = result['items'][0]['metadata']['paper_ledger']
        self.assertFalse(result['request']['costs_explicit'])
        self.assertEqual(
            result['request']['cost_inputs_provided'],
            {'fee': True, 'slippage': False, 'tax': False},
        )
        self.assertFalse(ledger['costs_explicit'])
        self.assertEqual(ledger['missing_cost_components'], ['slippage', 'tax'])
        self.assertIn('partial_cost_assumption_missing:slippage,tax', ledger['cost_warnings'])

    def test_short_term_watch_does_not_enter_above_chase_limit(self):
        source = _feedback_source(
            feedback_id=24,
            agent_name='short-term-decider',
            action='watch_for_trigger',
        )
        repository = FakeDeciderEvaluationRepository([source])
        bars = [
            _bar(f'2026-04-{day:02d}', 104, 110, 100, 106)
            for day in (24, 25, 28, 29, 30)
        ]
        service = DeciderPostDecisionEvaluationService(
            repository=repository,
            daily_bar_loader=lambda _symbol, **_kwargs: bars,
        )

        result = service.compute_evaluations(
            market_date_from='2026-04-23',
            market_date_to='2026-04-23',
            agent_names=['short-term-decider'],
        )

        item = result['items'][0]
        self.assertEqual(item['outcome_label'], OUTCOME_NEITHER_HIT)
        self.assertIsNone(item['entry_trade_date'])
        self.assertIsNone(item['entry_price'])
        self.assertEqual(item['metadata']['paper_ledger']['exit_reason'], 'no_entry')

    def test_short_term_watch_waits_for_complete_window_before_no_entry_outcome(self):
        source = _feedback_source(
            feedback_id=27,
            agent_name='short-term-decider',
            action='watch_for_trigger',
        )
        repository = FakeDeciderEvaluationRepository([source])
        bars = [
            _bar('2026-04-24', 100, 100, 98, 99),
            _bar('2026-04-25', 99, 100, 97, 98),
            _bar('2026-04-28', 98, 100, 96, 97),
        ]
        service = DeciderPostDecisionEvaluationService(
            repository=repository,
            daily_bar_loader=lambda _symbol, **_kwargs: bars,
        )

        result = service.compute_evaluations(
            market_date_from='2026-04-23',
            market_date_to='2026-04-23',
            agent_names=['short-term-decider'],
        )

        item = result['items'][0]
        self.assertEqual(
            result['request']['evaluation_engine_version'],
            'decider_post_decision_eval_v3',
        )
        self.assertEqual(item['outcome_label'], OUTCOME_INVALID_MISSING_DATA)
        self.assertIn('incomplete_next_5_trading_days', item['data_quality_warnings'])
        self.assertEqual(
            item['metadata']['schema_version'],
            'decider_post_decision_eval_v3',
        )
        self.assertFalse(item['metadata']['price_window_complete'])
        self.assertIsNone(item['metadata']['paper_ledger'])

    def test_short_term_watch_does_not_close_triggered_trade_before_window_is_complete(self):
        source = _feedback_source(
            feedback_id=28,
            agent_name='short-term-decider',
            action='watch_for_trigger',
        )
        repository = FakeDeciderEvaluationRepository([source])
        bars = [
            _bar('2026-04-24', 100, 102, 98, 101),
            _bar('2026-04-25', 101, 103, 99, 102),
            _bar('2026-04-28', 102, 104, 100, 103),
        ]
        service = DeciderPostDecisionEvaluationService(
            repository=repository,
            daily_bar_loader=lambda _symbol, **_kwargs: bars,
        )

        result = service.compute_evaluations(
            market_date_from='2026-04-23',
            market_date_to='2026-04-23',
            agent_names=['short-term-decider'],
        )

        item = result['items'][0]
        self.assertEqual(item['outcome_label'], OUTCOME_INVALID_MISSING_DATA)
        self.assertEqual(item['entry_price'], 101)
        self.assertIn('incomplete_next_5_trading_days', item['data_quality_warnings'])
        self.assertFalse(item['metadata']['price_window_complete'])
        self.assertIsNone(item['metadata']['paper_ledger'])

    def test_short_term_watch_waits_when_observation_window_has_incomplete_ohlc(self):
        source = _feedback_source(
            feedback_id=29,
            agent_name='short-term-decider',
            action='watch_for_trigger',
        )
        repository = FakeDeciderEvaluationRepository([source])
        bars = [
            _bar('2026-04-24', 100, 100, 98, 99),
            _bar('2026-04-25', 99, 100, 97, 98),
            _bar('2026-04-28', 98, 100, 96, None),
            _bar('2026-04-29', 97, 100, 95, 96),
            _bar('2026-04-30', 96, 100, 94, 95),
        ]
        service = DeciderPostDecisionEvaluationService(
            repository=repository,
            daily_bar_loader=lambda _symbol, **_kwargs: bars,
        )

        result = service.compute_evaluations(
            market_date_from='2026-04-23',
            market_date_to='2026-04-23',
            agent_names=['short-term-decider'],
        )

        item = result['items'][0]
        self.assertEqual(item['outcome_label'], OUTCOME_INVALID_MISSING_DATA)
        self.assertNotIn('incomplete_next_5_trading_days', item['data_quality_warnings'])
        self.assertIn('incomplete_ohlc_in_observed_window', item['data_quality_warnings'])
        self.assertEqual(item['metadata']['observed_bars_count'], 5)
        self.assertEqual(item['metadata']['usable_bars_count'], 4)
        self.assertFalse(item['metadata']['price_window_complete'])
        self.assertIsNone(item['metadata']['paper_ledger'])

    def test_short_term_watch_rejects_missing_exit_levels_before_entry_scan(self):
        source = _feedback_source(
            feedback_id=25,
            agent_name='short-term-decider',
            action='watch_for_trigger',
            target_price=None,
        )
        repository = FakeDeciderEvaluationRepository([source])
        bars = [
            _bar(f'2026-04-{day:02d}', 100, 100, 99, 100)
            for day in (24, 25, 28, 29, 30)
        ]
        service = DeciderPostDecisionEvaluationService(
            repository=repository,
            daily_bar_loader=lambda _symbol, **_kwargs: bars,
        )

        result = service.compute_evaluations(
            market_date_from='2026-04-23',
            market_date_to='2026-04-23',
            agent_names=['short-term-decider'],
        )

        item = result['items'][0]
        self.assertEqual(item['outcome_label'], OUTCOME_INVALID_LEVELS)
        self.assertIn('missing_target_price', item['data_quality_warnings'])
        self.assertEqual(item['metadata']['action_interpretation'], 'trigger_gated_paper_watch')

    def test_compute_evaluations_dedupes_symbol_day_cohort_to_latest_revision(self):
        older = _feedback_source(feedback_id=31, decision_snapshot_id=91)
        newer = _feedback_source(feedback_id=32, decision_snapshot_id=92)
        newer['received_at'] = datetime(2026, 4, 23, 7, 0, tzinfo=timezone.utc)
        newer['feedback_received_at'] = newer['received_at']
        newer['created_at'] = newer['received_at']
        repository = FakeDeciderEvaluationRepository([older, newer])
        bars = [
            _bar('2026-04-24', 100, 104, 98, 103),
            _bar('2026-04-25', 103, 106, 101, 105),
            _bar('2026-04-28', 105, 109, 102, 108),
            _bar('2026-04-29', 108, 108, 101, 102),
            _bar('2026-04-30', 102, 103, 100, 101),
        ]
        service = DeciderPostDecisionEvaluationService(
            repository=repository,
            daily_bar_loader=lambda _symbol, **_kwargs: bars,
        )

        result = service.compute_evaluations(
            market_date_from='2026-04-23',
            market_date_to='2026-04-23',
        )

        self.assertEqual(result['summary']['feedback_rows_loaded'], 2)
        self.assertEqual(result['summary']['duplicate_revisions_ignored'], 1)
        self.assertEqual(result['summary']['feedback_rows_considered'], 1)
        self.assertEqual(result['items'][0]['source']['feedback_id'], 32)

    def test_opt_in_normal_regime_avoid_chase_shadow_rule_is_report_only(self):
        source = _feedback_source(
            feedback_id=41,
            agent_name='short-term-decider',
            action='avoid_chase',
            target_price=111,
            stop_loss_price=96,
        )
        source['normalized_feedback_payload'].update(
            {
                'trigger_price': 101,
                'chase_limit_price': 103,
                'confidence_score_10': 7.2,
                'wait_reason': {'code': 'extended_entry_price'},
            }
        )
        source['decision_request_payload'] = {
            'market_regime_context': {'market_regime': 'normal'},
            'symbol_day_context': {
                'technical_snapshot': {'breakout_20d': True},
                'action_summary': {'summary_payload': {'setup_state': 'continuation'}},
                'market_relative_context': {
                    'relative_strength': {'return_percentile': 0.80}
                },
            },
            'pre_decider_enrichment_context': {
                'trading_value': {
                    'source': 'pykrx',
                    'trading_value_source': 'raw',
                    'raw_trading_value_krw': 5_000_000_000,
                }
            },
            'decision_input_quality_context': {
                'final_judgment': {'buy_quality_score': 74},
            },
        }
        repository = FakeDeciderEvaluationRepository([source])
        bars = [
            _bar('2026-04-24', 100, 104, 98, 103),
            _bar('2026-04-25', 103, 112, 101, 111),
            _bar('2026-04-28', 111, 113, 108, 112),
            _bar('2026-04-29', 112, 114, 109, 113),
            _bar('2026-04-30', 113, 114, 111, 112),
        ]
        service = DeciderPostDecisionEvaluationService(
            repository=repository,
            daily_bar_loader=lambda _symbol, **_kwargs: bars,
        )

        result = service.compute_evaluations(
            market_date_from='2026-04-23',
            market_date_to='2026-04-23',
            agent_names=['short-term-decider'],
            enable_shadow_avoid_chase_rule=True,
        )

        item = result['items'][0]
        shadow = item['metadata']['shadow_avoid_chase_rule']
        self.assertEqual(item['action'], 'avoid_chase')
        self.assertEqual(item['outcome_label'], OUTCOME_TARGET_HIT)
        self.assertTrue(shadow['eligible'])
        self.assertEqual(shadow['metrics']['raw_trading_value_provider'], 'pykrx')
        self.assertEqual(shadow['mode'], 'report_only')
        self.assertFalse(shadow['live_behavior_changed'])
        self.assertEqual(result['summary']['shadow_avoid_chase']['eligible_count'], 1)

    def test_shadow_avoid_chase_requires_extended_entry_price_reason(self):
        source = _feedback_source(
            feedback_id=42,
            agent_name='short-term-decider',
            action='avoid_chase',
            target_price=111,
            stop_loss_price=96,
        )
        source['decision_request_payload'] = {
            'market_regime_context': {'market_regime': 'normal'},
            'symbol_day_context': {
                'technical_snapshot': {'breakout_20d': True},
                'market_relative_context': {
                    'relative_strength': {'return_percentile': 0.80}
                },
            },
            'pre_decider_enrichment_context': {
                'trading_value': {
                    'source': 'pykrx',
                    'trading_value_source': 'raw',
                    'raw_trading_value_krw': 5_000_000_000,
                }
            },
        }
        source['normalized_feedback_payload']['wait_reason'] = {
            'code': 'liquidity_below_threshold'
        }
        repository = FakeDeciderEvaluationRepository([source])
        service = DeciderPostDecisionEvaluationService(
            repository=repository,
            daily_bar_loader=lambda _symbol, **_kwargs: [],
        )

        result = service.compute_evaluations(
            market_date_from='2026-04-23',
            market_date_to='2026-04-23',
            agent_names=['short-term-decider'],
            enable_shadow_avoid_chase_rule=True,
        )

        shadow = result['items'][0]['metadata']['shadow_avoid_chase_rule']
        self.assertFalse(shadow['eligible'])
        self.assertFalse(shadow['checks']['extended_entry_price_reason'])

    def test_calibration_metrics_are_diagnostic_only_for_small_samples(self):
        source = _feedback_source(feedback_id=51, action='buy')
        source['normalized_feedback_payload']['confidence_score_10'] = 7.4
        source['decision_request_payload'] = {
            'decision_input_quality_context': {
                'final_judgment': {'buy_quality_score': 68},
            }
        }
        repository = FakeDeciderEvaluationRepository([source])
        bars = [
            _bar('2026-04-24', 100, 104, 98, 103),
            _bar('2026-04-25', 103, 106, 101, 105),
            _bar('2026-04-28', 105, 109, 102, 108),
            _bar('2026-04-29', 108, 108, 101, 102),
            _bar('2026-04-30', 102, 103, 100, 101),
        ]
        service = DeciderPostDecisionEvaluationService(
            repository=repository,
            daily_bar_loader=lambda _symbol, **_kwargs: bars,
        )

        result = service.compute_evaluations(
            market_date_from='2026-04-23',
            market_date_to='2026-04-23',
        )

        diagnostics = result['summary']['calibration_diagnostics']
        self.assertEqual(diagnostics['sample_count'], 1)
        self.assertEqual(diagnostics['confidence_score_10']['average'], 7.4)
        self.assertEqual(diagnostics['buy_quality_score']['average'], 68.0)
        self.assertEqual(diagnostics['policy'], 'diagnostic_only')
        self.assertFalse(diagnostics['threshold_adjustment_applied'])
        self.assertEqual(diagnostics['threshold_adjustment_reason'], 'automatic_threshold_changes_disabled')
