from __future__ import annotations

from contextlib import contextmanager
from datetime import date
import json
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

from sqlalchemy.exc import SQLAlchemyError

from app.eod.derived_stage_repair import (
    DERIVED_GUARD_IDLE_TRANSACTION_TIMEOUT_MS,
    ExactDerivedStageRepair,
    GuardTransactionExitError,
    MANIFEST_SCHEMA_VERSION,
    capture_guard_transaction_failure,
    exact_derived_stage_guard,
)
from app.eod.market_membership import market_membership_sha256
from app.internal.exact_repair_manifest import canonical_json_sha256


SYMBOLS = ['000660', '005930']


def _manifest(*, market_date: str = '2026-10-28', symbols=None) -> dict:
    exact_symbols = list(symbols or SYMBOLS)
    return {
        'schema_version': MANIFEST_SCHEMA_VERSION,
        'date_count': 1,
        'dates': [
            {
                'market_date': market_date,
                'symbol_count': len(exact_symbols),
                'membership_sha256': market_membership_sha256(exact_symbols),
                'symbols': exact_symbols,
            }
        ],
    }


@contextmanager
def _guard(**_kwargs):
    yield True, None


def _stage_result(successful_symbols, *, stage_id, exact_symbols=None):
    successes = list(successful_symbols)
    exact = list(exact_symbols or SYMBOLS)
    return {
        'successful_stock_codes': successes,
        'retry_debt_stock_codes': sorted(set(exact) - set(successes)),
        'items': [{'symbol': symbol} for symbol in successes],
        'scope_finalization': {
            'stage': stage_id,
            'market_date': '2026-10-28',
            'exact_symbol_count': len(exact),
            'exact_membership_sha256': market_membership_sha256(exact),
            'successful_symbol_count': len(successes),
            'successful_membership_sha256': market_membership_sha256(
                successes
            ),
            'ready': True,
        },
    }


def _completion_state(
    memberships=None,
    *,
    source_version='v1',
    output_version='v1',
):
    observed = memberships or {
        stage_id: list(SYMBOLS)
        for stage_id in (
            'price_contexts',
            'technical_snapshots',
            'action_summaries',
        )
    }
    return {
        'source_fingerprint': {
            'history_row_limit': 60,
            'row_count': 120,
            'source_sha256': canonical_json_sha256(
                {'source_version': source_version}
            ),
        },
        'memberships': {
            stage_id: list(symbols)
            for stage_id, symbols in observed.items()
        },
        'output_fingerprints': {
            stage_id: {
                'row_count': len(symbols),
                'output_sha256': canonical_json_sha256(
                    {
                        'stage_id': stage_id,
                        'symbols': list(symbols),
                        'output_version': output_version,
                    }
                ),
            }
            for stage_id, symbols in observed.items()
        },
    }


class _ScalarResult:
    def __init__(self, value):
        self.value = value

    def scalar_one(self):
        return self.value


class _GuardSession:
    def __init__(self, counts, events):
        self.counts = iter(counts)
        self.events = events

    def get_bind(self):
        return SimpleNamespace(dialect=SimpleNamespace(name='postgresql'))

    def execute(self, statement, *_args, **_kwargs):
        rendered = str(statement)
        if rendered.startswith('SET LOCAL idle_in_transaction_session_timeout'):
            self.events.append(rendered)
            return None
        if rendered.startswith('LOCK TABLE'):
            self.events.append(
                'symbol_writers_locked'
                if rendered == 'LOCK TABLE symbols IN SHARE MODE'
                else 'writer_tables_locked'
            )
            return None
        if 'pg_advisory_xact_lock' in rendered:
            self.events.append('dart_signal_writer_locked')
            return None
        self.events.append('active_writer_checked')
        return _ScalarResult(next(self.counts))


class ExactDerivedStageRepairTests(unittest.TestCase):
    def test_guard_failure_capture_does_not_swallow_entry_sql_error(self):
        @contextmanager
        def failing_entry_guard(**_kwargs):
            raise SQLAlchemyError('entry failed')
            yield True, None

        state = {}
        with self.assertRaisesRegex(SQLAlchemyError, 'entry failed'):
            with capture_guard_transaction_failure(
                failing_entry_guard,
                state=state,
            ):
                pass

        self.assertEqual(state, {})

    def test_guard_failure_capture_does_not_swallow_body_sql_error(self):
        @contextmanager
        def healthy_guard(**_kwargs):
            yield True, None

        state = {}
        with self.assertRaisesRegex(SQLAlchemyError, 'body failed'):
            with capture_guard_transaction_failure(
                healthy_guard,
                state=state,
            ):
                raise SQLAlchemyError('body failed')

        self.assertEqual(state, {})

    def test_guard_failure_capture_marks_post_yield_sql_error(self):
        @contextmanager
        def failing_exit_guard(**_kwargs):
            yield True, None
            raise SQLAlchemyError('exit failed')

        state = {}
        with self.assertRaises(GuardTransactionExitError):
            with capture_guard_transaction_failure(
                failing_exit_guard,
                state=state,
            ):
                pass

        self.assertIsInstance(state.get('error'), SQLAlchemyError)

    def test_guard_failure_capture_propagates_non_sql_exit_error(self):
        @contextmanager
        def failing_exit_guard(**_kwargs):
            yield True, None
            raise RuntimeError('unexpected exit failure')

        state = {}
        with self.assertRaisesRegex(
            RuntimeError,
            'unexpected exit failure',
        ):
            with capture_guard_transaction_failure(
                failing_exit_guard,
                state=state,
            ):
                pass

        self.assertEqual(state, {})

    def test_guard_rejects_unbounded_or_role_default_idle_budget(self):
        with patch(
            'app.eod.derived_stage_repair.'
            'DERIVED_GUARD_IDLE_TRANSACTION_TIMEOUT_MS',
            60_000,
        ):
            with self.assertRaisesRegex(
                ValueError,
                'idle transaction budget is invalid',
            ):
                with exact_derived_stage_guard(
                    market_date=date(2026, 10, 28)
                ):
                    pass

    def test_guard_requires_global_price_writer_admission_for_history_reads(self):
        observed = {}

        @contextmanager
        def blocked_history_lock(**kwargs):
            observed.update(kwargs)
            yield False, '2026-10-27'

        with patch(
            'app.eod.derived_stage_repair.advisory_eod_collection_locks',
            blocked_history_lock,
        ):
            with exact_derived_stage_guard(
                market_date=date(2026, 10, 28)
            ) as (acquired, reason):
                self.assertFalse(acquired)
                self.assertEqual(reason, 'daily_eod_advisory_lock_unavailable')

        self.assertTrue(observed['exclusive_admission'])
        self.assertEqual(observed['source'], 'daily_price_bars')
        self.assertEqual(observed['trade_dates'], ('2026-10-28',))

    def test_guard_holds_writer_table_fence_through_repair_body(self):
        events = []
        session = _GuardSession([0, 0, 0, 0, 0], events)

        @contextmanager
        def advisory_lock(**_kwargs):
            events.append('advisory_lock_entered')
            try:
                self.assertTrue(_kwargs['exclusive_admission'])
                yield True, None
            finally:
                events.append('advisory_lock_released')

        @contextmanager
        def guarded_session(**kwargs):
            self.assertEqual(kwargs['lock_timeout_ms'], 1)
            self.assertTrue(kwargs['isolated_connection'])
            events.append('writer_session_entered')
            try:
                yield session
            finally:
                events.append('writer_session_released')

        with (
            patch(
                'app.eod.derived_stage_repair.advisory_eod_collection_locks',
                advisory_lock,
            ),
            patch(
                'app.eod.derived_stage_repair.session_scope',
                guarded_session,
            ),
        ):
            with exact_derived_stage_guard(
                market_date=date(2026, 10, 28)
            ) as (acquired, reason):
                self.assertTrue(acquired)
                self.assertIsNone(reason)
                self.assertIn('writer_tables_locked', events)
                self.assertIn('dart_signal_writer_locked', events)
                self.assertNotIn('writer_session_released', events)
                events.append('repair_body')

        self.assertLess(
            events.index('writer_tables_locked'),
            events.index('repair_body'),
        )
        self.assertLess(
            events.index('repair_body'),
            events.index('writer_session_released'),
        )
        self.assertIn(
            'SET LOCAL idle_in_transaction_session_timeout '
            f'= {DERIVED_GUARD_IDLE_TRANSACTION_TIMEOUT_MS}',
            events,
        )
        self.assertLess(
            events.index(
                'SET LOCAL idle_in_transaction_session_timeout '
                f'= {DERIVED_GUARD_IDLE_TRANSACTION_TIMEOUT_MS}'
            ),
            events.index('writer_tables_locked'),
        )

    def test_factor_guard_blocks_global_symbol_writers_through_repair_body(self):
        events = []
        session = _GuardSession([0, 0, 0, 0, 0], events)

        @contextmanager
        def advisory_lock(**_kwargs):
            yield True, None

        @contextmanager
        def guarded_session(**_kwargs):
            try:
                yield session
            finally:
                events.append('writer_session_released')

        with (
            patch(
                'app.eod.derived_stage_repair.advisory_eod_collection_locks',
                advisory_lock,
            ),
            patch(
                'app.eod.derived_stage_repair.session_scope',
                guarded_session,
            ),
        ):
            with exact_derived_stage_guard(
                market_date=date(2026, 10, 28),
                lock_symbol_writes=True,
            ) as (acquired, reason):
                self.assertTrue(acquired)
                self.assertIsNone(reason)
                self.assertIn('symbol_writers_locked', events)
                events.append('repair_body')

        self.assertLess(
            events.index('symbol_writers_locked'),
            events.index('repair_body'),
        )
        self.assertLess(
            events.index('repair_body'),
            events.index('writer_session_released'),
        )

    def test_guard_refuses_active_daily_writer(self):
        events = []
        session = _GuardSession([1, 0], events)

        @contextmanager
        def advisory_lock(**_kwargs):
            yield True, None

        @contextmanager
        def guarded_session(**_kwargs):
            yield session

        with (
            patch(
                'app.eod.derived_stage_repair.advisory_eod_collection_locks',
                advisory_lock,
            ),
            patch(
                'app.eod.derived_stage_repair.session_scope',
                guarded_session,
            ),
            exact_derived_stage_guard(
                market_date=date(2026, 10, 28)
            ) as (acquired, reason),
        ):
            self.assertFalse(acquired)
            self.assertEqual(reason, 'active_daily_or_eod_writer')

    def test_exact_scope_is_preserved_across_causal_stage_chain(self):
        price_runner = Mock(
            return_value=_stage_result(
                ['000660'],
                stage_id='price_contexts',
            )
        )
        technical_runner = Mock(
            return_value=_stage_result(
                ['000660'],
                stage_id='technical_snapshots',
            )
        )
        action_runner = Mock(
            return_value=_stage_result(
                ['000660'],
                stage_id='action_summaries',
            )
        )
        repair = ExactDerivedStageRepair(
            _manifest(),
            price_runner=price_runner,
            technical_runner=technical_runner,
            action_runner=action_runner,
            stored_symbols_loader=lambda _market_date: SYMBOLS,
            completion_state_loader=lambda *_args: _completion_state(),
            guard_factory=_guard,
            finality_validator=lambda _market_date: None,
        )
        with TemporaryDirectory() as directory:
            result = repair.run(
                apply=True,
                expected_manifest_sha256=repair.manifest_sha256,
                checkpoint_path=Path(directory) / 'checkpoint.json',
            )

        self.assertEqual(result['status'], 'partial')
        price_runner.assert_called_once()
        self.assertEqual(
            price_runner.call_args.kwargs['symbols'],
            SYMBOLS,
        )
        self.assertEqual(
            price_runner.call_args.kwargs['exact_scope_symbols'],
            SYMBOLS,
        )
        self.assertEqual(
            technical_runner.call_args.kwargs['symbols'],
            ['000660'],
        )
        self.assertEqual(
            technical_runner.call_args.kwargs['exact_scope_symbols'],
            SYMBOLS,
        )
        self.assertEqual(
            action_runner.call_args.kwargs['symbols'],
            ['000660'],
        )
        self.assertEqual(
            action_runner.call_args.kwargs['exact_scope_symbols'],
            SYMBOLS,
        )

    def test_empty_upstream_success_still_atomically_clears_downstream_scope(self):
        price_runner = Mock(
            return_value=_stage_result([], stage_id='price_contexts')
        )
        technical_runner = Mock(
            return_value=_stage_result([], stage_id='technical_snapshots')
        )
        action_runner = Mock(
            return_value=_stage_result([], stage_id='action_summaries')
        )
        repair = ExactDerivedStageRepair(
            _manifest(),
            price_runner=price_runner,
            technical_runner=technical_runner,
            action_runner=action_runner,
            stored_symbols_loader=lambda _market_date: SYMBOLS,
            completion_state_loader=lambda *_args: _completion_state(),
            guard_factory=_guard,
            finality_validator=lambda _market_date: None,
        )

        with TemporaryDirectory() as directory:
            result = repair.run(
                apply=True,
                expected_manifest_sha256=repair.manifest_sha256,
                checkpoint_path=Path(directory) / 'checkpoint.json',
            )

        self.assertEqual(result['status'], 'partial')
        self.assertEqual(technical_runner.call_args.kwargs['symbols'], [])
        self.assertEqual(action_runner.call_args.kwargs['symbols'], [])
        self.assertEqual(
            technical_runner.call_args.kwargs['exact_scope_symbols'],
            SYMBOLS,
        )
        self.assertEqual(
            action_runner.call_args.kwargs['exact_scope_symbols'],
            SYMBOLS,
        )

    def test_guard_exit_failure_reconciles_durable_derived_rows(self):
        @contextmanager
        def failing_exit_guard(**_kwargs):
            yield True, None
            raise SQLAlchemyError('guard commit failed')

        repair = ExactDerivedStageRepair(
            _manifest(),
            price_runner=Mock(
                return_value=_stage_result(
                    SYMBOLS,
                    stage_id='price_contexts',
                )
            ),
            technical_runner=Mock(
                return_value=_stage_result(
                    SYMBOLS,
                    stage_id='technical_snapshots',
                )
            ),
            action_runner=Mock(
                return_value=_stage_result(
                    SYMBOLS,
                    stage_id='action_summaries',
                )
            ),
            stored_symbols_loader=lambda _market_date: SYMBOLS,
            completion_state_loader=lambda *_args: _completion_state(),
            guard_factory=failing_exit_guard,
            finality_validator=lambda _market_date: None,
        )
        with TemporaryDirectory() as directory:
            checkpoint = Path(directory) / 'checkpoint.json'
            result = repair.run(
                apply=True,
                expected_manifest_sha256=repair.manifest_sha256,
                checkpoint_path=checkpoint,
            )
            saved = json.loads(checkpoint.read_text())

        self.assertEqual(result['status'], 'partial')
        self.assertEqual(result['reason'], 'guard_transaction_exit_failed')
        self.assertTrue(result['uncertain_commit'])
        self.assertEqual(result['reconciled_proof_count'], 1)
        self.assertTrue(
            saved['units']['2026-10-28']['last_uncertain_attempt'][
                'live_completion_verified'
            ]
        )

    def test_guard_exit_failure_reconciles_durable_partial_derived_rows(self):
        @contextmanager
        def failing_exit_guard(**_kwargs):
            yield True, None
            raise SQLAlchemyError('guard commit failed')

        partial_memberships = {
            stage_id: ['000660']
            for stage_id in (
                'price_contexts',
                'technical_snapshots',
                'action_summaries',
            )
        }
        repair = ExactDerivedStageRepair(
            _manifest(),
            price_runner=Mock(
                return_value=_stage_result(
                    ['000660'],
                    stage_id='price_contexts',
                )
            ),
            technical_runner=Mock(
                return_value=_stage_result(
                    ['000660'],
                    stage_id='technical_snapshots',
                )
            ),
            action_runner=Mock(
                return_value=_stage_result(
                    ['000660'],
                    stage_id='action_summaries',
                )
            ),
            stored_symbols_loader=lambda _market_date: SYMBOLS,
            completion_state_loader=lambda *_args: _completion_state(
                partial_memberships
            ),
            guard_factory=failing_exit_guard,
            finality_validator=lambda _market_date: None,
        )
        with TemporaryDirectory() as directory:
            result = repair.run(
                apply=True,
                expected_manifest_sha256=repair.manifest_sha256,
                checkpoint_path=Path(directory) / 'checkpoint.json',
            )

        self.assertEqual(result['status'], 'partial')
        self.assertEqual(result['reason'], 'guard_transaction_exit_failed')
        self.assertTrue(result['uncertain_commit'])
        self.assertTrue(result['retry_required'])
        self.assertEqual(result['reconciled_proof_count'], 1)

    def test_guard_exit_failure_blocks_when_derived_live_proof_drifts(self):
        @contextmanager
        def failing_exit_guard(**_kwargs):
            yield True, None
            raise SQLAlchemyError('guard commit failed')

        states = iter(
            [
                _completion_state(),
                _completion_state(),
                _completion_state(output_version='drifted'),
            ]
        )
        repair = ExactDerivedStageRepair(
            _manifest(),
            price_runner=Mock(
                return_value=_stage_result(
                    SYMBOLS,
                    stage_id='price_contexts',
                )
            ),
            technical_runner=Mock(
                return_value=_stage_result(
                    SYMBOLS,
                    stage_id='technical_snapshots',
                )
            ),
            action_runner=Mock(
                return_value=_stage_result(
                    SYMBOLS,
                    stage_id='action_summaries',
                )
            ),
            stored_symbols_loader=lambda _market_date: SYMBOLS,
            completion_state_loader=lambda *_args: next(states),
            guard_factory=failing_exit_guard,
            finality_validator=lambda _market_date: None,
        )
        with TemporaryDirectory() as directory:
            checkpoint = Path(directory) / 'checkpoint.json'
            result = repair.run(
                apply=True,
                expected_manifest_sha256=repair.manifest_sha256,
                checkpoint_path=checkpoint,
            )
            saved = json.loads(checkpoint.read_text())

        self.assertEqual(result['status'], 'blocked')
        self.assertEqual(result['reason'], 'guard_transaction_exit_failed')
        self.assertTrue(result['uncertain_commit'])
        self.assertTrue(result['retry_required'])
        self.assertEqual(result['reconciled_proof_count'], 0)
        self.assertFalse(
            saved['units']['2026-10-28']['last_uncertain_attempt'][
                'live_completion_verified'
            ]
        )

    def test_guard_exit_failure_cancels_active_writer_return(self):
        @contextmanager
        def failing_blocked_guard(**_kwargs):
            yield False, 'active_writer'
            raise SQLAlchemyError('guard commit failed')

        repair = ExactDerivedStageRepair(
            _manifest(),
            price_runner=Mock(),
            technical_runner=Mock(),
            action_runner=Mock(),
            stored_symbols_loader=lambda _market_date: SYMBOLS,
            guard_factory=failing_blocked_guard,
            finality_validator=lambda _market_date: None,
        )
        with TemporaryDirectory() as directory:
            result = repair.run(
                apply=True,
                expected_manifest_sha256=repair.manifest_sha256,
                checkpoint_path=Path(directory) / 'checkpoint.json',
            )

        self.assertEqual(result['status'], 'blocked')
        self.assertEqual(result['reason'], 'guard_transaction_exit_failed')
        self.assertEqual(result['error_type'], 'SQLAlchemyError')
        self.assertTrue(result['uncertain_commit'])

    def test_guard_exit_failure_cancels_completed_checkpoint_continue(self):
        @contextmanager
        def failing_exit_guard(**_kwargs):
            yield True, None
            raise SQLAlchemyError('guard commit failed')

        price_runner = Mock(
            return_value=_stage_result(
                SYMBOLS,
                stage_id='price_contexts',
            )
        )
        technical_runner = Mock(
            return_value=_stage_result(
                SYMBOLS,
                stage_id='technical_snapshots',
            )
        )
        action_runner = Mock(
            return_value=_stage_result(
                SYMBOLS,
                stage_id='action_summaries',
            )
        )
        common_kwargs = {
            'price_runner': price_runner,
            'technical_runner': technical_runner,
            'action_runner': action_runner,
            'stored_symbols_loader': lambda _market_date: SYMBOLS,
            'completion_state_loader': lambda *_args: _completion_state(),
            'finality_validator': lambda _market_date: None,
        }
        initial = ExactDerivedStageRepair(
            _manifest(),
            guard_factory=_guard,
            **common_kwargs,
        )
        with TemporaryDirectory() as directory:
            checkpoint = Path(directory) / 'checkpoint.json'
            completed = initial.run(
                apply=True,
                expected_manifest_sha256=initial.manifest_sha256,
                checkpoint_path=checkpoint,
            )
            saved_before = json.loads(checkpoint.read_text())
            resumed = ExactDerivedStageRepair(
                _manifest(),
                guard_factory=failing_exit_guard,
                **common_kwargs,
            ).run(
                apply=True,
                expected_manifest_sha256=initial.manifest_sha256,
                checkpoint_path=checkpoint,
            )
            saved_after = json.loads(checkpoint.read_text())

        self.assertEqual(completed['status'], 'completed')
        self.assertEqual(resumed['status'], 'blocked')
        self.assertEqual(
            resumed['reason'],
            'guard_transaction_exit_failed',
        )
        self.assertEqual(resumed['error_type'], 'SQLAlchemyError')
        self.assertEqual(price_runner.call_count, 1)
        self.assertEqual(technical_runner.call_count, 1)
        self.assertEqual(action_runner.call_count, 1)
        self.assertEqual(
            saved_after['units']['2026-10-28']['status'],
            saved_before['units']['2026-10-28']['status'],
        )
        self.assertEqual(
            saved_after['units']['2026-10-28']['result'],
            saved_before['units']['2026-10-28']['result'],
        )

    def test_checkpoint_failure_reconciles_durable_derived_rows(self):
        repair = ExactDerivedStageRepair(
            _manifest(),
            price_runner=Mock(
                return_value=_stage_result(
                    SYMBOLS,
                    stage_id='price_contexts',
                )
            ),
            technical_runner=Mock(
                return_value=_stage_result(
                    SYMBOLS,
                    stage_id='technical_snapshots',
                )
            ),
            action_runner=Mock(
                return_value=_stage_result(
                    SYMBOLS,
                    stage_id='action_summaries',
                )
            ),
            stored_symbols_loader=lambda _market_date: SYMBOLS,
            completion_state_loader=lambda *_args: _completion_state(),
            guard_factory=_guard,
            finality_validator=lambda _market_date: None,
        )
        with (
            TemporaryDirectory() as directory,
            patch(
                'app.eod.derived_stage_repair.write_checkpoint',
                side_effect=OSError('checkpoint disk unavailable'),
            ),
        ):
            result = repair.run(
                apply=True,
                expected_manifest_sha256=repair.manifest_sha256,
                checkpoint_path=Path(directory) / 'checkpoint.json',
            )

        self.assertEqual(result['status'], 'partial')
        self.assertEqual(
            result['reason'],
            'checkpoint_write_failed_after_commit',
        )
        self.assertTrue(result['uncertain_commit'])
        self.assertFalse(result['checkpoint_persisted'])
        self.assertEqual(result['reconciled_proof_count'], 1)

    def test_dry_run_validates_scope_without_running_stages(self):
        price_runner = Mock()
        technical_runner = Mock()
        action_runner = Mock()
        repair = ExactDerivedStageRepair(
            _manifest(),
            price_runner=price_runner,
            technical_runner=technical_runner,
            action_runner=action_runner,
            stored_symbols_loader=lambda _market_date: SYMBOLS,
            guard_factory=_guard,
            finality_validator=lambda _market_date: None,
        )

        result = repair.run()

        self.assertEqual(result['status'], 'dry_run')
        self.assertEqual(result['validated_date_count'], 1)
        price_runner.assert_not_called()
        technical_runner.assert_not_called()
        action_runner.assert_not_called()

    def test_stored_membership_drift_blocks_dry_run(self):
        repair = ExactDerivedStageRepair(
            _manifest(),
            price_runner=Mock(),
            technical_runner=Mock(),
            action_runner=Mock(),
            stored_symbols_loader=lambda _market_date: ['000660'],
            guard_factory=_guard,
            finality_validator=lambda _market_date: None,
        )

        result = repair.run()

        self.assertEqual(result['status'], 'blocked')
        self.assertIn('005930', result['scope_errors'][0]['error'])

    def test_checkpoint_rejects_changed_date_manifest(self):
        shared = {
            'price_runner': Mock(
                return_value=_stage_result(
                    SYMBOLS,
                    stage_id='price_contexts',
                )
            ),
            'technical_runner': Mock(
                return_value=_stage_result(
                    SYMBOLS,
                    stage_id='technical_snapshots',
                )
            ),
            'action_runner': Mock(
                return_value=_stage_result(
                    SYMBOLS,
                    stage_id='action_summaries',
                )
            ),
            'stored_symbols_loader': lambda _market_date: SYMBOLS,
            'completion_state_loader': lambda *_args: _completion_state(),
            'guard_factory': _guard,
            'finality_validator': lambda _market_date: None,
        }
        first = ExactDerivedStageRepair(_manifest(), **shared)
        second = ExactDerivedStageRepair(
            _manifest(market_date='2026-10-29'),
            **shared,
        )
        with TemporaryDirectory() as directory:
            checkpoint = Path(directory) / 'checkpoint.json'
            completed = first.run(
                apply=True,
                expected_manifest_sha256=first.manifest_sha256,
                checkpoint_path=checkpoint,
            )
            blocked = second.run(
                apply=True,
                expected_manifest_sha256=second.manifest_sha256,
                checkpoint_path=checkpoint,
            )

        self.assertEqual(completed['status'], 'completed')
        self.assertEqual(blocked['status'], 'blocked')
        self.assertEqual(blocked['reason'], 'checkpoint_manifest_mismatch')

    def test_completed_date_is_rebuilt_after_durable_output_deletion(self):
        memberships = {
            stage_id: list(SYMBOLS) for stage_id in (
                'price_contexts',
                'technical_snapshots',
                'action_summaries',
            )
        }

        def runner(stage_id):
            def run(**kwargs):
                successes = list(kwargs['symbols'])
                memberships[stage_id] = successes
                return _stage_result(
                    successes,
                    stage_id=stage_id,
                    exact_symbols=kwargs['exact_scope_symbols'],
                )

            return Mock(side_effect=run)

        price_runner = runner('price_contexts')
        technical_runner = runner('technical_snapshots')
        action_runner = runner('action_summaries')
        repair = ExactDerivedStageRepair(
            _manifest(),
            price_runner=price_runner,
            technical_runner=technical_runner,
            action_runner=action_runner,
            stored_symbols_loader=lambda _market_date: SYMBOLS,
            completion_state_loader=lambda *_args: _completion_state(
                memberships
            ),
            guard_factory=_guard,
            finality_validator=lambda _market_date: None,
        )

        with TemporaryDirectory() as directory:
            checkpoint = Path(directory) / 'checkpoint.json'
            first = repair.run(
                apply=True,
                expected_manifest_sha256=repair.manifest_sha256,
                checkpoint_path=checkpoint,
            )
            memberships['technical_snapshots'] = ['000660']
            resumed = repair.run(
                apply=True,
                expected_manifest_sha256=repair.manifest_sha256,
                checkpoint_path=checkpoint,
            )

        self.assertEqual(first['status'], 'completed')
        self.assertEqual(resumed['status'], 'completed')
        self.assertEqual(resumed['skipped_dates'], 0)
        self.assertEqual(price_runner.call_count, 2)
        self.assertEqual(technical_runner.call_count, 2)
        self.assertEqual(action_runner.call_count, 2)

    def test_completed_date_does_not_survive_stored_input_drift(self):
        stored_symbols = list(SYMBOLS)
        stage_memberships = {
            stage_id: list(SYMBOLS) for stage_id in (
                'price_contexts',
                'technical_snapshots',
                'action_summaries',
            )
        }
        price_runner = Mock(
            return_value=_stage_result(SYMBOLS, stage_id='price_contexts')
        )
        technical_runner = Mock(
            return_value=_stage_result(SYMBOLS, stage_id='technical_snapshots')
        )
        action_runner = Mock(
            return_value=_stage_result(SYMBOLS, stage_id='action_summaries')
        )
        repair = ExactDerivedStageRepair(
            _manifest(),
            price_runner=price_runner,
            technical_runner=technical_runner,
            action_runner=action_runner,
            stored_symbols_loader=lambda _market_date: list(stored_symbols),
            completion_state_loader=lambda *_args: _completion_state(
                stage_memberships
            ),
            guard_factory=_guard,
            finality_validator=lambda _market_date: None,
        )

        with TemporaryDirectory() as directory:
            checkpoint = Path(directory) / 'checkpoint.json'
            first = repair.run(
                apply=True,
                expected_manifest_sha256=repair.manifest_sha256,
                checkpoint_path=checkpoint,
            )
            stored_symbols[:] = ['000660']
            resumed = repair.run(
                apply=True,
                expected_manifest_sha256=repair.manifest_sha256,
                checkpoint_path=checkpoint,
            )

        self.assertEqual(first['status'], 'completed')
        self.assertEqual(resumed['status'], 'partial')
        self.assertEqual(resumed['skipped_dates'], 0)
        self.assertEqual(price_runner.call_count, 1)
        self.assertEqual(technical_runner.call_count, 1)
        self.assertEqual(action_runner.call_count, 1)

    def test_completed_date_is_rebuilt_after_same_membership_content_drift(self):
        output_version = ['v1']
        price_runner = Mock(
            return_value=_stage_result(SYMBOLS, stage_id='price_contexts')
        )
        technical_runner = Mock(
            return_value=_stage_result(SYMBOLS, stage_id='technical_snapshots')
        )
        action_runner = Mock(
            return_value=_stage_result(SYMBOLS, stage_id='action_summaries')
        )

        def state(*_args):
            current = _completion_state(output_version=output_version[0])
            output_version[0] = 'v1'
            return current

        repair = ExactDerivedStageRepair(
            _manifest(),
            price_runner=price_runner,
            technical_runner=technical_runner,
            action_runner=action_runner,
            stored_symbols_loader=lambda _market_date: SYMBOLS,
            completion_state_loader=state,
            guard_factory=_guard,
            finality_validator=lambda _market_date: None,
        )
        with TemporaryDirectory() as directory:
            checkpoint = Path(directory) / 'checkpoint.json'
            first = repair.run(
                apply=True,
                expected_manifest_sha256=repair.manifest_sha256,
                checkpoint_path=checkpoint,
            )
            output_version[0] = 'drifted'
            resumed = repair.run(
                apply=True,
                expected_manifest_sha256=repair.manifest_sha256,
                checkpoint_path=checkpoint,
            )

        self.assertEqual(first['status'], 'completed')
        self.assertEqual(resumed['status'], 'completed')
        self.assertEqual(resumed['skipped_dates'], 0)
        self.assertEqual(price_runner.call_count, 2)

    def test_completed_date_is_rebuilt_after_source_content_drift(self):
        source_version = ['v1']
        price_runner = Mock(
            return_value=_stage_result(SYMBOLS, stage_id='price_contexts')
        )
        technical_runner = Mock(
            return_value=_stage_result(SYMBOLS, stage_id='technical_snapshots')
        )
        action_runner = Mock(
            return_value=_stage_result(SYMBOLS, stage_id='action_summaries')
        )

        def state(*_args):
            current = _completion_state(source_version=source_version[0])
            source_version[0] = 'v1'
            return current

        repair = ExactDerivedStageRepair(
            _manifest(),
            price_runner=price_runner,
            technical_runner=technical_runner,
            action_runner=action_runner,
            stored_symbols_loader=lambda _market_date: SYMBOLS,
            completion_state_loader=state,
            guard_factory=_guard,
            finality_validator=lambda _market_date: None,
        )
        with TemporaryDirectory() as directory:
            checkpoint = Path(directory) / 'checkpoint.json'
            first = repair.run(
                apply=True,
                expected_manifest_sha256=repair.manifest_sha256,
                checkpoint_path=checkpoint,
            )
            source_version[0] = 'drifted'
            resumed = repair.run(
                apply=True,
                expected_manifest_sha256=repair.manifest_sha256,
                checkpoint_path=checkpoint,
            )

        self.assertEqual(first['status'], 'completed')
        self.assertEqual(resumed['status'], 'completed')
        self.assertEqual(resumed['skipped_dates'], 0)
        self.assertEqual(price_runner.call_count, 2)


if __name__ == '__main__':
    unittest.main()
