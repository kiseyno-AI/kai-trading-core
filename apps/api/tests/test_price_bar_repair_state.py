from __future__ import annotations

from contextlib import contextmanager
from datetime import date, datetime, timedelta, timezone
from types import SimpleNamespace
import unittest
from unittest.mock import MagicMock, patch

from app.db_models import DailyMomentumFactorResult
from app.eod.market_membership import market_membership_sha256
from app.eod.price_bar_repair_state import (
    _causal_raw_dependency_watermarks,
    _exact_factor_stage_evidence,
    _signal_context_rebuild_evidence,
    _verify_pending_price_bar_rebuilds_locked,
    rebuild_pending_price_dependent_evaluations,
    record_pending_exact_factor_rebuild,
    verify_pending_price_bar_rebuilds,
)


MARKET_DATE = date(2026, 9, 23)
UTC_NOW = datetime(2026, 9, 24, 1, 0, tzinfo=timezone.utc)


@contextmanager
def _session_scope_for(session):
    yield session


class PriceBarRepairFactorEvidenceTests(unittest.TestCase):
    def test_causal_watermarks_flow_forward_without_pushing_backward(
        self,
    ) -> None:
        first_date = MARKET_DATE - timedelta(days=3)
        second_date = MARKET_DATE - timedelta(days=2)
        third_date = MARKET_DATE - timedelta(days=1)
        first_raw_at = UTC_NOW - timedelta(minutes=5)
        second_raw_at = UTC_NOW - timedelta(minutes=30)
        third_raw_at = UTC_NOW - timedelta(minutes=10)
        later_raw_at = UTC_NOW
        session = MagicMock()
        session.execute.return_value.all.return_value = [
            (first_date, first_raw_at),
            (second_date, second_raw_at),
            (third_date, third_raw_at),
            (MARKET_DATE, later_raw_at),
        ]

        result = _causal_raw_dependency_watermarks(
            session,
            required_dates=(
                first_date,
                second_date,
                third_date,
                MARKET_DATE,
            ),
        )

        self.assertEqual(result[first_date], first_raw_at)
        self.assertEqual(result[second_date], first_raw_at)
        self.assertEqual(result[third_date], first_raw_at)
        self.assertEqual(result[MARKET_DATE], later_raw_at)

    def test_factor_verifier_uses_causal_watermark_for_each_output_date(
        self,
    ) -> None:
        earlier_date = MARKET_DATE - timedelta(days=1)
        symbols = ['000660', '005930']
        rebuilt_at = UTC_NOW - timedelta(minutes=30)
        own_raw_at = UTC_NOW - timedelta(minutes=20)
        derived_at = UTC_NOW - timedelta(minutes=10)
        unrelated_later_raw_at = UTC_NOW - timedelta(minutes=5)
        expected_input_hash = 'unchanged-input-sha256'
        session = MagicMock()
        session.execute.return_value.all.return_value = [
            (symbol, derived_at, index, own_raw_at)
            for index, symbol in enumerate(symbols, start=1)
        ]
        metadata = {
            'dates': {
                earlier_date.isoformat(): {
                    'exact_membership_count': 2,
                    'ranking_membership_sha256': (
                        market_membership_sha256(symbols)
                    ),
                    'daily_price_input_sha256': expected_input_hash,
                    'repair_started_at': rebuilt_at.isoformat(),
                }
            }
        }

        with patch(
            'app.eod.price_bar_repair_state._daily_price_input_fingerprint',
            return_value=(expected_input_hash, 2),
        ):
            result = _exact_factor_stage_evidence(
                session,
                model=DailyMomentumFactorResult,
                required_dates=(earlier_date,),
                factor_rebuild_metadata=metadata,
                raw_dependency_watermarks={
                    earlier_date: own_raw_at,
                    MARKET_DATE: unrelated_later_raw_at,
                },
            )

        self.assertTrue(result['ready'])
        self.assertEqual(result['dates'][0]['fresh_after_dependency'], 2)

    def test_factor_verifier_rejects_later_output_older_than_earlier_raw(
        self,
    ) -> None:
        earlier_date = MARKET_DATE - timedelta(days=1)
        symbols = ['000660', '005930']
        rebuilt_at = UTC_NOW - timedelta(minutes=30)
        own_raw_at = UTC_NOW - timedelta(minutes=25)
        derived_at = UTC_NOW - timedelta(minutes=10)
        earlier_repaired_raw_at = UTC_NOW - timedelta(minutes=5)
        expected_input_hash = 'unchanged-input-sha256'
        session = MagicMock()
        session.execute.return_value.all.return_value = [
            (symbol, derived_at, index, own_raw_at)
            for index, symbol in enumerate(symbols, start=1)
        ]
        metadata = {
            'dates': {
                MARKET_DATE.isoformat(): {
                    'exact_membership_count': 2,
                    'ranking_membership_sha256': (
                        market_membership_sha256(symbols)
                    ),
                    'daily_price_input_sha256': expected_input_hash,
                    'repair_started_at': rebuilt_at.isoformat(),
                }
            }
        }

        with patch(
            'app.eod.price_bar_repair_state._daily_price_input_fingerprint',
            return_value=(expected_input_hash, 2),
        ):
            result = _exact_factor_stage_evidence(
                session,
                model=DailyMomentumFactorResult,
                required_dates=(MARKET_DATE,),
                factor_rebuild_metadata=metadata,
                raw_dependency_watermarks={
                    earlier_date: earlier_repaired_raw_at,
                    MARKET_DATE: earlier_repaired_raw_at,
                },
            )

        self.assertFalse(result['ready'])
        self.assertEqual(result['dates'][0]['fresh_after_raw'], 2)
        self.assertEqual(result['dates'][0]['fresh_after_dependency'], 0)

    def test_recorder_persists_exact_membership_and_raw_input_hash(self) -> None:
        batch = SimpleNamespace(
            created_at=UTC_NOW - timedelta(hours=1),
            required_rebuild_trade_dates=[MARKET_DATE.isoformat()],
            factor_rebuild_metadata={
                'schema_version': 1,
                'dates': {'2026-09-22': {'preserved': True}},
            },
        )
        unaffected_batch = SimpleNamespace(
            created_at=UTC_NOW - timedelta(hours=1),
            required_rebuild_trade_dates=['2026-09-20'],
            factor_rebuild_metadata=None,
        )
        overlapping_newer_batch = SimpleNamespace(
            created_at=UTC_NOW - timedelta(minutes=5),
            required_rebuild_trade_dates=[MARKET_DATE.isoformat()],
            factor_rebuild_metadata=None,
        )
        session = MagicMock()
        session.scalars.return_value.all.return_value = [
            batch,
            unaffected_batch,
            overlapping_newer_batch,
        ]

        with (
            patch(
                'app.eod.price_bar_repair_state.session_scope',
                return_value=_session_scope_for(session),
            ) as session_scope_mock,
            patch(
                'app.eod.price_bar_repair_state._daily_price_input_fingerprint',
                return_value=('raw-input-sha256', 2),
            ) as fingerprint,
        ):
            result = record_pending_exact_factor_rebuild(
                market_date=MARKET_DATE,
                exact_symbols=('005930', '000660', '005930'),
                provider_membership_sha256='ABCDEF',
                repair_started_at=UTC_NOW - timedelta(minutes=10),
                rebuild_completed_at=UTC_NOW,
                membership_evidence_source='stored_eod_run_metadata',
                membership_evidence_run_id=321,
                membership_evidence_run_source='pykrx',
            )

        expected_symbols = ['000660', '005930']
        expected_membership_hash = market_membership_sha256(expected_symbols)
        self.assertEqual(result['updated_batch_count'], 1)
        session_scope_mock.assert_called_once_with(
            statement_timeout_ms=300_000,
        )
        self.assertEqual(
            result['ranking_membership_sha256'],
            expected_membership_hash,
        )
        fingerprint.assert_called_once_with(
            session,
            market_date=MARKET_DATE,
            symbols=expected_symbols,
        )
        self.assertEqual(batch.factor_rebuild_metadata['schema_version'], 1)
        self.assertEqual(
            batch.factor_rebuild_metadata['dates']['2026-09-22'],
            {'preserved': True},
        )
        evidence = batch.factor_rebuild_metadata['dates'][
            MARKET_DATE.isoformat()
        ]
        self.assertEqual(evidence['exact_membership_count'], 2)
        self.assertEqual(
            evidence['ranking_membership_sha256'],
            expected_membership_hash,
        )
        self.assertEqual(evidence['provider_membership_sha256'], 'abcdef')
        self.assertEqual(
            evidence['membership_evidence_source'],
            'stored_eod_run_metadata',
        )
        self.assertEqual(evidence['membership_evidence_run_id'], 321)
        self.assertEqual(evidence['membership_evidence_run_source'], 'pykrx')
        self.assertEqual(
            evidence['daily_price_input_sha256'],
            'raw-input-sha256',
        )
        self.assertEqual(evidence['rebuild_completed_at'], UTC_NOW.isoformat())
        self.assertIsNone(unaffected_batch.factor_rebuild_metadata)
        self.assertIsNone(overlapping_newer_batch.factor_rebuild_metadata)

    def test_recorder_rejects_live_membership_with_stored_run_lineage(
        self,
    ) -> None:
        with self.assertRaisesRegex(
            ValueError,
            'must not claim stored run lineage',
        ):
            record_pending_exact_factor_rebuild(
                market_date=MARKET_DATE,
                exact_symbols=('005930',),
                provider_membership_sha256='abcdef',
                repair_started_at=UTC_NOW - timedelta(minutes=10),
                rebuild_completed_at=UTC_NOW,
                membership_evidence_source='live_pykrx_market_ohlcv',
                membership_evidence_run_id=321,
                membership_evidence_run_source='pykrx',
            )

    def test_factor_verifier_rejects_raw_input_changed_after_rebuild(self) -> None:
        symbols = ['000660', '005930']
        rebuilt_at = UTC_NOW - timedelta(minutes=20)
        derived_at = UTC_NOW - timedelta(minutes=5)
        raw_at = UTC_NOW - timedelta(minutes=10)
        session = MagicMock()
        session.execute.return_value.all.return_value = [
            (symbol, derived_at, index, raw_at)
            for index, symbol in enumerate(symbols, start=1)
        ]
        metadata = {
            'dates': {
                MARKET_DATE.isoformat(): {
                    'exact_membership_count': 2,
                    'ranking_membership_sha256': (
                        market_membership_sha256(symbols)
                    ),
                    'daily_price_input_sha256': 'recorded-input-sha256',
                    'repair_started_at': rebuilt_at.isoformat(),
                }
            }
        }

        with patch(
            'app.eod.price_bar_repair_state._daily_price_input_fingerprint',
            return_value=('changed-input-sha256', 2),
        ):
            result = _exact_factor_stage_evidence(
                session,
                model=DailyMomentumFactorResult,
                required_dates=(MARKET_DATE,),
                factor_rebuild_metadata=metadata,
                raw_dependency_watermarks={MARKET_DATE: raw_at},
            )

        self.assertFalse(result['ready'])
        date_evidence = result['dates'][0]
        self.assertEqual(date_evidence['fresh_after_raw'], 2)
        self.assertNotEqual(
            date_evidence['observed_daily_price_input_sha256'],
            date_evidence['expected_daily_price_input_sha256'],
        )

    def test_factor_verifier_rejects_derived_rows_older_than_raw_dependency(
        self,
    ) -> None:
        symbols = ['000660', '005930']
        rebuilt_at = UTC_NOW - timedelta(minutes=30)
        derived_at = UTC_NOW - timedelta(minutes=10)
        raw_at = UTC_NOW - timedelta(minutes=5)
        expected_input_hash = 'unchanged-input-sha256'
        session = MagicMock()
        session.execute.return_value.all.return_value = [
            (symbol, derived_at, index, raw_at)
            for index, symbol in enumerate(symbols, start=1)
        ]
        metadata = {
            'dates': {
                MARKET_DATE.isoformat(): {
                    'exact_membership_count': 2,
                    'ranking_membership_sha256': (
                        market_membership_sha256(symbols)
                    ),
                    'daily_price_input_sha256': expected_input_hash,
                    'repair_started_at': rebuilt_at.isoformat(),
                }
            }
        }

        with patch(
            'app.eod.price_bar_repair_state._daily_price_input_fingerprint',
            return_value=(expected_input_hash, 2),
        ):
            result = _exact_factor_stage_evidence(
                session,
                model=DailyMomentumFactorResult,
                required_dates=(MARKET_DATE,),
                factor_rebuild_metadata=metadata,
                raw_dependency_watermarks={MARKET_DATE: raw_at},
            )

        self.assertFalse(result['ready'])
        date_evidence = result['dates'][0]
        self.assertEqual(date_evidence['fresh_after_rebuild'], 2)
        self.assertEqual(date_evidence['fresh_after_raw'], 0)
        self.assertEqual(date_evidence['fresh_after_dependency'], 0)

    def test_signal_context_verifier_rejects_factor_updates_after_rebuild(
        self,
    ) -> None:
        rebuild_started_at = UTC_NOW - timedelta(minutes=20)
        signal_updated_at = UTC_NOW - timedelta(minutes=10)
        dependency_updated_at = UTC_NOW - timedelta(minutes=5)
        batch = SimpleNamespace(
            created_at=UTC_NOW - timedelta(hours=1),
            evaluation_rebuild_metadata={
                'run_started_at': rebuild_started_at.isoformat(),
                'deterministic_signal_contexts': {
                    'dates': {
                        MARKET_DATE.isoformat(): {
                            'signal_feature_count': 1,
                            'signal_feature_sha256': 'signal-sha256',
                            'market_participation_sha256': (
                                'participation-sha256'
                            ),
                        }
                    }
                },
            },
        )

        with (
            patch(
                'app.eod.price_bar_repair_state._signal_feature_fingerprint',
                return_value=(
                    'signal-sha256',
                    1,
                    signal_updated_at,
                    signal_updated_at,
                ),
            ),
            patch(
                'app.eod.price_bar_repair_state.'
                '_market_participation_fingerprint',
                return_value=(
                    'participation-sha256',
                    1,
                    signal_updated_at,
                ),
            ),
            patch(
                'app.eod.price_bar_repair_state.'
                '_signal_feature_dependency_watermark',
                return_value=dependency_updated_at,
            ),
        ):
            result = _signal_context_rebuild_evidence(
                MagicMock(),
                batch=batch,
                required_dates=(MARKET_DATE,),
            )

        self.assertFalse(result['ready'])
        self.assertFalse(result['dates'][0]['signal_features_fresh'])


class PriceBarRepairEvaluationRebuildTests(unittest.TestCase):
    def test_rebuild_refreshes_stored_signal_contexts_and_evaluations_without_agents(
        self,
    ) -> None:
        pending = [
            {
                'repair_batch_id': 'repair-batch-1',
                'affected_symbols': ['005930'],
                'required_rebuild_trade_dates': ['2026-09-19'],
                'required_evaluation_market_date_from': '2026-09-21',
                'required_evaluation_market_date_to': '2026-09-23',
            }
        ]
        batch = SimpleNamespace(
            repair_batch_id='repair-batch-1',
            created_at=UTC_NOW - timedelta(days=1),
            evaluation_rebuilt_at=None,
            evaluation_rebuild_metadata=None,
        )
        read_session = MagicMock()
        read_session.scalars.return_value.all.return_value = []
        signal_evidence_session = MagicMock()
        write_session = MagicMock()
        write_session.scalars.return_value.all.return_value = [batch]
        sessions = iter(
            (read_session, signal_evidence_session, write_session)
        )
        lock_calls: list[dict] = []
        session_scope_calls: list[dict] = []

        @contextmanager
        def fake_session_scope(**kwargs):
            session_scope_calls.append(kwargs)
            yield next(sessions)

        @contextmanager
        def available_locks(**kwargs):
            lock_calls.append(kwargs)
            yield True, None

        post_result = {
            'request': {'window_days': 5},
            'summary': {
                'feedback_rows_considered': 1,
                'evaluations_upserted': 1,
            },
            'items': [{'source': {'feedback_id': 71}}],
        }
        signal_result = {
            'request': {},
            'summary': {
                'status': 'completed',
                'decisions_considered': 1,
                'outcomes_upserted': 4,
                'evaluations_upserted': 4,
                'not_computed_count': 0,
            },
            'items': [{'snapshot_id': 81}],
        }
        calendar_result = {
            'request': {},
            'summary': {'review_rows_upserted': 4},
            'run': {'id': 91, 'status': 'completed'},
        }
        maintenance_result = {
            'status': 'completed',
            'evaluation_refresh': signal_result,
            'calendar_refresh': calendar_result,
        }

        with (
            patch(
                'app.eod.price_bar_repair_state.load_pending_price_bar_rebuild_batches',
                return_value=pending,
            ),
            patch(
                'app.eod.price_bar_repair_state.advisory_eod_collection_locks',
                side_effect=available_locks,
            ),
            patch(
                'app.eod.price_bar_repair_state.session_scope',
                side_effect=fake_session_scope,
            ),
            patch(
                'app.agent_context.decider_evaluation_service.'
                'decider_post_decision_evaluation_service.compute_evaluations',
                return_value=post_result,
            ) as post_compute,
            patch(
                'app.agent_context.signal_decider_evaluation_maintenance_service.'
                'signal_decider_evaluation_maintenance_service.'
                'repair_coverage_range',
                return_value=maintenance_result,
            ) as maintenance,
            patch(
                'app.signals.daily_signal_features.daily_signal_feature_service.'
                'rebuild_stored_owned_fallback_for_market_date',
                return_value={
                    'features_persisted': 1,
                    'preserved_direct_source_rows': 1,
                },
            ) as rebuild_signal_features,
            patch(
                'app.agent_context.derived_contexts.'
                'daily_market_participation_context_service.'
                'rebuild_for_market_date',
                return_value={'captured_symbol_count': 1},
            ) as rebuild_participation,
            patch(
                'app.eod.price_bar_repair_state._signal_feature_fingerprint',
                return_value=(
                    'signal-sha256',
                    1,
                    UTC_NOW,
                    UTC_NOW,
                ),
            ),
            patch(
                'app.eod.price_bar_repair_state.'
                '_market_participation_fingerprint',
                return_value=('participation-sha256', 1, UTC_NOW),
            ),
            patch(
                'app.agent_context.orchestrator.agent_feedback_orchestrator.'
                'dispatch_for_symbols'
            ) as agent_dispatch,
        ):
            result = rebuild_pending_price_dependent_evaluations()

        self.assertEqual(result['status'], 'completed')
        self.assertEqual(result['updated_batch_count'], 1)
        self.assertEqual(
            session_scope_calls,
            [{'statement_timeout_ms': 300_000}] * 3,
        )
        self.assertEqual(
            lock_calls,
            [
                {
                    'market': 'KRX',
                    'trade_dates': [
                        date(2026, 9, 19),
                        date(2026, 9, 21),
                        date(2026, 9, 22),
                        date(2026, 9, 23),
                    ],
                    'source': 'daily_price_bars',
                }
            ],
        )
        post_compute.assert_called_once_with(
            market_date_from=date(2026, 9, 21),
            market_date_to=date(2026, 9, 23),
            symbols=['005930'],
            window_days=5,
            force_recompute=True,
        )
        maintenance.assert_called_once_with(
            market_date_from=date(2026, 9, 21),
            market_date_to=date(2026, 9, 23),
            symbols=['005930'],
            horizons=(1, 3, 5, 10),
            primary_horizons=(1, 3, 5, 10),
            evaluation_version='signal_decider_eval_v1',
            force_recompute=True,
            refresh_calendar=True,
            fail_on_mature_gaps=True,
            trigger='price_bar_repair',
        )
        agent_dispatch.assert_not_called()
        rebuild_signal_features.assert_called_once_with(
            market_date=date(2026, 9, 19)
        )
        rebuild_participation.assert_called_once_with(
            market_date=date(2026, 9, 19),
            market='KRX',
            source='kis_ranking',
        )
        self.assertIsNotNone(batch.evaluation_rebuilt_at)
        self.assertEqual(
            batch.evaluation_rebuild_metadata['calendar_review_run_id'],
            91,
        )
        self.assertEqual(
            batch.evaluation_rebuild_metadata[
                'expected_calendar_review_count'
            ],
            4,
        )
        self.assertEqual(
            batch.evaluation_rebuild_metadata[
                'deterministic_signal_contexts'
            ]['dates']['2026-09-19']['signal_feature_sha256'],
            'signal-sha256',
        )

    def test_rebuild_stops_before_any_work_when_one_date_lock_is_denied(
        self,
    ) -> None:
        pending = [
            {
                'repair_batch_id': 'repair-batch-1',
                'affected_symbols': ['005930'],
                'required_rebuild_trade_dates': [
                    '2026-09-23',
                    '2026-09-21',
                    '2026-09-23',
                ],
                'required_evaluation_market_date_from': '2026-09-21',
                'required_evaluation_market_date_to': '2026-09-23',
            }
        ]
        lock_calls: list[dict] = []

        @contextmanager
        def denied_locks(**kwargs):
            lock_calls.append(kwargs)
            yield False, '2026-09-22'

        with (
            patch(
                'app.eod.price_bar_repair_state.load_pending_price_bar_rebuild_batches',
                return_value=pending,
            ),
            patch(
                'app.eod.price_bar_repair_state.advisory_eod_collection_locks',
                side_effect=denied_locks,
            ),
            patch(
                'app.eod.price_bar_repair_state.'
                '_rebuild_pending_price_dependent_evaluations_locked'
            ) as rebuild_locked,
        ):
            result = rebuild_pending_price_dependent_evaluations()

        self.assertEqual(
            lock_calls[0]['trade_dates'],
            [
                date(2026, 9, 21),
                date(2026, 9, 22),
                date(2026, 9, 23),
            ],
        )
        self.assertEqual(result['status'], 'blocked')
        self.assertEqual(result['reason'], 'daily_price_bar_writer_active')
        self.assertEqual(result['blocked_trade_date'], '2026-09-22')
        rebuild_locked.assert_not_called()

    def test_verifier_stops_before_reading_batches_when_date_lock_is_denied(
        self,
    ) -> None:
        pending = [
            {
                'repair_batch_id': 'repair-batch-1',
                'required_rebuild_trade_dates': [
                    '2026-09-23',
                    '2026-09-21',
                    '2026-09-23',
                ],
            }
        ]
        lock_calls: list[dict] = []

        @contextmanager
        def denied_locks(**kwargs):
            lock_calls.append(kwargs)
            yield False, '2026-09-23'

        with (
            patch(
                'app.eod.price_bar_repair_state.load_pending_price_bar_rebuild_batches',
                return_value=pending,
            ),
            patch(
                'app.eod.price_bar_repair_state.advisory_eod_collection_locks',
                side_effect=denied_locks,
            ),
            patch(
                'app.eod.price_bar_repair_state.'
                '_verify_pending_price_bar_rebuilds_locked'
            ) as verify_locked,
        ):
            result = verify_pending_price_bar_rebuilds(apply=True)

        self.assertEqual(
            lock_calls,
            [
                {
                    'market': 'KRX',
                    'trade_dates': [date(2026, 9, 21), date(2026, 9, 23)],
                    'source': 'daily_price_bars',
                }
            ],
        )
        self.assertEqual(result['status'], 'blocked')
        self.assertFalse(result['applied'])
        self.assertEqual(result['pending_batch_count'], 1)
        verify_locked.assert_not_called()

    def test_locked_verifier_uses_configured_repair_timeout(self) -> None:
        session = MagicMock()
        session.scalars.return_value.all.return_value = []
        session_scope_calls: list[dict] = []

        @contextmanager
        def fake_session_scope(**kwargs):
            session_scope_calls.append(kwargs)
            yield session

        with (
            patch(
                'app.eod.price_bar_repair_state.session_scope',
                side_effect=fake_session_scope,
            ),
            patch(
                'app.eod.price_bar_repair_state.settings.'
                'price_bar_repair_db_statement_timeout_ms',
                420_000,
            ),
        ):
            result = _verify_pending_price_bar_rebuilds_locked(apply=False)

        self.assertEqual(result['status'], 'ready')
        self.assertEqual(
            session_scope_calls,
            [{'statement_timeout_ms': 420_000}],
        )

    def test_locked_verifier_shares_ready_global_evidence_at_newest_threshold(
        self,
    ) -> None:
        older_created_at = UTC_NOW - timedelta(hours=2)
        newer_created_at = UTC_NOW - timedelta(hours=1)
        factor_metadata = {
            'schema_version': 1,
            'dates': {MARKET_DATE.isoformat(): {'shared': True}},
        }
        batches = [
            SimpleNamespace(
                repair_batch_id='repair-batch-older',
                created_at=older_created_at,
                repaired_row_count=1,
                required_rebuild_trade_dates=[MARKET_DATE.isoformat()],
                factor_rebuild_metadata=factor_metadata,
                affected_symbols=['005930'],
                required_evaluation_market_date_from=MARKET_DATE,
                required_evaluation_market_date_to=MARKET_DATE,
                evaluation_rebuild_metadata={'batch': 'older'},
            ),
            SimpleNamespace(
                repair_batch_id='repair-batch-newer',
                created_at=newer_created_at,
                repaired_row_count=2,
                required_rebuild_trade_dates=[MARKET_DATE.isoformat()],
                factor_rebuild_metadata=factor_metadata,
                affected_symbols=['000660'],
                required_evaluation_market_date_from=MARKET_DATE,
                required_evaluation_market_date_to=MARKET_DATE,
                evaluation_rebuild_metadata={'batch': 'newer'},
            ),
        ]
        session = MagicMock()
        session.scalars.return_value.all.return_value = batches
        session.scalar.side_effect = [1, 2]
        shared_stage_evidence = {'ready': True, 'issues': []}

        with (
            patch(
                'app.eod.price_bar_repair_state.session_scope',
                return_value=_session_scope_for(session),
            ),
            patch(
                'app.eod.price_bar_repair_state._expected_price_counts',
                return_value={MARKET_DATE: 2},
            ) as expected_counts,
            patch(
                'app.eod.price_bar_repair_state.'
                '_causal_raw_dependency_watermarks',
                return_value={MARKET_DATE: UTC_NOW - timedelta(minutes=5)},
            ) as dependency_watermarks,
            patch(
                'app.eod.price_bar_repair_state.'
                '_full_coverage_stage_evidence',
                return_value=shared_stage_evidence,
            ) as full_coverage,
            patch(
                'app.eod.price_bar_repair_state._exact_factor_stage_evidence',
                return_value=shared_stage_evidence,
            ) as exact_factor,
            patch(
                'app.eod.price_bar_repair_state._trending_evidence',
                return_value=shared_stage_evidence,
            ) as trending,
            patch(
                'app.eod.price_bar_repair_state.'
                '_signal_context_rebuild_evidence',
                side_effect=[
                    {'ready': True, 'batch': 'older'},
                    {'ready': True, 'batch': 'newer'},
                ],
            ) as signal_context,
            patch(
                'app.eod.price_bar_repair_state._evaluation_rebuild_evidence',
                side_effect=[
                    {'ready': True, 'batch': 'older'},
                    {'ready': True, 'batch': 'newer'},
                ],
            ) as evaluation,
        ):
            result = _verify_pending_price_bar_rebuilds_locked(apply=True)

        self.assertEqual(result['status'], 'completed')
        self.assertTrue(result['applied'])
        self.assertEqual(result['completed_batch_count'], 2)
        self.assertEqual(expected_counts.call_count, 1)
        self.assertEqual(dependency_watermarks.call_count, 1)
        self.assertEqual(full_coverage.call_count, 3)
        self.assertEqual(exact_factor.call_count, 9)
        self.assertEqual(trending.call_count, 1)
        self.assertEqual(signal_context.call_count, 2)
        self.assertEqual(evaluation.call_count, 2)
        self.assertTrue(
            all(
                call.kwargs['rebuilt_after'] == newer_created_at
                for call in full_coverage.call_args_list
            )
        )
        self.assertEqual(
            trending.call_args.kwargs['rebuilt_after'],
            newer_created_at,
        )
        self.assertEqual(
            [item['repair_audit_count'] for item in result['batches']],
            [1, 2],
        )
        self.assertEqual(
            result['batches'][0]['stages'][
                'signal_features_and_market_participation'
            ]['batch'],
            'older',
        )
        self.assertEqual(
            result['batches'][1]['stages'][
                'price_dependent_decision_evaluations'
            ]['batch'],
            'newer',
        )
        self.assertEqual(
            set(result['batches'][0]),
            {
                'repair_batch_id',
                'ready',
                'repaired_row_count',
                'repair_audit_count',
                'required_rebuild_trade_dates',
                'required_price_row_counts',
                'raw_dependency_watermark',
                'raw_dependency_watermarks',
                'missing_price_dates',
                'stages',
                'issues',
                'verified_at',
            },
        )
        self.assertEqual(batches[0].status, 'completed')
        self.assertIs(
            batches[0].verification_metadata,
            result['batches'][0],
        )
        self.assertIsNot(
            batches[0].verification_metadata['stages']['price_contexts'],
            batches[1].verification_metadata['stages']['price_contexts'],
        )

    def test_locked_verifier_rechecks_older_threshold_when_newest_is_stale(
        self,
    ) -> None:
        older_created_at = UTC_NOW - timedelta(hours=2)
        newer_created_at = UTC_NOW - timedelta(hours=1)
        factor_metadata = {'dates': {'2026-09-23': {'shared': True}}}
        batches = [
            SimpleNamespace(
                repair_batch_id='repair-batch-older',
                created_at=older_created_at,
                repaired_row_count=1,
                required_rebuild_trade_dates=[MARKET_DATE.isoformat()],
                factor_rebuild_metadata=factor_metadata,
                affected_symbols=['005930'],
                required_evaluation_market_date_from=MARKET_DATE,
                required_evaluation_market_date_to=MARKET_DATE,
                evaluation_rebuild_metadata={'batch': 'older'},
            ),
            SimpleNamespace(
                repair_batch_id='repair-batch-newer',
                created_at=newer_created_at,
                repaired_row_count=1,
                required_rebuild_trade_dates=[MARKET_DATE.isoformat()],
                factor_rebuild_metadata=factor_metadata,
                affected_symbols=['000660'],
                required_evaluation_market_date_from=MARKET_DATE,
                required_evaluation_market_date_to=MARKET_DATE,
                evaluation_rebuild_metadata={'batch': 'newer'},
            ),
        ]
        session = MagicMock()
        session.scalars.return_value.all.return_value = batches
        session.scalar.side_effect = [1, 1]

        def freshness_evidence(*_args, rebuilt_after, **_kwargs):
            return {
                'ready': rebuilt_after == older_created_at,
                'rebuilt_after': rebuilt_after.isoformat(),
            }

        with (
            patch(
                'app.eod.price_bar_repair_state.session_scope',
                return_value=_session_scope_for(session),
            ),
            patch(
                'app.eod.price_bar_repair_state._expected_price_counts',
                return_value={MARKET_DATE: 2},
            ),
            patch(
                'app.eod.price_bar_repair_state.'
                '_causal_raw_dependency_watermarks',
                return_value={MARKET_DATE: UTC_NOW - timedelta(minutes=5)},
            ),
            patch(
                'app.eod.price_bar_repair_state.'
                '_full_coverage_stage_evidence',
                side_effect=freshness_evidence,
            ) as full_coverage,
            patch(
                'app.eod.price_bar_repair_state._exact_factor_stage_evidence',
                return_value={'ready': True},
            ) as exact_factor,
            patch(
                'app.eod.price_bar_repair_state._trending_evidence',
                side_effect=freshness_evidence,
            ) as trending,
            patch(
                'app.eod.price_bar_repair_state.'
                '_signal_context_rebuild_evidence',
                return_value={'ready': True},
            ),
            patch(
                'app.eod.price_bar_repair_state._evaluation_rebuild_evidence',
                return_value={'ready': True},
            ),
        ):
            result = _verify_pending_price_bar_rebuilds_locked(apply=False)

        older, newer = result['batches']
        self.assertTrue(older['ready'])
        self.assertEqual(older['issues'], [])
        self.assertFalse(newer['ready'])
        self.assertIn('price_contexts_incomplete_or_stale', newer['issues'])
        self.assertIn('trending_ranks_incomplete_or_stale', newer['issues'])
        self.assertEqual(full_coverage.call_count, 6)
        self.assertEqual(trending.call_count, 2)
        self.assertEqual(exact_factor.call_count, 9)

    def test_locked_verifier_does_not_share_distinct_factor_metadata(
        self,
    ) -> None:
        batches = [
            SimpleNamespace(
                repair_batch_id=f'repair-batch-{marker}',
                created_at=UTC_NOW - timedelta(hours=offset),
                repaired_row_count=1,
                required_rebuild_trade_dates=[MARKET_DATE.isoformat()],
                factor_rebuild_metadata={'marker': marker},
                affected_symbols=[symbol],
                required_evaluation_market_date_from=MARKET_DATE,
                required_evaluation_market_date_to=MARKET_DATE,
                evaluation_rebuild_metadata={'batch': marker},
            )
            for marker, symbol, offset in (
                ('older', '005930', 2),
                ('newer', '000660', 1),
            )
        ]
        session = MagicMock()
        session.scalars.return_value.all.return_value = batches
        session.scalar.side_effect = [1, 1]

        def factor_evidence(
            *_args,
            factor_rebuild_metadata,
            **_kwargs,
        ):
            return {
                'ready': True,
                'marker': factor_rebuild_metadata['marker'],
            }

        with (
            patch(
                'app.eod.price_bar_repair_state.session_scope',
                return_value=_session_scope_for(session),
            ),
            patch(
                'app.eod.price_bar_repair_state._expected_price_counts',
                return_value={MARKET_DATE: 2},
            ),
            patch(
                'app.eod.price_bar_repair_state.'
                '_causal_raw_dependency_watermarks',
                return_value={MARKET_DATE: UTC_NOW - timedelta(minutes=5)},
            ),
            patch(
                'app.eod.price_bar_repair_state.'
                '_full_coverage_stage_evidence',
                return_value={'ready': True},
            ),
            patch(
                'app.eod.price_bar_repair_state._exact_factor_stage_evidence',
                side_effect=factor_evidence,
            ) as exact_factor,
            patch(
                'app.eod.price_bar_repair_state._trending_evidence',
                return_value={'ready': True},
            ),
            patch(
                'app.eod.price_bar_repair_state.'
                '_signal_context_rebuild_evidence',
                return_value={'ready': True},
            ),
            patch(
                'app.eod.price_bar_repair_state._evaluation_rebuild_evidence',
                return_value={'ready': True},
            ),
        ):
            result = _verify_pending_price_bar_rebuilds_locked(apply=False)

        self.assertEqual(exact_factor.call_count, 18)
        self.assertEqual(
            result['batches'][0]['stages']['momentum_factors']['marker'],
            'older',
        )
        self.assertEqual(
            result['batches'][1]['stages']['momentum_factors']['marker'],
            'newer',
        )


if __name__ == '__main__':
    unittest.main()
