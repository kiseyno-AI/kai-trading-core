from __future__ import annotations

from contextlib import contextmanager
from datetime import date, datetime, timedelta, timezone
from types import SimpleNamespace
import unittest
from unittest.mock import MagicMock, patch

from app.db_models import DailyMomentumFactorResult
from app.eod.market_membership import market_membership_sha256
from app.eod.price_bar_repair_state import (
    _exact_factor_stage_evidence,
    _signal_context_rebuild_evidence,
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
            ),
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
                raw_dependency_watermark=raw_at,
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
                raw_dependency_watermark=raw_at,
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

        @contextmanager
        def fake_session_scope():
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


if __name__ == '__main__':
    unittest.main()
