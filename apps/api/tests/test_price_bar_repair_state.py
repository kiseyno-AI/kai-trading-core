from __future__ import annotations

from contextlib import contextmanager
from copy import deepcopy
from datetime import date, datetime, timedelta, timezone
from types import SimpleNamespace
import unittest
from unittest.mock import ANY, MagicMock, patch

from app.db_models import DailyMomentumFactorResult
from app.eod.market_membership import (
    build_market_membership_snapshot,
    market_membership_sha256,
)
from app.eod.price_bar_repair_state import (
    _causal_raw_dependency_watermarks,
    _exact_factor_stage_evidence,
    _expected_price_counts,
    _full_coverage_stage_evidence,
    _missing_bar_insert_repair_evidence,
    _pending_frozen_signal_feature_scopes,
    _signal_context_rebuild_evidence,
    _stable_json_sha256,
    _trending_evidence,
    _verify_pending_price_bar_rebuilds_locked,
    rebuild_pending_price_dependent_evaluations,
    record_pending_exact_factor_rebuild,
    record_pending_trending_rebuild,
    resolve_pending_exact_market_membership_evidence,
    verify_pending_price_bar_rebuilds,
)


MARKET_DATE = date(2026, 9, 23)
UTC_NOW = datetime(2026, 9, 24, 1, 0, tzinfo=timezone.utc)
FACTOR_STAGE_ID = 'momentum_factors'
FACTOR_OUTPUT_SHA256 = 'unchanged-output-sha256'


@contextmanager
def _session_scope_for(session):
    yield session


def _factor_rebuild_metadata(
    *,
    market_date: date,
    symbols: list[str],
    rebuilt_at: datetime,
    input_sha256: str,
    output_sha256: str = FACTOR_OUTPUT_SHA256,
) -> dict:
    return {
        'schema_version': 3,
        'dates': {
            market_date.isoformat(): {
                'exact_membership_count': len(symbols),
                'ranking_membership_sha256': market_membership_sha256(
                    symbols
                ),
                'provider_membership_sha256': (
                    market_membership_sha256(symbols)
                ),
                'membership_evidence_source': 'stored_eod_run_metadata',
                'membership_evidence_run_id': 321,
                'membership_evidence_run_source': 'pykrx',
                'membership_evidence_sha256': None,
                'membership_evidence_repair_batch_ids': [],
                'daily_price_input_sha256': input_sha256,
                'stage_output_fingerprints': {
                    FACTOR_STAGE_ID: {
                        'row_count': len(symbols),
                        'output_sha256': output_sha256,
                    }
                },
                'repair_started_at': rebuilt_at.isoformat(),
            }
        },
    }


def _trending_rebuild_metadata() -> dict:
    return {
        'schema_version': 2,
        'dates': {
            MARKET_DATE.isoformat(): {
                'trending_output_fingerprints': {
                    period: {
                        'row_count': 2,
                        'output_sha256': f'{period}-output-sha256',
                    }
                    for period in ('1d', '5d', '20d')
                }
            }
        },
    }


def _missing_bar_batch_with_stored_lineage() -> SimpleNamespace:
    target_date = MARKET_DATE
    downstream_date = MARKET_DATE + timedelta(days=1)
    target_symbols = ['005930']
    downstream_symbols = ['000660']
    target_input_sha256 = '1' * 64
    downstream_input_sha256 = '2' * 64
    provider_manifest = build_market_membership_snapshot(
        symbols=(
            SimpleNamespace(symbol='005930', market='KOSPI'),
        ),
        markets=('KOSPI', 'KOSDAQ', 'KONEX'),
    )
    provider_sha256 = market_membership_sha256(target_symbols)
    provider_manifest['membership_sha256'] = provider_sha256
    empty_fallback_bundle_sha256 = _stable_json_sha256(
        {
            'schema_version': 'missing_bar_downstream_membership_v1',
            'manifests': {},
        }
    )
    repair_evidence = {
        'repair_kind': 'missing_bar_insert',
        'schema_version': 'missing_bar_insert_v3',
        'provider': 'pykrx',
        'market_date': target_date.isoformat(),
        'inserted_symbols': target_symbols,
        'inserted_symbol_count': 1,
        'inserted_price_input_sha256': target_input_sha256,
        'expected_provider_membership_count': 1,
        'expected_provider_membership_sha256': provider_sha256,
        'expected_provider_payload_sha256': '3' * 64,
        'provider_price_input_sha256': target_input_sha256,
        'provider_membership_manifest': provider_manifest,
        'price_dependent_membership_symbols': target_symbols,
        'price_dependent_membership_count': 1,
        'price_dependent_membership_sha256': provider_sha256,
        'reviewed_missing_symbol_count': 1,
        'reviewed_missing_membership_sha256': provider_sha256,
        'downstream_fallback_manifest_count': 0,
        'downstream_fallback_bundle_sha256': (
            empty_fallback_bundle_sha256
        ),
        'price_dependent_membership_by_date': {
            target_date.isoformat(): {
                'basis': 'reviewed_live_provider_membership',
                'symbols': target_symbols,
                'symbol_count': 1,
                'membership_sha256': provider_sha256,
                'daily_price_input_sha256': target_input_sha256,
            },
            downstream_date.isoformat(): {
                'basis': 'validated_eod_run_market_membership',
                'eod_run_id': 321,
                'eod_run_source': 'pykrx',
                'symbols': downstream_symbols,
                'symbol_count': 1,
                'membership_sha256': market_membership_sha256(
                    downstream_symbols
                ),
                'daily_price_input_sha256': downstream_input_sha256,
            },
        },
    }
    return SimpleNamespace(
        verification_metadata={'repair_evidence': repair_evidence},
        affected_symbols=target_symbols,
        affected_trade_dates=[target_date.isoformat()],
        required_rebuild_trade_dates=[
            target_date.isoformat(),
            downstream_date.isoformat(),
        ],
        repaired_row_count=1,
    )


def _stored_lineage_fingerprint(
    _session,
    *,
    market_date: date,
    symbols,
) -> tuple[str, int]:
    if market_date == MARKET_DATE:
        return '1' * 64, len(symbols)
    if market_date == MARKET_DATE + timedelta(days=1):
        return '2' * 64, len(symbols)
    raise AssertionError(f'unexpected fingerprint date: {market_date}')


class PriceBarRepairStoredMembershipLineageTests(unittest.TestCase):
    def test_deleted_stored_run_fails_closed(self) -> None:
        session = MagicMock()
        session.get.return_value = None

        with patch(
            'app.eod.price_bar_repair_state.'
            'exact_daily_price_bar_scope_fingerprint',
            side_effect=_stored_lineage_fingerprint,
        ):
            result = _missing_bar_insert_repair_evidence(
                session,
                batch=_missing_bar_batch_with_stored_lineage(),
            )

        self.assertFalse(result['ready'])
        self.assertIn(
            'missing_bar_insert_stored_membership_run_missing',
            result['issues'],
        )
        session.get.assert_called_once_with(ANY, 321)

    def test_wrong_stored_run_identity_or_snapshot_fails_closed(self) -> None:
        downstream_date = MARKET_DATE + timedelta(days=1)
        base = {
            'market': 'KRX',
            'trade_date': downstream_date,
            'source': 'pykrx',
            'status': 'completed',
            'run_metadata': {},
        }
        invalid_runs = {
            'wrong_date': {**base, 'trade_date': MARKET_DATE},
            'wrong_status': {**base, 'status': 'running'},
            'wrong_source': {**base, 'source': 'kis'},
            'missing_snapshot': base,
        }

        for label, values in invalid_runs.items():
            with self.subTest(label=label):
                session = MagicMock()
                session.get.return_value = SimpleNamespace(**values)
                with patch(
                    'app.eod.price_bar_repair_state.'
                    'exact_daily_price_bar_scope_fingerprint',
                    side_effect=_stored_lineage_fingerprint,
                ):
                    result = _missing_bar_insert_repair_evidence(
                        session,
                        batch=_missing_bar_batch_with_stored_lineage(),
                    )

                self.assertFalse(result['ready'])
                self.assertIn(
                    'missing_bar_insert_stored_membership_lineage_invalid',
                    result['issues'],
                )

    def test_stored_run_symbol_mismatch_fails_closed(self) -> None:
        session = MagicMock()
        session.get.return_value = SimpleNamespace()

        with (
            patch(
                'app.eod.price_bar_repair_state.'
                'exact_daily_price_bar_scope_fingerprint',
                side_effect=_stored_lineage_fingerprint,
            ),
            patch(
                'app.eod.missing_bar_repair.'
                '_strict_stored_price_dependent_membership',
                return_value=['999999'],
            ) as strict_validator,
        ):
            result = _missing_bar_insert_repair_evidence(
                session,
                batch=_missing_bar_batch_with_stored_lineage(),
            )

        self.assertFalse(result['ready'])
        self.assertIn(
            'missing_bar_insert_stored_membership_scope_mismatch',
            result['issues'],
        )
        strict_validator.assert_called_once_with(
            session.get.return_value,
            market_date=MARKET_DATE + timedelta(days=1),
        )

    def test_exact_stored_run_lineage_remains_ready(self) -> None:
        session = MagicMock()
        session.get.return_value = SimpleNamespace()

        with (
            patch(
                'app.eod.price_bar_repair_state.'
                'exact_daily_price_bar_scope_fingerprint',
                side_effect=_stored_lineage_fingerprint,
            ),
            patch(
                'app.eod.missing_bar_repair.'
                '_strict_stored_price_dependent_membership',
                return_value=['000660'],
            ),
        ):
            result = _missing_bar_insert_repair_evidence(
                session,
                batch=_missing_bar_batch_with_stored_lineage(),
            )

        self.assertTrue(result['ready'], result['issues'])


class PriceBarRepairFactorEvidenceTests(unittest.TestCase):
    def test_conflicting_frozen_signal_scopes_fail_closed(self) -> None:
        def pending_batch(repair_batch_id, symbols):
            return {
                'repair_batch_id': repair_batch_id,
                'required_rebuild_trade_dates': [MARKET_DATE.isoformat()],
                'verification_metadata': {
                    'repair_evidence': {
                        'repair_kind': 'missing_bar_insert',
                        'schema_version': 'missing_bar_insert_v3',
                        'price_dependent_membership_by_date': {
                            MARKET_DATE.isoformat(): {
                                'symbols': list(symbols),
                                'symbol_count': len(symbols),
                                'membership_sha256': (
                                    market_membership_sha256(symbols)
                                ),
                            }
                        },
                    }
                },
            }

        with self.assertRaisesRegex(
            RuntimeError,
            'conflicting frozen signal membership scopes',
        ):
            _pending_frozen_signal_feature_scopes(
                pending=(
                    pending_batch('first', ('000660',)),
                    pending_batch('second', ('000660', '005930')),
                ),
                required_dates=(MARKET_DATE,),
            )

    def test_frozen_membership_queries_stay_below_bind_limit_for_long_suffix(
        self,
    ) -> None:
        required_dates = tuple(
            MARKET_DATE + timedelta(days=offset) for offset in range(30)
        )
        symbols = tuple(f'{value:06d}' for value in range(2_700))
        frozen_symbols_by_date = {
            market_date: symbols for market_date in required_dates
        }
        raw_dependency_watermarks = {
            market_date: UTC_NOW for market_date in required_dates
        }
        query_result = MagicMock()
        query_result.all.return_value = []
        session = MagicMock()
        session.execute.return_value = query_result

        _expected_price_counts(
            session,
            required_dates=required_dates,
            frozen_symbols_by_date=frozen_symbols_by_date,
        )
        _causal_raw_dependency_watermarks(
            session,
            required_dates=required_dates,
            frozen_symbols_by_date=frozen_symbols_by_date,
        )
        _full_coverage_stage_evidence(
            session,
            model=DailyMomentumFactorResult,
            required_dates=required_dates,
            expected_counts={},
            rebuilt_after=UTC_NOW,
            raw_dependency_watermarks=raw_dependency_watermarks,
            frozen_symbols_by_date=frozen_symbols_by_date,
        )
        _trending_evidence(
            session,
            required_dates=required_dates,
            rebuilt_after=UTC_NOW,
            raw_dependency_watermarks=raw_dependency_watermarks,
            frozen_symbols_by_date=frozen_symbols_by_date,
        )

        self.assertEqual(session.execute.call_count, 30 * 4)
        for execute_call in session.execute.call_args_list:
            parameters = execute_call.args[0].compile().params
            expanded_parameter_count = sum(
                len(value)
                if isinstance(value, (list, tuple, set))
                else 1
                for value in parameters.values()
            )
            self.assertLess(expanded_parameter_count, 65_535)
            self.assertLessEqual(expanded_parameter_count, len(symbols) + 20)

    def test_factor_verifier_uses_frozen_historical_membership_not_market(self):
        symbol = '005930'
        rebuilt_at = UTC_NOW - timedelta(minutes=30)
        raw_at = UTC_NOW - timedelta(minutes=20)
        derived_at = UTC_NOW - timedelta(minutes=10)
        expected_input_hash = 'frozen-input-sha256'
        session = MagicMock()
        session.execute.return_value.all.return_value = [
            (symbol, derived_at, 1, raw_at)
        ]
        metadata = _factor_rebuild_metadata(
            market_date=MARKET_DATE,
            symbols=[symbol],
            rebuilt_at=rebuilt_at,
            input_sha256=expected_input_hash,
        )

        with (
            patch(
                'app.eod.price_bar_repair_state.'
                '_daily_price_input_fingerprint',
                return_value=(expected_input_hash, 1),
            ),
            patch(
                'app.eod.price_bar_repair_state.'
                '_derived_output_fingerprint',
                return_value=(FACTOR_OUTPUT_SHA256, 1),
            ),
        ):
            result = _exact_factor_stage_evidence(
                session,
                stage_id=FACTOR_STAGE_ID,
                model=DailyMomentumFactorResult,
                required_dates=(MARKET_DATE,),
                factor_rebuild_metadata=metadata,
                raw_dependency_watermarks={MARKET_DATE: raw_at},
                frozen_symbols_by_date={MARKET_DATE: (symbol,)},
            )

        self.assertTrue(result['ready'])
        rendered_statement = str(session.execute.call_args.args[0])
        self.assertIn('symbols.symbol IN', rendered_statement)
        self.assertNotIn('symbols.market IN', rendered_statement)

    def test_factor_verifier_requires_current_membership_lineage(self):
        symbol = '005930'
        rebuilt_at = UTC_NOW - timedelta(minutes=30)
        raw_at = UTC_NOW - timedelta(minutes=20)
        derived_at = UTC_NOW - timedelta(minutes=10)
        expected_input_hash = 'frozen-input-sha256'
        session = MagicMock()
        session.execute.return_value.all.return_value = [
            (symbol, derived_at, 1, raw_at)
        ]
        metadata = _factor_rebuild_metadata(
            market_date=MARKET_DATE,
            symbols=[symbol],
            rebuilt_at=rebuilt_at,
            input_sha256=expected_input_hash,
        )
        scope = metadata['dates'][MARKET_DATE.isoformat()]
        scope.update(
            {
                'membership_evidence_source': (
                    'pending_missing_bar_repair_metadata'
                ),
                'membership_evidence_run_id': None,
                'membership_evidence_run_source': 'pykrx',
                'membership_evidence_sha256': 'b' * 64,
                'membership_evidence_repair_batch_ids': [
                    'missing-bar-authority'
                ],
            }
        )
        current_pending_authority = {
            'membership_evidence_source': (
                'pending_missing_bar_repair_metadata'
            ),
            'membership_evidence_run_id': None,
            'membership_evidence_run_source': 'pykrx',
            'membership_evidence_sha256': 'b' * 64,
            'membership_evidence_repair_batch_ids': [
                'missing-bar-authority'
            ],
            'ranking_membership_count': 1,
            'ranking_membership_sha256': scope[
                'ranking_membership_sha256'
            ],
            'provider_membership_sha256': scope[
                'provider_membership_sha256'
            ],
            'daily_price_input_sha256': expected_input_hash,
        }

        def verify(
            candidate: dict,
            *,
            current_authority: dict | None = None,
        ) -> dict:
            with (
                patch(
                    'app.eod.price_bar_repair_state.'
                    '_daily_price_input_fingerprint',
                    return_value=(expected_input_hash, 1),
                ),
                patch(
                    'app.eod.price_bar_repair_state.'
                    '_derived_output_fingerprint',
                    return_value=(FACTOR_OUTPUT_SHA256, 1),
                ),
            ):
                return _exact_factor_stage_evidence(
                    session,
                    stage_id=FACTOR_STAGE_ID,
                    model=DailyMomentumFactorResult,
                    required_dates=(MARKET_DATE,),
                    factor_rebuild_metadata=candidate,
                    raw_dependency_watermarks={MARKET_DATE: raw_at},
                    frozen_symbols_by_date={MARKET_DATE: (symbol,)},
                    pending_membership_authority_by_date={
                        MARKET_DATE: (
                            current_authority
                            if current_authority is not None
                            else current_pending_authority
                        )
                    },
                )

        self.assertTrue(verify(metadata)['ready'])
        corruptions = (
            ('schema_version', 2),
            ('provider_membership_sha256', 'invalid'),
            ('membership_evidence_run_id', True),
            ('membership_evidence_sha256', None),
            ('membership_evidence_repair_batch_ids', []),
        )
        for field, value in corruptions:
            with self.subTest(field=field):
                corrupted = deepcopy(metadata)
                if field == 'schema_version':
                    corrupted[field] = value
                else:
                    corrupted['dates'][MARKET_DATE.isoformat()][field] = value
                result = verify(corrupted)
                self.assertFalse(result['ready'])
                self.assertEqual(
                    result['issues'][0]['reason'],
                    'factor_rebuild_scope_missing_or_invalid',
                )

        authority_corruptions = (
            ('membership_evidence_sha256', 'c' * 64),
            ('provider_membership_sha256', 'd' * 64),
            ('membership_evidence_repair_batch_ids', ['other-batch']),
        )
        for field, value in authority_corruptions:
            with self.subTest(current_authority_field=field):
                corrupted_authority = deepcopy(current_pending_authority)
                corrupted_authority[field] = value
                result = verify(
                    metadata,
                    current_authority=corrupted_authority,
                )
                self.assertFalse(result['ready'])
                self.assertFalse(
                    result['dates'][0][
                        'current_pending_authority_matches'
                    ]
                )

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

    def test_downstream_watermark_uses_frozen_other_membership_not_market(
        self,
    ) -> None:
        downstream_date = MARKET_DATE + timedelta(days=1)
        historical_other_symbol = '123456'
        raw_at = UTC_NOW
        session = MagicMock()
        session.execute.return_value.all.return_value = [
            (downstream_date, raw_at)
        ]

        result = _causal_raw_dependency_watermarks(
            session,
            required_dates=(downstream_date,),
            frozen_symbols_by_date={
                downstream_date: (historical_other_symbol,)
            },
        )

        self.assertEqual(result, {downstream_date: raw_at})
        rendered_statement = str(session.execute.call_args.args[0])
        self.assertIn('symbols.symbol IN', rendered_statement)
        self.assertNotIn('symbols.market IN', rendered_statement)

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
        metadata = _factor_rebuild_metadata(
            market_date=earlier_date,
            symbols=symbols,
            rebuilt_at=rebuilt_at,
            input_sha256=expected_input_hash,
        )

        with (
            patch(
                'app.eod.price_bar_repair_state.'
                '_daily_price_input_fingerprint',
                return_value=(expected_input_hash, 2),
            ),
            patch(
                'app.eod.price_bar_repair_state.'
                '_derived_output_fingerprint',
                return_value=(FACTOR_OUTPUT_SHA256, 2),
            ),
        ):
            result = _exact_factor_stage_evidence(
                session,
                stage_id=FACTOR_STAGE_ID,
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
        metadata = _factor_rebuild_metadata(
            market_date=MARKET_DATE,
            symbols=symbols,
            rebuilt_at=rebuilt_at,
            input_sha256=expected_input_hash,
        )

        with (
            patch(
                'app.eod.price_bar_repair_state.'
                '_daily_price_input_fingerprint',
                return_value=(expected_input_hash, 2),
            ),
            patch(
                'app.eod.price_bar_repair_state.'
                '_derived_output_fingerprint',
                return_value=(FACTOR_OUTPUT_SHA256, 2),
            ),
        ):
            result = _exact_factor_stage_evidence(
                session,
                stage_id=FACTOR_STAGE_ID,
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
            patch(
                'app.eod.price_bar_repair_state.'
                '_derived_output_fingerprint',
                return_value=('derived-output-sha256', 2),
            ) as output_fingerprint,
        ):
            result = record_pending_exact_factor_rebuild(
                market_date=MARKET_DATE,
                exact_symbols=('005930', '000660', '005930'),
                provider_membership_sha256='A' * 64,
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
        self.assertEqual(output_fingerprint.call_count, 9)
        self.assertEqual(batch.factor_rebuild_metadata['schema_version'], 3)
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
        self.assertEqual(evidence['provider_membership_sha256'], 'a' * 64)
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
        self.assertEqual(
            evidence['stage_output_fingerprints'][FACTOR_STAGE_ID],
            {
                'row_count': 2,
                'output_sha256': 'derived-output-sha256',
            },
        )
        self.assertEqual(evidence['rebuild_completed_at'], UTC_NOW.isoformat())
        self.assertIsNone(unaffected_batch.factor_rebuild_metadata)
        self.assertIsNone(overlapping_newer_batch.factor_rebuild_metadata)

    def test_pending_proof_authority_ids_may_be_subset_of_record_destinations(
        self,
    ) -> None:
        exact_symbols = ('000660', '005930')
        authority_batch = SimpleNamespace(
            repair_batch_id='missing-bar-authority',
            created_at=UTC_NOW - timedelta(hours=1),
            required_rebuild_trade_dates=[MARKET_DATE.isoformat()],
            factor_rebuild_metadata=None,
            verification_metadata={
                'repair_evidence': {
                    'repair_kind': 'missing_bar_insert',
                    'schema_version': 'missing_bar_insert_v3',
                    'price_dependent_membership_by_date': {
                        MARKET_DATE.isoformat(): {
                            'symbols': list(exact_symbols),
                            'symbol_count': len(exact_symbols),
                            'membership_sha256': market_membership_sha256(
                                exact_symbols
                            ),
                        }
                    },
                }
            },
        )
        destination_batch = SimpleNamespace(
            repair_batch_id='correction-destination',
            created_at=UTC_NOW - timedelta(hours=1),
            required_rebuild_trade_dates=[MARKET_DATE.isoformat()],
            factor_rebuild_metadata=None,
            verification_metadata=None,
        )
        session = MagicMock()
        session.scalars.return_value.all.return_value = [
            authority_batch,
            destination_batch,
        ]

        with (
            patch(
                'app.eod.price_bar_repair_state.session_scope',
                return_value=_session_scope_for(session),
            ),
            patch(
                'app.eod.price_bar_repair_state._daily_price_input_fingerprint',
                return_value=('raw-input-sha256', len(exact_symbols)),
            ),
            patch(
                'app.eod.price_bar_repair_state.'
                '_derived_output_fingerprint',
                return_value=('derived-output-sha256', len(exact_symbols)),
            ),
        ):
            result = record_pending_exact_factor_rebuild(
                market_date=MARKET_DATE,
                exact_symbols=exact_symbols,
                provider_membership_sha256='a' * 64,
                repair_started_at=UTC_NOW - timedelta(minutes=10),
                rebuild_completed_at=UTC_NOW,
                membership_evidence_source=(
                    'pending_missing_bar_repair_metadata'
                ),
                membership_evidence_run_id=None,
                membership_evidence_run_source='pykrx',
                membership_evidence_sha256='b' * 64,
                membership_evidence_repair_batch_ids=[
                    'missing-bar-authority'
                ],
            )

        self.assertEqual(result['updated_batch_count'], 2)
        for batch in (authority_batch, destination_batch):
            evidence = batch.factor_rebuild_metadata['dates'][
                MARKET_DATE.isoformat()
            ]
            self.assertEqual(
                evidence['membership_evidence_repair_batch_ids'],
                ['missing-bar-authority'],
            )
            self.assertEqual(
                evidence['membership_evidence_sha256'],
                'b' * 64,
            )

    def test_recorder_rejects_rebuilt_superset_of_frozen_membership(
        self,
    ) -> None:
        first_symbols = ('000660',)
        second_symbols = ('000660', '005930')

        def pending_batch(repair_batch_id, symbols):
            return SimpleNamespace(
                repair_batch_id=repair_batch_id,
                created_at=UTC_NOW - timedelta(hours=1),
                required_rebuild_trade_dates=[MARKET_DATE.isoformat()],
                factor_rebuild_metadata=None,
                verification_metadata={
                    'repair_evidence': {
                        'repair_kind': 'missing_bar_insert',
                        'schema_version': 'missing_bar_insert_v3',
                        'price_dependent_membership_by_date': {
                            MARKET_DATE.isoformat(): {
                                'symbols': list(symbols),
                                'symbol_count': len(symbols),
                                'membership_sha256': (
                                    market_membership_sha256(symbols)
                                ),
                            }
                        },
                    }
                },
            )

        first_batch = pending_batch('first-batch', first_symbols)
        second_batch = pending_batch('second-batch', second_symbols)
        session = MagicMock()
        session.scalars.return_value.all.return_value = [
            first_batch,
            second_batch,
        ]

        def fingerprint(_session, *, market_date, symbols):
            self.assertEqual(market_date, MARKET_DATE)
            return (
                market_membership_sha256(symbols),
                len(symbols),
            )

        with (
            patch(
                'app.eod.price_bar_repair_state.session_scope',
                return_value=_session_scope_for(session),
            ),
            patch(
                'app.eod.price_bar_repair_state._daily_price_input_fingerprint',
                side_effect=fingerprint,
            ) as fingerprint_mock,
        ):
            with self.assertRaisesRegex(
                RuntimeError,
                'does not exactly match pending frozen membership',
            ):
                record_pending_exact_factor_rebuild(
                    market_date=MARKET_DATE,
                    exact_symbols=second_symbols,
                    provider_membership_sha256='a' * 64,
                    repair_started_at=UTC_NOW - timedelta(minutes=10),
                    rebuild_completed_at=UTC_NOW,
                    membership_evidence_source='stored_eod_run_metadata',
                    membership_evidence_run_id=321,
                    membership_evidence_run_source='pykrx',
                )

        fingerprint_mock.assert_not_called()
        self.assertIsNone(first_batch.factor_rebuild_metadata)
        self.assertIsNone(second_batch.factor_rebuild_metadata)

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
                provider_membership_sha256='a' * 64,
                repair_started_at=UTC_NOW - timedelta(minutes=10),
                rebuild_completed_at=UTC_NOW,
                membership_evidence_source='live_pykrx_market_ohlcv',
                membership_evidence_run_id=321,
                membership_evidence_run_source='pykrx',
            )

    def test_recorder_requires_membership_evidence_source(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            'requires a membership evidence source',
        ):
            record_pending_exact_factor_rebuild(
                market_date=MARKET_DATE,
                exact_symbols=('005930',),
                provider_membership_sha256='a' * 64,
                repair_started_at=UTC_NOW - timedelta(minutes=10),
                rebuild_completed_at=UTC_NOW,
            )

    def test_recorder_rejects_boolean_membership_run_id(self) -> None:
        with self.assertRaisesRegex(ValueError, 'must not be boolean'):
            record_pending_exact_factor_rebuild(
                market_date=MARKET_DATE,
                exact_symbols=('005930',),
                provider_membership_sha256='a' * 64,
                repair_started_at=UTC_NOW - timedelta(minutes=10),
                rebuild_completed_at=UTC_NOW,
                membership_evidence_source='stored_eod_run_metadata',
                membership_evidence_run_id=True,
                membership_evidence_run_source='pykrx',
            )

    def test_recorder_rejects_invalid_provider_membership_hash(self) -> None:
        with self.assertRaisesRegex(ValueError, 'canonical SHA-256'):
            record_pending_exact_factor_rebuild(
                market_date=MARKET_DATE,
                exact_symbols=('005930',),
                provider_membership_sha256='invalid',
                repair_started_at=UTC_NOW - timedelta(minutes=10),
                rebuild_completed_at=UTC_NOW,
                membership_evidence_source='stored_eod_run_metadata',
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
        metadata = _factor_rebuild_metadata(
            market_date=MARKET_DATE,
            symbols=symbols,
            rebuilt_at=rebuilt_at,
            input_sha256='recorded-input-sha256',
        )

        with (
            patch(
                'app.eod.price_bar_repair_state.'
                '_daily_price_input_fingerprint',
                return_value=('changed-input-sha256', 2),
            ),
            patch(
                'app.eod.price_bar_repair_state.'
                '_derived_output_fingerprint',
                return_value=(FACTOR_OUTPUT_SHA256, 2),
            ),
        ):
            result = _exact_factor_stage_evidence(
                session,
                stage_id=FACTOR_STAGE_ID,
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

    def test_factor_verifier_rejects_output_changed_after_recording(
        self,
    ) -> None:
        symbols = ['000660', '005930']
        rebuilt_at = UTC_NOW - timedelta(minutes=20)
        derived_at = UTC_NOW - timedelta(minutes=5)
        raw_at = UTC_NOW - timedelta(minutes=10)
        input_sha256 = 'unchanged-input-sha256'
        session = MagicMock()
        session.execute.return_value.all.return_value = [
            (symbol, derived_at, index, raw_at)
            for index, symbol in enumerate(symbols, start=1)
        ]
        metadata = _factor_rebuild_metadata(
            market_date=MARKET_DATE,
            symbols=symbols,
            rebuilt_at=rebuilt_at,
            input_sha256=input_sha256,
            output_sha256='recorded-output-sha256',
        )

        with (
            patch(
                'app.eod.price_bar_repair_state.'
                '_daily_price_input_fingerprint',
                return_value=(input_sha256, 2),
            ),
            patch(
                'app.eod.price_bar_repair_state.'
                '_derived_output_fingerprint',
                return_value=('changed-output-sha256', 2),
            ),
        ):
            result = _exact_factor_stage_evidence(
                session,
                stage_id=FACTOR_STAGE_ID,
                model=DailyMomentumFactorResult,
                required_dates=(MARKET_DATE,),
                factor_rebuild_metadata=metadata,
                raw_dependency_watermarks={MARKET_DATE: raw_at},
            )

        self.assertFalse(result['ready'])
        date_evidence = result['dates'][0]
        self.assertEqual(
            date_evidence['expected_output_sha256'],
            'recorded-output-sha256',
        )
        self.assertEqual(
            date_evidence['observed_output_sha256'],
            'changed-output-sha256',
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
        metadata = _factor_rebuild_metadata(
            market_date=MARKET_DATE,
            symbols=symbols,
            rebuilt_at=rebuilt_at,
            input_sha256=expected_input_hash,
        )

        with (
            patch(
                'app.eod.price_bar_repair_state.'
                '_daily_price_input_fingerprint',
                return_value=(expected_input_hash, 2),
            ),
            patch(
                'app.eod.price_bar_repair_state.'
                '_derived_output_fingerprint',
                return_value=(FACTOR_OUTPUT_SHA256, 2),
            ),
        ):
            result = _exact_factor_stage_evidence(
                session,
                stage_id=FACTOR_STAGE_ID,
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

    def test_trending_recorder_persists_period_output_fingerprints(
        self,
    ) -> None:
        batch = SimpleNamespace(
            repair_batch_id='repair-batch-1',
            created_at=UTC_NOW - timedelta(hours=1),
            required_rebuild_trade_dates=[MARKET_DATE.isoformat()],
            verification_metadata=None,
            factor_rebuild_metadata={
                'schema_version': 2,
                'dates': {
                    MARKET_DATE.isoformat(): {'factor_marker': True}
                },
            },
        )
        session = MagicMock()
        session.scalars.return_value.all.return_value = [batch]

        def output_fingerprint(
            _session,
            *,
            model,
            market_date,
            symbols,
            periods,
        ):
            self.assertEqual(market_date, MARKET_DATE)
            self.assertIsNone(symbols)
            return f'{periods[0]}-output-sha256', 2

        with (
            patch(
                'app.eod.price_bar_repair_state.session_scope',
                return_value=_session_scope_for(session),
            ),
            patch(
                'app.eod.price_bar_repair_state.'
                '_derived_output_fingerprint',
                side_effect=output_fingerprint,
            ) as fingerprint,
        ):
            result = record_pending_trending_rebuild(
                market_date=MARKET_DATE,
                periods=('20D', '1d', '5d', '1d'),
                repair_started_at=UTC_NOW - timedelta(minutes=10),
                rebuild_completed_at=UTC_NOW,
                exact_symbols=('005930', '000660'),
            )

        self.assertEqual(result['updated_batch_count'], 1)
        self.assertEqual(result['periods'], ['1d', '5d', '20d'])
        self.assertEqual(fingerprint.call_count, 3)
        date_scope = batch.factor_rebuild_metadata['dates'][
            MARKET_DATE.isoformat()
        ]
        self.assertTrue(date_scope['factor_marker'])
        self.assertEqual(
            date_scope['trending_output_fingerprints']['5d'][
                'output_sha256'
            ],
            '5d-output-sha256',
        )
        self.assertEqual(
            date_scope['trending_output_fingerprints']['5d'][
                'scope_symbol_count'
            ],
            2,
        )

    def test_trending_verifier_accepts_unchanged_recorded_outputs(
        self,
    ) -> None:
        session = MagicMock()
        session.execute.return_value.all.return_value = [
            (MARKET_DATE, period, 2, 2, 2, 2, 2, 2, 2, 1, 2, 2)
            for period in ('1d', '5d', '20d')
        ]

        with patch(
            'app.eod.price_bar_repair_state._derived_output_fingerprint',
            side_effect=lambda *_args, **kwargs: (
                f"{kwargs['periods'][0]}-output-sha256",
                2,
            ),
        ):
            result = _trending_evidence(
                session,
                required_dates=(MARKET_DATE,),
                rebuilt_after=UTC_NOW - timedelta(minutes=20),
                raw_dependency_watermarks={MARKET_DATE: UTC_NOW},
                factor_rebuild_metadata=_trending_rebuild_metadata(),
            )

        self.assertTrue(result['ready'])
        self.assertEqual(result['output_fingerprints_verified'], 3)

    def test_trending_verifier_rejects_output_changed_after_recording(
        self,
    ) -> None:
        session = MagicMock()
        session.execute.return_value.all.return_value = [
            (MARKET_DATE, period, 2, 2, 2, 2, 2, 2, 2, 1, 2, 2)
            for period in ('1d', '5d', '20d')
        ]

        def output_fingerprint(*_args, **kwargs):
            period = kwargs['periods'][0]
            return (
                'changed-output-sha256'
                if period == '5d'
                else f'{period}-output-sha256',
                2,
            )

        with patch(
            'app.eod.price_bar_repair_state._derived_output_fingerprint',
            side_effect=output_fingerprint,
        ):
            result = _trending_evidence(
                session,
                required_dates=(MARKET_DATE,),
                rebuilt_after=UTC_NOW - timedelta(minutes=20),
                raw_dependency_watermarks={MARKET_DATE: UTC_NOW},
                factor_rebuild_metadata=_trending_rebuild_metadata(),
            )

        self.assertFalse(result['ready'])
        self.assertEqual(result['output_fingerprints_verified'], 2)
        self.assertEqual(result['issues'][0]['period'], '5d')
        self.assertEqual(
            result['issues'][0]['observed_output_sha256'],
            'changed-output-sha256',
        )

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


class PendingExactMarketMembershipEvidenceTests(unittest.TestCase):
    @staticmethod
    def _proof_batches() -> tuple[list[dict], list[str], str]:
        kospi = [f'{index:06d}' for index in range(700)]
        kosdaq = [f'{100000 + index:06d}' for index in range(1200)]
        konex = ['900001']
        exact_symbols = sorted([*kospi, *kosdaq])
        input_sha256 = 'a' * 64
        excluded = [{'market': 'KOSPI', 'code': '00104K'}]
        excluded_sha256 = _stable_json_sha256(excluded)
        provider_members = [
            *(SimpleNamespace(symbol=symbol, market='KOSPI') for symbol in kospi),
            *(
                SimpleNamespace(symbol=symbol, market='KOSDAQ')
                for symbol in kosdaq
            ),
            *(SimpleNamespace(symbol=symbol, market='KONEX') for symbol in konex),
        ]
        provider_manifest = build_market_membership_snapshot(
            symbols=provider_members,
            markets=('KOSPI', 'KOSDAQ', 'KONEX'),
        )
        all_provider_symbols = sorted([*exact_symbols, *konex])
        provider_manifest['membership_sha256'] = market_membership_sha256(
            all_provider_symbols
        )
        target_scope = {
            'basis': 'reviewed_live_provider_membership',
            'eod_run_id': None,
            'eod_run_source': 'pykrx',
            'symbols': exact_symbols,
            'symbol_count': len(exact_symbols),
            'membership_sha256': market_membership_sha256(exact_symbols),
            'daily_price_input_sha256': input_sha256,
        }
        target_recorded = {
            'repair_kind': 'missing_bar_insert',
            'schema_version': 'missing_bar_insert_v3',
            'provider': 'pykrx',
            'market_date': MARKET_DATE.isoformat(),
            'expected_provider_membership_count': len(all_provider_symbols),
            'expected_provider_membership_sha256': (
                market_membership_sha256(all_provider_symbols)
            ),
            'provider_membership_manifest': provider_manifest,
            'provider_excluded_ohlcv_codes': excluded,
            'provider_excluded_ohlcv_code_sha256': excluded_sha256,
            'price_dependent_membership_symbols': exact_symbols,
            'price_dependent_membership_count': len(exact_symbols),
            'price_dependent_membership_sha256': (
                market_membership_sha256(exact_symbols)
            ),
            'price_dependent_membership_by_date': {
                MARKET_DATE.isoformat(): target_scope,
            },
        }
        market_membership = build_market_membership_snapshot(
            symbols=[
                *(
                    SimpleNamespace(symbol=symbol, market='KOSPI')
                    for symbol in kospi
                ),
                *(
                    SimpleNamespace(symbol=symbol, market='KOSDAQ')
                    for symbol in kosdaq
                ),
            ],
            markets=('KOSPI', 'KOSDAQ'),
        )
        prepared_fallback_scope = {
            'basis': 'reviewed_live_exact_date_pykrx_membership',
            'source': 'pykrx',
            'universe_source': 'pykrx_market_ohlcv',
            'market_date': MARKET_DATE.isoformat(),
            'markets': ['KOSPI', 'KOSDAQ'],
            'market_membership': market_membership,
            'symbols': exact_symbols,
            'symbol_count': len(exact_symbols),
            'membership_sha256': market_membership_sha256(exact_symbols),
            'excluded_ohlcv_codes': excluded,
            'excluded_ohlcv_code_sha256': excluded_sha256,
            'daily_price_input_count': len(exact_symbols),
            'daily_price_input_sha256': input_sha256,
        }
        fallback_bundle_sha256 = _stable_json_sha256(
            {
                'schema_version': 'missing_bar_downstream_membership_v1',
                'manifests': {
                    MARKET_DATE.isoformat(): prepared_fallback_scope,
                },
            }
        )
        persisted_fallback_scope = {
            **prepared_fallback_scope,
            'eod_run_id': None,
            'eod_run_source': 'pykrx',
        }
        fallback_recorded = {
            'repair_kind': 'missing_bar_insert',
            'schema_version': 'missing_bar_insert_v3',
            'provider': 'pykrx',
            'market_date': (MARKET_DATE - timedelta(days=1)).isoformat(),
            'downstream_fallback_manifest_count': 1,
            'downstream_fallback_bundle_sha256': fallback_bundle_sha256,
            'price_dependent_membership_by_date': {
                MARKET_DATE.isoformat(): persisted_fallback_scope,
            },
        }
        return (
            [
                {
                    'repair_batch_id': 'target-batch',
                    'required_rebuild_trade_dates': [MARKET_DATE.isoformat()],
                    'verification_metadata': {
                        'repair_evidence': target_recorded
                    },
                },
                {
                    'repair_batch_id': 'fallback-batch',
                    'required_rebuild_trade_dates': [MARKET_DATE.isoformat()],
                    'verification_metadata': {
                        'repair_evidence': fallback_recorded
                    },
                },
            ],
            exact_symbols,
            input_sha256,
        )

    def test_equivalent_target_and_fallback_proofs_coalesce(self) -> None:
        batches, exact_symbols, input_sha256 = self._proof_batches()

        result = resolve_pending_exact_market_membership_evidence(
            market_date=MARKET_DATE,
            pending=batches,
            input_fingerprint_loader=lambda market_date, symbols: (
                input_sha256,
                len(symbols),
            ),
        )

        self.assertIsNotNone(result)
        assert result is not None
        self.assertEqual(
            result['membership_evidence_repair_batch_ids'],
            ['fallback-batch', 'target-batch'],
        )
        self.assertEqual(result['ranking_membership_count'], len(exact_symbols))
        self.assertEqual(result['provider_membership_count'], len(exact_symbols) + 1)
        self.assertEqual(
            result['protected_provider_codes_by_market']['KOSPI'],
            ['00104K'],
        )

    def test_price_input_drift_is_rejected(self) -> None:
        batches, exact_symbols, _input_sha256 = self._proof_batches()

        with self.assertRaisesRegex(
            RuntimeError,
            'no longer matches stored price input',
        ):
            resolve_pending_exact_market_membership_evidence(
                market_date=MARKET_DATE,
                pending=batches,
                input_fingerprint_loader=lambda market_date, symbols: (
                    'f' * 64,
                    len(exact_symbols),
                ),
            )

    def test_excluded_code_hash_tampering_is_rejected(self) -> None:
        batches, _exact_symbols, input_sha256 = self._proof_batches()
        batches[0]['verification_metadata']['repair_evidence'][
            'provider_excluded_ohlcv_code_sha256'
        ] = '0' * 64

        with self.assertRaisesRegex(RuntimeError, 'exclusion hash'):
            resolve_pending_exact_market_membership_evidence(
                market_date=MARKET_DATE,
                pending=batches,
                input_fingerprint_loader=lambda market_date, symbols: (
                    input_sha256,
                    len(symbols),
                ),
            )

    def test_provider_exclusion_code_must_be_unique_across_markets(self) -> None:
        batches, exact_symbols, input_sha256 = self._proof_batches()
        target_batch = batches[0]
        recorded = target_batch['verification_metadata']['repair_evidence']
        duplicated = [
            {'market': 'KOSPI', 'code': '00104K'},
            {'market': 'KOSDAQ', 'code': '00104K'},
        ]
        recorded['provider_excluded_ohlcv_codes'] = duplicated
        recorded['provider_excluded_ohlcv_code_sha256'] = (
            _stable_json_sha256(duplicated)
        )

        with self.assertRaisesRegex(ValueError, 'duplicat'):
            resolve_pending_exact_market_membership_evidence(
                market_date=MARKET_DATE,
                pending=[target_batch],
                input_fingerprint_loader=lambda market_date, symbols: (
                    input_sha256,
                    len(exact_symbols),
                ),
            )

class PriceBarRepairFactorEvidenceContinuationTests(unittest.TestCase):
    def test_signal_context_verifier_binds_frozen_membership(self) -> None:
        rebuild_started_at = UTC_NOW - timedelta(minutes=20)
        signal_updated_at = UTC_NOW - timedelta(minutes=10)
        dependency_updated_at = UTC_NOW - timedelta(minutes=30)
        participation_updated_at = UTC_NOW - timedelta(minutes=5)
        frozen_symbols = ('000660', '005930')
        frozen_sha256 = market_membership_sha256(frozen_symbols)
        batch = SimpleNamespace(
            repair_batch_id='repair-batch-1',
            created_at=UTC_NOW - timedelta(hours=1),
            verification_metadata={
                'repair_evidence': {
                    'repair_kind': 'missing_bar_insert',
                    'schema_version': 'missing_bar_insert_v3',
                    'price_dependent_membership_by_date': {
                        MARKET_DATE.isoformat(): {
                            'symbols': list(frozen_symbols),
                            'symbol_count': len(frozen_symbols),
                            'membership_sha256': frozen_sha256,
                        }
                    },
                }
            },
            evaluation_rebuild_metadata={
                'run_started_at': rebuild_started_at.isoformat(),
                'deterministic_signal_contexts': {
                    'schema_version': 2,
                    'dates': {
                        MARKET_DATE.isoformat(): {
                            'signal_feature_count': 3,
                            'signal_feature_sha256': 'global-signal-sha256',
                            'market_participation_sha256': (
                                'participation-sha256'
                            ),
                            'frozen_membership_scopes': {
                                frozen_sha256: {
                                    'membership_count': len(frozen_symbols),
                                    'membership_sha256': frozen_sha256,
                                    'signal_feature_count': len(
                                        frozen_symbols
                                    ),
                                    'signal_feature_sha256': (
                                        'scoped-signal-sha256'
                                    ),
                                }
                            },
                        }
                    },
                },
            },
        )

        def signal_fingerprint(_session, *, market_date, symbols=None):
            self.assertEqual(market_date, MARKET_DATE)
            if symbols is None:
                return (
                    'global-signal-sha256',
                    3,
                    signal_updated_at,
                    signal_updated_at,
                )
            self.assertEqual(tuple(symbols), frozen_symbols)
            return (
                'scoped-signal-sha256',
                2,
                signal_updated_at,
                signal_updated_at,
            )

        with (
            patch(
                'app.eod.price_bar_repair_state._signal_feature_fingerprint',
                side_effect=signal_fingerprint,
            ),
            patch(
                'app.eod.price_bar_repair_state.'
                '_market_participation_fingerprint',
                return_value=(
                    'participation-sha256',
                    1,
                    participation_updated_at,
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

        self.assertTrue(result['ready'])
        date_result = result['dates'][0]
        self.assertTrue(date_result['frozen_scope_required'])
        self.assertTrue(date_result['frozen_scope_metadata_valid'])
        self.assertEqual(
            date_result['observed_scoped_signal_feature_count'],
            2,
        )

    def test_signal_context_verifier_rejects_empty_frozen_scope_rows(
        self,
    ) -> None:
        rebuild_started_at = UTC_NOW - timedelta(minutes=20)
        dependency_updated_at = UTC_NOW - timedelta(minutes=30)
        frozen_symbols = ('005930',)
        frozen_sha256 = market_membership_sha256(frozen_symbols)
        batch = SimpleNamespace(
            repair_batch_id='repair-batch-1',
            created_at=UTC_NOW - timedelta(hours=1),
            verification_metadata={
                'repair_evidence': {
                    'repair_kind': 'missing_bar_insert',
                    'schema_version': 'missing_bar_insert_v3',
                    'price_dependent_membership_by_date': {
                        MARKET_DATE.isoformat(): {
                            'symbols': list(frozen_symbols),
                            'symbol_count': 1,
                            'membership_sha256': frozen_sha256,
                        }
                    },
                }
            },
            evaluation_rebuild_metadata={
                'run_started_at': rebuild_started_at.isoformat(),
                'deterministic_signal_contexts': {
                    'schema_version': 2,
                    'dates': {
                        MARKET_DATE.isoformat(): {
                            'signal_feature_count': 1,
                            'signal_feature_sha256': 'global-sha256',
                            'market_participation_sha256': (
                                'participation-sha256'
                            ),
                            'frozen_membership_scopes': {
                                frozen_sha256: {
                                    'membership_count': 1,
                                    'membership_sha256': frozen_sha256,
                                    'signal_feature_count': 1,
                                    'signal_feature_sha256': 'scope-sha256',
                                }
                            },
                        }
                    },
                },
            },
        )
        fingerprint_results = iter(
            (
                (
                    'global-sha256',
                    1,
                    UTC_NOW - timedelta(minutes=10),
                    UTC_NOW - timedelta(minutes=10),
                ),
                (market_membership_sha256(()), 0, None, None),
            )
        )

        with (
            patch(
                'app.eod.price_bar_repair_state._signal_feature_fingerprint',
                side_effect=lambda *_args, **_kwargs: next(
                    fingerprint_results
                ),
            ),
            patch(
                'app.eod.price_bar_repair_state.'
                '_market_participation_fingerprint',
                return_value=(
                    'participation-sha256',
                    1,
                    UTC_NOW - timedelta(minutes=5),
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
        self.assertEqual(
            result['dates'][0]['observed_scoped_signal_feature_count'],
            0,
        )


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
                'verification_metadata': {
                    'repair_evidence': {
                        'repair_kind': 'missing_bar_insert',
                        'schema_version': 'missing_bar_insert_v3',
                        'price_dependent_membership_by_date': {
                            '2026-09-19': {
                                'symbols': ['005930'],
                                'symbol_count': 1,
                                'membership_sha256': (
                                    market_membership_sha256(('005930',))
                                ),
                            }
                        },
                    }
                },
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
                    'scope_symbol_count': 1,
                    'scope_membership_sha256': (
                        market_membership_sha256(('005930',))
                    ),
                    'exact_scope': True,
                },
            ) as rebuild_signal_features,
            patch(
                'app.agent_context.derived_contexts.'
                'daily_market_participation_context_service.'
                'rebuild_for_market_date',
                return_value={
                    'captured_symbol_count': 1,
                    'exact_scope': True,
                    'scope_symbol_count': 1,
                    'scope_membership_sha256': (
                        market_membership_sha256(('005930',))
                    ),
                },
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
            market_date=date(2026, 9, 19),
            exact_symbols=('005930',),
        )
        rebuild_participation.assert_called_once_with(
            market_date=date(2026, 9, 19),
            market='KRX',
            source='kis_ranking',
            exact_symbols=('005930',),
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
        self.assertEqual(
            batch.evaluation_rebuild_metadata[
                'deterministic_signal_contexts'
            ]['dates']['2026-09-19']['frozen_membership_scopes'][
                market_membership_sha256(('005930',))
            ]['signal_feature_count'],
            1,
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

    def test_verifier_freezes_pending_batch_ids_before_completion(self) -> None:
        pending = [
            {
                'repair_batch_id': 'repair-batch-older',
                'required_rebuild_trade_dates': ['2026-09-21'],
            },
            {
                'repair_batch_id': 'repair-batch-newer',
                'required_rebuild_trade_dates': ['2026-09-23'],
            },
        ]

        @contextmanager
        def available_locks(**_kwargs):
            yield True, None

        expected = {
            'status': 'completed',
            'applied': True,
            'pending_batch_count': 2,
            'completed_batch_count': 2,
            'batches': [],
        }
        with (
            patch(
                'app.eod.price_bar_repair_state.'
                'load_pending_price_bar_rebuild_batches',
                return_value=pending,
            ),
            patch(
                'app.eod.price_bar_repair_state.'
                'advisory_eod_collection_locks',
                side_effect=available_locks,
            ),
            patch(
                'app.eod.price_bar_repair_state.'
                '_verify_pending_price_bar_rebuilds_locked',
                return_value=expected,
            ) as verify_locked,
        ):
            result = verify_pending_price_bar_rebuilds(apply=True)

        self.assertEqual(result, expected)
        verify_locked.assert_called_once_with(
            apply=True,
            expected_batch_ids=(
                'repair-batch-newer',
                'repair-batch-older',
            ),
        )

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
            result = _verify_pending_price_bar_rebuilds_locked(
                apply=False,
                expected_batch_ids=(),
            )

        self.assertEqual(result['status'], 'ready')
        self.assertEqual(
            session_scope_calls,
            [{'statement_timeout_ms': 420_000}],
        )

    def test_locked_verifier_rejects_rows_outside_frozen_batch_ids(
        self,
    ) -> None:
        session = MagicMock()
        session.scalars.return_value.all.return_value = [
            SimpleNamespace(repair_batch_id='repair-batch-snapshot'),
            SimpleNamespace(repair_batch_id='repair-batch-late'),
        ]

        with patch(
            'app.eod.price_bar_repair_state.session_scope',
            return_value=_session_scope_for(session),
        ):
            result = _verify_pending_price_bar_rebuilds_locked(
                apply=True,
                expected_batch_ids=('repair-batch-snapshot',),
            )

        self.assertEqual(result['status'], 'blocked')
        self.assertFalse(result['applied'])
        self.assertEqual(result['reason'], 'pending_repair_batches_changed')
        self.assertEqual(result['completed_batch_count'], 0)

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
            result = _verify_pending_price_bar_rebuilds_locked(
                apply=True,
                expected_batch_ids=tuple(
                    batch.repair_batch_id for batch in batches
                ),
            )

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
            result = _verify_pending_price_bar_rebuilds_locked(
                apply=False,
                expected_batch_ids=tuple(
                    batch.repair_batch_id for batch in batches
                ),
            )

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
            result = _verify_pending_price_bar_rebuilds_locked(
                apply=False,
                expected_batch_ids=tuple(
                    batch.repair_batch_id for batch in batches
                ),
            )

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
