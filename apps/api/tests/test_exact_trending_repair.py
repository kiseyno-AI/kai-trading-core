from __future__ import annotations

from contextlib import contextmanager
from contextlib import redirect_stdout
from datetime import datetime, timezone
from io import StringIO
import json
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import Mock, patch

from sqlalchemy.exc import SQLAlchemyError

from app.eod.derived_stage_repair import MANIFEST_SCHEMA_VERSION
from app.eod.exact_trending_repair import (
    ExactTrendingRankRepair,
    PERIODS,
    main,
)
from app.eod.market_membership import market_membership_sha256
from app.internal.exact_repair_manifest import canonical_json_sha256


SYMBOLS = ['000660', '005930']


def _manifest(*, market_date='2026-10-28', symbols=None):
    exact = list(symbols or SYMBOLS)
    return {
        'schema_version': MANIFEST_SCHEMA_VERSION,
        'date_count': 1,
        'dates': [
            {
                'market_date': market_date,
                'symbol_count': len(exact),
                'membership_sha256': market_membership_sha256(exact),
                'symbols': exact,
            }
        ],
    }


def _trending_result(symbols=None):
    exact = list(symbols or SYMBOLS)
    return {
        'status': 'completed',
        'exact_scope': True,
        'scope_symbol_count': len(exact),
        'scope_membership_sha256': market_membership_sha256(exact),
        'periods_total': len(PERIODS),
        'periods_persisted': len(PERIODS),
        'periods_unavailable': 0,
        'periods': [
            {'period': period, 'status': 'completed'} for period in PERIODS
        ],
    }


def _fingerprints(symbols=None, *, suffix='v1'):
    exact = list(symbols or SYMBOLS)
    return {
        period: {
            'row_count': 2,
            'output_sha256': f'{period}-{suffix}',
            'exact_scope': True,
            'scope_symbol_count': len(exact),
            'scope_membership_sha256': market_membership_sha256(exact),
        }
        for period in PERIODS
    }


def _source_fingerprints(*, suffix='v1'):
    return {
        period: {
            'row_count': len(SYMBOLS),
            'source_sha256': canonical_json_sha256(
                {'period': period, 'suffix': suffix}
            ),
        }
        for period in PERIODS
    }


@contextmanager
def _guard(**_kwargs):
    yield True, None


class ExactTrendingRankRepairTests(unittest.TestCase):
    def _repair(self, **overrides):
        defaults = {
            'trending_runner': Mock(return_value=_trending_result()),
            'evidence_recorder': Mock(
                return_value={
                    'updated_batch_count': 1,
                    'output_fingerprints': _fingerprints(),
                }
            ),
            'stored_symbols_loader': lambda _market_date: SYMBOLS,
            'frozen_membership_loader': (
                lambda **_kwargs: tuple(SYMBOLS)
            ),
            'completion_state_loader': lambda *_args: {
                'metadata_verified': True,
                'source_fingerprints': _source_fingerprints(),
                'output_fingerprints': _fingerprints(),
            },
            'guard_factory': _guard,
            'finality_validator': lambda _market_date: None,
            'clock': lambda: datetime(2026, 10, 31, tzinfo=timezone.utc),
        }
        defaults.update(overrides)
        return ExactTrendingRankRepair(_manifest(), **defaults)

    def test_dry_run_reviews_exact_frozen_scope_without_mutation(self):
        trending_runner = Mock()
        evidence_recorder = Mock()
        repair = self._repair(
            trending_runner=trending_runner,
            evidence_recorder=evidence_recorder,
        )

        result = repair.run()

        self.assertEqual(result['status'], 'dry_run')
        self.assertEqual(result['validated_date_count'], 1)
        self.assertEqual(result['manifest_sha256'], repair.manifest_sha256)
        trending_runner.assert_not_called()
        evidence_recorder.assert_not_called()

    def test_main_loads_manifest_through_cli_entrypoint(self):
        with TemporaryDirectory() as directory:
            manifest_path = Path(directory) / 'manifest.json'
            manifest_path.write_text(
                json.dumps(_manifest()),
                encoding='utf-8',
            )
            repair = Mock()
            repair.run.return_value = {'status': 'dry_run'}
            with (
                patch(
                    'app.eod.exact_trending_repair.'
                    'ExactTrendingRankRepair',
                    return_value=repair,
                ) as repair_type,
                redirect_stdout(StringIO()),
            ):
                exit_code = main(
                    ['--manifest', str(manifest_path), '--dry-run']
                )

        self.assertEqual(exit_code, 0)
        repair_type.assert_called_once_with(_manifest())
        repair.run.assert_called_once_with(
            apply=False,
            expected_manifest_sha256=None,
            checkpoint_path=None,
        )

    def test_main_rejects_manifest_as_apply_checkpoint(self):
        with TemporaryDirectory() as directory:
            manifest_path = Path(directory) / 'manifest.json'
            manifest_path.write_text(
                json.dumps(_manifest()),
                encoding='utf-8',
            )

            with self.assertRaisesRegex(
                ValueError,
                'manifest and checkpoint paths must be different',
            ):
                main(
                    [
                        '--manifest',
                        str(manifest_path),
                        '--checkpoint',
                        str(manifest_path),
                        '--expected-manifest-sha256',
                        '0' * 64,
                        '--apply',
                    ]
                )

    def test_frozen_membership_mismatch_blocks_without_mutation(self):
        trending_runner = Mock()
        repair = self._repair(
            trending_runner=trending_runner,
            frozen_membership_loader=lambda **_kwargs: ('000660',),
        )

        result = repair.run()

        self.assertEqual(result['status'], 'blocked')
        self.assertIn('frozen membership', result['scope_errors'][0]['error'])
        trending_runner.assert_not_called()

    def test_apply_rejects_unreviewed_manifest_hash(self):
        trending_runner = Mock()
        repair = self._repair(trending_runner=trending_runner)

        result = repair.run(
            apply=True,
            expected_manifest_sha256='0' * 64,
            checkpoint_path='unused.json',
        )

        self.assertEqual(result['status'], 'blocked')
        self.assertEqual(result['reason'], 'expected_manifest_sha256_mismatch')
        trending_runner.assert_not_called()

    def test_apply_rebuilds_all_periods_and_records_pending_evidence(self):
        trending_runner = Mock(return_value=_trending_result())
        evidence_recorder = Mock(
            return_value={
                'updated_batch_count': 1,
                'output_fingerprints': _fingerprints(),
            }
        )
        repair = self._repair(
            trending_runner=trending_runner,
            evidence_recorder=evidence_recorder,
        )

        with TemporaryDirectory() as directory:
            result = repair.run(
                apply=True,
                expected_manifest_sha256=repair.manifest_sha256,
                checkpoint_path=Path(directory) / 'checkpoint.json',
            )

        self.assertEqual(result['status'], 'completed')
        self.assertEqual(
            trending_runner.call_args.kwargs,
            {
                'market_date': '2026-10-28',
                'periods': list(PERIODS),
                'source': 'exact_trending_rank_repair_v1',
                'symbols': SYMBOLS,
                'authoritative_scope': True,
            },
        )
        self.assertEqual(
            evidence_recorder.call_args.kwargs['periods'],
            PERIODS,
        )
        self.assertEqual(
            evidence_recorder.call_args.kwargs['exact_symbols'],
            SYMBOLS,
        )

    def test_guard_exit_failure_reconciles_durable_trending_rows(self):
        @contextmanager
        def failing_exit_guard(**_kwargs):
            yield True, None
            raise SQLAlchemyError('guard commit failed')

        repair = self._repair(guard_factory=failing_exit_guard)
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

    def test_checkpoint_failure_reconciles_durable_trending_rows(self):
        repair = self._repair()
        with (
            TemporaryDirectory() as directory,
            patch(
                'app.eod.exact_trending_repair.write_checkpoint',
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

    def test_completed_date_is_rebuilt_after_output_fingerprint_drift(self):
        live_fingerprints = _fingerprints()
        trending_runner = Mock(return_value=_trending_result())

        def record(**_kwargs):
            live_fingerprints.clear()
            live_fingerprints.update(_fingerprints())
            return {
                'updated_batch_count': 1,
                'output_fingerprints': _fingerprints(),
            }

        repair = self._repair(
            trending_runner=trending_runner,
            evidence_recorder=Mock(side_effect=record),
            completion_state_loader=lambda *_args: {
                'metadata_verified': True,
                'source_fingerprints': _source_fingerprints(),
                'output_fingerprints': {
                    period: dict(evidence)
                    for period, evidence in live_fingerprints.items()
                },
            },
        )
        with TemporaryDirectory() as directory:
            checkpoint = Path(directory) / 'checkpoint.json'
            first = repair.run(
                apply=True,
                expected_manifest_sha256=repair.manifest_sha256,
                checkpoint_path=checkpoint,
            )
            live_fingerprints['5d']['output_sha256'] = 'deleted-row-drift'
            resumed = repair.run(
                apply=True,
                expected_manifest_sha256=repair.manifest_sha256,
                checkpoint_path=checkpoint,
            )

        self.assertEqual(first['status'], 'completed')
        self.assertEqual(resumed['status'], 'completed')
        self.assertEqual(resumed['skipped_dates'], 0)
        self.assertEqual(trending_runner.call_count, 2)

    def test_completed_date_skips_only_after_live_output_and_metadata_proof(self):
        trending_runner = Mock(return_value=_trending_result())
        repair = self._repair(trending_runner=trending_runner)
        with TemporaryDirectory() as directory:
            checkpoint = Path(directory) / 'checkpoint.json'
            first = repair.run(
                apply=True,
                expected_manifest_sha256=repair.manifest_sha256,
                checkpoint_path=checkpoint,
            )
            resumed = repair.run(
                apply=True,
                expected_manifest_sha256=repair.manifest_sha256,
                checkpoint_path=checkpoint,
            )

        self.assertEqual(first['status'], 'completed')
        self.assertEqual(resumed['status'], 'completed')
        self.assertEqual(resumed['skipped_dates'], 1)
        self.assertEqual(trending_runner.call_count, 1)

    def test_stored_bar_superset_is_allowed_for_frozen_ranking_scope(self):
        repair = self._repair(
            stored_symbols_loader=lambda _market_date: [
                '000660',
                '005930',
                '105740',
            ]
        )

        result = repair.run()

        self.assertEqual(result['status'], 'dry_run')
        self.assertEqual(result['validated_date_count'], 1)

    def test_completed_date_is_rebuilt_after_source_fingerprint_drift(self):
        source_suffix = ['v1']
        trending_runner = Mock(return_value=_trending_result())

        def completion_state(*_args):
            current = {
                'metadata_verified': True,
                'source_fingerprints': _source_fingerprints(
                    suffix=source_suffix[0]
                ),
                'output_fingerprints': _fingerprints(),
            }
            source_suffix[0] = 'v1'
            return current

        repair = self._repair(
            trending_runner=trending_runner,
            completion_state_loader=completion_state,
        )
        with TemporaryDirectory() as directory:
            checkpoint = Path(directory) / 'checkpoint.json'
            first = repair.run(
                apply=True,
                expected_manifest_sha256=repair.manifest_sha256,
                checkpoint_path=checkpoint,
            )
            source_suffix[0] = 'drifted'
            resumed = repair.run(
                apply=True,
                expected_manifest_sha256=repair.manifest_sha256,
                checkpoint_path=checkpoint,
            )

        self.assertEqual(first['status'], 'completed')
        self.assertEqual(resumed['status'], 'completed')
        self.assertEqual(resumed['skipped_dates'], 0)
        self.assertEqual(trending_runner.call_count, 2)


if __name__ == '__main__':
    unittest.main()
