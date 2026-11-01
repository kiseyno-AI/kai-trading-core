import argparse
import unittest
from datetime import date
from unittest.mock import AsyncMock, Mock, patch

from app.dart import repair_legacy_cfs_no_data as repair


def _candidate() -> dict:
    return {
        'id': 8,
        'rcept_no': '20260413000001',
        'corp_code': 'corp-333333',
        'corp_name': 'Legacy Corp',
        'stock_code': '333333',
        'report_nm': '사업보고서 (2025.12)',
        'filing_category': 'annual_report',
        'legacy_snapshot_id': 108,
        'legacy_source_status': '013',
    }


def _args(**overrides) -> argparse.Namespace:
    values = {
        'symbols': '333333',
        'start_date': date(2026, 1, 1),
        'end_date': date(2026, 10, 30),
        'apply': False,
        'expected_stock_code_manifest_sha256': None,
        'expected_candidate_manifest_sha256': None,
    }
    values.update(overrides)
    return argparse.Namespace(**values)


class LegacyCfsNoDataRepairTests(unittest.IsolatedAsyncioTestCase):
    async def test_dry_run_is_read_only_and_emits_hash_bound_manifest(self):
        repository = Mock()
        repository.list_legacy_cfs_no_data_recovery_candidates.return_value = [
            _candidate()
        ]
        service = Mock()
        service.capture_recent_fundamentals = AsyncMock()

        with (
            patch.object(repair, 'dart_fundamentals_repository', repository),
            patch.object(
                repair,
                'dart_fundamentals_capture_service',
                service,
            ),
        ):
            exit_code, result = await repair._run(_args())

        self.assertEqual(exit_code, 0)
        self.assertEqual(result['status'], 'dry_run')
        self.assertEqual(result['candidate_count'], 1)
        self.assertEqual(len(result['stock_code_manifest_sha256']), 64)
        self.assertEqual(len(result['candidate_manifest_sha256']), 64)
        service.capture_recent_fundamentals.assert_not_awaited()

    async def test_apply_rejects_hash_mismatch_before_capture(self):
        repository = Mock()
        repository.list_legacy_cfs_no_data_recovery_candidates.return_value = [
            _candidate()
        ]
        service = Mock()
        service.capture_recent_fundamentals = AsyncMock()

        with (
            patch.object(repair, 'dart_fundamentals_repository', repository),
            patch.object(
                repair,
                'dart_fundamentals_capture_service',
                service,
            ),
        ):
            with self.assertRaisesRegex(
                ValueError,
                'stock-code manifest SHA256 mismatch',
            ):
                await repair._run(
                    _args(
                        apply=True,
                        expected_stock_code_manifest_sha256='0' * 64,
                        expected_candidate_manifest_sha256='0' * 64,
                    )
                )

        service.capture_recent_fundamentals.assert_not_awaited()

    async def test_apply_uses_exact_event_ids_and_rechecks_remaining_debt(self):
        candidate = _candidate()
        repository = Mock()
        repository.list_legacy_cfs_no_data_recovery_candidates.side_effect = [
            [candidate],
            [],
        ]
        service = Mock()
        service.capture_recent_fundamentals = AsyncMock(
            return_value={
                'events_captured': 1,
                'events_failed': 0,
                'events_skipped': 0,
            }
        )
        stock_hash = repair._stock_code_manifest_sha256(['333333'])
        candidate_hash = (
            repair.legacy_cfs_recovery_candidate_manifest_sha256(
                [candidate]
            )
        )

        with (
            patch.object(repair, 'dart_fundamentals_repository', repository),
            patch.object(
                repair,
                'dart_fundamentals_capture_service',
                service,
            ),
        ):
            exit_code, result = await repair._run(
                _args(
                    apply=True,
                    expected_stock_code_manifest_sha256=stock_hash,
                    expected_candidate_manifest_sha256=candidate_hash,
                )
            )

        self.assertEqual(exit_code, 0)
        self.assertEqual(result['status'], 'completed')
        self.assertEqual(result['remaining_candidate_count'], 0)
        service.capture_recent_fundamentals.assert_awaited_once()
        kwargs = service.capture_recent_fundamentals.await_args.kwargs
        self.assertEqual(kwargs['event_ids'], [8])
        self.assertEqual(kwargs['stock_codes'], ['333333'])
        self.assertFalse(kwargs['force'])
        self.assertTrue(kwargs['legacy_cfs_no_data_recovery'])


if __name__ == '__main__':
    unittest.main()
