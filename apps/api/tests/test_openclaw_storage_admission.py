from __future__ import annotations

import shutil
import tempfile
import unittest
from datetime import date
from pathlib import Path
from unittest.mock import MagicMock, patch

from app.agent_context import orchestrator as orchestrator_module
from app.agent_context.feedback_failure_taxonomy import classify_feedback_failure
from app.agent_context.openclaw_storage_admission import (
    inspect_openclaw_storage_admission,
)
from app.agent_context.orchestrator import (
    AgentDispatchSpec,
    AgentDispatchTerminalError,
    OpenClawAgentClient,
    _is_retryable_dispatch_exception,
)
from app.core.settings import Settings


class OpenClawStorageAdmissionTests(unittest.TestCase):
    def setUp(self) -> None:
        self.dispatch_coordinator_patcher = patch.object(
            orchestrator_module,
            '_OPENCLAW_DISPATCH_COORDINATOR',
            orchestrator_module._OpenClawDispatchCoordinator(),
        )
        self.dispatch_coordinator_patcher.start()
        self.addCleanup(self.dispatch_coordinator_patcher.stop)

    def test_default_dispatch_guard_keeps_twenty_gibibytes_free(self) -> None:
        self.assertEqual(
            20 * 1024 * 1024 * 1024,
            Settings.model_fields['openclaw_dispatch_min_free_bytes'].default,
        )

    def test_admits_when_state_filesystem_has_required_free_space(self) -> None:
        with tempfile.TemporaryDirectory() as state_dir:
            with patch(
                'app.agent_context.openclaw_storage_admission.shutil.disk_usage',
                return_value=shutil._ntuple_diskusage(
                    total=100,
                    used=40,
                    free=60,
                ),
            ):
                result = inspect_openclaw_storage_admission(
                    state_dir=state_dir,
                    min_free_bytes=50,
                )

        self.assertTrue(result['admitted'])
        self.assertEqual(result['status'], 'ready')
        self.assertEqual(result['free_bytes'], 60)
        self.assertEqual(result['min_free_bytes'], 50)

    def test_blocks_before_dispatch_when_free_space_is_below_threshold(self) -> None:
        with tempfile.TemporaryDirectory() as state_dir:
            with patch(
                'app.agent_context.openclaw_storage_admission.shutil.disk_usage',
                return_value=shutil._ntuple_diskusage(
                    total=100,
                    used=60,
                    free=40,
                ),
            ):
                result = inspect_openclaw_storage_admission(
                    state_dir=state_dir,
                    min_free_bytes=50,
                )

        self.assertFalse(result['admitted'])
        self.assertEqual(result['status'], 'blocked')
        self.assertEqual(result['reason'], 'disk_free_below_minimum')
        self.assertEqual(result['shortfall_bytes'], 10)

    def test_uses_existing_parent_when_configured_state_dir_is_absent(self) -> None:
        with tempfile.TemporaryDirectory() as root:
            missing_state_dir = Path(root) / 'missing' / 'openclaw'
            disk_usage = MagicMock(
                return_value=shutil._ntuple_diskusage(
                    total=100,
                    used=10,
                    free=90,
                )
            )
            with patch(
                'app.agent_context.openclaw_storage_admission.shutil.disk_usage',
                disk_usage,
            ):
                result = inspect_openclaw_storage_admission(
                    state_dir=str(missing_state_dir),
                    min_free_bytes=50,
                )

        self.assertTrue(result['admitted'])
        disk_usage.assert_called_once_with(Path(root))

    def test_fails_closed_when_disk_usage_cannot_be_observed(self) -> None:
        with tempfile.TemporaryDirectory() as state_dir:
            with patch(
                'app.agent_context.openclaw_storage_admission.shutil.disk_usage',
                side_effect=OSError('statvfs unavailable'),
            ):
                result = inspect_openclaw_storage_admission(
                    state_dir=state_dir,
                    min_free_bytes=50,
                )

        self.assertFalse(result['admitted'])
        self.assertEqual(result['status'], 'blocked')
        self.assertEqual(result['reason'], 'disk_usage_unavailable')
        self.assertEqual(result['error_type'], 'OSError')

    def test_single_dispatch_does_not_open_http_client_when_storage_is_blocked(self) -> None:
        client = OpenClawAgentClient()
        spec = AgentDispatchSpec(
            role='eod_summary',
            agent_name='eod-summarizer',
            instruction='Summarize.',
        )
        blocked = {
            'admitted': False,
            'status': 'blocked',
            'reason': 'disk_free_below_minimum',
            'state_dir': '/tmp/openclaw',
            'probe_path': '/tmp',
            'free_bytes': 40,
            'min_free_bytes': 50,
            'shortfall_bytes': 10,
        }

        with (
            patch(
                'app.agent_context.orchestrator.inspect_openclaw_storage_admission',
                return_value=blocked,
            ),
            patch('app.agent_context.orchestrator.httpx.Client') as http_client,
        ):
            with self.assertRaises(AgentDispatchTerminalError) as raised:
                client.dispatch(
                    spec=spec,
                    payload={
                        'market_date': '2026-10-19',
                        'symbol': '005930',
                        'request_run_id': 'feedback-run-low-disk',
                    },
                )

        http_client.assert_not_called()
        self.assertEqual(
            raised.exception.details['failure_kind'],
            'openclaw_storage_admission_blocked',
        )
        self.assertTrue(raised.exception.details['retryable'])
        self.assertTrue(
            raised.exception.details['shared_dispatch_outage']
        )

    def test_batch_dispatch_does_not_open_http_client_when_storage_is_blocked(self) -> None:
        client = OpenClawAgentClient()
        spec = AgentDispatchSpec(
            role='eod_summary',
            agent_name='eod-summarizer',
            instruction='Summarize.',
        )
        blocked = {
            'admitted': False,
            'status': 'blocked',
            'reason': 'disk_usage_unavailable',
            'state_dir': '/tmp/openclaw',
            'probe_path': '/tmp',
            'free_bytes': None,
            'min_free_bytes': 50,
            'shortfall_bytes': None,
            'error_type': 'OSError',
        }

        with (
            patch(
                'app.agent_context.orchestrator.inspect_openclaw_storage_admission',
                return_value=blocked,
            ),
            patch('app.agent_context.orchestrator.httpx.Client') as http_client,
        ):
            with self.assertRaises(AgentDispatchTerminalError):
                client.dispatch_batch(
                    spec=spec,
                    market_date=date(2026, 10, 19),
                    batch_items=[
                        {
                            'market_date': '2026-10-19',
                            'symbol': '005930',
                            'request_run_id': 'feedback-run-low-disk',
                        }
                    ],
                )

        http_client.assert_not_called()

    def test_storage_admission_failure_keeps_capacity_taxonomy(self) -> None:
        classification = classify_feedback_failure(
            failure_kind='openclaw_storage_admission_blocked',
            context={
                'failure_status': 'capacity/storage-admission-blocked',
                'failure_category': 'capacity',
            },
        )

        self.assertEqual(
            classification.failure_kind,
            'openclaw_storage_admission_blocked',
        )
        self.assertEqual(
            classification.failure_status,
            'capacity/storage-admission-blocked',
        )
        self.assertEqual(classification.failure_category, 'capacity')

    def test_terminal_run_circuit_can_still_be_retryable_after_capacity_recovery(self) -> None:
        self.assertTrue(
            _is_retryable_dispatch_exception(
                AgentDispatchTerminalError(
                    'low disk',
                    details={'retryable': True},
                )
            )
        )
        self.assertFalse(
            _is_retryable_dispatch_exception(
                AgentDispatchTerminalError(
                    'hard capacity limit',
                    details={'retryable': False},
                )
            )
        )


if __name__ == '__main__':
    unittest.main()
