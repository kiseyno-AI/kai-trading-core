import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

from app.dart.backfill_current_universe_runner import main


class DartBackfillCurrentUniverseRunnerTests(unittest.TestCase):
    def test_runner_advances_batches_and_writes_log(self):
        results = [
            {
                'active_symbols_total': 12,
                'symbols_selected': 5,
                'symbols_resolved': 5,
                'sync': {'run_id': 11, 'events_inserted': 7},
                'capture': {'run_id': 21, 'events_captured': 4, 'events_failed': 1},
            },
            {
                'active_symbols_total': 12,
                'symbols_selected': 5,
                'symbols_resolved': 5,
                'sync': {'run_id': 12, 'events_inserted': 8},
                'capture': {'run_id': 22, 'events_captured': 5, 'events_failed': 0},
            },
            {
                'active_symbols_total': 12,
                'symbols_selected': 2,
                'symbols_resolved': 2,
                'sync': {'run_id': 13, 'events_inserted': 3},
                'capture': {'run_id': 23, 'events_captured': 2, 'events_failed': 0},
            },
        ]
        with tempfile.TemporaryDirectory() as tmpdir:
            log_file = Path(tmpdir) / 'runner.log'
            with (
                patch(
                    'app.dart.backfill_current_universe_runner.dart_fundamentals_capture_service.backfill_current_universe_fundamentals',
                    new=AsyncMock(side_effect=results),
                ) as backfill_mock,
                patch('sys.stdout.write') as stdout_write,
            ):
                exit_code = main(['--start-offset', '0', '--batch-size', '5', '--capture-limit', '20', '--log-file', str(log_file)])

            self.assertEqual(exit_code, 0)
            self.assertEqual(backfill_mock.await_count, 3)
            payload = json.loads(''.join(call.args[0] for call in stdout_write.call_args_list))
            self.assertEqual(payload['status'], 'completed')
            self.assertEqual(payload['next_offset'], 12)
            self.assertEqual(payload['total_events_inserted'], 18)
            self.assertTrue(log_file.exists())
            log_lines = [json.loads(line) for line in log_file.read_text(encoding='utf-8').splitlines()]
            self.assertEqual(log_lines[0]['event'], 'runner_started')
            self.assertEqual(log_lines[-1]['event'], 'runner_completed')

    def test_runner_logs_failure_and_returns_non_zero(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            log_file = Path(tmpdir) / 'runner.log'
            with (
                patch(
                    'app.dart.backfill_current_universe_runner.dart_fundamentals_capture_service.backfill_current_universe_fundamentals',
                    new=AsyncMock(side_effect=RuntimeError('boom')),
                ),
                patch('sys.stderr.write') as stderr_write,
            ):
                exit_code = main(['--start-offset', '25', '--batch-size', '10', '--capture-limit', '50', '--log-file', str(log_file)])

            self.assertEqual(exit_code, 1)
            payload = json.loads(''.join(call.args[0] for call in stderr_write.call_args_list))
            self.assertEqual(payload['status'], 'failed')
            self.assertEqual(payload['error'], 'boom')
            log_lines = [json.loads(line) for line in log_file.read_text(encoding='utf-8').splitlines()]
            self.assertEqual(log_lines[-1]['event'], 'batch_failed')
            self.assertEqual(log_lines[-1]['current_offset'], 25)

    def test_runner_rejects_invalid_bounds(self):
        with self.assertRaises(SystemExit):
            main(['--batch-size', '0'])
        with self.assertRaises(SystemExit):
            main(['--start-offset', '-1'])
        with self.assertRaises(SystemExit):
            main(['--capture-limit', '0'])
