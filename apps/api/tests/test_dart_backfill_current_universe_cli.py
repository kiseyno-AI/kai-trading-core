import json
import unittest
from unittest.mock import AsyncMock, patch

from app.dart.backfill_current_universe import main


class DartBackfillCurrentUniverseCliTests(unittest.TestCase):
    def test_main_prints_json_result(self):
        result = {
            'scope': 'current_active_universe',
            'symbol_offset': 5,
            'sync': {'run_id': 9},
            'capture': {'run_id': 10},
        }
        with (
            patch('app.dart.backfill_current_universe.dart_fundamentals_capture_service.backfill_current_universe_fundamentals', new=AsyncMock(return_value=result)),
            patch('sys.stdout.write') as stdout_write,
        ):
            exit_code = main(['--symbol-limit', '10', '--symbol-offset', '5', '--capture-limit', '200', '--fs-div', 'CFS'])

        self.assertEqual(exit_code, 0)
        written = ''.join(call.args[0] for call in stdout_write.call_args_list)
        payload = json.loads(written)
        self.assertEqual(payload['sync']['run_id'], 9)
        self.assertEqual(payload['capture']['run_id'], 10)

    def test_main_returns_non_zero_on_failure(self):
        with (
            patch(
                'app.dart.backfill_current_universe.dart_fundamentals_capture_service.backfill_current_universe_fundamentals',
                new=AsyncMock(side_effect=RuntimeError('boom')),
            ),
            patch('sys.stderr.write') as stderr_write,
        ):
            exit_code = main(['--symbol-limit', '10'])

        self.assertEqual(exit_code, 1)
        written = ''.join(call.args[0] for call in stderr_write.call_args_list)
        payload = json.loads(written)
        self.assertEqual(payload['status'], 'failed')
        self.assertEqual(payload['error'], 'boom')
