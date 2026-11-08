"""Regression tests for the code-review fixes (control route, runner isolation, request ids)."""

from __future__ import annotations

import os
import signal
import unittest
import uuid
from datetime import date, datetime
from types import SimpleNamespace
from unittest.mock import patch

from fastapi.testclient import TestClient

from app.eod.runner import EODBackgroundRunner
from app.main import _resolve_request_id, app
from app.market.clock import KST
from app.quotes import service as quotes_service
from app.scanner.state import ScannerState


class CoreControlRouteTests(unittest.TestCase):
    def setUp(self) -> None:
        self.client = TestClient(app)  # no lifespan: no DB, no background services

    def test_port_other_than_the_served_port_is_rejected_without_touching_processes(self) -> None:
        with patch('app.api.routes_agent_context.subprocess.run') as run, patch(
            'app.api.routes_agent_context.os.kill'
        ) as kill:
            response = self.client.post(
                '/agent-context/internal/core/control',
                json={'action': 'kill', 'signal_name': 'SIGKILL', 'port': 8010},
            )

        self.assertEqual(response.status_code, 422)
        run.assert_not_called()
        kill.assert_not_called()

    def test_default_port_targets_only_the_listening_process_on_the_served_port(self) -> None:
        listening_pid = os.getpid() + 100000
        completed = SimpleNamespace(stdout=f'{listening_pid}\n', stderr='', returncode=0)
        killed: list[tuple[int, int]] = []

        with patch('app.api.routes_agent_context.subprocess.run', return_value=completed) as run, patch(
            'app.api.routes_agent_context.os.kill',
            side_effect=lambda pid, sig: killed.append((pid, int(sig))),
        ):
            response = self.client.post(
                '/agent-context/internal/core/control',
                json={'action': 'kill', 'signal_name': 'SIGTERM'},
            )

        self.assertEqual(response.status_code, 200)
        argv = run.call_args.args[0]
        served_port = response.json()['port']
        self.assertEqual(argv[0], 'lsof')
        self.assertIn(f'TCP:{served_port}', argv)
        self.assertIn('-sTCP:LISTEN', argv)
        self.assertEqual(killed, [(listening_pid, int(signal.SIGTERM))])


class BackgroundRunnerStepIsolationTests(unittest.IsolatedAsyncioTestCase):
    async def test_best_effort_async_step_failure_does_not_abort_the_tick(self) -> None:
        runner = EODBackgroundRunner(interval_seconds=60)

        async def boom() -> None:
            raise RuntimeError('exec dispatch failed')

        result = await runner._run_async_step('best_effort_step', boom())

        self.assertIs(result, False)
        self.assertEqual(runner._last_tick_error['type'], 'RuntimeError')
        self.assertIn('best_effort_step', runner._last_tick_error['message'])

    async def test_fail_closed_async_step_failure_still_propagates(self) -> None:
        runner = EODBackgroundRunner(interval_seconds=60)

        async def boom() -> None:
            raise RuntimeError('critical failure')

        with self.assertRaises(RuntimeError):
            await runner._run_async_step('critical_step', boom(), fail_closed=True)

    async def test_best_effort_blocking_step_failure_does_not_abort_the_tick(self) -> None:
        runner = EODBackgroundRunner(interval_seconds=60)

        def boom() -> None:
            raise RuntimeError('blocking failure')

        await runner._run_blocking_step('best_effort_blocking', boom)

        self.assertEqual(runner._last_tick_error['type'], 'RuntimeError')

    async def test_fail_closed_blocking_step_failure_still_propagates(self) -> None:
        runner = EODBackgroundRunner(interval_seconds=60)

        def boom() -> None:
            raise RuntimeError('blocking critical failure')

        with self.assertRaises(RuntimeError):
            await runner._run_blocking_step('critical_blocking', boom, fail_closed=True)

    async def test_successful_step_does_not_record_a_failure(self) -> None:
        runner = EODBackgroundRunner(interval_seconds=60)

        async def ok() -> str:
            return 'done'

        self.assertIs(await runner._run_async_step('ok_step', ok()), True)
        self.assertEqual(await runner._run_async_step('ok_result', ok(), return_task_result=True), 'done')
        self.assertIsNone(runner._last_tick_error)


class RequestIdTests(unittest.TestCase):
    def test_header_safe_ids_are_kept(self) -> None:
        for value in ('req-test-1', 'abc.DEF_123:xyz', str(uuid.uuid4()), 'a' * 128):
            self.assertEqual(_resolve_request_id(value), value)

    def test_unsafe_or_oversized_ids_are_replaced_with_a_uuid(self) -> None:
        for value in (None, '', '   ', 'has space', 'new\nline', 'semi;colon', 'a' * 129, '한글'):
            resolved = _resolve_request_id(value)
            self.assertNotEqual(resolved, value)
            uuid.UUID(resolved)


class CalendarDateFallbackTests(unittest.TestCase):
    def test_unparseable_server_time_falls_back_to_the_korea_calendar_date(self) -> None:
        # 07:00 KST on 2026-11-05 is still 2026-11-04 in UTC.
        with patch.object(quotes_service, 'datetime') as fake_datetime:
            fake_datetime.now.return_value = datetime(2026, 11, 5, 7, 0, tzinfo=KST)
            resolved = quotes_service._market_calendar_date('')

        fake_datetime.now.assert_called_once_with(KST)
        self.assertEqual(resolved, date(2026, 11, 5))


class ScannerStateTimestampTests(unittest.TestCase):
    def test_update_keeps_the_naive_utc_iso_z_wire_format(self) -> None:
        state = ScannerState()
        snapshot = state.update(['005930'])

        stamp = snapshot['last_updated_at']
        self.assertTrue(stamp.endswith('Z'))
        self.assertNotIn('+', stamp)
        datetime.fromisoformat(stamp[:-1])


if __name__ == '__main__':
    unittest.main()
