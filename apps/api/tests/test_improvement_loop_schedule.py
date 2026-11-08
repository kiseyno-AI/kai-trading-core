"""Core's background runner starts the weekly agent improvement loop."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path
import subprocess
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch
from zoneinfo import ZoneInfo

from app.analytics import improvement_loop_schedule as schedule
from app.core.settings import settings
from app.eod import runner as runner_module
from app.eod.runner import AGENT_IMPROVEMENT_LOOP_CHECK_INTERVAL, EODBackgroundRunner

KST = ZoneInfo('Asia/Seoul')
SATURDAY_10 = datetime(2026, 11, 14, 10, 0, tzinfo=KST)


class ScheduleTests(unittest.TestCase):
    def setUp(self) -> None:
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.out = Path(tmp.name)

    def test_week_label_matches_the_script(self) -> None:
        # date +%G-W%V in Asia/Seoul; 2027-01-02 (Sat) belongs to ISO week 2026-W53.
        self.assertEqual(schedule.iso_week_label(SATURDAY_10), '2026-W46')
        self.assertEqual(schedule.iso_week_label(datetime(2027, 1, 2, 10, tzinfo=KST)), '2026-W53')

    def test_due_on_saturday_from_nine_kst_until_the_week_is_done(self) -> None:
        self.assertTrue(schedule.is_due(SATURDAY_10, out_dir=self.out))
        self.assertTrue(schedule.is_due(SATURDAY_10.astimezone(timezone.utc), out_dir=self.out))
        self.assertFalse(schedule.is_due(SATURDAY_10.replace(hour=8, minute=59), out_dir=self.out))
        self.assertFalse(schedule.is_due(SATURDAY_10 - timedelta(days=1), out_dir=self.out))
        schedule.marker_path(SATURDAY_10, self.out).touch()
        self.assertFalse(schedule.is_due(SATURDAY_10, out_dir=self.out))

    def test_run_starts_the_script_and_reports_completion_by_marker(self) -> None:
        calls = []

        def fake(args, **kwargs):
            calls.append((args, kwargs))
            schedule.marker_path(SATURDAY_10, self.out).touch()
            return SimpleNamespace(returncode=0)

        script = Path('/repo/scripts/run_agent_improvement_local.sh')
        result = schedule.run(now=SATURDAY_10, out_dir=self.out, script=script, runner=fake)
        self.assertEqual(result, {'status': 'completed', 'week': '2026-W46', 'returncode': 0})
        self.assertEqual(calls[0][0], ['/bin/bash', str(script)])
        self.assertEqual(calls[0][1]['cwd'], '/repo')
        self.assertEqual(schedule.run(now=SATURDAY_10, out_dir=self.out, runner=fake), {'status': 'not_due'})
        self.assertEqual(len(calls), 1)

    def test_a_run_without_marker_is_failed_and_retried(self) -> None:
        with self.assertLogs('app.analytics.improvement_loop_schedule', level='WARNING'):
            result = schedule.run(now=SATURDAY_10, out_dir=self.out, runner=lambda *a, **k: SimpleNamespace(returncode=1))
        self.assertEqual(result['status'], 'failed')
        self.assertTrue(schedule.is_due(SATURDAY_10, out_dir=self.out))

    def test_timeout_is_reported(self) -> None:
        def slow(*args, **kwargs):
            raise subprocess.TimeoutExpired(args[0], 1)

        with self.assertLogs('app.analytics.improvement_loop_schedule', level='WARNING'):
            self.assertEqual(schedule.run(now=SATURDAY_10, out_dir=self.out, runner=slow)['status'], 'timed_out')

    def test_script_and_output_paths_are_the_repository_ones(self) -> None:
        self.assertTrue(schedule.SCRIPT.exists())
        self.assertEqual(schedule.OUT_DIR, schedule.REPO_ROOT / 'var' / 'alpha-lab')


class RunnerWiringTests(unittest.TestCase):
    def setUp(self) -> None:
        self.runner = EODBackgroundRunner(interval_seconds=60)

    def test_checked_at_most_hourly_and_only_when_enabled(self) -> None:
        now = SATURDAY_10.astimezone(timezone.utc)
        with patch('app.analytics.improvement_loop_schedule.is_due', return_value=True) as due:
            self.assertTrue(self.runner._agent_improvement_loop_due(now=now))
            self.assertFalse(self.runner._agent_improvement_loop_due(now=now + timedelta(minutes=30)))
            self.assertTrue(self.runner._agent_improvement_loop_due(now=now + AGENT_IMPROVEMENT_LOOP_CHECK_INTERVAL))
            self.assertEqual(due.call_count, 2)
        with patch.object(settings, 'agent_improvement_loop_enabled', False):
            self.assertFalse(EODBackgroundRunner()._agent_improvement_loop_due(now=now))

    def test_started_as_a_single_background_thread_step(self) -> None:
        with patch.object(self.runner, '_start_background_thread_step') as start:
            self.runner._start_agent_improvement_loop()
        name, func = start.call_args.args
        self.assertEqual(name, runner_module.AGENT_IMPROVEMENT_LOOP_STEP)
        self.assertIs(func, schedule.run)


if __name__ == '__main__':
    unittest.main()
