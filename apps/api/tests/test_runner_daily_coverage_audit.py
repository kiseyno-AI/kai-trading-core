"""The background runner's hourly, read-only daily-coverage check."""

from __future__ import annotations

import asyncio
import unittest
from datetime import date, datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import patch

from app.core.settings import settings
from app.eod import runner as runner_module
from app.eod.runner import DAILY_COVERAGE_AUDIT_MIN_INTERVAL, EODBackgroundRunner
from app.internal.daily_coverage_audit import COVERAGE_TABLES, evaluate_daily_coverage

AUDIT_MODULE = 'app.internal.daily_coverage_audit'
NOW = datetime(2026, 11, 6, 6, 0, tzinfo=timezone.utc)


def _report(market_date: str = '2026-11-05', **overrides: int) -> dict:
    counts = {spec.key: 2769 for spec in COVERAGE_TABLES}
    counts.update(overrides)
    return evaluate_daily_coverage(counts, market_date=market_date)


def _market(state: str) -> SimpleNamespace:
    return SimpleNamespace(state=state)


class DailyCoverageAuditDueTests(unittest.TestCase):
    def setUp(self) -> None:
        self.runner = EODBackgroundRunner(interval_seconds=60)
        self._idle = patch.object(
            runner_module.auto_pykrx_eod_scheduler.state, 'active_task', False
        )
        self._idle.start()
        self.addCleanup(self._idle.stop)

    def test_due_when_enabled_idle_and_never_audited(self) -> None:
        for state in ('PREOPEN', 'OPEN', 'WEEKEND', 'HOLIDAY'):
            self.assertTrue(self.runner._daily_coverage_audit_due(_market(state), now=NOW), state)

    def test_not_due_while_the_close_pipeline_window_is_open(self) -> None:
        self.assertFalse(self.runner._daily_coverage_audit_due(_market('CLOSED'), now=NOW))
        self.assertFalse(self.runner._daily_coverage_audit_due(None, now=NOW))

    def test_not_due_while_the_daily_pipeline_is_active(self) -> None:
        with patch.object(runner_module.auto_pykrx_eod_scheduler.state, 'active_task', True):
            self.assertFalse(self.runner._daily_coverage_audit_due(_market('PREOPEN'), now=NOW))

    def test_not_due_when_disabled_by_setting(self) -> None:
        with patch.object(settings, 'daily_coverage_audit_enabled', False):
            self.assertFalse(self.runner._daily_coverage_audit_due(_market('OPEN'), now=NOW))

    def test_at_most_once_per_interval(self) -> None:
        self.runner._last_daily_coverage_audit_at = NOW - DAILY_COVERAGE_AUDIT_MIN_INTERVAL + timedelta(minutes=1)
        self.assertFalse(self.runner._daily_coverage_audit_due(_market('OPEN'), now=NOW))
        self.runner._last_daily_coverage_audit_at = NOW - DAILY_COVERAGE_AUDIT_MIN_INTERVAL
        self.assertTrue(self.runner._daily_coverage_audit_due(_market('OPEN'), now=NOW))


class DailyCoverageAuditExecutionTests(unittest.TestCase):
    def setUp(self) -> None:
        self.runner = EODBackgroundRunner(interval_seconds=60)

    def _audit(self, report: dict | None):
        dates = [date(2026, 11, 5)] if report is not None else []
        return (
            patch(f'{AUDIT_MODULE}.load_recent_trade_dates', return_value=dates),
            patch(f'{AUDIT_MODULE}.run_audit', return_value=[report] if report else []),
        )

    def test_snapshot_exposes_no_audit_before_the_first_run(self) -> None:
        self.assertIsNone(self.runner.snapshot()['daily_coverage'])

    def test_a_healthy_day_is_recorded_without_a_warning(self) -> None:
        loader, auditor = self._audit(_report())
        with loader, auditor, self.assertNoLogs('app.eod.runner', level='WARNING'):
            summary = self.runner._audit_daily_coverage_sync(now=NOW)

        self.assertEqual(summary['status'], 'ok')
        self.assertEqual(summary['problem_tables'], [])
        self.assertEqual(self.runner.snapshot()['daily_coverage']['market_date'], '2026-11-05')

    def test_a_hole_is_logged_and_exposed_through_the_snapshot(self) -> None:
        report = _report('2026-11-04', momentum=82, composite=82, signal_features=0)
        loader, auditor = self._audit(report)
        with loader, auditor, self.assertLogs('app.eod.runner', level='WARNING') as captured:
            self.runner._audit_daily_coverage_sync(now=NOW)

        message = captured.output[0]
        self.assertIn('2026-11-04', message)
        self.assertIn('momentum=82(low)', message)
        self.assertIn('signal_features=0(empty)', message)
        coverage = self.runner.snapshot()['daily_coverage']
        self.assertEqual(coverage['status'], 'degraded')
        self.assertEqual(
            {item['table'] for item in coverage['problem_tables']},
            {'momentum', 'composite', 'signal_features'},
        )

    def test_the_same_hole_warns_once_and_a_new_or_recurring_hole_warns_again(self) -> None:
        hole = _report('2026-11-04', momentum=82)
        other = _report('2026-11-04', momentum=82, beta=10)
        ok = _report('2026-11-04')

        def run(report):
            loader, auditor = self._audit(report)
            with loader, auditor:
                self.runner._audit_daily_coverage_sync(now=NOW)

        with self.assertLogs('app.eod.runner', level='WARNING') as captured:
            run(hole)
            run(hole)  # unchanged -> no second warning
            run(other)  # different set -> warns
            run(ok)  # recovered -> resets the alert key
            run(other)  # recurs -> warns again
        self.assertEqual(len(captured.output), 3)

    def test_no_price_bars_leaves_the_previous_result_untouched(self) -> None:
        loader, auditor = self._audit(None)
        with loader, auditor:
            self.assertIsNone(self.runner._audit_daily_coverage_sync(now=NOW))
        self.assertIsNone(self.runner.snapshot()['daily_coverage'])

    def test_a_failing_audit_never_raises_into_the_tick(self) -> None:
        with patch(f'{AUDIT_MODULE}.load_recent_trade_dates', side_effect=RuntimeError('db down')), \
                self.assertLogs('app.eod.runner', level='WARNING') as captured:
            asyncio.run(self.runner._run_daily_coverage_audit())

        self.assertTrue(any('Daily coverage audit failed' in line or 'blocking step failed' in line for line in captured.output))
        self.assertIsNotNone(self.runner._last_daily_coverage_audit_at)

    def test_a_successful_step_records_the_attempt_time_and_result(self) -> None:
        loader, auditor = self._audit(_report())
        with loader, auditor:
            asyncio.run(self.runner._run_daily_coverage_audit())

        self.assertIsNotNone(self.runner._last_daily_coverage_audit_at)
        self.assertEqual(self.runner.snapshot()['daily_coverage']['status'], 'ok')


if __name__ == '__main__':
    unittest.main()
