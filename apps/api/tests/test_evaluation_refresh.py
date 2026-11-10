import tempfile
import unittest
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock
from zoneinfo import ZoneInfo

from app.agent_context import evaluation_refresh as refresh
from app.agent_context.evaluation_refresh import refresh_signal_decider_evaluations_if_due
from app.agent_context.signal_decider_loop_service import SIGNAL_DECIDER_EVALUATION_PREREQUISITE_STAGE_IDS

KST = ZoneInfo('Asia/Seoul')


def _market(state, latest='2026-11-06'):
    return SimpleNamespace(state=state, latest_valid_trading_date=latest)


class EvaluationRefreshTests(unittest.TestCase):
    def setUp(self):
        refresh._last_attempt_at.clear()
        self._dir = tempfile.TemporaryDirectory()
        self.addCleanup(self._dir.cleanup)
        self.root = Path(self._dir.name)

    def _run(self, market, now, compute):
        return refresh_signal_decider_evaluations_if_due(
            market=market, now=now, compute=compute, marker_dir=self.root
        )

    def test_runs_once_per_session_when_idle(self):
        compute = MagicMock(return_value={'summary': {'status': 'completed', 'evaluations_upserted': 12}})

        first = self._run(_market('WEEKEND'), datetime(2026, 11, 7, 9, 0, tzinfo=KST), compute)
        second = self._run(_market('WEEKEND'), datetime(2026, 11, 7, 12, 0, tzinfo=KST), compute)

        self.assertEqual(first['status'], 'completed')
        self.assertEqual(second['reason'], 'already_refreshed')
        compute.assert_called_once_with('2026-11-06')

    def test_never_runs_while_the_market_is_live_or_before_the_evening_window(self):
        compute = MagicMock()
        for market, now in (
            (_market('OPEN'), datetime(2026, 11, 9, 10, 0, tzinfo=KST)),
            (_market('PREOPEN'), datetime(2026, 11, 9, 8, 30, tzinfo=KST)),
            (_market('CLOSED'), datetime(2026, 11, 9, 21, 0, tzinfo=KST)),
        ):
            with self.subTest(state=market.state):
                self.assertEqual(self._run(market, now, compute)['status'], 'skipped')
        compute.assert_not_called()

    def test_gated_attempt_is_retried_with_spacing(self):
        gated = MagicMock(return_value={'summary': {'status': 'blocked', 'gated': True}, 'blocked_stage_ids': ['technical_snapshots']})
        now = datetime(2026, 11, 9, 22, 40, tzinfo=KST)

        self.assertEqual(self._run(_market('CLOSED', '2026-11-09'), now, gated)['status'], 'gated')
        self.assertEqual(
            self._run(_market('CLOSED', '2026-11-09'), now.replace(minute=50), gated)['reason'],
            'attempt_interval',
        )
        ok = MagicMock(return_value={'summary': {'status': 'completed'}})
        later = datetime(2026, 11, 9, 23, 15, tzinfo=KST)
        self.assertEqual(self._run(_market('CLOSED', '2026-11-09'), later, ok)['status'], 'completed')

    def test_failed_or_partial_evaluations_remain_retryable(self):
        for day, status in enumerate(('failed', 'partial'), start=9):
            with self.subTest(status=status):
                market_date = f'2026-11-{day:02d}'
                compute = MagicMock(side_effect=[
                    {'summary': {'status': status, 'failed_count': 1}},
                    {'summary': {'status': 'completed'}},
                ])
                now = datetime(2026, 11, day, 22, 40, tzinfo=KST)

                first = self._run(_market('CLOSED', market_date), now, compute)
                self.assertEqual(first['status'], status)
                self.assertEqual(refresh._load_marker(self.root, market_date)['status'], status)

                later = now.replace(hour=23, minute=15)
                retry = self._run(_market('CLOSED', market_date), later, compute)
                self.assertEqual(retry['status'], 'completed')
                self.assertEqual(compute.call_count, 2)

    def test_old_completed_marker_with_failed_summary_does_not_hide_retry_debt(self):
        refresh._save_marker(self.root, '2026-11-06', {
            'schema_version': refresh.MARKER_SCHEMA_VERSION,
            'status': 'completed',
            'summary': {'status': 'failed', 'retry_debt_count': 1},
        })
        compute = MagicMock(return_value={'summary': {'status': 'completed'}})

        result = self._run(_market('WEEKEND'), datetime(2026, 11, 7, 9, 0, tzinfo=KST), compute)

        self.assertEqual(result['status'], 'completed')
        compute.assert_called_once_with('2026-11-06')

    def test_evaluation_gate_only_needs_price_derived_readiness(self):
        # Evaluations read decision snapshots and later price bars only.
        self.assertEqual(SIGNAL_DECIDER_EVALUATION_PREREQUISITE_STAGE_IDS, ('technical_snapshots',))

    def test_watch_history_is_saved_independently_of_a_gated_evaluation(self):
        now = datetime(2026, 11, 9, 22, 40, tzinfo=KST)
        market = _market('CLOSED', '2026-11-09')
        gated = MagicMock(return_value={'summary': {'status': 'blocked', 'gated': True}})
        report = {'summary': {'status': 'completed', 'items': 1}, 'items': [
            {'symbol': '005930', 'simulation': {'gross_pnl_krw_per_share': -100}},
        ]}
        self.assertEqual(self._run(market, now, gated)['status'], 'gated')

        audit = MagicMock(return_value=report)
        result = refresh.refresh_decider_watch_audit_if_due(
            market=market, now=now, compute=audit, marker_dir=self.root,
        )
        saved = refresh._load_marker(self.root / 'watch-audit', '2026-11-09')
        self.assertEqual(result['status'], 'completed')
        self.assertEqual(saved['report'], report)
        self.assertEqual(refresh.refresh_decider_watch_audit_if_due(
            market=market, now=now, compute=audit, marker_dir=self.root,
        )['reason'], 'already_refreshed')
        audit.assert_called_once_with('2026-11-09')


if __name__ == '__main__':
    unittest.main()
