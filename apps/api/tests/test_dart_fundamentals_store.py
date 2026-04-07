import unittest
from datetime import datetime, timedelta, timezone

from app.dart.fundamentals_store import DartFundamentalsRepository
from app.db import session_scope
from app.db_models import DartFundamentalsCaptureRun
from sqlalchemy import delete, select


class DartFundamentalsRepositoryTests(unittest.TestCase):
    def setUp(self):
        self.repository = DartFundamentalsRepository()
        self.created_run_ids: list[int] = []

    def tearDown(self):
        if not self.created_run_ids:
            return
        with session_scope() as session:
            session.execute(
                delete(DartFundamentalsCaptureRun).where(DartFundamentalsCaptureRun.id.in_(self.created_run_ids))
            )

    def _create_run(self, *, trigger: str) -> int:
        run_id = self.repository.create_capture_run(
            trigger=trigger,
            limit_requested=1,
            force=False,
            fs_div='CFS',
            run_metadata=None,
        )
        self.created_run_ids.append(run_id)
        return run_id

    def test_fail_stale_capture_runs_uses_last_progress_at_when_present(self):
        now = datetime.now(timezone.utc)
        fresh_run_id = self._create_run(trigger='test_stale_fresh_progress')
        stale_progress_run_id = self._create_run(trigger='test_stale_old_progress')
        stale_no_progress_run_id = self._create_run(trigger='test_stale_no_progress')

        with session_scope() as session:
            fresh_run = session.execute(
                select(DartFundamentalsCaptureRun).where(DartFundamentalsCaptureRun.id == fresh_run_id)
            ).scalar_one()
            fresh_run.started_at = now - timedelta(hours=3)
            fresh_run.run_metadata = {'last_progress_at': (now - timedelta(minutes=5)).isoformat()}

            stale_progress_run = session.execute(
                select(DartFundamentalsCaptureRun).where(DartFundamentalsCaptureRun.id == stale_progress_run_id)
            ).scalar_one()
            stale_progress_run.started_at = now - timedelta(hours=3)
            stale_progress_run.run_metadata = {'last_progress_at': (now - timedelta(hours=2)).isoformat()}

            stale_no_progress_run = session.execute(
                select(DartFundamentalsCaptureRun).where(DartFundamentalsCaptureRun.id == stale_no_progress_run_id)
            ).scalar_one()
            stale_no_progress_run.started_at = now - timedelta(hours=3)
            stale_no_progress_run.run_metadata = None

        failed_run_ids = self.repository.fail_stale_capture_runs(
            stale_before=now - timedelta(minutes=60),
            error_message='stale',
        )

        self.assertCountEqual(failed_run_ids, [stale_progress_run_id, stale_no_progress_run_id])

        with session_scope() as session:
            fresh_run = session.execute(
                select(DartFundamentalsCaptureRun).where(DartFundamentalsCaptureRun.id == fresh_run_id)
            ).scalar_one()
            stale_progress_run = session.execute(
                select(DartFundamentalsCaptureRun).where(DartFundamentalsCaptureRun.id == stale_progress_run_id)
            ).scalar_one()
            stale_no_progress_run = session.execute(
                select(DartFundamentalsCaptureRun).where(DartFundamentalsCaptureRun.id == stale_no_progress_run_id)
            ).scalar_one()

            self.assertEqual(fresh_run.status, 'running')
            self.assertEqual(stale_progress_run.status, 'failed')
            self.assertEqual(stale_no_progress_run.status, 'failed')
