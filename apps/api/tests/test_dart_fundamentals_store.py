import unittest
from datetime import date, datetime, timedelta, timezone
from uuid import uuid4

from app.dart.fundamentals_store import DartFundamentalsRepository
from app.db import session_scope
from app.db_models import DartDisclosureEvent, DartFundamentalsCaptureRun, DartFundamentalsSnapshot
from sqlalchemy import delete, select


class DartFundamentalsRepositoryTests(unittest.TestCase):
    def setUp(self):
        self.repository = DartFundamentalsRepository()
        self.created_run_ids: list[int] = []
        self.created_event_ids: list[int] = []
        self.created_snapshot_ids: list[int] = []

    def tearDown(self):
        if not self.created_run_ids and not self.created_event_ids and not self.created_snapshot_ids:
            return
        with session_scope() as session:
            if self.created_snapshot_ids:
                session.execute(
                    delete(DartFundamentalsSnapshot).where(DartFundamentalsSnapshot.id.in_(self.created_snapshot_ids))
                )
            if self.created_event_ids:
                session.execute(
                    delete(DartDisclosureEvent).where(DartDisclosureEvent.id.in_(self.created_event_ids))
                )
            if self.created_run_ids:
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

    def _create_event_snapshot(
        self,
        *,
        stock_code: str,
        receipt_date: date,
        capture_status: str,
        business_year: int,
        report_code: str = '11011',
        fs_div: str = 'CFS',
    ) -> tuple[int, int]:
        suffix = uuid4().hex[:16]
        rcept_no = f'T{suffix}'
        corp_code = f'C{suffix[:8]}'
        with session_scope() as session:
            event = DartDisclosureEvent(
                rcept_no=rcept_no,
                corp_code=corp_code,
                corp_name=f'Test Corp {stock_code}',
                stock_code=stock_code,
                corp_cls='Y',
                report_nm='Annual Report (2025.12)',
                filing_category='annual_report',
                is_financial_report=True,
                receipt_date=receipt_date,
                payload={'rcept_no': rcept_no},
                fundamentals_capture_status=capture_status,
            )
            session.add(event)
            session.flush()

            snapshot = DartFundamentalsSnapshot(
                disclosure_event_id=event.id,
                capture_run_id=None,
                rcept_no=rcept_no,
                corp_code=corp_code,
                stock_code=stock_code,
                corp_name=f'Test Corp {stock_code}',
                business_year=business_year,
                report_code=report_code,
                filing_category='annual_report',
                fs_div=fs_div,
                statement_type='financial_statement',
                period_type='annual',
                period_end_month=12,
                currency='KRW',
                source_status='000',
                source_message='OK',
                request_payload={},
                response_payload={},
            )
            session.add(snapshot)
            session.flush()

            event_id = event.id
            snapshot_id = snapshot.id

        self.created_event_ids.append(event_id)
        self.created_snapshot_ids.append(snapshot_id)
        return event_id, snapshot_id

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

    def test_list_symbol_fundamentals_states_ignores_failed_capture_snapshots(self):
        covered_stock_code = f'T{uuid4().hex[:10]}'
        failed_only_stock_code = f'T{uuid4().hex[:10]}'
        self._create_event_snapshot(
            stock_code=covered_stock_code,
            receipt_date=date(2026, 3, 20),
            capture_status='captured',
            business_year=2025,
        )
        self._create_event_snapshot(
            stock_code=covered_stock_code,
            receipt_date=date(2026, 4, 10),
            capture_status='failed',
            business_year=2026,
        )
        self._create_event_snapshot(
            stock_code=failed_only_stock_code,
            receipt_date=date(2026, 4, 11),
            capture_status='failed',
            business_year=2026,
        )

        states = self.repository.list_symbol_fundamentals_states(
            stock_codes=[covered_stock_code, failed_only_stock_code],
            fs_div='CFS',
        )

        self.assertTrue(states[covered_stock_code]['has_fundamentals'])
        self.assertEqual(states[covered_stock_code]['latest_fundamentals_receipt_date'], '2026-03-20')
        self.assertEqual(
            states[covered_stock_code]['latest_snapshot']['disclosure']['fundamentals_capture_status'],
            'captured',
        )
        self.assertFalse(states[failed_only_stock_code]['has_fundamentals'])
        self.assertIsNone(states[failed_only_stock_code]['latest_fundamentals_receipt_date'])
        self.assertIsNone(states[failed_only_stock_code]['latest_snapshot'])
