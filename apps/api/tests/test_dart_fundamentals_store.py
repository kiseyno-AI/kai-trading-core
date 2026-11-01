import unittest
from contextlib import contextmanager
from datetime import date, datetime, timedelta, timezone
from unittest.mock import patch
from uuid import uuid4

from app.agent_context.reuse_service import AgentContextReuseService
from app.dart.fundamentals_store import (
    DartFundamentalsRepository,
    FundamentalsCaptureCandidateDriftError,
    LegacyCfsRecoveryCandidateDriftError,
    eligible_fundamentals_fs_divs,
    is_usable_fundamentals_source_status,
)
from app.db import session_scope
from app.db_models import (
    DartDisclosureEvent,
    DartFundamentalsCaptureRun,
    DartFundamentalsHighlight,
    DartFundamentalsRefreshRun,
    DartFundamentalsRow,
    DartFundamentalsSnapshot,
)
from sqlalchemy import delete, select


class FundamentalsSourceStatusTests(unittest.TestCase):
    def test_only_success_status_is_eligible_for_factor_inputs(self):
        self.assertTrue(is_usable_fundamentals_source_status('000'))
        self.assertFalse(is_usable_fundamentals_source_status('013'))
        self.assertFalse(is_usable_fundamentals_source_status(None))

    def test_cfs_reads_allow_ofs_fallback_without_weakening_explicit_ofs_reads(self):
        self.assertEqual(eligible_fundamentals_fs_divs('CFS'), ('CFS', 'OFS'))
        self.assertEqual(eligible_fundamentals_fs_divs('OFS'), ('OFS',))

    def test_default_stored_read_query_requires_successful_captured_cfs_or_ofs(self):
        statements = []

        class EmptyResult:
            def all(self):
                return []

        class RecordingSession:
            def execute(self, statement):
                statements.append(statement)
                return EmptyResult()

        @contextmanager
        def fake_session_scope():
            yield RecordingSession()

        repository = DartFundamentalsRepository()
        with patch('app.dart.fundamentals_store.session_scope', fake_session_scope):
            self.assertEqual(
                repository.list_stored_fundamentals_summary(stock_codes=['005930']),
                [],
            )

        self.assertEqual(len(statements), 1)
        compiled = statements[0].compile()
        parameter_values = list(compiled.params.values())
        self.assertIn(['CFS', 'OFS'], parameter_values)
        self.assertIn('000', parameter_values)
        self.assertIn('captured', parameter_values)
        compiled_sql = str(compiled)
        self.assertIn('CASE WHEN', compiled_sql)
        order_by_sql = compiled_sql.split(' ORDER BY ', 1)[1]
        self.assertLess(
            order_by_sql.index('receipt_date DESC'),
            order_by_sql.index('rcept_no DESC'),
        )
        self.assertLess(
            order_by_sql.index('rcept_no DESC'),
            order_by_sql.index('CASE WHEN'),
        )


class DartFundamentalsRepositoryTests(unittest.TestCase):
    def setUp(self):
        self.repository = DartFundamentalsRepository()
        self.created_run_ids: list[int] = []
        self.created_refresh_run_ids: list[int] = []
        self.created_event_ids: list[int] = []
        self.created_snapshot_ids: list[int] = []

    def tearDown(self):
        if (
            not self.created_run_ids
            and not self.created_refresh_run_ids
            and not self.created_event_ids
            and not self.created_snapshot_ids
        ):
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
            if self.created_refresh_run_ids:
                session.execute(
                    delete(DartFundamentalsRefreshRun).where(
                        DartFundamentalsRefreshRun.id.in_(self.created_refresh_run_ids)
                    )
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

    def _create_refresh_run(self, *, trigger: str) -> int:
        run_id = self.repository.create_refresh_run(
            market_date=date(2026, 4, 14),
            trigger=trigger,
            fs_div='CFS',
            force=False,
            symbol_limit=1,
            symbol_offset=0,
            symbols_total=1,
            run_metadata=None,
        )
        self.created_refresh_run_ids.append(run_id)
        return run_id

    def _create_event_snapshot(
        self,
        *,
        stock_code: str,
        receipt_date: date | None,
        capture_status: str,
        business_year: int,
        report_code: str = '11011',
        fs_div: str = 'CFS',
        source_status: str | None = '000',
        request_payload: dict | None = None,
        rcept_no: str | None = None,
        disclosure_event_id: int | None = None,
    ) -> tuple[int, int]:
        suffix = uuid4().hex[:16]
        resolved_rcept_no = rcept_no or f'T{suffix}'
        corp_code = f'C{suffix[:8]}'
        with session_scope() as session:
            if disclosure_event_id is None:
                event = DartDisclosureEvent(
                    rcept_no=resolved_rcept_no,
                    corp_code=corp_code,
                    corp_name=f'Test Corp {stock_code}',
                    stock_code=stock_code,
                    corp_cls='Y',
                    report_nm='Annual Report (2025.12)',
                    filing_category='annual_report',
                    is_financial_report=True,
                    receipt_date=receipt_date,
                    payload={'rcept_no': resolved_rcept_no},
                    fundamentals_capture_status=capture_status,
                )
                session.add(event)
                session.flush()
                self.created_event_ids.append(event.id)
            else:
                event = session.get(DartDisclosureEvent, disclosure_event_id)
                if event is None:
                    raise AssertionError(
                        f'Missing test disclosure event {disclosure_event_id}'
                    )

            snapshot = DartFundamentalsSnapshot(
                disclosure_event_id=event.id,
                capture_run_id=None,
                rcept_no=event.rcept_no,
                corp_code=event.corp_code,
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
                source_status=source_status,
                source_message='OK',
                request_payload=request_payload or {},
                response_payload={},
            )
            session.add(snapshot)
            session.flush()

            event_id = event.id
            snapshot_id = snapshot.id

        self.created_snapshot_ids.append(snapshot_id)
        return event_id, snapshot_id

    def test_legacy_cfs_no_data_recovery_candidates_are_latest_exact_and_unproven(self):
        recoverable = f'T{uuid4().hex[:10]}'.upper()
        newer_success = f'T{uuid4().hex[:10]}'.upper()
        already_fallback = f'T{uuid4().hex[:10]}'.upper()
        terminal_proof = f'T{uuid4().hex[:10]}'.upper()

        recoverable_event_id, recoverable_snapshot_id = (
            self._create_event_snapshot(
                stock_code=recoverable,
                receipt_date=date(2026, 3, 18),
                capture_status='captured',
                business_year=2025,
                source_status='013',
            )
        )
        self._create_event_snapshot(
            stock_code=newer_success,
            receipt_date=date(2026, 3, 18),
            capture_status='captured',
            business_year=2024,
            source_status='013',
        )
        self._create_event_snapshot(
            stock_code=newer_success,
            receipt_date=date(2026, 3, 19),
            capture_status='captured',
            business_year=2025,
            source_status='000',
        )
        self._create_event_snapshot(
            stock_code=already_fallback,
            receipt_date=date(2026, 3, 18),
            capture_status='captured',
            business_year=2025,
            source_status='013',
            request_payload={
                'requested_fs_div': 'CFS',
                'fallback_from_status': '013',
            },
        )
        terminal_event_id, _ = self._create_event_snapshot(
            stock_code=terminal_proof,
            receipt_date=date(2026, 3, 18),
            capture_status='no_data',
            business_year=2025,
            source_status='013',
        )
        with session_scope() as session:
            terminal_event = session.get(
                DartDisclosureEvent,
                terminal_event_id,
            )
            terminal_event.fundamentals_last_error = (
                'CFS [013] no consolidated statements; '
                'OFS [013]: no standalone statements'
            )

        candidates = (
            self.repository.list_legacy_cfs_no_data_recovery_candidates(
                stock_codes=[
                    recoverable,
                    newer_success,
                    already_fallback,
                    terminal_proof,
                ],
                receipt_date_from=date(2026, 1, 1),
                receipt_date_to=date(2026, 10, 30),
            )
        )

        self.assertEqual(len(candidates), 1)
        self.assertEqual(candidates[0]['id'], recoverable_event_id)
        self.assertEqual(candidates[0]['stock_code'], recoverable)
        self.assertEqual(
            candidates[0]['legacy_snapshot_id'],
            recoverable_snapshot_id,
        )
        self.assertEqual(candidates[0]['legacy_source_status'], '013')

    def test_legacy_recovery_excludes_event_snapshot_stock_code_drift(self):
        original_stock_code = f'T{uuid4().hex[:10]}'.upper()
        event_id, _ = self._create_event_snapshot(
            stock_code=original_stock_code,
            receipt_date=date(2026, 3, 18),
            capture_status='captured',
            business_year=2025,
            source_status='013',
        )
        with session_scope() as session:
            event = session.get(DartDisclosureEvent, event_id)
            event.stock_code = f'T{uuid4().hex[:10]}'.upper()

        candidates = (
            self.repository.list_legacy_cfs_no_data_recovery_candidates(
                stock_codes=[original_stock_code],
                receipt_date_from=date(2026, 1, 1),
                receipt_date_to=date(2026, 10, 30),
            )
        )

        self.assertEqual(candidates, [])

    def test_legacy_recovery_newer_snapshot_outside_window_tombstones_old_013(self):
        stock_code = f'T{uuid4().hex[:10]}'.upper()
        self._create_event_snapshot(
            stock_code=stock_code,
            receipt_date=date(2026, 10, 30),
            capture_status='captured',
            business_year=2025,
            source_status='013',
        )
        self._create_event_snapshot(
            stock_code=stock_code,
            receipt_date=date(2026, 10, 31),
            capture_status='captured',
            business_year=2025,
            source_status='000',
        )

        candidates = (
            self.repository.list_legacy_cfs_no_data_recovery_candidates(
                stock_codes=[stock_code],
                receipt_date_from=date(2026, 1, 1),
                receipt_date_to=date(2026, 10, 30),
            )
        )

        self.assertEqual(candidates, [])

    def test_atomic_snapshot_publication_rolls_back_when_event_success_write_fails(self):
        stock_code = f'T{uuid4().hex[:10]}'.upper()
        event_id, _ = self._create_event_snapshot(
            stock_code=stock_code,
            receipt_date=date(2026, 3, 18),
            capture_status='failed',
            business_year=2025,
            source_status='013',
        )
        run_id = self._create_run(trigger='atomic_publication_test')
        with session_scope() as session:
            event = session.get(DartDisclosureEvent, event_id)
            event_identity = {
                'rcept_no': event.rcept_no,
                'corp_code': event.corp_code,
                'corp_name': event.corp_name,
                'stock_code': event.stock_code,
                'filing_category': event.filing_category,
            }

        with patch.object(
            self.repository,
            '_mark_event_capture_success_in_session',
            side_effect=RuntimeError('simulated event status failure'),
        ):
            with self.assertRaisesRegex(
                RuntimeError,
                'simulated event status failure',
            ):
                self.repository.replace_snapshot_dataset_and_mark_event_success(
                    event_id=event_id,
                    capture_run_id=run_id,
                    snapshot_payload={
                        **event_identity,
                        'business_year': 2025,
                        'report_code': '11011',
                        'fs_div': 'OFS',
                        'statement_type': 'separate',
                        'period_type': 'annual',
                        'period_end_month': 12,
                        'currency': 'KRW',
                        'source_status': '000',
                        'source_message': 'OK',
                        'request_payload': {
                            'requested_fs_div': 'CFS',
                            'fallback_from_status': '013',
                        },
                        'response_payload': {'status': '000', 'list': []},
                    },
                    row_payloads=[],
                    highlight_payloads=[],
                )

        with session_scope() as session:
            event = session.get(DartDisclosureEvent, event_id)
            ofs_snapshot = session.execute(
                select(DartFundamentalsSnapshot).where(
                    DartFundamentalsSnapshot.disclosure_event_id == event_id,
                    DartFundamentalsSnapshot.fs_div == 'OFS',
                )
            ).scalar_one_or_none()
            self.assertEqual(event.fundamentals_capture_status, 'failed')
            self.assertIsNone(ofs_snapshot)

    def test_force_refresh_failure_preserves_existing_usable_capture(self):
        stock_code = f'T{uuid4().hex[:10]}'.upper()
        event_id, snapshot_id = self._create_event_snapshot(
            stock_code=stock_code,
            receipt_date=date(2026, 3, 18),
            capture_status='captured',
            business_year=2025,
            source_status='000',
        )
        expected_event = {
            'id': event_id,
            'fundamentals_capture_status': 'captured',
            'fundamentals_capture_attempts': 0,
        }

        published = self.repository.mark_event_capture_failed(
            event_id,
            error_message='provider temporarily unavailable',
            expected_event=expected_event,
        )
        terminal_published = self.repository.mark_event_capture_terminal(
            event_id,
            capture_status='no_data',
            error_message='provider returned no data',
            expected_event=expected_event,
        )

        self.assertFalse(published)
        self.assertFalse(terminal_published)
        with session_scope() as session:
            event = session.get(DartDisclosureEvent, event_id)
            snapshot = session.get(DartFundamentalsSnapshot, snapshot_id)
            self.assertEqual(event.fundamentals_capture_status, 'captured')
            self.assertEqual(event.fundamentals_capture_attempts, 0)
            self.assertIsNone(event.fundamentals_last_error)
            self.assertEqual(snapshot.source_status, '000')

    def test_unknown_commit_readback_requires_exact_publication_identity(self):
        stock_code = f'T{uuid4().hex[:10]}'.upper()
        event_id, _ = self._create_event_snapshot(
            stock_code=stock_code,
            receipt_date=date(2026, 3, 18),
            capture_status='failed',
            business_year=2025,
            source_status='013',
        )
        run_id = self._create_run(trigger='unknown_commit_readback_test')
        with session_scope() as session:
            event = session.get(DartDisclosureEvent, event_id)
            event_identity = {
                'rcept_no': event.rcept_no,
                'corp_code': event.corp_code,
                'corp_name': event.corp_name,
                'stock_code': event.stock_code,
                'filing_category': event.filing_category,
            }

        snapshot_id = (
            self.repository.replace_snapshot_dataset_and_mark_event_success(
                event_id=event_id,
                capture_run_id=run_id,
                snapshot_payload={
                    **event_identity,
                    'business_year': 2025,
                    'report_code': '11011',
                    'fs_div': 'CFS',
                    'statement_type': 'consolidated',
                    'period_type': 'annual',
                    'period_end_month': 12,
                    'currency': 'KRW',
                    'source_status': '000',
                    'source_message': 'OK',
                    'request_payload': {},
                    'response_payload': {'status': '000'},
                },
                row_payloads=[],
                highlight_payloads=[],
            )
        )

        self.assertEqual(
            self.repository.find_committed_snapshot_publication(
                event_id=event_id,
                capture_run_id=run_id,
                rcept_no=event_identity['rcept_no'],
                business_year=2025,
                report_code='11011',
                fs_div='CFS',
            ),
            snapshot_id,
        )
        self.assertIsNone(
            self.repository.find_committed_snapshot_publication(
                event_id=event_id,
                capture_run_id=run_id + 1,
                rcept_no=event_identity['rcept_no'],
                business_year=2025,
                report_code='11011',
                fs_div='CFS',
            )
        )

    def test_stale_failure_cannot_overwrite_concurrent_capture_success(self):
        stock_code = f'T{uuid4().hex[:10]}'.upper()
        event_id, _ = self._create_event_snapshot(
            stock_code=stock_code,
            receipt_date=date(2026, 3, 18),
            capture_status='pending',
            business_year=2025,
            source_status='000',
        )
        stale_candidate = {
            'id': event_id,
            'fundamentals_capture_status': 'pending',
            'fundamentals_capture_attempts': 0,
        }
        self.repository.mark_event_capture_success(event_id)

        with self.assertRaises(FundamentalsCaptureCandidateDriftError):
            self.repository.mark_event_capture_failed(
                event_id,
                error_message='late provider failure',
                expected_event=stale_candidate,
            )

        with session_scope() as session:
            event = session.get(DartDisclosureEvent, event_id)
            self.assertEqual(event.fundamentals_capture_status, 'captured')
            self.assertEqual(event.fundamentals_capture_attempts, 1)
            self.assertIsNone(event.fundamentals_last_error)

    def test_conditional_failure_write_rejects_candidate_coordinate_drift(self):
        stock_code = f'{int(uuid4().hex[:8], 16) % 1_000_000:06d}'
        event_id, _ = self._create_event_snapshot(
            stock_code=stock_code,
            receipt_date=date(2026, 3, 18),
            capture_status='captured',
            business_year=2025,
            source_status='013',
        )
        candidates = (
            self.repository.list_legacy_cfs_no_data_recovery_candidates(
                stock_codes=[stock_code],
                receipt_date_from=date(2026, 1, 1),
                receipt_date_to=date(2026, 10, 30),
                event_ids=[event_id],
            )
        )
        self.assertEqual(len(candidates), 1)
        with session_scope() as session:
            event = session.get(DartDisclosureEvent, event_id)
            event.corp_code = f'C{uuid4().hex[:8]}'

        with self.assertRaises(LegacyCfsRecoveryCandidateDriftError):
            self.repository.mark_event_capture_failed(
                event_id,
                error_message='provider temporarily unavailable',
                expected_recovery_candidate=candidates[0],
            )

        with session_scope() as session:
            event = session.get(DartDisclosureEvent, event_id)
            self.assertEqual(event.fundamentals_capture_status, 'captured')
            self.assertEqual(event.fundamentals_capture_attempts, 0)
            self.assertIsNone(event.fundamentals_last_error)

    def _create_financial_event(
        self,
        *,
        stock_code: str,
        receipt_date: date,
        capture_status: str,
        attempts: int = 0,
    ) -> int:
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
                fundamentals_capture_attempts=attempts,
            )
            session.add(event)
            session.flush()
            event_id = event.id

        self.created_event_ids.append(event_id)
        return event_id

    def test_list_stored_fundamentals_summary_excludes_snapshots_at_or_after_cutoff(self):
        stock_code = f'T{uuid4().hex[:10]}'
        _, before_snapshot_id = self._create_event_snapshot(
            stock_code=stock_code,
            receipt_date=date(2026, 4, 7),
            capture_status='captured',
            business_year=2024,
        )
        _, at_cutoff_snapshot_id = self._create_event_snapshot(
            stock_code=stock_code,
            receipt_date=date(2026, 4, 8),
            capture_status='captured',
            business_year=2025,
        )
        cutoff = datetime(2026, 4, 8, 6, 30, tzinfo=timezone.utc)
        with session_scope() as session:
            before_snapshot = session.get(
                DartFundamentalsSnapshot,
                before_snapshot_id,
            )
            at_cutoff_snapshot = session.get(
                DartFundamentalsSnapshot,
                at_cutoff_snapshot_id,
            )
            before_snapshot.captured_at = cutoff - timedelta(microseconds=1)
            at_cutoff_snapshot.captured_at = cutoff

        rows = self.repository.list_stored_fundamentals_summary(
            stock_codes=[stock_code],
            fs_div='CFS',
            captured_before=cutoff,
        )

        self.assertEqual(
            [row['id'] for row in rows],
            [before_snapshot_id],
        )

    def test_default_stored_fundamentals_read_prefers_successful_cfs_and_falls_back_to_ofs(self):
        cfs_preferred_stock_code = f'T{uuid4().hex[:10]}'
        fallback_stock_code = f'T{uuid4().hex[:10]}'
        preferred_event_id, preferred_cfs_id = self._create_event_snapshot(
            stock_code=cfs_preferred_stock_code,
            receipt_date=date(2026, 4, 7),
            capture_status='captured',
            business_year=2025,
            fs_div='CFS',
        )
        self._create_event_snapshot(
            stock_code=cfs_preferred_stock_code,
            receipt_date=date(2026, 4, 7),
            capture_status='captured',
            business_year=2025,
            fs_div='OFS',
            disclosure_event_id=preferred_event_id,
        )
        self._create_event_snapshot(
            stock_code=fallback_stock_code,
            receipt_date=date(2026, 4, 8),
            capture_status='captured',
            business_year=2025,
            fs_div='CFS',
            source_status='013',
        )
        _, fallback_ofs_id = self._create_event_snapshot(
            stock_code=fallback_stock_code,
            receipt_date=date(2026, 4, 8),
            capture_status='captured',
            business_year=2025,
            fs_div='OFS',
        )
        self._create_event_snapshot(
            stock_code=fallback_stock_code,
            receipt_date=date(2026, 4, 9),
            capture_status='failed',
            business_year=2026,
            fs_div='CFS',
        )

        rows = self.repository.list_stored_fundamentals_summary(
            stock_codes=[cfs_preferred_stock_code, fallback_stock_code],
        )

        self.assertEqual(
            {row['id'] for row in rows},
            {preferred_cfs_id, fallback_ofs_id},
        )
        self.assertEqual({row['source_status'] for row in rows}, {'000'})
        self.assertEqual(
            {row['disclosure']['fundamentals_capture_status'] for row in rows},
            {'captured'},
        )

    def test_default_stored_read_prefers_newer_same_day_correction_before_cfs(self):
        stock_code = f'T{uuid4().hex[:10]}'
        self._create_event_snapshot(
            stock_code=stock_code,
            receipt_date=date(2026, 4, 7),
            capture_status='captured',
            business_year=2025,
            fs_div='CFS',
            rcept_no='20260407000001',
        )
        _, corrected_ofs_id = self._create_event_snapshot(
            stock_code=stock_code,
            receipt_date=date(2026, 4, 7),
            capture_status='captured',
            business_year=2025,
            fs_div='OFS',
            rcept_no='20260407000002',
        )

        rows = self.repository.list_stored_fundamentals_summary(
            stock_codes=[stock_code],
            fs_div='CFS',
        )

        self.assertEqual([row['id'] for row in rows], [corrected_ofs_id])
        self.assertEqual(rows[0]['fs_div'], 'OFS')

    def test_summary_loads_detail_only_for_latest_snapshot_per_symbol(self):
        stock_code = f'T{uuid4().hex[:10]}'
        _, older_snapshot_id = self._create_event_snapshot(
            stock_code=stock_code,
            receipt_date=date(2025, 4, 7),
            capture_status='captured',
            business_year=2024,
        )
        _, latest_snapshot_id = self._create_event_snapshot(
            stock_code=stock_code,
            receipt_date=date(2026, 4, 7),
            capture_status='captured',
            business_year=2025,
        )
        _, future_snapshot_id = self._create_event_snapshot(
            stock_code=stock_code,
            receipt_date=date(2027, 4, 7),
            capture_status='captured',
            business_year=2026,
        )
        with session_scope() as session:
            for snapshot_id, amount in (
                (older_snapshot_id, 100),
                (latest_snapshot_id, 200),
                (future_snapshot_id, 300),
            ):
                session.add(
                    DartFundamentalsHighlight(
                        snapshot_id=snapshot_id,
                        metric_key='revenue',
                        metric_type='amount',
                        current_amount=amount,
                    )
                )
                session.add(
                    DartFundamentalsRow(
                        snapshot_id=snapshot_id,
                        row_key='revenue',
                        current_amount=amount,
                        row_order=1,
                        payload={},
                    )
                )

        summary_rows = self.repository.list_stored_fundamentals_summary(
            stock_codes=[stock_code],
            fs_div='CFS',
            receipt_date_to=date(2026, 4, 7),
        )

        self.assertEqual(
            [row['id'] for row in summary_rows],
            [latest_snapshot_id, older_snapshot_id],
        )
        self.assertEqual(summary_rows[0]['statement_row_count'], 1)
        self.assertEqual(
            summary_rows[0]['highlights'][0]['current_amount'],
            200,
        )
        self.assertEqual(summary_rows[1]['statement_row_count'], 0)
        self.assertEqual(summary_rows[1]['highlights'], [])

        full_rows = self.repository.list_stored_fundamentals(
            stock_codes=[stock_code],
            fs_div='CFS',
        )
        full_by_id = {row['id']: row for row in full_rows}
        self.assertEqual(
            full_by_id[older_snapshot_id]['highlights'][0]['current_amount'],
            100,
        )
        self.assertEqual(
            len(full_by_id[older_snapshot_id]['statement_rows']),
            1,
        )

    def test_summary_receipt_cutoff_preserves_direct_backfill_without_date(self):
        stock_code = f'T{uuid4().hex[:10]}'
        self._create_event_snapshot(
            stock_code=stock_code,
            receipt_date=date(2026, 3, 18),
            capture_status='captured',
            business_year=2025,
            rcept_no='20260318000001',
        )
        _, backfill_snapshot_id = self._create_event_snapshot(
            stock_code=stock_code,
            receipt_date=None,
            capture_status='captured',
            business_year=2025,
            rcept_no=f'db-{stock_code}-2025-11011-CFS',
        )
        with session_scope() as session:
            session.add(
                DartFundamentalsHighlight(
                    snapshot_id=backfill_snapshot_id,
                    metric_key='revenue',
                    metric_type='amount',
                    current_amount=200,
                )
            )
            session.add(
                DartFundamentalsRow(
                    snapshot_id=backfill_snapshot_id,
                    row_key='revenue',
                    current_amount=200,
                    row_order=1,
                    payload={},
                )
            )

        rows = self.repository.list_stored_fundamentals_summary(
            stock_codes=[stock_code],
            fs_div='CFS',
            receipt_date_to=date(2026, 4, 8),
        )

        self.assertEqual([row['id'] for row in rows], [backfill_snapshot_id])
        self.assertIsNone(rows[0]['disclosure']['receipt_date'])
        self.assertEqual(rows[0]['statement_row_count'], 1)
        self.assertEqual(rows[0]['highlights'][0]['current_amount'], 200)

    def test_retry_fingerprint_uses_same_canonical_period_snapshots_as_dispatch(
        self,
    ):
        stock_code = f'T{uuid4().hex[:10]}'.upper()
        self._create_event_snapshot(
            stock_code=stock_code,
            receipt_date=date(2026, 4, 7),
            capture_status='captured',
            business_year=2025,
            report_code='11011',
            rcept_no='20260407000001',
        )
        _, latest_snapshot_id = self._create_event_snapshot(
            stock_code=stock_code,
            receipt_date=date(2026, 4, 7),
            capture_status='captured',
            business_year=2025,
            report_code='11011',
            rcept_no='20260407000002',
        )

        canonical_rows = self.repository.list_stored_fundamentals_summary(
            stock_codes=[stock_code],
            fs_div='CFS',
        )
        fingerprint_payload = (
            AgentContextReuseService()
            .build_fundamental_input_fingerprint_payload(
                market_date=date(2026, 4, 14),
                symbol=stock_code,
                fs_div='CFS',
                disclosures_limit=0,
            )
        )
        fingerprint_fundamentals = fingerprint_payload['input'][
            'finalized_dispatch_context'
        ]['fundamentals']

        self.assertEqual(
            [row['id'] for row in canonical_rows],
            [latest_snapshot_id],
        )
        self.assertEqual(
            fingerprint_fundamentals['history_summary']['entries'],
            len(canonical_rows),
        )
        self.assertEqual(
            fingerprint_fundamentals['latest_snapshot']['filing'][
                'rcept_no'
            ],
            canonical_rows[0]['rcept_no'],
        )

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

        self.assertIn(stale_progress_run_id, failed_run_ids)
        self.assertIn(stale_no_progress_run_id, failed_run_ids)

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

    def test_list_capture_candidates_excludes_terminal_statuses_and_can_drain_oldest_first(self):
        stock_code = f'T{uuid4().hex[:10]}'
        old_retryable_event_id = self._create_financial_event(
            stock_code=stock_code,
            receipt_date=date(2026, 4, 9),
            capture_status='failed',
            attempts=0,
        )
        newer_pending_event_id = self._create_financial_event(
            stock_code=stock_code,
            receipt_date=date(2026, 4, 11),
            capture_status='pending',
            attempts=0,
        )
        attempted_retry_event_id = self._create_financial_event(
            stock_code=stock_code,
            receipt_date=date(2026, 4, 8),
            capture_status='failed',
            attempts=2,
        )
        self._create_financial_event(
            stock_code=stock_code,
            receipt_date=date(2026, 4, 12),
            capture_status='no_data',
        )
        self._create_financial_event(
            stock_code=stock_code,
            receipt_date=date(2026, 4, 13),
            capture_status='mapping_failed',
        )

        candidates = self.repository.list_capture_candidates(
            limit=None,
            stock_codes=[stock_code],
            receipt_date_from=date(2026, 4, 1),
            receipt_date_to=date(2026, 4, 30),
            oldest_first=True,
        )

        self.assertEqual(
            [candidate['id'] for candidate in candidates],
            [old_retryable_event_id, newer_pending_event_id, attempted_retry_event_id],
        )
        self.assertEqual(
            {candidate['fundamentals_capture_status'] for candidate in candidates},
            {'pending', 'failed'},
        )

    def test_list_symbol_fundamentals_states_uses_terminal_event_as_resolved_baseline(self):
        no_data_only_stock_code = f'T{uuid4().hex[:10]}'
        captured_then_terminal_stock_code = f'T{uuid4().hex[:10]}'
        self._create_financial_event(
            stock_code=no_data_only_stock_code,
            receipt_date=date(2026, 4, 12),
            capture_status='no_data',
        )
        self._create_event_snapshot(
            stock_code=captured_then_terminal_stock_code,
            receipt_date=date(2026, 3, 20),
            capture_status='captured',
            business_year=2025,
        )
        self._create_financial_event(
            stock_code=captured_then_terminal_stock_code,
            receipt_date=date(2026, 4, 13),
            capture_status='no_data',
        )

        states = self.repository.list_symbol_fundamentals_states(
            stock_codes=[no_data_only_stock_code, captured_then_terminal_stock_code],
            fs_div='CFS',
        )

        self.assertFalse(states[no_data_only_stock_code]['has_fundamentals'])
        self.assertEqual(
            states[no_data_only_stock_code]['latest_resolved_fundamentals_receipt_date'],
            '2026-04-12',
        )
        self.assertEqual(
            states[no_data_only_stock_code]['latest_terminal_fundamentals_event']['fundamentals_capture_status'],
            'no_data',
        )
        self.assertTrue(states[captured_then_terminal_stock_code]['has_fundamentals'])
        self.assertEqual(
            states[captured_then_terminal_stock_code]['latest_fundamentals_receipt_date'],
            '2026-03-20',
        )
        self.assertEqual(
            states[captured_then_terminal_stock_code]['latest_resolved_fundamentals_receipt_date'],
            '2026-04-13',
        )

    def test_fail_stale_refresh_runs_uses_last_progress_at_when_present(self):
        now = datetime.now(timezone.utc)
        fresh_run_id = self._create_refresh_run(trigger='test_refresh_fresh_progress')
        stale_progress_run_id = self._create_refresh_run(trigger='test_refresh_old_progress')
        stale_no_progress_run_id = self._create_refresh_run(trigger='test_refresh_no_progress')

        with session_scope() as session:
            fresh_run = session.execute(
                select(DartFundamentalsRefreshRun).where(DartFundamentalsRefreshRun.id == fresh_run_id)
            ).scalar_one()
            fresh_run.started_at = now - timedelta(hours=3)
            fresh_run.run_metadata = {'last_progress_at': (now - timedelta(minutes=5)).isoformat()}

            stale_progress_run = session.execute(
                select(DartFundamentalsRefreshRun).where(DartFundamentalsRefreshRun.id == stale_progress_run_id)
            ).scalar_one()
            stale_progress_run.started_at = now - timedelta(hours=3)
            stale_progress_run.run_metadata = {'last_progress_at': (now - timedelta(hours=2)).isoformat()}

            stale_no_progress_run = session.execute(
                select(DartFundamentalsRefreshRun).where(DartFundamentalsRefreshRun.id == stale_no_progress_run_id)
            ).scalar_one()
            stale_no_progress_run.started_at = now - timedelta(hours=3)
            stale_no_progress_run.run_metadata = None

        failed_run_ids = self.repository.fail_stale_refresh_runs(
            stale_before=now - timedelta(minutes=60),
            error_message='stale',
        )

        self.assertIn(stale_progress_run_id, failed_run_ids)
        self.assertIn(stale_no_progress_run_id, failed_run_ids)

        with session_scope() as session:
            fresh_run = session.execute(
                select(DartFundamentalsRefreshRun).where(DartFundamentalsRefreshRun.id == fresh_run_id)
            ).scalar_one()
            stale_progress_run = session.execute(
                select(DartFundamentalsRefreshRun).where(DartFundamentalsRefreshRun.id == stale_progress_run_id)
            ).scalar_one()
            stale_no_progress_run = session.execute(
                select(DartFundamentalsRefreshRun).where(DartFundamentalsRefreshRun.id == stale_no_progress_run_id)
            ).scalar_one()

            self.assertEqual(fresh_run.status, 'running')
            self.assertEqual(stale_progress_run.status, 'failed')
            self.assertEqual(stale_no_progress_run.status, 'failed')
            self.assertEqual(stale_progress_run.run_metadata['stale_failure_reason'], 'stale')
            self.assertIn('stale_failed_at', stale_progress_run.run_metadata)
