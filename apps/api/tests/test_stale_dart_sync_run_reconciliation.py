import unittest
from contextlib import contextmanager
from copy import deepcopy
from datetime import date, datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

from sqlalchemy import create_engine, select, text
from sqlalchemy.exc import DBAPIError, OperationalError
from sqlalchemy.orm import Session, sessionmaker

from app.db import engine as app_engine
from app.dart.stale_sync_run_reconciliation import (
    _DART_MAINTENANCE_TABLES,
    _acquire_dart_maintenance_locks,
    FROZEN_RECONCILIATION_MANIFEST,
    FROZEN_RECONCILIATION_MANIFEST_SHA256,
    NEVER_TARGET_RUN_IDS,
    RECONCILIATION_MARKER_KEY,
    TARGET_STATUS,
    StaleDartSyncRunReconciliation,
)
from app.db_models import (
    DartDisclosureEvent,
    DartDisclosureSyncRun,
    DartFundamentalsCaptureRun,
    DartFundamentalsRefreshRun,
)


class StaleDartSyncRunReconciliationTests(unittest.TestCase):
    def test_production_manifest_freezes_reviewed_exact_scope(self) -> None:
        self.assertEqual(
            FROZEN_RECONCILIATION_MANIFEST['target_run_ids'],
            [3, 327, 1147, 1148, 1149, 2038],
        )
        self.assertEqual(
            FROZEN_RECONCILIATION_MANIFEST['all_current_running_run_ids'],
            [3, 327, 1147, 1148, 1149, 2038],
        )
        self.assertEqual(
            FROZEN_RECONCILIATION_MANIFEST[
                'stale_started_at_max_inclusive'
            ],
            '2026-05-15T17:20:13.989031+09:00',
        )
        self.assertEqual(
            FROZEN_RECONCILIATION_MANIFEST_SHA256,
            '8bf109497496c493e2cab3b2cee9977718938e7d337b80514c258a636f738424',
        )
        self.assertEqual(NEVER_TARGET_RUN_IDS, (5594,))
        self.assertNotIn(
            5594,
            FROZEN_RECONCILIATION_MANIFEST['target_run_ids'],
        )

    def setUp(self) -> None:
        self.engine = create_engine('sqlite+pysqlite:///:memory:')
        DartDisclosureSyncRun.__table__.create(self.engine)
        DartDisclosureEvent.__table__.create(self.engine)
        DartFundamentalsCaptureRun.__table__.create(self.engine)
        DartFundamentalsRefreshRun.__table__.create(self.engine)
        self.session_factory = sessionmaker(bind=self.engine, future=True)
        self.manifest = self._test_manifest()
        self.reconciliation = StaleDartSyncRunReconciliation(self.manifest)
        self._seed_rows()

    def tearDown(self) -> None:
        self.engine.dispose()

    @staticmethod
    def _persisted(
        *,
        run_id: int,
        started_at: str,
        trigger: str,
    ) -> dict:
        return {
            'id': run_id,
            'trigger': trigger,
            'status': 'running',
            'window_start': '2026-06-01',
            'window_end': '2026-07-01',
            'page_size': 100,
            'pages_fetched': run_id,
            'events_seen': run_id + 1,
            'events_inserted': run_id + 2,
            'events_updated': run_id + 3,
            'financial_report_events': run_id + 4,
            'error_message': None,
            'run_metadata': {
                'scope': 'test',
                'requested_corporations': run_id,
            },
            'started_at': started_at,
            'completed_at': None,
        }

    def _test_manifest(self) -> dict:
        return {
            'schema_version': (
                'dart_disclosure_stale_sync_reconciliation_manifest_v1'
            ),
            'target_run_ids': [41, 42],
            'expected_status': 'running',
            'expected_completed_at': None,
            'stale_started_at_min': '2026-07-01T10:00:00+00:00',
            'stale_started_at_max_inclusive': (
                '2026-07-01T11:00:00+00:00'
            ),
            'all_current_running_run_ids': [41, 42],
            'excluded_current_running_run_ids': [],
            'target_rows': [
                {
                    'persisted': self._persisted(
                        run_id=41,
                        started_at='2026-07-01T10:00:00+00:00',
                        trigger='manual_api',
                    ),
                    'event_references': {
                        'first_seen_run_id_count': 1,
                        'last_seen_run_id_count': 0,
                        'both_first_and_last_count': 0,
                    },
                },
                {
                    'persisted': self._persisted(
                        run_id=42,
                        started_at='2026-07-01T11:00:00+00:00',
                        trigger='daily_incremental_refresh',
                    ),
                    'event_references': {
                        'first_seen_run_id_count': 0,
                        'last_seen_run_id_count': 1,
                        'both_first_and_last_count': 0,
                    },
                },
            ],
        }

    def _seed_rows(self) -> None:
        with self.session_factory.begin() as session:
            for expected in self.manifest['target_rows']:
                persisted = expected['persisted']
                session.add(
                    DartDisclosureSyncRun(
                        id=persisted['id'],
                        trigger=persisted['trigger'],
                        status=persisted['status'],
                        window_start=date.fromisoformat(
                            persisted['window_start']
                        ),
                        window_end=date.fromisoformat(
                            persisted['window_end']
                        ),
                        page_size=persisted['page_size'],
                        pages_fetched=persisted['pages_fetched'],
                        events_seen=persisted['events_seen'],
                        events_inserted=persisted['events_inserted'],
                        events_updated=persisted['events_updated'],
                        financial_report_events=persisted[
                            'financial_report_events'
                        ],
                        error_message=None,
                        run_metadata=deepcopy(persisted['run_metadata']),
                        started_at=datetime.fromisoformat(
                            persisted['started_at']
                        ),
                        completed_at=None,
                    )
                )
            session.add(
                DartDisclosureSyncRun(
                    id=5594,
                    trigger='targeted_active_backfill',
                    status='completed',
                    window_start=date(2025, 1, 1),
                    window_end=date(2026, 10, 30),
                    page_size=100,
                    pages_fetched=103,
                    events_seen=5342,
                    events_inserted=115,
                    events_updated=683,
                    financial_report_events=798,
                    error_message=None,
                    run_metadata={'scope': 'targeted_active_universe'},
                    started_at=datetime(
                        2026,
                        11,
                        1,
                        5,
                        0,
                        tzinfo=timezone.utc,
                    ),
                    completed_at=datetime(
                        2026,
                        11,
                        1,
                        6,
                        10,
                        tzinfo=timezone.utc,
                    ),
                )
            )
            session.add(
                DartDisclosureEvent(
                    rcept_no='20260701000001',
                    corp_code='00126380',
                    corp_name='Test Corp',
                    stock_code='005930',
                    report_nm='Annual Report',
                    filing_category='periodic_report',
                    is_financial_report=True,
                    payload={},
                    first_seen_run_id=41,
                    last_seen_run_id=42,
                )
            )

    @contextmanager
    def _session_scope(self, **_kwargs):
        session = self.session_factory()
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    def _run(self, *, apply: bool) -> dict:
        with patch(
            'app.dart.stale_sync_run_reconciliation.session_scope',
            self._session_scope,
        ):
            return self.reconciliation.reconcile(
                apply=apply,
                expected_manifest_sha256=(
                    self.reconciliation.manifest_sha256 if apply else None
                ),
            )

    def test_dry_run_apply_and_idempotent_replay_preserve_data(self) -> None:
        with self.session_factory() as session:
            live_before = self._live_state(session)

        dry_run = self._run(apply=False)

        self.assertEqual(dry_run['status'], 'ready')
        self.assertTrue(all(dry_run['invariants'].values()))
        self.assertEqual(dry_run['actual_stale_running_run_ids'], [41, 42])

        applied = self._run(apply=True)

        self.assertEqual(applied['status'], 'completed')
        self.assertEqual(applied['cas_rowcounts'], {41: 1, 42: 1})
        self.assertTrue(applied['postconditions']['counters_preserved'])
        self.assertTrue(
            applied['postconditions']['event_references_preserved']
        )
        self.assertTrue(
            applied['postconditions']['global_running_set_revalidated']
        )
        with self.session_factory() as session:
            rows = list(
                session.scalars(
                    select(DartDisclosureSyncRun)
                    .where(DartDisclosureSyncRun.id.in_((41, 42)))
                    .order_by(DartDisclosureSyncRun.id)
                )
            )
            event = session.scalar(select(DartDisclosureEvent))
            self.assertEqual([row.status for row in rows], [TARGET_STATUS] * 2)
            self.assertTrue(all(row.completed_at is not None for row in rows))
            self.assertEqual(
                [
                    (
                        row.pages_fetched,
                        row.events_seen,
                        row.events_inserted,
                        row.events_updated,
                        row.financial_report_events,
                    )
                    for row in rows
                ],
                [(41, 42, 43, 44, 45), (42, 43, 44, 45, 46)],
            )
            self.assertEqual(event.first_seen_run_id, 41)
            self.assertEqual(event.last_seen_run_id, 42)
            self.assertIn(RECONCILIATION_MARKER_KEY, rows[0].run_metadata)
            self.assertEqual(self._live_state(session), live_before)
            completed_at = rows[0].completed_at

        replay = self._run(apply=True)

        self.assertEqual(replay['status'], 'already_applied')
        with self.session_factory() as session:
            self.assertEqual(
                session.get(DartDisclosureSyncRun, 41).completed_at,
                completed_at,
            )
            self.assertEqual(self._live_state(session), live_before)

    @staticmethod
    def _live_state(session) -> tuple:
        row = session.get(DartDisclosureSyncRun, 5594)
        return (
            row.status,
            row.pages_fetched,
            row.events_seen,
            row.events_inserted,
            row.events_updated,
            row.financial_report_events,
            row.started_at,
            row.completed_at,
            deepcopy(row.run_metadata),
        )

    def test_row_state_mismatch_blocks_every_write(self) -> None:
        with self.session_factory.begin() as session:
            session.get(DartDisclosureSyncRun, 42).events_seen += 1

        result = self._run(apply=True)

        self.assertEqual(result['status'], 'blocked')
        self.assertEqual(
            result['reason'],
            'stale_dart_sync_run_manifest_mismatch',
        )
        self.assertFalse(result['invariants']['row_fields_match'])
        self.assertFalse(result['invariants']['row_hashes_match'])
        with self.session_factory() as session:
            self.assertEqual(
                [
                    session.get(DartDisclosureSyncRun, run_id).status
                    for run_id in (41, 42)
                ],
                ['running', 'running'],
            )

    def test_event_reference_drift_blocks_every_write(self) -> None:
        with self.session_factory.begin() as session:
            session.add(
                DartDisclosureEvent(
                    rcept_no='20260701000002',
                    corp_code='00126381',
                    corp_name='Another Corp',
                    stock_code='000660',
                    report_nm='Annual Report',
                    filing_category='periodic_report',
                    is_financial_report=True,
                    payload={},
                    first_seen_run_id=41,
                    last_seen_run_id=41,
                )
            )

        result = self._run(apply=True)

        self.assertEqual(result['status'], 'blocked')
        self.assertFalse(
            result['invariants']['event_reference_counts_match']
        )
        self.assertIn('event_references', result['row_mismatches'][41])

    def test_new_live_run_blocks_but_is_never_mutated(self) -> None:
        with self.session_factory.begin() as session:
            live = session.get(DartDisclosureSyncRun, 5594)
            live.status = 'running'
            live.completed_at = None
        with self.session_factory() as session:
            live_before = self._live_state(session)

        result = self._run(apply=True)

        self.assertEqual(result['status'], 'blocked')
        self.assertFalse(result['invariants']['global_running_ids_match'])
        self.assertFalse(result['invariants']['excluded_running_ids_match'])
        self.assertEqual(
            result['actual_excluded_running_run_ids'],
            [5594],
        )
        with self.session_factory() as session:
            self.assertEqual(self._live_state(session), live_before)
            self.assertEqual(
                [
                    session.get(DartDisclosureSyncRun, run_id).status
                    for run_id in (41, 42)
                ],
                ['running', 'running'],
            )

    def test_idempotent_replay_refuses_new_live_sync_work(self) -> None:
        applied = self._run(apply=True)
        self.assertEqual(applied['status'], 'completed')
        with self.session_factory.begin() as session:
            live = session.get(DartDisclosureSyncRun, 5594)
            live.status = 'running'
            live.completed_at = None
        with self.session_factory() as session:
            live_before = self._live_state(session)

        replay = self._run(apply=True)

        self.assertEqual(replay['status'], 'blocked')
        self.assertEqual(
            replay['reason'],
            'invalid_reconciliation_marker_state',
        )
        self.assertEqual(replay['actual_running_run_ids'], [5594])
        with self.session_factory() as session:
            self.assertEqual(self._live_state(session), live_before)
            self.assertEqual(
                [
                    session.get(DartDisclosureSyncRun, run_id).status
                    for run_id in (41, 42)
                ],
                [TARGET_STATUS, TARGET_STATUS],
            )

    def test_cutoff_drift_blocks_every_write(self) -> None:
        with self.session_factory.begin() as session:
            row = session.get(DartDisclosureSyncRun, 42)
            row.started_at += timedelta(hours=1)

        result = self._run(apply=True)

        self.assertEqual(result['status'], 'blocked')
        self.assertFalse(result['invariants']['stale_running_ids_match'])
        self.assertFalse(result['invariants']['stale_cutoff_bounds_match'])

    def test_apply_requires_reviewed_hash_before_database_access(self) -> None:
        with patch(
            'app.dart.stale_sync_run_reconciliation.session_scope'
        ) as session_scope:
            result = self.reconciliation.reconcile(apply=True)

        self.assertEqual(result['status'], 'blocked')
        self.assertEqual(result['reason'], 'expected_manifest_sha256_required')
        session_scope.assert_not_called()

    def test_wrong_reviewed_hash_blocks_before_database_access(self) -> None:
        with patch(
            'app.dart.stale_sync_run_reconciliation.session_scope'
        ) as session_scope:
            result = self.reconciliation.reconcile(
                apply=True,
                expected_manifest_sha256='not-the-reviewed-hash',
            )

        self.assertEqual(result['status'], 'blocked')
        self.assertEqual(result['reason'], 'expected_manifest_sha256_mismatch')
        session_scope.assert_not_called()

    def test_cas_conflict_rolls_back_every_write(self) -> None:
        original_cas_update = self.reconciliation._cas_update_row

        def first_update_then_conflict(session, expected, *, applied_at):
            if expected['persisted']['id'] == 41:
                return original_cas_update(
                    session,
                    expected,
                    applied_at=applied_at,
                )
            return 0

        with (
            patch(
                'app.dart.stale_sync_run_reconciliation.session_scope',
                self._session_scope,
            ),
            patch.object(
                self.reconciliation,
                '_cas_update_row',
                side_effect=first_update_then_conflict,
            ),
        ):
            result = self.reconciliation.reconcile(
                apply=True,
                expected_manifest_sha256=(
                    self.reconciliation.manifest_sha256
                ),
            )

        self.assertEqual(result['status'], 'blocked')
        self.assertEqual(result['reason'], 'cas_update_conflict')
        with self.session_factory() as session:
            self.assertEqual(
                [
                    session.get(DartDisclosureSyncRun, run_id).status
                    for run_id in (41, 42)
                ],
                ['running', 'running'],
            )
            self.assertNotIn(
                RECONCILIATION_MARKER_KEY,
                session.get(DartDisclosureSyncRun, 41).run_metadata,
            )

    def test_active_fundamentals_runs_block_every_write(self) -> None:
        with self.session_factory.begin() as session:
            session.add(
                DartFundamentalsCaptureRun(
                    id=700,
                    trigger='test',
                    status='running',
                    limit_requested=10,
                    force=False,
                    fs_div='CFS',
                    events_seen=0,
                    events_captured=0,
                    events_failed=0,
                    events_skipped=0,
                    run_metadata={'scope': 'test'},
                    started_at=datetime.now(timezone.utc),
                )
            )
            session.add(
                DartFundamentalsRefreshRun(
                    id=701,
                    market_date=date(2026, 7, 1),
                    trigger='test',
                    status='running',
                    fs_div='CFS',
                    force=False,
                    symbol_offset=0,
                    symbols_total=0,
                    symbols_checked=0,
                    symbols_missing=0,
                    symbols_stale=0,
                    symbols_fresh=0,
                    symbols_refreshed=0,
                    symbols_failed=0,
                    events_detected=0,
                    events_captured=0,
                    events_failed=0,
                    run_metadata={'scope': 'test'},
                    started_at=datetime.now(timezone.utc),
                )
            )

        result = self._run(apply=True)

        self.assertEqual(result['status'], 'blocked')
        self.assertEqual(
            result['reason'],
            'stale_dart_sync_run_manifest_mismatch',
        )
        self.assertFalse(
            result['invariants']['no_active_fundamentals_capture_runs']
        )
        self.assertFalse(
            result['invariants']['no_active_fundamentals_refresh_runs']
        )
        self.assertEqual(result['active_fundamentals_capture_run_ids'], [700])
        self.assertEqual(result['active_fundamentals_refresh_run_ids'], [701])
        with self.session_factory() as session:
            self.assertEqual(
                [
                    session.get(DartDisclosureSyncRun, run_id).status
                    for run_id in (41, 42)
                ],
                ['running', 'running'],
            )

    def test_dry_run_never_acquires_maintenance_lock(self) -> None:
        with (
            patch(
                'app.dart.stale_sync_run_reconciliation.session_scope',
                self._session_scope,
            ),
            patch(
                'app.dart.stale_sync_run_reconciliation.'
                '_acquire_dart_maintenance_locks'
            ) as maintenance_lock,
        ):
            result = self.reconciliation.reconcile(apply=False)

        self.assertEqual(result['status'], 'ready')
        maintenance_lock.assert_not_called()

    def test_apply_acquires_maintenance_lock_before_writing(self) -> None:
        with (
            patch(
                'app.dart.stale_sync_run_reconciliation.session_scope',
                self._session_scope,
            ),
            patch(
                'app.dart.stale_sync_run_reconciliation.'
                '_acquire_dart_maintenance_locks',
                wraps=_acquire_dart_maintenance_locks,
            ) as maintenance_lock,
        ):
            result = self.reconciliation.reconcile(
                apply=True,
                expected_manifest_sha256=(
                    self.reconciliation.manifest_sha256
                ),
            )

        self.assertEqual(result['status'], 'completed')
        maintenance_lock.assert_called_once()

    def test_postgres_maintenance_lock_uses_deadlock_safe_table_order(
        self,
    ) -> None:
        session = MagicMock()
        bind = MagicMock()
        bind.dialect.name = 'postgresql'
        session.get_bind.return_value = bind

        _acquire_dart_maintenance_locks(session)

        self.assertEqual(
            str(session.execute.call_args.args[0]),
            (
                'LOCK TABLE dart_disclosure_sync_runs, '
                'dart_fundamentals_capture_runs, '
                'dart_fundamentals_refresh_runs, '
                'dart_disclosure_events IN SHARE ROW EXCLUSIVE MODE'
            ),
        )

    def test_maintenance_lock_failure_blocks_before_every_write(self) -> None:
        lock_error = OperationalError(
            'LOCK TABLE',
            {},
            RuntimeError('writer is active'),
        )
        with (
            patch(
                'app.dart.stale_sync_run_reconciliation.session_scope',
                self._session_scope,
            ),
            patch(
                'app.dart.stale_sync_run_reconciliation.'
                '_acquire_dart_maintenance_locks',
                side_effect=lock_error,
            ),
        ):
            result = self.reconciliation.reconcile(
                apply=True,
                expected_manifest_sha256=(
                    self.reconciliation.manifest_sha256
                ),
            )

        self.assertEqual(result['status'], 'blocked')
        self.assertEqual(
            result['reason'],
            'dart_maintenance_lock_unavailable',
        )
        with self.session_factory() as session:
            self.assertEqual(
                [
                    session.get(DartDisclosureSyncRun, run_id).status
                    for run_id in (41, 42)
                ],
                ['running', 'running'],
            )

    def test_postgres_maintenance_lock_fences_all_dart_writers(self) -> None:
        if app_engine.dialect.name != 'postgresql':
            self.skipTest('PostgreSQL lock contract requires PostgreSQL')
        with app_engine.connect() as connection:
            can_lock_tables = all(
                bool(
                    connection.execute(
                        text(
                            'SELECT has_table_privilege('
                            "current_user, :table_name, 'SELECT, UPDATE')"
                        ),
                        {'table_name': table_name},
                    ).scalar_one()
                )
                for table_name in _DART_MAINTENANCE_TABLES
            )
        if not can_lock_tables:
            self.skipTest(
                'PostgreSQL test role lacks DART table lock privileges'
            )

        maintenance = Session(bind=app_engine)
        writer = Session(bind=app_engine)
        try:
            _acquire_dart_maintenance_locks(maintenance)
            for table_name in _DART_MAINTENANCE_TABLES:
                with self.subTest(table_name=table_name):
                    writer.execute(text('SET LOCAL lock_timeout = 100'))
                    with self.assertRaises(DBAPIError):
                        writer.execute(
                            text(
                                f'LOCK TABLE {table_name} '
                                'IN ROW EXCLUSIVE MODE NOWAIT'
                            )
                        )
                    writer.rollback()

            maintenance.rollback()
            writer.execute(
                text(
                    'LOCK TABLE '
                    f'{", ".join(_DART_MAINTENANCE_TABLES)} '
                    'IN ROW EXCLUSIVE MODE NOWAIT'
                )
            )
        finally:
            maintenance.rollback()
            writer.rollback()
            maintenance.close()
            writer.close()


if __name__ == '__main__':
    unittest.main()
