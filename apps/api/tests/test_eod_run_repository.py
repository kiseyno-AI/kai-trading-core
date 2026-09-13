import unittest
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from sqlalchemy import delete

from app.db import session_scope
from app.db_models import EODCollectionRun
from app.repositories.eod_run_repository import (
    advisory_eod_collection_lock,
    complete_eod_collection_run,
    create_eod_collection_run,
    list_eod_collection_runs,
    load_most_complete_eod_collection_run,
)


class AdvisoryEODCollectionLockTests(unittest.TestCase):
    @patch('app.repositories.eod_run_repository.engine')
    def test_uses_autocommit_connection_for_long_running_session_lock(self, engine):
        raw_connection = MagicMock()
        connection = MagicMock()
        engine.connect.return_value.__enter__.return_value = raw_connection
        raw_connection.execution_options.return_value = connection
        connection.execute.return_value.scalar.return_value = True

        with advisory_eod_collection_lock(
            market='KRX',
            trade_date='2026-08-21',
            source='pykrx',
        ) as acquired:
            self.assertTrue(acquired)

        raw_connection.execution_options.assert_called_once_with(
            isolation_level='AUTOCOMMIT'
        )
        self.assertEqual(connection.execute.call_count, 2)
        acquire_params = connection.execute.call_args_list[0].args[1]
        release_params = connection.execute.call_args_list[1].args[1]
        self.assertEqual(acquire_params, {'source_key': 10_001, 'trade_key': 20_260_821})
        self.assertEqual(release_params, acquire_params)

    @patch('app.repositories.eod_run_repository.engine')
    def test_unlock_failure_does_not_replace_completed_pipeline_result(self, engine):
        raw_connection = MagicMock()
        connection = MagicMock()
        engine.connect.return_value.__enter__.return_value = raw_connection
        raw_connection.execution_options.return_value = connection
        connection.execute.return_value.scalar.return_value = True
        connection.execute.side_effect = [MagicMock(scalar=lambda: True), RuntimeError('closed')]

        with advisory_eod_collection_lock(
            market='KRX',
            trade_date='2026-08-21',
            source='pykrx',
        ) as acquired:
            self.assertTrue(acquired)

        engine.connect.return_value.__exit__.assert_called_once_with(None, None, None)


class CompleteEODCollectionRunUnitTests(unittest.TestCase):
    def test_completion_timestamp_is_timezone_aware_utc(self):
        expected_completed_at = datetime(2026, 9, 12, 16, 5, tzinfo=timezone.utc)
        run = SimpleNamespace(
            status='running',
            symbols_succeeded=0,
            symbols_failed=0,
            error_message=None,
            completed_at=None,
            run_metadata=None,
        )
        session = MagicMock()
        session.get.return_value = run
        session_context = MagicMock()
        session_context.__enter__.return_value = session

        with (
            patch(
                'app.repositories.eod_run_repository.session_scope',
                return_value=session_context,
            ),
            patch('app.repositories.eod_run_repository.datetime') as datetime_type,
        ):
            datetime_type.now.return_value = expected_completed_at
            datetime_type.utcnow.return_value = expected_completed_at.replace(tzinfo=None)

            complete_eod_collection_run(
                521,
                status='completed_with_errors',
                symbols_succeeded=3_548,
                symbols_failed=1,
            )

        self.assertEqual(run.completed_at, expected_completed_at)
        datetime_type.now.assert_called_once_with(timezone.utc)


class EODRunRepositoryTests(unittest.TestCase):
    def setUp(self):
        self.created_run_ids: list[int] = []

    def tearDown(self):
        if not self.created_run_ids:
            return
        with session_scope() as session:
            session.execute(
                delete(EODCollectionRun).where(EODCollectionRun.id.in_(self.created_run_ids))
            )

    def _create_completed_run(
        self,
        *,
        trade_date: str,
        trigger: str,
        status: str,
        symbols_total: int,
        symbols_succeeded: int,
        symbols_failed: int,
        source: str = 'pykrx',
        run_metadata: dict | None = None,
    ) -> int:
        run_id = create_eod_collection_run(
            market='KRX',
            trade_date=trade_date,
            source=source,
            trigger=trigger,
            symbols_total=symbols_total,
            run_metadata=run_metadata,
        )
        self.created_run_ids.append(run_id)
        complete_eod_collection_run(
            run_id,
            status=status,
            symbols_succeeded=symbols_succeeded,
            symbols_failed=symbols_failed,
            run_metadata=run_metadata,
        )
        return run_id

    def test_list_eod_collection_runs_returns_plain_dict_rows(self):
        run_id = self._create_completed_run(
            trade_date='2099-12-31',
            trigger='test_repo_plain_rows',
            status='completed',
            symbols_total=5,
            symbols_succeeded=4,
            symbols_failed=1,
            run_metadata={'mode': 'daily_auto', 'last_progress_at': '2099-12-31T08:45:00+00:00'},
        )

        rows = list_eod_collection_runs(
            market='KRX',
            trade_date='2099-12-31',
            source='pykrx',
        )

        self.assertEqual(len(rows), 1)
        self.assertIsInstance(rows[0], dict)
        self.assertEqual(rows[0]['id'], run_id)
        self.assertEqual(rows[0]['status'], 'completed')
        self.assertEqual(rows[0]['symbols_succeeded'], 4)
        self.assertEqual(rows[0]['run_metadata']['mode'], 'daily_auto')
        self.assertNotIn('_sa_instance_state', rows[0])

    def test_list_eod_collection_runs_excludes_metadata_modes_before_limit(self):
        daily_run_id = self._create_completed_run(
            trade_date='2099-12-29',
            trigger='test_repo_daily_before_limit',
            status='completed',
            symbols_total=5,
            symbols_succeeded=5,
            symbols_failed=0,
            run_metadata={'mode': 'daily_auto'},
        )
        for index in range(11):
            self._create_completed_run(
                trade_date='2099-12-29',
                trigger=f'test_repo_historical_{index}',
                status='completed',
                symbols_total=5,
                symbols_succeeded=5,
                symbols_failed=0,
                run_metadata={'mode': 'historical_backfill'},
            )

        rows = list_eod_collection_runs(
            market='KRX',
            trade_date='2099-12-29',
            source='pykrx',
            excluded_run_metadata_modes=('historical_backfill',),
            limit=1,
        )

        self.assertEqual([row['id'] for row in rows], [daily_run_id])

    def test_load_most_complete_eod_collection_run_prefers_highest_success_count(self):
        losing_run_id = self._create_completed_run(
            trade_date='2099-12-30',
            trigger='test_repo_partial_loser',
            status='completed',
            symbols_total=5,
            symbols_succeeded=0,
            symbols_failed=0,
        )
        winning_run_id = self._create_completed_run(
            trade_date='2099-12-30',
            trigger='test_repo_partial_winner',
            status='failed',
            symbols_total=5,
            symbols_succeeded=4,
            symbols_failed=1,
            run_metadata={'last_progress_at': '2099-12-30T08:30:00+00:00'},
        )

        row = load_most_complete_eod_collection_run(
            market='KRX',
            trade_date='2099-12-30',
            statuses=('completed', 'failed'),
        )

        self.assertIsNotNone(row)
        self.assertIsInstance(row, dict)
        self.assertNotEqual(row['id'], losing_run_id)
        self.assertEqual(row['id'], winning_run_id)
        self.assertEqual(row['status'], 'failed')
        self.assertEqual(row['symbols_succeeded'], 4)
        self.assertEqual(row['symbols_failed'], 1)
