import unittest

from sqlalchemy import delete

from app.db import session_scope
from app.db_models import EODCollectionRun
from app.repositories.eod_run_repository import (
    complete_eod_collection_run,
    create_eod_collection_run,
    list_eod_collection_runs,
    load_most_complete_eod_collection_run,
)


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
