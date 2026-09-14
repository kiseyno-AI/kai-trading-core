import unittest
from contextlib import contextmanager
from datetime import date, datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from sqlalchemy import create_engine, delete, event, text
from sqlalchemy.orm import sessionmaker

from app.db import engine, session_scope
from app.db_models import DailyPipelineRun, DailyPipelineStageRun, EODCollectionRun
from app.repositories.eod_run_repository import (
    advisory_eod_collection_lock,
    complete_eod_collection_run,
    count_eod_run_persisted_symbols,
    create_eod_collection_run,
    eod_collection_advisory_lock_keys,
    list_eod_collection_runs,
    load_most_complete_eod_collection_run,
    load_oldest_outstanding_auto_eod_recovery_date,
)


class EODCollectionCoverageTests(unittest.TestCase):
    def test_persisted_symbol_count_is_distinct_and_trade_date_scoped(self):
        scalar_result = MagicMock()
        scalar_result.scalar_one.return_value = 3
        session = MagicMock()
        session.execute.return_value = scalar_result
        session_context = MagicMock()
        session_context.__enter__.return_value = session

        with patch(
            'app.repositories.eod_run_repository.session_scope',
            return_value=session_context,
        ):
            result = count_eod_run_persisted_symbols(
                522,
                trade_date='2026-09-14',
            )

        self.assertEqual(result, 3)
        statement = session.execute.call_args.args[0]
        sql = str(statement)
        self.assertIn(
            'count(distinct(daily_price_bars.symbol_id))',
            sql.lower(),
        )
        self.assertIn('daily_price_bars.eod_run_id', sql)
        self.assertIn('daily_price_bars.trade_date', sql)
        self.assertEqual(
            set(statement.compile().params.values()),
            {522, datetime(2026, 9, 14).date()},
        )


class OldestOutstandingAutoEODRecoveryDateTests(unittest.TestCase):
    def setUp(self) -> None:
        self.sqlite_engine = create_engine('sqlite+pysqlite:///:memory:')
        EODCollectionRun.__table__.create(self.sqlite_engine)
        DailyPipelineRun.__table__.create(self.sqlite_engine)
        DailyPipelineStageRun.__table__.create(self.sqlite_engine)
        self.session_factory = sessionmaker(bind=self.sqlite_engine, future=True)

    def tearDown(self) -> None:
        self.sqlite_engine.dispose()

    @contextmanager
    def _session_scope(self):
        with self.session_factory.begin() as session:
            yield session

    def test_lower_bound_excludes_stale_debt_and_keeps_recent_recovery(
        self,
    ) -> None:
        started_at = datetime(2026, 9, 14, tzinfo=timezone.utc)
        with self.session_factory.begin() as session:
            stale_price_debt = EODCollectionRun(
                market='KRX',
                trade_date=date(2026, 4, 7),
                source='pykrx',
                trigger='auto_close',
                status='completed_with_errors',
                symbols_total=2,
                symbols_succeeded=1,
                symbols_failed=1,
                started_at=started_at,
                run_metadata={'mode': 'daily_auto'},
            )
            stale_followup_debt = EODCollectionRun(
                market='KRX',
                trade_date=date(2026, 6, 12),
                source='pykrx',
                trigger='auto_close_scheduler',
                status='completed',
                symbols_total=2,
                symbols_succeeded=2,
                symbols_failed=0,
                started_at=started_at,
                run_metadata={
                    'mode': 'daily_auto',
                    'followup_recovery_required': True,
                },
            )
            boundary_followup = EODCollectionRun(
                market='KRX',
                trade_date=date(2026, 9, 8),
                source='pykrx',
                trigger='auto_close_scheduler',
                status='completed',
                symbols_total=2,
                symbols_succeeded=2,
                symbols_failed=0,
                started_at=started_at,
                run_metadata={
                    'mode': 'daily_auto',
                    'followup_recovery_required': True,
                },
            )
            interrupted_eod = EODCollectionRun(
                market='KRX',
                trade_date=date(2026, 9, 11),
                source='pykrx',
                trigger='auto_close_scheduler',
                status='completed',
                symbols_total=2,
                symbols_succeeded=2,
                symbols_failed=0,
                started_at=started_at,
                run_metadata={'mode': 'daily_auto'},
            )
            session.add_all(
                [
                    stale_price_debt,
                    stale_followup_debt,
                    boundary_followup,
                    interrupted_eod,
                ]
            )
            session.flush()
            session.add(
                DailyPipelineRun(
                    market_date=date(2026, 9, 11),
                    trigger='auto_close_scheduler',
                    status='interrupted',
                    current_stage='technical_snapshots',
                    started_at=started_at,
                    run_metadata={
                        'entrypoint': (
                            'PYKRX_BACKFILL_SERVICE.'
                            'run_daily_pipeline_if_due'
                        ),
                        'eod_collection_run_id': interrupted_eod.id,
                        'resumable': True,
                    },
                )
            )

        with patch(
            'app.repositories.eod_run_repository.session_scope',
            self._session_scope,
        ):
            boundary_result = load_oldest_outstanding_auto_eod_recovery_date(
                on_or_after='2026-09-08',
                on_or_before='2026-09-14',
            )
            interrupted_result = load_oldest_outstanding_auto_eod_recovery_date(
                on_or_after='2026-09-09',
                on_or_before='2026-09-14',
            )

        self.assertEqual(boundary_result, '2026-09-08')
        self.assertEqual(interrupted_result, '2026-09-11')

    def test_returns_oldest_latest_automatic_daily_eod_with_price_retry_scope(
        self,
    ) -> None:
        started_at = datetime(2026, 9, 14, tzinfo=timezone.utc)
        with self.session_factory.begin() as session:
            session.add_all(
                [
                    EODCollectionRun(
                        market='KRX',
                        trade_date=date(2026, 9, 9),
                        source='pykrx',
                        trigger='manual_api',
                        status='failed',
                        symbols_total=2,
                        symbols_succeeded=1,
                        symbols_failed=1,
                        started_at=started_at,
                        run_metadata={'mode': 'daily_auto'},
                    ),
                    EODCollectionRun(
                        market='KRX',
                        trade_date=date(2026, 9, 10),
                        source='pykrx',
                        trigger='auto_close_scheduler',
                        status='failed',
                        symbols_total=2,
                        symbols_succeeded=1,
                        symbols_failed=1,
                        started_at=started_at,
                        run_metadata={'mode': 'historical_backfill'},
                    ),
                    EODCollectionRun(
                        market='KRX',
                        trade_date=date(2026, 9, 11),
                        source='pykrx',
                        trigger='auto_close_scheduler',
                        status='failed',
                        symbols_total=2,
                        symbols_succeeded=1,
                        symbols_failed=1,
                        started_at=started_at,
                        run_metadata={
                            'mode': 'daily_auto',
                            'requested_symbols': ['005930', '000660'],
                            'failed_symbols': ['000660'],
                        },
                    ),
                    EODCollectionRun(
                        market='KRX',
                        trade_date=date(2026, 9, 12),
                        source='pykrx',
                        trigger='auto_close_scheduler',
                        status='completed',
                        symbols_total=2,
                        symbols_succeeded=2,
                        symbols_failed=0,
                        started_at=started_at,
                        run_metadata={'mode': 'daily_auto'},
                    ),
                    EODCollectionRun(
                        market='KRX',
                        trade_date=date(2026, 9, 13),
                        source='pykrx',
                        trigger='auto_close_scheduler',
                        status='failed',
                        symbols_total=2,
                        symbols_succeeded=1,
                        symbols_failed=1,
                        started_at=started_at,
                        run_metadata={'mode': 'daily_auto'},
                    ),
                    EODCollectionRun(
                        market='KRX',
                        trade_date=date(2026, 9, 13),
                        source='pykrx',
                        trigger='auto_close_scheduler',
                        status='completed',
                        symbols_total=2,
                        symbols_succeeded=2,
                        symbols_failed=0,
                        started_at=started_at + timedelta(minutes=1),
                        run_metadata={'mode': 'daily_auto'},
                    ),
                ]
            )

        with patch(
            'app.repositories.eod_run_repository.session_scope',
            self._session_scope,
        ):
            result = load_oldest_outstanding_auto_eod_recovery_date(
                on_or_after='2026-09-01',
                on_or_before='2026-09-14',
            )

        self.assertEqual(result, '2026-09-11')

    def test_newer_unrelated_rows_do_not_hide_latest_automatic_daily_lineage(
        self,
    ) -> None:
        started_at = datetime(2026, 9, 14, tzinfo=timezone.utc)
        with self.session_factory.begin() as session:
            session.add_all(
                [
                    EODCollectionRun(
                        market='KRX',
                        trade_date=date(2026, 9, 8),
                        source='pykrx',
                        trigger='auto_close_scheduler',
                        status='completed_with_errors',
                        symbols_total=2,
                        symbols_succeeded=1,
                        symbols_failed=1,
                        started_at=started_at,
                        run_metadata={'mode': 'daily_auto'},
                    ),
                    EODCollectionRun(
                        market='KRX',
                        trade_date=date(2026, 9, 8),
                        source='pykrx',
                        trigger='manual_api',
                        status='completed',
                        symbols_total=2,
                        symbols_succeeded=2,
                        symbols_failed=0,
                        started_at=started_at + timedelta(minutes=1),
                        run_metadata={'mode': 'manual'},
                    ),
                    EODCollectionRun(
                        market='KRX',
                        trade_date=date(2026, 9, 9),
                        source='pykrx',
                        trigger='auto_close_scheduler',
                        status='completed_with_errors',
                        symbols_total=2,
                        symbols_succeeded=1,
                        symbols_failed=1,
                        started_at=started_at,
                        run_metadata={'mode': 'daily_auto'},
                    ),
                    EODCollectionRun(
                        market='KRX',
                        trade_date=date(2026, 9, 9),
                        source='pykrx',
                        trigger='auto_close_scheduler',
                        status='completed',
                        symbols_total=2,
                        symbols_succeeded=2,
                        symbols_failed=0,
                        started_at=started_at + timedelta(minutes=1),
                        run_metadata={'mode': 'historical_backfill'},
                    ),
                    EODCollectionRun(
                        market='KRX',
                        trade_date=date(2026, 9, 10),
                        source='pykrx',
                        trigger='auto_close',
                        status='completed_with_errors',
                        symbols_total=2,
                        symbols_succeeded=1,
                        symbols_failed=1,
                        started_at=started_at,
                        run_metadata={},
                    ),
                    EODCollectionRun(
                        market='KRX',
                        trade_date=date(2026, 9, 10),
                        source='pykrx',
                        trigger='manual_api',
                        status='completed',
                        symbols_total=2,
                        symbols_succeeded=2,
                        symbols_failed=0,
                        started_at=started_at + timedelta(minutes=1),
                        run_metadata={'mode': 'manual'},
                    ),
                ]
            )

        with patch(
            'app.repositories.eod_run_repository.session_scope',
            self._session_scope,
        ):
            manual_result = load_oldest_outstanding_auto_eod_recovery_date(
                on_or_after='2026-09-08',
                on_or_before='2026-09-14',
            )
            historical_result = load_oldest_outstanding_auto_eod_recovery_date(
                on_or_after='2026-09-09',
                on_or_before='2026-09-14',
            )
            legacy_result = load_oldest_outstanding_auto_eod_recovery_date(
                on_or_after='2026-09-10',
                on_or_before='2026-09-14',
            )

        self.assertEqual(manual_result, '2026-09-08')
        self.assertEqual(historical_result, '2026-09-09')
        self.assertEqual(legacy_result, '2026-09-10')

    def test_bounded_candidate_lookup_avoids_json_materialization(self) -> None:
        with self.session_factory.begin() as session:
            session.add(
                EODCollectionRun(
                    market='KRX',
                    trade_date=date(2026, 9, 11),
                    source='pykrx',
                    trigger='auto_close_scheduler',
                    status='completed',
                    symbols_total=1,
                    symbols_succeeded=1,
                    symbols_failed=0,
                    started_at=datetime(2026, 9, 14, tzinfo=timezone.utc),
                    run_metadata={'mode': 'daily_auto'},
                )
            )

        statements: list[str] = []

        def capture_statement(
            _connection,
            _cursor,
            statement,
            _parameters,
            _context,
            _executemany,
        ) -> None:
            statements.append(str(statement))

        event.listen(
            self.sqlite_engine,
            'before_cursor_execute',
            capture_statement,
        )
        try:
            with patch(
                'app.repositories.eod_run_repository.session_scope',
                self._session_scope,
            ):
                load_oldest_outstanding_auto_eod_recovery_date(
                    on_or_after='2026-09-01',
                    on_or_before='2026-09-14',
                )
        finally:
            event.remove(
                self.sqlite_engine,
                'before_cursor_execute',
                capture_statement,
            )

        first_lookup = next(
            statement.lower()
            for statement in statements
            if 'from eod_collection_runs' in statement.lower()
        )
        self.assertNotIn('row_number(', first_lookup)
        self.assertNotIn('run_metadata', first_lookup)
        self.assertNotIn('exists', first_lookup)
        self.assertIn('eod_collection_runs.trade_date >=', first_lookup)
        self.assertIn('eod_collection_runs.trade_date <=', first_lookup)
        self.assertIn('eod_collection_runs."trigger"', first_lookup)
        self.assertIn(
            'order by eod_collection_runs.trade_date asc, '
            'eod_collection_runs.started_at desc, '
            'eod_collection_runs.id desc',
            first_lookup,
        )

    def test_uses_latest_exact_linked_automatic_pipeline_state(self) -> None:
        started_at = datetime(2026, 9, 14, tzinfo=timezone.utc)
        with self.session_factory.begin() as session:
            mismatched = EODCollectionRun(
                market='KRX',
                trade_date=date(2026, 9, 8),
                source='pykrx',
                trigger='auto_close_scheduler',
                status='completed',
                symbols_total=1,
                symbols_succeeded=1,
                symbols_failed=0,
                started_at=started_at,
                run_metadata={'mode': 'daily_auto'},
            )
            recovered = EODCollectionRun(
                market='KRX',
                trade_date=date(2026, 9, 9),
                source='pykrx',
                trigger='auto_close_scheduler',
                status='completed',
                symbols_total=1,
                symbols_succeeded=1,
                symbols_failed=0,
                started_at=started_at,
                run_metadata={'mode': 'daily_auto'},
            )
            outstanding = EODCollectionRun(
                market='KRX',
                trade_date=date(2026, 9, 11),
                source='pykrx',
                trigger='auto_close_scheduler',
                status='completed',
                symbols_total=1,
                symbols_succeeded=1,
                symbols_failed=0,
                started_at=started_at,
                run_metadata={'mode': 'daily_auto'},
            )
            session.add_all([mismatched, recovered, outstanding])
            session.flush()
            session.add_all(
                [
                    DailyPipelineRun(
                        market_date=date(2026, 9, 8),
                        trigger='auto_close_scheduler',
                        status='partial',
                        started_at=started_at,
                        run_metadata={
                            'entrypoint': (
                                'PYKRX_BACKFILL_SERVICE.'
                                'run_daily_pipeline_if_due'
                            ),
                            'eod_collection_run_id': mismatched.id + 100,
                        },
                    ),
                    DailyPipelineRun(
                        market_date=date(2026, 9, 9),
                        trigger='auto_close_scheduler',
                        status='partial',
                        started_at=started_at,
                        run_metadata={
                            'entrypoint': (
                                'PYKRX_BACKFILL_SERVICE.'
                                'run_daily_pipeline_if_due'
                            ),
                            'eod_collection_run_id': recovered.id,
                        },
                    ),
                    DailyPipelineRun(
                        market_date=date(2026, 9, 9),
                        trigger='auto_eod_followup_resume',
                        status='completed',
                        started_at=started_at + timedelta(minutes=1),
                        run_metadata={
                            'entrypoint': 'daily_pipeline_stage_service.run',
                            'reused_eod_collection_run_id': recovered.id,
                        },
                    ),
                    DailyPipelineRun(
                        market_date=date(2026, 9, 11),
                        trigger='auto_close_scheduler',
                        status='interrupted',
                        started_at=started_at,
                        run_metadata={
                            'entrypoint': (
                                'PYKRX_BACKFILL_SERVICE.'
                                'run_daily_pipeline_if_due'
                            ),
                            'eod_collection_run_id': outstanding.id,
                            'resumable': True,
                        },
                    ),
                ]
            )

        with patch(
            'app.repositories.eod_run_repository.session_scope',
            self._session_scope,
        ):
            result = load_oldest_outstanding_auto_eod_recovery_date(
                on_or_after='2026-09-01',
                on_or_before='2026-09-14',
            )

        self.assertEqual(result, '2026-09-11')

    def test_conflicting_explicit_pipeline_lineage_is_not_recovery_evidence(
        self,
    ) -> None:
        started_at = datetime(2026, 9, 14, tzinfo=timezone.utc)
        with self.session_factory.begin() as session:
            eod_run = EODCollectionRun(
                market='KRX',
                trade_date=date(2026, 9, 11),
                source='pykrx',
                trigger='auto_close_scheduler',
                status='completed',
                symbols_total=2,
                symbols_succeeded=2,
                symbols_failed=0,
                started_at=started_at,
                run_metadata={'mode': 'daily_auto'},
            )
            session.add(eod_run)
            session.flush()
            pipeline_run = DailyPipelineRun(
                market_date=date(2026, 9, 11),
                trigger='auto_close_scheduler',
                status='interrupted',
                current_stage='technical_snapshots',
                started_at=started_at,
                run_metadata={
                    'entrypoint': (
                        'PYKRX_BACKFILL_SERVICE.'
                        'run_daily_pipeline_if_due'
                    ),
                    'eod_collection_run_id': eod_run.id,
                    'reused_eod_collection_run_id': eod_run.id + 100,
                    'resumable': True,
                },
            )
            session.add(pipeline_run)
            session.flush()
            eod_run.run_metadata = {
                'mode': 'daily_auto',
                'daily_pipeline_run_id': pipeline_run.id,
            }

        with patch(
            'app.repositories.eod_run_repository.session_scope',
            self._session_scope,
        ):
            result = load_oldest_outstanding_auto_eod_recovery_date(
                on_or_after='2026-09-08',
                on_or_before='2026-09-14',
            )

        self.assertIsNone(result)

    def test_skips_exhausted_price_only_date_without_starving_newer_date(
        self,
    ) -> None:
        started_at = datetime(2026, 9, 14, tzinfo=timezone.utc)
        with self.session_factory.begin() as session:
            exhausted_eod_run = None
            for attempt in range(5):
                exhausted_eod_run = EODCollectionRun(
                    market='KRX',
                    trade_date=date(2026, 9, 10),
                    source='pykrx',
                    trigger='auto_close_scheduler',
                    status='failed',
                    symbols_total=2,
                    symbols_succeeded=1,
                    symbols_failed=1,
                    started_at=started_at + timedelta(minutes=attempt),
                    run_metadata={
                        'mode': 'daily_auto',
                        'requested_symbols': ['005930', '000660'],
                        'failed_symbols': ['000660'],
                    },
                )
                session.add(exhausted_eod_run)
            session.flush()
            price_only_pipeline = DailyPipelineRun(
                market_date=date(2026, 9, 10),
                trigger='auto_close_scheduler',
                status='failed',
                current_stage='price_collection',
                started_at=started_at + timedelta(minutes=5),
                run_metadata={
                    'entrypoint': (
                        'PYKRX_BACKFILL_SERVICE.'
                        'run_daily_pipeline_if_due'
                    ),
                    'eod_collection_run_id': exhausted_eod_run.id,
                    'stage_statuses': {'price_collection': 'failed'},
                },
            )
            session.add(price_only_pipeline)
            session.flush()
            session.add(
                DailyPipelineStageRun(
                    pipeline_run_id=price_only_pipeline.id,
                    market_date=date(2026, 9, 10),
                    stage_id='price_collection',
                    status='failed',
                    started_at=started_at + timedelta(minutes=5),
                    failed=1,
                )
            )
            session.add(
                EODCollectionRun(
                    market='KRX',
                    trade_date=date(2026, 9, 11),
                    source='pykrx',
                    trigger='auto_close_scheduler',
                    status='failed',
                    symbols_total=2,
                    symbols_succeeded=1,
                    symbols_failed=1,
                    started_at=started_at,
                    run_metadata={
                        'mode': 'daily_auto',
                        'requested_symbols': ['005930', '000660'],
                        'failed_symbols': ['000660'],
                    },
                )
            )

        with patch(
            'app.repositories.eod_run_repository.session_scope',
            self._session_scope,
        ):
            result = load_oldest_outstanding_auto_eod_recovery_date(
                on_or_after='2026-09-01',
                on_or_before='2026-09-14',
            )

        self.assertEqual(result, '2026-09-11')

    def test_skips_exhausted_followup_lineages_without_using_price_budget(
        self,
    ) -> None:
        started_at = datetime(2026, 9, 14, tzinfo=timezone.utc)
        with self.session_factory.begin() as session:
            price_recovery = EODCollectionRun(
                market='KRX',
                trade_date=date(2026, 9, 7),
                source='pykrx',
                trigger='auto_close_scheduler',
                status='completed_with_errors',
                symbols_total=2,
                symbols_succeeded=1,
                symbols_failed=1,
                started_at=started_at,
                run_metadata={'mode': 'daily_auto'},
            )
            exhausted_followup = EODCollectionRun(
                market='KRX',
                trade_date=date(2026, 9, 8),
                source='pykrx',
                trigger='auto_close_scheduler',
                status='completed',
                symbols_total=2,
                symbols_succeeded=2,
                symbols_failed=0,
                started_at=started_at,
                run_metadata={
                    'mode': 'daily_auto',
                    'followup_recovery_required': True,
                },
            )
            exhausted_pipeline = EODCollectionRun(
                market='KRX',
                trade_date=date(2026, 9, 9),
                source='pykrx',
                trigger='auto_close_scheduler',
                status='completed',
                symbols_total=2,
                symbols_succeeded=2,
                symbols_failed=0,
                started_at=started_at,
                run_metadata={'mode': 'daily_auto'},
            )
            superseded_current = EODCollectionRun(
                market='KRX',
                trade_date=date(2026, 9, 14),
                source='pykrx',
                trigger='auto_close_scheduler',
                status='completed',
                symbols_total=2,
                symbols_succeeded=2,
                symbols_failed=0,
                started_at=started_at - timedelta(minutes=1),
                run_metadata={'mode': 'daily_auto'},
            )
            current_followup = EODCollectionRun(
                market='KRX',
                trade_date=date(2026, 9, 14),
                source='pykrx',
                trigger='auto_close_scheduler',
                status='completed',
                symbols_total=2,
                symbols_succeeded=2,
                symbols_failed=0,
                started_at=started_at,
                run_metadata={
                    'mode': 'daily_auto',
                    'followup_recovery_required': True,
                },
            )
            session.add_all(
                [
                    price_recovery,
                    exhausted_followup,
                    exhausted_pipeline,
                    superseded_current,
                    current_followup,
                ]
            )
            session.flush()
            for eod_run in (
                price_recovery,
                exhausted_followup,
                exhausted_pipeline,
            ):
                session.add_all(
                    [
                        DailyPipelineRun(
                            market_date=eod_run.trade_date,
                            trigger='auto_eod_followup_resume',
                            status='failed',
                            current_stage='technical_snapshots',
                            started_at=started_at + timedelta(minutes=attempt),
                            run_metadata={
                                'reused_eod_collection_run_id': eod_run.id,
                            },
                        )
                        for attempt in range(5)
                    ]
                )
            session.add_all(
                [
                    DailyPipelineRun(
                        market_date=current_followup.trade_date,
                        trigger='auto_eod_followup_resume',
                        status='failed',
                        current_stage='technical_snapshots',
                        started_at=started_at + timedelta(minutes=attempt),
                        run_metadata={
                            'reused_eod_collection_run_id': (
                                superseded_current.id
                            ),
                        },
                    )
                    for attempt in range(5)
                ]
            )
            session.add_all(
                [
                    DailyPipelineRun(
                        market_date=current_followup.trade_date,
                        trigger='auto_eod_followup_resume',
                        status='failed',
                        current_stage='technical_snapshots',
                        started_at=(
                            started_at + timedelta(minutes=attempt + 5)
                        ),
                        run_metadata={
                            'reused_eod_collection_run_id': current_followup.id,
                        },
                    )
                    for attempt in range(4)
                ]
            )

        with patch(
            'app.repositories.eod_run_repository.session_scope',
            self._session_scope,
        ):
            exhausted_result = load_oldest_outstanding_auto_eod_recovery_date(
                on_or_after='2026-09-08',
                on_or_before='2026-09-14',
            )
            price_result = load_oldest_outstanding_auto_eod_recovery_date(
                on_or_after='2026-09-07',
                on_or_before='2026-09-14',
            )

        self.assertEqual(exhausted_result, '2026-09-14')
        self.assertEqual(price_result, '2026-09-07')

    def test_exhausted_terminal_satisfied_followup_remains_eligible_for_convergence(
        self,
    ) -> None:
        started_at = datetime(2026, 9, 14, tzinfo=timezone.utc)
        with self.session_factory.begin() as session:
            convergence_eod = EODCollectionRun(
                market='KRX',
                trade_date=date(2026, 9, 8),
                source='pykrx',
                trigger='auto_close_scheduler',
                status='completed',
                symbols_total=2,
                symbols_succeeded=2,
                symbols_failed=0,
                started_at=started_at,
                run_metadata={
                    'mode': 'daily_auto',
                    'followup_recovery_required': True,
                },
            )
            exhausted_failed_eod = EODCollectionRun(
                market='KRX',
                trade_date=date(2026, 9, 9),
                source='pykrx',
                trigger='auto_close_scheduler',
                status='completed',
                symbols_total=2,
                symbols_succeeded=2,
                symbols_failed=0,
                started_at=started_at,
                run_metadata={
                    'mode': 'daily_auto',
                    'followup_recovery_required': True,
                },
            )
            current_followup = EODCollectionRun(
                market='KRX',
                trade_date=date(2026, 9, 14),
                source='pykrx',
                trigger='auto_close_scheduler',
                status='completed',
                symbols_total=2,
                symbols_succeeded=2,
                symbols_failed=0,
                started_at=started_at,
                run_metadata={
                    'mode': 'daily_auto',
                    'followup_recovery_required': True,
                },
            )
            session.add_all(
                [convergence_eod, exhausted_failed_eod, current_followup]
            )
            session.flush()

            for eod_run in (convergence_eod, exhausted_failed_eod):
                session.add_all(
                    [
                        DailyPipelineRun(
                            market_date=eod_run.trade_date,
                            trigger='auto_eod_followup_resume',
                            status='failed',
                            current_stage='technical_snapshots',
                            started_at=started_at + timedelta(minutes=attempt),
                            run_metadata={
                                'reused_eod_collection_run_id': eod_run.id,
                            },
                        )
                        for attempt in range(4)
                    ]
                )

            terminal_recovery = DailyPipelineRun(
                market_date=convergence_eod.trade_date,
                trigger='auto_eod_followup_resume',
                status='completed',
                current_stage='next_open_execution_plan',
                started_at=started_at + timedelta(minutes=4),
                completed_at=started_at + timedelta(minutes=5),
                run_metadata={
                    'reused_eod_collection_run_id': convergence_eod.id,
                    'resolved_stages': [
                        'technical_snapshots',
                        'next_open_execution_plan',
                    ],
                },
            )
            failed_recovery = DailyPipelineRun(
                market_date=exhausted_failed_eod.trade_date,
                trigger='auto_eod_followup_resume',
                status='failed',
                current_stage='technical_snapshots',
                started_at=started_at + timedelta(minutes=4),
                completed_at=started_at + timedelta(minutes=5),
                run_metadata={
                    'reused_eod_collection_run_id': exhausted_failed_eod.id,
                    'resolved_stages': ['technical_snapshots'],
                },
            )
            session.add_all([terminal_recovery, failed_recovery])
            session.flush()
            session.add_all(
                [
                    DailyPipelineStageRun(
                        pipeline_run_id=terminal_recovery.id,
                        market_date=convergence_eod.trade_date,
                        stage_id='technical_snapshots',
                        status='completed',
                        processed=2,
                        total=2,
                        succeeded=2,
                        failed=0,
                        started_at=started_at + timedelta(minutes=4),
                        completed_at=started_at + timedelta(minutes=5),
                    ),
                    DailyPipelineStageRun(
                        pipeline_run_id=terminal_recovery.id,
                        market_date=convergence_eod.trade_date,
                        stage_id='next_open_execution_plan',
                        status='skipped',
                        started_at=started_at + timedelta(minutes=4),
                        completed_at=started_at + timedelta(minutes=5),
                        details={
                            'result_summary': {
                                'reason': 'next_open_window_expired',
                            },
                        },
                    ),
                    DailyPipelineStageRun(
                        pipeline_run_id=failed_recovery.id,
                        market_date=exhausted_failed_eod.trade_date,
                        stage_id='technical_snapshots',
                        status='failed',
                        processed=1,
                        total=2,
                        succeeded=1,
                        failed=1,
                        started_at=started_at + timedelta(minutes=4),
                        completed_at=started_at + timedelta(minutes=5),
                    ),
                ]
            )

        with patch(
            'app.repositories.eod_run_repository.session_scope',
            self._session_scope,
        ):
            convergence_result = (
                load_oldest_outstanding_auto_eod_recovery_date(
                    on_or_after='2026-09-08',
                    on_or_before='2026-09-14',
                )
            )
            failed_result = load_oldest_outstanding_auto_eod_recovery_date(
                on_or_after='2026-09-09',
                on_or_before='2026-09-14',
            )

        self.assertEqual(convergence_result, '2026-09-08')
        self.assertEqual(failed_result, '2026-09-14')

    def test_exhausted_satisfied_interrupted_or_running_followup_allows_convergence(
        self,
    ) -> None:
        started_at = datetime(2026, 9, 14, tzinfo=timezone.utc)
        parent_states = (
            (date(2026, 9, 8), 'interrupted'),
            (date(2026, 9, 9), 'running'),
            (date(2026, 9, 10), 'failed'),
            (date(2026, 9, 11), 'blocked'),
        )
        with self.session_factory.begin() as session:
            for market_date, parent_status in parent_states:
                eod_run = EODCollectionRun(
                    market='KRX',
                    trade_date=market_date,
                    source='pykrx',
                    trigger='auto_close_scheduler',
                    status='completed',
                    symbols_total=2,
                    symbols_succeeded=2,
                    symbols_failed=0,
                    started_at=started_at,
                    run_metadata={
                        'mode': 'daily_auto',
                        'followup_recovery_required': True,
                    },
                )
                session.add(eod_run)
                session.flush()
                session.add_all(
                    [
                        DailyPipelineRun(
                            market_date=market_date,
                            trigger='auto_eod_followup_resume',
                            status='failed',
                            current_stage='technical_snapshots',
                            started_at=started_at + timedelta(minutes=attempt),
                            run_metadata={
                                'reused_eod_collection_run_id': eod_run.id,
                            },
                        )
                        for attempt in range(4)
                    ]
                )
                latest_recovery = DailyPipelineRun(
                    market_date=market_date,
                    trigger='auto_eod_followup_resume',
                    status=parent_status,
                    current_stage='technical_snapshots',
                    started_at=started_at + timedelta(minutes=4),
                    run_metadata={
                        'reused_eod_collection_run_id': eod_run.id,
                        'resolved_stages': ['technical_snapshots'],
                    },
                )
                session.add(latest_recovery)
                session.flush()
                session.add(
                    DailyPipelineStageRun(
                        pipeline_run_id=latest_recovery.id,
                        market_date=market_date,
                        stage_id='technical_snapshots',
                        status='completed',
                        processed=2,
                        total=2,
                        succeeded=2,
                        failed=0,
                        started_at=started_at + timedelta(minutes=4),
                        completed_at=started_at + timedelta(minutes=5),
                    )
                )

            session.add(
                EODCollectionRun(
                    market='KRX',
                    trade_date=date(2026, 9, 14),
                    source='pykrx',
                    trigger='auto_close_scheduler',
                    status='completed',
                    symbols_total=2,
                    symbols_succeeded=2,
                    symbols_failed=0,
                    started_at=started_at,
                    run_metadata={
                        'mode': 'daily_auto',
                        'followup_recovery_required': True,
                    },
                )
            )

        with patch(
            'app.repositories.eod_run_repository.session_scope',
            self._session_scope,
        ):
            interrupted_result = (
                load_oldest_outstanding_auto_eod_recovery_date(
                    on_or_after='2026-09-08',
                    on_or_before='2026-09-14',
                )
            )
            running_result = load_oldest_outstanding_auto_eod_recovery_date(
                on_or_after='2026-09-09',
                on_or_before='2026-09-14',
            )
            rejected_parent_result = (
                load_oldest_outstanding_auto_eod_recovery_date(
                    on_or_after='2026-09-10',
                    on_or_before='2026-09-14',
                )
            )

        self.assertEqual(interrupted_result, '2026-09-08')
        self.assertEqual(running_result, '2026-09-09')
        self.assertEqual(rejected_parent_result, '2026-09-14')

    def test_exhausted_price_date_remains_eligible_for_incomplete_pipeline(
        self,
    ) -> None:
        started_at = datetime(2026, 9, 14, tzinfo=timezone.utc)
        with self.session_factory.begin() as session:
            latest_eod_run = None
            for attempt in range(5):
                latest_eod_run = EODCollectionRun(
                    market='KRX',
                    trade_date=date(2026, 9, 11),
                    source='pykrx',
                    trigger='auto_close_scheduler',
                    status='failed',
                    symbols_total=2,
                    symbols_succeeded=1,
                    symbols_failed=1,
                    started_at=started_at + timedelta(minutes=attempt),
                    run_metadata={
                        'mode': 'daily_auto',
                        'requested_symbols': ['005930', '000660'],
                        'failed_symbols': ['000660'],
                    },
                )
                session.add(latest_eod_run)
            session.flush()
            pipeline_run = DailyPipelineRun(
                market_date=date(2026, 9, 11),
                trigger='auto_close_scheduler',
                status='interrupted',
                current_stage='technical_snapshots',
                started_at=started_at + timedelta(minutes=5),
                run_metadata={
                    'entrypoint': (
                        'PYKRX_BACKFILL_SERVICE.'
                        'run_daily_pipeline_if_due'
                    ),
                    'eod_collection_run_id': latest_eod_run.id,
                },
            )
            session.add(pipeline_run)
            session.flush()
            session.add(
                DailyPipelineStageRun(
                    pipeline_run_id=pipeline_run.id,
                    market_date=date(2026, 9, 11),
                    stage_id='technical_snapshots',
                    status='interrupted',
                    started_at=started_at + timedelta(minutes=5),
                )
            )

        with patch(
            'app.repositories.eod_run_repository.session_scope',
            self._session_scope,
        ):
            result = load_oldest_outstanding_auto_eod_recovery_date(
                on_or_after='2026-09-01',
                on_or_before='2026-09-14',
            )

        self.assertEqual(result, '2026-09-11')

    def test_exhausted_price_date_remains_eligible_with_non_price_metadata_checkpoint(
        self,
    ) -> None:
        started_at = datetime(2026, 9, 14, tzinfo=timezone.utc)
        with self.session_factory.begin() as session:
            latest_eod_run = None
            for attempt in range(5):
                latest_eod_run = EODCollectionRun(
                    market='KRX',
                    trade_date=date(2026, 9, 11),
                    source='pykrx',
                    trigger='auto_close_scheduler',
                    status='failed',
                    symbols_total=2,
                    symbols_succeeded=1,
                    symbols_failed=1,
                    started_at=started_at + timedelta(minutes=attempt),
                    run_metadata={
                        'mode': 'daily_auto',
                        'requested_symbols': ['005930', '000660'],
                        'failed_symbols': ['000660'],
                    },
                )
                session.add(latest_eod_run)
            session.flush()
            session.add(
                DailyPipelineRun(
                    market_date=date(2026, 9, 11),
                    trigger='auto_close_scheduler',
                    status='interrupted',
                    current_stage='price_collection',
                    started_at=started_at + timedelta(minutes=5),
                    run_metadata={
                        'entrypoint': (
                            'PYKRX_BACKFILL_SERVICE.'
                            'run_daily_pipeline_if_due'
                        ),
                        'eod_collection_run_id': latest_eod_run.id,
                        'stage_statuses': {
                            'price_collection': 'completed',
                            'technical_snapshots': 'interrupted',
                        },
                    },
                )
            )

        with patch(
            'app.repositories.eod_run_repository.session_scope',
            self._session_scope,
        ):
            result = load_oldest_outstanding_auto_eod_recovery_date(
                on_or_after='2026-09-01',
                on_or_before='2026-09-14',
            )

        self.assertEqual(result, '2026-09-11')


class AdvisoryEODCollectionLockTests(unittest.TestCase):
    def test_transaction_lock_detects_live_session_lock(self):
        source_key, trade_key = eod_collection_advisory_lock_keys(
            market='KRX',
            trade_date='2099-12-30',
            source='pykrx',
        )
        parameters = {'source_key': source_key, 'trade_key': trade_key}
        with engine.connect() as raw_connection:
            owner = raw_connection.execution_options(isolation_level='AUTOCOMMIT')
            acquired = bool(
                owner.execute(
                    text(
                        'SELECT pg_try_advisory_lock('
                        ':source_key, :trade_key)'
                    ),
                    parameters,
                ).scalar()
            )
            self.assertTrue(acquired)
            try:
                with engine.connect() as contender:
                    transaction = contender.begin()
                    try:
                        contender_acquired = bool(
                            contender.execute(
                                text(
                                    'SELECT pg_try_advisory_xact_lock('
                                    ':source_key, :trade_key)'
                                ),
                                parameters,
                            ).scalar()
                        )
                    finally:
                        transaction.rollback()
                self.assertFalse(contender_acquired)
            finally:
                owner.execute(
                    text(
                        'SELECT pg_advisory_unlock('
                        ':source_key, :trade_key)'
                    ),
                    parameters,
                )
        with engine.connect() as contender:
            transaction = contender.begin()
            try:
                contender_acquired = bool(
                    contender.execute(
                        text(
                            'SELECT pg_try_advisory_xact_lock('
                            ':source_key, :trade_key)'
                        ),
                        parameters,
                    ).scalar()
                )
            finally:
                transaction.rollback()
        self.assertTrue(contender_acquired)

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
