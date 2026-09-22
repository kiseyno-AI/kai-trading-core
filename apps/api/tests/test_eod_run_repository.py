import unittest
from contextlib import contextmanager
from datetime import date, datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from sqlalchemy import create_engine, delete, event, select, text
from sqlalchemy.orm import sessionmaker

from app.db import engine, session_scope
from app.db_models import (
    AgentContextFeedbackRun,
    DailyPipelineRun,
    DailyPipelineStageRun,
    EODCollectionRun,
)
from app.repositories.eod_run_repository import (
    AutoEODRecoveryTarget,
    advisory_eod_collection_lock,
    complete_eod_collection_run,
    count_eod_run_persisted_symbols,
    create_eod_collection_run,
    eod_collection_advisory_lock_keys,
    list_eod_collection_runs,
    load_latest_eod_collection_run,
    load_latest_pykrx_enrichment_repair_checkpoint,
    load_automatic_daily_eod_collection_run_by_id,
    load_exact_automatic_daily_eod_collection_run,
    load_most_complete_eod_collection_run,
    load_oldest_outstanding_auto_eod_recovery_date,
    load_oldest_outstanding_auto_eod_recovery_target,
)


def _ready_pykrx_repair_details() -> dict:
    return {
        'result_summary': {
            'market_date': '2026-09-18',
            'status': 'completed',
            'point_in_time_recovery': True,
            'markets': ['KOSPI', 'KOSDAQ'],
            'symbols_requested': None,
            'coverage_scope': 'full_market',
            'downstream_preflight': {
                'market_date': '2026-09-18',
                'status': 'ready',
                'allowed': True,
                'required_categories': [
                    'sector_classification',
                    'investor_flow_symbol',
                    'short_selling',
                ],
                'category_row_counts': {
                    'sector_classification': 2762,
                    'investor_flow_symbol': 2565,
                    'short_selling': 2762,
                },
                'failed_categories': [],
                'blocked_categories': [],
                'waiting_categories': [],
                'zero_coverage_categories': [],
            },
        },
    }


def _ready_pykrx_warning_details() -> dict:
    categories = {
        'sector_classification': {
            'status': 'completed',
            'rows_persisted': 10,
            'errors': [],
        },
        'investor_flow_symbol': {
            'status': 'completed',
            'rows_persisted': 10,
            'errors': [],
        },
        'short_selling': {
            'status': 'partial',
            'rows_persisted': 10,
            'errors': ['empty provider frame'],
        },
    }
    return {
        'result_summary': {
            'status': 'completed',
            'point_in_time_recovery': True,
            'categories_requested': list(categories),
            'categories': categories,
            'downstream_preflight': {
                'status': 'ready',
                'allowed': True,
                'required_categories': list(categories),
                'category_row_counts': {
                    category: 10 for category in categories
                },
                'failed_categories': [],
                'blocked_categories': [],
                'waiting_categories': [],
                'zero_coverage_categories': [],
            },
        },
    }


class EODCollectionCoverageTests(unittest.TestCase):
    def test_list_eod_runs_uses_explicit_statement_timeout(self):
        session = MagicMock()
        session.execute.return_value.scalars.return_value = []
        session_context = MagicMock()
        session_context.__enter__.return_value = session

        with patch(
            'app.repositories.eod_run_repository.session_scope',
            return_value=session_context,
        ) as session_scope_mock:
            rows = list_eod_collection_runs(
                market='KRX',
                trade_date='2026-09-18',
                statement_timeout_ms=15_000,
            )

        self.assertEqual(rows, [])
        session_scope_mock.assert_called_once_with(statement_timeout_ms=15_000)

    def test_latest_eod_run_forwards_explicit_statement_timeout(self):
        with patch(
            'app.repositories.eod_run_repository.list_eod_collection_runs',
            return_value=[{'id': 522}],
        ) as list_runs_mock:
            result = load_latest_eod_collection_run(
                market='KRX',
                trade_date='2026-09-18',
                source='pykrx',
                statuses=['completed'],
                statement_timeout_ms=15_000,
            )

        self.assertEqual(result, {'id': 522})
        list_runs_mock.assert_called_once_with(
            market='KRX',
            trade_date='2026-09-18',
            source='pykrx',
            statuses=['completed'],
            limit=1,
            statement_timeout_ms=15_000,
        )

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
        AgentContextFeedbackRun.__table__.create(self.sqlite_engine)
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

    def test_recovery_target_preserves_selected_automatic_eod_run_id(
        self,
    ) -> None:
        started_at = datetime(2026, 9, 14, tzinfo=timezone.utc)
        with self.session_factory.begin() as session:
            automatic_run = EODCollectionRun(
                market='KRX',
                trade_date=date(2026, 9, 11),
                source='pykrx',
                trigger='auto_close_scheduler',
                status='completed_with_errors',
                symbols_total=2,
                symbols_succeeded=1,
                symbols_failed=1,
                started_at=started_at,
                run_metadata={'mode': 'daily_auto'},
            )
            session.add(automatic_run)
            session.flush()
            automatic_run_id = automatic_run.id
            session.add(
                EODCollectionRun(
                    market='KRX',
                    trade_date=date(2026, 9, 11),
                    source='pykrx',
                    trigger='manual_api',
                    status='completed',
                    symbols_total=2,
                    symbols_succeeded=2,
                    symbols_failed=0,
                    started_at=started_at + timedelta(minutes=1),
                    run_metadata={'mode': 'manual'},
                )
            )

        with patch(
            'app.repositories.eod_run_repository.session_scope',
            self._session_scope,
        ):
            target = load_oldest_outstanding_auto_eod_recovery_target(
                on_or_after='2026-09-11',
                on_or_before='2026-09-14',
            )

        self.assertEqual(
            target,
            AutoEODRecoveryTarget(
                trade_date='2026-09-11',
                eod_run_id=automatic_run_id,
            ),
        )

    def test_skips_followup_when_only_agent_feedback_is_missing(
        self,
    ) -> None:
        market_date = date(2026, 9, 21)
        started_at = datetime(2026, 9, 21, 7, tzinfo=timezone.utc)
        resolved_stages = [
            'technical_snapshots',
            'agent_feedback_summaries',
            'signal_decider',
            'next_open_execution_plan',
            'signal_decider_evaluations',
        ]
        with self.session_factory.begin() as session:
            eod_run = EODCollectionRun(
                market='KRX',
                trade_date=market_date,
                source='pykrx',
                trigger='auto_close_scheduler',
                status='completed',
                symbols_total=1,
                symbols_succeeded=1,
                symbols_failed=0,
                started_at=started_at,
                completed_at=started_at + timedelta(minutes=1),
                run_metadata={
                    'mode': 'daily_auto',
                    'followup_recovery_required': True,
                },
            )
            session.add(eod_run)
            session.flush()
            pipeline_run = DailyPipelineRun(
                market_date=market_date,
                trigger='auto_close_scheduler',
                status='failed',
                current_stage='agent_feedback_summaries',
                started_at=started_at,
                completed_at=started_at + timedelta(minutes=2),
                error_message='agent feedback unavailable',
                run_metadata={
                    'entrypoint': (
                        'PYKRX_BACKFILL_SERVICE.run_daily_pipeline_if_due'
                    ),
                    'eod_collection_run_id': eod_run.id,
                    'resolved_stages': resolved_stages,
                    'resumable': True,
                },
            )
            session.add(pipeline_run)
            session.flush()
            eod_run_id = eod_run.id
            pipeline_run_id = pipeline_run.id
            session.add_all(
                [
                    DailyPipelineStageRun(
                        pipeline_run_id=pipeline_run.id,
                        market_date=market_date,
                        stage_id='technical_snapshots',
                        status='completed',
                        processed=1,
                        total=1,
                        succeeded=1,
                        failed=0,
                        remaining=0,
                        started_at=started_at,
                        completed_at=started_at + timedelta(minutes=1),
                    ),
                    DailyPipelineStageRun(
                        pipeline_run_id=pipeline_run.id,
                        market_date=market_date,
                        stage_id='agent_feedback_summaries',
                        status='failed',
                        processed=1,
                        total=1,
                        succeeded=0,
                        failed=1,
                        remaining=0,
                        started_at=started_at + timedelta(minutes=1),
                        completed_at=started_at + timedelta(minutes=2),
                        error_message='agent feedback unavailable',
                    ),
                    *[
                        DailyPipelineStageRun(
                            pipeline_run_id=pipeline_run.id,
                            market_date=market_date,
                            stage_id=stage_id,
                            status='blocked',
                            processed=0,
                            total=0,
                            succeeded=0,
                            failed=0,
                            remaining=0,
                            started_at=started_at + timedelta(minutes=2),
                            completed_at=started_at + timedelta(minutes=2),
                        )
                        for stage_id in resolved_stages[2:]
                    ],
                ]
            )

        with patch(
            'app.repositories.eod_run_repository.session_scope',
            self._session_scope,
        ):
            target = load_oldest_outstanding_auto_eod_recovery_target(
                on_or_after=market_date,
                on_or_before=market_date,
            )

        self.assertIsNone(target)

        with self.session_factory.begin() as session:
            technical_stage = session.execute(
                select(DailyPipelineStageRun).where(
                    DailyPipelineStageRun.pipeline_run_id
                    == pipeline_run_id,
                    DailyPipelineStageRun.stage_id
                    == 'technical_snapshots',
                )
            ).scalar_one()
            technical_stage.status = 'failed'
            technical_stage.succeeded = 0
            technical_stage.failed = 1
            technical_stage.error_message = 'numeric snapshot unavailable'

        with patch(
            'app.repositories.eod_run_repository.session_scope',
            self._session_scope,
        ):
            numeric_target = (
                load_oldest_outstanding_auto_eod_recovery_target(
                    on_or_after=market_date,
                    on_or_before=market_date,
                )
            )

        self.assertEqual(
            numeric_target,
            AutoEODRecoveryTarget(
                trade_date=market_date.isoformat(),
                eod_run_id=eod_run_id,
            ),
        )

        with self.session_factory.begin() as session:
            eod_run = session.get(EODCollectionRun, eod_run_id)
            eod_run.run_metadata = {
                **eod_run.run_metadata,
                'downstream_retry_symbols': [],
                'downstream_retry_symbols_count': 1,
            }
            pipeline_run = session.get(DailyPipelineRun, pipeline_run_id)
            pipeline_run.trigger = 'auto_eod_followup_resume'
            pipeline_run.run_metadata = {
                **pipeline_run.run_metadata,
                'reused_eod_collection_run_id': eod_run_id,
            }
            technical_stage = session.execute(
                select(DailyPipelineStageRun).where(
                    DailyPipelineStageRun.pipeline_run_id
                    == pipeline_run_id,
                    DailyPipelineStageRun.stage_id
                    == 'technical_snapshots',
                )
            ).scalar_one()
            technical_stage.status = 'completed'
            technical_stage.succeeded = 1
            technical_stage.failed = 0
            technical_stage.error_message = None

        with patch(
            'app.repositories.eod_run_repository.session_scope',
            self._session_scope,
        ):
            settled_recovery_target = (
                load_oldest_outstanding_auto_eod_recovery_target(
                    on_or_after=market_date,
                    on_or_before=market_date,
                )
            )

        self.assertIsNone(settled_recovery_target)

    def test_exact_automatic_loader_ignores_newer_manual_and_historical_rows(
        self,
    ) -> None:
        started_at = datetime(2026, 9, 14, tzinfo=timezone.utc)
        with self.session_factory.begin() as session:
            automatic_run = EODCollectionRun(
                market='KRX',
                trade_date=date(2026, 9, 11),
                source='pykrx',
                trigger='auto_close_scheduler',
                status='completed_with_errors',
                symbols_total=2,
                symbols_succeeded=1,
                symbols_failed=1,
                started_at=started_at,
                run_metadata={'mode': 'daily_auto'},
            )
            session.add(automatic_run)
            session.flush()
            automatic_run_id = automatic_run.id
            session.add_all(
                [
                    EODCollectionRun(
                        market='KRX',
                        trade_date=date(2026, 9, 11),
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
                        trade_date=date(2026, 9, 11),
                        source='pykrx',
                        trigger='auto_close_scheduler',
                        status='completed',
                        symbols_total=2,
                        symbols_succeeded=2,
                        symbols_failed=0,
                        started_at=started_at + timedelta(minutes=2),
                        run_metadata={'mode': 'historical_backfill'},
                    ),
                ]
            )

        with patch(
            'app.repositories.eod_run_repository.session_scope',
            self._session_scope,
        ):
            loaded = load_exact_automatic_daily_eod_collection_run(
                eod_run_id=automatic_run_id,
                trade_date='2026-09-11',
            )

        self.assertIsNotNone(loaded)
        self.assertEqual(loaded['id'], automatic_run_id)
        self.assertEqual(loaded['status'], 'completed_with_errors')

    def test_exact_automatic_loader_rejects_superseded_automatic_target(
        self,
    ) -> None:
        started_at = datetime(2026, 9, 14, tzinfo=timezone.utc)
        with self.session_factory.begin() as session:
            selected_run = EODCollectionRun(
                market='KRX',
                trade_date=date(2026, 9, 11),
                source='pykrx',
                trigger='auto_close_scheduler',
                status='completed_with_errors',
                symbols_total=2,
                symbols_succeeded=1,
                symbols_failed=1,
                started_at=started_at,
                run_metadata={'mode': 'daily_auto'},
            )
            session.add(selected_run)
            session.flush()
            selected_run_id = selected_run.id
            session.add(
                EODCollectionRun(
                    market='KRX',
                    trade_date=date(2026, 9, 11),
                    source='pykrx',
                    trigger='auto_close_scheduler',
                    status='completed',
                    symbols_total=2,
                    symbols_succeeded=2,
                    symbols_failed=0,
                    started_at=started_at + timedelta(minutes=1),
                    run_metadata={'mode': 'daily_auto'},
                )
            )

        with patch(
            'app.repositories.eod_run_repository.session_scope',
            self._session_scope,
        ):
            loaded = load_exact_automatic_daily_eod_collection_run(
                eod_run_id=selected_run_id,
                trade_date='2026-09-11',
            )

        self.assertIsNone(loaded)

    def test_automatic_daily_loader_by_id_keeps_superseded_lineage_scoped(
        self,
    ) -> None:
        started_at = datetime(2026, 9, 14, tzinfo=timezone.utc)
        with self.session_factory.begin() as session:
            selected_run = EODCollectionRun(
                market='KRX',
                trade_date=date(2026, 9, 11),
                source='pykrx',
                trigger='auto_close_scheduler',
                status='completed_with_errors',
                symbols_total=2,
                symbols_succeeded=1,
                symbols_failed=1,
                started_at=started_at,
                run_metadata={'mode': 'daily_auto'},
            )
            manual_run = EODCollectionRun(
                market='KRX',
                trade_date=date(2026, 9, 11),
                source='pykrx',
                trigger='manual_api',
                status='completed',
                symbols_total=2,
                symbols_succeeded=2,
                symbols_failed=0,
                started_at=started_at + timedelta(minutes=1),
                run_metadata={'mode': 'manual'},
            )
            session.add_all([selected_run, manual_run])
            session.flush()
            selected_run_id = selected_run.id
            manual_run_id = manual_run.id
            session.add(
                EODCollectionRun(
                    market='KRX',
                    trade_date=date(2026, 9, 11),
                    source='pykrx',
                    trigger='auto_close_scheduler',
                    status='completed',
                    symbols_total=2,
                    symbols_succeeded=2,
                    symbols_failed=0,
                    started_at=started_at + timedelta(minutes=2),
                    run_metadata={'mode': 'daily_auto'},
                )
            )

        with patch(
            'app.repositories.eod_run_repository.session_scope',
            self._session_scope,
        ):
            loaded = load_automatic_daily_eod_collection_run_by_id(
                eod_run_id=selected_run_id,
                trade_date='2026-09-11',
            )
            manual = load_automatic_daily_eod_collection_run_by_id(
                eod_run_id=manual_run_id,
                trade_date='2026-09-11',
            )
            wrong_date = load_automatic_daily_eod_collection_run_by_id(
                eod_run_id=selected_run_id,
                trade_date='2026-09-12',
            )

        self.assertIsNotNone(loaded)
        self.assertEqual(loaded['id'], selected_run_id)
        self.assertEqual(loaded['trade_date'], '2026-09-11')
        self.assertEqual(loaded['status'], 'completed_with_errors')
        self.assertIsNone(manual)
        self.assertIsNone(wrong_date)

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

    def test_selects_running_fifth_auto_attempt_for_stale_reconciliation(
        self,
    ) -> None:
        started_at = datetime.now(timezone.utc) - timedelta(minutes=45)
        with self.session_factory.begin() as session:
            running_attempt = None
            for attempt in range(5):
                running_attempt = EODCollectionRun(
                    market='KRX',
                    trade_date=date(2026, 9, 10),
                    source='pykrx',
                    trigger='auto_close_scheduler',
                    status='running' if attempt == 4 else 'failed',
                    symbols_total=2,
                    symbols_succeeded=1,
                    symbols_failed=0 if attempt == 4 else 1,
                    started_at=started_at + timedelta(minutes=attempt),
                    run_metadata={
                        'mode': 'daily_auto',
                        'requested_symbols': ['005930', '000660'],
                        'failed_symbols': (
                            [] if attempt == 4 else ['000660']
                        ),
                    },
                )
                session.add(running_attempt)
            session.flush()
            running_attempt_id = running_attempt.id
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
            target = load_oldest_outstanding_auto_eod_recovery_target(
                on_or_after='2026-09-01',
                on_or_before='2026-09-14',
            )

        self.assertEqual(
            target,
            AutoEODRecoveryTarget(
                trade_date='2026-09-10',
                eod_run_id=running_attempt_id,
            ),
        )

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

    def test_exhausted_feedback_lease_loss_continuation_remains_selectable(
        self,
    ) -> None:
        market_date = date(2026, 9, 18)
        started_at = datetime(2026, 9, 20, tzinfo=timezone.utc)
        summary_child_run_id = 'feedback-cycle-2026-09-18-cafebabe00'
        child_run_id = 'feedback-cycle-2026-09-18-deadbeef01'
        lease_loss_error = (
            f'feedback retry lease ownership lost for {child_run_id} at '
            'before_parent_finalization: '
            'heartbeat_refresh_grace_exceeded'
        )
        suffix = [
            'agent_feedback_summaries',
            'signal_decider',
            'next_open_execution_plan',
            'signal_decider_evaluations',
        ]
        with self.session_factory.begin() as session:
            eod_run = EODCollectionRun(
                market='KRX',
                trade_date=market_date,
                source='pykrx',
                trigger='auto_close_scheduler',
                status='completed',
                symbols_total=1,
                symbols_succeeded=1,
                symbols_failed=0,
                started_at=started_at,
                run_metadata={
                    'mode': 'daily_auto',
                    'followup_recovery_required': True,
                },
            )
            session.add(eod_run)
            session.flush()
            prior_runs = [
                DailyPipelineRun(
                    market_date=market_date,
                    trigger='auto_eod_followup_resume',
                    status='failed',
                    current_stage='technical_snapshots',
                    started_at=started_at + timedelta(minutes=attempt),
                    completed_at=started_at + timedelta(minutes=attempt + 1),
                    run_metadata={
                        'reused_eod_collection_run_id': eod_run.id,
                    },
                )
                for attempt in range(5)
            ]
            session.add_all(prior_runs)
            session.flush()
            terminal_run = DailyPipelineRun(
                market_date=market_date,
                trigger='auto_eod_followup_resume',
                status='failed',
                current_stage='signal_decider',
                started_at=started_at + timedelta(minutes=5),
                completed_at=started_at + timedelta(minutes=6),
                error_message=lease_loss_error,
                run_metadata={
                    'entrypoint': 'daily_pipeline_stage_service.run',
                    'reused_eod_collection_run_id': eod_run.id,
                    'resolved_stages': suffix,
                    'resume_from_stage': 'agent_feedback_summaries',
                    'terminal_continuation_count': 1,
                    'terminal_continuation_version': 1,
                    'terminal_continuation_reason': (
                        'summary_knowledge_cutoff_repair'
                    ),
                    'terminal_continuation_claimed_at': (
                        '2026-09-20T00:05:00+00:00'
                    ),
                    'terminal_continuation_evidence': {},
                    'symbols': ['005930'],
                    'fs_div': 'CFS',
                    'reuse_existing_feedback': True,
                    'force': False,
                    'stage_statuses': {
                        stage_id: 'pending' for stage_id in suffix
                    },
                },
            )
            session.add(terminal_run)
            session.flush()
            terminal_run.run_metadata = {
                **terminal_run.run_metadata,
                'terminal_continuation_evidence': {
                    'lineage': {
                        'expected_run_count': 6,
                        'observed_run_count': 6,
                        'ordered_pipeline_run_ids': [
                            *(row.id for row in prior_runs),
                            terminal_run.id,
                        ],
                        'target_ordinal': 6,
                        'latest_pipeline_run_id': terminal_run.id,
                        'reused_eod_collection_run_id': eod_run.id,
                    },
                    'summary_proof': {
                        'knowledge_cutoff_at': (
                            '2026-09-20T00:55:00+00:00'
                        ),
                    },
                },
            }
            session.add_all(
                [
                    DailyPipelineStageRun(
                        pipeline_run_id=terminal_run.id,
                        market_date=market_date,
                        stage_id='agent_feedback_summaries',
                        status='completed',
                        processed=1,
                        total=1,
                        succeeded=1,
                        failed=0,
                        remaining=0,
                        started_at=started_at + timedelta(minutes=5),
                        completed_at=started_at + timedelta(minutes=5),
                        source_run_ids={
                            'agent_feedback_run_id': summary_child_run_id
                        },
                        details={'result_summary': {'status': 'completed'}},
                    ),
                    DailyPipelineStageRun(
                        pipeline_run_id=terminal_run.id,
                        market_date=market_date,
                        stage_id='signal_decider',
                        status='failed',
                        processed=20,
                        total=160,
                        succeeded=0,
                        failed=20,
                        remaining=140,
                        current_step='signal_decider_failed',
                        started_at=started_at + timedelta(minutes=5),
                        completed_at=started_at + timedelta(minutes=6),
                        error_message=lease_loss_error,
                        source_run_ids={
                            'agent_feedback_run_id': child_run_id
                        },
                        details={},
                    ),
                    *[
                        DailyPipelineStageRun(
                            pipeline_run_id=terminal_run.id,
                            market_date=market_date,
                            stage_id=stage_id,
                            status='pending',
                            processed=0,
                            total=0,
                            succeeded=0,
                            failed=0,
                            remaining=0,
                            current_step='terminal_continuation_pending',
                            started_at=started_at + timedelta(minutes=5),
                            source_run_ids={},
                            details={},
                        )
                        for stage_id in suffix[2:]
                    ],
                ]
            )
            session.add(
                AgentContextFeedbackRun(
                    run_id=summary_child_run_id,
                    market_date=market_date,
                    status='completed',
                    symbols_total=3,
                    completed_count=3,
                    failed_count=0,
                    request_payload={
                        'market_date': market_date.isoformat(),
                        'request_run_id': summary_child_run_id,
                        'mode': 'daily_retry_cycle_role_sequenced',
                        'roles': [
                            'eod-summarizer',
                            'technical-summarizer',
                            'fundamental-summarizer',
                        ],
                        'requested_symbols': ['005930'],
                        'feedback_scope_mode': (
                            'broad_feedback_universe'
                        ),
                        'admission_scope_by_role': {
                            'eod-summarizer': ['005930'],
                            'technical-summarizer': ['005930'],
                            'fundamental-summarizer': ['005930'],
                        },
                        'role_symbol_scopes': {
                            'eod-summarizer': ['005930'],
                            'technical-summarizer': ['005930'],
                            'fundamental-summarizer': ['005930'],
                        },
                        'role_symbol_counts': {
                            'eod-summarizer': 1,
                            'technical-summarizer': 1,
                            'fundamental-summarizer': 1,
                        },
                        'fs_div': 'CFS',
                        'reuse_existing': True,
                        'force_recompute': False,
                        'knowledge_cutoff_at': (
                            '2026-09-20T00:55:00+00:00'
                        ),
                        'admission_knowledge_cutoff_at': (
                            '2026-09-20T00:55:00+00:00'
                        ),
                        'requested_knowledge_cutoff_at': (
                            '2026-09-20T00:55:00+00:00'
                        ),
                    },
                    summary_payload={},
                    started_at=started_at + timedelta(minutes=4),
                    completed_at=started_at + timedelta(minutes=5),
                )
            )
            session.add(
                AgentContextFeedbackRun(
                    run_id=child_run_id,
                    market_date=market_date,
                    status='abandoned',
                    symbols_total=2,
                    completed_count=0,
                    failed_count=2,
                    request_payload={
                        'market_date': market_date.isoformat(),
                        'request_run_id': child_run_id,
                        'mode': 'daily_retry_cycle_role_sequenced',
                        'roles': [
                            'short-term-decider',
                            'signal-decider',
                        ],
                        'requested_symbols': ['005930'],
                        'feedback_scope_mode': (
                            'decider_scope_fast_path'
                        ),
                        'admission_scope_by_role': {
                            'short-term-decider': ['005930'],
                            'signal-decider': ['005930'],
                        },
                        'role_symbol_scopes': {
                            'short-term-decider': ['005930'],
                            'signal-decider': ['005930'],
                        },
                        'role_symbol_counts': {
                            'short-term-decider': 1,
                            'signal-decider': 1,
                        },
                        'fs_div': 'CFS',
                        'reuse_existing': True,
                        'force_recompute': False,
                        'knowledge_cutoff_at': (
                            '2026-09-20T00:55:00+00:00'
                        ),
                        'admission_knowledge_cutoff_at': (
                            '2026-09-20T00:55:00+00:00'
                        ),
                        'requested_knowledge_cutoff_at': (
                            '2026-09-20T00:55:00+00:00'
                        ),
                        'retry_admission': {
                            'schema_version': (
                                'agent_feedback_retry_admission_v1'
                            ),
                            'claimed_at': (
                                '2026-09-20T00:59:00+00:00'
                            ),
                            'owner_token_hash': 'a' * 64,
                        },
                    },
                    summary_payload={
                        'admission_lease_loss': {
                            'schema_version': (
                                'agent_feedback_retry_admission_lease_loss_v1'
                            ),
                            'detected_at': (
                                '2026-09-20T00:06:00+00:00'
                            ),
                            'stage': (
                                'before_checkpoint:role_batch_completed'
                            ),
                            'reason': 'heartbeat_refresh_grace_exceeded',
                            'owner_token_fingerprint': 'a' * 16,
                        }
                    },
                    started_at=started_at + timedelta(minutes=5),
                    completed_at=started_at + timedelta(minutes=6),
                )
            )
            eod_run_id = eod_run.id
            terminal_run_id = terminal_run.id

        with patch(
            'app.repositories.eod_run_repository.session_scope',
            self._session_scope,
        ):
            target = load_oldest_outstanding_auto_eod_recovery_target(
                on_or_after=market_date.isoformat(),
                on_or_before=market_date.isoformat(),
            )
        self.assertEqual(
            target,
            AutoEODRecoveryTarget(
                trade_date=market_date.isoformat(),
                eod_run_id=eod_run_id,
            ),
        )

        for adjacent_attempt_count in (5, 7):
            with (
                patch(
                    'app.repositories.eod_run_repository.session_scope',
                    self._session_scope,
                ),
                patch(
                    'app.repositories.eod_run_repository.'
                    '_count_auto_followup_recovery_attempts',
                    return_value=adjacent_attempt_count,
                ),
            ):
                adjacent_target = (
                    load_oldest_outstanding_auto_eod_recovery_target(
                        on_or_after=market_date.isoformat(),
                        on_or_before=market_date.isoformat(),
                    )
                )
            self.assertIsNone(adjacent_target)

        for malformed_resume_count in (1, '0', 'corrupt', True):
            with self.session_factory.begin() as session:
                terminal_run = session.get(
                    DailyPipelineRun,
                    terminal_run_id,
                )
                terminal_run.run_metadata = {
                    **terminal_run.run_metadata,
                    'terminal_continuation_resume_count': (
                        malformed_resume_count
                    ),
                }
            with patch(
                'app.repositories.eod_run_repository.session_scope',
                self._session_scope,
            ):
                malformed_target = (
                    load_oldest_outstanding_auto_eod_recovery_target(
                        on_or_after=market_date.isoformat(),
                        on_or_before=market_date.isoformat(),
                    )
                )
            self.assertIsNone(malformed_target)

        with self.session_factory.begin() as session:
            terminal_run = session.get(DailyPipelineRun, terminal_run_id)
            restored_metadata = dict(terminal_run.run_metadata)
            restored_metadata.pop(
                'terminal_continuation_resume_count',
                None,
            )
            terminal_run.run_metadata = restored_metadata

        with self.session_factory.begin() as session:
            terminal_run = session.get(DailyPipelineRun, terminal_run_id)
            terminal_run.error_message = 'upstream request failed'
            decider_stage = session.execute(
                select(DailyPipelineStageRun).where(
                    DailyPipelineStageRun.pipeline_run_id
                    == terminal_run_id,
                    DailyPipelineStageRun.stage_id == 'signal_decider',
                )
            ).scalar_one()
            decider_stage.error_message = 'upstream request failed'
        with patch(
            'app.repositories.eod_run_repository.session_scope',
            self._session_scope,
        ):
            rejected = load_oldest_outstanding_auto_eod_recovery_target(
                on_or_after=market_date.isoformat(),
                on_or_before=market_date.isoformat(),
            )
        self.assertIsNone(rejected)

        stale_activity_at = datetime(
            2026,
            9,
            20,
            16,
            0,
            tzinfo=timezone.utc,
        )
        with self.session_factory.begin() as session:
            terminal_run = session.get(DailyPipelineRun, terminal_run_id)
            terminal_run.status = 'running'
            terminal_run.current_stage = 'signal_decider'
            terminal_run.updated_at = stale_activity_at
            terminal_run.completed_at = None
            terminal_run.error_message = None
            terminal_run.run_metadata = {
                **terminal_run.run_metadata,
                'terminal_continuation_resume_count': 1,
                'terminal_continuation_resumed_from_status': 'failed',
                'terminal_continuation_resume_stage': 'signal_decider',
                'terminal_continuation_completed_prefix': [
                    'agent_feedback_summaries'
                ],
                'terminal_continuation_feedback_lease_loss_run_id': (
                    child_run_id
                ),
                'stage_statuses': {
                    'agent_feedback_summaries': 'completed',
                    'signal_decider': 'pending',
                    'next_open_execution_plan': 'pending',
                    'signal_decider_evaluations': 'pending',
                },
            }
            decider_stage = session.execute(
                select(DailyPipelineStageRun).where(
                    DailyPipelineStageRun.pipeline_run_id
                    == terminal_run_id,
                    DailyPipelineStageRun.stage_id == 'signal_decider',
                )
            ).scalar_one()
            decider_stage.status = 'running'
            decider_stage.processed = 0
            decider_stage.total = 0
            decider_stage.succeeded = 0
            decider_stage.failed = 0
            decider_stage.remaining = 0
            decider_stage.current_step = 'signal_decider_running'
            decider_stage.updated_at = stale_activity_at
            decider_stage.completed_at = None
            decider_stage.error_message = None
            decider_stage.source_run_ids = {}
            decider_stage.details = {}

        with patch(
            'app.repositories.eod_run_repository.session_scope',
            self._session_scope,
        ):
            stale_running_target = (
                load_oldest_outstanding_auto_eod_recovery_target(
                    on_or_after=market_date.isoformat(),
                    on_or_before=market_date.isoformat(),
                )
            )
        self.assertIsNone(stale_running_target)

        interrupted_at = stale_activity_at + timedelta(minutes=1)
        with self.session_factory.begin() as session:
            terminal_run = session.get(DailyPipelineRun, terminal_run_id)
            terminal_run.status = 'interrupted'
            terminal_run.updated_at = interrupted_at
            terminal_run.completed_at = interrupted_at
            terminal_run.error_message = 'Core restarted.'
            terminal_run.run_metadata = {
                **terminal_run.run_metadata,
                'resumable': True,
                'interrupted_reason': 'Core restarted.',
                'interrupted_at': interrupted_at.isoformat(),
            }
            decider_stage = session.execute(
                select(DailyPipelineStageRun).where(
                    DailyPipelineStageRun.pipeline_run_id
                    == terminal_run_id,
                    DailyPipelineStageRun.stage_id == 'signal_decider',
                )
            ).scalar_one()
            decider_stage.status = 'interrupted'
            decider_stage.current_step = 'core_restart_interrupted'
            decider_stage.updated_at = interrupted_at
            decider_stage.completed_at = interrupted_at
            decider_stage.error_message = 'Core restarted.'
            decider_stage.details = {
                'resumable': True,
                'interrupted_reason': 'Core restarted.',
            }

        with patch(
            'app.repositories.eod_run_repository.session_scope',
            self._session_scope,
        ):
            interrupted_target = (
                load_oldest_outstanding_auto_eod_recovery_target(
                    on_or_after=market_date.isoformat(),
                    on_or_before=market_date.isoformat(),
                )
            )
        self.assertEqual(
            interrupted_target,
            AutoEODRecoveryTarget(
                trade_date=market_date.isoformat(),
                eod_run_id=eod_run_id,
            ),
        )

        with self.session_factory.begin() as session:
            terminal_run = session.get(DailyPipelineRun, terminal_run_id)
            terminal_run.run_metadata = {
                **terminal_run.run_metadata,
                'terminal_continuation_resume_count': 2,
                'terminal_continuation_resumed_from_status': 'interrupted',
            }
        with patch(
            'app.repositories.eod_run_repository.session_scope',
            self._session_scope,
        ):
            repeated_restart_target = (
                load_oldest_outstanding_auto_eod_recovery_target(
                    on_or_after=market_date.isoformat(),
                    on_or_before=market_date.isoformat(),
                )
            )
        self.assertEqual(
            repeated_restart_target,
            AutoEODRecoveryTarget(
                trade_date=market_date.isoformat(),
                eod_run_id=eod_run_id,
            ),
        )

        with self.session_factory.begin() as session:
            summary_child_run = session.execute(
                select(AgentContextFeedbackRun).where(
                    AgentContextFeedbackRun.run_id
                    == summary_child_run_id
                )
            ).scalar_one()
            summary_child_run.failed_count = 1
        with patch(
            'app.repositories.eod_run_repository.session_scope',
            self._session_scope,
        ):
            invalid_recorded_child_target = (
                load_oldest_outstanding_auto_eod_recovery_target(
                    on_or_after=market_date.isoformat(),
                    on_or_before=market_date.isoformat(),
                )
            )
        self.assertIsNone(invalid_recorded_child_target)
        with self.session_factory.begin() as session:
            summary_child_run = session.execute(
                select(AgentContextFeedbackRun).where(
                    AgentContextFeedbackRun.run_id
                    == summary_child_run_id
                )
            ).scalar_one()
            summary_child_run.failed_count = 0

        advanced_interrupted_at = interrupted_at + timedelta(minutes=1)
        advanced_child_run_id = 'feedback-cycle-2026-09-18-feedface02'
        with self.session_factory.begin() as session:
            terminal_run = session.get(DailyPipelineRun, terminal_run_id)
            terminal_run.current_stage = 'next_open_execution_plan'
            terminal_run.updated_at = advanced_interrupted_at
            terminal_run.completed_at = advanced_interrupted_at
            terminal_run.error_message = 'Core restarted.'
            terminal_run.run_metadata = {
                **terminal_run.run_metadata,
                'interrupted_at': advanced_interrupted_at.isoformat(),
            }
            stage_rows = {
                stage.stage_id: stage
                for stage in session.execute(
                    select(DailyPipelineStageRun).where(
                        DailyPipelineStageRun.pipeline_run_id
                        == terminal_run_id
                    )
                ).scalars()
            }
            decider_stage = stage_rows['signal_decider']
            decider_stage.status = 'completed'
            decider_stage.processed = 160
            decider_stage.total = 160
            decider_stage.succeeded = 160
            decider_stage.failed = 0
            decider_stage.remaining = 0
            decider_stage.current_step = 'signal_decider_completed'
            decider_stage.updated_at = advanced_interrupted_at
            decider_stage.completed_at = advanced_interrupted_at
            decider_stage.error_message = None
            decider_stage.source_run_ids = {
                'agent_feedback_run_id': advanced_child_run_id
            }
            decider_stage.details = {
                'result_summary': {'status': 'completed'}
            }
            plan_stage = stage_rows['next_open_execution_plan']
            plan_stage.status = 'interrupted'
            plan_stage.current_step = 'core_restart_interrupted'
            plan_stage.updated_at = advanced_interrupted_at
            plan_stage.completed_at = advanced_interrupted_at
            plan_stage.error_message = 'Core restarted.'
            plan_stage.details = {
                'resumable': True,
                'interrupted_reason': 'Core restarted.',
            }
            session.add(
                AgentContextFeedbackRun(
                    run_id=advanced_child_run_id,
                    market_date=market_date,
                    status='completed',
                    symbols_total=2,
                    completed_count=2,
                    failed_count=0,
                    request_payload={
                        'market_date': market_date.isoformat(),
                        'request_run_id': advanced_child_run_id,
                        'mode': 'daily_retry_cycle_role_sequenced',
                        'roles': [
                            'short-term-decider',
                            'signal-decider',
                        ],
                        'requested_symbols': ['005930'],
                        'feedback_scope_mode': (
                            'decider_scope_fast_path'
                        ),
                        'admission_scope_by_role': {
                            'short-term-decider': ['005930'],
                            'signal-decider': ['005930'],
                        },
                        'role_symbol_scopes': {
                            'short-term-decider': ['005930'],
                            'signal-decider': ['005930'],
                        },
                        'role_symbol_counts': {
                            'short-term-decider': 1,
                            'signal-decider': 1,
                        },
                        'fs_div': 'CFS',
                        'reuse_existing': True,
                        'force_recompute': False,
                        'knowledge_cutoff_at': (
                            '2026-09-20T00:55:00+00:00'
                        ),
                        'admission_knowledge_cutoff_at': (
                            '2026-09-20T00:55:00+00:00'
                        ),
                        'requested_knowledge_cutoff_at': (
                            '2026-09-20T00:55:00+00:00'
                        ),
                    },
                    summary_payload={},
                    started_at=interrupted_at,
                    completed_at=advanced_interrupted_at,
                )
            )

        with patch(
            'app.repositories.eod_run_repository.session_scope',
            self._session_scope,
        ):
            advanced_restart_target = (
                load_oldest_outstanding_auto_eod_recovery_target(
                    on_or_after=market_date.isoformat(),
                    on_or_before=market_date.isoformat(),
                )
            )
        self.assertEqual(
            advanced_restart_target,
            AutoEODRecoveryTarget(
                trade_date=market_date.isoformat(),
                eod_run_id=eod_run_id,
            ),
        )

        with self.session_factory.begin() as session:
            advanced_child_run = session.execute(
                select(AgentContextFeedbackRun).where(
                    AgentContextFeedbackRun.run_id
                    == advanced_child_run_id
                )
            ).scalar_one()
            request_payload = dict(advanced_child_run.request_payload)
            request_payload['role_symbol_scopes'] = {
                **request_payload['role_symbol_scopes'],
                'signal-decider': [['005930']],
            }
            advanced_child_run.request_payload = request_payload
        with patch(
            'app.repositories.eod_run_repository.session_scope',
            self._session_scope,
        ):
            malformed_advanced_child_target = (
                load_oldest_outstanding_auto_eod_recovery_target(
                    on_or_after=market_date.isoformat(),
                    on_or_before=market_date.isoformat(),
                )
            )
        self.assertIsNone(malformed_advanced_child_target)
        with self.session_factory.begin() as session:
            advanced_child_run = session.execute(
                select(AgentContextFeedbackRun).where(
                    AgentContextFeedbackRun.run_id
                    == advanced_child_run_id
                )
            ).scalar_one()
            request_payload = dict(advanced_child_run.request_payload)
            request_payload['role_symbol_scopes'] = {
                **request_payload['role_symbol_scopes'],
                'signal-decider': ['005930'],
            }
            advanced_child_run.request_payload = request_payload

        with self.session_factory.begin() as session:
            session.execute(
                delete(AgentContextFeedbackRun).where(
                    AgentContextFeedbackRun.run_id
                    == advanced_child_run_id
                )
            )
        with patch(
            'app.repositories.eod_run_repository.session_scope',
            self._session_scope,
        ):
            invalid_advanced_child_target = (
                load_oldest_outstanding_auto_eod_recovery_target(
                    on_or_after=market_date.isoformat(),
                    on_or_before=market_date.isoformat(),
                )
            )
        self.assertIsNone(invalid_advanced_child_target)

        with self.session_factory.begin() as session:
            terminal_run = session.get(DailyPipelineRun, terminal_run_id)
            terminal_run.current_stage = 'signal_decider'
            terminal_run.updated_at = interrupted_at
            terminal_run.completed_at = interrupted_at
            terminal_run.error_message = 'Core restarted.'
            terminal_run.run_metadata = {
                **terminal_run.run_metadata,
                'interrupted_at': interrupted_at.isoformat(),
            }
            stage_rows = {
                stage.stage_id: stage
                for stage in session.execute(
                    select(DailyPipelineStageRun).where(
                        DailyPipelineStageRun.pipeline_run_id
                        == terminal_run_id
                    )
                ).scalars()
            }
            decider_stage = stage_rows['signal_decider']
            decider_stage.status = 'interrupted'
            decider_stage.processed = 0
            decider_stage.total = 0
            decider_stage.succeeded = 0
            decider_stage.failed = 0
            decider_stage.remaining = 0
            decider_stage.current_step = 'core_restart_interrupted'
            decider_stage.updated_at = interrupted_at
            decider_stage.completed_at = interrupted_at
            decider_stage.error_message = 'Core restarted.'
            decider_stage.source_run_ids = {}
            decider_stage.details = {
                'resumable': True,
                'interrupted_reason': 'Core restarted.',
            }
            plan_stage = stage_rows['next_open_execution_plan']
            plan_stage.status = 'pending'
            plan_stage.current_step = 'terminal_continuation_pending'
            plan_stage.updated_at = interrupted_at
            plan_stage.completed_at = None
            plan_stage.error_message = None
            plan_stage.details = {}

        for invalid_resume_count, invalid_origin in (
            (2, 'failed'),
            (1, 'running'),
            (1, 'interrupted'),
        ):
            with self.session_factory.begin() as session:
                terminal_run = session.get(
                    DailyPipelineRun,
                    terminal_run_id,
                )
                terminal_run.run_metadata = {
                    **terminal_run.run_metadata,
                    'terminal_continuation_resume_count': (
                        invalid_resume_count
                    ),
                    'terminal_continuation_resumed_from_status': (
                        invalid_origin
                    ),
                }
            with patch(
                'app.repositories.eod_run_repository.session_scope',
                self._session_scope,
            ):
                invalid_resume_target = (
                    load_oldest_outstanding_auto_eod_recovery_target(
                        on_or_after=market_date.isoformat(),
                        on_or_before=market_date.isoformat(),
                    )
                )
            self.assertIsNone(invalid_resume_target)

        with self.session_factory.begin() as session:
            terminal_run = session.get(DailyPipelineRun, terminal_run_id)
            terminal_run.run_metadata = {
                **terminal_run.run_metadata,
                'terminal_continuation_resume_count': 1,
                'terminal_continuation_resumed_from_status': 'failed',
            }

        with self.session_factory.begin() as session:
            terminal_run = session.get(DailyPipelineRun, terminal_run_id)
            terminal_run.run_metadata = {
                **terminal_run.run_metadata,
                'resumable': False,
            }
        with patch(
            'app.repositories.eod_run_repository.session_scope',
            self._session_scope,
        ):
            invalid_interrupted_target = (
                load_oldest_outstanding_auto_eod_recovery_target(
                    on_or_after=market_date.isoformat(),
                    on_or_before=market_date.isoformat(),
                )
            )
        self.assertIsNone(invalid_interrupted_target)

        with self.session_factory.begin() as session:
            terminal_run = session.get(DailyPipelineRun, terminal_run_id)
            terminal_run.run_metadata = {
                **terminal_run.run_metadata,
                'resumable': True,
            }
            child_run = session.execute(
                select(AgentContextFeedbackRun).where(
                    AgentContextFeedbackRun.run_id == child_run_id
                )
            ).scalar_one()
            child_run.summary_payload = {
                'admission_lease_loss': {
                    **child_run.summary_payload['admission_lease_loss'],
                    'owner_token_fingerprint': 'b' * 16,
                }
            }
        with patch(
            'app.repositories.eod_run_repository.session_scope',
            self._session_scope,
        ):
            tampered_child_target = (
                load_oldest_outstanding_auto_eod_recovery_target(
                    on_or_after=market_date.isoformat(),
                    on_or_before=market_date.isoformat(),
                )
            )
        self.assertIsNone(tampered_child_target)

        with self.session_factory.begin() as session:
            session.execute(
                delete(AgentContextFeedbackRun).where(
                    AgentContextFeedbackRun.run_id == child_run_id
                )
            )
        with patch(
            'app.repositories.eod_run_repository.session_scope',
            self._session_scope,
        ):
            missing_child_target = (
                load_oldest_outstanding_auto_eod_recovery_target(
                    on_or_after=market_date.isoformat(),
                    on_or_before=market_date.isoformat(),
                )
            )
        self.assertIsNone(missing_child_target)

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
                prior_attempts = 5 if eod_run is convergence_eod else 4
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
                        for attempt in range(prior_attempts)
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
                        'pykrx_enrichment',
                        'next_open_execution_plan',
                    ],
                    'terminal_continuation_count': 1,
                    'terminal_continuation_version': 1,
                    'terminal_continuation_reason': (
                        'summary_knowledge_cutoff_repair'
                    ),
                    'terminal_continuation_claimed_at': (
                        '2026-09-14T00:03:00+00:00'
                    ),
                    'terminal_continuation_evidence': {},
                    'resume_from_stage': 'agent_feedback_summaries',
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
                        stage_id='pykrx_enrichment',
                        status='completed',
                        processed=3,
                        total=3,
                        succeeded=2,
                        failed=1,
                        remaining=0,
                        started_at=started_at + timedelta(minutes=4),
                        completed_at=started_at + timedelta(minutes=5),
                        details=_ready_pykrx_warning_details(),
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

    def test_fresh_ready_enrichment_repair_rearms_one_exhausted_followup(
        self,
    ) -> None:
        started_at = datetime(2026, 9, 18, tzinfo=timezone.utc)
        with self.session_factory.begin() as session:
            latest_eod_run = None
            for attempt in range(5):
                latest_eod_run = EODCollectionRun(
                    market='KRX',
                    trade_date=date(2026, 9, 18),
                    source='pykrx',
                    trigger='auto_close_scheduler',
                    status='failed',
                    symbols_total=5,
                    symbols_succeeded=0,
                    symbols_failed=5,
                    started_at=started_at + timedelta(minutes=attempt),
                    run_metadata={
                        'mode': 'daily_auto',
                        'markets': ['KOSPI', 'KOSDAQ'],
                        'failed_symbols': [
                            '006380',
                            '121850',
                            '465320',
                            '465780',
                            '472220',
                        ],
                        'downstream_retry_symbols': ['005930', '000660'],
                        'downstream_retry_resume_from_stage': (
                            'pykrx_enrichment'
                        ),
                    },
                )
                session.add(latest_eod_run)
            session.flush()
            latest_eod_run_id = int(latest_eod_run.id)

            for attempt in range(5):
                recovery_started_at = started_at + timedelta(
                    minutes=10 + attempt
                )
                session.add(
                    DailyPipelineRun(
                        market_date=date(2026, 9, 18),
                        trigger='auto_eod_followup_resume',
                        status='failed',
                        current_stage='pykrx_enrichment',
                        started_at=recovery_started_at,
                        completed_at=recovery_started_at + timedelta(seconds=30),
                        run_metadata={
                            'reused_eod_collection_run_id': latest_eod_run_id,
                            'resolved_stages': ['pykrx_enrichment'],
                        },
                    )
                )

            repair_started_at = started_at + timedelta(minutes=20)
            repair_run = DailyPipelineRun(
                market_date=date(2026, 9, 18),
                trigger='internal_ops_cli',
                status='completed',
                current_stage='pykrx_enrichment',
                started_at=repair_started_at,
                completed_at=repair_started_at + timedelta(minutes=1),
                run_metadata={
                    'resolved_stages': ['pykrx_enrichment'],
                    'markets': ['KOSPI', 'KOSDAQ'],
                    'point_in_time_recovery': True,
                },
            )
            session.add(repair_run)
            session.flush()
            session.add(
                DailyPipelineStageRun(
                    pipeline_run_id=repair_run.id,
                    market_date=date(2026, 9, 18),
                    stage_id='pykrx_enrichment',
                    status='completed',
                    processed=7,
                    total=7,
                    succeeded=6,
                    failed=1,
                    started_at=repair_started_at,
                    completed_at=repair_started_at + timedelta(minutes=1),
                    details=_ready_pykrx_repair_details(),
                )
            )

        with patch(
            'app.repositories.eod_run_repository.session_scope',
            self._session_scope,
        ):
            target = load_oldest_outstanding_auto_eod_recovery_target(
                on_or_after='2026-09-18',
                on_or_before='2026-09-18',
            )

        self.assertEqual(
            target,
            AutoEODRecoveryTarget(
                trade_date='2026-09-18',
                eod_run_id=latest_eod_run_id,
            ),
        )

        with self.session_factory.begin() as session:
            consumed_at = started_at + timedelta(minutes=22)
            session.add(
                DailyPipelineRun(
                    market_date=date(2026, 9, 18),
                    trigger='auto_eod_followup_resume',
                    status='failed',
                    current_stage='pykrx_enrichment',
                    started_at=consumed_at,
                    completed_at=consumed_at + timedelta(seconds=30),
                    run_metadata={
                        'reused_eod_collection_run_id': latest_eod_run_id,
                        'resolved_stages': ['pykrx_enrichment'],
                    },
                )
            )

        with (
            patch(
                'app.repositories.eod_run_repository.session_scope',
                self._session_scope,
            ),
            patch(
                'app.repositories.eod_run_repository.'
                '_load_latest_pykrx_enrichment_repair_checkpoint',
                return_value={'pipeline_run_id': 999},
            ) as load_repair_checkpoint,
        ):
            consumed_target = load_oldest_outstanding_auto_eod_recovery_target(
                on_or_after='2026-09-18',
                on_or_before='2026-09-18',
            )

        self.assertIsNone(consumed_target)
        load_repair_checkpoint.assert_not_called()

    def test_repair_checkpoint_requires_fresh_stage_and_parent(self) -> None:
        cutoff = datetime(2026, 9, 18, 12, tzinfo=timezone.utc)
        with self.session_factory.begin() as session:
            repair_run = DailyPipelineRun(
                market_date=date(2026, 9, 18),
                trigger='internal_ops_cli',
                status='completed',
                current_stage='pykrx_enrichment',
                started_at=cutoff - timedelta(minutes=10),
                completed_at=cutoff + timedelta(minutes=10),
                run_metadata={
                    'resolved_stages': ['pykrx_enrichment'],
                    'markets': ['KOSPI', 'KOSDAQ'],
                    'point_in_time_recovery': True,
                },
            )
            session.add(repair_run)
            session.flush()
            repair_stage = DailyPipelineStageRun(
                pipeline_run_id=repair_run.id,
                market_date=date(2026, 9, 18),
                stage_id='pykrx_enrichment',
                status='completed',
                processed=7,
                total=7,
                succeeded=6,
                failed=1,
                started_at=cutoff - timedelta(minutes=10),
                completed_at=cutoff - timedelta(minutes=1),
                details=_ready_pykrx_repair_details(),
            )
            session.add(repair_stage)
            session.flush()
            repair_stage_id = int(repair_stage.id)

        with patch(
            'app.repositories.eod_run_repository.session_scope',
            self._session_scope,
        ):
            checkpoint = load_latest_pykrx_enrichment_repair_checkpoint(
                trade_date='2026-09-18',
                markets=('KOSPI', 'KOSDAQ'),
                completed_after=cutoff,
            )

        self.assertIsNone(checkpoint)

        with patch(
            'app.repositories.eod_run_repository.session_scope',
            self._session_scope,
        ):
            invalid_cutoff_checkpoint = (
                load_latest_pykrx_enrichment_repair_checkpoint(
                    trade_date='2026-09-18',
                    markets=('KOSPI', 'KOSDAQ'),
                    completed_after='not-a-timestamp',
                )
            )

        self.assertIsNone(invalid_cutoff_checkpoint)

        with self.session_factory.begin() as session:
            repair_stage = session.get(
                DailyPipelineStageRun,
                repair_stage_id,
            )
            repair_stage.completed_at = cutoff + timedelta(minutes=5)
            malformed_details = dict(repair_stage.details or {})
            malformed_summary = dict(
                malformed_details.get('result_summary') or {}
            )
            malformed_preflight = dict(
                malformed_summary.get('downstream_preflight') or {}
            )
            malformed_preflight['required_categories'] = None
            malformed_summary['downstream_preflight'] = malformed_preflight
            malformed_details['result_summary'] = malformed_summary
            repair_stage.details = malformed_details

        with patch(
            'app.repositories.eod_run_repository.session_scope',
            self._session_scope,
        ):
            malformed_checkpoint = (
                load_latest_pykrx_enrichment_repair_checkpoint(
                    trade_date='2026-09-18',
                    markets=('KOSPI', 'KOSDAQ'),
                    completed_after=cutoff,
                )
            )

        self.assertIsNone(malformed_checkpoint)

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

    def test_exhausted_price_date_with_incomplete_primary_pipeline_does_not_starve_newer_date(
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
            session.add(
                EODCollectionRun(
                    market='KRX',
                    trade_date=date(2026, 9, 12),
                    source='pykrx',
                    trigger='auto_close_scheduler',
                    status='failed',
                    symbols_total=2,
                    symbols_succeeded=1,
                    symbols_failed=1,
                    started_at=started_at + timedelta(minutes=6),
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

        self.assertEqual(result, '2026-09-12')

    def test_exhausted_price_date_ignores_primary_non_price_metadata_checkpoint(
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

        self.assertIsNone(result)


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
