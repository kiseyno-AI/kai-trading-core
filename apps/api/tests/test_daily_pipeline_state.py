import unittest
from contextlib import contextmanager
from datetime import date, datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.db_models import DailyPipelineRun, DailyPipelineStageRun, EODCollectionRun
from app.eod.daily_pipeline_state import DailyPipelineRunStateService


class DailyPipelineRunStateServiceTests(unittest.TestCase):
    def test_startup_interrupt_marks_linked_eod_run_retryable_immediately(
        self,
    ) -> None:
        now = datetime(2026, 9, 14, 7, 34, 11, tzinfo=timezone.utc)
        run = SimpleNamespace(
            id=387,
            status='running',
            error_message=None,
            completed_at=None,
            updated_at=now - timedelta(seconds=6),
            run_metadata={'mode': 'daily_auto'},
        )
        stage = SimpleNamespace(
            status='running',
            processed=3_148,
            total=3_549,
            remaining=3_549,
            error_message=None,
            current_step='fetch_symbol_ohlcv',
            current_activity='Collecting prices',
            completed_at=None,
            updated_at=now - timedelta(seconds=6),
            source_run_ids={'eod_run_id': 522},
            details={},
        )
        eod_run = SimpleNamespace(
            id=522,
            status='running',
            symbols_total=3_549,
            symbols_succeeded=3_147,
            symbols_failed=1,
            error_message=None,
            completed_at=None,
            run_metadata={
                'requested_symbols': ['005930', '000660'],
                'failed_symbols': ['121850'],
                'unattempted_symbols': ['319660'],
                'stage_progress': {
                    'price_collection': {
                        'status': 'running',
                        'current_step': 'fetch_symbol_ohlcv',
                    }
                },
            },
        )
        runs_result = MagicMock()
        runs_result.scalars.return_value = [run]
        stages_result = MagicMock()
        stages_result.scalars.return_value = [stage]
        session = MagicMock()
        session.execute.side_effect = [runs_result, stages_result]
        session.get.return_value = eod_run
        session_context = MagicMock()
        session_context.__enter__.return_value = session
        reason = 'Core process started while a daily pipeline was running.'

        with (
            patch(
                'app.eod.daily_pipeline_state.session_scope',
                return_value=session_context,
            ),
            patch('app.eod.daily_pipeline_state._utcnow', return_value=now),
        ):
            result = DailyPipelineRunStateService().mark_active_runs_interrupted(
                reason=reason
            )

        self.assertEqual(result['run_ids'], [387])
        self.assertEqual(result['interrupted_eod_run_ids'], [522])
        self.assertEqual(stage.remaining, 401)
        session.get.assert_called_once_with(EODCollectionRun, 522)
        self.assertEqual(eod_run.status, 'failed')
        self.assertEqual(eod_run.symbols_succeeded, 3_147)
        self.assertEqual(eod_run.symbols_failed, 402)
        self.assertEqual(eod_run.completed_at, now)
        self.assertEqual(eod_run.error_message, reason)
        self.assertTrue(eod_run.run_metadata['resumable'])
        self.assertEqual(
            eod_run.run_metadata['stage_progress']['price_collection']['status'],
            'failed',
        )
        self.assertEqual(
            eod_run.run_metadata['stage_progress']['price_collection'][
                'current_step'
            ],
            'core_restart_interrupted',
        )

    def test_latest_run_without_market_date_uses_actual_newest_run(self) -> None:
        engine = create_engine('sqlite+pysqlite:///:memory:')
        DailyPipelineRun.__table__.create(engine)
        DailyPipelineStageRun.__table__.create(engine)
        session_factory = sessionmaker(bind=engine, future=True)
        started_at = datetime(2026, 9, 12, tzinfo=timezone.utc)
        with session_factory.begin() as session:
            session.add_all(
                [
                    DailyPipelineRun(
                        market_date=date(2026, 9, 8),
                        trigger='auto_close_scheduler',
                        status='completed',
                        started_at=started_at,
                        run_metadata={},
                    ),
                    DailyPipelineRun(
                        market_date=date(2026, 9, 11),
                        trigger='manual_daily_api',
                        status='partial',
                        started_at=started_at + timedelta(hours=1),
                        run_metadata={},
                    ),
                ]
            )

        @contextmanager
        def _session_scope():
            with session_factory.begin() as session:
                yield session

        try:
            with patch('app.eod.daily_pipeline_state.session_scope', _session_scope):
                result = DailyPipelineRunStateService().get_latest_run(
                    market_date=None
                )
        finally:
            engine.dispose()

        self.assertIsNotNone(result)
        self.assertEqual(result['market_date'], '2026-09-11')
        self.assertEqual(result['trigger'], 'manual_daily_api')
        self.assertEqual(result['status'], 'partial')

    def test_context_lookup_filters_before_limit_with_more_than_100_unrelated_runs(self) -> None:
        engine = create_engine('sqlite+pysqlite:///:memory:')
        DailyPipelineRun.__table__.create(engine)
        DailyPipelineStageRun.__table__.create(engine)
        session_factory = sessionmaker(bind=engine, future=True)
        market_date = date(2026, 9, 5)
        started_at = datetime(2026, 9, 5, tzinfo=timezone.utc)
        with session_factory.begin() as session:
            session.add(
                DailyPipelineRun(
                    market_date=market_date,
                    trigger='auto_close_scheduler',
                    status='partial',
                    started_at=started_at,
                    run_metadata={
                        'entrypoint': 'PYKRX_BACKFILL_SERVICE.run_daily_pipeline_if_due'
                    },
                )
            )
            session.add_all(
                [
                    DailyPipelineRun(
                        market_date=market_date,
                        trigger='manual_api',
                        status='completed',
                        started_at=started_at + timedelta(minutes=index + 1),
                        run_metadata={'entrypoint': 'daily_pipeline_stage_service.run'},
                    )
                    for index in range(101)
                ]
            )

        @contextmanager
        def _session_scope():
            with session_factory.begin() as session:
                yield session

        try:
            with patch('app.eod.daily_pipeline_state.session_scope', _session_scope):
                result = DailyPipelineRunStateService().get_latest_run_for_context(
                    market_date=market_date,
                    triggers=('auto_eod_followup_resume',),
                    entrypoints=(
                        'PYKRX_BACKFILL_SERVICE.run_daily_pipeline_if_due',
                    ),
                    search_limit=100,
                )
        finally:
            engine.dispose()

        self.assertIsNotNone(result)
        self.assertEqual(result['trigger'], 'auto_close_scheduler')
        self.assertEqual(result['status'], 'partial')

    def test_context_lookup_ignores_newer_unrelated_manual_run(self) -> None:
        now = datetime(2026, 9, 6, tzinfo=timezone.utc)
        manual_run = SimpleNamespace(
            id=402,
            market_date=date(2026, 9, 5),
            trigger='manual_api',
            status='completed',
            current_stage=None,
            started_at=now,
            updated_at=now,
            completed_at=now,
            error_message=None,
            run_metadata={'entrypoint': 'daily_pipeline_stage_service.run'},
        )
        automatic_run = SimpleNamespace(
            id=401,
            market_date=date(2026, 9, 5),
            trigger='auto_close_scheduler',
            status='partial',
            current_stage='signal_decider',
            started_at=now - timedelta(minutes=5),
            updated_at=now - timedelta(minutes=4),
            completed_at=now - timedelta(minutes=4),
            error_message=None,
            run_metadata={
                'entrypoint': 'PYKRX_BACKFILL_SERVICE.run_daily_pipeline_if_due',
                'resolved_stages': ['signal_decider'],
            },
        )
        runs_result = MagicMock()
        runs_result.scalars.return_value = [manual_run, automatic_run]
        stages_result = MagicMock()
        stages_result.scalars.return_value = []
        session = MagicMock()
        session.execute.side_effect = [runs_result, stages_result]
        session_context = MagicMock()
        session_context.__enter__.return_value = session

        with patch('app.eod.daily_pipeline_state.session_scope', return_value=session_context):
            result = DailyPipelineRunStateService().get_latest_run_for_context(
                market_date='2026-09-05',
                triggers=('auto_eod_followup_resume',),
                entrypoints=('PYKRX_BACKFILL_SERVICE.run_daily_pipeline_if_due',),
            )

        self.assertIsNotNone(result)
        self.assertEqual(result['id'], 401)
        self.assertEqual(result['status'], 'partial')

    def test_count_runs_uses_exact_trigger(self) -> None:
        run_ids_result = MagicMock()
        run_ids_result.scalars.return_value = [501, 502, 503]
        session = MagicMock()
        session.execute.return_value = run_ids_result
        session_context = MagicMock()
        session_context.__enter__.return_value = session

        with patch('app.eod.daily_pipeline_state.session_scope', return_value=session_context):
            result = DailyPipelineRunStateService().count_runs(
                market_date='2026-09-05',
                trigger='auto_eod_followup_resume',
            )

        self.assertEqual(result, 3)

    def test_monitor_recovery_acquires_advisory_lock_before_deduplicating(self) -> None:
        now = datetime(2026, 8, 24, tzinfo=timezone.utc)
        existing_run = SimpleNamespace(
            id=315,
            market_date=date(2026, 8, 21),
            trigger='monitor_stale_continuation',
            status='completed',
            current_stage=None,
            started_at=now,
            updated_at=now,
            completed_at=now,
            error_message=None,
            run_metadata={},
        )
        existing_result = MagicMock()
        existing_result.scalar_one_or_none.return_value = existing_run
        stages_result = MagicMock()
        stages_result.scalars.return_value = []
        session = MagicMock()
        active_result = MagicMock()
        active_result.scalars.return_value = []
        session.execute.side_effect = [MagicMock(), active_result, existing_result, stages_result]
        session_context = MagicMock()
        session_context.__enter__.return_value = session

        with (
            patch('app.eod.daily_pipeline_state.session_scope', return_value=session_context),
            patch('app.eod.daily_pipeline_state._utcnow', return_value=now),
        ):
            result = DailyPipelineRunStateService().start_run(
                market_date='2026-08-21',
                trigger='monitor_stale_continuation',
                requested_stages=['price_collection'],
                resolved_stages=['price_collection'],
            )

        advisory_lock_call = session.execute.call_args_list[0]
        self.assertEqual(
            str(advisory_lock_call.args[0]),
            'SELECT pg_advisory_xact_lock(:source_key, :trade_key)',
        )
        self.assertEqual(
            advisory_lock_call.args[1],
            {'source_key': 20_001, 'trade_key': 20260821},
        )
        self.assertTrue(result['deduplicated'])
        self.assertEqual(result['deduplication_reason'], 'recovery_already_completed')

    def test_manual_api_deduplicates_overlapping_active_run(self) -> None:
        now = datetime(2026, 8, 29, tzinfo=timezone.utc)
        existing_run = SimpleNamespace(
            id=341,
            market_date=date(2026, 8, 28),
            trigger='manual_api',
            status='running',
            current_stage='agent_feedback_summaries',
            started_at=now,
            updated_at=now,
            completed_at=None,
            error_message=None,
            run_metadata={
                'resolved_stages': ['agent_feedback_summaries', 'signal_decider'],
            },
        )
        active_result = MagicMock()
        active_result.scalars.return_value = [existing_run]
        stages_result = MagicMock()
        stages_result.scalars.return_value = []
        session = MagicMock()
        session.execute.side_effect = [MagicMock(), active_result, stages_result]
        session_context = MagicMock()
        session_context.__enter__.return_value = session

        with (
            patch('app.eod.daily_pipeline_state.session_scope', return_value=session_context),
            patch('app.eod.daily_pipeline_state._utcnow', return_value=now),
        ):
            result = DailyPipelineRunStateService().start_run(
                market_date='2026-08-28',
                trigger='manual_api',
                requested_stages=['signal_decider'],
                resolved_stages=['signal_decider'],
            )

        self.assertTrue(result['deduplicated'])
        self.assertEqual(result['id'], 341)
        self.assertEqual(result['deduplication_reason'], 'overlapping_run_already_active')

    def test_start_run_abandons_stale_activity_before_admitting_recovery(self) -> None:
        now = datetime(2026, 8, 29, 12, 0, tzinfo=timezone.utc)
        last_activity_at = now - timedelta(minutes=31)
        stale_run = SimpleNamespace(
            id=341,
            market_date=date(2026, 8, 28),
            trigger='manual_api',
            status='running',
            current_stage='signal_decider',
            started_at=last_activity_at - timedelta(minutes=5),
            updated_at=last_activity_at,
            completed_at=None,
            error_message=None,
            run_metadata={'resolved_stages': ['signal_decider'], 'owner': 'stale-worker'},
        )
        stale_stage = SimpleNamespace(
            id=812,
            pipeline_run_id=341,
            market_date=date(2026, 8, 28),
            stage_id='signal_decider',
            status='running',
            processed=2,
            total=10,
            succeeded=2,
            failed=0,
            remaining=8,
            current_step='dispatching',
            current_activity='Dispatching signal decider',
            current_symbol='005930',
            current_period=None,
            started_at=last_activity_at - timedelta(minutes=5),
            updated_at=last_activity_at,
            completed_at=None,
            error_message=None,
            source_run_ids={},
            details={},
        )
        active_result = MagicMock()
        active_result.scalars.return_value = [stale_run]
        stages_result = MagicMock()
        stages_result.scalars.return_value = [stale_stage]
        existing_recovery_result = MagicMock()
        existing_recovery_result.scalar_one_or_none.return_value = None
        session = MagicMock()
        session.execute.side_effect = [
            MagicMock(),
            active_result,
            stages_result,
            existing_recovery_result,
        ]
        session_context = MagicMock()
        session_context.__enter__.return_value = session

        with (
            patch('app.eod.daily_pipeline_state.session_scope', return_value=session_context),
            patch('app.eod.daily_pipeline_state._utcnow', return_value=now),
            patch('app.eod.daily_pipeline_state.settings.eod_auto_pykrx_stale_run_minutes', 30),
        ):
            result = DailyPipelineRunStateService().start_run(
                market_date='2026-08-28',
                trigger='monitor_stale_continuation',
                requested_stages=['signal_decider'],
                resolved_stages=['signal_decider'],
            )

        self.assertNotIn('deduplicated', result)
        self.assertEqual(stale_run.status, 'abandoned')
        self.assertEqual(stale_run.completed_at, now)
        self.assertEqual(stale_run.run_metadata['abandoned_reason'], 'stale_activity_timeout')
        self.assertEqual(stale_run.run_metadata['last_activity_at'], last_activity_at.isoformat())
        self.assertTrue(stale_run.run_metadata['resumable'])
        self.assertEqual(stale_stage.status, 'abandoned')
        self.assertEqual(stale_stage.current_step, 'stale_run_abandoned')
        self.assertEqual(stale_stage.details['stale_after_minutes'], 30)
        admitted_run = session.add.call_args.args[0]
        self.assertEqual(admitted_run.status, 'running')
        self.assertEqual(admitted_run.trigger, 'monitor_stale_continuation')
        self.assertEqual(admitted_run.run_metadata['resolved_stages'], ['signal_decider'])
        self.assertNotIn('owner', admitted_run.run_metadata)

    def test_finish_run_rejects_terminal_run_owned_by_reconciled_worker(self) -> None:
        abandoned_run = SimpleNamespace(id=341, status='abandoned')
        run_result = MagicMock()
        run_result.scalar_one_or_none.return_value = abandoned_run
        session = MagicMock()
        session.execute.return_value = run_result
        session_context = MagicMock()
        session_context.__enter__.return_value = session

        with patch('app.eod.daily_pipeline_state.session_scope', return_value=session_context):
            with self.assertRaisesRegex(
                ValueError,
                r'daily pipeline run is not active: 341 \(abandoned\)',
            ):
                DailyPipelineRunStateService().finish_run(
                    pipeline_run_id=341,
                    status='completed',
                )

    def test_finish_stage_recomputes_stale_remaining_from_terminal_counts(self) -> None:
        now = datetime(2026, 9, 12, 16, 5, tzinfo=timezone.utc)
        run = SimpleNamespace(
            id=386,
            status='running',
            current_stage='price_collection',
            updated_at=now,
        )
        stage = SimpleNamespace(
            id=901,
            pipeline_run_id=386,
            market_date=date(2026, 9, 11),
            stage_id='price_collection',
            status='running',
            processed=3_548,
            total=3_549,
            succeeded=3_548,
            failed=0,
            remaining=3_549,
            current_step='collecting',
            current_activity='Collecting prices',
            current_symbol='472220',
            current_period=None,
            started_at=now - timedelta(minutes=10),
            updated_at=now - timedelta(minutes=1),
            completed_at=None,
            error_message=None,
            source_run_ids={},
            details={},
        )
        run_result = MagicMock()
        run_result.scalar_one_or_none.return_value = run
        stage_result = MagicMock()
        stage_result.scalar_one_or_none.return_value = stage
        session = MagicMock()
        session.execute.side_effect = [run_result, stage_result]
        session_context = MagicMock()
        session_context.__enter__.return_value = session

        with (
            patch(
                'app.eod.daily_pipeline_state.session_scope',
                return_value=session_context,
            ),
            patch('app.eod.daily_pipeline_state._utcnow', return_value=now),
        ):
            result = DailyPipelineRunStateService().finish_stage(
                pipeline_run_id=386,
                stage_id='price_collection',
                market_date='2026-09-11',
                status='partial',
                processed=3_549,
                total=3_549,
                succeeded=3_548,
                failed=1,
            )

        self.assertEqual(result['remaining'], 0)
        self.assertEqual(stage.remaining, 0)

    def test_direct_terminal_update_recomputes_stale_remaining(self) -> None:
        now = datetime(2026, 9, 12, 16, 5, tzinfo=timezone.utc)
        run = SimpleNamespace(
            id=386,
            status='running',
            current_stage='price_collection',
            updated_at=now,
        )
        stage = SimpleNamespace(
            id=901,
            pipeline_run_id=386,
            market_date=date(2026, 9, 11),
            stage_id='price_collection',
            status='running',
            processed=3_548,
            total=3_549,
            succeeded=3_548,
            failed=0,
            remaining=3_549,
            current_step='collecting',
            current_activity='Collecting prices',
            current_symbol='472220',
            current_period=None,
            started_at=now - timedelta(minutes=10),
            updated_at=now - timedelta(minutes=1),
            completed_at=None,
            error_message=None,
            source_run_ids={},
            details={},
        )
        run_result = MagicMock()
        run_result.scalar_one_or_none.return_value = run
        stage_result = MagicMock()
        stage_result.scalar_one_or_none.return_value = stage
        session = MagicMock()
        session.execute.side_effect = [run_result, stage_result]
        session_context = MagicMock()
        session_context.__enter__.return_value = session

        with (
            patch(
                'app.eod.daily_pipeline_state.session_scope',
                return_value=session_context,
            ),
            patch('app.eod.daily_pipeline_state._utcnow', return_value=now),
        ):
            result = DailyPipelineRunStateService().update_stage(
                pipeline_run_id=386,
                stage_id='price_collection',
                market_date='2026-09-11',
                status='partial',
                processed=3_549,
                total=3_549,
                succeeded=3_548,
                failed=1,
            )

        self.assertEqual(result['remaining'], 0)
        self.assertEqual(stage.remaining, 0)


if __name__ == '__main__':
    unittest.main()
