import unittest
from datetime import date, datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from app.eod.daily_pipeline_state import DailyPipelineRunStateService


class DailyPipelineRunStateServiceTests(unittest.TestCase):
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


if __name__ == '__main__':
    unittest.main()
