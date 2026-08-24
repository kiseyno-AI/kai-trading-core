import unittest
from datetime import date, datetime, timezone
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
        session.execute.side_effect = [MagicMock(), existing_result, stages_result]
        session_context = MagicMock()
        session_context.__enter__.return_value = session

        with patch('app.eod.daily_pipeline_state.session_scope', return_value=session_context):
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


if __name__ == '__main__':
    unittest.main()
