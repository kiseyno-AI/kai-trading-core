import logging
from unittest import TestCase

from app.eod.daily_pipeline_state import _log_run_finish, _log_stage_transition


class DailyPipelineTransitionLoggingTests(TestCase):
    def test_stage_finish_line_carries_status_counts_and_duration(self) -> None:
        stage = {
            'pipeline_run_id': 613, 'market_date': '2026-11-05', 'stage_id': 'market_risk_filter',
            'status': 'completed', 'processed': 2769, 'total': 2769, 'succeeded': 2769, 'failed': 0,
            'details': {'duration_seconds': 2171.4}, 'error_message': None,
        }
        with self.assertLogs('app.eod.daily_pipeline_state', level='INFO') as captured:
            _log_stage_transition('finish', stage)
        line = captured.records[0].getMessage()
        for expected in ('run=613', 'stage=market_risk_filter', 'status=completed', '2769/2769', 'duration_s=2171.4'):
            self.assertIn(expected, line)
        self.assertEqual(captured.records[0].levelno, logging.INFO)

    def test_failed_and_blocked_stages_log_at_warning(self) -> None:
        for status in ('failed', 'blocked'):
            with self.assertLogs('app.eod.daily_pipeline_state', level='INFO') as captured:
                _log_stage_transition('finish', {'stage_id': 's', 'status': status, 'error_message': 'boom'})
            self.assertEqual(captured.records[0].levelno, logging.WARNING)
            self.assertIn('error=boom', captured.records[0].getMessage())

    def test_run_finish_summarises_stage_statuses(self) -> None:
        run = {'id': 7, 'market_date': '2026-11-05', 'trigger': 'auto_close_scheduler', 'status': 'partial',
               'stages': {'a': {'status': 'completed'}, 'b': {'status': 'partial'}, 'c': {'status': 'completed'}}}
        with self.assertLogs('app.eod.daily_pipeline_state', level='INFO') as captured:
            _log_run_finish(run)
        line = captured.records[0].getMessage()
        self.assertIn("'completed': 2", line)
        self.assertIn("'partial': 1", line)

    def test_malformed_input_never_raises(self) -> None:
        _log_stage_transition('start', {'details': 'not-a-dict', 'error_message': 5})
        _log_run_finish({'stages': 'oops'})
