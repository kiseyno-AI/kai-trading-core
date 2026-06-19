import unittest
from contextlib import contextmanager
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import patch

from app.agent_context.status_service import AgentContextStatusService


class FakeExecuteResult:
    def __init__(self, rows):
        self._rows = rows

    def scalars(self):
        return self

    def all(self):
        return self._rows


class FakeStatusSession:
    def __init__(self):
        self.feedback_rows = [
            SimpleNamespace(
                symbol='005930',
                agent_name='eod-summarizer',
                run_id='partial-run',
                received_at=datetime(2026, 4, 14, 1, 0, tzinfo=timezone.utc),
                id=1,
            )
        ]
        self.failure_rows = [
            SimpleNamespace(
                symbol='005930',
                agent_name='signal-decider',
                run_id='partial-run',
                failure_kind='missing_decider_upstream_summaries:fundamental-summarizer',
                retryable=True,
                summary_payload={
                    'error': 'missing_decider_upstream_summaries:fundamental-summarizer',
                },
                context_payload={
                    'dependency_status': 'blocked_missing_upstream_summaries',
                    'missing_roles': ['fundamental-summarizer'],
                },
                received_at=datetime(2026, 4, 14, 1, 5, tzinfo=timezone.utc),
                id=2,
            )
        ]

    def execute(self, statement):
        sql = str(statement.compile(compile_kwargs={'literal_binds': True}))
        if 'agent_context_feedback_failures' in sql:
            return FakeExecuteResult(self.failure_rows)
        return FakeExecuteResult(self.feedback_rows)


class AgentContextStatusServiceTests(unittest.TestCase):
    def test_feedback_status_groups_missing_upstream_without_marking_symbol_in_progress(self):
        fake_session = FakeStatusSession()

        @contextmanager
        def fake_session_scope():
            yield fake_session

        with patch('app.agent_context.status_service.session_scope', fake_session_scope):
            with patch(
                'app.agent_context.status_service.agent_feedback_prerequisite_service.inspect_feedback_prerequisites',
                return_value={'ready': True, 'status': 'ready', 'missing_prerequisites': []},
            ):
                with patch(
                    'app.agent_context.status_service.agent_context_feedback_run_state_service.find_latest_run',
                    return_value={
                        'run_id': 'retry-run',
                        'request_payload': {'feedback_scope_mode': 'decider_scope_fast_path'},
                        'summary_payload': {},
                    },
                ):
                    result = AgentContextStatusService().get_feedback_status(
                        market_date='2026-04-14',
                        symbols=['005930'],
                    )

        self.assertEqual(result['summary']['status'], 'partial')
        self.assertEqual(result['summary']['latest_run_id'], 'retry-run')
        self.assertEqual(result['summary']['feedback_scope_mode'], 'decider_scope_fast_path')
        self.assertEqual(result['summary']['in_progress_symbol_count'], 0)
        self.assertEqual(result['summary']['partial_symbol_count'], 1)
        self.assertEqual(result['summary']['failure_groups']['dependency/missing-upstream']['count'], 1)
        item = result['items'][0]
        self.assertEqual(item['status'], 'partial')
        self.assertEqual(item['failed_agents'], ['signal-decider'])
        self.assertEqual(item['pending_agents'], ['technical-summarizer', 'fundamental-summarizer', 'short-term-decider'])
        self.assertEqual(item['failure_groups']['dependency/missing-upstream']['agents'], {'signal-decider': 1})


if __name__ == '__main__':
    unittest.main()
