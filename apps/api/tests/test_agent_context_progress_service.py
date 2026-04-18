import unittest
from contextlib import contextmanager
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import patch

from app.agent_context.progress_service import AgentContextProgressService


class FakeExecuteResult:
    def __init__(self, *, scalars=None, scalar=None, rows=None):
        self._scalars = scalars or []
        self._scalar = scalar
        self._rows = rows or []

    def scalars(self):
        return self

    def all(self):
        return self._scalars

    def scalar_one(self):
        return self._scalar

    def __iter__(self):
        return iter(self._rows)


class FakeProgressSession:
    def __init__(self):
        self.statements = []
        self.global_first_feedback_at = datetime(2026, 4, 14, 0, 0, tzinfo=timezone.utc)
        self.filtered_first_feedback_at = datetime(2026, 4, 14, 1, 0, tzinfo=timezone.utc)
        self.filtered_latest_feedback_at = datetime(2026, 4, 14, 1, 5, tzinfo=timezone.utc)

    def execute(self, statement):
        self.statements.append(statement)
        call_index = len(self.statements)

        if call_index == 1:
            return FakeExecuteResult(scalars=['005930'])

        if call_index == 2:
            sql = self._compile(statement)
            scalar = self.filtered_first_feedback_at if 'agent_context_feedback.symbol IN' in sql else self.global_first_feedback_at
            return FakeExecuteResult(scalar=scalar)

        if call_index == 3:
            return FakeExecuteResult(scalar=self.filtered_latest_feedback_at)

        return FakeExecuteResult(
            rows=[
                SimpleNamespace(
                    symbol='005930',
                    agent_name='eod-summarizer',
                    run_id='run-1',
                    received_at=self.filtered_first_feedback_at,
                    id=1,
                )
            ]
        )

    @staticmethod
    def _compile(statement) -> str:
        return str(statement.compile(compile_kwargs={'literal_binds': True}))


class AgentContextProgressServiceTests(unittest.TestCase):
    def test_feedback_progress_filters_first_feedback_timestamp_by_requested_symbols(self):
        fake_session = FakeProgressSession()

        @contextmanager
        def fake_session_scope():
            yield fake_session

        with patch('app.agent_context.progress_service.session_scope', fake_session_scope):
            result = AgentContextProgressService().get_feedback_progress(
                market_date='2026-04-14',
                symbols=['005930'],
            )

        first_feedback_sql = fake_session._compile(fake_session.statements[1])
        latest_feedback_sql = fake_session._compile(fake_session.statements[2])

        self.assertIn('agent_context_feedback.symbol IN', first_feedback_sql)
        self.assertIn('agent_context_feedback.symbol IN', latest_feedback_sql)
        self.assertEqual(
            result['summary']['first_feedback_at'],
            fake_session.filtered_first_feedback_at.isoformat(),
        )


if __name__ == '__main__':
    unittest.main()
