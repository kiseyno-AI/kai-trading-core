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
        self.target_rows = [
            SimpleNamespace(
                symbol='005930',
                agent_name='eod-summarizer',
                run_id='run-1',
                received_at=self.filtered_first_feedback_at,
                id=1,
            ),
            SimpleNamespace(
                symbol='005930',
                agent_name='technical-summarizer',
                run_id='run-1',
                received_at=datetime(2026, 4, 14, 1, 2, tzinfo=timezone.utc),
                id=2,
            ),
            SimpleNamespace(
                symbol='005930',
                agent_name='fundamental-summarizer',
                run_id='run-1',
                received_at=self.filtered_latest_feedback_at,
                id=3,
            ),
        ]
        self.global_rows = [
            SimpleNamespace(
                symbol='__MARKET__',
                agent_name='eod-summarizer',
                run_id='market-run',
                received_at=self.global_first_feedback_at,
                id=99,
            ),
            *self.target_rows,
        ]

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

        sql = self._compile(statement)
        rows = self.target_rows if 'agent_context_feedback.symbol IN' in sql else self.global_rows
        return FakeExecuteResult(rows=rows)

    @staticmethod
    def _compile(statement) -> str:
        return str(statement.compile(compile_kwargs={'literal_binds': True}))


class AgentContextProgressServiceTests(unittest.TestCase):
    def _call_service(self, fake_session, *, symbols, expected_agents=None):
        @contextmanager
        def fake_session_scope():
            yield fake_session

        with patch('app.agent_context.progress_service.session_scope', fake_session_scope):
            return AgentContextProgressService().get_feedback_progress(
                market_date='2026-04-14',
                symbols=symbols,
                expected_agents=expected_agents,
            )

    def test_feedback_progress_filters_first_feedback_timestamp_by_requested_symbols(self):
        fake_session = FakeProgressSession()

        result = self._call_service(fake_session, symbols=['005930'])

        first_feedback_sql = fake_session._compile(fake_session.statements[1])
        latest_feedback_sql = fake_session._compile(fake_session.statements[2])

        self.assertIn('agent_context_feedback.symbol IN', first_feedback_sql)
        self.assertIn('agent_context_feedback.symbol IN', latest_feedback_sql)
        self.assertEqual(
            result['summary']['first_feedback_at'],
            fake_session.filtered_first_feedback_at.isoformat(),
        )

    def test_feedback_progress_filters_timing_to_target_universe_when_symbols_omitted(self):
        fake_session = FakeProgressSession()

        result = self._call_service(fake_session, symbols=None)

        first_feedback_sql = fake_session._compile(fake_session.statements[1])
        latest_feedback_sql = fake_session._compile(fake_session.statements[2])
        rows_sql = fake_session._compile(fake_session.statements[3])

        self.assertIn('agent_context_feedback.symbol IN', first_feedback_sql)
        self.assertIn('agent_context_feedback.symbol IN', latest_feedback_sql)
        self.assertIn('agent_context_feedback.symbol IN', rows_sql)
        self.assertEqual(result['summary']['total_symbols'], 1)
        self.assertEqual(result['summary']['completed_feedback_count'], 3)
        self.assertEqual(
            result['summary']['first_feedback_at'],
            fake_session.filtered_first_feedback_at.isoformat(),
        )

    def test_feedback_progress_returns_timing_rate_and_eta_fields(self):
        fake_session = FakeProgressSession()

        class FixedDateTime(datetime):
            @classmethod
            def now(cls, tz=None):
                value = cls(2026, 4, 14, 1, 10, tzinfo=timezone.utc)
                return value if tz is not None else value.replace(tzinfo=None)

        with patch('app.agent_context.progress_service.datetime', FixedDateTime):
            result = self._call_service(fake_session, symbols=['005930'])

        summary = result['summary']
        self.assertEqual(summary['run_status'], 'completed')
        self.assertEqual(summary['first_feedback_at'], '2026-04-14T01:00:00+00:00')
        self.assertEqual(summary['latest_feedback_at'], '2026-04-14T01:05:00+00:00')
        self.assertEqual(summary['elapsed_seconds'], 300)
        self.assertEqual(summary['completed_feedback_per_minute'], 0.6)
        self.assertEqual(summary['completed_symbols_per_minute'], 0.2)
        self.assertEqual(summary['estimated_remaining_seconds'], 0)
        self.assertEqual(summary['estimated_completion_at'], '2026-04-14T01:05:00+00:00')
        self.assertEqual(summary['eta_confidence'], 'high')

    def test_feedback_progress_scopes_counts_to_expected_agents(self):
        fake_session = FakeProgressSession()

        result = self._call_service(
            fake_session,
            symbols=['005930'],
            expected_agents=['technical-summarizer'],
        )

        first_feedback_sql = fake_session._compile(fake_session.statements[1])
        rows_sql = fake_session._compile(fake_session.statements[3])
        summary = result['summary']

        self.assertIn('agent_context_feedback.agent_name IN', first_feedback_sql)
        self.assertIn('agent_context_feedback.agent_name IN', rows_sql)
        self.assertEqual(result['request']['expected_agents'], ['technical-summarizer'])
        self.assertEqual(summary['completed_symbols'], 1)
        self.assertEqual(summary['completed_feedback_count'], 1)
        self.assertEqual(summary['expected_feedback_count'], 1)


if __name__ == '__main__':
    unittest.main()
