import unittest
from contextlib import contextmanager
from datetime import date
from types import SimpleNamespace
from unittest.mock import patch

from app.agent_context.retry_service import AgentFeedbackRetryService, EXPECTED_ROLES
from scripts import run_feedback_retry_cycles


class FakeExecuteResult:
    def __init__(self, rows):
        self.rows = rows

    def all(self):
        return self.rows


class FakeSession:
    def __init__(self, symbols):
        self.symbols = symbols

    def execute(self, statement):
        return FakeExecuteResult([SimpleNamespace(symbol=symbol) for symbol in self.symbols])


def _decision(*, should_dispatch):
    return SimpleNamespace(should_dispatch=should_dispatch)


class FeedbackRetryCycleTests(unittest.TestCase):
    def test_load_incomplete_symbols_keeps_stale_feedback_retryable(self):
        service = AgentFeedbackRetryService()

        @contextmanager
        def fake_session_scope():
            yield FakeSession(['005930'])

        decisions = {
            '005930': {
                'eod-summarizer': _decision(should_dispatch=False),
                'technical-summarizer': _decision(should_dispatch=True),
                'fundamental-summarizer': _decision(should_dispatch=False),
            }
        }

        with patch('app.agent_context.retry_service.session_scope', fake_session_scope):
            with patch(
                'app.agent_context.retry_service.agent_context_reuse_service.build_latest_decisions',
                return_value=decisions,
            ):
                result = service.load_incomplete_symbols(
                    market_date=date.fromisoformat('2026-04-16'),
                    roles=EXPECTED_ROLES,
                )

        self.assertEqual(result, ['005930'])

    def test_load_incomplete_symbols_drops_freshly_complete_symbols(self):
        service = AgentFeedbackRetryService()

        @contextmanager
        def fake_session_scope():
            yield FakeSession(['005930'])

        decisions = {
            '005930': {
                role: _decision(should_dispatch=False)
                for role in EXPECTED_ROLES
            }
        }

        with patch('app.agent_context.retry_service.session_scope', fake_session_scope):
            with patch(
                'app.agent_context.retry_service.agent_context_reuse_service.build_latest_decisions',
                return_value=decisions,
            ):
                result = service.load_incomplete_symbols(
                    market_date=date.fromisoformat('2026-04-16'),
                    roles=EXPECTED_ROLES,
                )

        self.assertEqual(result, [])

    def test_parse_args_uses_env_market_date_default(self):
        with patch.dict('os.environ', {'FEEDBACK_RETRY_MARKET_DATE': '2026-04-16'}, clear=True):
            with patch('sys.argv', ['run_feedback_retry_cycles.py']):
                args = run_feedback_retry_cycles._parse_args()

        self.assertEqual(args.market_date, date.fromisoformat('2026-04-16'))

    def test_parse_args_cli_market_date_overrides_env_default(self):
        with patch.dict('os.environ', {'FEEDBACK_RETRY_MARKET_DATE': '2026-04-15'}, clear=True):
            with patch('sys.argv', ['run_feedback_retry_cycles.py', '--market-date', '2026-04-16']):
                args = run_feedback_retry_cycles._parse_args()

        self.assertEqual(args.market_date, date.fromisoformat('2026-04-16'))


if __name__ == '__main__':
    unittest.main()
