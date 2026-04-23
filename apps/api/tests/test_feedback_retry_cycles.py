import unittest
from datetime import date
from types import SimpleNamespace
from unittest.mock import ANY, patch

from app.agent_context.retry_service import AgentFeedbackRetryService, EXPECTED_ROLES
from scripts import run_feedback_retry_cycles


def _decision(*, should_dispatch):
    return SimpleNamespace(should_dispatch=should_dispatch)


class FeedbackRetryCycleTests(unittest.TestCase):
    def test_load_incomplete_symbols_keeps_stale_feedback_retryable(self):
        service = AgentFeedbackRetryService()

        decisions = {
            '005930': {
                'eod-summarizer': _decision(should_dispatch=False),
                'technical-summarizer': _decision(should_dispatch=True),
                'fundamental-summarizer': _decision(should_dispatch=False),
            }
        }

        with patch(
            'app.agent_context.retry_service.load_daily_piotroski_filter_results',
            return_value=[{'symbol': '005930', 'passed_filter': True}],
        ):
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

        decisions = {
            '005930': {
                role: _decision(should_dispatch=False)
                for role in EXPECTED_ROLES
            }
        }

        with patch(
            'app.agent_context.retry_service.load_daily_piotroski_filter_results',
            return_value=[{'symbol': '005930', 'passed_filter': True}],
        ):
            with patch(
                'app.agent_context.retry_service.agent_context_reuse_service.build_latest_decisions',
                return_value=decisions,
            ):
                result = service.load_incomplete_symbols(
                    market_date=date.fromisoformat('2026-04-16'),
                    roles=EXPECTED_ROLES,
                )

        self.assertEqual(result, [])

    def test_load_incomplete_symbols_uses_only_passed_piotroski_symbols(self):
        service = AgentFeedbackRetryService()
        decisions = {
            '005930': {
                role: _decision(should_dispatch=True)
                for role in EXPECTED_ROLES
            }
        }

        with patch(
            'app.agent_context.retry_service.load_daily_piotroski_filter_results',
            return_value=[{'symbol': '005930', 'passed_filter': True}],
        ) as load_results:
            with patch(
                'app.agent_context.retry_service.agent_context_reuse_service.build_latest_decisions',
                return_value=decisions,
            ) as build_decisions:
                result = service.load_incomplete_symbols(
                    market_date=date.fromisoformat('2026-04-16'),
                    roles=EXPECTED_ROLES,
                )

        load_results.assert_called_once_with(
            market_date=date.fromisoformat('2026-04-16'),
            passed_filter=True,
            limit=None,
        )
        build_decisions.assert_called_once_with(
            market_date=date.fromisoformat('2026-04-16'),
            symbols=['005930'],
            roles=EXPECTED_ROLES,
        )
        self.assertEqual(result, ['005930'])

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

    def test_run_retry_cycles_includes_progress_response_fields(self):
        service = AgentFeedbackRetryService()

        progress = {
            'request': {
                'market_date': '2026-04-16',
                'symbols': None,
                'expected_agents': list(EXPECTED_ROLES),
            },
            'summary': {
                'first_feedback_at': '2026-04-16T01:00:00+00:00',
                'elapsed_seconds': 300,
                'completed_feedback_per_minute': 0.6,
                'completed_symbols_per_minute': 0.2,
                'estimated_remaining_seconds': 0,
                'estimated_completion_at': '2026-04-16T01:05:00+00:00',
                'eta_confidence': 'high',
            },
        }

        with patch.object(service, 'load_incomplete_symbols', return_value=[]):
            with patch(
                'app.agent_context.retry_service.agent_context_progress_service.get_feedback_progress',
                return_value=progress,
            ) as get_progress:
                result = service.run_retry_cycles(
                    config=SimpleNamespace(
                        market_date=date.fromisoformat('2026-04-16'),
                        roles=EXPECTED_ROLES,
                        max_cycles=3,
                        outer_batch_size=20,
                        reuse_existing=True,
                    )
                )

        get_progress.assert_called_once_with(
            market_date=date.fromisoformat('2026-04-16'),
            symbols=None,
            expected_agents=EXPECTED_ROLES,
            run_id=ANY,
        )
        self.assertTrue(result['completed'])
        self.assertEqual(result['progress']['summary']['first_feedback_at'], '2026-04-16T01:00:00+00:00')
        self.assertEqual(result['progress']['summary']['completed_feedback_per_minute'], 0.6)
        self.assertEqual(result['progress']['summary']['estimated_completion_at'], '2026-04-16T01:05:00+00:00')

    def test_run_retry_cycles_scopes_progress_to_requested_roles(self):
        service = AgentFeedbackRetryService()
        progress = {
            'request': {
                'market_date': '2026-04-16',
                'symbols': None,
                'expected_agents': ['technical-summarizer'],
            },
            'summary': {
                'completed_symbols': 10,
                'expected_feedback_count': 10,
                'estimated_remaining_seconds': 0,
            },
        }

        with patch.object(service, 'load_incomplete_symbols', return_value=[]):
            with patch(
                'app.agent_context.retry_service.agent_context_progress_service.get_feedback_progress',
                return_value=progress,
            ) as get_progress:
                result = service.run_retry_cycles(
                    config=SimpleNamespace(
                        market_date=date.fromisoformat('2026-04-16'),
                        roles=('technical-summarizer',),
                        max_cycles=3,
                        outer_batch_size=20,
                        reuse_existing=True,
                    )
                )

        get_progress.assert_called_once_with(
            market_date=date.fromisoformat('2026-04-16'),
            symbols=None,
            expected_agents=('technical-summarizer',),
            run_id=ANY,
        )
        self.assertEqual(result['progress']['request']['expected_agents'], ['technical-summarizer'])
        self.assertEqual(result['progress']['summary']['expected_feedback_count'], 10)


if __name__ == '__main__':
    unittest.main()
