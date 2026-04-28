import unittest
from datetime import date
from unittest.mock import patch

from app.agent_context.eligibility import (
    AGENT_FEEDBACK_SCOPE_ALL,
    AGENT_FEEDBACK_SCOPE_REQUESTED,
    load_agent_feedback_eligible_symbols,
    resolve_agent_feedback_scope,
    split_requested_symbols_by_agent_feedback_eligibility,
)


class AgentFeedbackEligibilityTests(unittest.TestCase):
    def test_load_agent_feedback_eligible_symbols_uses_intersection_of_both_filters(self) -> None:
        with patch(
            'app.agent_context.eligibility.load_daily_piotroski_filter_results',
            return_value=[
                {'symbol': '000660', 'passed_filter': True},
                {'symbol': '005930', 'passed_filter': True},
                {'symbol': '035420', 'passed_filter': True},
            ],
        ) as load_piotroski:
            with patch(
                'app.agent_context.eligibility.load_daily_market_risk_filter_results',
                return_value=[
                    {'symbol': '005930', 'passed_filter': True},
                    {'symbol': '035420', 'passed_filter': True},
                    {'symbol': '114630', 'passed_filter': True},
                ],
            ) as load_market_risk:
                result = load_agent_feedback_eligible_symbols(
                    market_date=date.fromisoformat('2026-04-16'),
                )

        load_piotroski.assert_called_once_with(
            market_date=date.fromisoformat('2026-04-16'),
            symbols=None,
            passed_filter=True,
            limit=None,
        )
        load_market_risk.assert_called_once_with(
            market_date=date.fromisoformat('2026-04-16'),
            symbols=None,
            passed_filter=True,
            limit=None,
        )
        self.assertEqual(result, ['005930', '035420'])

    def test_requested_symbol_scope_preserves_request_order_inside_filter_intersection(self) -> None:
        with patch(
            'app.agent_context.eligibility.load_daily_piotroski_filter_results',
            return_value=[
                {'symbol': '035420', 'passed_filter': True},
                {'symbol': '005930', 'passed_filter': True},
            ],
        ):
            with patch(
                'app.agent_context.eligibility.load_daily_market_risk_filter_results',
                return_value=[
                    {'symbol': '005930', 'passed_filter': True},
                    {'symbol': '035420', 'passed_filter': True},
                ],
            ):
                eligible, skipped = split_requested_symbols_by_agent_feedback_eligibility(
                    market_date=date.fromisoformat('2026-04-16'),
                    requested_symbols=['005930', '114630', '035420'],
                )

        self.assertEqual(eligible, ['005930', '035420'])
        self.assertEqual(skipped, ['114630'])

    def test_resolve_agent_feedback_scope_reflects_requested_subset_usage(self) -> None:
        self.assertEqual(resolve_agent_feedback_scope(requested_symbols=None), AGENT_FEEDBACK_SCOPE_ALL)
        self.assertEqual(
            resolve_agent_feedback_scope(requested_symbols=['005930']),
            AGENT_FEEDBACK_SCOPE_REQUESTED,
        )


if __name__ == '__main__':
    unittest.main()
