import unittest
from datetime import date
from unittest.mock import patch

from app.agent_context.eligibility import (
    AGENT_FEEDBACK_SCOPE_ALL,
    AGENT_FEEDBACK_SCOPE_REQUESTED,
    AGENT_FEEDBACK_STRATEGY_SCOPE_VALUE_LONG_ONLY,
    load_agent_feedback_eligible_symbols,
    load_agent_feedback_eligibility_records,
    resolve_agent_feedback_scope,
    split_requested_symbols_by_agent_feedback_eligibility,
    split_requested_symbols_by_agent_feedback_eligibility_with_diagnostics,
)


class AgentFeedbackEligibilityTests(unittest.TestCase):
    def test_load_agent_feedback_eligible_symbols_uses_market_risk_hard_gate_with_piotroski_soft_penalty(self) -> None:
        with patch(
            'app.agent_context.eligibility.load_daily_piotroski_filter_results',
            return_value=[
                {
                    'symbol': '066570',
                    'passed_filter': False,
                    'pbr': 2.21,
                    'pbr_percentile_rank': 0.763,
                    'is_low_pbr': False,
                    'passes_piotroski_score': True,
                    'filter_payload': {'thresholds': {'low_pbr_percentile': 0.3}},
                },
                {'symbol': '005930', 'passed_filter': True, 'filter_payload': {}},
            ],
        ) as load_piotroski:
            with patch(
                'app.agent_context.eligibility.load_daily_market_risk_filter_results',
                return_value=[
                    {
                        'symbol': '066570',
                        'passed_filter': True,
                        'risk_state': 'watchlist',
                        'watchlist_flagged': True,
                        'filter_payload': {'watchlist_reasons': ['volatility_interruption']},
                    },
                    {'symbol': '005930', 'passed_filter': True, 'filter_payload': {}},
                ],
            ) as load_market_risk:
                result = load_agent_feedback_eligible_symbols(
                    market_date=date.fromisoformat('2026-04-16'),
                )
                records = load_agent_feedback_eligibility_records(
                    market_date=date.fromisoformat('2026-04-16'),
                )

        self.assertEqual(load_piotroski.call_count, 2)
        load_piotroski.assert_any_call(
            market_date=date.fromisoformat('2026-04-16'),
            symbols=None,
            passed_filter=None,
            limit=None,
        )
        self.assertEqual(load_market_risk.call_count, 2)
        load_market_risk.assert_any_call(
            market_date=date.fromisoformat('2026-04-16'),
            symbols=None,
            passed_filter=None,
            limit=None,
        )
        self.assertEqual(result, ['066570', '005930'])
        lg_record = next(item for item in records if item['symbol'] == '066570')
        self.assertFalse(lg_record['hard_blocked'])
        self.assertEqual(lg_record['status'], 'soft_penalized_dispatched')
        self.assertIn('not_low_pbr', {item['code'] for item in lg_record['soft_penalties']})
        self.assertIn(
            'market_risk_watchlist_volatility_interruption',
            {item['code'] for item in lg_record['soft_penalties']},
        )

    def test_value_long_only_scope_preserves_legacy_piotroski_hard_gate(self) -> None:
        with patch(
            'app.agent_context.eligibility.load_daily_piotroski_filter_results',
            return_value=[
                {'symbol': '066570', 'passed_filter': False, 'is_low_pbr': False, 'filter_payload': {}},
                {'symbol': '005930', 'passed_filter': True, 'filter_payload': {}},
            ],
        ):
            with patch(
                'app.agent_context.eligibility.load_daily_market_risk_filter_results',
                return_value=[
                    {'symbol': '066570', 'passed_filter': True, 'filter_payload': {}},
                    {'symbol': '005930', 'passed_filter': True, 'filter_payload': {}},
                ],
            ):
                result = load_agent_feedback_eligible_symbols(
                    market_date=date.fromisoformat('2026-04-16'),
                    strategy_scope=AGENT_FEEDBACK_STRATEGY_SCOPE_VALUE_LONG_ONLY,
                )

        self.assertEqual(result, ['005930'])

    def test_hard_market_risk_block_still_excludes_symbol(self) -> None:
        with patch(
            'app.agent_context.eligibility.load_daily_piotroski_filter_results',
            return_value=[{'symbol': '005930', 'passed_filter': True, 'filter_payload': {}}],
        ):
            with patch(
                'app.agent_context.eligibility.load_daily_market_risk_filter_results',
                return_value=[
                    {
                        'symbol': '005930',
                        'passed_filter': False,
                        'risk_state': 'fail',
                        'filter_payload': {'blocking_reasons': ['trade_halted']},
                    },
                ],
            ):
                eligible, skipped = split_requested_symbols_by_agent_feedback_eligibility(
                    market_date=date.fromisoformat('2026-04-16'),
                    requested_symbols=['005930'],
                )

        self.assertEqual(eligible, [])
        self.assertEqual(skipped, ['005930'])

    def test_requested_symbol_scope_preserves_order_and_explains_hard_vs_soft_results(self) -> None:
        with patch(
            'app.agent_context.eligibility.load_daily_piotroski_filter_results',
            return_value=[
                {'symbol': '035420', 'passed_filter': True},
                {'symbol': '005930', 'passed_filter': False, 'is_low_pbr': False, 'filter_payload': {}},
                {'symbol': '114630', 'passed_filter': True},
            ],
        ):
            with patch(
                'app.agent_context.eligibility.load_daily_market_risk_filter_results',
                return_value=[
                    {'symbol': '005930', 'passed_filter': True},
                    {'symbol': '035420', 'passed_filter': True},
                    {
                        'symbol': '114630',
                        'passed_filter': False,
                        'filter_payload': {'blocking_reasons': ['management_item']},
                    },
                ],
            ):
                eligible, skipped, diagnostics = split_requested_symbols_by_agent_feedback_eligibility_with_diagnostics(
                    market_date=date.fromisoformat('2026-04-16'),
                    requested_symbols=['005930', '114630', '035420'],
                )

        self.assertEqual(eligible, ['005930', '035420'])
        self.assertEqual(skipped, ['114630'])
        self.assertEqual(diagnostics['hard_blocked_symbols'], ['114630'])
        self.assertEqual(diagnostics['soft_penalized_symbols'], ['005930'])
        self.assertIn('records_by_symbol', diagnostics)

    def test_resolve_agent_feedback_scope_reflects_requested_subset_usage(self) -> None:
        self.assertEqual(resolve_agent_feedback_scope(requested_symbols=None), AGENT_FEEDBACK_SCOPE_ALL)
        self.assertEqual(
            resolve_agent_feedback_scope(requested_symbols=['005930']),
            AGENT_FEEDBACK_SCOPE_REQUESTED,
        )


if __name__ == '__main__':
    unittest.main()
