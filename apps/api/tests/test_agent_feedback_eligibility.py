import unittest
from datetime import date
from unittest.mock import patch

from app.agent_context.eligibility import (
    AGENT_FEEDBACK_SCOPE_ALL,
    AGENT_FEEDBACK_SCOPE_REQUESTED,
    AGENT_FEEDBACK_STRATEGY_SCOPE_VALUE_LONG_ONLY,
    _load_forced_inclusion_reasons,
    build_agent_feedback_role_funnel,
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

    def test_default_universe_applies_top_percentile_candidate_cap(self) -> None:
        symbols = [f'{index:06d}' for index in range(1, 11)]
        with patch(
            'app.agent_context.eligibility.load_daily_piotroski_filter_results',
            return_value=[
                {'symbol': symbol, 'passed_filter': False, 'is_low_pbr': False, 'filter_payload': {}}
                for symbol in symbols
            ],
        ):
            with patch(
                'app.agent_context.eligibility.load_daily_market_risk_filter_results',
                return_value=[
                    {'symbol': symbol, 'passed_filter': True, 'filter_payload': {}}
                    for symbol in symbols
                ],
            ):
                with patch(
                    'app.agent_context.eligibility.load_agent_feedback_candidate_score_rows',
                    return_value=[
                        {'symbol': symbol, 'signal_score': index}
                        for index, symbol in enumerate(symbols, start=1)
                    ],
                ):
                    with patch(
                        'app.agent_context.eligibility.settings.openclaw_feedback_candidate_cap_percentile',
                        0.2,
                    ):
                        with patch('app.agent_context.eligibility.settings.openclaw_feedback_candidate_cap_min_symbols', 0):
                            with patch('app.agent_context.eligibility.settings.openclaw_feedback_candidate_cap_max_symbols', 0):
                                result = load_agent_feedback_eligible_symbols(
                                    market_date=date.fromisoformat('2026-04-16'),
                                )
                                records = load_agent_feedback_eligibility_records(
                                    market_date=date.fromisoformat('2026-04-16'),
                                )

        self.assertEqual(result, ['000010', '000009'])
        capped = [record['symbol'] for record in records if record.get('cap_excluded')]
        self.assertEqual(len(capped), 8)
        top_record = next(record for record in records if record['symbol'] == '000010')
        self.assertEqual(top_record['candidate_rank'], 1)
        self.assertEqual(top_record['candidate_cap']['pre_cap_eligible_count'], 10)
        self.assertEqual(top_record['candidate_cap']['post_cap_dispatched_count'], 2)
        self.assertEqual(top_record['candidate_cap']['cap_scope'], 'broad_feedback_universe')

    def test_requested_symbol_scope_is_not_percentile_capped(self) -> None:
        symbols = [f'{index:06d}' for index in range(1, 11)]
        with patch(
            'app.agent_context.eligibility.load_daily_piotroski_filter_results',
            return_value=[
                {'symbol': symbol, 'passed_filter': False, 'is_low_pbr': False, 'filter_payload': {}}
                for symbol in symbols
            ],
        ):
            with patch(
                'app.agent_context.eligibility.load_daily_market_risk_filter_results',
                return_value=[
                    {'symbol': symbol, 'passed_filter': True, 'filter_payload': {}}
                    for symbol in symbols
                ],
            ):
                with patch(
                    'app.agent_context.eligibility.settings.openclaw_feedback_candidate_cap_percentile',
                    0.2,
                ):
                    with patch('app.agent_context.eligibility.settings.openclaw_feedback_candidate_cap_min_symbols', 0):
                        with patch('app.agent_context.eligibility.settings.openclaw_feedback_candidate_cap_max_symbols', 0):
                            result = load_agent_feedback_eligible_symbols(
                                market_date=date.fromisoformat('2026-04-16'),
                                requested_symbols=symbols,
                            )
                            records = load_agent_feedback_eligibility_records(
                                market_date=date.fromisoformat('2026-04-16'),
                                requested_symbols=symbols,
                            )

        self.assertEqual(result, symbols)
        self.assertFalse(any(record.get('cap_excluded') for record in records))
        self.assertTrue(
            all(
                record.get('candidate_selection_reason') == 'requested_symbol_scope_cap_bypass'
                for record in records
            )
        )

    def test_large_requested_symbol_scope_is_ranked_and_candidate_capped(self) -> None:
        symbols = [f'{index:06d}' for index in range(1, 11)]
        with patch(
            'app.agent_context.eligibility.load_daily_piotroski_filter_results',
            return_value=[
                {'symbol': symbol, 'passed_filter': False, 'is_low_pbr': False, 'filter_payload': {}}
                for symbol in symbols
            ],
        ):
            with patch(
                'app.agent_context.eligibility.load_daily_market_risk_filter_results',
                return_value=[
                    {'symbol': symbol, 'passed_filter': True, 'filter_payload': {}}
                    for symbol in symbols
                ],
            ):
                with patch(
                    'app.agent_context.eligibility.load_agent_feedback_candidate_score_rows',
                    return_value=[
                        {'symbol': symbol, 'signal_score': index}
                        for index, symbol in enumerate(symbols, start=1)
                    ],
                ):
                    with patch(
                        'app.agent_context.eligibility.settings.openclaw_feedback_requested_scope_cap_bypass_max_symbols',
                        3,
                    ):
                        with patch('app.agent_context.eligibility.settings.openclaw_feedback_candidate_cap_percentile', 0.2):
                            with patch('app.agent_context.eligibility.settings.openclaw_feedback_candidate_cap_min_symbols', 0):
                                with patch('app.agent_context.eligibility.settings.openclaw_feedback_candidate_cap_max_symbols', 0):
                                    result = load_agent_feedback_eligible_symbols(
                                        market_date=date.fromisoformat('2026-04-16'),
                                        requested_symbols=symbols,
                                    )
                                    records = load_agent_feedback_eligibility_records(
                                        market_date=date.fromisoformat('2026-04-16'),
                                        requested_symbols=symbols,
                                    )

        self.assertEqual(result, ['000009', '000010'])
        self.assertEqual(len([record for record in records if record.get('cap_excluded')]), 8)
        self.assertTrue(
            all(
                record.get('candidate_selection_reason') != 'requested_symbol_scope_cap_bypass'
                for record in records
            )
        )

    def test_large_requested_symbol_scope_skips_prior_missed_forced_inclusion(self) -> None:
        symbols = ['005930', '000660', '035420']
        with patch(
            'app.agent_context.eligibility.settings.openclaw_feedback_requested_scope_cap_bypass_max_symbols',
            2,
        ):
            with patch(
                'app.agent_context.eligibility.settings.openclaw_feedback_force_include_prior_missed_opportunities_broad_scope_enabled',
                False,
            ):
                with patch(
                    'app.agent_context.eligibility._load_prior_positive_decision_reasons',
                    return_value={'status': 'loaded', 'matched_symbols': 0},
                ):
                    with patch(
                        'app.agent_context.eligibility._load_prior_missed_opportunity_reasons',
                        return_value={'status': 'loaded', 'matched_symbols': 3},
                    ) as load_prior_missed:
                        with patch(
                            'app.agent_context.eligibility._load_event_symbol_reasons',
                            return_value={'status': 'loaded', 'matched_events': 0},
                        ):
                            with patch(
                                'app.agent_context.eligibility._load_active_order_reasons',
                                return_value={'status': 'loaded', 'matched_rows': 0},
                            ):
                                with patch(
                                    'app.agent_context.eligibility._load_active_watch_reasons',
                                    return_value={'status': 'loaded', 'matched_rows': 0},
                                ):
                                    with patch(
                                        'app.agent_context.eligibility._add_score_signal_forced_reasons',
                                        return_value={'status': 'loaded', 'matched_symbols': 0},
                                    ):
                                        forced, diagnostics = _load_forced_inclusion_reasons(
                                            market_date=date.fromisoformat('2026-04-16'),
                                            symbols=symbols,
                                            requested_symbols=None,
                                            score_by_symbol={},
                                        )

        load_prior_missed.assert_not_called()
        self.assertEqual(forced, {})
        self.assertEqual(diagnostics['prior_missed_opportunities']['status'], 'skipped_broad_scope')

    def test_broad_candidate_cap_preserves_forced_symbols_over_rank_cutoff(self) -> None:
        symbols = [f'{index:06d}' for index in range(1, 6)]
        forced_reasons = {
            '000001': [
                {
                    'code': 'same_day_disclosure_event',
                    'source': 'dart_disclosure_events',
                    'message': 'Same-day disclosure event exists.',
                }
            ]
        }
        with patch(
            'app.agent_context.eligibility.load_daily_piotroski_filter_results',
            return_value=[
                {'symbol': symbol, 'passed_filter': False, 'is_low_pbr': False, 'filter_payload': {}}
                for symbol in symbols
            ],
        ):
            with patch(
                'app.agent_context.eligibility.load_daily_market_risk_filter_results',
                return_value=[
                    {'symbol': symbol, 'passed_filter': True, 'filter_payload': {}}
                    for symbol in symbols
                ],
            ):
                with patch(
                    'app.agent_context.eligibility.load_agent_feedback_candidate_score_rows',
                    return_value=[
                        {'symbol': symbol, 'signal_score': index}
                        for index, symbol in enumerate(symbols, start=1)
                    ],
                ):
                    with patch(
                        'app.agent_context.eligibility._load_forced_inclusion_reasons',
                        return_value=(forced_reasons, {'event_symbols': {'status': 'loaded', 'matched_events': 1}}),
                    ):
                        with patch('app.agent_context.eligibility.settings.openclaw_feedback_candidate_cap_percentile', 0.2):
                            with patch('app.agent_context.eligibility.settings.openclaw_feedback_candidate_cap_min_symbols', 0):
                                with patch('app.agent_context.eligibility.settings.openclaw_feedback_candidate_cap_max_symbols', 0):
                                    result = load_agent_feedback_eligible_symbols(
                                        market_date=date.fromisoformat('2026-04-16'),
                                    )
                                    records = load_agent_feedback_eligibility_records(
                                        market_date=date.fromisoformat('2026-04-16'),
                                    )

        self.assertEqual(result, ['000005', '000001'])
        forced_record = next(record for record in records if record['symbol'] == '000001')
        self.assertTrue(forced_record['forced_included'])
        self.assertEqual(forced_record['candidate_rank'], 5)
        self.assertEqual(
            forced_record['candidate_selection_reason'],
            'forced_included_over_broad_rank_cap',
        )
        self.assertEqual(forced_record['candidate_cap']['post_cap_dispatched_count'], 2)
        self.assertEqual(forced_record['candidate_cap']['forced_included_count'], 1)
        self.assertEqual(
            forced_record['candidate_cap']['forced_source_diagnostics']['event_symbols']['matched_events'],
            1,
        )

    def test_role_funnel_applies_role_caps_and_summarizer_gate_to_deciders(self) -> None:
        records = [
            {
                'symbol': '005930',
                'candidate_rank': 1,
                'candidate_score': 90,
                'hard_blocked': False,
                'cap_excluded': False,
                'forced_inclusion_reasons': [],
            },
            {
                'symbol': '000660',
                'candidate_rank': 2,
                'candidate_score': 80,
                'hard_blocked': False,
                'cap_excluded': False,
                'forced_inclusion_reasons': [],
            },
            {
                'symbol': '035420',
                'candidate_rank': 3,
                'candidate_score': 10,
                'hard_blocked': False,
                'cap_excluded': False,
                'forced_inclusion_reasons': [],
            },
        ]
        summarizer_gate = {
            '035420': {
                'details': {
                    'summarizer_gate': {
                        'candidate_action': 'advance_to_decider',
                        'priority_score': 0.95,
                    }
                }
            }
        }

        with patch('app.agent_context.eligibility.settings.openclaw_feedback_role_cap_eod_summarizer', 3):
            with patch('app.agent_context.eligibility.settings.openclaw_feedback_role_cap_signal_decider', 1):
                funnel = build_agent_feedback_role_funnel(
                    records=records,
                    roles=['eod-summarizer', 'signal-decider'],
                    summarizer_gate_by_symbol=summarizer_gate,
                )

        self.assertEqual(funnel['role_symbols']['eod-summarizer'], ['005930', '000660', '035420'])
        self.assertEqual(funnel['role_symbols']['signal-decider'], ['035420'])
        self.assertEqual(funnel['role_symbol_counts']['signal-decider'], 1)
        self.assertTrue(
            funnel['role_diagnostics']['signal-decider']['role_cap']['applied']
        )
        self.assertIn(
            'summarizer_gate_advance_to_decider',
            funnel['role_diagnostics']['signal-decider']['selection_reasons_by_symbol']['035420'],
        )

    def test_role_funnel_keeps_forced_symbols_over_role_cap(self) -> None:
        records = [
            {
                'symbol': '005930',
                'candidate_rank': 1,
                'candidate_score': 100,
                'hard_blocked': False,
                'cap_excluded': False,
                'forced_inclusion_reasons': [],
            },
            {
                'symbol': '000660',
                'candidate_rank': 2,
                'candidate_score': 90,
                'hard_blocked': False,
                'cap_excluded': False,
                'forced_inclusion_reasons': [],
            },
            {
                'symbol': '035420',
                'candidate_rank': 3,
                'candidate_score': 1,
                'hard_blocked': False,
                'cap_excluded': False,
                'forced_inclusion_reasons': [
                    {
                        'code': 'active_exec_watch_item',
                        'source': 'signal_decider_execution_plan_items',
                    }
                ],
            },
        ]

        with patch('app.agent_context.eligibility.settings.openclaw_feedback_role_cap_signal_decider', 1):
            funnel = build_agent_feedback_role_funnel(
                records=records,
                roles=['signal-decider'],
            )

        self.assertEqual(funnel['role_symbols']['signal-decider'], ['005930', '035420'])
        diagnostics = funnel['role_diagnostics']['signal-decider']
        self.assertEqual(diagnostics['role_cap']['configured_cap'], 1)
        self.assertEqual(diagnostics['role_cap']['post_cap_symbol_count'], 2)
        self.assertEqual(diagnostics['role_cap']['forced_included_count'], 1)
        self.assertIn('035420', diagnostics['forced_included_symbols'])
        self.assertIn('000660', diagnostics['cap_excluded_symbols'])

    def test_role_funnel_requested_symbol_dispatch_bypasses_role_caps(self) -> None:
        records = [
            {
                'symbol': symbol,
                'candidate_rank': index,
                'candidate_score': 100 - index,
                'hard_blocked': False,
                'cap_excluded': False,
            }
            for index, symbol in enumerate(['005930', '000660', '035420'], start=1)
        ]

        with patch('app.agent_context.eligibility.settings.openclaw_feedback_role_cap_signal_decider', 1):
            funnel = build_agent_feedback_role_funnel(
                records=records,
                roles=['signal-decider'],
                requested_symbols=['005930', '000660', '035420'],
            )

        self.assertEqual(funnel['role_symbols']['signal-decider'], ['005930', '000660', '035420'])
        diagnostics = funnel['role_diagnostics']['signal-decider']
        self.assertFalse(diagnostics['role_cap']['applied'])
        self.assertTrue(diagnostics['role_cap']['requested_scope_bypass'])
        self.assertEqual(
            diagnostics['role_cap']['bypass_reason'],
            'requested_symbol_scope_cap_bypass',
        )

    def test_role_funnel_large_requested_symbol_scope_still_applies_role_caps(self) -> None:
        records = [
            {
                'symbol': symbol,
                'candidate_rank': index,
                'candidate_score': 100 - index,
                'hard_blocked': False,
                'cap_excluded': False,
            }
            for index, symbol in enumerate(['005930', '000660', '035420'], start=1)
        ]

        with patch('app.agent_context.eligibility.settings.openclaw_feedback_requested_scope_cap_bypass_max_symbols', 2):
            with patch('app.agent_context.eligibility.settings.openclaw_feedback_role_cap_signal_decider', 1):
                funnel = build_agent_feedback_role_funnel(
                    records=records,
                    roles=['signal-decider'],
                    requested_symbols=['005930', '000660', '035420'],
                )

        self.assertEqual(funnel['role_symbols']['signal-decider'], ['005930'])
        diagnostics = funnel['role_diagnostics']['signal-decider']
        self.assertTrue(diagnostics['role_cap']['applied'])
        self.assertFalse(diagnostics['role_cap']['requested_scope_bypass'])
        self.assertEqual(diagnostics['cap_excluded_symbols'], ['000660', '035420'])

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
