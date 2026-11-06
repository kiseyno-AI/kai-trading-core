from __future__ import annotations

from datetime import date
import unittest

from app.analytics import improvement_findings as imp


def report(verdicts: dict, *, partial=None, sims=None):
    """verdicts: {(signal, h): verdict}"""
    rows = []
    for (signal, h), verdict in verdicts.items():
        family = 'agent' if any(token in signal for token in ('decider', 'summarizer', 'short_term')) else 'deterministic'
        row = {
            'signal': signal, 'family': family, 'horizon': h, 'verdict': verdict,
            'mean_ic': -0.09 if 'inverse' in verdict else (0.07 if 'positive' in verdict else -0.02),
            't_hac': -4.0 if 'inverse' in verdict else (4.0 if 'positive' in verdict else -1.0),
            'days': 90, 'partial_ic_vs_vol': (partial or {}).get(signal), 'partial_t_hac': 2.5 if (partial or {}).get(signal) else None,
        }
        rows.append(row)
    return {'signals': rows, 'n_tests': 48, 'required_t': 3.28}


def run(verdicts_by_run, *, last_change=None, **report_kwargs):
    history = []
    findings = []
    for i, verdicts in enumerate(verdicts_by_run):
        rep = report(verdicts, **report_kwargs)
        run_id = f'2026110{i + 1}'
        findings = imp.build_findings(rep, history, run_id=run_id, today=date(2026, 11, 1 + i * 7), last_change_date=last_change)
        history = imp.update_history(history, rep, run_id=run_id)
    return findings


class HistoryTests(unittest.TestCase):
    def test_streak_counts_consecutive_runs_ending_now(self) -> None:
        history = []
        for i, verdict in enumerate(['no_evidence', 'candidate_inverse', 'candidate_inverse']):
            history = imp.update_history(history, report({('signal_decider_expret', 5): verdict}), run_id=f'r{i}')

        self.assertEqual(imp.streak(history, 'signal_decider_expret|5', 'candidate_inverse'), 2)
        self.assertEqual(imp.streak(history, 'signal_decider_expret|5', 'no_evidence'), 0)

    def test_history_keeps_one_entry_per_run_id(self) -> None:
        history = imp.update_history([], report({('a', 1): 'no_evidence'}), run_id='r1')
        history = imp.update_history(history, report({('a', 1): 'candidate_inverse'}), run_id='r1')
        self.assertEqual(len(history), 1)
        self.assertEqual(history[0]['verdicts']['a|1'], 'candidate_inverse')


class RuleTests(unittest.TestCase):
    def test_a_single_run_never_triggers_a_change(self) -> None:
        findings = run([{('signal_decider_expret', 1): 'candidate_inverse'}])
        self.assertEqual([f for f in findings if f['action'] == 'text_change'], [])

    def test_a_persistent_inverse_signal_of_a_decider_allows_a_text_change_for_that_agent(self) -> None:
        findings = run([{('signal_decider_expret', 1): 'candidate_inverse'}] * 2)
        changes = [f for f in findings if f['action'] == 'text_change']

        self.assertEqual(len(changes), 1)
        self.assertEqual(changes[0]['agent'], 'signal-decider')
        self.assertEqual(changes[0]['rule'], 'persistent_inverse_claim')
        self.assertIn('signal_decider_expret|1', changes[0]['id'])
        self.assertEqual(changes[0]['evidence']['streak'], 2)

    def test_summarizer_findings_become_issues_not_text_changes(self) -> None:
        findings = run([{('technical_summarizer_conf', 5): 'candidate_inverse'}] * 2)
        self.assertEqual([f['action'] for f in findings], ['code_issue'])
        self.assertEqual(findings[0]['agent'], 'technical-summarizer')

    def test_persistent_no_skill_needs_four_runs(self) -> None:
        three = run([{('short_term_conf', 5): 'no_evidence'}] * 3)
        four = run([{('short_term_conf', 5): 'no_evidence'}] * 4)

        self.assertEqual(three, [])
        self.assertEqual([f['rule'] for f in four], ['persistent_no_skill'])
        self.assertEqual(four[0]['agent'], 'short-term-decider')

    def test_a_deterministic_signal_that_survives_the_volatility_control_becomes_a_code_issue(self) -> None:
        findings = run([{('quality_pct', 5): 'candidate_positive'}] * 2, partial={'quality_pct': 0.041})
        self.assertEqual([(f['action'], f['rule']) for f in findings], [('code_issue', 'expose_surviving_signal')])

    def test_a_signal_that_is_only_volatility_is_not_promoted(self) -> None:
        findings = run([{('neg_vol20', 5): 'candidate_positive'}] * 2, partial={'neg_vol20': 0.004})
        self.assertEqual(findings, [])

    def test_cooldown_suppresses_text_changes_but_not_issues(self) -> None:
        verdicts = {('signal_decider_expret', 1): 'candidate_inverse', ('quality_pct', 5): 'candidate_positive'}
        findings = run([verdicts] * 2, partial={'quality_pct': 0.05}, last_change=date(2026, 10, 30))
        actions = sorted(f['action'] for f in findings)
        self.assertEqual(actions, ['code_issue'])

    def test_finding_ids_are_stable_across_runs(self) -> None:
        a = run([{('signal_decider_expret', 1): 'candidate_inverse'}] * 2)
        b = run([{('signal_decider_expret', 1): 'candidate_inverse'}] * 3)
        self.assertEqual(a[0]['id'], b[0]['id'])


class BreakEvenTests(unittest.TestCase):
    def test_capital_needed_to_cover_the_monthly_agent_bill(self) -> None:
        # 2% net edge per month needs 25M won to earn 500k
        self.assertEqual(imp.breakeven_capital_krw(500_000, 2.0), 25_000_000)

    def test_no_positive_edge_means_no_capital_covers_the_bill(self) -> None:
        self.assertIsNone(imp.breakeven_capital_krw(500_000, 0.0))
        self.assertIsNone(imp.breakeven_capital_krw(500_000, -1.5))

    def test_monthly_edge_from_a_period_simulation(self) -> None:
        # +1.0% excess per 5-session period, ~21 sessions per month -> 4.2 periods
        self.assertAlmostEqual(imp.monthly_net_excess_pct(1.0, horizon=5), 4.2)


class EconomicsTests(unittest.TestCase):
    @staticmethod
    def _report(periods, excess, t):
        return {
            'signals': [
                {'signal': 'quality_pct', 'horizon': 5, 'verdict': 'candidate_positive',
                 'topk_at_assumed_cost': {'periods': periods, 'mean_excess_pct': excess, 'excess_t': t}},
            ]
        }

    def test_break_even_is_only_given_for_a_long_and_significant_edge(self) -> None:
        ok = imp.economics(self._report(30, 1.0, 3.1), monthly_cost_krw=500_000)[0]
        few = imp.economics(self._report(15, 1.0, 3.1), monthly_cost_krw=500_000)[0]
        noisy = imp.economics(self._report(30, 1.0, 0.8), monthly_cost_krw=500_000)[0]

        self.assertTrue(ok['reliable'])
        self.assertEqual(ok['breakeven_capital_krw'], imp.breakeven_capital_krw(500_000, 4.2))
        self.assertIsNone(few['breakeven_capital_krw'])
        self.assertIsNone(noisy['breakeven_capital_krw'])

    def test_reliable_rows_sort_first(self) -> None:
        report = {'signals': [
            {'signal': 'a', 'horizon': 5, 'verdict': 'x', 'topk_at_assumed_cost': {'periods': 5, 'mean_excess_pct': 9.0, 'excess_t': 5.0}},
            {'signal': 'b', 'horizon': 5, 'verdict': 'x', 'topk_at_assumed_cost': {'periods': 40, 'mean_excess_pct': 0.5, 'excess_t': 2.5}},
        ]}
        self.assertEqual([r['signal'] for r in imp.economics(report, monthly_cost_krw=500_000)], ['b', 'a'])


if __name__ == '__main__':
    unittest.main()
