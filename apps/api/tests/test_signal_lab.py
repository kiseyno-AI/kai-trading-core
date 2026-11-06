from __future__ import annotations

import unittest

import numpy as np
import pandas as pd

from app.analytics import signal_lab as lab


def _synthetic(n_days=120, n_names=120, seed=3, edge=0.0):
    rng = np.random.default_rng(seed)
    frames = []
    for d in pd.bdate_range('2026-04-14', periods=n_days):
        good = rng.normal(size=n_names)
        junk = rng.normal(size=n_names)
        vol = rng.uniform(1, 5, size=n_names)
        r1 = edge * good + rng.normal(scale=2.0, size=n_names)
        frame = pd.DataFrame(
            {
                'symbol': [f'S{i:03d}' for i in range(n_names)], 'd': d,
                'low_risk_score': good, 'momentum_percentile': junk, 'composite_percentile': junk,
                'quality_percentile': junk, 'neg_vol20': -vol, 'vol20': vol, 'neg_tr5': junk, 'tr20': junk,
                'placebo': rng.normal(size=n_names), 'r1': r1, 'r5': r1 * 1.5, 'r10': r1 * 2.0,
            }
        )
        # an agent covering a 30-name pool with no skill
        pool = rng.choice(n_names, size=30, replace=False)
        frame['sig_conf'] = np.nan
        frame.loc[pool, 'sig_conf'] = rng.normal(size=30)
        frame['sig_act'] = None
        frame.loc[pool, 'sig_act'] = rng.choice(['buy', 'wait', 'reject'], size=30, p=[0.2, 0.6, 0.2])
        frames.append(frame)
    return pd.concat(frames, ignore_index=True)


class EvaluatePanelTests(unittest.TestCase):
    def test_a_real_edge_is_found_and_noise_and_the_placebo_are_not(self) -> None:
        result = lab.evaluate_panel(_synthetic(edge=1.0), horizons=(1, 5))
        by = {(row['signal'], row['horizon']): row for row in result['signals']}

        self.assertEqual(by[('low_risk_score', 1)]['verdict'], 'candidate_positive')
        self.assertEqual(by[('placebo_random', 1)]['verdict'], 'no_evidence')
        self.assertEqual(by[('momentum_pct', 1)]['verdict'], 'no_evidence')
        self.assertEqual(by[('signal_decider_conf', 1)]['verdict'], 'no_evidence')

    def test_threshold_reflects_every_signal_and_horizon_that_was_tried(self) -> None:
        result = lab.evaluate_panel(_synthetic(), horizons=(1, 5))
        present = {row['signal'] for row in result['signals']}
        self.assertEqual(result['n_tests'], len(present) * 2)
        self.assertGreaterEqual(result['required_t'], 2.0)

    def test_universe_signals_get_a_cost_sensitivity_table_and_agent_pool_signals_do_not(self) -> None:
        result = lab.evaluate_panel(_synthetic(edge=1.0), horizons=(5,))
        row = next(r for r in result['signals'] if r['signal'] == 'low_risk_score')
        net = {cost: row['topk_simulation'][cost]['mean_net_pct'] for cost in row['topk_simulation']}

        self.assertGreater(net['0'], net['51'])
        self.assertGreater(net['51'], net['80'])
        self.assertNotIn('topk_simulation', next(r for r in result['signals'] if r['signal'] == 'signal_decider_conf'))

    def test_decision_scoreboard_compares_actions_within_the_same_pool(self) -> None:
        result = lab.evaluate_panel(_synthetic(), horizons=(5,))
        pairs = [row for row in result['decisions'] if row['action'] == 'buy-minus-wait']
        self.assertEqual(len(pairs), 1)
        self.assertEqual(pairs[0]['agent'], 'signal-decider')

    def test_markdown_report_states_assumptions_and_labels(self) -> None:
        result = lab.evaluate_panel(_synthetic(edge=1.0), horizons=(5,))
        text = lab.render_markdown(result, start='2026-04-14', end='2026-09-30', universe_days=120)

        self.assertIn('multiplicity-adjusted', text)
        self.assertIn('round-trip cost', text)
        self.assertIn('low_risk_score', text)
        self.assertIn('candidate_positive', text)
        self.assertIn('one market regime', text)

    def test_signals_without_data_are_skipped_instead_of_failing(self) -> None:
        panel = _synthetic().drop(columns=['quality_percentile', 'sig_conf', 'sig_act'])
        result = lab.evaluate_panel(panel, horizons=(5,))
        self.assertNotIn('quality_pct', {row['signal'] for row in result['signals']})


class LoaderSmokeTests(unittest.TestCase):
    def test_load_panel_on_an_empty_window_returns_an_empty_frame(self) -> None:
        self.assertTrue(lab.load_panel('2099-01-01', '2099-01-31').empty)


if __name__ == '__main__':
    unittest.main()
