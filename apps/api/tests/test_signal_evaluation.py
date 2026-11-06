from __future__ import annotations

import unittest

import numpy as np
import pandas as pd

from app.analytics import signal_evaluation as ev


def _bars(rows):
    return pd.DataFrame(rows, columns=['symbol', 'd', 'open', 'close', 'volume']).assign(
        d=lambda df: pd.to_datetime(df['d'])
    )


def _panel(n_days=60, n_names=40, seed=7, signal_strength=0.0, vol_strength=0.0):
    """Synthetic cross-section: r5 = strength * score + noise; optional volatility effect."""
    rng = np.random.default_rng(seed)
    dates = pd.bdate_range('2026-01-02', periods=n_days)
    frames = []
    for d in dates:
        score = rng.normal(size=n_names)
        vol = rng.uniform(1, 5, size=n_names)
        noise = rng.normal(scale=2.0, size=n_names)
        r5 = signal_strength * score - vol_strength * vol + noise
        frames.append(
            pd.DataFrame(
                {'symbol': [f'S{i:03d}' for i in range(n_names)], 'd': d, 'score': score, 'vol': vol, 'r5': r5, 'r1': r5 / 3}
            )
        )
    return pd.concat(frames, ignore_index=True)


class ForwardReturnTests(unittest.TestCase):
    def test_entry_is_next_session_open_and_exit_is_horizon_close(self) -> None:
        bars = _bars(
            [
                ('A', '2026-01-02', 100, 101, 10),
                ('A', '2026-01-05', 110, 112, 10),  # entry for decisions on 01-02
                ('A', '2026-01-06', 112, 120, 10),
                ('A', '2026-01-07', 120, 132, 10),
            ]
        )

        out = ev.forward_returns(bars, horizons=(1, 2)).set_index('d')

        first = out.loc['2026-01-02']
        self.assertAlmostEqual(first['r1'], (112 / 110 - 1) * 100)  # exit = close of next session
        self.assertAlmostEqual(first['r2'], (120 / 110 - 1) * 100)

    def test_return_does_not_depend_on_the_decision_day_close(self) -> None:
        def r1(close_on_decision_day):
            bars = _bars(
                [
                    ('A', '2026-01-02', 100, close_on_decision_day, 10),
                    ('A', '2026-01-05', 110, 112, 10),
                    ('A', '2026-01-06', 112, 120, 10),
                ]
            )
            return ev.forward_returns(bars, horizons=(1,)).set_index('d').loc['2026-01-02', 'r1']

        self.assertEqual(r1(90), r1(999))  # no look-ahead into the decision-day close

    def test_untradable_entry_day_or_missing_bar_gives_nan_not_a_wrong_number(self) -> None:
        bars = _bars(
            [
                ('A', '2026-01-02', 100, 101, 10),
                ('A', '2026-01-05', 110, 110, 0),  # halted: cannot enter
                ('A', '2026-01-06', 110, 115, 10),
                ('B', '2026-01-02', 100, 101, 10),
                # B has no 01-05 bar at all (gap), but the market calendar has the session
                ('B', '2026-01-06', 105, 108, 10),
                ('C', '2026-01-05', 50, 50, 10),
            ]
        )

        out = ev.forward_returns(bars, horizons=(1,)).set_index(['symbol', 'd'])

        self.assertTrue(np.isnan(out.loc[('A', pd.Timestamp('2026-01-02')), 'r1']))
        self.assertTrue(np.isnan(out.loc[('B', pd.Timestamp('2026-01-02')), 'r1']))


class EntryDayDriftTests(unittest.TestCase):
    def test_intraday1_is_the_entry_sessions_open_to_close_move(self) -> None:
        bars = _bars(
            [
                ('A', '2026-01-02', 100, 101, 10),
                ('A', '2026-01-05', 110, 99, 10),  # fades from 110 to 99 on the entry day
                ('A', '2026-01-06', 100, 105, 10),
            ]
        )
        out = ev.forward_returns(bars, horizons=(1,)).set_index('d')

        self.assertAlmostEqual(out.loc['2026-01-02', 'intraday1'], (99 / 110 - 1) * 100)

    def test_intraday1_is_nan_when_the_entry_day_is_halted(self) -> None:
        bars = _bars([('A', '2026-01-02', 100, 101, 10), ('A', '2026-01-05', 110, 110, 0), ('A', '2026-01-06', 110, 112, 10)])
        out = ev.forward_returns(bars, horizons=(1,)).set_index('d')
        self.assertTrue(np.isnan(out.loc['2026-01-02', 'intraday1']))


class IcTests(unittest.TestCase):
    def test_perfect_and_inverted_scores(self) -> None:
        panel = _panel(signal_strength=0.0)
        panel['r5'] = panel['score'] * 10
        self.assertAlmostEqual(ev.cross_sectional_ic(panel, 'score', 'r5').mean(), 1.0)
        panel['r5'] = -panel['score']
        self.assertAlmostEqual(ev.cross_sectional_ic(panel, 'score', 'r5').mean(), -1.0)

    def test_pure_noise_has_no_ic(self) -> None:
        ics = ev.cross_sectional_ic(_panel(n_days=200, signal_strength=0.0), 'score', 'r5')
        self.assertLess(abs(ics.mean()), 0.03)

    def test_dates_with_too_few_names_are_dropped(self) -> None:
        panel = _panel(n_days=5, n_names=10)
        self.assertEqual(len(ev.cross_sectional_ic(panel, 'score', 'r5', min_names=20)), 0)
        self.assertEqual(len(ev.cross_sectional_ic(panel, 'score', 'r5', min_names=10)), 5)

    def test_partial_ic_removes_a_pure_volatility_effect(self) -> None:
        panel = _panel(n_days=120, n_names=80, vol_strength=3.0)
        # A "signal" that only repeats volatility (plus unrelated noise): returns depend on vol alone.
        panel['score'] = panel['vol'] + np.random.default_rng(5).normal(scale=0.5, size=len(panel))
        raw = ev.cross_sectional_ic(panel, 'score', 'r5').mean()
        partial = ev.partial_cross_sectional_ic(panel, 'score', 'vol', 'r5').mean()

        self.assertLess(raw, -0.3)
        # Rank-residualising is linear in ranks, so a little of the true driver can leak
        # through; what matters is that almost all of the raw effect is gone.
        self.assertLess(abs(partial), 0.08)
        self.assertLess(abs(partial), abs(raw) / 8)


class SummaryTests(unittest.TestCase):
    def test_hac_t_is_smaller_than_naive_for_autocorrelated_series(self) -> None:
        rng = np.random.default_rng(1)
        noise = rng.normal(size=400)
        series = pd.Series(np.convolve(noise, np.ones(5) / 5, mode='same') + 0.05)  # overlapping windows
        naive = series.mean() / (series.std(ddof=1) / np.sqrt(len(series)))

        self.assertLess(abs(ev.newey_west_t(series, lag=4)), abs(naive))
        self.assertAlmostEqual(ev.newey_west_t(series, lag=0), naive, places=2)

    def test_summary_reports_days_halves_and_overlap_aware_tests(self) -> None:
        ics = pd.Series(0.06 + np.random.default_rng(3).normal(scale=0.05, size=120), index=pd.bdate_range('2026-01-02', periods=120))
        s = ev.summarize_ic(ics, horizon=5)

        self.assertEqual(s['days'], 120)
        self.assertGreater(s['mean_ic'], 0.04)
        self.assertGreater(s['first_half_ic'], 0)
        self.assertGreater(s['second_half_ic'], 0)
        self.assertTrue(np.isfinite(s['t_hac']) and np.isfinite(s['t_naive']))
        self.assertGreater(s['t_nonoverlap'], 0)
        self.assertIn('positive_day_share', s)

    def test_quantile_spread_is_positive_for_an_informative_score(self) -> None:
        spread = ev.quantile_spread(_panel(n_days=100, n_names=100, signal_strength=2.0), 'score', 'r5')
        self.assertGreater(spread.mean(), 1.0)

    def test_paired_difference_is_computed_within_each_date(self) -> None:
        panel = pd.DataFrame(
            {
                'd': pd.to_datetime(['2026-01-02'] * 4 + ['2026-01-05'] * 4),
                'act': ['buy', 'buy', 'wait', 'wait'] * 2,
                'r5': [3.0, 5.0, 1.0, 1.0, 2.0, 2.0, 1.0, 3.0],
            }
        )
        diff = ev.paired_difference(panel, 'act', 'buy', 'wait', 'r5')

        self.assertEqual(diff.round(6).tolist(), [3.0, 0.0])


class VerdictTests(unittest.TestCase):
    base = dict(days=80, mean_ic=0.05, t_hac=3.2, t_nonoverlap=2.0, first_half_ic=0.04, second_half_ic=0.06)

    def test_labels(self) -> None:
        self.assertEqual(ev.verdict(self.base), 'candidate_positive')
        self.assertEqual(ev.verdict({**self.base, 'mean_ic': -0.05, 't_hac': -3.2, 't_nonoverlap': -2.0, 'first_half_ic': -0.04, 'second_half_ic': -0.06}), 'candidate_inverse')
        self.assertEqual(ev.verdict({**self.base, 'days': 20}), 'insufficient')
        self.assertEqual(ev.verdict({**self.base, 't_hac': 1.4}), 'no_evidence')
        self.assertEqual(ev.verdict({**self.base, 'second_half_ic': -0.02}), 'no_evidence')  # not stable across time
        self.assertEqual(ev.verdict({**self.base, 't_nonoverlap': 0.3}), 'no_evidence')  # only the overlapping test passes

    def test_required_t_grows_with_the_number_of_signals_tested(self) -> None:
        self.assertLess(ev.required_t(1), ev.required_t(10))
        self.assertLess(ev.required_t(10), ev.required_t(100))
        self.assertGreaterEqual(ev.required_t(1), 2.0)

    def test_verdict_uses_the_multiple_testing_threshold(self) -> None:
        self.assertEqual(ev.verdict({**self.base, 't_hac': 2.6}, n_tests=1), 'candidate_positive')
        self.assertEqual(ev.verdict({**self.base, 't_hac': 2.6}, n_tests=200), 'no_evidence')


class SimulationTests(unittest.TestCase):
    def test_costs_reduce_the_net_return_and_are_charged_per_period(self) -> None:
        panel = _panel(n_days=60, n_names=40, signal_strength=5.0)
        free = ev.simulate_topk(panel, 'score', 'r5', k=5, step=5, roundtrip_cost_bps=0)
        paid = ev.simulate_topk(panel, 'score', 'r5', k=5, step=5, roundtrip_cost_bps=100)

        self.assertEqual(free['periods'], paid['periods'])
        self.assertAlmostEqual(free['mean_gross_pct'] - paid['mean_net_pct'], 1.0, places=6)
        self.assertGreater(free['mean_net_pct'], paid['mean_net_pct'])

    def test_informative_score_beats_the_universe_and_noise_does_not(self) -> None:
        good = ev.simulate_topk(_panel(n_days=150, n_names=60, signal_strength=4.0), 'score', 'r5', k=5, step=5, roundtrip_cost_bps=0)
        noise = ev.simulate_topk(_panel(n_days=150, n_names=60, signal_strength=0.0, seed=11), 'score', 'r5', k=5, step=5, roundtrip_cost_bps=0)

        self.assertGreater(good['mean_excess_pct'], 2.0)
        self.assertLess(abs(noise['mean_excess_pct']), 1.5)

    def test_rebalances_only_every_step_dates_so_windows_never_overlap(self) -> None:
        sim = ev.simulate_topk(_panel(n_days=50, n_names=30), 'score', 'r5', k=3, step=10, roundtrip_cost_bps=0)
        self.assertLessEqual(sim['periods'], 5)

    def test_ascending_order_selects_the_lowest_scores(self) -> None:
        panel = _panel(n_days=40, n_names=30)
        panel['r5'] = -panel['score'] * 10
        high = ev.simulate_topk(panel, 'score', 'r5', k=3, step=5, roundtrip_cost_bps=0)
        low = ev.simulate_topk(panel, 'score', 'r5', k=3, step=5, roundtrip_cost_bps=0, ascending=True)

        self.assertLess(high['mean_gross_pct'], 0)
        self.assertGreater(low['mean_gross_pct'], 0)

    def test_drawdown_and_empty_inputs(self) -> None:
        sim = ev.simulate_topk(_panel(n_days=60, n_names=30, signal_strength=1.0), 'score', 'r5', k=5, step=5, roundtrip_cost_bps=30)
        self.assertLessEqual(sim['max_drawdown_pct'], 0.0)
        empty = ev.simulate_topk(_panel(n_days=3, n_names=5), 'score', 'r5', k=5, step=5, roundtrip_cost_bps=30, min_names=20)
        self.assertEqual(empty['periods'], 0)


if __name__ == '__main__':
    unittest.main()
