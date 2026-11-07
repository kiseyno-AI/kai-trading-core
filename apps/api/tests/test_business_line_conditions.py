import unittest

import numpy as np
import pandas as pd

from app.market import business_line_conditions as bc


def _series(dates, values, lag_days=1):
    dates = pd.to_datetime(dates)
    return pd.DataFrame(
        {'observation_date': dates, 'released_on': dates + pd.Timedelta(days=lag_days), 'value': values}
    )


class PointInTimeTests(unittest.TestCase):
    def test_a_value_is_unseen_on_its_release_day(self):
        days = pd.DatetimeIndex(['2024-02-12', '2024-02-13', '2024-02-14'])
        cpi = pd.DataFrame(
            {'released_on': pd.to_datetime(['2024-01-11', '2024-02-13']), 'value': [3.4, 3.1]}
        )

        seen = bc.as_of(days, cpi)

        self.assertEqual(list(seen), [3.4, 3.4, 3.1])

    def test_forward_excess_sums_the_next_days_including_today(self):
        metrics = pd.DataFrame(
            {
                'trade_date': pd.to_datetime(['2024-01-01', '2024-01-02', '2024-01-03']),
                'business_line': 'banks',
                'excess_return': [0.01, 0.02, 0.03],
            }
        )

        forward = bc.forward_excess(metrics, 2)

        self.assertAlmostEqual(forward.loc['2024-01-01', 'banks'], 3.0)
        self.assertTrue(np.isnan(forward.loc['2024-01-03', 'banks']))


class PlantedEffectTests(unittest.TestCase):
    def test_a_line_that_rises_after_fear_days_is_found_and_consistent(self):
        rng = np.random.default_rng(7)
        days = pd.bdate_range('2008-01-01', '2024-12-31')
        fear = pd.Series(False, index=days)
        for start in range(0, len(days), 120):
            fear.iloc[start:start + 25] = True  # recurring 25-day fear spells
        vix = np.where(fear, 35.0, 14.0)
        frames = []
        for line, effect in (('defense', 0.004), ('banks', 0.0)):
            noise = rng.normal(0, 0.01, len(days))
            frames.append(
                pd.DataFrame({'trade_date': days, 'business_line': line, 'excess_return': noise + np.where(fear, effect, 0.0)})
            )
        metrics = pd.concat(frames)
        macro = {'VIXCLS': _series(days - pd.Timedelta(days=1), vix)}

        report = bc.build_report(metrics, macro)
        rows = {row['line']: row for row in report['conditions']['vix_ge30']['h20']}

        self.assertGreater(rows['defense']['lift'], 2.0)
        self.assertGreaterEqual(rows['defense']['t'], 2)
        self.assertTrue(rows['defense']['consistent'])
        self.assertLess(abs(rows['banks']['t']), 2)
        markdown = bc.to_markdown(report)
        self.assertIn('방산·항공우주', markdown)
        self.assertIn('VIX 30 이상 (공포)', markdown)

    def test_episode_counting_ignores_onsets_inside_the_horizon(self):
        mask = pd.Series([False, True, False, True, False] + [False] * 20 + [True])
        self.assertEqual(bc.episodes(mask, 20), 2)


if __name__ == '__main__':
    unittest.main()
