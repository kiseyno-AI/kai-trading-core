from __future__ import annotations

import unittest

from app.internal.daily_coverage_audit import (
    BASELINE_KEY,
    COVERAGE_TABLES,
    DEFAULT_MIN_RATIO,
    STATUS_EMPTY,
    STATUS_LOW,
    STATUS_NO_BASELINE,
    STATUS_OK,
    evaluate_daily_coverage,
)


def _full_counts(universe: int, **overrides: int) -> dict[str, int]:
    counts = {spec.key: universe for spec in COVERAGE_TABLES}
    counts.update(overrides)
    return counts


def _by_key(report: dict) -> dict[str, dict]:
    return {row['key']: row for row in report['tables']}


class EvaluateDailyCoverageTests(unittest.TestCase):
    def test_a_fully_populated_day_is_ok(self) -> None:
        report = evaluate_daily_coverage(_full_counts(2769), market_date='2026-11-05')

        self.assertEqual(report['status'], 'ok')
        self.assertEqual(report['problem_tables'], [])
        self.assertEqual(report['baseline_count'], 2769)
        self.assertTrue(all(row['status'] == STATUS_OK for row in report['tables']))
        self.assertNotIn(BASELINE_KEY, _by_key(report))

    def test_the_2026_11_04_hole_is_detected_even_though_each_stage_looked_complete(self) -> None:
        # 82 factor rows against 2,769 price bars (stage statuses only saw 82/82).
        counts = _full_counts(
            2769,
            momentum=82,
            value=82,
            low_risk=82,
            beta=82,
            advanced_value=82,
            unified_value=82,
            quality=82,
            composite=82,
            signal_features=82,
            piotroski=2655,
        )
        report = evaluate_daily_coverage(counts, market_date='2026-11-04')

        self.assertEqual(report['status'], 'degraded')
        self.assertEqual(
            sorted(report['problem_tables']),
            sorted(
                [
                    'momentum',
                    'value',
                    'low_risk',
                    'beta',
                    'advanced_value',
                    'unified_value',
                    'quality',
                    'composite',
                    'signal_features',
                ]
            ),
        )
        self.assertEqual(_by_key(report)['momentum']['status'], STATUS_LOW)
        self.assertEqual(_by_key(report)['piotroski']['status'], STATUS_OK)

    def test_known_legitimate_gaps_do_not_trip_the_default_threshold(self) -> None:
        # 11-05: 2,619 of 2,769 (preferred shares, SPACs and new listings have no factors).
        counts = _full_counts(2769, momentum=2619, value=2619, composite=2619, signal_features=2619)
        report = evaluate_daily_coverage(counts, market_date='2026-11-05')

        self.assertEqual(report['status'], 'ok')
        self.assertAlmostEqual(_by_key(report)['momentum']['ratio'], 0.9458, places=4)

    def test_empty_tables_are_reported_as_empty(self) -> None:
        counts = _full_counts(2767, signal_features=0, market_risk_snapshot=0, market_risk_filter=0)
        report = evaluate_daily_coverage(counts, market_date='2026-11-02')

        self.assertEqual(report['status'], 'degraded')
        rows = _by_key(report)
        for key in ('signal_features', 'market_risk_snapshot', 'market_risk_filter'):
            self.assertEqual(rows[key]['status'], STATUS_EMPTY)
            self.assertEqual(rows[key]['count'], 0)

    def test_missing_keys_count_as_zero(self) -> None:
        report = evaluate_daily_coverage({BASELINE_KEY: 100}, market_date='2026-11-02')

        self.assertEqual(report['status'], 'degraded')
        self.assertTrue(all(row['status'] == STATUS_EMPTY for row in report['tables']))

    def test_the_threshold_boundary_is_inclusive(self) -> None:
        at_boundary = evaluate_daily_coverage(
            _full_counts(1000, momentum=900), market_date='2026-11-05', min_ratio=0.9
        )
        below = evaluate_daily_coverage(
            _full_counts(1000, momentum=899), market_date='2026-11-05', min_ratio=0.9
        )

        self.assertEqual(_by_key(at_boundary)['momentum']['status'], STATUS_OK)
        self.assertEqual(_by_key(below)['momentum']['status'], STATUS_LOW)

    def test_a_day_without_price_bars_is_degraded_not_silently_ok(self) -> None:
        report = evaluate_daily_coverage(_full_counts(0), market_date='2026-11-07')

        self.assertEqual(report['status'], 'degraded')
        self.assertTrue(all(row['status'] == STATUS_NO_BASELINE for row in report['tables']))
        self.assertTrue(all(row['ratio'] is None for row in report['tables']))

    def test_min_ratio_must_be_a_fraction(self) -> None:
        for bad in (-0.01, 1.01):
            with self.assertRaises(ValueError):
                evaluate_daily_coverage(_full_counts(10), market_date='2026-11-05', min_ratio=bad)

    def test_default_threshold_is_documented_value(self) -> None:
        self.assertEqual(DEFAULT_MIN_RATIO, 0.90)

    def test_table_specs_use_only_plain_identifiers(self) -> None:
        for spec in COVERAGE_TABLES:
            self.assertRegex(spec.table, r'^[a-z_]+$')
            self.assertRegex(spec.date_column, r'^[a-z_]+$')
        keys = [spec.key for spec in COVERAGE_TABLES]
        self.assertEqual(len(keys), len(set(keys)))


if __name__ == '__main__':
    unittest.main()
