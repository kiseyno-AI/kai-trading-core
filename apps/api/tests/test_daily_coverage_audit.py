from __future__ import annotations

import io
import json
import unittest
from contextlib import contextmanager, redirect_stdout
from datetime import date
from unittest.mock import patch

from app.internal import daily_coverage_audit
from app.internal.daily_coverage_audit import (
    BASELINE_KEY,
    COVERAGE_TABLES,
    CoverageTable,
    DEFAULT_MIN_RATIO,
    STATUS_EMPTY,
    STATUS_LOW,
    STATUS_NO_BASELINE,
    STATUS_OK,
    EXIT_DEGRADED,
    EXIT_OK,
    _count_statement,
    collect_daily_table_counts,
    main,
    render_text,
    run_audit,
    evaluate_daily_coverage,
    load_recent_trade_dates,
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


class _FakeResult:
    def __init__(self, value=None, rows=None) -> None:
        self._value = value
        self._rows = rows or []

    def scalar_one(self):
        return self._value

    def all(self):
        return self._rows


class _FakeSession:
    """Records statements and answers count queries from a table->count map."""

    def __init__(self, counts_by_table: dict[str, int], recent_dates=()) -> None:
        self.counts_by_table = counts_by_table
        self.recent_dates = list(recent_dates)
        self.statements: list[str] = []
        self.params: list[dict] = []

    def execute(self, statement, params=None):
        sql = str(statement)
        self.statements.append(sql)
        self.params.append(dict(params or {}))
        if sql.startswith('select count(*) from '):
            table = sql.split(' from ', 1)[1].split(' where ', 1)[0]
            return _FakeResult(value=self.counts_by_table.get(table))
        if 'select distinct trade_date' in sql:
            return _FakeResult(rows=[(d,) for d in self.recent_dates])
        return _FakeResult()


def _patched_session_scope(fake: _FakeSession):
    @contextmanager
    def scope(**_kwargs):
        yield fake

    return patch.object(daily_coverage_audit, 'session_scope', scope)


class CollectorTests(unittest.TestCase):
    def test_count_statement_binds_the_date_and_uses_only_spec_identifiers(self) -> None:
        sql = str(_count_statement(CoverageTable('x', 'daily_momentum_factor_results', 'market_date')))

        self.assertEqual(
            sql,
            'select count(*) from daily_momentum_factor_results where market_date = :market_date',
        )

    def test_count_statement_rejects_unsafe_identifiers(self) -> None:
        for table, column in (
            ('daily_price_bars; drop table symbols', 'trade_date'),
            ('daily_price_bars', 'trade_date or 1=1'),
            ('Daily_Price_Bars', 'trade_date'),
        ):
            with self.assertRaises(ValueError):
                _count_statement(CoverageTable('x', table, column))

    def test_collect_counts_runs_in_a_read_only_transaction_and_maps_keys(self) -> None:
        fake = _FakeSession(
            {spec.table: index + 1 for index, spec in enumerate(COVERAGE_TABLES)}
        )
        with _patched_session_scope(fake):
            counts = collect_daily_table_counts(date(2026, 11, 5))

        self.assertEqual(fake.statements[0], 'SET TRANSACTION READ ONLY')
        self.assertEqual(
            counts, {spec.key: index + 1 for index, spec in enumerate(COVERAGE_TABLES)}
        )
        count_params = [p for s, p in zip(fake.statements, fake.params) if s.startswith('select count')]
        self.assertTrue(all(p == {'market_date': date(2026, 11, 5)} for p in count_params))
        self.assertEqual(len(count_params), len(COVERAGE_TABLES))

    def test_a_null_count_is_treated_as_zero(self) -> None:
        fake = _FakeSession({})
        with _patched_session_scope(fake):
            counts = collect_daily_table_counts(date(2026, 11, 5))

        self.assertTrue(all(value == 0 for value in counts.values()))

    def test_recent_trade_dates_are_read_only_and_validated(self) -> None:
        fake = _FakeSession({}, recent_dates=[date(2026, 11, 5), date(2026, 11, 4)])
        with _patched_session_scope(fake):
            dates = load_recent_trade_dates(2)

        self.assertEqual(dates, [date(2026, 11, 5), date(2026, 11, 4)])
        self.assertEqual(fake.statements[0], 'SET TRANSACTION READ ONLY')
        self.assertEqual(fake.params[-1], {'limit': 2})
        with self.assertRaises(ValueError):
            load_recent_trade_dates(0)


HOLE_DAY = {
    **{spec.key: 2769 for spec in COVERAGE_TABLES},
    'momentum': 82,
    'composite': 82,
    'signal_features': 82,
}


def _collector(by_date: dict[date, dict[str, int]]):
    return lambda market_date: by_date[market_date]


def _run_main(argv, **kwargs) -> tuple[int, str]:
    out = io.StringIO()
    with redirect_stdout(out):
        code = main(argv, **kwargs)
    return code, out.getvalue()


class CliTests(unittest.TestCase):
    OK_DAY = {spec.key: 2769 for spec in COVERAGE_TABLES}

    def test_run_audit_evaluates_each_date_in_order(self) -> None:
        reports = run_audit(
            [date(2026, 11, 5), date(2026, 11, 4)],
            collector=_collector({date(2026, 11, 5): self.OK_DAY, date(2026, 11, 4): HOLE_DAY}),
        )

        self.assertEqual([r['market_date'] for r in reports], ['2026-11-05', '2026-11-04'])
        self.assertEqual([r['status'] for r in reports], ['ok', 'degraded'])

    def test_exit_code_is_zero_when_all_days_are_ok(self) -> None:
        code, output = _run_main(
            ['--market-date', '2026-11-05'],
            collector=_collector({date(2026, 11, 5): self.OK_DAY}),
        )

        self.assertEqual(code, EXIT_OK)
        self.assertIn('market_date=2026-11-05 price_bars=2769 status=OK', output)
        self.assertNotIn('<--', output)

    def test_exit_code_flags_a_hole_and_marks_the_problem_rows(self) -> None:
        code, output = _run_main(
            ['--market-date', '2026-11-04'],
            collector=_collector({date(2026, 11, 4): HOLE_DAY}),
        )

        self.assertEqual(code, EXIT_DEGRADED)
        self.assertIn('status=DEGRADED', output)
        momentum_line = next(line for line in output.splitlines() if line.strip().startswith('momentum'))
        self.assertIn('82', momentum_line)
        self.assertIn('low', momentum_line)
        self.assertTrue(momentum_line.endswith('<--'))
        value_line = next(line for line in output.splitlines() if line.strip().startswith('value '))
        self.assertNotIn('<--', value_line)

    def test_last_uses_the_date_loader_and_default_is_five(self) -> None:
        seen: list[int] = []

        def loader(limit: int) -> list[date]:
            seen.append(limit)
            return [date(2026, 11, 5)]

        code, _ = _run_main([], collector=_collector({date(2026, 11, 5): self.OK_DAY}), date_loader=loader)
        _run_main(['--last', '2'], collector=_collector({date(2026, 11, 5): self.OK_DAY}), date_loader=loader)

        self.assertEqual(code, EXIT_OK)
        self.assertEqual(seen, [5, 2])

    def test_any_degraded_day_makes_the_whole_run_degraded(self) -> None:
        code, output = _run_main(
            ['--last', '2'],
            collector=_collector({date(2026, 11, 5): self.OK_DAY, date(2026, 11, 4): HOLE_DAY}),
            date_loader=lambda _limit: [date(2026, 11, 5), date(2026, 11, 4)],
        )

        self.assertEqual(code, EXIT_DEGRADED)
        self.assertIn('market_date=2026-11-05 price_bars=2769 status=OK', output)
        self.assertIn('market_date=2026-11-04 price_bars=2769 status=DEGRADED', output)

    def test_no_available_dates_cannot_report_a_successful_audit(self) -> None:
        code, output = _run_main(
            ['--format', 'json'],
            date_loader=lambda _limit: [],
        )

        self.assertEqual(code, EXIT_DEGRADED)
        self.assertEqual(json.loads(output), [])

    def test_json_format_is_machine_readable(self) -> None:
        code, output = _run_main(
            ['--market-date', '2026-11-04', '--format', 'json'],
            collector=_collector({date(2026, 11, 4): HOLE_DAY}),
        )

        payload = json.loads(output)
        self.assertEqual(code, EXIT_DEGRADED)
        self.assertEqual(payload[0]['market_date'], '2026-11-04')
        self.assertIn('momentum', payload[0]['problem_tables'])

    def test_min_ratio_is_applied(self) -> None:
        counts = {**self.OK_DAY, 'momentum': 2500}
        strict, _ = _run_main(
            ['--market-date', '2026-11-05', '--min-ratio', '0.95'],
            collector=_collector({date(2026, 11, 5): counts}),
        )
        lenient, _ = _run_main(
            ['--market-date', '2026-11-05', '--min-ratio', '0.50'],
            collector=_collector({date(2026, 11, 5): counts}),
        )

        self.assertEqual(strict, EXIT_DEGRADED)
        self.assertEqual(lenient, EXIT_OK)

    def test_market_date_and_last_are_mutually_exclusive(self) -> None:
        with self.assertRaises(SystemExit) as raised, redirect_stdout(io.StringIO()):
            import contextlib

            with contextlib.redirect_stderr(io.StringIO()):
                main(['--market-date', '2026-11-05', '--last', '3'])

        self.assertEqual(raised.exception.code, 2)

    def test_render_text_is_stable_for_a_known_report(self) -> None:
        report = evaluate_daily_coverage(
            {**self.OK_DAY, 'momentum': 0}, market_date='2026-11-05'
        )

        text = render_text([report])
        self.assertTrue(text.endswith('\n'))
        self.assertIn('momentum', text)
        self.assertIn('empty', text)


if __name__ == '__main__':
    unittest.main()
