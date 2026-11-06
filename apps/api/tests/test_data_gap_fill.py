from __future__ import annotations

from datetime import date, datetime, time, timedelta
from zoneinfo import ZoneInfo
import unittest

from app.eod.data_gap_fill import (
    TIER_A_KINDS,
    TIER_B_KINDS,
    DataGapFillService,
    Gap,
    GapFillConfig,
)

KST = ZoneInfo('Asia/Seoul')
# Wednesday 2026-11-04, 10:00 KST: inside the default window.
IN_WINDOW = datetime(2026, 11, 4, 10, 0, tzinfo=KST)
SESSIONS = ['2026-10-29', '2026-10-30', '2026-11-02', '2026-11-03']


def _config(**overrides) -> GapFillConfig:
    values = dict(
        enabled=True,
        apply=True,
        lookback_trading_days=10,
        window_start=time(9, 30),
        window_end=time(15, 0),
        min_interval_seconds=1800,
        budget_seconds=900,
        failure_cooldown_seconds=6 * 3600,
        provider_check_sessions=2,
    )
    values.update(overrides)
    return GapFillConfig(**values)


class _Harness:
    """Records every detector/fixer call so tests can assert on side effects."""

    def __init__(self, *, gaps=None, fixer_results=None, sessions=None):
        self.gaps = gaps or {}  # (kind, date) -> count
        self.fixer_results = fixer_results or {}  # (kind, date) -> mapping | Exception
        self.sessions = list(SESSIONS if sessions is None else sessions)
        self.detector_calls: list[tuple[str, str]] = []
        self.fixer_calls: list[tuple[str, str]] = []
        self.sessions_limit: int | None = None
        self.finality_checked: list[str] = []
        self.now = IN_WINDOW
        self.mono = 0.0

    def sessions_loader(self, limit):
        self.sessions_limit = limit
        return self.sessions[-limit:]

    def detector(self, kind):
        def run(market_date):
            self.detector_calls.append((kind, market_date))
            count = self.gaps.get((kind, market_date))
            return Gap(kind, market_date, count) if count else None

        return run

    def fixer(self, kind):
        def run(market_date):
            self.fixer_calls.append((kind, market_date))
            outcome = self.fixer_results.get((kind, market_date), {'status': 'completed'})
            if isinstance(outcome, Exception):
                raise outcome
            # A completed fixer closes the gap so a second run is idempotent.
            if outcome.get('status') == 'completed':
                self.gaps.pop((kind, market_date), None)
            return outcome

        return run

    def finality(self, market_date, **_kwargs):
        self.finality_checked.append(str(market_date))
        if str(market_date) == '2026-11-03' and getattr(self, 'reject_1103', False):
            raise ValueError('not finalized')
        return market_date

    def service(self, config=None, *, today=None, trading_day=True):
        return DataGapFillService(
            config=config or _config(),
            sessions_loader=self.sessions_loader,
            detectors={kind: self.detector(kind) for kind in (*TIER_A_KINDS, *TIER_B_KINDS)},
            fixers={kind: self.fixer(kind) for kind in TIER_A_KINDS},
            finality_validator=self.finality,
            clock=lambda: self.now,
            monotonic=lambda: self.mono,
            is_trading_day=lambda _d: trading_day,
        )


class SpecScheduleTests(unittest.TestCase):
    def test_1_disabled_calls_nothing(self) -> None:
        h = _Harness(gaps={('signal_features_missing', '2026-11-03'): 5})
        result = h.service(_config(enabled=False)).run_if_due()

        self.assertEqual(result['status'], 'disabled')
        self.assertEqual((h.detector_calls, h.fixer_calls), ([], []))
        self.assertIsNone(h.sessions_limit)

    def test_2_outside_window_weekend_or_holiday_is_a_noop(self) -> None:
        for label, now, trading_day in (
            ('before window', IN_WINDOW.replace(hour=8), True),
            ('after window', IN_WINDOW.replace(hour=15, minute=0), True),
            ('saturday', datetime(2026, 11, 7, 10, 0, tzinfo=KST), True),
            ('holiday', IN_WINDOW, False),
        ):
            with self.subTest(label):
                h = _Harness(gaps={('signal_features_missing', '2026-11-03'): 5})
                h.now = now
                result = h.service(trading_day=trading_day).run_if_due()
                self.assertEqual(result['status'], 'not_due')
                self.assertEqual((h.detector_calls, h.fixer_calls), ([], []))

    def test_2b_window_boundaries_are_start_inclusive_end_exclusive(self) -> None:
        h = _Harness()
        h.now = IN_WINDOW.replace(hour=9, minute=30)
        self.assertEqual(h.service().run_if_due()['status'], 'completed')
        h = _Harness()
        h.now = IN_WINDOW.replace(hour=14, minute=59)
        self.assertEqual(h.service().run_if_due()['status'], 'completed')

    def test_3_min_interval_blocks_a_second_run_until_it_elapses(self) -> None:
        h = _Harness()
        service = h.service()
        self.assertEqual(service.run_if_due()['status'], 'completed')

        h.now = IN_WINDOW + timedelta(minutes=10)
        self.assertEqual(service.run_if_due()['status'], 'not_due')

        h.now = IN_WINDOW + timedelta(minutes=31)
        self.assertEqual(service.run_if_due()['status'], 'completed')


class SpecApplyTests(unittest.TestCase):
    def test_4_apply_off_reports_gaps_and_runs_no_fixer(self) -> None:
        h = _Harness(gaps={('signal_features_missing', '2026-11-03'): 7})
        result = h.service(_config(apply=False)).run_if_due()

        self.assertEqual(h.fixer_calls, [])
        self.assertEqual([(g['kind'], g['market_date'], g['count']) for g in result['gaps']], [('signal_features_missing', '2026-11-03', 7)])
        self.assertEqual(result['repaired'], [])
        self.assertFalse(result['apply'])

    def test_5_only_tier_a_kinds_ever_reach_a_fixer(self) -> None:
        gaps = {(kind, '2026-11-03'): 3 for kind in (*TIER_A_KINDS, *TIER_B_KINDS)}
        h = _Harness(gaps=gaps)
        result = h.service().run_if_due()

        self.assertEqual({kind for kind, _ in h.fixer_calls}, set(TIER_A_KINDS))
        self.assertEqual({item['kind'] for item in result['tier_b']}, set(TIER_B_KINDS))
        self.assertEqual(result['repaired_kinds'], list(TIER_A_KINDS))

    def test_6_today_and_unfinalized_sessions_are_never_repaired(self) -> None:
        h = _Harness(
            gaps={
                ('signal_features_missing', '2026-11-03'): 4,
                ('signal_features_missing', '2026-11-04'): 4,  # today
            },
            sessions=[*SESSIONS, '2026-11-04'],
        )
        h.reject_1103 = True
        result = h.service().run_if_due()

        self.assertNotIn('2026-11-04', {d for _, d in h.detector_calls})
        self.assertNotIn('2026-11-03', {d for _, d in h.fixer_calls})
        self.assertIn('2026-11-03', result['skipped_unfinalized'])

    def test_7_only_the_configured_lookback_is_examined(self) -> None:
        h = _Harness()
        h.service(_config(lookback_trading_days=2)).run_if_due()

        self.assertEqual(h.sessions_limit, 3)  # +1 so "today" can be dropped
        self.assertEqual({d for _, d in h.detector_calls}, {'2026-11-02', '2026-11-03'})

    def test_8_fixers_run_in_dependency_order_oldest_session_first(self) -> None:
        gaps = {
            ('turnover_incomplete', '2026-10-29'): 1,
            ('turnover_incomplete', '2026-10-30'): 1,
            ('participation_context_missing', '2026-10-30'): 1,
            ('signal_features_missing', '2026-10-30'): 1,
            ('signal_features_missing', '2026-10-29'): 1,
        }
        h = _Harness(gaps=gaps)
        h.service().run_if_due()

        self.assertEqual(
            h.fixer_calls,
            [
                ('signal_features_missing', '2026-10-29'),
                ('turnover_incomplete', '2026-10-29'),
                ('signal_features_missing', '2026-10-30'),
                ('participation_context_missing', '2026-10-30'),
                ('turnover_incomplete', '2026-10-30'),
            ],
        )

    def test_9_budget_exhaustion_defers_the_rest(self) -> None:
        gaps = {('signal_features_missing', d): 1 for d in SESSIONS}
        h = _Harness(gaps=gaps)
        service = h.service(_config(budget_seconds=10))
        original = h.fixer

        def slow_fixer(kind):
            inner = original(kind)

            def run(market_date):
                h.mono += 6.0  # each fix "takes" 6 seconds
                return inner(market_date)

            return run

        service.fixers = {kind: slow_fixer(kind) for kind in TIER_A_KINDS}
        result = service.run_if_due()

        self.assertEqual(len(h.fixer_calls), 2)
        self.assertEqual(len(result['deferred']), 2)
        self.assertTrue(all(item['reason'] == 'budget_exhausted' for item in result['deferred']))

    def test_10_a_failing_fixer_is_isolated_and_cooled_down(self) -> None:
        gaps = {
            ('signal_features_missing', '2026-10-30'): 1,
            ('signal_features_missing', '2026-11-02'): 1,
        }
        h = _Harness(
            gaps=gaps,
            fixer_results={('signal_features_missing', '2026-10-30'): RuntimeError('boom')},
        )
        service = h.service()
        first = service.run_if_due()

        self.assertEqual([item['market_date'] for item in first['failed']], ['2026-10-30'])
        self.assertEqual([item['market_date'] for item in first['repaired']], ['2026-11-02'])

        h.fixer_calls.clear()
        h.now = IN_WINDOW + timedelta(hours=1)
        second = service.run_if_due()
        self.assertEqual(h.fixer_calls, [])  # still cooling down
        self.assertEqual(second['deferred'][0]['reason'], 'cooldown')

        h.fixer_results.clear()
        h.now = IN_WINDOW + timedelta(hours=6, minutes=5)
        service.run_if_due(force_schedule=True)  # CLI mode ignores the window
        self.assertEqual(h.fixer_calls, [('signal_features_missing', '2026-10-30')])

    def test_11_blocked_fence_is_deferred_not_failed(self) -> None:
        h = _Harness(
            gaps={('signal_features_missing', '2026-11-03'): 1},
            fixer_results={
                ('signal_features_missing', '2026-11-03'): {
                    'status': 'blocked',
                    'reason': 'active_daily_or_eod_writer',
                }
            },
        )
        result = h.service().run_if_due()

        self.assertEqual(result['failed'], [])
        self.assertEqual(result['deferred'][0]['reason'], 'active_daily_or_eod_writer')

    def test_12_second_run_after_success_is_idempotent(self) -> None:
        h = _Harness(gaps={('signal_features_missing', '2026-11-03'): 4})
        service = h.service()
        first = service.run_if_due()
        self.assertEqual(len(first['repaired']), 1)

        h.fixer_calls.clear()
        h.now = IN_WINDOW + timedelta(minutes=45)
        second = service.run_if_due()
        self.assertEqual(second['gaps'], [])
        self.assertEqual(h.fixer_calls, [])

    def test_13_a_failing_detector_is_unknown_and_does_not_stop_the_others(self) -> None:
        h = _Harness(gaps={('turnover_incomplete', '2026-11-03'): 2})
        service = h.service()

        def broken(_market_date):
            raise RuntimeError('db hiccup')

        service.detectors = {**service.detectors, 'signal_features_missing': broken}
        result = service.run_if_due()

        self.assertIn('signal_features_missing', {item['kind'] for item in result['detector_errors']})
        self.assertEqual(h.fixer_calls, [('turnover_incomplete', '2026-11-03')])

    def test_15_completed_but_unchanged_is_not_retried_every_tick(self) -> None:
        h = _Harness(
            gaps={('turnover_incomplete', '2026-11-03'): 9},
            fixer_results={
                ('turnover_incomplete', '2026-11-03'): {'status': 'completed_partial', 'updated_count': 0}
            },
        )
        service = h.service()
        first = service.run_if_due()
        self.assertEqual([item['market_date'] for item in first['no_progress']], ['2026-11-03'])
        self.assertEqual(first['repaired'], [])

        h.fixer_calls.clear()
        h.now = IN_WINDOW + timedelta(hours=1)
        second = service.run_if_due()
        self.assertEqual(h.fixer_calls, [])
        self.assertEqual(second['deferred'][0]['reason'], 'cooldown')

    def test_14_gap_dataclass_is_json_friendly(self) -> None:
        gap = Gap('turnover_incomplete', '2026-11-03', 2, {'symbols': 2})
        self.assertEqual(
            gap.as_dict(),
            {'kind': 'turnover_incomplete', 'market_date': '2026-11-03', 'count': 2, 'detail': {'symbols': 2}},
        )


class RolloutSafetyTests(unittest.TestCase):
    def test_defaults_are_off_and_report_only(self) -> None:
        from app.core.settings import Settings

        defaults = Settings(_env_file=None)
        self.assertFalse(defaults.data_gap_fill_enabled)
        self.assertFalse(defaults.data_gap_fill_apply)

    def test_runner_step_is_gated_by_the_flag_and_an_open_market(self) -> None:
        import inspect

        from app.eod import runner

        source = inspect.getsource(runner)
        gate = "if settings.data_gap_fill_enabled and market.state == 'OPEN':"
        self.assertIn(gate, source)
        self.assertEqual(source.count('data_gap_fill_run_if_due,'), 1)
        # The step must sit inside the non-weekend/holiday branch, after the
        # portfolio refresh, so it can never precede trading-critical steps.
        self.assertLess(source.index('portfolio_summary_refresh_if_due'), source.index(gate))


if __name__ == '__main__':
    unittest.main()
