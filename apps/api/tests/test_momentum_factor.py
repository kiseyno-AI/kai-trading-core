import unittest
from contextlib import contextmanager
from datetime import date, timedelta
from unittest.mock import patch

from app.signals.momentum_factor import (
    DailyMomentumFactorService,
    MomentumEligibilityRule,
    build_daily_momentum_factor_rows,
)


def _build_close_series(initial_close: float, daily_returns: list[float]) -> list[float]:
    closes = [initial_close]
    for value in daily_returns:
        closes.append(round(closes[-1] * (1.0 + value), 6))
    return closes


def _build_series_with_trailing_returns(
    trailing_returns: list[float],
    *,
    initial_close: float = 100.0,
    baseline_return: float = 0.001,
) -> list[float]:
    if len(trailing_returns) > 252:
        raise ValueError('trailing_returns must fit inside the 252-day return history window')
    daily_returns = ([baseline_return] * (252 - len(trailing_returns))) + list(trailing_returns)
    return _build_close_series(initial_close, daily_returns)


def _build_rankable_momentum_series(daily_return: float) -> list[float]:
    return _build_close_series(100.0, ([daily_return] * 231) + ([0.0] * 21))


def _build_bars(closes: list[float], *, start_date: date = date(2025, 1, 1)) -> list[dict]:
    bars: list[dict] = []
    for index, close in enumerate(closes):
        trade_date = (start_date + timedelta(days=index)).isoformat()
        bars.append(
            {
                'trade_date': trade_date,
                'open_price': close,
                'high_price': close + 1,
                'low_price': max(close - 1, 1),
                'close_price': close,
                'volume': 1_000_000 + index,
                'turnover': close * 1_000,
                'source': 'pykrx',
            }
        )
    return bars


class DailyMomentumFactorBuilderTests(unittest.TestCase):
    def test_build_rows_computes_expected_12_1_momentum(self) -> None:
        closes = [100] + [150] * 252

        rows = build_daily_momentum_factor_rows(
            market_date='2025-09-10',
            price_histories={'005930': _build_bars(closes)},
        )

        self.assertEqual(rows[0]['symbol'], '005930')
        self.assertEqual(rows[0]['momentum_12_1'], 0.5)
        self.assertEqual(rows[0]['momentum_rank'], 1)
        self.assertEqual(rows[0]['momentum_percentile'], 1.0)
        self.assertTrue(rows[0]['has_sufficient_history'])
        self.assertEqual(rows[0]['factor_payload']['inputs']['reference_close_t_minus_21'], 150)
        self.assertEqual(rows[0]['factor_payload']['inputs']['reference_close_t_minus_252'], 100)

    def test_build_rows_marks_insufficient_history_without_momentum_rank(self) -> None:
        closes = [100] * 120

        rows = build_daily_momentum_factor_rows(
            market_date='2025-04-30',
            price_histories={'005930': _build_bars(closes)},
        )

        self.assertIsNone(rows[0]['momentum_12_1'])
        self.assertFalse(rows[0]['has_sufficient_history'])
        self.assertIsNone(rows[0]['momentum_rank'])
        self.assertIsNone(rows[0]['momentum_percentile'])
        self.assertEqual(
            rows[0]['factor_payload']['insufficient_history_reason'],
            'requires_at_least_253_stored_trading_bars',
        )

    def test_build_rows_computes_moving_average_flags(self) -> None:
        closes = ([100] * 133) + ([150] * 60) + ([200] * 60)

        rows = build_daily_momentum_factor_rows(
            market_date='2025-09-10',
            price_histories={'005930': _build_bars(closes)},
        )

        self.assertEqual(rows[0]['ma_60'], 200.0)
        self.assertEqual(rows[0]['ma_120'], 175.0)
        self.assertTrue(rows[0]['is_price_above_ma120'])
        self.assertTrue(rows[0]['is_ma60_above_ma120'])
        self.assertTrue(rows[0]['is_momentum_eligible'])

    def test_build_rows_marks_extreme_spike_overheated_with_percentile_and_volatility_context(self) -> None:
        normal_closes = _build_series_with_trailing_returns([0.004, 0.004, 0.004, 0.004, 0.004])
        hot_closes = _build_series_with_trailing_returns([0.015, 0.02, 0.025, 0.03, 0.035])

        rows = build_daily_momentum_factor_rows(
            market_date='2025-09-10',
            price_histories={
                '000660': _build_bars(normal_closes),
                '005930': _build_bars(hot_closes),
            },
        )

        by_symbol = {row['symbol']: row for row in rows}
        self.assertFalse(by_symbol['000660']['is_recent_overheated'])
        self.assertEqual(by_symbol['005930']['return_5d_rank'], 1)
        self.assertEqual(by_symbol['005930']['return_5d_percentile'], 1.0)
        self.assertTrue(by_symbol['005930']['has_sufficient_cross_sectional_context'])
        self.assertTrue(by_symbol['005930']['is_return_5d_extreme'])
        self.assertTrue(by_symbol['005930']['is_volatility_adjusted_overheated'])
        self.assertTrue(by_symbol['005930']['passes_minimum_absolute_return'])
        self.assertTrue(by_symbol['005930']['is_recent_overheated'])
        self.assertEqual(by_symbol['005930']['overheat_rule_version'], 'momentum_overheat_v2')
        self.assertEqual(
            by_symbol['005930']['factor_payload']['overheat_rule']['cross_sectional_percentile']['threshold'],
            0.98,
        )
        self.assertFalse(by_symbol['005930']['is_momentum_eligible'])

    def test_build_rows_keeps_high_absolute_return_when_volatility_context_absorbs_move(self) -> None:
        high_volatility_closes = _build_series_with_trailing_returns(
            [
                0.08,
                -0.07,
                0.08,
                -0.06,
                0.09,
                -0.08,
                0.08,
                -0.07,
                0.09,
                -0.08,
                0.08,
                -0.06,
                0.08,
                -0.07,
                0.09,
                0.08,
                -0.05,
                0.08,
                -0.03,
                0.08,
            ]
        )
        steady_closes = _build_series_with_trailing_returns([0.006, 0.006, 0.006, 0.006, 0.006])

        rows = build_daily_momentum_factor_rows(
            market_date='2025-09-10',
            price_histories={
                '000660': _build_bars(steady_closes),
                '005930': _build_bars(high_volatility_closes),
            },
        )

        overheated_candidate = {row['symbol']: row for row in rows}['005930']
        self.assertTrue(overheated_candidate['is_return_5d_extreme'])
        self.assertTrue(overheated_candidate['passes_minimum_absolute_return'])
        self.assertGreater(overheated_candidate['return_5d'], 0.07)
        self.assertLess(overheated_candidate['return_5d_vol_adjusted'], 2.25)
        self.assertFalse(overheated_candidate['is_volatility_adjusted_overheated'])
        self.assertFalse(overheated_candidate['is_recent_overheated'])

    def test_build_rows_requires_minimum_absolute_return_even_for_top_percentile_name(self) -> None:
        mild_leader_closes = _build_series_with_trailing_returns([0.01, 0.01, 0.01, 0.01, 0.01])
        laggard_closes = _build_series_with_trailing_returns([0.0, 0.0, 0.0, 0.0, 0.0])

        rows = build_daily_momentum_factor_rows(
            market_date='2025-09-10',
            price_histories={
                '000660': _build_bars(laggard_closes),
                '005930': _build_bars(mild_leader_closes),
            },
        )

        mild_leader = {row['symbol']: row for row in rows}['005930']
        self.assertTrue(mild_leader['is_return_5d_extreme'])
        self.assertTrue(mild_leader['is_volatility_adjusted_overheated'])
        self.assertFalse(mild_leader['passes_minimum_absolute_return'])
        self.assertFalse(mild_leader['is_recent_overheated'])

    def test_build_rows_does_not_flag_single_symbol_without_cross_sectional_context(self) -> None:
        hot_closes = _build_series_with_trailing_returns([0.015, 0.02, 0.025, 0.03, 0.035])

        rows = build_daily_momentum_factor_rows(
            market_date='2025-09-10',
            price_histories={'005930': _build_bars(hot_closes)},
        )

        self.assertEqual(rows[0]['return_5d_rank'], 1)
        self.assertEqual(rows[0]['return_5d_percentile'], 1.0)
        self.assertEqual(rows[0]['return_5d_ranked_universe_size'], 1)
        self.assertFalse(rows[0]['has_sufficient_cross_sectional_context'])
        self.assertFalse(rows[0]['is_return_5d_extreme'])
        self.assertFalse(rows[0]['is_recent_overheated'])

    def test_build_rows_assigns_cross_sectional_rank_and_percentile(self) -> None:
        histories = {
            '005930': _build_bars([100] + [150] * 252),
            '000660': _build_bars([100] + [120] * 252),
            '035420': _build_bars([100] + [90] * 252),
        }

        rows = build_daily_momentum_factor_rows(
            market_date='2025-09-10',
            price_histories=histories,
        )

        by_symbol = {row['symbol']: row for row in rows}
        self.assertEqual(by_symbol['005930']['momentum_rank'], 1)
        self.assertEqual(by_symbol['005930']['momentum_percentile'], 1.0)
        self.assertEqual(by_symbol['000660']['momentum_rank'], 2)
        self.assertEqual(by_symbol['000660']['momentum_percentile'], 0.5)
        self.assertEqual(by_symbol['035420']['momentum_rank'], 3)
        self.assertEqual(by_symbol['035420']['momentum_percentile'], 0.0)

    def test_build_rows_requires_top_percentile_cutoff_for_eligibility(self) -> None:
        rows = build_daily_momentum_factor_rows(
            market_date='2025-09-10',
            price_histories={
                '005930': _build_bars(_build_rankable_momentum_series(0.0035)),
                '000660': _build_bars(_build_rankable_momentum_series(0.0030)),
                '035420': _build_bars(_build_rankable_momentum_series(0.0025)),
                '051910': _build_bars(_build_rankable_momentum_series(0.0020)),
                '068270': _build_bars(_build_rankable_momentum_series(0.0015)),
            },
        )

        by_symbol = {row['symbol']: row for row in rows}
        self.assertTrue(by_symbol['005930']['passes_momentum_top_percentile'])
        self.assertTrue(by_symbol['005930']['is_momentum_eligible'])
        self.assertEqual(by_symbol['005930']['momentum_eligibility_status'], 'eligible')

        self.assertEqual(by_symbol['000660']['momentum_percentile'], 0.75)
        self.assertEqual(by_symbol['000660']['momentum_percentile_threshold'], 0.8)
        self.assertFalse(by_symbol['000660']['passes_momentum_top_percentile'])
        self.assertFalse(by_symbol['000660']['is_momentum_eligible'])
        self.assertEqual(by_symbol['000660']['momentum_eligibility_status'], 'below_top_percentile_cutoff')
        self.assertIn(
            'passes_momentum_top_percentile',
            by_symbol['000660']['factor_payload']['eligibility']['failed_requirements'],
        )

    def test_build_rows_allows_configurable_top_percentile_cutoff(self) -> None:
        rows = build_daily_momentum_factor_rows(
            market_date='2025-09-10',
            price_histories={
                '005930': _build_bars(_build_rankable_momentum_series(0.0035)),
                '000660': _build_bars(_build_rankable_momentum_series(0.0030)),
                '035420': _build_bars(_build_rankable_momentum_series(0.0025)),
                '051910': _build_bars(_build_rankable_momentum_series(0.0020)),
                '068270': _build_bars(_build_rankable_momentum_series(0.0015)),
            },
            eligibility_rule=MomentumEligibilityRule(top_percentile_threshold=0.75),
        )

        by_symbol = {row['symbol']: row for row in rows}
        self.assertEqual(by_symbol['000660']['momentum_percentile_threshold'], 0.75)
        self.assertTrue(by_symbol['000660']['passes_momentum_top_percentile'])
        self.assertTrue(by_symbol['000660']['is_momentum_eligible'])
        self.assertEqual(by_symbol['000660']['momentum_eligibility_status'], 'eligible')


class DailyMomentumFactorServiceTests(unittest.TestCase):
    def test_rebuild_for_market_date_persists_rows_for_selected_universe(self) -> None:
        service = DailyMomentumFactorService()
        histories = {
            '005930': list(reversed(_build_bars([100] + [150] * 252))),
            '000660': list(reversed(_build_bars([100] + [120] * 252))),
        }

        with (
            patch(
                'app.signals.momentum_factor.list_symbols_with_daily_bar_on_date',
                return_value=['005930', '000660'],
            ),
            patch(
                'app.signals.momentum_factor.load_recent_price_bars_by_symbol',
                return_value=histories,
            ),
            patch('app.signals.momentum_factor.upsert_daily_momentum_factor_results') as upsert,
        ):
            result = service.rebuild_for_market_date(market_date='2025-09-10')

        self.assertEqual(result['requested_symbols'], 2)
        self.assertEqual(result['requested_symbol_count'], 2)
        self.assertEqual(result['symbols_evaluated'], 2)
        self.assertEqual(result['ready_count'], 2)
        self.assertEqual(result['unready_count'], 0)
        self.assertEqual(result['persisted_ready_count'], 2)
        self.assertEqual(result['persisted_unready_count'], 0)
        self.assertEqual(result['ranked_symbols'], 2)
        self.assertEqual(result['top_percentile_symbols'], 1)
        self.assertEqual(result['results_persisted'], 2)
        self.assertEqual(result['results_failed'], 0)
        upsert.assert_called_once()
        persisted = {item['symbol']: item for item in upsert.call_args.args[0]}
        self.assertEqual(persisted['005930']['momentum_rank'], 1)
        self.assertEqual(persisted['000660']['momentum_rank'], 2)

    def test_persisted_readiness_excludes_the_only_ready_row_when_its_write_fails(self) -> None:
        service = DailyMomentumFactorService()
        histories = {
            '005930': list(reversed(_build_bars([100] + [150] * 252))),
            '000660': list(
                reversed(
                    _build_bars(
                        [100] * 40,
                        start_date=date(2025, 8, 2),
                    )
                )
            ),
        }

        def persist_one(row):
            if row.get('has_sufficient_history'):
                raise RuntimeError('ready row write failed')

        with (
            patch(
                'app.signals.momentum_factor.list_symbols_with_daily_bar_on_date',
                return_value=['005930', '000660'],
            ),
            patch(
                'app.signals.momentum_factor.load_recent_price_bars_by_symbol',
                return_value=histories,
            ),
            patch(
                'app.signals.momentum_factor.upsert_daily_momentum_factor_results',
                side_effect=RuntimeError('batch failed'),
            ),
            patch(
                'app.signals.momentum_factor.upsert_daily_momentum_factor_result',
                side_effect=persist_one,
            ),
        ):
            result = service.rebuild_for_market_date(market_date='2025-09-10')

        self.assertEqual(result['ready_count'], 1)
        self.assertEqual(result['unready_count'], 1)
        self.assertEqual(result['results_persisted'], 1)
        self.assertEqual(result['results_failed'], 1)
        self.assertEqual(result['persisted_ready_count'], 0)
        self.assertEqual(result['persisted_unready_count'], 1)

    def test_subset_rebuild_uses_full_day_universe_rankings_and_only_persists_target(self) -> None:
        service = DailyMomentumFactorService()
        histories = {
            '005930': list(reversed(_build_bars([100] + [150] * 252))),
            '000660': list(reversed(_build_bars([100] + [120] * 252))),
            '035420': list(reversed(_build_bars([100] + [90] * 252))),
        }

        with (
            patch(
                'app.signals.momentum_factor.list_symbols_with_daily_bar_on_date',
                return_value=['005930', '000660', '035420'],
            ),
            patch(
                'app.signals.momentum_factor.load_recent_price_bars_by_symbol',
                return_value=histories,
            ) as load_histories,
            patch('app.signals.momentum_factor.upsert_daily_momentum_factor_results') as upsert,
        ):
            result = service.rebuild_for_market_date(
                market_date='2025-09-10',
                symbols=['000660'],
            )

        load_histories.assert_called_once_with(
            market_date='2025-09-10',
            symbols=['005930', '000660', '035420'],
            history_days=service.history_window,
        )
        self.assertEqual(result['requested_symbols'], 1)
        self.assertEqual(result['requested_symbol_count'], 1)
        self.assertEqual(result['symbols_evaluated'], 1)
        self.assertEqual(result['ranked_symbols'], 1)
        self.assertEqual(result['results_persisted'], 1)
        self.assertEqual([item['symbol'] for item in result['items']], ['000660'])
        upsert.assert_called_once()
        persisted = upsert.call_args.args[0][0]
        self.assertEqual(persisted['symbol'], '000660')
        self.assertEqual(persisted['momentum_rank'], 2)
        self.assertEqual(persisted['momentum_percentile'], 0.5)
        self.assertEqual(persisted['factor_payload']['ranking']['ranked_universe_size'], 3)

    def test_evaluate_symbol_uses_requested_universe_for_cross_sectional_rank(self) -> None:
        service = DailyMomentumFactorService()
        histories = {
            '005930': list(reversed(_build_bars([100] + [150] * 252))),
            '000660': list(reversed(_build_bars([100] + [120] * 252))),
        }

        with patch(
            'app.signals.momentum_factor.load_recent_price_bars_by_symbol',
            return_value=histories,
        ):
            result = service.evaluate_symbol(
                symbol='5930',
                market_date='2025-09-10',
                universe_symbols=['005930', '000660'],
            )

        self.assertEqual(result['symbol'], '005930')
        self.assertEqual(result['momentum_rank'], 1)
        self.assertEqual(result['factor_payload']['ranking']['ranked_universe_size'], 2)

    def test_evaluate_symbol_holds_date_fence_before_persisting(self) -> None:
        service = DailyMomentumFactorService()
        histories = {
            '005930': list(reversed(_build_bars([100] + [150] * 252))),
            '000660': list(reversed(_build_bars([100] + [120] * 252))),
        }
        events: list[tuple[str, str]] = []

        @contextmanager
        def admission(*, market_date: str | date):
            normalized = (
                market_date.isoformat()
                if isinstance(market_date, date)
                else market_date
            )
            events.append(('lock_enter', normalized))
            yield
            events.append(('lock_exit', normalized))

        def persist(row: dict) -> None:
            self.assertEqual(events, [('lock_enter', '2025-09-10')])
            events.append(('persist', row['symbol']))

        with (
            patch(
                'app.signals.momentum_factor.daily_price_bar_writer_admission_lock',
                side_effect=admission,
            ),
            patch(
                'app.signals.momentum_factor.load_recent_price_bars_by_symbol',
                return_value=histories,
            ),
            patch(
                'app.signals.momentum_factor.upsert_daily_momentum_factor_result',
                side_effect=persist,
            ),
        ):
            result = service.evaluate_symbol(
                symbol='005930',
                market_date='2025-09-10',
                universe_symbols=['005930', '000660'],
                persist=True,
            )

        self.assertEqual(result['symbol'], '005930')
        self.assertEqual(
            events,
            [
                ('lock_enter', '2025-09-10'),
                ('persist', '005930'),
                ('lock_exit', '2025-09-10'),
            ],
        )

    def test_inspect_returns_summary_for_requested_symbols(self) -> None:
        service = DailyMomentumFactorService()
        stored_items = [
            {
                'symbol': '005930',
                'market_date': '2025-09-10',
                'return_5d': 0.134337,
                'momentum_rank': 1,
                'momentum_percentile': 0.83,
                'is_momentum_eligible': False,
                'is_recent_overheated': True,
                'factor_payload': {
                    'ranking': {
                        'momentum_percentile_threshold': 0.8,
                        'passes_momentum_top_percentile': True,
                    },
                    'eligibility': {
                        'passes_trend_confirmation': True,
                        'passes_momentum_top_percentile': True,
                        'momentum_eligibility_status': 'recently_overheated',
                        'failed_requirements': ['not_recently_overheated'],
                    },
                    'returns': {
                        'return_5d_rank': 1,
                        'return_5d_percentile': 1.0,
                        'return_5d_ranked_universe_size': 75,
                    },
                    'volatility': {
                        'return_5d_realized_volatility_20d': 0.019851,
                        'return_5d_vol_adjusted': 3.026114,
                    },
                    'overheat_rule': {'rule_version': 'momentum_overheat_v2'},
                    'overheat': {
                        'has_sufficient_cross_sectional_context': True,
                        'is_return_5d_extreme': True,
                        'is_volatility_adjusted_overheated': True,
                        'passes_minimum_absolute_return': True,
                        'is_recent_overheated': True,
                    },
                },
            }
        ]

        with patch(
            'app.signals.momentum_factor.load_daily_momentum_factor_results',
            return_value=stored_items,
        ) as loader:
            result = service.inspect(market_date='2025-09-10', symbols=['5930', '000660'], limit=10)

        loader.assert_called_once_with(
            market_date=date(2025, 9, 10),
            symbols=['005930', '000660'],
            limit=10,
        )
        self.assertEqual(result['summary']['entries'], 1)
        self.assertEqual(result['summary']['matched_symbols'], ['005930'])
        self.assertEqual(result['summary']['missing_symbols'], ['000660'])
        self.assertEqual(result['summary']['top_percentile_symbols'], 1)
        self.assertEqual(result['summary']['eligible_symbols'], 0)
        item = result['items'][0]
        self.assertEqual(item['momentum_percentile_threshold'], 0.8)
        self.assertTrue(item['passes_momentum_top_percentile'])
        self.assertEqual(item['momentum_eligibility_status'], 'recently_overheated')
        self.assertEqual(item['return_5d_rank'], 1)
        self.assertEqual(item['return_5d_percentile'], 1.0)
        self.assertEqual(item['return_5d_ranked_universe_size'], 75)
        self.assertEqual(item['return_5d_realized_volatility_20d'], 0.019851)
        self.assertEqual(item['return_5d_vol_adjusted'], 3.026114)
        self.assertTrue(item['has_sufficient_cross_sectional_context'])
        self.assertTrue(item['is_return_5d_extreme'])
        self.assertTrue(item['is_volatility_adjusted_overheated'])
        self.assertTrue(item['passes_minimum_absolute_return'])
        self.assertTrue(item['is_recent_overheated'])
        self.assertEqual(item['overheat_rule_version'], 'momentum_overheat_v2')
