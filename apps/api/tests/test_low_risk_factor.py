import math
import unittest
from datetime import date, timedelta
from statistics import stdev
from unittest.mock import patch

from app.signals.low_risk_factor import (
    LOW_RISK_ELIGIBLE_SCORE,
    LOW_RISK_FEATURE_VERSION,
    LOW_RISK_LIQUIDITY_FLOOR_PERCENTILE,
    LOW_RISK_WATCHLIST_SCORE,
    DailyLowRiskFactorService,
    LowRiskFactorInput,
    build_turnover_series_from_bars,
    compute_average_trading_value,
    compute_max_drawdown,
    compute_realized_volatility,
)


def _build_close_series(initial_close: float, returns: list[float]) -> list[float]:
    closes = [initial_close]
    for value in returns:
        closes.append(closes[-1] * (1.0 + value))
    return closes


def _build_bars(
    market_date: date,
    *,
    closes: list[float],
    turnover: int,
) -> list[dict]:
    bars: list[dict] = []
    for offset, close_price in enumerate(reversed(closes)):
        trade_date = (market_date - timedelta(days=offset)).isoformat()
        scaled_close = int(round(close_price * 1000))
        bars.append(
            {
                'trade_date': trade_date,
                'open_price': scaled_close,
                'high_price': scaled_close,
                'low_price': scaled_close,
                'close_price': scaled_close,
                'volume': 1_000_000,
                'turnover': turnover,
                'source': 'pykrx',
                'captured_at': f'{trade_date}T15:30:00+09:00',
            }
        )
    return bars


class LowRiskFactorMathTests(unittest.TestCase):
    def test_compute_realized_volatility_60d_uses_simple_returns_and_annualization(self) -> None:
        returns = ([0.01, -0.005, 0.012, -0.004, 0.0] * 12)[:60]
        close_prices = _build_close_series(100.0, returns)

        result = compute_realized_volatility(close_prices, return_days=60)

        expected = stdev(returns) * math.sqrt(252.0)
        self.assertAlmostEqual(result, expected, places=6)

    def test_compute_realized_volatility_120d_uses_simple_returns_and_annualization(self) -> None:
        returns = ([0.008, -0.006, 0.01, -0.003, 0.002, -0.001] * 20)[:120]
        close_prices = _build_close_series(200.0, returns)

        result = compute_realized_volatility(close_prices, return_days=120)

        expected = stdev(returns) * math.sqrt(252.0)
        self.assertAlmostEqual(result, expected, places=6)

    def test_compute_max_drawdown_120d_returns_worst_peak_to_trough_loss(self) -> None:
        close_prices = [100.0] * 60 + [120.0, 118.0, 116.0, 90.0] + [92.0] * 56

        result = compute_max_drawdown(close_prices, lookback_bars=120)

        self.assertAlmostEqual(result, -0.25, places=6)

    def test_compute_average_trading_value_20d_uses_latest_20_bars(self) -> None:
        turnovers = [float(value) for value in range(200, 180, -1)]

        result = compute_average_trading_value(turnovers, lookback_bars=20)

        self.assertEqual(result, 190.5)

    def test_compute_average_trading_value_uses_exact_latest_window(self) -> None:
        result = compute_average_trading_value(
            [100.0, 80.0, -20.0, 60.0, 40.0],
            lookback_bars=3,
        )

        self.assertIsNone(result)

    def test_compute_average_trading_value_counts_true_no_trade_bar_as_zero(self) -> None:
        result = compute_average_trading_value(
            [100.0, 0.0, 80.0],
            lookback_bars=3,
        )

        self.assertEqual(result, 60.0)

    def test_turnover_series_marks_positive_volume_zero_turnover_invalid(self) -> None:
        result = build_turnover_series_from_bars(
            [
                {'volume': 10, 'turnover': 0},
                {'volume': 0, 'turnover': 0},
            ]
        )

        self.assertTrue(math.isnan(result[0]))
        self.assertEqual(result[1], 0.0)


class DailyLowRiskFactorServiceTests(unittest.TestCase):
    def test_zero_trading_value_is_not_ranked_as_liquid(self) -> None:
        service = DailyLowRiskFactorService()
        items = [
            {'symbol': '000001', 'avg_trading_value_20d': 0.0},
            {'symbol': '000002', 'avg_trading_value_20d': 1_000_000.0},
        ]

        count = service._assign_metric_rankings(
            items,
            value_key='avg_trading_value_20d',
            rank_key='liquidity_rank',
            percentile_key='liquidity_percentile',
            best='higher',
            positive_only=True,
        )

        self.assertEqual(count, 1)
        self.assertIsNone(items[0].get('liquidity_rank'))
        self.assertIsNone(items[0].get('liquidity_percentile'))
        self.assertEqual(items[1]['liquidity_rank'], 1)

    def test_rebuild_assigns_percentiles_and_combined_classification(self) -> None:
        service = DailyLowRiskFactorService()
        market_date = date(2026, 4, 23)
        low_risk_returns = ([0.002, -0.001, 0.0015, -0.0005] * 30)[:120]
        mid_risk_returns = ([0.01, -0.008, 0.012, -0.009] * 30)[:120]
        high_risk_returns = ([0.02, -0.018, 0.025, -0.02] * 30)[:120]
        mid_risk_returns[70] = -0.08
        high_risk_returns[70] = -0.25

        inputs = [
            LowRiskFactorInput(
                symbol='000001',
                name_ko='Low Risk',
                market='KOSPI',
                bars=_build_bars(
                    market_date,
                    closes=_build_close_series(100.0, low_risk_returns),
                    turnover=9_000_000,
                ),
            ),
            LowRiskFactorInput(
                symbol='000002',
                name_ko='Mid Risk',
                market='KOSPI',
                bars=_build_bars(
                    market_date,
                    closes=_build_close_series(100.0, mid_risk_returns),
                    turnover=4_000_000,
                ),
            ),
            LowRiskFactorInput(
                symbol='000003',
                name_ko='High Risk',
                market='KOSPI',
                bars=_build_bars(
                    market_date,
                    closes=_build_close_series(100.0, high_risk_returns),
                    turnover=1_000_000,
                ),
            ),
        ]

        with (
            patch.object(
                service,
                '_load_inputs',
                return_value=(inputs, ['000001', '000002', '000003'], []),
            ),
            patch('app.signals.low_risk_factor.upsert_daily_low_risk_factor_results') as upsert,
        ):
            result = service.rebuild_for_market_date(market_date=market_date)

        self.assertEqual(result['feature_version'], LOW_RISK_FEATURE_VERSION)
        self.assertEqual(result['results_persisted'], 3)
        self.assertEqual(result['results_failed'], 0)
        upsert.assert_called_once()

        persisted_by_symbol = {item['symbol']: item for item in upsert.call_args.args[0]}
        low_risk = persisted_by_symbol['000001']
        mid_risk = persisted_by_symbol['000002']
        high_risk = persisted_by_symbol['000003']

        self.assertEqual(low_risk['volatility_60d_rank'], 1)
        self.assertEqual(low_risk['volatility_60d_percentile'], 1.0)
        self.assertEqual(low_risk['volatility_120d_percentile'], 1.0)
        self.assertEqual(low_risk['max_drawdown_120d_percentile'], 1.0)
        self.assertEqual(low_risk['liquidity_percentile'], 1.0)
        self.assertTrue(low_risk['passes_liquidity_floor'])
        self.assertEqual(low_risk['low_risk_classification'], 'eligible')
        self.assertTrue(low_risk['is_low_risk_eligible'])
        self.assertGreaterEqual(low_risk['low_risk_score'], LOW_RISK_ELIGIBLE_SCORE)

        self.assertEqual(mid_risk['volatility_60d_rank'], 2)
        self.assertEqual(mid_risk['volatility_60d_percentile'], 0.5)
        self.assertEqual(mid_risk['low_risk_classification'], 'watchlist')
        self.assertFalse(mid_risk['is_low_risk_eligible'])
        self.assertTrue(mid_risk['passes_liquidity_floor'])
        self.assertAlmostEqual(mid_risk['low_risk_score'], LOW_RISK_WATCHLIST_SCORE, places=6)

        self.assertEqual(high_risk['volatility_60d_rank'], 3)
        self.assertEqual(high_risk['volatility_60d_percentile'], 0.0)
        self.assertEqual(high_risk['liquidity_percentile'], 0.0)
        self.assertFalse(high_risk['passes_liquidity_floor'])
        self.assertEqual(high_risk['low_risk_classification'], 'liquidity_excluded')
        self.assertFalse(high_risk['is_low_risk_eligible'])

        methodology = low_risk['factor_payload']['methodology']
        self.assertEqual(methodology['return_type'], 'simple')
        self.assertEqual(methodology['annualization_days'], 252)
        self.assertEqual(
            low_risk['factor_payload']['liquidity_rule']['floor_percentile'],
            LOW_RISK_LIQUIDITY_FLOOR_PERCENTILE,
        )
        self.assertEqual(result['summary']['eligible_count'], 1)
        self.assertEqual(result['summary']['watchlist_count'], 1)
        self.assertEqual(result['summary']['liquidity_excluded_count'], 1)

    def test_rebuild_marks_insufficient_history_but_preserves_available_raw_metrics(self) -> None:
        service = DailyLowRiskFactorService()
        market_date = date(2026, 4, 23)
        short_history_returns = ([0.004, -0.003, 0.002, -0.001] * 20)[:79]
        inputs = [
            LowRiskFactorInput(
                symbol='005930',
                name_ko='삼성전자',
                market='KOSPI',
                bars=_build_bars(
                    market_date,
                    closes=_build_close_series(100.0, short_history_returns),
                    turnover=7_500_000,
                ),
            )
        ]

        with (
            patch.object(service, '_load_inputs', return_value=(inputs, ['005930'], [])),
            patch('app.signals.low_risk_factor.upsert_daily_low_risk_factor_results') as upsert,
        ):
            result = service.rebuild_for_market_date(market_date=market_date)

        persisted = upsert.call_args.args[0][0]
        self.assertIsNotNone(persisted['volatility_60d'])
        self.assertIsNone(persisted['volatility_120d'])
        self.assertIsNone(persisted['max_drawdown_120d'])
        self.assertIsNotNone(persisted['avg_trading_value_20d'])
        self.assertEqual(persisted['low_risk_classification'], 'insufficient_history')
        self.assertIsNone(persisted['low_risk_score'])
        self.assertFalse(persisted['is_low_risk_eligible'])
        self.assertEqual(result['summary']['insufficient_history_count'], 1)
        self.assertEqual(result['summary']['ready_count'], 0)
        self.assertEqual(result['summary']['unready_count'], 1)

    def test_subset_rerun_uses_full_market_date_universe_for_persisted_rankings(self) -> None:
        service = DailyLowRiskFactorService()
        market_date = date(2026, 4, 23)
        low_risk_returns = ([0.002, -0.001, 0.0015, -0.0005] * 30)[:120]
        mid_risk_returns = ([0.01, -0.008, 0.012, -0.009] * 30)[:120]
        high_risk_returns = ([0.02, -0.018, 0.025, -0.02] * 30)[:120]
        mid_risk_returns[70] = -0.08
        high_risk_returns[70] = -0.25

        all_inputs = [
            LowRiskFactorInput(
                symbol='000001',
                name_ko='Low Risk',
                market='KOSPI',
                bars=_build_bars(
                    market_date,
                    closes=_build_close_series(100.0, low_risk_returns),
                    turnover=9_000_000,
                ),
            ),
            LowRiskFactorInput(
                symbol='000002',
                name_ko='Mid Risk',
                market='KOSPI',
                bars=_build_bars(
                    market_date,
                    closes=_build_close_series(100.0, mid_risk_returns),
                    turnover=4_000_000,
                ),
            ),
            LowRiskFactorInput(
                symbol='000003',
                name_ko='High Risk',
                market='KOSPI',
                bars=_build_bars(
                    market_date,
                    closes=_build_close_series(100.0, high_risk_returns),
                    turnover=1_000_000,
                ),
            ),
        ]

        def load_inputs_side_effect(*, market_date, symbols):
            if symbols is None:
                return (all_inputs, ['000001', '000002', '000003'], [])
            self.assertEqual(symbols, ['000002'])
            return ([all_inputs[1]], ['000002'], [])

        with (
            patch.object(service, '_load_inputs', side_effect=load_inputs_side_effect),
            patch('app.signals.low_risk_factor.upsert_daily_low_risk_factor_results') as upsert,
        ):
            result = service.rebuild_for_market_date(
                market_date=market_date,
                symbols=['000002'],
            )

        persisted = upsert.call_args.args[0][0]
        self.assertEqual(result['results_persisted'], 1)
        self.assertEqual(result['summary']['ranking_universe_counts']['volatility_60d'], 3)
        self.assertEqual(result['summary']['ranking_universe_counts']['combined_score'], 3)
        self.assertEqual(persisted['symbol'], '000002')
        self.assertEqual(persisted['volatility_60d_rank'], 2)
        self.assertEqual(persisted['volatility_60d_percentile'], 0.5)
        self.assertEqual(persisted['low_risk_classification'], 'watchlist')
        self.assertFalse(persisted['is_low_risk_eligible'])

    def test_inspect_returns_single_item_for_symbol(self) -> None:
        service = DailyLowRiskFactorService()
        stored_items = [
            {
                'symbol': '005930',
                'market_date': '2026-04-23',
                'low_risk_classification': 'eligible',
                'passes_liquidity_floor': True,
                'is_low_risk_eligible': True,
            }
        ]

        with patch(
            'app.signals.low_risk_factor.load_daily_low_risk_factor_results',
            return_value=stored_items,
        ) as loader:
            result = service.inspect(market_date='2026-04-23', symbol='5930', limit=50)

        loader.assert_called_once_with(
            market_date=date(2026, 4, 23),
            symbols=['005930'],
            limit=1,
        )
        self.assertEqual(result['summary']['requested_symbol'], '005930')
        self.assertEqual(result['item'], stored_items[0])
