import unittest
from datetime import date, timedelta
from decimal import Decimal

from app.us_equities.screening import (
    USFundamentalScreenMetrics,
    USPriceScreenMetrics,
    USScreeningPolicy,
    screen_us_fundamental_candidates,
    screen_us_price_candidates,
)


def _price(symbol: str, **overrides) -> USPriceScreenMetrics:
    values = {
        'symbol': symbol,
        'latest_price': Decimal('50'),
        'history_days': 300,
        'median_dollar_volume_20d': Decimal('20000000'),
        'median_volume_20d': Decimal('500000'),
        'volatility_60d': Decimal('0.35'),
        'max_drawdown_120d': Decimal('-0.20'),
        'momentum_6m': Decimal('0.15'),
        'momentum_12m': Decimal('0.25'),
        'return_5d': Decimal('0.02'),
    }
    values.update(overrides)
    return USPriceScreenMetrics(**values)


def _fundamental(symbol: str, market_date: date, **overrides) -> USFundamentalScreenMetrics:
    values = {
        'symbol': symbol,
        'latest_periodic_filing_date': market_date - timedelta(days=30),
        'ttm_revenue': Decimal('1000'),
        'ttm_operating_cash_flow': Decimal('150'),
        'ttm_net_income': Decimal('100'),
        'total_assets': Decimal('2000'),
        'total_liabilities': Decimal('600'),
        'shares_outstanding_growth_yoy': Decimal('0.02'),
        'has_bankruptcy_or_delinquency_flag': False,
    }
    values.update(overrides)
    return USFundamentalScreenMetrics(**values)


class USPriceScreeningTests(unittest.TestCase):
    def test_applies_boundary_filters_and_reports_all_reasons(self):
        metrics = [
            _price('GOOD'),
            _price(
                'BAD',
                latest_price=Decimal('4.99'),
                history_days=251,
                median_dollar_volume_20d=Decimal('9999999'),
                median_volume_20d=Decimal('199999'),
                return_5d=Decimal('0.30'),
            ),
        ]

        result = screen_us_price_candidates(metrics)

        self.assertEqual([item.symbol for item in result.selected], ['GOOD'])
        self.assertEqual(
            result.exclusions['BAD'],
            ('price_below_floor', 'insufficient_history', 'dollar_volume_below_floor', 'volume_below_floor', 'recent_overheat'),
        )

    def test_caps_large_universe_deterministically(self):
        policy = USScreeningPolicy(price_candidate_cap=300)
        metrics = [
            _price(
                f'S{index:05d}',
                median_dollar_volume_20d=Decimal(10_000_000 + index),
                momentum_12m=Decimal(index) / Decimal('10000'),
            )
            for index in range(10_000)
        ]

        first = screen_us_price_candidates(reversed(metrics), policy=policy)
        second = screen_us_price_candidates(metrics, policy=policy)

        self.assertEqual(len(first.selected), 300)
        self.assertEqual(first.selected, second.selected)
        self.assertEqual(first.selected[0].symbol, 'S09999')


class USFundamentalScreeningTests(unittest.TestCase):
    def test_filters_stale_weak_levered_dilutive_and_flagged_companies(self):
        market_date = date(2026, 8, 19)
        metrics = [
            _fundamental('GOOD', market_date),
            _fundamental(
                'BAD',
                market_date,
                latest_periodic_filing_date=market_date - timedelta(days=200),
                ttm_operating_cash_flow=Decimal('-1'),
                total_liabilities=Decimal('1900'),
                shares_outstanding_growth_yoy=Decimal('0.20'),
                has_bankruptcy_or_delinquency_flag=True,
            ),
        ]

        result = screen_us_fundamental_candidates(metrics, market_date=market_date)

        self.assertEqual([item.symbol for item in result.selected], ['GOOD'])
        self.assertEqual(
            result.exclusions['BAD'],
            ('stale_periodic_filing', 'non_positive_operating_cash_flow', 'leverage_above_ceiling', 'dilution_above_ceiling', 'bankruptcy_or_delinquency'),
        )

    def test_caps_agent_input_and_keeps_score_order_deterministic(self):
        market_date = date(2026, 8, 19)
        policy = USScreeningPolicy(fundamental_candidate_cap=150)
        metrics = [
            _fundamental(
                f'F{index:04d}',
                market_date,
                ttm_operating_cash_flow=Decimal(100 + index),
                ttm_net_income=Decimal(50 + index),
                total_liabilities=Decimal(1000 - index),
            )
            for index in range(500)
        ]

        result = screen_us_fundamental_candidates(metrics, market_date=market_date, policy=policy)

        self.assertEqual(len(result.selected), 150)
        self.assertEqual(result.selected[0].symbol, 'F0499')


if __name__ == '__main__':
    unittest.main()
