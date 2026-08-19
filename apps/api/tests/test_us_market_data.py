import unittest
from datetime import date, timedelta
from decimal import Decimal

from app.us_equities.market_data import (
    USPriceBar,
    compute_us_price_screen_metrics,
    parse_kis_daily_price_payload,
)


class USMarketDataTests(unittest.TestCase):
    def test_parses_kis_daily_price_rows_as_decimal_without_losing_precision(self):
        payload = {
            'output2': [
                {
                    'xymd': '20260818', 'open': '220.1250', 'high': '225.5000', 'low': '219.8750',
                    'clos': '224.3750', 'tvol': '1234567', 'tamt': '276900000.25',
                },
                {'xymd': '', 'clos': ''},
            ]
        }

        bars = parse_kis_daily_price_payload(payload, symbol='AAPL')

        self.assertEqual(len(bars), 1)
        self.assertEqual(bars[0].trade_date, date(2026, 8, 18))
        self.assertEqual(bars[0].close_price, Decimal('224.3750'))
        self.assertEqual(bars[0].turnover, Decimal('276900000.25'))

    def test_computes_screening_metrics_from_adjusted_history(self):
        start = date(2025, 1, 1)
        bars = []
        for index in range(300):
            close = Decimal('10') + Decimal(index) / Decimal('10')
            bars.append(
                USPriceBar(
                    symbol='TEST',
                    trade_date=start + timedelta(days=index),
                    open_price=close - Decimal('0.1'),
                    high_price=close + Decimal('0.2'),
                    low_price=close - Decimal('0.2'),
                    close_price=close,
                    volume=500_000 + index,
                    turnover=close * Decimal(500_000 + index),
                )
            )

        metrics = compute_us_price_screen_metrics('TEST', bars)

        self.assertEqual(metrics.history_days, 300)
        self.assertEqual(metrics.latest_price, bars[-1].close_price)
        self.assertIsNotNone(metrics.volatility_60d)
        self.assertGreater(metrics.momentum_6m, Decimal('0'))
        self.assertGreater(metrics.momentum_12m, Decimal('0'))
        self.assertLessEqual(metrics.max_drawdown_120d, Decimal('0'))

    def test_rejects_duplicate_dates_deterministically(self):
        payload = {'output2': [
            {'xymd': '20260818', 'open': '10', 'high': '11', 'low': '9', 'clos': '10', 'tvol': '100', 'tamt': '1000'},
            {'xymd': '20260818', 'open': '20', 'high': '21', 'low': '19', 'clos': '20', 'tvol': '200', 'tamt': '4000'},
        ]}

        bars = parse_kis_daily_price_payload(payload, symbol='TEST')

        self.assertEqual(len(bars), 1)
        self.assertEqual(bars[0].close_price, Decimal('10'))


if __name__ == '__main__':
    unittest.main()
