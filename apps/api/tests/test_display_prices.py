import unittest
from datetime import date
from unittest.mock import patch

from app.quotes.cache import quote_cache
from app.quotes.display_prices import build_display_price_fields


class DisplayPriceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.original_items = dict(quote_cache.items)
        self.original_updated_at = dict(quote_cache.updated_at)
        quote_cache.items.clear()
        quote_cache.updated_at.clear()

    def tearDown(self) -> None:
        quote_cache.items = self.original_items
        quote_cache.updated_at = self.original_updated_at

    def _unavailable_realtime_fields(self) -> dict:
        return {
            'current_price': None,
            'price_as_of': None,
            'price_received_at': None,
            'price_source': None,
            'price_status': 'unavailable',
            'realtime_subscribed': False,
            'realtime_subscription_reason': 'not_monitored',
            'display_price_available': False,
        }

    def test_build_display_price_uses_same_day_current_quote(self) -> None:
        with (
            patch(
                'app.quotes.display_prices.load_current_quote',
                return_value={
                    'symbol': '000660',
                    'price': 182500,
                    'cached_at': '2026-04-30T00:06:00+00:00',
                    'as_of': '2026-04-30T00:06:00+00:00',
                    'trade_date': '2026-04-30',
                },
            ),
            patch('app.quotes.display_prices.load_daily_bars_for_symbol') as load_daily_bars,
        ):
            result = build_display_price_fields(
                '000660',
                market_date=date(2026, 4, 30),
                realtime_fields=self._unavailable_realtime_fields(),
            )

        self.assertEqual(result['current_price'], 182500)
        self.assertEqual(result['price_source'], 'cached_quote')
        self.assertEqual(result['price_status'], 'fallback')
        self.assertTrue(result['display_price_available'])
        self.assertEqual(result['display_price_fallback_reason'], 'current_quote_cache')
        load_daily_bars.assert_not_called()

    def test_build_display_price_uses_previous_current_quote_as_stale(self) -> None:
        with (
            patch(
                'app.quotes.display_prices.load_current_quote',
                return_value={
                    'symbol': '000660',
                    'price': 180000,
                    'cached_at': '2026-04-29T06:10:00+00:00',
                    'trade_date': '2026-04-29',
                },
            ),
            patch('app.quotes.display_prices.load_daily_bars_for_symbol') as load_daily_bars,
        ):
            result = build_display_price_fields(
                '000660',
                market_date=date(2026, 4, 30),
                realtime_fields=self._unavailable_realtime_fields(),
            )

        self.assertEqual(result['current_price'], 180000)
        self.assertEqual(result['price_source'], 'cached_quote')
        self.assertEqual(result['price_status'], 'stale')
        self.assertTrue(result['display_price_available'])
        load_daily_bars.assert_not_called()

    def test_build_display_price_rejects_future_current_quote_for_historical_market_date(self) -> None:
        with (
            patch(
                'app.quotes.display_prices.load_current_quote',
                return_value={
                    'symbol': '000660',
                    'price': 182500,
                    'cached_at': '2026-04-30T00:06:00+00:00',
                    'as_of': '2026-04-30T00:06:00+00:00',
                    'trade_date': '2026-04-30',
                },
            ),
            patch('app.quotes.display_prices.load_daily_bars_for_symbol', return_value=[]) as load_daily_bars,
        ):
            result = build_display_price_fields(
                '000660',
                market_date=date(2026, 4, 29),
                realtime_fields=self._unavailable_realtime_fields(),
            )

        self.assertIsNone(result['current_price'])
        self.assertIsNone(result['price_source'])
        self.assertEqual(result['price_status'], 'unavailable')
        self.assertFalse(result['display_price_available'])
        load_daily_bars.assert_called_once_with('000660', end_date=date(2026, 4, 29), limit=2)


if __name__ == '__main__':
    unittest.main()
