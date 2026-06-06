import unittest
from unittest.mock import AsyncMock, patch

from app.market.clock import MarketState
from app.quotes.cache import quote_cache
from app.quotes.service import QuoteService


class QuoteServiceTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.original_items = dict(quote_cache.items)
        self.original_updated_at = dict(quote_cache.updated_at)
        quote_cache.items.clear()
        quote_cache.updated_at.clear()

    def tearDown(self) -> None:
        quote_cache.items = self.original_items
        quote_cache.updated_at = self.original_updated_at

    async def test_get_quote_uses_same_day_daily_price_bar_when_market_closed(self) -> None:
        service = QuoteService()
        market_state = MarketState(
            market='KRX',
            state='CLOSED',
            is_trading_day=True,
            is_open=False,
            server_time='2026-04-08T18:00:00+09:00',
            session_open_at='2026-04-08T09:00:00+09:00',
            session_close_at='2026-04-08T15:30:00+09:00',
        )
        quote_cache.items['005930'] = {
            'symbol': '005930',
            'company_name': 'Samsung Electronics',
            'price': 61000,
            'change': -500,
            'change_pct': -0.81,
            'volume': 10,
            'turnover': 100,
            'as_of': '153000',
        }
        quote_cache.updated_at['005930'] = '2026-04-03T15:31:00+09:00'
        resolve_company_name = AsyncMock(return_value='Samsung Electronics')

        with (
            patch('app.quotes.service.async_get_market_state', return_value=market_state),
            patch.object(service, '_resolve_company_name', new=resolve_company_name),
            patch('app.quotes.service.load_daily_bars_for_symbol', return_value=[
                {
                    'trade_date': '2026-04-07',
                    'open_price': 60000,
                    'high_price': 61200,
                    'low_price': 59800,
                    'close_price': 60500,
                    'volume': 1000,
                    'turnover': 100000,
                },
                {
                    'trade_date': '2026-04-08',
                    'open_price': 60500,
                    'high_price': 62000,
                    'low_price': 60400,
                    'close_price': 61500,
                    'volume': 2000,
                    'turnover': 200000,
                },
            ]),
        ):
            result = await service.get_quote('005930')

        self.assertEqual(result['source'], 'daily_price_bar')
        self.assertEqual(result['trade_date'], '2026-04-08')
        self.assertEqual(result['price'], 61500)
        self.assertEqual(result['change'], 1000)
        self.assertAlmostEqual(result['change_pct'], (1000 / 60500) * 100)
        self.assertTrue(result['fallback'])
        self.assertTrue(result['available'])
        self.assertEqual(result['fallback_reason'], 'daily_price_bar_current_trade_date')
        resolve_company_name.assert_not_called()

    async def test_get_quote_marks_unavailable_when_only_stale_cache_exists(self) -> None:
        service = QuoteService()
        market_state = MarketState(
            market='KRX',
            state='CLOSED',
            is_trading_day=True,
            is_open=False,
            server_time='2026-04-08T18:00:00+09:00',
            session_open_at='2026-04-08T09:00:00+09:00',
            session_close_at='2026-04-08T15:30:00+09:00',
        )
        quote_cache.items['005930'] = {
            'symbol': '005930',
            'company_name': 'Samsung Electronics',
            'price': 61000,
            'change': -500,
            'change_pct': -0.81,
            'volume': 10,
            'turnover': 100,
            'as_of': '153000',
        }
        quote_cache.updated_at['005930'] = '2026-04-03T15:31:00+09:00'
        resolve_company_name = AsyncMock(return_value='Samsung Electronics')

        with (
            patch('app.quotes.service.async_get_market_state', return_value=market_state),
            patch.object(service, '_resolve_company_name', new=resolve_company_name),
            patch('app.quotes.service.load_daily_bars_for_symbol', return_value=[]),
            patch('app.quotes.service.load_current_quote', return_value={
                'symbol': '005930',
                'company_name': 'Samsung Electronics',
                'price': 61000,
                'change': -500,
                'change_pct': -0.81,
                'volume': 10,
                'turnover': 100,
                'as_of': '153000',
                'cached_at': '2026-04-03T15:31:00+09:00',
            }),
        ):
            result = await service.get_quote('005930')

        self.assertEqual(result['source'], 'unavailable')
        self.assertEqual(result['price'], None)
        self.assertFalse(result['available'])
        self.assertEqual(result['unavailable_reason'], 'no_live_quote_or_fresh_fallback')
        resolve_company_name.assert_not_called()

    async def test_get_quote_uses_recent_previous_daily_bar_for_preopen(self) -> None:
        service = QuoteService()
        market_state = MarketState(
            market='KRX',
            state='PREOPEN',
            is_trading_day=True,
            is_open=False,
            server_time='2026-04-13T08:40:00+09:00',
            session_open_at='2026-04-13T09:00:00+09:00',
            session_close_at='2026-04-13T15:30:00+09:00',
        )

        resolve_company_name = AsyncMock(return_value='Samsung Electronics')

        with (
            patch('app.quotes.service.async_get_market_state', return_value=market_state),
            patch.object(service, '_resolve_company_name', new=resolve_company_name),
            patch('app.quotes.service.load_daily_bars_for_symbol', return_value=[
                {
                    'trade_date': '2026-04-10',
                    'open_price': 60500,
                    'high_price': 62000,
                    'low_price': 60400,
                    'close_price': 61500,
                    'volume': 2000,
                    'turnover': 200000,
                },
            ]),
        ):
            result = await service.get_quote('005930')

        self.assertEqual(result['source'], 'daily_price_bar')
        self.assertEqual(result['trade_date'], '2026-04-10')
        self.assertEqual(result['fallback_reason'], 'daily_price_bar_recent_previous_trade_date')
        self.assertTrue(result['available'])
        self.assertEqual(result['change'], None)
        self.assertEqual(result['change_pct'], None)
        resolve_company_name.assert_not_called()

    async def test_get_quote_uses_recent_previous_daily_bar_for_holiday(self) -> None:
        service = QuoteService()
        market_state = MarketState(
            market='KRX',
            state='HOLIDAY',
            is_trading_day=False,
            is_open=False,
            server_time='2026-05-05T12:00:00+09:00',
            session_open_at=None,
            session_close_at=None,
            reason='holiday',
            current_calendar_date='2026-05-05',
            latest_valid_trading_date='2026-05-04',
            market_session='holiday',
        )

        resolve_company_name = AsyncMock(return_value='Samsung Electronics')

        with (
            patch('app.quotes.service.async_get_market_state', return_value=market_state),
            patch.object(service, '_resolve_company_name', new=resolve_company_name),
            patch('app.quotes.service.load_daily_bars_for_symbol', return_value=[
                {
                    'trade_date': '2026-05-04',
                    'open_price': 60500,
                    'high_price': 62000,
                    'low_price': 60400,
                    'close_price': 61500,
                    'volume': 2000,
                    'turnover': 200000,
                },
            ]),
        ):
            result = await service.get_quote('005930')

        self.assertEqual(result['source'], 'daily_price_bar')
        self.assertEqual(result['trade_date'], '2026-05-04')
        self.assertEqual(result['fallback_reason'], 'daily_price_bar_recent_previous_trade_date')
        self.assertTrue(result['available'])
        self.assertEqual(result['change'], None)
        self.assertEqual(result['change_pct'], None)
        resolve_company_name.assert_not_called()

    async def test_get_quote_resolves_company_name_for_selected_live_quote(self) -> None:
        service = QuoteService()
        market_state = MarketState(
            market='KRX',
            state='OPEN',
            is_trading_day=True,
            is_open=True,
            server_time='2026-04-08T10:15:00+09:00',
            session_open_at='2026-04-08T09:00:00+09:00',
            session_close_at='2026-04-08T15:30:00+09:00',
        )
        resolve_company_name = AsyncMock(return_value='Samsung Electronics')

        with (
            patch('app.quotes.service.async_get_market_state', return_value=market_state),
            patch.object(service, '_resolve_company_name', new=resolve_company_name),
            patch.object(
                service.client,
                'fetch_domestic_quote',
                new=AsyncMock(
                    return_value={
                        'output': {
                            'stck_prpr': '61500',
                            'bidp': '61400',
                            'askp': '61500',
                        }
                    }
                ),
            ),
            patch('app.quotes.service.persist_quote_snapshot'),
        ):
            result = await service.get_quote('005930')

        self.assertEqual(result['source'], 'kis')
        self.assertEqual(result['company_name'], 'Samsung Electronics')
        self.assertEqual(result['bid_price'], 61400)
        self.assertEqual(result['ask_price'], 61500)
        resolve_company_name.assert_awaited_once_with('005930')


if __name__ == '__main__':
    unittest.main()
