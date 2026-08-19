import unittest
from datetime import date, timedelta
from unittest.mock import patch

from app.us_equities.price_collection_service import USPriceCollectionService, build_ranked_prefilter


def _symbol(symbol: str, symbol_id: int, market: str = 'NASDAQ') -> dict:
    return {
        'id': symbol_id,
        'symbol': symbol,
        'market': market,
        'exchange_code': market,
        'source_payload': {'kis_symbol': symbol},
    }


class FakeKISClient:
    def __init__(self) -> None:
        self.daily_calls = []

    async def fetch_overseas_market_cap_ranking(self, *, exchange, max_pages):
        return {'output2': [{'symb': 'AAPL'}, {'symb': 'MSFT'}] if exchange == 'NASDAQ' else []}

    async def fetch_overseas_trading_value_ranking(self, *, exchange, minimum_price, max_pages):
        return {'output2': [{'symb': 'MSFT'}, {'symb': 'AAPL'}] if exchange == 'NASDAQ' else []}

    async def fetch_overseas_daily_prices(self, *, exchange, symbol, as_of_date, adjusted):
        self.daily_calls.append((exchange, symbol, as_of_date, adjusted))
        start = date(2025, 8, 1)
        rows = []
        for index in range(260):
            close = 100 + index / 100
            rows.append({
                'xymd': (start + timedelta(days=index)).strftime('%Y%m%d'),
                'open': str(close - 0.1),
                'high': str(close + 0.2),
                'low': str(close - 0.2),
                'clos': str(close),
                'tvol': '500000',
                'tamt': '50000000',
            })
        return {'output2': rows}


class USPriceCollectionServiceTests(unittest.IsolatedAsyncioTestCase):
    def test_prefilter_combines_rankings_and_deduplicates(self):
        symbols = [_symbol('AAPL', 1), _symbol('MSFT', 2), _symbol('NVDA', 3)]
        payloads = [
            {'output2': [{'symb': 'AAPL'}, {'symb': 'MSFT'}]},
            {'output2': [{'symb': 'MSFT'}, {'symb': 'AAPL'}, {'symb': 'UNKNOWN'}]},
        ]

        result = build_ranked_prefilter(symbols, payloads, cap=2)

        self.assertEqual([row['symbol'] for row in result], ['AAPL', 'MSFT'])

    async def test_collects_only_ranked_symbols_and_applies_price_screen(self):
        client = FakeKISClient()
        service = USPriceCollectionService(
            kis_client=client,
            symbol_loader=lambda: [_symbol('AAPL', 1), _symbol('MSFT', 2), _symbol('NVDA', 3)],
        )
        with (
            patch('app.us_equities.price_collection_service.settings.us_equity_history_max_pages', 1),
            patch('app.us_equities.price_collection_service.settings.us_equity_history_request_pause_seconds', 0),
            patch('app.us_equities.price_collection_service.settings.us_equity_price_prefilter_cap', 2),
        ):
            result = await service.collect_and_screen(market_date=date(2026, 8, 19), persist=False)

        self.assertEqual(result['counts']['universe'], 3)
        self.assertEqual(result['counts']['ranking_prefilter'], 2)
        self.assertEqual(result['counts']['histories_collected'], 2)
        self.assertEqual(result['counts']['price_selected'], 2)
        self.assertEqual(set(result['selected_symbols']), {'AAPL', 'MSFT'})
        self.assertEqual({call[1] for call in client.daily_calls}, {'AAPL', 'MSFT'})


if __name__ == '__main__':
    unittest.main()
