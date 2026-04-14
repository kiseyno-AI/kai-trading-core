import unittest

from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.api import routes_favorite_symbols


class FavoriteSymbolsRouteTests(unittest.TestCase):
    def setUp(self):
        self.app = FastAPI()
        self.app.include_router(routes_favorite_symbols.router)

    def test_get_favorite_symbols_returns_store_payload(self):
        original_loader = routes_favorite_symbols.load_favorite_symbols
        routes_favorite_symbols.load_favorite_symbols = lambda: [
            {
                'symbol': '005930',
                'standard_code': 'KR7005930003',
                'name_ko': 'Samsung Electronics',
                'market': 'KOSPI',
                'instrument_type': 'equity',
                'is_active': True,
                'sort_order': 0,
                'favorited_at': '2026-04-14T13:35:00+09:00',
            }
        ]
        try:
            client = TestClient(self.app)
            response = client.get('/symbols/favorites')
        finally:
            routes_favorite_symbols.load_favorite_symbols = original_loader

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload['count'], 1)
        self.assertEqual(payload['items'][0]['symbol'], '005930')

    def test_delete_favorite_symbol_normalizes_symbol_and_returns_result(self):
        calls = []
        original_remover = routes_favorite_symbols.remove_favorite_symbol

        def fake_remove(symbol: str) -> bool:
            calls.append(symbol)
            return True

        routes_favorite_symbols.remove_favorite_symbol = fake_remove
        try:
            client = TestClient(self.app)
            response = client.delete('/symbols/favorites/abc123')
        finally:
            routes_favorite_symbols.remove_favorite_symbol = original_remover

        self.assertEqual(response.status_code, 200)
        self.assertEqual(calls, ['ABC123'])
        self.assertEqual(response.json(), {'symbol': 'ABC123', 'removed': True})

    def test_delete_favorite_symbol_is_idempotent_for_missing_symbol(self):
        original_remover = routes_favorite_symbols.remove_favorite_symbol
        routes_favorite_symbols.remove_favorite_symbol = lambda symbol: False
        try:
            client = TestClient(self.app)
            response = client.delete('/symbols/favorites/005930')
        finally:
            routes_favorite_symbols.remove_favorite_symbol = original_remover

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json(), {'symbol': '005930', 'removed': False})


if __name__ == '__main__':
    unittest.main()
