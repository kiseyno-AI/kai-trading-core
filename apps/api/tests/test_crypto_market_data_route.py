import unittest

from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.api import routes_crypto
from app.crypto.market_data import CryptoMarketDataValidationError
from app.integrations.upbit.client import UpbitQuotationAPIError


class FakeCryptoMarketDataService:
    async def list_markets(self):
        return {
            'provider': 'upbit',
            'asset_class': 'crypto',
            'venue': 'UPBIT',
            'trading_enabled_in_core': False,
            'items': [{'market': 'KRW-BTC'}],
        }

    async def get_tickers(self, *, markets=None):
        return {
            'provider': 'upbit',
            'requested_markets': ['KRW-BTC'] if markets else ['KRW-BTC', 'KRW-ETH'],
            'trading_enabled_in_core': False,
            'items': [{'market': 'KRW-BTC', 'price': 145831000}],
        }

    async def get_candles(self, *, market, interval='day', unit=1, count=30, to=None):
        return {
            'provider': 'upbit',
            'market': market,
            'interval': interval,
            'unit': unit if interval == 'minute' else None,
            'trading_enabled_in_core': False,
            'items': [{'market': market, 'close': 145831000}],
        }


class InvalidCryptoMarketDataService(FakeCryptoMarketDataService):
    async def get_tickers(self, *, markets=None):
        raise CryptoMarketDataValidationError('Unsupported crypto market(s): KRW-XRP. Allowed: KRW-BTC, KRW-ETH')


class UpstreamErrorCryptoMarketDataService(FakeCryptoMarketDataService):
    async def list_markets(self):
        raise UpbitQuotationAPIError('Upbit quotation request failed [500]: upstream down')


class CryptoMarketDataRouteTests(unittest.TestCase):
    def setUp(self) -> None:
        self.app = FastAPI()
        self.app.include_router(routes_crypto.router)
        self.original_service = routes_crypto.crypto_market_data_service
        routes_crypto.crypto_market_data_service = FakeCryptoMarketDataService()

    def tearDown(self) -> None:
        routes_crypto.crypto_market_data_service = self.original_service

    def test_markets_route_returns_read_only_crypto_payload(self) -> None:
        client = TestClient(self.app)
        response = client.get('/market-data/crypto/markets')

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload['provider'], 'upbit')
        self.assertEqual(payload['asset_class'], 'crypto')
        self.assertFalse(payload['trading_enabled_in_core'])

    def test_tickers_route_passes_markets_query(self) -> None:
        client = TestClient(self.app)
        response = client.get('/market-data/crypto/tickers?markets=KRW-BTC')

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload['requested_markets'], ['KRW-BTC'])
        self.assertEqual(payload['items'][0]['price'], 145831000)

    def test_candles_route_passes_interval_scope(self) -> None:
        client = TestClient(self.app)
        response = client.get('/market-data/crypto/candles?market=KRW-BTC&interval=minute&unit=15&count=30')

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload['market'], 'KRW-BTC')
        self.assertEqual(payload['interval'], 'minute')
        self.assertEqual(payload['unit'], 15)
        self.assertFalse(payload['trading_enabled_in_core'])

    def test_validation_errors_map_to_400(self) -> None:
        routes_crypto.crypto_market_data_service = InvalidCryptoMarketDataService()

        client = TestClient(self.app)
        response = client.get('/market-data/crypto/tickers?markets=KRW-XRP')

        self.assertEqual(response.status_code, 400)
        self.assertIn('Unsupported crypto market', response.json()['detail'])

    def test_upbit_errors_map_to_502(self) -> None:
        routes_crypto.crypto_market_data_service = UpstreamErrorCryptoMarketDataService()

        client = TestClient(self.app)
        response = client.get('/market-data/crypto/markets')

        self.assertEqual(response.status_code, 502)
        self.assertIn('Upbit market data request failed', response.json()['detail'])


if __name__ == '__main__':
    unittest.main()
