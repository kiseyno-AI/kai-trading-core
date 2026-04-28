import unittest

from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.api import routes_trading_accounts
from app.orders.account_inquiry_service import (
    TradingAccountInquiryUpstreamError,
    TradingAccountInquiryValidationError,
)


class FakeTradingAccountInquiryService:
    def __init__(self) -> None:
        self.summary_calls: list[dict] = []
        self.positions_calls: list[dict] = []
        self.buying_power_calls: list[dict] = []
        self.summary_exception: Exception | None = None
        self.positions_exception: Exception | None = None
        self.buying_power_exception: Exception | None = None

    async def get_account_summary(self, **kwargs):
        self.summary_calls.append(kwargs)
        if self.summary_exception is not None:
            raise self.summary_exception
        return {
            'account_reference': kwargs['account_reference'],
            'cash': {'orderable_cash': 2500000},
            'valuation': {'total_asset_value': 3072000},
        }

    async def get_positions(self, **kwargs):
        self.positions_calls.append(kwargs)
        if self.positions_exception is not None:
            raise self.positions_exception
        return {
            'account_reference': kwargs['account_reference'],
            'count': 1,
            'items': [{'symbol': kwargs.get('symbol') or '005930', 'quantity': 10}],
        }

    async def get_buying_power(self, **kwargs):
        self.buying_power_calls.append(kwargs)
        if self.buying_power_exception is not None:
            raise self.buying_power_exception
        return {
            'account_reference': kwargs['account_reference'],
            'symbol': kwargs['symbol'],
            'buying_power': {'buyable_quantity': 17, 'buyable_amount': 969000},
        }


class TradingAccountRouteTests(unittest.TestCase):
    def setUp(self) -> None:
        self.app = FastAPI()
        self.app.include_router(routes_trading_accounts.router)
        self.service = FakeTradingAccountInquiryService()
        self.original_service = routes_trading_accounts.trading_account_inquiry_service
        routes_trading_accounts.trading_account_inquiry_service = self.service

    def tearDown(self) -> None:
        routes_trading_accounts.trading_account_inquiry_service = self.original_service

    def test_summary_positions_and_buying_power_routes_use_service(self) -> None:
        client = TestClient(self.app)

        summary_response = client.get('/trading/accounts/KIS:12345678-01/summary')
        positions_response = client.get(
            '/trading/accounts/KIS:12345678-01/positions',
            params={'symbol': '005930', 'include_zero': 'true'},
        )
        buying_power_response = client.get(
            '/trading/accounts/KIS:12345678-01/buying-power',
            params={'symbol': '005930', 'price': '57000', 'order_type': 'market', 'include_cma': 'true'},
        )

        self.assertEqual(summary_response.status_code, 200)
        self.assertEqual(summary_response.json()['valuation']['total_asset_value'], 3072000)
        self.assertEqual(positions_response.status_code, 200)
        self.assertEqual(positions_response.json()['items'][0]['symbol'], '005930')
        self.assertEqual(buying_power_response.status_code, 200)
        self.assertEqual(buying_power_response.json()['buying_power']['buyable_quantity'], 17)
        self.assertEqual(self.service.summary_calls[0]['account_reference'], 'KIS:12345678-01')
        self.assertEqual(self.service.positions_calls[0]['symbol'], '005930')
        self.assertTrue(self.service.positions_calls[0]['include_zero'])
        self.assertEqual(self.service.buying_power_calls[0]['order_type'], 'market')
        self.assertTrue(self.service.buying_power_calls[0]['include_cma'])

    def test_route_maps_validation_and_configuration_errors(self) -> None:
        client = TestClient(self.app)
        self.service.summary_exception = TradingAccountInquiryValidationError(
            'account_reference is required',
            payload={'detail': 'bad account reference'},
        )
        self.service.positions_exception = TradingAccountInquiryUpstreamError(
            'KIS not configured',
            payload={'detail': 'KIS not configured'},
        )

        summary_response = client.get('/trading/accounts/not-kis/summary')
        positions_response = client.get('/trading/accounts/KIS:12345678-01/positions')

        self.assertEqual(summary_response.status_code, 400)
        self.assertEqual(summary_response.json()['detail']['detail'], 'bad account reference')
        self.assertEqual(positions_response.status_code, 503)
        self.assertEqual(positions_response.json()['detail']['detail'], 'KIS not configured')


if __name__ == '__main__':
    unittest.main()
