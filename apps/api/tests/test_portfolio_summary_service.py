import unittest

from app.orders.account_inquiry_service import TradingAccountInquiryUpstreamError
from app.orders.portfolio_summary_service import PortfolioSummaryService


class FakePortfolioInquiryService:
    def __init__(self) -> None:
        self.summary_calls: list[dict] = []
        self.positions_calls: list[dict] = []
        self.summary_results: dict[str, dict] = {}
        self.positions_results: dict[str, dict] = {}
        self.summary_exceptions: dict[str, Exception] = {}
        self.positions_exceptions: dict[str, Exception] = {}

    async def get_account_summary(self, **kwargs):
        account_reference = kwargs['account_reference']
        self.summary_calls.append(kwargs)
        if account_reference in self.summary_exceptions:
            raise self.summary_exceptions[account_reference]
        return self.summary_results[account_reference]

    async def get_positions(self, **kwargs):
        account_reference = kwargs['account_reference']
        self.positions_calls.append(kwargs)
        if account_reference in self.positions_exceptions:
            raise self.positions_exceptions[account_reference]
        return self.positions_results[account_reference]


class FakePortfolioOrderService:
    def __init__(self) -> None:
        self.list_calls: list[dict] = []
        self.order_lists: dict[str, dict] = {}
        self.order_errors: dict[str, Exception] = {}

    def list_orders(self, **kwargs):
        account_reference = kwargs['account_reference']
        self.list_calls.append(kwargs)
        if account_reference in self.order_errors:
            raise self.order_errors[account_reference]
        return self.order_lists.get(account_reference, {'count': 0, 'items': []})


class PortfolioSummaryServiceTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.inquiry_service = FakePortfolioInquiryService()
        self.order_service = FakePortfolioOrderService()

    async def test_portfolio_summary_normalizes_synced_account_payload(self) -> None:
        account_reference = 'KIS:12345678-01'
        self.inquiry_service.summary_results[account_reference] = {
            'account_reference': account_reference,
            'as_of': '2026-04-29T01:23:45+00:00',
            'cash': {
                'settled_cash': 2500000,
                'withdrawable_cash': 2400000,
                'orderable_cash': 2500000,
            },
            'valuation': {
                'securities_value': 572000,
                'total_asset_value': 3072000,
                'pnl_amount': 22000,
                'pnl_pct': 0.04,
            },
        }
        self.inquiry_service.positions_results[account_reference] = {
            'account_reference': account_reference,
            'as_of': '2026-04-29T01:23:45+00:00',
            'items': [
                {
                    'symbol': '005930',
                    'name': 'Samsung Electronics',
                    'quantity': 10,
                    'average_price': 55000,
                    'last_price': 57200,
                    'market_value': 572000,
                    'pnl_amount': 22000,
                    'pnl_pct': 0.04,
                    'weight_pct': 0.1862,
                }
            ],
        }
        self.order_service.order_lists[account_reference] = {
            'count': 1,
            'items': [
                {
                    'id': 11,
                    'client_order_id': 'ORDER-1',
                    'symbol': '005930',
                    'side': 'buy',
                    'current_status': 'filled',
                    'requested_quantity': 5,
                    'filled_quantity': 5,
                    'average_fill_price': 57100,
                    'first_submitted_at': '2026-04-29T01:20:00+00:00',
                    'last_broker_update_at': '2026-04-29T01:21:00+00:00',
                }
            ],
        }

        service = PortfolioSummaryService(
            account_inquiry_service=self.inquiry_service,
            order_service=self.order_service,
            portfolio_account_references='Main=KIS:12345678-01',
        )

        result = await service.get_portfolio_summary()

        self.assertEqual(result['sync']['status'], 'synced')
        self.assertEqual(result['source'], 'kis_portfolio_summary_v1')
        self.assertEqual(len(result['accounts']), 1)
        account = result['accounts'][0]
        self.assertEqual(account['label'], 'Main')
        self.assertEqual(account['status'], 'synced')
        self.assertEqual(account['summary']['totalAssets']['amount'], 3072000)
        self.assertEqual(account['summary']['cash']['amount'], 2400000)
        self.assertEqual(account['summary']['buyingPower']['amount'], 2500000)
        self.assertEqual(account['summary']['pnl']['totalPercent'], 4.0)
        self.assertEqual(account['holdings'][0]['unrealizedPnlPercent'], 4.0)
        self.assertEqual(account['holdings'][0]['allocationPercent'], 18.62)
        self.assertEqual(account['allocation'][0]['percent'], 18.62)
        self.assertEqual(account['recentActivity'][0]['id'], 'ORDER-1')
        self.assertEqual(account['recentActivity'][0]['status'], 'filled')
        self.assertEqual(result['notices'], [])

    async def test_missing_portfolio_account_configuration_returns_not_configured_state(self) -> None:
        service = PortfolioSummaryService(
            account_inquiry_service=self.inquiry_service,
            order_service=self.order_service,
            portfolio_account_references='',
        )

        result = await service.get_portfolio_summary()

        self.assertEqual(result['sync']['status'], 'not_configured')
        self.assertEqual(result['accounts'], [])
        self.assertIn('PORTFOLIO_ACCOUNT_REFERENCES', result['notices'][0])

    async def test_partial_account_failures_mark_summary_stale_without_collapsing_to_not_configured(self) -> None:
        main_account = 'KIS:12345678-01'
        isa_account = 'KIS:87654321-22'
        self.inquiry_service.summary_results[main_account] = {
            'account_reference': main_account,
            'as_of': '2026-04-29T01:23:45+00:00',
            'cash': {'settled_cash': 2500000, 'withdrawable_cash': 2400000, 'orderable_cash': 2500000},
            'valuation': {'securities_value': 572000, 'total_asset_value': 3072000, 'pnl_amount': 22000, 'pnl_pct': 0.04},
        }
        self.inquiry_service.positions_results[main_account] = {
            'account_reference': main_account,
            'as_of': '2026-04-29T01:23:45+00:00',
            'items': [{'symbol': '005930', 'quantity': 10, 'market_value': 572000, 'pnl_pct': 0.04, 'weight_pct': 0.1862}],
        }
        configuration_error = TradingAccountInquiryUpstreamError(
            'KIS credentials must be configured',
            payload={'detail': 'KIS credentials must be configured', 'configured': False, 'retryable': False},
        )
        self.inquiry_service.summary_exceptions[isa_account] = configuration_error
        self.inquiry_service.positions_exceptions[isa_account] = configuration_error

        service = PortfolioSummaryService(
            account_inquiry_service=self.inquiry_service,
            order_service=self.order_service,
            portfolio_account_references='Main=KIS:12345678-01,ISA=KIS:87654321-22',
        )

        result = await service.get_portfolio_summary()

        self.assertEqual(result['sync']['status'], 'stale')
        self.assertEqual(len(result['accounts']), 2)
        self.assertEqual(result['accounts'][0]['status'], 'synced')
        self.assertEqual(result['accounts'][1]['status'], 'unavailable')
        self.assertTrue(any('ISA: account summary unavailable' in notice for notice in result['notices']))

    async def test_all_account_configuration_failures_return_not_configured_sync_state(self) -> None:
        account_reference = 'KIS:12345678-01'
        configuration_error = TradingAccountInquiryUpstreamError(
            'KIS credentials must be configured',
            payload={'detail': 'KIS credentials must be configured', 'configured': False, 'retryable': False},
        )
        self.inquiry_service.summary_exceptions[account_reference] = configuration_error
        self.inquiry_service.positions_exceptions[account_reference] = configuration_error

        service = PortfolioSummaryService(
            account_inquiry_service=self.inquiry_service,
            order_service=self.order_service,
            portfolio_account_references='Main=KIS:12345678-01',
        )

        result = await service.get_portfolio_summary()

        self.assertEqual(result['sync']['status'], 'not_configured')
        self.assertEqual(result['accounts'][0]['status'], 'unavailable')
        self.assertIn('not configured', result['sync']['message'].lower())


if __name__ == '__main__':
    unittest.main()
