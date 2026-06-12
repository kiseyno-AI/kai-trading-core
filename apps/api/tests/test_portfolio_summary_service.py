import unittest
from datetime import timedelta

from app.core.settings import settings
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
        self.original_settings = {
            'app_env': settings.app_env,
            'kis_app_key': settings.kis_app_key,
            'kis_app_secret': settings.kis_app_secret,
            'kis_use_mock': settings.kis_use_mock,
            'portfolio_summary_cache_ttl_seconds': settings.portfolio_summary_cache_ttl_seconds,
        }
        settings.app_env = 'development'
        settings.kis_app_key = 'test-kis-key'
        settings.kis_app_secret = 'test-kis-secret'
        settings.kis_use_mock = True
        settings.portfolio_summary_cache_ttl_seconds = 60

    def tearDown(self) -> None:
        for field_name, value in self.original_settings.items():
            setattr(settings, field_name, value)

    def _build_service(
        self,
        *,
        portfolio_account_references: str = 'Main=KIS:12345678-01',
        cache_ttl_seconds: int = 60,
    ) -> PortfolioSummaryService:
        return PortfolioSummaryService(
            account_inquiry_service=self.inquiry_service,
            order_service=self.order_service,
            portfolio_account_references=portfolio_account_references,
            cache_ttl_seconds=cache_ttl_seconds,
        )

    def _seed_synced_account(
        self,
        *,
        account_reference: str = 'KIS:12345678-01',
        as_of: str = '2026-04-29T01:23:45+00:00',
        total_asset_value: int = 3072000,
        market_value: int = 572000,
        pnl_amount: int = 22000,
        pnl_pct: float = 0.04,
        day_pnl_amount: int | None = None,
        day_pnl_pct: float | None = None,
        holdings: list[dict] | None = None,
        recent_order_side: str = 'buy',
    ) -> None:
        valuation = {
            'securities_value': market_value,
            'total_asset_value': total_asset_value,
            'pnl_amount': pnl_amount,
            'pnl_pct': pnl_pct,
        }
        if day_pnl_amount is not None:
            valuation['day_pnl_amount'] = day_pnl_amount
        if day_pnl_pct is not None:
            valuation['day_pnl_pct'] = day_pnl_pct

        if holdings is None:
            holdings = [
                {
                    'symbol': '005930',
                    'name': 'Samsung Electronics',
                    'quantity': 10,
                    'average_price': 55000,
                    'last_price': 57200,
                    'market_value': market_value,
                    'pnl_amount': pnl_amount,
                    'pnl_pct': pnl_pct,
                    'weight_pct': 0.1862,
                }
            ]

        self.inquiry_service.summary_results[account_reference] = {
            'account_reference': account_reference,
            'as_of': as_of,
            'cash': {
                'settled_cash': 2500000,
                'withdrawable_cash': 2400000,
                'orderable_cash': 2500000,
            },
            'valuation': valuation,
        }
        self.inquiry_service.positions_results[account_reference] = {
            'account_reference': account_reference,
            'as_of': as_of,
            'items': holdings,
        }
        self.order_service.order_lists[account_reference] = {
            'count': 1,
            'items': [
                {
                    'id': 11,
                    'client_order_id': 'ORDER-1',
                    'symbol': '005930',
                    'side': recent_order_side,
                    'current_status': 'filled',
                    'requested_quantity': 5,
                    'filled_quantity': 5,
                    'average_fill_price': 57100,
                    'first_submitted_at': '2026-04-29T01:20:00+00:00',
                    'last_broker_update_at': '2026-04-29T01:21:00+00:00',
                }
            ],
        }

    async def test_portfolio_summary_normalizes_synced_account_payload_and_freshness(self) -> None:
        self._seed_synced_account()
        service = self._build_service()

        result = await service.get_portfolio_summary()

        self.assertEqual(result['sync']['status'], 'synced')
        self.assertEqual(result['source'], 'kis_portfolio_summary_v1')
        self.assertEqual(result['freshness']['servedFromCache'], False)
        self.assertEqual(result['freshness']['lastRefreshStatus'], 'success')
        self.assertFalse(result['freshness']['isStale'])
        self.assertEqual(len(result['accounts']), 1)
        account = result['accounts'][0]
        self.assertEqual(account['label'], 'Main')
        self.assertEqual(account['status'], 'synced')
        self.assertEqual(account['summary']['totalAssets']['amount'], 3072000)
        self.assertEqual(account['summary']['cash']['amount'], 2400000)
        self.assertEqual(account['summary']['buyingPower']['amount'], 2500000)
        self.assertEqual(account['summary']['pnl']['day']['amount'], 22000)
        self.assertEqual(account['summary']['pnl']['total']['amount'], 22000)
        self.assertEqual(account['summary']['pnl']['totalPercent'], 4.0)
        self.assertEqual(account['holdings'][0]['unrealizedPnlPercent'], 4.0)
        self.assertEqual(account['holdings'][0]['allocationPercent'], 18.62)
        self.assertEqual(account['allocation'][0]['percent'], 18.62)
        self.assertEqual(account['recentActivity'][0]['id'], 'ORDER-1')
        self.assertEqual(account['recentActivity'][0]['status'], 'filled')
        self.assertEqual(result['notices'], [])

    async def test_portfolio_summary_uses_kis_day_pnl_and_keeps_unrealized_separate(self) -> None:
        self._seed_synced_account(
            pnl_amount=-5000,
            pnl_pct=-0.01,
            day_pnl_amount=20366,
            day_pnl_pct=0.0066,
            recent_order_side='sell',
        )
        service = self._build_service()

        result = await service.get_portfolio_summary()

        account = result['accounts'][0]
        self.assertEqual(account['summary']['pnl']['unrealized']['amount'], -5000)
        self.assertEqual(account['summary']['pnl']['day']['amount'], 20366)
        self.assertEqual(account['summary']['pnl']['total']['amount'], 20366)
        self.assertEqual(account['summary']['pnl']['totalPercent'], 0.66)
        self.assertEqual(account['holdings'][0]['unrealizedPnl']['amount'], -5000)
        self.assertEqual(account['recentActivity'][0]['side'], 'sell')

    async def test_sold_position_missing_from_holdings_does_not_drive_day_pnl_negative(self) -> None:
        self._seed_synced_account(
            market_value=0,
            pnl_amount=-12000,
            pnl_pct=-0.03,
            day_pnl_amount=20366,
            day_pnl_pct=0.0066,
            holdings=[],
            recent_order_side='sell',
        )
        service = self._build_service()

        result = await service.get_portfolio_summary()

        account = result['accounts'][0]
        self.assertEqual(account['holdings'], [])
        self.assertEqual(account['allocation'], [])
        self.assertEqual(account['summary']['pnl']['unrealized']['amount'], -12000)
        self.assertEqual(account['summary']['pnl']['day']['amount'], 20366)
        self.assertEqual(account['summary']['pnl']['total']['amount'], 20366)

    async def test_portfolio_summary_falls_back_to_unrealized_pnl_when_day_pnl_absent(self) -> None:
        self._seed_synced_account(pnl_amount=22000, pnl_pct=0.04)
        service = self._build_service()

        result = await service.get_portfolio_summary()

        pnl = result['accounts'][0]['summary']['pnl']
        self.assertEqual(pnl['unrealized']['amount'], 22000)
        self.assertEqual(pnl['day']['amount'], 22000)
        self.assertEqual(pnl['total']['amount'], 22000)
        self.assertEqual(pnl['totalPercent'], 4.0)

    async def test_repeated_reads_use_cached_summary_until_ttl_expires(self) -> None:
        self._seed_synced_account()
        service = self._build_service(cache_ttl_seconds=60)

        first = await service.get_portfolio_summary()
        second = await service.get_portfolio_summary()

        self.assertFalse(first['freshness']['servedFromCache'])
        self.assertTrue(second['freshness']['servedFromCache'])
        self.assertFalse(second['freshness']['isStale'])
        self.assertEqual(len(self.inquiry_service.summary_calls), 1)
        self.assertEqual(len(self.inquiry_service.positions_calls), 1)
        self.assertEqual(len(self.order_service.list_calls), 1)

    async def test_stale_cache_returns_cached_payload_and_revalidates_in_background(self) -> None:
        account_reference = 'KIS:12345678-01'
        self._seed_synced_account(account_reference=account_reference)
        service = self._build_service(cache_ttl_seconds=60)

        initial = await service.get_portfolio_summary()
        self.assertEqual(initial['accounts'][0]['summary']['totalAssets']['amount'], 3072000)
        assert service._cache is not None
        service._cache.cached_at -= timedelta(minutes=5)
        service._cache.last_refresh_attempted_at = service._cache.cached_at
        service._cache.last_refresh_succeeded_at = service._cache.cached_at

        self._seed_synced_account(
            account_reference=account_reference,
            as_of='2026-04-29T01:25:45+00:00',
            total_asset_value=4072000,
            market_value=1572000,
        )

        stale = await service.get_portfolio_summary()

        self.assertTrue(stale['freshness']['servedFromCache'])
        self.assertTrue(stale['freshness']['isStale'])
        self.assertEqual(stale['freshness']['staleReason'], 'refresh_due')
        self.assertTrue(stale['freshness']['refreshInFlight'])
        self.assertEqual(stale['sync']['status'], 'stale')
        self.assertEqual(stale['accounts'][0]['summary']['totalAssets']['amount'], 3072000)

        refresh_task = service._refresh_task
        self.assertIsNotNone(refresh_task)
        await refresh_task

        refreshed = await service.get_portfolio_summary()
        self.assertEqual(refreshed['sync']['status'], 'synced')
        self.assertEqual(refreshed['accounts'][0]['summary']['totalAssets']['amount'], 4072000)
        self.assertEqual(refreshed['accounts'][0]['holdings'][0]['marketValue']['amount'], 1572000)
        self.assertEqual(len(self.inquiry_service.summary_calls), 2)
        self.assertEqual(len(self.inquiry_service.positions_calls), 2)
        self.assertEqual(len(self.order_service.list_calls), 2)

    async def test_refresh_failure_with_existing_cache_returns_stale_cached_summary(self) -> None:
        account_reference = 'KIS:12345678-01'
        self._seed_synced_account(account_reference=account_reference)
        service = self._build_service()

        initial = await service.get_portfolio_summary()
        self.assertEqual(initial['accounts'][0]['summary']['totalAssets']['amount'], 3072000)
        self.inquiry_service.summary_exceptions[account_reference] = RuntimeError('upstream timeout')

        result = await service.get_portfolio_summary(refresh=True)

        self.assertTrue(result['freshness']['servedFromCache'])
        self.assertTrue(result['freshness']['isStale'])
        self.assertEqual(result['freshness']['staleReason'], 'refresh_failed')
        self.assertEqual(result['freshness']['lastRefreshStatus'], 'error')
        self.assertIn('upstream timeout', result['freshness']['lastRefreshError'])
        self.assertEqual(result['sync']['status'], 'stale')
        self.assertEqual(result['accounts'][0]['summary']['totalAssets']['amount'], 3072000)
        self.assertEqual(len(self.inquiry_service.summary_calls), 2)

    async def test_missing_portfolio_account_configuration_returns_not_configured_state(self) -> None:
        service = self._build_service(portfolio_account_references='')

        result = await service.get_portfolio_summary()

        self.assertEqual(result['sync']['status'], 'not_configured')
        self.assertEqual(result['accounts'], [])
        self.assertFalse(result['freshness']['hasCachedSnapshot'])
        self.assertIn('PORTFOLIO_ACCOUNT_REFERENCES', result['notices'][0])

    async def test_missing_kis_credentials_return_not_configured_before_upstream_calls(self) -> None:
        settings.kis_app_key = ''
        settings.kis_app_secret = ''
        service = self._build_service()

        result = await service.get_portfolio_summary()

        self.assertEqual(result['sync']['status'], 'not_configured')
        self.assertEqual(self.inquiry_service.summary_calls, [])
        self.assertEqual(self.inquiry_service.positions_calls, [])
        self.assertIn('KIS_APP_KEY', result['sync']['message'])
        self.assertIn('KIS_APP_SECRET', result['sync']['message'])
        self.assertEqual(result['freshness']['lastRefreshStatus'], 'error')

    def test_runtime_status_reports_production_mock_environment_as_not_ready(self) -> None:
        settings.app_env = 'production'
        settings.kis_use_mock = True
        service = self._build_service()

        result = service.runtime_status()

        self.assertFalse(result['ready'])
        self.assertEqual(result['kis_environment'], 'demo')
        self.assertEqual(result['configured_account_count'], 1)
        self.assertFalse(result['cache']['has_snapshot'])
        self.assertIn('KIS_USE_MOCK must be false in production', result['blocking_reasons'][0])

    async def test_partial_account_failures_mark_summary_stale_without_collapsing_to_not_configured(self) -> None:
        main_account = 'KIS:12345678-01'
        isa_account = 'KIS:87654321-22'
        self._seed_synced_account(account_reference=main_account)
        configuration_error = TradingAccountInquiryUpstreamError(
            'KIS credentials must be configured',
            payload={'detail': 'KIS credentials must be configured', 'configured': False, 'retryable': False},
        )
        self.inquiry_service.summary_exceptions[isa_account] = configuration_error
        self.inquiry_service.positions_exceptions[isa_account] = configuration_error

        service = self._build_service(
            portfolio_account_references='Main=KIS:12345678-01,ISA=KIS:87654321-22'
        )

        result = await service.get_portfolio_summary()

        self.assertEqual(result['sync']['status'], 'stale')
        self.assertEqual(result['freshness']['lastRefreshStatus'], 'degraded')
        self.assertFalse(result['freshness']['isStale'])
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

        service = self._build_service()

        result = await service.get_portfolio_summary()

        self.assertEqual(result['sync']['status'], 'not_configured')
        self.assertEqual(result['accounts'][0]['status'], 'unavailable')
        self.assertEqual(result['freshness']['lastRefreshStatus'], 'degraded')
        self.assertFalse(result['freshness']['isStale'])
        self.assertIn('not configured', result['sync']['message'].lower())


if __name__ == '__main__':
    unittest.main()
