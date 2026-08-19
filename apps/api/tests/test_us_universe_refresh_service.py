import unittest
from unittest.mock import AsyncMock, Mock

from app.integrations.kis_overseas_master.client import KISOverseasMasterSecurity
from app.integrations.sec_edgar.client import SECCompanyTicker
from app.us_equities.universe import USListedSecurity
from app.us_equities.universe_service import USUniverseRefreshService


class USUniverseRefreshServiceTests(unittest.IsolatedAsyncioTestCase):
    async def test_refresh_cross_validates_sources_and_persists_only_integrated_symbols(self):
        listed_client = Mock()
        listed_client.fetch_universe = AsyncMock(return_value=[
            USListedSecurity('AAPL', 'Apple Inc. Common Stock', 'NASDAQ', False, False, 'N'),
        ])
        kis_client = Mock()
        kis_client.fetch_us_universe = AsyncMock(return_value=[
            KISOverseasMasterSecurity(
                symbol='AAPL', realtime_symbol='NASAAPL', exchange='NASDAQ', korean_name='애플',
                english_name='APPLE INC', security_type='2', currency='USD', decimal_places=4,
                base_price='220.0000', bid_order_size='1', ask_order_size='1', market_open_hhmm='930',
                market_close_hhmm='1600', is_depositary_receipt=False, industry_code='013',
                constituent_flag='1', etp_type_code='004',
            )
        ])
        sec_client = Mock()
        sec_client.fetch_company_ticker_exchange_mapping = AsyncMock(return_value=[
            SECCompanyTicker('0000320193', 'Apple Inc.', 'AAPL', 'Nasdaq'),
        ])
        persister = Mock(return_value=1)
        service = USUniverseRefreshService(
            nasdaq_client=listed_client,
            kis_master_client=kis_client,
            sec_client=sec_client,
            persister=persister,
        )

        result = await service.refresh(persist=True, preview_limit=5)

        self.assertEqual(result['status'], 'completed')
        self.assertEqual(result['counts']['integrated'], 1)
        self.assertEqual(result['counts']['sec_matched'], 1)
        self.assertEqual(result['persisted_count'], 1)
        self.assertEqual(result['preview'][0]['symbol'], 'AAPL')
        persisted_items = persister.call_args.args[0]
        self.assertEqual(persisted_items[0].regulatory_id, '0000320193')
        self.assertEqual(persister.call_args.kwargs['source'], 'us_integrated_master')

    async def test_dry_run_does_not_persist(self):
        listed_client = Mock(fetch_universe=AsyncMock(return_value=[]))
        kis_client = Mock(fetch_us_universe=AsyncMock(return_value=[]))
        sec_client = Mock(fetch_company_ticker_exchange_mapping=AsyncMock(return_value=[]))
        persister = Mock()
        service = USUniverseRefreshService(
            nasdaq_client=listed_client,
            kis_master_client=kis_client,
            sec_client=sec_client,
            persister=persister,
        )

        result = await service.refresh(persist=False)

        self.assertEqual(result['persisted_count'], 0)
        persister.assert_not_called()


if __name__ == '__main__':
    unittest.main()
