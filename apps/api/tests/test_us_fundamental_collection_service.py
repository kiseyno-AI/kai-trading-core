import unittest
from datetime import date
from unittest.mock import patch

from app.us_equities.fundamental_collection_service import USFundamentalCollectionService


def _companyfacts(multiplier: int = 1) -> dict:
    def annual(value: int) -> dict:
        return {'USD': [{
            'start': '2024-01-01', 'end': '2024-12-31', 'val': value * multiplier,
            'accn': 'annual', 'fy': 2024, 'fp': 'FY', 'form': '10-K', 'filed': '2025-02-20',
        }]}

    return {'facts': {
        'us-gaap': {
            'Revenues': {'units': annual(1000)},
            'NetCashProvidedByUsedInOperatingActivities': {'units': annual(200)},
            'NetIncomeLoss': {'units': annual(100)},
            'Assets': {'units': {'USD': [
                {'end': '2024-12-31', 'val': 2000 * multiplier, 'accn': 'annual', 'fy': 2024, 'fp': 'FY', 'form': '10-K', 'filed': '2025-02-20'},
            ]}},
            'Liabilities': {'units': {'USD': [
                {'end': '2024-12-31', 'val': 800 * multiplier, 'accn': 'annual', 'fy': 2024, 'fp': 'FY', 'form': '10-K', 'filed': '2025-02-20'},
            ]}},
        },
        'dei': {
            'EntityCommonStockSharesOutstanding': {'units': {'shares': [
                {'end': '2023-12-31', 'val': 100, 'accn': 'old', 'form': '10-K', 'filed': '2024-02-20'},
                {'end': '2024-12-31', 'val': 102, 'accn': 'annual', 'form': '10-K', 'filed': '2025-02-20'},
            ]}},
        },
    }}


def _submissions() -> dict:
    return {'filings': {'recent': {
        'accessionNumber': ['annual'], 'filingDate': ['2025-02-20'], 'reportDate': ['2024-12-31'],
        'form': ['10-K'], 'items': [''],
    }}}


class FakeSECClient:
    async def fetch_companyfacts(self, cik):
        return _companyfacts(int(str(cik)[-1]) or 1)

    async def fetch_submissions(self, cik):
        return _submissions()


class USFundamentalCollectionServiceTests(unittest.IsolatedAsyncioTestCase):
    async def test_collects_only_price_candidates_and_returns_agent_shortlist(self):
        candidates = [
            {'id': 1, 'symbol': 'AAPL', 'regulatory_id': '0000000001', 'collection_run_id': 7, 'metrics_payload': {'latest_price': '100'}},
            {'id': 2, 'symbol': 'MSFT', 'regulatory_id': '0000000002', 'collection_run_id': 7, 'metrics_payload': {'latest_price': '200'}},
        ]
        service = USFundamentalCollectionService(
            sec_client=FakeSECClient(),
            candidate_loader=lambda **kwargs: candidates,
        )

        result = await service.collect_and_screen(market_date=date(2025, 6, 1), persist=False)

        self.assertEqual(result['counts']['price_candidates'], 2)
        self.assertEqual(result['counts']['sec_snapshots_collected'], 2)
        self.assertEqual(result['counts']['agent_candidates'], 2)
        self.assertEqual(set(result['selected_symbols']), {'AAPL', 'MSFT'})

    async def test_missing_cik_is_isolated_as_symbol_error(self):
        service = USFundamentalCollectionService(
            sec_client=FakeSECClient(),
            candidate_loader=lambda **kwargs: [
                {'id': 1, 'symbol': 'NOSEC', 'regulatory_id': None, 'collection_run_id': None, 'metrics_payload': {}},
            ],
        )

        result = await service.collect_and_screen(market_date=date(2025, 6, 1), persist=False)

        self.assertEqual(result['status'], 'completed_with_errors')
        self.assertEqual(result['counts']['agent_candidates'], 0)
        self.assertEqual(result['error_preview'], {'NOSEC': 'missing_sec_cik'})


if __name__ == '__main__':
    unittest.main()
