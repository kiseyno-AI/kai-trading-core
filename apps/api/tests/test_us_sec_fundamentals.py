import unittest
from datetime import date
from decimal import Decimal

from app.us_equities.sec_fundamentals import parse_sec_fundamental_snapshot


def _flow_units(annual: int, current_q1: int, prior_q1: int) -> dict:
    return {'USD': [
        {
            'start': '2024-01-01', 'end': '2024-12-31', 'val': annual, 'accn': 'annual',
            'fy': 2024, 'fp': 'FY', 'form': '10-K', 'filed': '2025-02-20',
        },
        {
            'start': '2024-01-01', 'end': '2024-03-31', 'val': prior_q1, 'accn': 'prior-q1',
            'fy': 2024, 'fp': 'Q1', 'form': '10-Q', 'filed': '2024-05-01',
        },
        {
            'start': '2025-01-01', 'end': '2025-03-31', 'val': current_q1, 'accn': 'current-q1',
            'fy': 2025, 'fp': 'Q1', 'form': '10-Q', 'filed': '2025-05-01',
        },
    ]}


class USSECFundamentalTests(unittest.TestCase):
    def test_builds_ttm_metrics_and_dilution_from_companyfacts(self):
        companyfacts = {
            'entityName': 'Example Inc.',
            'facts': {
                'us-gaap': {
                    'RevenueFromContractWithCustomerExcludingAssessedTax': {'units': _flow_units(1000, 300, 250)},
                    'NetCashProvidedByUsedInOperatingActivities': {'units': _flow_units(200, 60, 50)},
                    'NetIncomeLoss': {'units': _flow_units(100, 35, 25)},
                    'Assets': {'units': {'USD': [
                        {'end': '2025-03-31', 'val': 2000, 'accn': 'current-q1', 'fy': 2025, 'fp': 'Q1', 'form': '10-Q', 'filed': '2025-05-01'},
                    ]}},
                    'Liabilities': {'units': {'USD': [
                        {'end': '2025-03-31', 'val': 900, 'accn': 'current-q1', 'fy': 2025, 'fp': 'Q1', 'form': '10-Q', 'filed': '2025-05-01'},
                    ]}},
                },
                'dei': {
                    'EntityCommonStockSharesOutstanding': {'units': {'shares': [
                        {'end': '2024-03-31', 'val': 100, 'accn': 'prior-q1', 'form': '10-Q', 'filed': '2024-05-01'},
                        {'end': '2025-03-31', 'val': 110, 'accn': 'current-q1', 'form': '10-Q', 'filed': '2025-05-01'},
                    ]}},
                },
            },
        }
        submissions = {'filings': {'recent': {
            'accessionNumber': ['current-q1'],
            'filingDate': ['2025-05-01'],
            'reportDate': ['2025-03-31'],
            'form': ['10-Q'],
            'items': [''],
        }}}

        snapshot = parse_sec_fundamental_snapshot(
            symbol='TEST',
            cik='0000000001',
            companyfacts=companyfacts,
            submissions=submissions,
            market_date=date(2025, 6, 1),
        )

        self.assertIsNotNone(snapshot)
        self.assertEqual(snapshot.revenue_usd, Decimal('1050'))
        self.assertEqual(snapshot.operating_cash_flow_usd, Decimal('210'))
        self.assertEqual(snapshot.net_income_usd, Decimal('110'))
        self.assertEqual(snapshot.metrics.total_liabilities, Decimal('900'))
        self.assertEqual(snapshot.metrics.shares_outstanding_growth_yoy, Decimal('0.1'))
        self.assertFalse(snapshot.metrics.has_bankruptcy_or_delinquency_flag)

    def test_marks_recent_bankruptcy_and_late_filing_forms(self):
        companyfacts = {'facts': {'us-gaap': {
            'Assets': {'units': {'USD': [
                {'end': '2025-03-31', 'val': 10, 'accn': 'q1', 'form': '10-Q', 'filed': '2025-05-01'},
            ]}},
        }}}
        submissions = {'filings': {'recent': {
            'accessionNumber': ['q1', 'late', 'bankruptcy'],
            'filingDate': ['2025-05-01', '2025-05-10', '2025-05-15'],
            'reportDate': ['2025-03-31', '', ''],
            'form': ['10-Q', 'NT 10-Q', '8-K'],
            'items': ['', '', '1.03'],
        }}}

        snapshot = parse_sec_fundamental_snapshot(
            symbol='RISK', cik='0000000002', companyfacts=companyfacts, submissions=submissions,
            market_date=date(2025, 6, 1),
        )

        self.assertTrue(snapshot.metrics.has_bankruptcy_or_delinquency_flag)


if __name__ == '__main__':
    unittest.main()
