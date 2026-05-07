import unittest
from datetime import date

from app.dart.fundamentals_history_service import DartFundamentalsHistoryService


class FakeHistoryRepository:
    def __init__(self, items):
        self.items = items
        self.last_args = None

    def list_stored_fundamentals(self, *, stock_codes, fs_div=None):
        self.last_args = {'stock_codes': stock_codes, 'fs_div': fs_div}
        return self.items


class DartFundamentalsHistoryServiceTests(unittest.TestCase):
    def test_history_service_groups_entries_by_symbol_and_filters_period_window(self):
        repository = FakeHistoryRepository(
            [
                {
                    'rcept_no': '20250312000123',
                    'corp_code': '00126380',
                    'stock_code': '005930',
                    'corp_name': '삼성전자',
                    'business_year': 2024,
                    'report_code': '11011',
                    'filing_category': 'annual_report',
                    'fs_div': 'CFS',
                    'statement_type': 'consolidated',
                    'period_type': 'annual',
                    'period_end_month': 12,
                    'currency': 'KRW',
                    'source_status': '000',
                    'source_message': 'OK',
                    'captured_at': '2025-03-12T01:02:03+00:00',
                    'disclosure': {
                        'report_nm': '사업보고서 (2024.12)',
                        'receipt_date': '2025-03-12',
                        'remark': None,
                    },
                    'highlights': [
                        {
                            'metric_key': 'revenue',
                            'metric_type': 'account',
                            'statement_type': 'IS',
                            'statement_name': '손익계산서',
                            'account_id': 'ifrs-full_Revenue',
                            'account_name': '매출액',
                            'current_amount': 100,
                            'current_cumulative_amount': 100,
                            'prior_amount': 90,
                            'prior_cumulative_amount': 90,
                            'pre_prior_amount': None,
                            'currency': 'KRW',
                            'ratio_value': None,
                        },
                        {
                            'metric_key': 'total_assets',
                            'metric_type': 'account',
                            'statement_type': 'BS',
                            'statement_name': '재무상태표',
                            'account_id': 'ifrs-full_Assets',
                            'account_name': '자산총계',
                            'current_amount': 400,
                            'current_cumulative_amount': None,
                            'prior_amount': 380,
                            'prior_cumulative_amount': None,
                            'pre_prior_amount': None,
                            'currency': 'KRW',
                            'ratio_value': None,
                        },
                        {
                            'metric_key': 'total_liabilities',
                            'metric_type': 'account',
                            'statement_type': 'BS',
                            'statement_name': '재무상태표',
                            'account_id': 'ifrs-full_Liabilities',
                            'account_name': '부채총계',
                            'current_amount': 160,
                            'current_cumulative_amount': None,
                            'prior_amount': 150,
                            'prior_cumulative_amount': None,
                            'pre_prior_amount': None,
                            'currency': 'KRW',
                            'ratio_value': None,
                        },
                        {
                            'metric_key': 'total_equity',
                            'metric_type': 'account',
                            'statement_type': 'BS',
                            'statement_name': '재무상태표',
                            'account_id': 'ifrs-full_Equity',
                            'account_name': '자본총계',
                            'current_amount': 240,
                            'current_cumulative_amount': None,
                            'prior_amount': 230,
                            'prior_cumulative_amount': None,
                            'pre_prior_amount': None,
                            'currency': 'KRW',
                            'ratio_value': None,
                        },
                        {
                            'metric_key': 'operating_margin',
                            'metric_type': 'ratio',
                            'statement_type': None,
                            'statement_name': None,
                            'account_id': None,
                            'account_name': None,
                            'current_amount': None,
                            'current_cumulative_amount': None,
                            'prior_amount': None,
                            'prior_cumulative_amount': None,
                            'pre_prior_amount': None,
                            'currency': None,
                            'ratio_value': '0.2000',
                        },
                    ],
                    'statement_rows': [
                        {
                            'row_key': 'row-1',
                            'statement_type': 'IS',
                            'statement_name': '손익계산서',
                            'account_id': 'ifrs-full_Revenue',
                            'account_name': '매출액',
                            'current_amount': 100,
                            'current_cumulative_amount': 100,
                            'prior_amount': 90,
                            'prior_cumulative_amount': 90,
                            'pre_prior_amount': None,
                            'order': 1,
                            'currency': 'KRW',
                        }
                    ],
                },
                {
                    'rcept_no': '20241114000123',
                    'corp_code': '00126380',
                    'stock_code': '005930',
                    'corp_name': '삼성전자',
                    'business_year': 2024,
                    'report_code': '11014',
                    'filing_category': 'quarterly_report',
                    'fs_div': 'CFS',
                    'statement_type': 'consolidated',
                    'period_type': 'q3',
                    'period_end_month': 9,
                    'currency': 'KRW',
                    'source_status': '000',
                    'source_message': 'OK',
                    'captured_at': '2024-11-14T01:02:03+00:00',
                    'disclosure': {
                        'report_nm': '[기재정정] 분기보고서 (2024.09)',
                        'receipt_date': '2024-11-14',
                        'remark': '정',
                    },
                    'highlights': [],
                    'statement_rows': [],
                },
            ]
        )
        service = DartFundamentalsHistoryService(repository=repository)

        result = service.get_historical_fundamentals(
            symbols=['005930'],
            start_date=date(2024, 10, 1),
            end_date=date(2025, 12, 31),
            fs_div='CFS',
        )

        self.assertEqual(repository.last_args, {'stock_codes': ['005930'], 'fs_div': 'CFS'})
        self.assertEqual(result['summary']['requested_symbol_count'], 1)
        self.assertEqual(result['summary']['matched_symbol_count'], 1)
        self.assertEqual(result['summary']['entries'], 1)
        self.assertEqual(result['companies'][0]['symbol'], '005930')
        entry = result['companies'][0]['entries'][0]
        self.assertEqual(entry['period']['period_end_date'], '2024-12-31')
        self.assertEqual(entry['period']['report_code'], '11011')
        self.assertEqual(entry['highlights']['revenue']['current_amount'], 100)
        self.assertEqual(entry['highlights']['ratios']['operating_margin'], '0.2000')
        self.assertEqual(
            entry['fundamental_context']['normalized_facts']['balance_sheet']['total_assets']['current_value'],
            400,
        )
        self.assertEqual(
            entry['fundamental_context']['analysis']['leverage_liquidity']['metrics']['debt_to_equity'],
            '0.6667',
        )
        self.assertEqual(entry['fundamental_context']['source_provenance']['rcept_no'], '20250312000123')
        self.assertEqual(entry['statements']['count'], 1)

    def test_history_service_reports_missing_symbols(self):
        service = DartFundamentalsHistoryService(repository=FakeHistoryRepository([]))

        result = service.get_historical_fundamentals(
            symbols=['005930', '000660'],
            exact_date=date(2024, 12, 31),
        )

        self.assertEqual(result['summary']['matched_symbol_count'], 0)
        self.assertEqual(result['summary']['missing_symbols'], ['005930', '000660'])

    def test_history_service_rejects_missing_date_window(self):
        service = DartFundamentalsHistoryService(repository=FakeHistoryRepository([]))

        with self.assertRaisesRegex(ValueError, 'Provide date or at least one of start_date/end_date'):
            service.get_historical_fundamentals(symbols=['005930'])
