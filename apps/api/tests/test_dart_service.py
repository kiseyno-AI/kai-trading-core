import unittest

from app.dart.service import DartService


class FakeDartClient:
    async def lookup_corp_code(self, *, corp_code=None, stock_code=None, corp_name=None):
        self.lookup_args = {
            'corp_code': corp_code,
            'stock_code': stock_code,
            'corp_name': corp_name,
        }
        return {
            'corp_code': '00126380',
            'corp_name': '삼성전자',
            'stock_code': '005930',
            'modify_date': '20250101',
        }

    async def fetch_company_overview(self, corp_code):
        self.company_corp_code = corp_code
        return {
            'status': '000',
            'corp_name': '삼성전자',
            'stock_code': '005930',
            'ceo_nm': 'Test CEO',
        }

    async def fetch_filings(self, *, corp_code, bgn_de, end_de, page_count, last_reprt_at='Y'):
        self.filings_args = {
            'corp_code': corp_code,
            'bgn_de': bgn_de,
            'end_de': end_de,
            'page_count': page_count,
            'last_reprt_at': last_reprt_at,
        }
        return {
            'status': '000',
            'total_count': '1',
            'page_count': str(page_count),
            'page_no': '1',
            'list': [
                {
                    'rcept_no': '20250101000001',
                    'report_nm': '사업보고서',
                }
            ],
        }

    async def fetch_financial_statements(self, *, corp_code, bsns_year, reprt_code, fs_div='CFS'):
        self.fundamentals_args = {
            'corp_code': corp_code,
            'bsns_year': bsns_year,
            'reprt_code': reprt_code,
            'fs_div': fs_div,
        }
        return {
            'status': '000',
            'message': 'OK',
            'list': [
                {
                    'account_id': 'ifrs-full_Revenue',
                    'account_nm': '매출액',
                    'sj_nm': '손익계산서',
                    'fs_nm': '연결재무제표',
                    'currency': 'KRW',
                    'thstrm_amount': '1000',
                    'frmtrm_amount': '900',
                    'bfefrmtrm_amount': '800',
                },
                {
                    'account_id': 'dart_OperatingIncomeLoss',
                    'account_nm': '영업이익',
                    'sj_nm': '손익계산서',
                    'fs_nm': '연결재무제표',
                    'currency': 'KRW',
                    'thstrm_amount': '200',
                    'frmtrm_amount': '180',
                    'bfefrmtrm_amount': '160',
                },
                {
                    'account_id': 'ifrs-full_ProfitLoss',
                    'account_nm': '당기순이익',
                    'sj_nm': '손익계산서',
                    'fs_nm': '연결재무제표',
                    'currency': 'KRW',
                    'thstrm_amount': '150',
                    'frmtrm_amount': '140',
                    'bfefrmtrm_amount': '120',
                },
                {
                    'account_id': 'ifrs-full_Liabilities',
                    'account_nm': '부채총계',
                    'sj_nm': '재무상태표',
                    'fs_nm': '연결재무제표',
                    'currency': 'KRW',
                    'thstrm_amount': '600',
                    'frmtrm_amount': '550',
                    'bfefrmtrm_amount': '500',
                },
                {
                    'account_id': 'ifrs-full_Equity',
                    'account_nm': '자본총계',
                    'sj_nm': '재무상태표',
                    'fs_nm': '연결재무제표',
                    'currency': 'KRW',
                    'thstrm_amount': '400',
                    'frmtrm_amount': '350',
                    'bfefrmtrm_amount': '300',
                },
            ],
        }


class DartServiceTests(unittest.IsolatedAsyncioTestCase):
    async def test_company_snapshot_combines_lookup_company_and_filings(self):
        client = FakeDartClient()
        service = DartService(client=client)

        result = await service.get_company_snapshot(stock_code='5930', filing_days=90, filings_limit=5)

        self.assertEqual(client.lookup_args['stock_code'], '5930')
        self.assertEqual(client.company_corp_code, '00126380')
        self.assertEqual(client.filings_args['corp_code'], '00126380')
        self.assertEqual(client.filings_args['page_count'], 5)
        self.assertEqual(result['lookup']['stock_code'], '005930')
        self.assertEqual(result['company']['corp_name'], '삼성전자')
        self.assertEqual(result['recent_filings']['items'][0]['rcept_no'], '20250101000001')

    async def test_company_fundamentals_normalizes_highlights_and_report_semantics(self):
        client = FakeDartClient()
        service = DartService(client=client)

        result = await service.get_company_fundamentals(
            stock_code='005930',
            fiscal_year=2025,
            period='q1',
            statement_type='consolidated',
        )

        self.assertEqual(client.fundamentals_args['reprt_code'], '11013')
        self.assertEqual(client.fundamentals_args['fs_div'], 'CFS')
        self.assertEqual(result['request']['period'], 'q1')
        self.assertEqual(result['request']['period_scope'], 'year_to_date')
        self.assertEqual(result['highlights']['revenue']['current_amount'], '1000')
        self.assertEqual(result['highlights']['ratios']['operating_margin'], '0.2000')

    async def test_company_fundamentals_rejects_mismatched_period_and_report_code(self):
        client = FakeDartClient()
        service = DartService(client=client)

        with self.assertRaisesRegex(ValueError, 'period and report_code refer to different filing windows'):
            await service.get_company_fundamentals(
                stock_code='005930',
                fiscal_year=2025,
                period='annual',
                report_code='11013',
            )


if __name__ == '__main__':
    unittest.main()
