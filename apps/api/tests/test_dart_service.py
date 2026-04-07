import unittest

from app.dart.service import DartService, build_fundamentals_dataset


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

    async def fetch_single_company_financial_statements(self, *, corp_code, bsns_year, reprt_code, fs_div):
        self.financials_args = {
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
                    'rcept_no': '20260311001234',
                    'reprt_code': reprt_code,
                    'bsns_year': bsns_year,
                    'sj_div': 'BS',
                    'sj_nm': '재무상태표',
                    'account_id': 'ifrs-full_Assets',
                    'account_nm': '자산총계',
                    'thstrm_nm': '제 57 기말',
                    'thstrm_amount': '500,000',
                    'frmtrm_nm': '제 56 기말',
                    'frmtrm_amount': '450,000',
                    'ord': '1',
                    'currency': 'KRW',
                },
                {
                    'rcept_no': '20260311001234',
                    'reprt_code': reprt_code,
                    'bsns_year': bsns_year,
                    'sj_div': 'IS',
                    'sj_nm': '손익계산서',
                    'account_id': 'ifrs-full_Revenue',
                    'account_nm': '매출액',
                    'thstrm_nm': '제 57 기',
                    'thstrm_amount': '100,000',
                    'thstrm_add_amount': '100,000',
                    'frmtrm_nm': '제 56 기',
                    'frmtrm_amount': '95,000',
                    'frmtrm_add_amount': '95,000',
                    'ord': '2',
                    'currency': 'KRW',
                },
                {
                    'rcept_no': '20260311001234',
                    'reprt_code': reprt_code,
                    'bsns_year': bsns_year,
                    'sj_div': 'IS',
                    'sj_nm': '손익계산서',
                    'account_id': 'dart_OperatingIncomeLoss',
                    'account_nm': '영업이익',
                    'thstrm_nm': '제 57 기',
                    'thstrm_amount': '20,000',
                    'frmtrm_nm': '제 56 기',
                    'frmtrm_amount': '18,000',
                    'ord': '3',
                    'currency': 'KRW',
                },
                {
                    'rcept_no': '20260311001234',
                    'reprt_code': reprt_code,
                    'bsns_year': bsns_year,
                    'sj_div': 'IS',
                    'sj_nm': '손익계산서',
                    'account_id': 'ifrs-full_ProfitLoss',
                    'account_nm': '당기순이익',
                    'thstrm_nm': '제 57 기',
                    'thstrm_amount': '15,000',
                    'frmtrm_nm': '제 56 기',
                    'frmtrm_amount': '14,000',
                    'ord': '4',
                    'currency': 'KRW',
                },
                {
                    'rcept_no': '20260311001234',
                    'reprt_code': reprt_code,
                    'bsns_year': bsns_year,
                    'sj_div': 'BS',
                    'sj_nm': '재무상태표',
                    'account_id': 'ifrs-full_Liabilities',
                    'account_nm': '부채총계',
                    'thstrm_nm': '제 57 기말',
                    'thstrm_amount': '300,000',
                    'frmtrm_nm': '제 56 기말',
                    'frmtrm_amount': '280,000',
                    'ord': '5',
                    'currency': 'KRW',
                },
                {
                    'rcept_no': '20260311001234',
                    'reprt_code': reprt_code,
                    'bsns_year': bsns_year,
                    'sj_div': 'BS',
                    'sj_nm': '재무상태표',
                    'account_id': 'ifrs-full_Equity',
                    'account_nm': '자본총계',
                    'thstrm_nm': '제 57 기말',
                    'thstrm_amount': '200,000',
                    'frmtrm_nm': '제 56 기말',
                    'frmtrm_amount': '170,000',
                    'ord': '6',
                    'currency': 'KRW',
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

    async def test_fundamentals_returns_normalized_financials_and_highlights(self):
        client = FakeDartClient()
        service = DartService(client=client)

        result = await service.get_fundamentals(stock_code='005930', business_year=2025)

        self.assertEqual(client.financials_args['corp_code'], '00126380')
        self.assertEqual(client.financials_args['bsns_year'], '2025')
        self.assertEqual(client.financials_args['reprt_code'], '11011')
        self.assertEqual(client.financials_args['fs_div'], 'CFS')
        self.assertEqual(result['request']['dataset'], 'fnlttSinglAcntAll')
        self.assertEqual(result['statements']['count'], 6)
        self.assertEqual(result['statements']['items'][0]['current_amount'], 500000)
        self.assertEqual(result['highlights']['total_assets']['current_amount'], 500000)
        self.assertEqual(result['highlights']['revenue']['current_cumulative_amount'], 100000)
        self.assertEqual(result['highlights']['ratios']['operating_margin'], '0.2000')

    async def test_fundamentals_keeps_period_and_statement_type_usability(self):
        client = FakeDartClient()
        service = DartService(client=client)

        result = await service.get_company_fundamentals(
            stock_code='005930',
            fiscal_year=2025,
            period='q1',
            statement_type='consolidated',
        )

        self.assertEqual(client.financials_args['reprt_code'], '11013')
        self.assertEqual(client.financials_args['fs_div'], 'CFS')
        self.assertEqual(result['request']['period'], 'q1')
        self.assertEqual(result['request']['period_scope'], 'year_to_date')
        self.assertEqual(result['request']['statement_type'], 'consolidated')

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

    def test_build_fundamentals_dataset_matches_financial_industry_variants(self):
        dataset = build_fundamentals_dataset(
            {
                'list': [
                    {
                        'rcept_no': '20260407000001',
                        'reprt_code': '11011',
                        'bsns_year': '2024',
                        'sj_div': 'BS',
                        'sj_nm': '재무상태표',
                        'account_id': 'ifrs-full_Assets',
                        'account_nm': '자산총계',
                        'thstrm_amount': '1,325,242',
                        'ord': '10',
                        'currency': 'KRW',
                    },
                    {
                        'rcept_no': '20260407000001',
                        'reprt_code': '11011',
                        'bsns_year': '2024',
                        'sj_div': 'BS',
                        'sj_nm': '재무상태표',
                        'account_id': 'ifrs-full_Liabilities',
                        'account_nm': '부채총계',
                        'thstrm_amount': '1,017,688',
                        'ord': '35',
                        'currency': 'KRW',
                    },
                    {
                        'rcept_no': '20260407000001',
                        'reprt_code': '11011',
                        'bsns_year': '2024',
                        'sj_div': 'BS',
                        'sj_nm': '재무상태표',
                        'account_id': 'ifrs-full_Equity',
                        'account_nm': '자본총계',
                        'thstrm_amount': '307,554',
                        'ord': '24',
                        'currency': 'KRW',
                    },
                    {
                        'rcept_no': '20260407000001',
                        'reprt_code': '11011',
                        'bsns_year': '2024',
                        'sj_div': 'CIS',
                        'sj_nm': '포괄손익계산서',
                        'account_id': 'ifrs-full_FeeAndCommissionIncome',
                        'account_nm': '수수료수익',
                        'thstrm_amount': '1,192',
                        'ord': '19',
                        'currency': 'KRW',
                    },
                    {
                        'rcept_no': '20260407000001',
                        'reprt_code': '11011',
                        'bsns_year': '2024',
                        'sj_div': 'CIS',
                        'sj_nm': '포괄손익계산서',
                        'account_id': 'ifrs-full_RevenueFromInterest',
                        'account_nm': '이자수익',
                        'thstrm_amount': '83,149',
                        'ord': '23',
                        'currency': 'KRW',
                    },
                    {
                        'rcept_no': '20260407000001',
                        'reprt_code': '11011',
                        'bsns_year': '2024',
                        'sj_div': 'CIS',
                        'sj_nm': '포괄손익계산서',
                        'account_id': 'ifrs-full_ProfitLossFromOperatingActivities',
                        'account_nm': '영업이익(손실)',
                        'thstrm_amount': '6,847',
                        'ord': '32',
                        'currency': 'KRW',
                    },
                    {
                        'rcept_no': '20260407000001',
                        'reprt_code': '11011',
                        'bsns_year': '2024',
                        'sj_div': 'CIS',
                        'sj_nm': '포괄손익계산서',
                        'account_id': 'ifrs-full_ProfitLoss',
                        'account_nm': '당기순이익',
                        'thstrm_amount': '7,013',
                        'ord': '30',
                        'currency': 'KRW',
                    },
                    {
                        'rcept_no': '20260407000001',
                        'reprt_code': '11011',
                        'bsns_year': '2024',
                        'sj_div': 'CF',
                        'sj_nm': '현금흐름표',
                        'account_id': 'ifrs-full_CashFlowsFromUsedInOperatingActivities',
                        'account_nm': '영업활동순현금흐름',
                        'thstrm_amount': '-11,878',
                        'ord': '28',
                        'currency': 'KRW',
                    },
                    {
                        'rcept_no': '20260407000001',
                        'reprt_code': '11011',
                        'bsns_year': '2024',
                        'sj_div': 'SCE',
                        'sj_nm': '자본변동표',
                        'account_id': 'ifrs-full_ProfitLoss',
                        'account_nm': '당기순이익',
                        'account_detail': '자본 [구성요소]|이익잉여금 [구성요소]',
                        'thstrm_amount': '7,013',
                        'ord': '16',
                        'currency': 'KRW',
                    },
                ],
            }
        )

        self.assertEqual(dataset['highlights']['revenue']['account_id'], 'ifrs-full_RevenueFromInterest')
        self.assertEqual(dataset['highlights']['operating_income']['account_id'], 'ifrs-full_ProfitLossFromOperatingActivities')
        self.assertEqual(dataset['highlights']['net_income']['statement_type'], 'CIS')
        self.assertEqual(dataset['highlights']['operating_cash_flow']['account_name'], '영업활동순현금흐름')

    def test_build_fundamentals_dataset_prefers_primary_statement_rows_over_component_rows(self):
        dataset = build_fundamentals_dataset(
            {
                'list': [
                    {
                        'rcept_no': '20260407000002',
                        'reprt_code': '11011',
                        'bsns_year': '2024',
                        'sj_div': 'SCE',
                        'sj_nm': '자본변동표',
                        'account_id': 'ifrs-full_Equity',
                        'account_nm': '자본총계',
                        'account_detail': '자본 [구성요소]|이익잉여금 [구성요소]',
                        'thstrm_amount': '297,547',
                        'ord': '17',
                        'currency': 'KRW',
                    },
                    {
                        'rcept_no': '20260407000002',
                        'reprt_code': '11011',
                        'bsns_year': '2024',
                        'sj_div': 'BS',
                        'sj_nm': '재무상태표',
                        'account_id': 'ifrs-full_Equity',
                        'account_nm': '자본총계',
                        'account_detail': '-',
                        'thstrm_amount': '307,554',
                        'ord': '24',
                        'currency': 'KRW',
                    },
                ],
            }
        )

        self.assertEqual(dataset['highlights']['total_equity']['statement_type'], 'BS')
        self.assertEqual(dataset['highlights']['total_equity']['current_amount'], 307554)


if __name__ == '__main__':
    unittest.main()
