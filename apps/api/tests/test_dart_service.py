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


if __name__ == '__main__':
    unittest.main()
