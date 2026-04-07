import unittest

from app.integrations.dart.client import DartAPIError, DartClient


class FakeResponse:
    def __init__(self, payload):
        self._payload = payload
        self.status_code = 200
        self.text = ''
        self.content = b''

    def json(self):
        return self._payload


class TestableDartClient(DartClient):
    def __init__(self, payload):
        super().__init__()
        self.payload = payload

    async def _request(self, path, params):
        return FakeResponse(self.payload)


class DartClientTests(unittest.IsolatedAsyncioTestCase):
    async def test_fetch_single_company_financial_statements_returns_empty_list_for_no_data_status(self):
        client = TestableDartClient({'status': '013', 'message': 'No data'})

        result = await client.fetch_single_company_financial_statements(
            corp_code='00126380',
            bsns_year='2025',
            reprt_code='11011',
            fs_div='CFS',
        )

        self.assertEqual(result['status'], '013')
        self.assertEqual(result['list'], [])

    async def test_fetch_filings_returns_empty_list_for_no_data_status(self):
        client = TestableDartClient({'status': '013', 'message': 'No data'})

        result = await client.fetch_filings(
            corp_code='00126380',
            bgn_de='20260101',
            end_de='20260131',
        )

        self.assertEqual(result['status'], '013')
        self.assertEqual(result['total_count'], '0')
        self.assertEqual(result['list'], [])

    async def test_fetch_financial_statements_returns_empty_list_for_no_data_status(self):
        client = TestableDartClient({'status': '013', 'message': 'No data'})

        result = await client.fetch_financial_statements(
            corp_code='00126380',
            bsns_year='2025',
            reprt_code='11011',
        )

        self.assertEqual(result['status'], '013')
        self.assertEqual(result['list'], [])

    async def test_fetch_company_overview_still_raises_for_non_success_status(self):
        client = TestableDartClient({'status': '014', 'message': 'File does not exist'})

        with self.assertRaises(DartAPIError):
            await client.fetch_company_overview('00126380')


if __name__ == '__main__':
    unittest.main()
