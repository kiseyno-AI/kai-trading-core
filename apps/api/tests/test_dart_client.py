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
        self.assertEqual(result['page_no'], '1')

    async def test_fetch_company_overview_still_raises_for_non_success_status(self):
        client = TestableDartClient({'status': '014', 'message': 'File does not exist'})

        with self.assertRaises(DartAPIError):
            await client.fetch_company_overview('00126380')

    async def test_fetch_disclosure_list_uses_requested_page_number_for_no_data_status(self):
        client = TestableDartClient({'status': '013', 'message': 'No data'})

        result = await client.fetch_disclosure_list(
            bgn_de='20260101',
            end_de='20260131',
            page_no=3,
            page_count=100,
        )

        self.assertEqual(result['status'], '013')
        self.assertEqual(result['page_no'], '3')
        self.assertEqual(result['list'], [])


if __name__ == '__main__':
    unittest.main()
