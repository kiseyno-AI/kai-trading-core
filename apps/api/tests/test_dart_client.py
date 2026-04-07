import unittest
from unittest.mock import patch

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


class FakeHTTPErrorResponse:
    def __init__(self, status_code=400, text=''):
        self.status_code = status_code
        self.text = text


class FakeAsyncClient:
    def __init__(self, *args, **kwargs):
        pass

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False


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
        self.assertEqual(result['page_no'], '1')

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

    async def test_request_http_error_logging_omits_raw_response_body(self):
        client = DartClient()
        client.api_key = 'test-secret-key'
        client.base_url = 'https://dart.example.test'
        response = FakeHTTPErrorResponse(
            status_code=400,
            text='echoed crtfc_key=test-secret-key',
        )

        with (
            patch('app.integrations.dart.client.httpx.AsyncClient', FakeAsyncClient),
            patch('app.integrations.dart.client.request_with_audit', return_value=response),
            patch('app.integrations.dart.client.log_fetch_event') as log_fetch_event,
        ):
            with self.assertRaises(DartAPIError):
                await client._request('/company.json', {'corp_code': '00126380'})

        self.assertEqual(log_fetch_event.call_args.kwargs['operation'], 'request_http')
        self.assertEqual(log_fetch_event.call_args.kwargs['status'], 'failed')
        self.assertNotIn('test-secret-key', log_fetch_event.call_args.kwargs['error_message'])
        self.assertIn('body omitted', log_fetch_event.call_args.kwargs['error_message'])


if __name__ == '__main__':
    unittest.main()
