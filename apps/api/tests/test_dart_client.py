import unittest
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import AsyncMock, patch

import httpx

from app.core.settings import settings
from app.integrations.dart.client import DartAPIError, DartClient


class FakeResponse:
    def __init__(self, payload, *, content=b'', headers=None):
        self._payload = payload
        self.status_code = 200
        self.text = ''
        self.content = content
        self.headers = headers or {}

    def json(self):
        return self._payload


class TestableDartClient(DartClient):
    __test__ = False

    def __init__(self, payload, *, content=b'', headers=None):
        super().__init__()
        self.payload = payload
        self.content = content
        self.headers = headers or {}

    async def _request(self, path, params):
        return FakeResponse(self.payload, content=self.content, headers=self.headers)


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
    _CORP_CODE_XML = (
        b'<?xml version="1.0" encoding="UTF-8"?>'
        b'<result><list><corp_code>00126380</corp_code><corp_name>Samsung Electronics</corp_name>'
        b'<stock_code>005930</stock_code><modify_date>20260408</modify_date></list></result>'
    )

    def setUp(self):
        self._cache_setting_patch = patch.object(settings, 'dart_corp_code_cache_file', '')
        self._cache_setting_patch.start()
        self.addCleanup(self._cache_setting_patch.stop)

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

    async def test_download_document_returns_binary_payload_with_header_filename(self):
        client = TestableDartClient(
            {'status': '000'},
            content=b'PK\x03\x04document',
            headers={
                'content-type': 'application/zip',
                'content-disposition': 'attachment; filename="original.zip"',
            },
        )

        result = await client.download_document(rcept_no='20260407000001')

        self.assertEqual(result['filename'], 'original.zip')
        self.assertEqual(result['content_type'], 'application/zip')
        self.assertEqual(result['size_bytes'], len(b'PK\x03\x04document'))

    async def test_download_xbrl_uses_default_filename_when_header_missing(self):
        client = TestableDartClient(
            {'status': '000'},
            content=b'PK\x03\x04xbrl',
            headers={'content-type': 'application/octet-stream'},
        )

        result = await client.download_xbrl(
            rcept_no='20260407000001',
            reprt_code='11011',
        )

        self.assertEqual(result['filename'], 'dart-xbrl-20260407000001-11011.zip')
        self.assertEqual(result['size_bytes'], len(b'PK\x03\x04xbrl'))

    async def test_download_document_raises_for_json_error_envelope(self):
        client = TestableDartClient(
            {'status': '014', 'message': 'File does not exist'},
            content=b'{"status":"014","message":"File does not exist"}',
            headers={'content-type': 'application/json'},
        )

        with self.assertRaisesRegex(DartAPIError, '014'):
            await client.download_document(rcept_no='20260407000001')

    async def test_download_xbrl_raises_for_xml_error_envelope(self):
        client = TestableDartClient(
            {'status': '014', 'message': 'File does not exist'},
            content=b'<result><status>014</status><message>File does not exist</message></result>',
            headers={'content-type': 'application/xml'},
        )

        with self.assertRaisesRegex(DartAPIError, '014'):
            await client.download_xbrl(rcept_no='20260407000001', reprt_code='11011')

    async def test_lookup_corp_code_raises_upstream_xml_error_for_corp_archive(self):
        client = TestableDartClient(
            {'status': '020', 'message': 'Rate limit'},
            content=b'<?xml version="1.0" encoding="UTF-8"?><result><status>020</status><message>Rate limit</message></result>',
            headers={'content-type': 'application/xml'},
        )

        with self.assertRaisesRegex(DartAPIError, '020'):
            await client.lookup_corp_code(stock_code='005930')

    async def test_lookup_corp_code_accepts_direct_xml_without_zip_wrapper(self):
        client = TestableDartClient(
            {'status': '000'},
            content=self._CORP_CODE_XML,
            headers={'content-type': 'application/xml'},
        )

        corp = await client.lookup_corp_code(stock_code='005930')

        self.assertEqual(corp['corp_code'], '00126380')
        self.assertEqual(corp['corp_name'], 'Samsung Electronics')

    async def test_successful_corp_code_fetch_persists_last_known_good_payload(self):
        with TemporaryDirectory() as directory:
            cache_file = Path(directory) / 'dart' / 'corp-codes.xml'
            client = TestableDartClient(
                {'status': '000'},
                content=self._CORP_CODE_XML,
                headers={'content-type': 'application/xml'},
            )

            with patch.object(settings, 'dart_corp_code_cache_file', str(cache_file)):
                corp = await client.lookup_corp_code(stock_code='005930')

            self.assertEqual(corp['corp_code'], '00126380')
            self.assertEqual(cache_file.read_bytes(), self._CORP_CODE_XML)
            self.assertEqual(cache_file.stat().st_mode & 0o777, 0o600)

    async def test_corp_code_fetch_uses_persisted_payload_when_upstream_is_unavailable(self):
        with TemporaryDirectory() as directory:
            cache_file = Path(directory) / 'corp-codes.xml'
            cache_file.write_bytes(self._CORP_CODE_XML)
            client = TestableDartClient(
                {'status': '800', 'message': 'Maintenance'},
                content=b'<result><status>800</status><message>Maintenance</message></result>',
                headers={'content-type': 'application/xml'},
            )

            with patch.object(settings, 'dart_corp_code_cache_file', str(cache_file)):
                corp = await client.lookup_corp_code(stock_code='005930')

            self.assertEqual(corp['corp_code'], '00126380')

    async def test_fresh_persisted_corp_list_can_prove_missing_mapping(self):
        with TemporaryDirectory() as directory:
            cache_file = Path(directory) / 'corp-codes.xml'
            cache_file.write_bytes(self._CORP_CODE_XML)
            client = TestableDartClient(
                {'status': '800', 'message': 'Maintenance'},
                content=(
                    b'<result><status>800</status>'
                    b'<message>Maintenance</message></result>'
                ),
                headers={'content-type': 'application/xml'},
            )

            with patch.object(
                settings,
                'dart_corp_code_cache_file',
                str(cache_file),
            ):
                result = await client.lookup_corp_codes_for_stocks(
                    stock_codes=['005930', '999999'],
                )

            self.assertEqual(
                [item['stock_code'] for item in result['resolved']],
                ['005930'],
            )
            self.assertEqual(result['unsupported_stock_codes'], ['999999'])
            self.assertEqual(result['conflicting_mappings'], [])
            self.assertTrue(result['mapping_authority']['authoritative'])
            self.assertEqual(
                result['mapping_authority']['source'],
                'persistent_fallback',
            )

    async def test_stale_persisted_corp_list_keeps_all_mappings_retryable(self):
        with TemporaryDirectory() as directory:
            cache_file = Path(directory) / 'corp-codes.xml'
            cache_file.write_bytes(self._CORP_CODE_XML)
            stale_at = datetime.now(timezone.utc) - timedelta(
                seconds=settings.dart_corp_code_cache_ttl_seconds + 60
            )
            os.utime(
                cache_file,
                (stale_at.timestamp(), stale_at.timestamp()),
            )
            client = TestableDartClient(
                {'status': '800', 'message': 'Maintenance'},
                content=(
                    b'<result><status>800</status>'
                    b'<message>Maintenance</message></result>'
                ),
                headers={'content-type': 'application/xml'},
            )

            with patch.object(
                settings,
                'dart_corp_code_cache_file',
                str(cache_file),
            ):
                result = await client.lookup_corp_codes_for_stocks(
                    stock_codes=['005930', '999999'],
                )

            self.assertEqual(result['resolved'], [])
            self.assertEqual(result['unsupported_stock_codes'], [])
            self.assertEqual(
                result['conflicting_mappings'],
                [
                    {
                        'stock_code': '005930',
                        'corp_codes': ['00126380'],
                        'reason': (
                            'stock_mapping_resolved_from_non_authoritative_corp_list'
                        ),
                    },
                    {
                        'stock_code': '999999',
                        'corp_codes': [],
                        'reason': (
                            'stock_mapping_absent_from_non_authoritative_corp_list'
                        ),
                    }
                ],
            )
            self.assertFalse(result['mapping_authority']['authoritative'])

    async def test_invalid_persisted_corp_code_payload_does_not_mask_upstream_error(self):
        with TemporaryDirectory() as directory:
            cache_file = Path(directory) / 'corp-codes.xml'
            cache_file.write_bytes(b'not-a-valid-corp-code-payload')
            client = TestableDartClient(
                {'status': '800', 'message': 'Maintenance'},
                content=b'<result><status>800</status><message>Maintenance</message></result>',
                headers={'content-type': 'application/xml'},
            )

            with patch.object(settings, 'dart_corp_code_cache_file', str(cache_file)):
                with self.assertRaisesRegex(DartAPIError, '800'):
                    await client.lookup_corp_code(stock_code='005930')

    async def test_lookup_corp_code_invalid_payload_reports_diagnostics(self):
        client = TestableDartClient(
            {'status': '000'},
            content=b'not-a-zip-payload',
            headers={'content-type': 'text/plain'},
        )

        with self.assertRaisesRegex(DartAPIError, 'content_type=text/plain'):
            await client.lookup_corp_code(stock_code='005930')

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
            patch(
                'app.integrations.dart.client.request_read_with_retry_and_audit',
                return_value=response,
            ),
            patch('app.integrations.dart.client.log_fetch_event') as log_fetch_event,
        ):
            with self.assertRaises(DartAPIError):
                await client._request('/company.json', {'corp_code': '00126380'})

        self.assertEqual(log_fetch_event.call_args.kwargs['operation'], 'request_http')
        self.assertEqual(log_fetch_event.call_args.kwargs['status'], 'failed')
        self.assertNotIn('test-secret-key', log_fetch_event.call_args.kwargs['error_message'])
        self.assertIn('body omitted', log_fetch_event.call_args.kwargs['error_message'])

    async def test_request_retries_transient_transport_failure(self):
        attempts = 0

        def handler(request: httpx.Request) -> httpx.Response:
            nonlocal attempts
            attempts += 1
            if attempts == 1:
                raise httpx.ReadTimeout('', request=request)
            return httpx.Response(200, json={'status': '000'}, request=request)

        client = DartClient(transport=httpx.MockTransport(handler))
        client.api_key = 'test-secret-key'
        client.base_url = 'https://dart.example.test'

        with patch(
            'app.core.audit_logging.asyncio.sleep',
            new_callable=AsyncMock,
        ):
            response = await client._request(
                '/company.json',
                {'corp_code': '00126380'},
            )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(attempts, 2)

    async def test_request_retries_transient_http_status(self):
        attempts = 0

        def handler(request: httpx.Request) -> httpx.Response:
            nonlocal attempts
            attempts += 1
            if attempts == 1:
                return httpx.Response(503, request=request)
            return httpx.Response(200, json={'status': '000'}, request=request)

        client = DartClient(transport=httpx.MockTransport(handler))
        client.api_key = 'test-secret-key'
        client.base_url = 'https://dart.example.test'

        with patch(
            'app.core.audit_logging.asyncio.sleep',
            new_callable=AsyncMock,
        ):
            response = await client._request(
                '/company.json',
                {'corp_code': '00126380'},
            )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(attempts, 2)

    async def test_request_stops_after_one_same_call_retry(self):
        attempts = 0

        def handler(request: httpx.Request) -> httpx.Response:
            nonlocal attempts
            attempts += 1
            raise httpx.ReadTimeout('', request=request)

        client = DartClient(transport=httpx.MockTransport(handler))
        client.api_key = 'test-secret-key'
        client.base_url = 'https://dart.example.test'

        with (
            patch(
                'app.core.audit_logging.asyncio.sleep',
                new_callable=AsyncMock,
            ),
            self.assertRaises(httpx.ReadTimeout),
        ):
            await client._request('/company.json', {'corp_code': '00126380'})

        self.assertEqual(attempts, 2)

    async def test_request_does_not_retry_nontransient_client_error(self):
        attempts = 0

        def handler(request: httpx.Request) -> httpx.Response:
            nonlocal attempts
            attempts += 1
            return httpx.Response(400, text='bad request', request=request)

        client = DartClient(transport=httpx.MockTransport(handler))
        client.api_key = 'test-secret-key'
        client.base_url = 'https://dart.example.test'

        with self.assertRaisesRegex(DartAPIError, r'\[400\]'):
            await client._request('/company.json', {'corp_code': '00126380'})

        self.assertEqual(attempts, 1)


if __name__ == '__main__':
    unittest.main()
