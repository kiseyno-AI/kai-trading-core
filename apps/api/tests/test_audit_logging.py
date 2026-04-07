import json
import logging
import tempfile
import unittest
from unittest.mock import patch

import httpx

from app.core import audit_logging
from app.core.settings import settings


class AuditLoggingTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.original_settings = {
            'api_request_logging_enabled': settings.api_request_logging_enabled,
            'upstream_http_logging_enabled': settings.upstream_http_logging_enabled,
            'api_log_directory': settings.api_log_directory,
            'api_request_log_file_name': settings.api_request_log_file_name,
            'upstream_http_log_file_name': settings.upstream_http_log_file_name,
            'upstream_bearer_token': settings.upstream_bearer_token,
        }
        self.temp_dir = tempfile.TemporaryDirectory()
        settings.api_log_directory = self.temp_dir.name
        settings.api_request_logging_enabled = True
        settings.upstream_http_logging_enabled = True
        settings.api_request_log_file_name = 'api-requests.log'
        settings.upstream_http_log_file_name = 'upstream-http.log'
        settings.upstream_bearer_token = None
        audit_logging.reset_logging_configuration_for_tests()
        audit_logging.configure_file_logging(force=True)

    def tearDown(self):
        for key, value in self.original_settings.items():
            setattr(settings, key, value)
        audit_logging.reset_logging_configuration_for_tests()
        audit_logging.configure_file_logging(force=True)
        self.temp_dir.cleanup()

    def _read_log_lines(self, filename: str) -> list[dict]:
        for logger_name in (audit_logging.API_AUDIT_LOGGER_NAME, audit_logging.UPSTREAM_AUDIT_LOGGER_NAME):
            for handler in logging.getLogger(logger_name).handlers:
                handler.flush()
        path = audit_logging.get_log_directory() / filename
        if not path.exists():
            return []
        return [json.loads(line) for line in path.read_text(encoding='utf-8').splitlines() if line.strip()]

    async def test_request_logging_writes_redacted_api_log_line(self):
        event = audit_logging.build_api_request_event(
            request_id='req-test-1',
            method='GET',
            path='/health',
            query_params={'foo': 'bar', 'access_token': 'top-secret'},
            client_ip='127.0.0.1',
            user_agent='audit-test',
            status_code=200,
            duration_ms=12.5,
            request_headers={
                'Authorization': 'Bearer local-secret',
                'User-Agent': 'audit-test',
                'X-Request-ID': 'req-test-1',
            },
            response_headers={'content-type': 'application/json', 'content-length': '42'},
        )
        audit_logging.log_api_request_event(event)

        event = self._read_log_lines('api-requests.log')[-1]
        self.assertEqual(event['event'], 'api_request')
        self.assertEqual(event['request_id'], 'req-test-1')
        self.assertEqual(event['path'], '/health')
        self.assertEqual(event['query_params']['foo'], 'bar')
        self.assertEqual(event['query_params']['access_token'], '[REDACTED]')
        self.assertEqual(event['request_headers']['authorization'], '[REDACTED]')
        self.assertEqual(event['status_code'], 200)

    async def test_upstream_logging_redacts_query_params_and_propagates_request_id(self):
        seen_request_headers = {}

        def handler(request: httpx.Request) -> httpx.Response:
            seen_request_headers.update(request.headers)
            return httpx.Response(
                200,
                headers={'content-type': 'application/json', 'content-length': '2'},
                json={},
                request=request,
            )

        token = audit_logging.set_request_id('req-upstream-1')
        try:
            transport = httpx.MockTransport(handler)
            async with httpx.AsyncClient(transport=transport) as client:
                response = await audit_logging.request_with_audit(
                    client,
                    'test_service',
                    'GET',
                    'https://example.test/resource',
                    params={'authkey': 'secret', 'symbol': '005930'},
                    headers={'Authorization': 'Bearer upstream-secret'},
                )
        finally:
            audit_logging.reset_request_id(token)

        self.assertEqual(response.status_code, 200)
        self.assertEqual(seen_request_headers['x-request-id'], 'req-upstream-1')

        event = self._read_log_lines('upstream-http.log')[-1]
        self.assertEqual(event['service'], 'test_service')
        self.assertIn('authkey=%5BREDACTED%5D', event['url'])
        self.assertIn('symbol=005930', event['url'])
        self.assertEqual(event['request_headers']['authorization'], '[REDACTED]')
        self.assertEqual(event['request_headers']['x-request-id'], 'req-upstream-1')
        self.assertEqual(event['status_code'], 200)

    async def test_upstream_logging_redacts_secret_values_embedded_in_path(self):
        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(200, json={}, request=request)

        transport = httpx.MockTransport(handler)
        async with httpx.AsyncClient(transport=transport) as client:
            response = await audit_logging.request_with_audit(
                client,
                'bok_ecos',
                'GET',
                'https://ecos.example.test/api/StatisticSearch/test-key/json/kr/1/100/STAT123/M/202401/202403',
                secret_values=['test-key'],
            )

        self.assertEqual(response.status_code, 200)

        event = self._read_log_lines('upstream-http.log')[-1]
        self.assertIn('/StatisticSearch/[REDACTED]/json/', event['url'])

    async def test_download_logging_records_non_httpx_upstream_calls(self):
        class FakeResponse:
            status = 200
            headers = {'content-type': 'application/zip', 'content-length': '4'}

            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

            def read(self):
                return b'data'

        with patch('app.core.audit_logging.urlopen', return_value=FakeResponse()):
            payload = audit_logging.download_bytes_with_audit(
                service='kis_master',
                url='https://example.test/master.zip?token=secret',
                timeout=30,
            )

        self.assertEqual(payload, b'data')
        event = self._read_log_lines('upstream-http.log')[-1]
        self.assertEqual(event['service'], 'kis_master')
        self.assertEqual(event['status_code'], 200)
        self.assertIn('token=%5BREDACTED%5D', event['url'])

    async def test_fetch_operation_logging_redacts_and_truncates_metadata(self):
        audit_logging.log_fetch_event(
            service='pykrx',
            operation='fetch_symbol_ohlcv',
            status='failed',
            symbol='005930',
            auth_token='secret',
            failed_symbols=[str(index).zfill(6) for index in range(30)],
            error_message='x' * 400,
        )

        event = self._read_log_lines('upstream-http.log')[-1]
        self.assertEqual(event['event'], 'fetch_operation')
        self.assertEqual(event['service'], 'pykrx')
        self.assertEqual(event['operation'], 'fetch_symbol_ohlcv')
        self.assertEqual(event['status'], 'failed')
        self.assertEqual(event['auth_token'], '[REDACTED]')
        self.assertEqual(len(event['failed_symbols']), 26)
        self.assertTrue(event['failed_symbols'][-1].startswith('... ('))
        self.assertTrue(event['error_message'].endswith('...'))


if __name__ == '__main__':
    unittest.main()
