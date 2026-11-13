import json
import logging
import tempfile
import unittest
from unittest.mock import AsyncMock, patch

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

    async def test_kis_http_error_logs_provider_code_without_response_body(self):
        body = {'rt_cd': '1', 'msg_cd': 'EGW00201', 'msg1': 'private response text'}
        transport = httpx.MockTransport(lambda _: httpx.Response(500, json=body))
        async with httpx.AsyncClient(transport=transport) as client:
            response = await audit_logging.request_with_audit(
                client, 'kis', 'GET', 'https://example.test/inquire-balance',
            )

        event = self._read_log_lines('upstream-http.log')[-1]
        self.assertEqual(event['kis_msg_cd'], 'EGW00201')
        self.assertNotIn('private response text', json.dumps(event))
        self.assertEqual(response.json(), body)

    async def test_upstream_audit_omits_untrusted_or_unrelated_kis_codes(self):
        cases = (
            ('kis', 500, b'not json'),
            ('kis', 500, b'[]'),
            ('kis', 500, b'{"msg_cd":"12345678"}'),
            ('kis', 500, b'{"msg_cd":"SECR1234"}'),
            ('kis', 200, b'{"msg_cd":"EGW00201"}'),
            ('other', 500, b'{"msg_cd":"EGW00201"}'),
        )
        for service, status, body in cases:
            with self.subTest(service=service, status=status, body=body):
                transport = httpx.MockTransport(lambda _: httpx.Response(status, content=body))
                async with httpx.AsyncClient(transport=transport) as client:
                    response = await audit_logging.request_with_audit(
                        client, service, 'GET', 'https://example.test/inquire-balance',
                        headers={'Authorization': 'Bearer SECR1234'},
                    )
                event = self._read_log_lines('upstream-http.log')[-1]
                self.assertNotIn('kis_msg_cd', event)
                self.assertNotIn('SECR1234', json.dumps(event))
                self.assertEqual(response.content, body)

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
        self.assertIn('/StatisticSearch/[REDACTED]/json/', event['path'])
        self.assertNotIn('test-key', json.dumps(event))

    async def test_upstream_logging_redacts_url_encoded_path_credentials(self):
        for encoded in ('private%2Fkey', 'private%2fkey'):
            with self.subTest(encoded=encoded):
                transport = httpx.MockTransport(
                    lambda request: httpx.Response(200, json={}, request=request)
                )
                async with httpx.AsyncClient(transport=transport) as client:
                    response = await audit_logging.request_with_audit(
                        client, 'test_service', 'GET',
                        f'https://example.test/api/{encoded}/data?symbol=005930',
                        secret_values=['private/key'],
                    )
                self.assertEqual(response.status_code, 200)
                event = self._read_log_lines('upstream-http.log')[-1]
                self.assertNotIn(encoded, json.dumps(event))
                self.assertNotIn('private/key', json.dumps(event))
                self.assertEqual(event['path'], '/api/[REDACTED]/data')
                self.assertIn('symbol=005930', event['url'])

    async def test_read_retry_redacts_credentials_in_exception_audit_fields(self):
        attempts = 0

        def handler(request: httpx.Request) -> httpx.Response:
            nonlocal attempts
            attempts += 1
            if attempts == 1:
                raise httpx.ConnectError(
                    f"cannot reach {request.url}; auth={request.headers['authorization']}",
                    request=request,
                )
            return httpx.Response(200, json={'ok': True}, request=request)

        with patch('app.core.audit_logging.asyncio.sleep', new_callable=AsyncMock):
            async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
                response = await audit_logging.request_read_with_retry_and_audit(
                    client, 'test_service', 'GET',
                    'https://example.test/api/private%2Fkey/data',
                    params={'authkey': 'query-secret', 'symbol': '005930'},
                    headers={'Authorization': 'Bearer header-secret'},
                    secret_values=iter(['private/key']),
                )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(attempts, 2)
        events = self._read_log_lines('upstream-http.log')
        for event in events:
            for secret in ('private/key', 'private%2Fkey', 'query-secret', 'header-secret'):
                self.assertNotIn(secret, json.dumps(event))
        errors = [event for event in events if event.get('error_message')]
        self.assertEqual(len(errors), 2)
        self.assertTrue(all('symbol=005930' in event['error_message'] for event in errors))

    async def test_download_redacts_credentials_in_exception_audit_fields(self):
        url = 'https://example.test/master.zip?token=download-secret&symbol=005930'
        failure = OSError(f'cannot download {url}')
        with patch('app.core.audit_logging.urlopen', side_effect=failure):
            with self.assertRaises(OSError) as raised:
                audit_logging.download_bytes_with_audit(
                    service='test_service', url=url, timeout=1,
                )

        self.assertIs(raised.exception, failure)
        event = self._read_log_lines('upstream-http.log')[-1]
        self.assertNotIn('download-secret', json.dumps(event))
        self.assertIn('symbol=005930', event['error_message'])

    async def test_read_retry_retries_transport_failure_and_audits_each_attempt(self):
        attempts = 0

        def handler(request: httpx.Request) -> httpx.Response:
            nonlocal attempts
            attempts += 1
            if attempts == 1:
                raise httpx.ReadTimeout('', request=request)
            return httpx.Response(200, json={'ok': True}, request=request)

        transport = httpx.MockTransport(handler)
        with patch('app.core.audit_logging.asyncio.sleep', new_callable=AsyncMock) as sleep:
            async with httpx.AsyncClient(transport=transport) as client:
                response = await audit_logging.request_read_with_retry_and_audit(
                    client,
                    'test_service',
                    'GET',
                    'https://example.test/resource',
                )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(attempts, 2)
        sleep.assert_awaited_once_with(0.25)
        request_events = [
            event
            for event in self._read_log_lines('upstream-http.log')
            if event['event'] == 'upstream_request'
        ]
        self.assertEqual(len(request_events), 2)
        self.assertEqual(request_events[0]['error_type'], 'ReadTimeout')
        self.assertEqual(request_events[0]['error_message'], 'ReadTimeout')
        self.assertEqual(request_events[1]['status_code'], 200)

    async def test_read_retry_bounds_retry_after_and_retries_transient_status(self):
        attempts = 0

        def handler(request: httpx.Request) -> httpx.Response:
            nonlocal attempts
            attempts += 1
            if attempts == 1:
                return httpx.Response(
                    429,
                    headers={'Retry-After': '120'},
                    request=request,
                )
            return httpx.Response(200, json={'ok': True}, request=request)

        transport = httpx.MockTransport(handler)
        with patch('app.core.audit_logging.asyncio.sleep', new_callable=AsyncMock) as sleep:
            async with httpx.AsyncClient(transport=transport) as client:
                response = await audit_logging.request_read_with_retry_and_audit(
                    client,
                    'test_service',
                    'HEAD',
                    'https://example.test/resource',
                )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(attempts, 2)
        sleep.assert_awaited_once_with(2.0)

    async def test_read_retry_does_not_retry_other_client_errors(self):
        attempts = 0

        def handler(request: httpx.Request) -> httpx.Response:
            nonlocal attempts
            attempts += 1
            return httpx.Response(401, request=request)

        transport = httpx.MockTransport(handler)
        with patch('app.core.audit_logging.asyncio.sleep', new_callable=AsyncMock) as sleep:
            async with httpx.AsyncClient(transport=transport) as client:
                response = await audit_logging.request_read_with_retry_and_audit(
                    client,
                    'test_service',
                    'GET',
                    'https://example.test/resource',
                )

        self.assertEqual(response.status_code, 401)
        self.assertEqual(attempts, 1)
        sleep.assert_not_awaited()

    async def test_read_retry_rejects_non_idempotent_methods_before_request(self):
        attempts = 0

        def handler(request: httpx.Request) -> httpx.Response:
            nonlocal attempts
            attempts += 1
            return httpx.Response(200, request=request)

        transport = httpx.MockTransport(handler)
        async with httpx.AsyncClient(transport=transport) as client:
            with self.assertRaisesRegex(ValueError, 'GET and HEAD'):
                await audit_logging.request_read_with_retry_and_audit(
                    client,
                    'test_service',
                    'POST',
                    'https://example.test/resource',
                )

        self.assertEqual(attempts, 0)

    async def test_read_retry_enforces_hard_attempt_limit(self):
        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(200, request=request)

        transport = httpx.MockTransport(handler)
        async with httpx.AsyncClient(transport=transport) as client:
            with self.assertRaisesRegex(ValueError, 'between 1 and 3'):
                await audit_logging.request_read_with_retry_and_audit(
                    client,
                    'test_service',
                    'GET',
                    'https://example.test/resource',
                    max_attempts=4,
                )

    async def test_read_retry_exhausts_after_three_transport_attempts(self):
        attempts = 0

        def handler(request: httpx.Request) -> httpx.Response:
            nonlocal attempts
            attempts += 1
            raise httpx.ConnectTimeout('', request=request)

        transport = httpx.MockTransport(handler)
        with patch('app.core.audit_logging.asyncio.sleep', new_callable=AsyncMock) as sleep:
            async with httpx.AsyncClient(transport=transport) as client:
                with self.assertRaises(httpx.ConnectTimeout):
                    await audit_logging.request_read_with_retry_and_audit(
                        client,
                        'test_service',
                        'GET',
                        'https://example.test/resource',
                    )

        self.assertEqual(attempts, 3)
        self.assertEqual(sleep.await_count, 2)

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

    async def test_fetch_operation_logging_bounds_nested_mappings_without_hiding_diagnostics(
        self,
    ):
        nested = {f'field_{index:03d}': index for index in range(60)}
        nested.update(
            {
                'error_message': 'provider response was incomplete',
                'pipeline_run_id': 596,
                'symbols_failed': 3,
            }
        )

        audit_logging.log_fetch_event(
            service='pykrx',
            operation='run_daily_pipeline_if_due',
            status='failed',
            result=nested,
        )

        event = self._read_log_lines('upstream-http.log')[-1]
        result = event['result']
        self.assertLessEqual(len(result), 26)
        self.assertEqual(
            result['error_message'],
            'provider response was incomplete',
        )
        self.assertEqual(result['pipeline_run_id'], 596)
        self.assertEqual(result['symbols_failed'], 3)
        self.assertEqual(result['_omitted_field_count'], 38)


if __name__ == '__main__':
    unittest.main()
