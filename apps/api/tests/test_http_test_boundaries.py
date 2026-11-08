import asyncio

import httpx
import pytest
import requests

from conftest import _install_http_boundary_guard


@pytest.fixture
def recorded_http_boundaries(monkeypatch):
    calls = []

    def request(self, method, url, *args, **kwargs):
        calls.append(('requests', method, str(url)))
        return 'recorded'

    def handle_request(self, request):
        calls.append(('httpx', request.method, str(request.url)))
        return httpx.Response(200)

    async def handle_async_request(self, request):
        calls.append(('httpx_async', request.method, str(request.url)))
        return httpx.Response(200)

    # Even if a guard assertion fails, these originals cannot send traffic.
    monkeypatch.setattr(requests.sessions.Session, 'request', request)
    monkeypatch.setattr(httpx.HTTPTransport, 'handle_request', handle_request)
    monkeypatch.setattr(httpx.AsyncHTTPTransport, 'handle_async_request', handle_async_request)
    _install_http_boundary_guard(monkeypatch)
    return calls


def test_external_http_fails_before_reaching_any_network_boundary(recorded_http_boundaries):
    url = 'https://provider.invalid/data'
    with pytest.raises(pytest.fail.Exception, match='Unmocked external HTTP'):
        requests.Session().get(url)
    with httpx.HTTPTransport() as transport:
        with pytest.raises(pytest.fail.Exception, match='Unmocked external HTTP'):
            transport.handle_request(httpx.Request('GET', url))

    async def request_async():
        async with httpx.AsyncHTTPTransport() as transport:
            with pytest.raises(pytest.fail.Exception, match='Unmocked external HTTP'):
                await transport.handle_async_request(httpx.Request('GET', url))

    asyncio.run(request_async())
    assert recorded_http_boundaries == []


@pytest.mark.parametrize('url', [
    'http://localhost/data',
    'http://127.0.0.1:8080/data',
    'http://[::1]/data',
])
def test_loopback_http_keeps_the_original_boundary(url, recorded_http_boundaries):
    assert requests.Session().get(url) == 'recorded'
    with httpx.HTTPTransport() as transport:
        assert transport.handle_request(httpx.Request('GET', url)).status_code == 200

    async def request_async():
        async with httpx.AsyncHTTPTransport() as transport:
            return await transport.handle_async_request(httpx.Request('GET', url))

    assert asyncio.run(request_async()).status_code == 200
    assert recorded_http_boundaries == [
        ('requests', 'GET', url),
        ('httpx', 'GET', url),
        ('httpx_async', 'GET', url),
    ]


def test_mock_and_asgi_transports_remain_available():
    def handler(request):
        return httpx.Response(200, json={'mocked': True})

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        assert client.get('https://provider.invalid/data').json() == {'mocked': True}

    async def app(scope, receive, send):
        await send({'type': 'http.response.start', 'status': 204, 'headers': []})
        await send({'type': 'http.response.body', 'body': b''})

    async def request_async():
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            assert (await client.get('https://provider.invalid/data')).json() == {'mocked': True}
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app)) as client:
            assert (await client.get('http://test-app/data')).status_code == 204

    asyncio.run(request_async())
