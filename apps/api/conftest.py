"""Repository-wide pytest safety gates loaded before application test modules."""

import os
import tempfile
from ipaddress import ip_address
from urllib.parse import urlsplit

import httpx
import pytest
import requests

# Keep the daily-pipeline live feed (snapshot + JSONL) out of the real
# apps/api/var while tests drive the run-state service.
_LIVE_DIR = tempfile.mkdtemp(prefix='kai-live-progress-tests-')
os.environ.setdefault('DAILY_PIPELINE_LIVE_DIR', _LIVE_DIR)
os.environ.setdefault('DAILY_PIPELINE_LIVE_LOG', os.path.join(_LIVE_DIR, 'live.jsonl'))

from app.core.settings import settings  # noqa: E402
from app.core.test_database_safety import assert_safe_test_database_settings  # noqa: E402


assert_safe_test_database_settings(settings)


def _require_loopback_http(url) -> None:
    hostname = None
    try:
        hostname = urlsplit(str(url)).hostname
        if hostname == 'localhost' or ip_address(hostname).is_loopback:
            return
    except ValueError:
        pass
    pytest.fail(f'Unmocked external HTTP request in test: {hostname or "<invalid host>"}')


def _install_http_boundary_guard(monkeypatch) -> None:
    request = requests.sessions.Session.request
    handle_request = httpx.HTTPTransport.handle_request
    handle_async_request = httpx.AsyncHTTPTransport.handle_async_request

    def guarded_request(self, method, url, *args, **kwargs):
        _require_loopback_http(url)
        return request(self, method, url, *args, **kwargs)

    def guarded_handle_request(self, request):
        _require_loopback_http(request.url)
        return handle_request(self, request)

    async def guarded_handle_async_request(self, request):
        _require_loopback_http(request.url)
        return await handle_async_request(self, request)

    monkeypatch.setattr(requests.sessions.Session, 'request', guarded_request)
    monkeypatch.setattr(httpx.HTTPTransport, 'handle_request', guarded_handle_request)
    monkeypatch.setattr(httpx.AsyncHTTPTransport, 'handle_async_request', guarded_handle_async_request)


@pytest.fixture(autouse=True)
def block_unmocked_external_http(monkeypatch):
    # Patch only real network transports: MockTransport, ASGITransport, and
    # explicit provider mocks stay usable. Database connections are untouched.
    _install_http_boundary_guard(monkeypatch)
