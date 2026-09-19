from __future__ import annotations

import os
import threading
from contextlib import contextmanager
from types import ModuleType
from typing import Any, Iterator

_PYKRX_IMPORT_LOCK = threading.RLock()
_PYKRX_STOCK_MODULE: ModuleType | None = None
_PYKRX_WEBIO_MODULE: ModuleType | None = None


@contextmanager
def suppress_pykrx_login_env() -> Iterator[None]:
    """Import PyKRX without making application startup depend on KRX login.

    PyKRX 1.2.x attempts a KRX data-site login at import time whenever KRX_ID
    and KRX_PW are present. KRX occasionally returns a non-JSON login response
    that can make the import fail before fallback logic can run. Provider calls
    must keep the credentials visible so PyKRX can establish its session lazily.
    """

    with _PYKRX_IMPORT_LOCK:
        saved = {name: os.environ.get(name) for name in ("KRX_ID", "KRX_PW")}
        for name in saved:
            os.environ.pop(name, None)
        try:
            yield
        finally:
            for name, value in saved.items():
                if value is None:
                    os.environ.pop(name, None)
                else:
                    os.environ[name] = value


@contextmanager
def pykrx_request_credentials_visible() -> Iterator[None]:
    """Serialize provider calls against the import-time env suppression window."""

    with _PYKRX_IMPORT_LOCK:
        yield


@contextmanager
def pykrx_request_context(*, timeout_seconds: int | float) -> Iterator[None]:
    """Serialize PyKRX calls and bound its authenticated/anonymous sessions.

    PyKRX 1.2.x performs data calls through both newly-created
    ``requests.Session`` objects and its authenticated session.  Replace only
    the two names resolved inside PyKRX's ``webio`` module so unrelated HTTP
    clients and concurrent instrumentation remain untouched.
    """

    with _PYKRX_IMPORT_LOCK:
        if timeout_seconds <= 0:
            yield
            return

        webio = import_pykrx_webio()
        original_requests = webio.requests
        original_get_session = getattr(webio, 'get_session', None)

        class SessionWithDefaultTimeout:
            def __init__(self, session: Any):
                self._session = session

            def request(self, method: str, url: str, **kwargs: Any):
                kwargs.setdefault('timeout', timeout_seconds)
                return self._session.request(method, url, **kwargs)

            def get(self, url: str, **kwargs: Any):
                kwargs.setdefault('timeout', timeout_seconds)
                return self._session.get(url, **kwargs)

            def post(self, url: str, **kwargs: Any):
                kwargs.setdefault('timeout', timeout_seconds)
                return self._session.post(url, **kwargs)

            def __getattr__(self, name: str) -> Any:
                return getattr(self._session, name)

        class RequestsWithDefaultTimeout:
            def Session(self, *args: Any, **kwargs: Any):
                return SessionWithDefaultTimeout(
                    original_requests.Session(*args, **kwargs)
                )

            def get(self, url: str, **kwargs: Any):
                kwargs.setdefault('timeout', timeout_seconds)
                return original_requests.get(url, **kwargs)

            def post(self, url: str, **kwargs: Any):
                kwargs.setdefault('timeout', timeout_seconds)
                return original_requests.post(url, **kwargs)

            def __getattr__(self, name: str) -> Any:
                return getattr(original_requests, name)

        class KrxSessionWithDefaultTimeout:
            def __init__(self, krx_session: Any):
                self._krx_session = krx_session
                self.session = SessionWithDefaultTimeout(krx_session.session)

            def __getattr__(self, name: str) -> Any:
                return getattr(self._krx_session, name)

        requests_with_timeout = RequestsWithDefaultTimeout()

        get_session_with_timeout = None
        if callable(original_get_session):
            def get_session_with_timeout():
                krx_session = original_get_session()
                if krx_session is None:
                    return None
                return KrxSessionWithDefaultTimeout(krx_session)

        webio.requests = requests_with_timeout
        if get_session_with_timeout is not None:
            webio.get_session = get_session_with_timeout
        try:
            yield
        finally:
            if webio.requests is requests_with_timeout:
                webio.requests = original_requests
            if (
                get_session_with_timeout is not None
                and getattr(webio, 'get_session', None) is get_session_with_timeout
            ):
                webio.get_session = original_get_session


def import_pykrx_stock():
    global _PYKRX_STOCK_MODULE
    with _PYKRX_IMPORT_LOCK:
        if _PYKRX_STOCK_MODULE is None:
            with suppress_pykrx_login_env():
                from pykrx import stock as pykrx_stock

            _PYKRX_STOCK_MODULE = pykrx_stock
        return _PYKRX_STOCK_MODULE


def import_pykrx_webio():
    global _PYKRX_WEBIO_MODULE
    with _PYKRX_IMPORT_LOCK:
        if _PYKRX_WEBIO_MODULE is None:
            with suppress_pykrx_login_env():
                from pykrx.website.comm import webio as pykrx_webio

            _PYKRX_WEBIO_MODULE = pykrx_webio
        return _PYKRX_WEBIO_MODULE
