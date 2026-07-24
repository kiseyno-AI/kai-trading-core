from __future__ import annotations

import os
from contextlib import contextmanager
from typing import Iterator


@contextmanager
def suppress_pykrx_login_env() -> Iterator[None]:
    """Import/use PyKRX without triggering optional KRX website login.

    PyKRX 1.2.x attempts a KRX data-site login at import time whenever KRX_ID
    and KRX_PW are present.  The public daily stock endpoints used by this
    pipeline do not require that login, and KRX occasionally returns a non-JSON
    login response that makes importing pykrx fail before fallback logic can run.
    """

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


def import_pykrx_stock():
    with suppress_pykrx_login_env():
        from pykrx import stock as pykrx_stock

    return pykrx_stock


def import_pykrx_webio():
    with suppress_pykrx_login_env():
        from pykrx.website.comm import webio as pykrx_webio

    return pykrx_webio
