from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import httpx

from app.core.settings import settings


@dataclass(slots=True)
class InternalAPIClient:
    """Authenticated localhost client for cases where internal HTTP is still required."""

    base_url: str = 'http://127.0.0.1:8000'
    bearer_token: str | None = settings.upstream_bearer_token
    timeout_seconds: float = 30.0
    transport: httpx.BaseTransport | httpx.AsyncBaseTransport | None = None

    def _headers(self) -> dict[str, str]:
        headers = {'Accept': 'application/json'}
        if self.bearer_token:
            headers['Authorization'] = f'Bearer {self.bearer_token}'
        return headers

    async def _request(self, method: str, path: str, *, params: dict[str, Any] | None = None) -> dict[str, Any]:
        async with httpx.AsyncClient(
            base_url=self.base_url,
            timeout=self.timeout_seconds,
            headers=self._headers(),
            transport=self.transport,
        ) as client:
            response = await client.request(method, path, params=params)
            response.raise_for_status()
            return response.json()

    async def get_market_status(self) -> dict[str, Any]:
        return await self._request('GET', '/market/status')

    async def get_tracked_quotes(self) -> dict[str, Any]:
        return await self._request('GET', '/quotes/tracked')

    async def get_tracked_symbols(self) -> dict[str, Any]:
        return await self._request('GET', '/scanner/tracked-symbols')

    async def get_eod_status(self) -> dict[str, Any]:
        return await self._request('GET', '/eod/status')

    async def run_pykrx_daily_if_due(
        self,
        *,
        market_date: str | None = None,
        markets: str | None = None,
        force: bool = False,
        refresh_universe: bool | None = None,
    ) -> dict[str, Any]:
        params: dict[str, Any] = {}
        if market_date:
            params['market_date'] = market_date
        if markets:
            params['markets'] = markets
        if force:
            params['force'] = 'true'
        if refresh_universe is not None:
            params['refresh_universe'] = 'true' if refresh_universe else 'false'
        return await self._request('POST', '/eod/pykrx/daily/run-if-due', params=params or None)
