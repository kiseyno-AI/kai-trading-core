from __future__ import annotations

import httpx

from app.core.audit_logging import log_fetch_event, request_with_audit
from app.core.settings import settings
from app.us_equities.universe import USListedSecurity, parse_nasdaq_listed, parse_other_listed


class NasdaqTraderError(RuntimeError):
    pass


class NasdaqTraderClient:
    def __init__(
        self,
        *,
        base_url: str | None = None,
        timeout_seconds: float | None = None,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        self.base_url = (base_url or settings.nasdaq_trader_base_url).rstrip('/')
        self.timeout_seconds = timeout_seconds or settings.nasdaq_trader_request_timeout_seconds
        self._transport = transport

    async def fetch_universe(self) -> list[USListedSecurity]:
        nasdaq_payload = await self._get_text('/dynamic/SymDir/nasdaqlisted.txt')
        other_payload = await self._get_text('/dynamic/SymDir/otherlisted.txt')
        rows = [*parse_nasdaq_listed(nasdaq_payload), *parse_other_listed(other_payload)]
        log_fetch_event(
            service='nasdaq_trader',
            operation='fetch_universe',
            status='completed',
            row_count=len(rows),
        )
        return sorted(rows, key=lambda item: (item.symbol, item.exchange, item.name))

    async def _get_text(self, path: str) -> str:
        url = f'{self.base_url}{path}'
        async with httpx.AsyncClient(timeout=self.timeout_seconds, transport=self._transport) as client:
            response = await request_with_audit(
                client,
                'nasdaq_trader',
                'GET',
                url,
                headers={'Accept': 'text/plain'},
            )
        if response.status_code >= 400:
            raise NasdaqTraderError(f'Nasdaq Trader request failed [{response.status_code}] for {path}')
        return response.text
