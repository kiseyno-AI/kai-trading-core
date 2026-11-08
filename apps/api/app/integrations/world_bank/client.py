from __future__ import annotations

from typing import Any

import httpx

from app.core.audit_logging import exception_message, log_fetch_event, request_read_with_retry_and_audit
from app.core.settings import settings


class WorldBankAPIError(RuntimeError):
    pass


class WorldBankClient:
    def __init__(self, *, transport: httpx.AsyncBaseTransport | None = None) -> None:
        self.base_url = settings.macro_context_world_bank_base_url.rstrip('/')
        self.timeout_seconds = float(settings.macro_context_request_timeout_seconds or 15.0)
        self.transport = transport

    async def fetch_indicator(
        self,
        *,
        country: str,
        indicator: str,
        mrnev: int = 1,
    ) -> dict[str, Any]:
        normalized_country = str(country or '').strip().upper()
        normalized_indicator = str(indicator or '').strip()
        if not normalized_country:
            raise ValueError('World Bank country is required')
        if not normalized_indicator:
            raise ValueError('World Bank indicator is required')

        url = f'{self.base_url}/country/{normalized_country}/indicator/{normalized_indicator}'
        params = {
            'format': 'json',
            'mrnev': str(max(1, int(mrnev or 1))),
            'per_page': str(max(1, int(mrnev or 1))),
        }
        try:
            async with httpx.AsyncClient(timeout=self.timeout_seconds, transport=self.transport) as client:
                response = await request_read_with_retry_and_audit(
                    client,
                    'world_bank',
                    'GET',
                    url,
                    params=params,
                )
        except httpx.HTTPError as exc:
            raise WorldBankAPIError(f'World Bank transport error: {exception_message(exc)}') from exc
        if response.status_code >= 400:
            raise WorldBankAPIError(f'World Bank request failed [{response.status_code}]: {response.text}')

        try:
            payload = response.json()
        except ValueError as exc:
            raise WorldBankAPIError(
                'World Bank response JSON decode failed'
            ) from exc
        if not isinstance(payload, list) or len(payload) < 2:
            raise WorldBankAPIError('World Bank response was not the expected JSON list')
        rows = payload[1]
        if rows is None:
            rows = []
        if not isinstance(rows, list):
            raise WorldBankAPIError('World Bank indicator rows were not a JSON list')
        log_fetch_event(
            service='world_bank',
            operation='fetch_indicator',
            status='completed',
            country=normalized_country,
            indicator=normalized_indicator,
            row_count=len(rows),
        )
        return {
            'metadata': payload[0] if isinstance(payload[0], dict) else {},
            'rows': rows,
            'raw': payload,
        }
