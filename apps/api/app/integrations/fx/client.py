from __future__ import annotations

from typing import Any

import httpx

from app.core.audit_logging import log_fetch_event, request_with_audit
from app.core.settings import settings


class KoreaEximConfigurationError(RuntimeError):
    pass


class KoreaEximAPIError(RuntimeError):
    pass


class KoreaEximClient:
    def __init__(self) -> None:
        self.base_url = settings.koreaexim_base_url.rstrip('/')

    async def _get(self, params: dict[str, str]) -> httpx.Response:
        async with httpx.AsyncClient(timeout=20.0) as client:
            return await request_with_audit(
                client,
                'koreaexim',
                'GET',
                self.base_url,
                params=params,
            )

    async def fetch_exchange_rates(self, *, search_date: str, data: str = 'AP01') -> list[dict[str, Any]]:
        if not settings.koreaexim_api_key:
            raise KoreaEximConfigurationError('KOREAEXIM_API_KEY is not configured')

        params = {
            'authkey': settings.koreaexim_api_key,
            'searchdate': search_date,
            'data': data,
        }
        try:
            response = await self._get(params)
        except httpx.HTTPError as exc:
            raise KoreaEximAPIError(f'Korea Exim transport error: {exc}') from exc
        if response.status_code >= 400:
            raise KoreaEximAPIError(f'Korea Exim request failed [{response.status_code}]: {response.text}')

        payload = response.json()
        if not isinstance(payload, list):
            log_fetch_event(
                service='koreaexim',
                operation='fetch_exchange_rates',
                status='failed',
                search_date=search_date,
                data=data,
                error_message='Korea Exim response was not a JSON list',
            )
            raise KoreaEximAPIError('Korea Exim response was not a JSON list')
        if payload and str(payload[0].get('result')) not in {'1', ''}:
            result_code = payload[0].get('result')
            log_fetch_event(
                service='koreaexim',
                operation='fetch_exchange_rates',
                status='failed',
                search_date=search_date,
                data=data,
                result_code=result_code,
                error_message=f'Korea Exim returned result={result_code}',
            )
            raise KoreaEximAPIError(f'Korea Exim returned result={result_code}')
        log_fetch_event(
            service='koreaexim',
            operation='fetch_exchange_rates',
            status='completed',
            search_date=search_date,
            data=data,
            record_count=len(payload),
        )
        return payload
