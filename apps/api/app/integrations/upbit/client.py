from __future__ import annotations

from typing import Any

import httpx

from app.core.audit_logging import log_fetch_event, request_with_audit
from app.core.settings import settings


class UpbitQuotationAPIError(RuntimeError):
    pass


class UpbitQuotationClient:
    """Read-only client for Upbit public quotation APIs."""

    def __init__(self) -> None:
        self.base_url = settings.upbit_base_url.rstrip('/')
        self.timeout_seconds = settings.upbit_request_timeout_seconds

    async def _get(self, path: str, params: dict[str, Any] | None = None) -> Any:
        url = f'{self.base_url}/v1/{path.lstrip("/")}'
        headers = {'Accept': 'application/json'}
        try:
            async with httpx.AsyncClient(timeout=self.timeout_seconds) as client:
                response = await request_with_audit(
                    client,
                    'upbit',
                    'GET',
                    url,
                    headers=headers,
                    params=params,
                )
        except httpx.HTTPError as exc:
            raise UpbitQuotationAPIError(f'Upbit transport error: {exc}') from exc

        if response.status_code >= 400:
            raise UpbitQuotationAPIError(f'Upbit quotation request failed [{response.status_code}]: {response.text}')

        try:
            return response.json()
        except ValueError as exc:
            raise UpbitQuotationAPIError('Upbit quotation response was not valid JSON') from exc

    async def fetch_markets(self, *, is_details: bool = True) -> list[dict[str, Any]]:
        payload = await self._get('market/all', params={'is_details': str(is_details).lower()})
        if not isinstance(payload, list):
            log_fetch_event(
                service='upbit',
                operation='fetch_markets',
                status='failed',
                error_message='Upbit market list response was not a JSON list',
            )
            raise UpbitQuotationAPIError('Upbit market list response was not a JSON list')
        log_fetch_event(service='upbit', operation='fetch_markets', status='completed', record_count=len(payload))
        return payload

    async def fetch_tickers(self, *, markets: list[str]) -> list[dict[str, Any]]:
        payload = await self._get('ticker', params={'markets': ','.join(markets)})
        if not isinstance(payload, list):
            log_fetch_event(
                service='upbit',
                operation='fetch_tickers',
                status='failed',
                markets=markets,
                error_message='Upbit ticker response was not a JSON list',
            )
            raise UpbitQuotationAPIError('Upbit ticker response was not a JSON list')
        log_fetch_event(
            service='upbit',
            operation='fetch_tickers',
            status='completed',
            markets=markets,
            record_count=len(payload),
        )
        return payload

    async def fetch_day_candles(
        self,
        *,
        market: str,
        count: int,
        to: str | None = None,
    ) -> list[dict[str, Any]]:
        params: dict[str, Any] = {'market': market, 'count': count}
        if to:
            params['to'] = to
        payload = await self._get('candles/days', params=params)
        return self._validate_candles_payload(payload, operation='fetch_day_candles', market=market)

    async def fetch_minute_candles(
        self,
        *,
        market: str,
        unit: int,
        count: int,
        to: str | None = None,
    ) -> list[dict[str, Any]]:
        params: dict[str, Any] = {'market': market, 'count': count}
        if to:
            params['to'] = to
        payload = await self._get(f'candles/minutes/{unit}', params=params)
        return self._validate_candles_payload(payload, operation='fetch_minute_candles', market=market)

    @staticmethod
    def _validate_candles_payload(
        payload: Any,
        *,
        operation: str,
        market: str,
    ) -> list[dict[str, Any]]:
        if not isinstance(payload, list):
            log_fetch_event(
                service='upbit',
                operation=operation,
                status='failed',
                market=market,
                error_message='Upbit candle response was not a JSON list',
            )
            raise UpbitQuotationAPIError('Upbit candle response was not a JSON list')
        log_fetch_event(
            service='upbit',
            operation=operation,
            status='completed',
            market=market,
            record_count=len(payload),
        )
        return payload
