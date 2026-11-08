from __future__ import annotations

from typing import Any

import httpx

from app.core.audit_logging import exception_message, log_fetch_event, request_read_with_retry_and_audit
from app.core.settings import settings


class FREDConfigurationError(RuntimeError):
    pass


class FREDAPIError(RuntimeError):
    pass


class FREDClient:
    def __init__(self, *, transport: httpx.AsyncBaseTransport | None = None) -> None:
        self.base_url = settings.fred_base_url.rstrip('/')
        self.timeout_seconds = float(settings.macro_context_request_timeout_seconds or 15.0)
        self.transport = transport

    async def fetch_latest_observations(
        self,
        *,
        series_id: str,
        limit: int = 1,
    ) -> dict[str, Any]:
        api_key = settings.fred_api_key.strip()
        if not api_key:
            raise FREDConfigurationError('FRED_API_KEY is not configured')
        normalized_series_id = str(series_id or '').strip()
        if not normalized_series_id:
            raise ValueError('FRED series_id is required')

        url = f'{self.base_url}/fred/series/observations'
        params = {
            'series_id': normalized_series_id,
            'api_key': api_key,
            'file_type': 'json',
            'sort_order': 'desc',
            'limit': str(max(1, int(limit or 1))),
        }
        try:
            async with httpx.AsyncClient(timeout=self.timeout_seconds, transport=self.transport) as client:
                response = await request_read_with_retry_and_audit(
                    client,
                    'fred',
                    'GET',
                    url,
                    params=params,
                    secret_values=[api_key],
                )
        except httpx.HTTPError as exc:
            raise FREDAPIError(f'FRED transport error: {exception_message(exc)}') from exc
        if response.status_code >= 400:
            raise FREDAPIError(f'FRED request failed [{response.status_code}]: {response.text}')

        try:
            payload = response.json()
        except ValueError as exc:
            raise FREDAPIError('FRED response JSON decode failed') from exc
        observations = payload.get('observations') if isinstance(payload, dict) else None
        if not isinstance(observations, list):
            raise FREDAPIError('FRED observations response was not a JSON list')
        log_fetch_event(
            service='fred',
            operation='fetch_latest_observations',
            status='completed',
            series_id=normalized_series_id,
            row_count=len(observations),
        )
        return {
            'observations': observations,
            'raw': payload,
        }

    async def fetch_observation_history(
        self,
        *,
        series_id: str,
        observation_start: str,
        initial_release: bool,
    ) -> list[dict[str, Any]]:
        """All observations since ``observation_start`` (oldest first).

        ``initial_release`` asks ALFRED for first-release values, whose
        ``realtime_start`` is the publication date. FRED refuses that for
        series with more than 2,000 vintages; callers fall back to latest
        values.
        """

        api_key = settings.fred_api_key.strip()
        if not api_key:
            raise FREDConfigurationError('FRED_API_KEY is not configured')
        params = {
            'series_id': series_id,
            'api_key': api_key,
            'file_type': 'json',
            'observation_start': observation_start,
            'sort_order': 'asc',
            'limit': '100000',
        }
        if initial_release:
            params.update({'realtime_start': '1776-07-04', 'realtime_end': '9999-12-31', 'output_type': '4'})
        try:
            async with httpx.AsyncClient(timeout=max(self.timeout_seconds, 60.0), transport=self.transport) as client:
                response = await request_read_with_retry_and_audit(
                    client, 'fred', 'GET', f'{self.base_url}/fred/series/observations',
                    params=params, secret_values=[api_key],
                )
        except httpx.HTTPError as exc:
            raise FREDAPIError(f'FRED transport error: {exception_message(exc)}') from exc
        if response.status_code >= 400:
            raise FREDAPIError(f'FRED request failed [{response.status_code}]: {response.text[:300]}')
        try:
            observations = response.json().get('observations')
        except (ValueError, AttributeError) as exc:
            raise FREDAPIError('FRED response JSON decode failed') from exc
        if not isinstance(observations, list):
            raise FREDAPIError('FRED observations response was not a JSON list')
        return observations

