from __future__ import annotations

import httpx

from app.core.settings import settings


class BOKClient:
    def __init__(self) -> None:
        self.base_url = settings.bok_base_url.rstrip('/')

    async def fetch_series(self, series_id: str, start_period: str, end_period: str, cycle: str = 'D') -> dict:
        url = (
            f"{self.base_url}/{settings.bok_api_key}/json/kr/1/100/"
            f"StatisticSearch/{series_id}/{cycle}/{start_period}/{end_period}"
        )
        async with httpx.AsyncClient(timeout=20.0) as client:
            response = await client.get(url)
            if response.status_code >= 400:
                raise RuntimeError(f'BOK request failed [{response.status_code}]: {response.text}')
            return response.json()
