from __future__ import annotations

import httpx

from app.core.settings import settings


class KoreaEximClient:
    def __init__(self) -> None:
        self.base_url = settings.koreaexim_base_url.rstrip('/')

    async def fetch_exchange_rates(self, search_date: str | None = None, data: str = 'AP01') -> dict:
        params = {
            'authkey': settings.koreaexim_api_key,
            'data': data,
        }
        if search_date:
            params['searchdate'] = search_date
        async with httpx.AsyncClient(timeout=20.0) as client:
            response = await client.get(self.base_url, params=params)
            if response.status_code >= 400:
                raise RuntimeError(f'Korea Exim request failed [{response.status_code}]: {response.text}')
            return {'items': response.json()}
