from __future__ import annotations

import httpx

from app.core.settings import settings


class KISClient:
    def __init__(self) -> None:
        self.base_url = settings.kis_mock_base_url if settings.kis_use_mock else settings.kis_base_url

    async def issue_access_token(self) -> dict:
        payload = {
            'grant_type': 'client_credentials',
            'appkey': settings.kis_app_key,
            'appsecret': settings.kis_app_secret,
        }
        async with httpx.AsyncClient(base_url=self.base_url, timeout=15.0) as client:
            response = await client.post('/oauth2/tokenP', json=payload)
            if response.status_code >= 400:
                raise RuntimeError(f'KIS token issue failed [{response.status_code}]: {response.text}')
            return response.json()

    async def healthcheck(self) -> dict:
        return {
            'broker': 'kis',
            'base_url': self.base_url,
            'use_mock': settings.kis_use_mock,
            'app_key_configured': bool(settings.kis_app_key),
            'app_secret_configured': bool(settings.kis_app_secret),
        }

    async def fetch_domestic_quote(self, symbol: str, market_code: str = 'J') -> dict:
        token = await self.issue_access_token()
        headers = {
            'authorization': f"Bearer {token['access_token']}",
            'appkey': settings.kis_app_key,
            'appsecret': settings.kis_app_secret,
            'tr_id': 'FHKST01010100',
        }
        params = {
            'fid_cond_mrkt_div_code': market_code,
            'fid_input_iscd': symbol,
        }
        async with httpx.AsyncClient(base_url=self.base_url, timeout=15.0) as client:
            response = await client.get('/uapi/domestic-stock/v1/quotations/inquire-price', headers=headers, params=params)
            if response.status_code >= 400:
                raise RuntimeError(f'KIS quote failed [{response.status_code}]: {response.text}')
            return response.json()
