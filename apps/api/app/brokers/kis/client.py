from __future__ import annotations

from datetime import datetime, timedelta, timezone

import httpx

from app.core.settings import settings


class KISClient:
    _token_cache: dict[str, object] = {
        'access_token': '',
        'expires_at': datetime.min.replace(tzinfo=timezone.utc),
    }

    def __init__(self) -> None:
        self.base_url = settings.kis_mock_base_url if settings.kis_use_mock else settings.kis_base_url

    async def issue_access_token(self) -> dict:
        now = datetime.now(timezone.utc)
        cached_token = str(self._token_cache.get('access_token') or '')
        expires_at = self._token_cache.get('expires_at')
        if cached_token and isinstance(expires_at, datetime) and now < expires_at:
            return {
                'access_token': cached_token,
                'expires_in': int((expires_at - now).total_seconds()),
                'token_type': 'Bearer',
                'cached': True,
            }

        payload = {
            'grant_type': 'client_credentials',
            'appkey': settings.kis_app_key,
            'appsecret': settings.kis_app_secret,
        }
        async with httpx.AsyncClient(base_url=self.base_url, timeout=15.0) as client:
            response = await client.post('/oauth2/tokenP', json=payload)
            if response.status_code >= 400:
                raise RuntimeError(f'KIS token issue failed [{response.status_code}]: {response.text}')
            token = response.json()

        expires_in = int(token.get('expires_in', 0) or 0)
        refresh_margin = 60
        expires_at = now + timedelta(seconds=max(0, expires_in - refresh_margin))
        self._token_cache = {
            'access_token': token.get('access_token', ''),
            'expires_at': expires_at,
        }
        return token

    async def healthcheck(self) -> dict:
        return {
            'broker': 'kis',
            'base_url': self.base_url,
            'use_mock': settings.kis_use_mock,
            'app_key_configured': bool(settings.kis_app_key),
            'app_secret_configured': bool(settings.kis_app_secret),
        }

    async def _get(self, path: str, *, tr_id: str, params: dict) -> dict:
        token = await self.issue_access_token()
        headers = {
            'authorization': f"Bearer {token['access_token']}",
            'appkey': settings.kis_app_key,
            'appsecret': settings.kis_app_secret,
            'tr_id': tr_id,
        }
        async with httpx.AsyncClient(base_url=self.base_url, timeout=15.0) as client:
            response = await client.get(path, headers=headers, params=params)
            if response.status_code >= 400:
                raise RuntimeError(f'KIS request failed [{response.status_code}] {path}: {response.text}')
            return response.json()

    async def fetch_domestic_quote(self, symbol: str, market_code: str = 'J') -> dict:
        return await self._get(
            '/uapi/domestic-stock/v1/quotations/inquire-price',
            tr_id='FHKST01010100',
            params={
                'fid_cond_mrkt_div_code': market_code,
                'fid_input_iscd': symbol,
            },
        )

    async def fetch_domestic_stock_info(self, symbol: str, product_type_code: str = '300') -> dict:
        return await self._get(
            '/uapi/domestic-stock/v1/quotations/search-stock-info',
            tr_id='CTPF1604R',
            params={
                'PDNO': symbol,
                'PRDT_TYPE_CD': product_type_code,
            },
        )

    async def fetch_domestic_product_info(self, symbol: str, product_type_code: str = '300') -> dict:
        return await self._get(
            '/uapi/domestic-stock/v1/quotations/search-info',
            tr_id='CTPF1604R',
            params={
                'PDNO': symbol,
                'PRDT_TYPE_CD': product_type_code,
            },
        )
