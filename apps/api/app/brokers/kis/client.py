from __future__ import annotations

from datetime import datetime, timedelta, timezone

import httpx

from app.core.audit_logging import request_with_audit
from app.core.settings import settings


class KISClient:
    _token_cache: dict[str, object] = {
        'access_token': '',
        'expires_at': datetime.min.replace(tzinfo=timezone.utc),
        'token_type': 'Bearer',
    }

    def __init__(self) -> None:
        self.base_url = settings.kis_mock_base_url if settings.kis_use_mock else settings.kis_base_url

    def _get_cached_token(self) -> dict | None:
        now = datetime.now(timezone.utc)
        cached_token = str(self._token_cache.get('access_token') or '')
        expires_at = self._token_cache.get('expires_at')
        token_type = str(self._token_cache.get('token_type') or 'Bearer')
        if cached_token and isinstance(expires_at, datetime) and now < expires_at:
            return {
                'access_token': cached_token,
                'expires_in': int((expires_at - now).total_seconds()),
                'token_type': token_type,
                'cached': True,
            }
        return None

    async def issue_access_token(self, force_refresh: bool = False) -> dict:
        if not force_refresh:
            cached = self._get_cached_token()
            if cached:
                return cached

        payload = {
            'grant_type': 'client_credentials',
            'appkey': settings.kis_app_key,
            'appsecret': settings.kis_app_secret,
        }
        async with httpx.AsyncClient(base_url=self.base_url, timeout=15.0) as client:
            response = await request_with_audit(
                client,
                'kis',
                'POST',
                '/oauth2/tokenP',
                json_body=payload,
            )
            if response.status_code >= 400:
                raise RuntimeError(f'KIS token issue failed [{response.status_code}]: {response.text}')
            token = response.json()

        now = datetime.now(timezone.utc)
        expires_in = int(token.get('expires_in', 0) or 0)
        refresh_margin = max(0, settings.kis_token_refresh_margin_seconds)
        expires_at = now + timedelta(seconds=max(0, expires_in - refresh_margin))
        self._token_cache = {
            'access_token': token.get('access_token', ''),
            'expires_at': expires_at,
            'token_type': token.get('token_type', 'Bearer'),
        }
        token['cached'] = False
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
            response = await request_with_audit(
                client,
                'kis',
                'GET',
                path,
                headers=headers,
                params=params,
            )
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

    async def fetch_domestic_daily_chart(
        self,
        symbol: str,
        *,
        market_code: str = 'J',
        start_date: str,
        end_date: str,
        period_code: str = 'D',
        adjusted_price: str = '0',
    ) -> dict:
        return await self._get(
            '/uapi/domestic-stock/v1/quotations/inquire-daily-itemchartprice',
            tr_id='FHKST03010100',
            params={
                'FID_COND_MRKT_DIV_CODE': market_code,
                'FID_INPUT_ISCD': symbol,
                'FID_INPUT_DATE_1': start_date,
                'FID_INPUT_DATE_2': end_date,
                'FID_PERIOD_DIV_CODE': period_code,
                'FID_ORG_ADJ_PRC': adjusted_price,
            },
        )

    async def fetch_domestic_volume_rank(self, market_code: str = 'J', input_iscd: str = '0000') -> dict:
        return await self._get(
            '/uapi/domestic-stock/v1/quotations/volume-rank',
            tr_id='FHPST01710000',
            params={
                'FID_COND_MRKT_DIV_CODE': market_code,
                'FID_COND_SCR_DIV_CODE': '20171',
                'FID_INPUT_ISCD': input_iscd,
                'FID_DIV_CLS_CODE': '0',
                'FID_BLNG_CLS_CODE': '0',
                'FID_TRGT_CLS_CODE': '111111111',
                'FID_TRGT_EXLS_CLS_CODE': '000000',
                'FID_INPUT_PRICE_1': '0',
                'FID_INPUT_PRICE_2': '0',
                'FID_VOL_CNT': '0',
                'FID_INPUT_DATE_1': '0',
            },
        )

    async def fetch_domestic_fluctuation_rank(self, market_code: str = 'J', count: int = 20) -> dict:
        return await self._get(
            '/uapi/domestic-stock/v1/ranking/fluctuation',
            tr_id='FHPST01700000',
            params={
                'fid_rsfl_rate2': '',
                'fid_cond_mrkt_div_code': market_code,
                'fid_cond_scr_div_code': '20170',
                'fid_input_iscd': '0000',
                'fid_rank_sort_cls_code': '0',
                'fid_input_cnt_1': str(count),
                'fid_prc_cls_code': '0',
                'fid_input_price_1': '',
                'fid_input_price_2': '',
                'fid_vol_cnt': '',
                'fid_trgt_cls_code': '0',
                'fid_trgt_exls_cls_code': '0',
                'fid_div_cls_code': '0',
                'fid_rsfl_rate1': '',
            },
        )

    async def fetch_domestic_hts_top_view(self) -> dict:
        return await self._get(
            '/uapi/domestic-stock/v1/ranking/hts-top-view',
            tr_id='HHMCM000100C0',
            params={},
        )
