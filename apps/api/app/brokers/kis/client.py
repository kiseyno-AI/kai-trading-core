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

    async def _get_response(self, path: str, *, tr_id: str, params: dict, tr_cont: str = '') -> tuple[dict, str]:
        token = await self.issue_access_token()
        headers = {
            'authorization': f"Bearer {token['access_token']}",
            'appkey': settings.kis_app_key,
            'appsecret': settings.kis_app_secret,
            'tr_id': tr_id,
        }
        if tr_cont:
            headers['tr_cont'] = tr_cont
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
            return response.json(), response.headers.get('tr_cont', '')

    async def _get(self, path: str, *, tr_id: str, params: dict) -> dict:
        body, _ = await self._get_response(path, tr_id=tr_id, params=params)
        return body

    async def _get_paginated_ranking(
        self,
        path: str,
        *,
        tr_id: str,
        params: dict,
        output_keys: tuple[str, ...],
        max_pages: int = 10,
    ) -> dict:
        merged: dict[str, list] = {key: [] for key in output_keys}
        pages: list[dict] = []
        tr_cont = ''
        continuation_pending = False

        for _ in range(max_pages):
            body, next_tr_cont = await self._get_response(path, tr_id=tr_id, params=params, tr_cont=tr_cont)
            pages.append(body)
            for key in output_keys:
                value = body.get(key)
                if isinstance(value, list):
                    merged[key].extend(value)
                elif isinstance(value, dict):
                    merged[key].append(value)

            continuation_pending = next_tr_cont in {'M', 'F'}
            if not continuation_pending:
                break
            tr_cont = 'N'

        return {
            **merged,
            'page_count': len(pages),
            'max_pages_reached': continuation_pending and len(pages) >= max_pages,
            'raw_pages': pages,
        }

    async def fetch_domestic_quote(self, symbol: str, market_code: str = 'J') -> dict:
        return await self._get(
            '/uapi/domestic-stock/v1/quotations/inquire-price',
            tr_id='FHKST01010100',
            params={
                'fid_cond_mrkt_div_code': market_code,
                'fid_input_iscd': symbol,
            },
        )

    async def fetch_domestic_quote_risk_status(self, symbol: str, market_code: str = 'J') -> dict:
        return await self._get(
            '/uapi/domestic-stock/v1/quotations/inquire-price-2',
            tr_id='FHPST01010000',
            params={
                'FID_COND_MRKT_DIV_CODE': market_code,
                'FID_INPUT_ISCD': symbol,
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

    async def fetch_domestic_ranking_volume_power(
        self,
        *,
        market_code: str = 'J',
        input_iscd: str = '0000',
        div_cls_code: str = '0',
        min_price: str = '0',
        max_price: str = '1000000',
        min_volume: str = '0',
        target_cls_code: str = '0',
        target_exclude_cls_code: str = '0',
        max_pages: int = 10,
    ) -> dict:
        return await self._get_paginated_ranking(
            '/uapi/domestic-stock/v1/ranking/volume-power',
            tr_id='FHPST01680000',
            params={
                'fid_trgt_exls_cls_code': target_exclude_cls_code,
                'fid_cond_mrkt_div_code': market_code,
                'fid_cond_scr_div_code': '20168',
                'fid_input_iscd': input_iscd,
                'fid_div_cls_code': div_cls_code,
                'fid_input_price_1': min_price,
                'fid_input_price_2': max_price,
                'fid_vol_cnt': min_volume,
                'fid_trgt_cls_code': target_cls_code,
            },
            output_keys=('output',),
            max_pages=max_pages,
        )

    async def fetch_domestic_ranking_overtime_volume(
        self,
        *,
        market_code: str = 'J',
        input_iscd: str = '0000',
        rank_sort_cls_code: str = '2',
        min_price: str = '',
        max_price: str = '',
        min_volume: str = '',
        target_cls_code: str = '',
        target_exclude_cls_code: str = '',
        max_pages: int = 10,
    ) -> dict:
        return await self._get_paginated_ranking(
            '/uapi/domestic-stock/v1/ranking/overtime-volume',
            tr_id='FHPST02350000',
            params={
                'FID_COND_MRKT_DIV_CODE': market_code,
                'FID_COND_SCR_DIV_CODE': '20235',
                'FID_INPUT_ISCD': input_iscd,
                'FID_RANK_SORT_CLS_CODE': rank_sort_cls_code,
                'FID_INPUT_PRICE_1': min_price,
                'FID_INPUT_PRICE_2': max_price,
                'FID_VOL_CNT': min_volume,
                'FID_TRGT_CLS_CODE': target_cls_code,
                'FID_TRGT_EXLS_CLS_CODE': target_exclude_cls_code,
            },
            output_keys=('output1', 'output2'),
            max_pages=max_pages,
        )

    async def fetch_domestic_ranking_overtime_fluctuation(
        self,
        *,
        market_code: str = 'J',
        market_cls_code: str = '',
        input_iscd: str = '0000',
        div_cls_code: str = '2',
        min_price: str = '',
        max_price: str = '',
        min_volume: str = '',
        target_cls_code: str = '',
        target_exclude_cls_code: str = '',
        max_pages: int = 10,
    ) -> dict:
        return await self._get_paginated_ranking(
            '/uapi/domestic-stock/v1/ranking/overtime-fluctuation',
            tr_id='FHPST02340000',
            params={
                'FID_COND_MRKT_DIV_CODE': market_code,
                'FID_MRKT_CLS_CODE': market_cls_code,
                'FID_COND_SCR_DIV_CODE': '20234',
                'FID_INPUT_ISCD': input_iscd,
                'FID_DIV_CLS_CODE': div_cls_code,
                'FID_INPUT_PRICE_1': min_price,
                'FID_INPUT_PRICE_2': max_price,
                'FID_VOL_CNT': min_volume,
                'FID_TRGT_CLS_CODE': target_cls_code,
                'FID_TRGT_EXLS_CLS_CODE': target_exclude_cls_code,
            },
            output_keys=('output1', 'output2'),
            max_pages=max_pages,
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
