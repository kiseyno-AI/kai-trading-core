from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any, Mapping

import httpx

from app.core.audit_logging import request_with_audit
from app.core.settings import settings


class KISClientError(RuntimeError):
    def __init__(
        self,
        message: str,
        *,
        payload: dict[str, Any] | None = None,
        status_code: int | None = None,
        retryable: bool = False,
        ambiguous_submission: bool = False,
    ) -> None:
        super().__init__(message)
        self.payload = payload or {}
        self.status_code = status_code
        self.retryable = retryable
        self.ambiguous_submission = ambiguous_submission


class KISConfigurationError(KISClientError):
    pass


class KISTimeoutError(KISClientError):
    pass


class KISTransportError(KISClientError):
    pass


class KISHTTPError(KISClientError):
    pass


class KISAPIError(KISClientError):
    def __init__(
        self,
        message: str,
        *,
        payload: dict[str, Any] | None = None,
        status_code: int | None = None,
        msg_code: str | None = None,
        result_code: str | None = None,
        retryable: bool = False,
        ambiguous_submission: bool = False,
    ) -> None:
        super().__init__(
            message,
            payload=payload,
            status_code=status_code,
            retryable=retryable,
            ambiguous_submission=ambiguous_submission,
        )
        self.msg_code = msg_code
        self.result_code = result_code


def _safe_json(response: httpx.Response) -> dict[str, Any]:
    try:
        payload = response.json()
    except ValueError:
        return {}
    return payload if isinstance(payload, dict) else {}


class KISClient:
    def __init__(self, *, use_mock: bool | None = None) -> None:
        self.use_mock = settings.kis_use_mock if use_mock is None else bool(use_mock)
        self.base_url = settings.kis_mock_base_url if self.use_mock else settings.kis_base_url
        self._token_cache: dict[str, object] = {
            'access_token': '',
            'expires_at': datetime.min.replace(tzinfo=timezone.utc),
            'token_type': 'Bearer',
        }

    def _environment(self) -> str:
        return 'demo' if self.use_mock else 'real'

    def websocket_url(self) -> str:
        return settings.kis_realtime_mock_ws_url if self.use_mock else settings.kis_realtime_ws_url

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

    def _resolve_tr_id(self, *, real: str, demo: str | None = None) -> str:
        if self._environment() == 'demo' and demo:
            return demo
        return real

    async def issue_access_token(self, force_refresh: bool = False) -> dict:
        if not settings.kis_app_key or not settings.kis_app_secret:
            raise KISConfigurationError('KIS_APP_KEY and KIS_APP_SECRET must be configured')

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
            try:
                response = await request_with_audit(
                    client,
                    'kis',
                    'POST',
                    '/oauth2/tokenP',
                    json_body=payload,
                )
            except httpx.TimeoutException as exc:
                raise KISTimeoutError('KIS token issue timed out', retryable=True) from exc
            except httpx.HTTPError as exc:
                raise KISTransportError('KIS token issue transport failure', retryable=True) from exc
            if response.status_code >= 400:
                error_payload = _safe_json(response)
                message = error_payload.get('msg1') or response.text
                raise KISHTTPError(
                    f'KIS token issue failed [{response.status_code}]: {message}',
                    payload=error_payload,
                    status_code=response.status_code,
                    retryable=response.status_code >= 500,
                )
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
            'websocket_url': self.websocket_url(),
            'environment': self._environment(),
            'use_mock': self.use_mock,
            'app_key_configured': bool(settings.kis_app_key),
            'app_secret_configured': bool(settings.kis_app_secret),
            'hts_id_configured': bool(settings.kis_hts_id),
            'realtime_order_notice_enabled': settings.kis_realtime_order_notice_enabled,
            'realtime_price_enabled': settings.kis_realtime_price_enabled,
        }

    async def issue_approval_key(self) -> dict[str, Any]:
        if not settings.kis_app_key or not settings.kis_app_secret:
            raise KISConfigurationError('KIS_APP_KEY and KIS_APP_SECRET must be configured')

        payload = {
            'grant_type': 'client_credentials',
            'appkey': settings.kis_app_key,
            'secretkey': settings.kis_app_secret,
        }
        async with httpx.AsyncClient(base_url=self.base_url, timeout=15.0) as client:
            try:
                response = await request_with_audit(
                    client,
                    'kis',
                    'POST',
                    '/oauth2/Approval',
                    json_body=payload,
                )
            except httpx.TimeoutException as exc:
                raise KISTimeoutError('KIS approval key issue timed out', retryable=True) from exc
            except httpx.HTTPError as exc:
                raise KISTransportError('KIS approval key transport failure', retryable=True) from exc
            if response.status_code >= 400:
                error_payload = _safe_json(response)
                message = error_payload.get('msg1') or response.text
                raise KISHTTPError(
                    f'KIS approval key issue failed [{response.status_code}]: {message}',
                    payload=error_payload,
                    status_code=response.status_code,
                    retryable=response.status_code >= 500,
                )
            approval_payload = _safe_json(response)

        approval_key = str(approval_payload.get('approval_key') or '').strip()
        if not approval_key:
            raise KISAPIError(
                'KIS approval key response did not include approval_key',
                payload=approval_payload,
            )
        return {
            'approval_key': approval_key,
            'cached': False,
        }

    async def _issue_hashkey(self, *, headers: Mapping[str, str], payload: Mapping[str, Any]) -> str | None:
        async with httpx.AsyncClient(base_url=self.base_url, timeout=15.0) as client:
            try:
                response = await request_with_audit(
                    client,
                    'kis',
                    'POST',
                    '/uapi/hashkey',
                    headers=headers,
                    json_body=dict(payload),
                )
            except httpx.TimeoutException as exc:
                raise KISTimeoutError('KIS hashkey request timed out', retryable=True) from exc
            except httpx.HTTPError as exc:
                raise KISTransportError('KIS hashkey transport failure', retryable=True) from exc
            if response.status_code >= 400:
                error_payload = _safe_json(response)
                message = error_payload.get('msg1') or response.text
                raise KISHTTPError(
                    f'KIS hashkey request failed [{response.status_code}]: {message}',
                    payload=error_payload,
                    status_code=response.status_code,
                    retryable=response.status_code >= 500,
                )
            hash_payload = _safe_json(response)
            return str(hash_payload.get('HASH') or '').strip() or None

    async def _request(
        self,
        method: str,
        path: str,
        *,
        tr_id: str,
        params: Mapping[str, Any] | None = None,
        json_body: Mapping[str, Any] | None = None,
        tr_cont: str = '',
        use_hashkey: bool = False,
    ) -> tuple[dict[str, Any], str]:
        token = await self.issue_access_token()
        headers = {
            'authorization': f"Bearer {token['access_token']}",
            'appkey': settings.kis_app_key,
            'appsecret': settings.kis_app_secret,
            'tr_id': tr_id,
            'custtype': 'P',
        }
        if tr_cont:
            headers['tr_cont'] = tr_cont
        if use_hashkey and json_body:
            hashkey = await self._issue_hashkey(headers=headers, payload=json_body)
            if hashkey:
                headers['hashkey'] = hashkey

        async with httpx.AsyncClient(base_url=self.base_url, timeout=15.0) as client:
            try:
                response = await request_with_audit(
                    client,
                    'kis',
                    method.upper(),
                    path,
                    headers=headers,
                    params=dict(params or {}),
                    json_body=dict(json_body) if json_body is not None else None,
                )
            except httpx.TimeoutException as exc:
                raise KISTimeoutError(
                    f'KIS request timed out for {path}',
                    retryable=True,
                    ambiguous_submission=method.upper() == 'POST',
                ) from exc
            except httpx.HTTPError as exc:
                raise KISTransportError(
                    f'KIS transport failure for {path}',
                    retryable=True,
                    ambiguous_submission=method.upper() == 'POST',
                ) from exc

        if response.status_code >= 400:
            error_payload = _safe_json(response)
            message = error_payload.get('msg1') or response.text
            raise KISHTTPError(
                f'KIS request failed [{response.status_code}] {path}: {message}',
                payload=error_payload,
                status_code=response.status_code,
                retryable=response.status_code >= 500,
                ambiguous_submission=method.upper() == 'POST' and response.status_code >= 500,
            )

        payload = _safe_json(response)
        result_code = str(payload.get('rt_cd') if payload.get('rt_cd') is not None else '').strip()
        if result_code and result_code != '0':
            message = payload.get('msg1') or 'KIS request rejected'
            raise KISAPIError(
                f'KIS request rejected {path}: {message}',
                payload=payload,
                status_code=response.status_code,
                msg_code=str(payload.get('msg_cd') or '').strip() or None,
                result_code=result_code,
            )

        return payload, response.headers.get('tr_cont', '')

    async def _get(self, path: str, *, tr_id: str, params: Mapping[str, Any]) -> dict[str, Any]:
        body, _ = await self._get_response(path, tr_id=tr_id, params=params)
        return body

    async def _get_response(
        self,
        path: str,
        *,
        tr_id: str,
        params: Mapping[str, Any],
        tr_cont: str = '',
    ) -> tuple[dict[str, Any], str]:
        return await self._request('GET', path, tr_id=tr_id, params=params, tr_cont=tr_cont)

    async def _post(self, path: str, *, tr_id: str, body: Mapping[str, Any]) -> dict[str, Any]:
        response_body, _ = await self._request(
            'POST',
            path,
            tr_id=tr_id,
            json_body=body,
            use_hashkey=True,
        )
        return response_body

    async def _get_paginated_ranking(
        self,
        path: str,
        *,
        tr_id: str,
        params: dict[str, Any],
        output_keys: tuple[str, ...],
        max_pages: int = 10,
    ) -> dict:
        merged: dict[str, list] = {key: [] for key in output_keys}
        pages: list[dict[str, Any]] = []
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

    async def fetch_domestic_orderbook(self, symbol: str, market_code: str = 'J') -> dict:
        return await self._get(
            '/uapi/domestic-stock/v1/quotations/inquire-asking-price-exp-ccn',
            tr_id='FHKST01010200',
            params={
                'FID_COND_MRKT_DIV_CODE': market_code,
                'FID_INPUT_ISCD': symbol,
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

    async def fetch_market_index_price(self, *, index_code: str) -> dict:
        return await self._get(
            '/uapi/domestic-stock/v1/quotations/inquire-index-price',
            tr_id='FHPUP02100000',
            params={
                'fid_cond_mrkt_div_code': 'U',
                'fid_input_iscd': index_code,
            },
        )

    async def fetch_investor_time_by_market(self, *, market_code: str, sector_code: str) -> dict:
        return await self._get(
            '/uapi/domestic-stock/v1/quotations/inquire-investor-time-by-market',
            tr_id='FHPTJ04030000',
            params={
                'FID_INPUT_ISCD': market_code,
                'FID_INPUT_ISCD_2': sector_code,
            },
        )

    async def fetch_domestic_program_trade_by_stock(
        self,
        symbol: str,
        *,
        market_code: str = 'J',
    ) -> dict:
        return await self._get(
            '/uapi/domestic-stock/v1/quotations/program-trade-by-stock',
            tr_id='FHPPG04650101',
            params={
                'FID_COND_MRKT_DIV_CODE': market_code,
                'FID_INPUT_ISCD': symbol,
            },
        )

    async def fetch_domestic_investor_trend_estimate(self, symbol: str) -> dict:
        return await self._get(
            '/uapi/domestic-stock/v1/quotations/investor-trend-estimate',
            tr_id='HHPTJ04160200',
            params={
                'MKSC_SHRN_ISCD': symbol,
            },
        )

    async def fetch_domestic_news_titles(
        self,
        *,
        symbol: str = '',
        news_offer_enterprise_code: str = '',
        market_class_code: str = '',
        title_contains: str = '',
        input_date: str = '',
        input_hour: str = '',
        rank_sort_cls_code: str = '',
        input_serial_number: str = '',
        max_pages: int = 10,
    ) -> dict:
        return await self._get_paginated_ranking(
            '/uapi/domestic-stock/v1/quotations/news-title',
            tr_id='FHKST01011800',
            params={
                'FID_NEWS_OFER_ENTP_CODE': news_offer_enterprise_code,
                'FID_COND_MRKT_CLS_CODE': market_class_code,
                'FID_INPUT_ISCD': symbol,
                'FID_TITL_CNTT': title_contains,
                'FID_INPUT_DATE_1': input_date,
                'FID_INPUT_HOUR_1': input_hour,
                'FID_RANK_SORT_CLS_CODE': rank_sort_cls_code,
                'FID_INPUT_SRNO': input_serial_number,
            },
            output_keys=('output',),
            max_pages=max_pages,
        )

    async def submit_domestic_cash_order(
        self,
        *,
        order_side: str,
        account_number: str,
        account_product_code: str,
        symbol: str,
        order_division_code: str,
        order_quantity: str,
        order_price: str,
        exchange_id_division_code: str = 'KRX',
        sell_type: str = '',
        condition_price: str = '',
    ) -> dict[str, Any]:
        normalized_side = order_side.strip().lower()
        if normalized_side not in {'buy', 'sell'}:
            raise ValueError('order_side must be buy or sell')

        tr_id = self._resolve_tr_id(
            real='TTTC0012U' if normalized_side == 'buy' else 'TTTC0011U',
            demo='VTTC0012U' if normalized_side == 'buy' else 'VTTC0011U',
        )
        body = {
            'CANO': account_number,
            'ACNT_PRDT_CD': account_product_code,
            'PDNO': symbol,
            'ORD_DVSN': order_division_code,
            'ORD_QTY': order_quantity,
            'ORD_UNPR': order_price,
            'EXCG_ID_DVSN_CD': exchange_id_division_code,
            'SLL_TYPE': sell_type,
            'CNDT_PRIC': condition_price,
        }
        return await self._post('/uapi/domestic-stock/v1/trading/order-cash', tr_id=tr_id, body=body)

    async def submit_domestic_order_cancel_replace(
        self,
        *,
        account_number: str,
        account_product_code: str,
        broker_order_id: str,
        exchange_forward_order_orgno: str,
        order_division_code: str,
        replace_cancel_division_code: str,
        order_quantity: str,
        order_price: str,
        all_quantity_order_yn: str,
        exchange_id_division_code: str = 'KRX',
        condition_price: str = '',
    ) -> dict[str, Any]:
        tr_id = self._resolve_tr_id(real='TTTC0013U', demo='VTTC0013U')
        body = {
            'CANO': account_number,
            'ACNT_PRDT_CD': account_product_code,
            'KRX_FWDG_ORD_ORGNO': exchange_forward_order_orgno,
            'ORGN_ODNO': broker_order_id,
            'ORD_DVSN': order_division_code,
            'RVSE_CNCL_DVSN_CD': replace_cancel_division_code,
            'ORD_QTY': order_quantity,
            'ORD_UNPR': order_price,
            'QTY_ALL_ORD_YN': all_quantity_order_yn,
            'EXCG_ID_DVSN_CD': exchange_id_division_code,
        }
        if condition_price:
            body['CNDT_PRIC'] = condition_price
        return await self._post('/uapi/domestic-stock/v1/trading/order-rvsecncl', tr_id=tr_id, body=body)

    async def fetch_domestic_cancelable_orders(
        self,
        *,
        account_number: str,
        account_product_code: str,
        inquiry_division_1: str = '0',
        inquiry_division_2: str = '0',
        max_pages: int = 10,
    ) -> dict[str, Any]:
        params = {
            'CANO': account_number,
            'ACNT_PRDT_CD': account_product_code,
            'INQR_DVSN_1': inquiry_division_1,
            'INQR_DVSN_2': inquiry_division_2,
            'CTX_AREA_FK100': '',
            'CTX_AREA_NK100': '',
        }
        items: list[dict[str, Any]] = []
        pages: list[dict[str, Any]] = []
        tr_cont = ''
        continuation_pending = False

        for _ in range(max_pages):
            body, next_tr_cont = await self._request(
                'GET',
                '/uapi/domestic-stock/v1/trading/inquire-psbl-rvsecncl',
                tr_id='TTTC0084R',
                params=params,
                tr_cont=tr_cont,
            )
            pages.append(body)
            value = body.get('output')
            if isinstance(value, list):
                items.extend(value)
            params['CTX_AREA_FK100'] = str(body.get('ctx_area_fk100') or '')
            params['CTX_AREA_NK100'] = str(body.get('ctx_area_nk100') or '')
            continuation_pending = next_tr_cont in {'M', 'F'}
            if not continuation_pending:
                break
            tr_cont = 'N'

        return {
            'output': items,
            'page_count': len(pages),
            'max_pages_reached': continuation_pending and len(pages) >= max_pages,
            'raw_pages': pages,
        }

    async def fetch_domestic_account_balance(
        self,
        *,
        account_number: str,
        account_product_code: str,
        after_hours_price_flag: str = 'N',
        inquiry_division: str = '02',
        unit_price_division: str = '01',
        include_fund_settlement: str = 'N',
        finance_auto_repayment: str = 'N',
        process_division: str = '00',
        max_pages: int = 10,
    ) -> dict[str, Any]:
        tr_id = self._resolve_tr_id(real='TTTC8434R', demo='VTTC8434R')
        params = {
            'CANO': account_number,
            'ACNT_PRDT_CD': account_product_code,
            'AFHR_FLPR_YN': after_hours_price_flag,
            'OFL_YN': '',
            'INQR_DVSN': inquiry_division,
            'UNPR_DVSN': unit_price_division,
            'FUND_STTL_ICLD_YN': include_fund_settlement,
            'FNCG_AMT_AUTO_RDPT_YN': finance_auto_repayment,
            'PRCS_DVSN': process_division,
            'CTX_AREA_FK100': '',
            'CTX_AREA_NK100': '',
        }
        rows: list[dict[str, Any]] = []
        pages: list[dict[str, Any]] = []
        summary: dict[str, Any] = {}
        tr_cont = ''
        continuation_pending = False

        for _ in range(max_pages):
            body, next_tr_cont = await self._request(
                'GET',
                '/uapi/domestic-stock/v1/trading/inquire-balance',
                tr_id=tr_id,
                params=params,
                tr_cont=tr_cont,
            )
            pages.append(body)
            output_rows = body.get('output1')
            if isinstance(output_rows, list):
                rows.extend(item for item in output_rows if isinstance(item, dict))
            output_summary = body.get('output2')
            if isinstance(output_summary, dict):
                summary = output_summary
            elif isinstance(output_summary, list):
                summary = next((item for item in reversed(output_summary) if isinstance(item, dict)), summary)
            params['CTX_AREA_FK100'] = str(body.get('ctx_area_fk100') or '')
            params['CTX_AREA_NK100'] = str(body.get('ctx_area_nk100') or '')
            continuation_pending = next_tr_cont in {'M', 'F'}
            if not continuation_pending:
                break
            tr_cont = 'N'

        return {
            'output1': rows,
            'output2': summary,
            'page_count': len(pages),
            'max_pages_reached': continuation_pending and len(pages) >= max_pages,
            'raw_pages': pages,
        }

    async def fetch_domestic_account_asset_summary(
        self,
        *,
        account_number: str,
        account_product_code: str,
        inquiry_division_1: str = '',
        apply_previous_base_date: str = '',
    ) -> dict[str, Any]:
        return await self._get(
            '/uapi/domestic-stock/v1/trading/inquire-account-balance',
            tr_id='CTRP6548R',
            params={
                'CANO': account_number,
                'ACNT_PRDT_CD': account_product_code,
                'INQR_DVSN_1': inquiry_division_1,
                'BSPR_BF_DT_APLY_YN': apply_previous_base_date,
            },
        )

    async def fetch_domestic_buying_power(
        self,
        *,
        account_number: str,
        account_product_code: str,
        symbol: str,
        order_price: str,
        order_division_code: str,
        include_cma_evaluation: str = 'N',
        include_overseas: str = 'N',
    ) -> dict[str, Any]:
        tr_id = self._resolve_tr_id(real='TTTC8908R', demo='VTTC8908R')
        return await self._get(
            '/uapi/domestic-stock/v1/trading/inquire-psbl-order',
            tr_id=tr_id,
            params={
                'CANO': account_number,
                'ACNT_PRDT_CD': account_product_code,
                'PDNO': symbol,
                'ORD_UNPR': order_price,
                'ORD_DVSN': order_division_code,
                'CMA_EVLU_AMT_ICLD_YN': include_cma_evaluation,
                'OVRS_ICLD_YN': include_overseas,
            },
        )

    async def fetch_domestic_daily_order_fills(
        self,
        *,
        period_division: str,
        account_number: str,
        account_product_code: str,
        inquiry_start_date: str,
        inquiry_end_date: str,
        sell_buy_division_code: str,
        conclusion_division: str,
        inquiry_division: str,
        inquiry_division_3: str,
        symbol: str = '',
        order_branch_number: str = '',
        broker_order_id: str = '',
        inquiry_division_1: str = '',
        exchange_id_division_code: str = 'KRX',
        max_pages: int = 10,
    ) -> dict[str, Any]:
        normalized_period = period_division.strip().lower()
        if normalized_period not in {'before', 'inner'}:
            raise ValueError('period_division must be before or inner')

        tr_id = self._resolve_tr_id(
            real='CTSC9215R' if normalized_period == 'before' else 'TTTC0081R',
            demo='VTSC9215R' if normalized_period == 'before' else 'VTTC0081R',
        )
        params = {
            'CANO': account_number,
            'ACNT_PRDT_CD': account_product_code,
            'INQR_STRT_DT': inquiry_start_date,
            'INQR_END_DT': inquiry_end_date,
            'SLL_BUY_DVSN_CD': sell_buy_division_code,
            'PDNO': symbol,
            'CCLD_DVSN': conclusion_division,
            'INQR_DVSN': inquiry_division,
            'INQR_DVSN_3': inquiry_division_3,
            'ORD_GNO_BRNO': order_branch_number,
            'ODNO': broker_order_id,
            'INQR_DVSN_1': inquiry_division_1,
            'CTX_AREA_FK100': '',
            'CTX_AREA_NK100': '',
            'EXCG_ID_DVSN_CD': exchange_id_division_code,
        }
        rows: list[dict[str, Any]] = []
        pages: list[dict[str, Any]] = []
        summary: dict[str, Any] = {}
        tr_cont = ''
        continuation_pending = False

        for _ in range(max_pages):
            body, next_tr_cont = await self._request(
                'GET',
                '/uapi/domestic-stock/v1/trading/inquire-daily-ccld',
                tr_id=tr_id,
                params=params,
                tr_cont=tr_cont,
            )
            pages.append(body)
            output_rows = body.get('output1')
            if isinstance(output_rows, list):
                rows.extend(output_rows)
            output_summary = body.get('output2')
            if isinstance(output_summary, dict):
                summary = output_summary
            params['CTX_AREA_FK100'] = str(body.get('ctx_area_fk100') or '')
            params['CTX_AREA_NK100'] = str(body.get('ctx_area_nk100') or '')
            continuation_pending = next_tr_cont in {'M', 'F'}
            if not continuation_pending:
                break
            tr_cont = 'N'

        return {
            'output1': rows,
            'output2': summary,
            'page_count': len(pages),
            'max_pages_reached': continuation_pending and len(pages) >= max_pages,
            'raw_pages': pages,
        }

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

    async def fetch_domestic_quote_balance_ranking(
        self,
        *,
        market_code: str = 'J',
        input_iscd: str = '0000',
        rank_sort_cls_code: str = '0',
        div_cls_code: str = '0',
        target_cls_code: str = '0',
        target_exclude_cls_code: str = '0',
        min_price: str = '',
        max_price: str = '',
        min_volume: str = '',
        max_pages: int = 10,
    ) -> dict:
        return await self._get_paginated_ranking(
            '/uapi/domestic-stock/v1/ranking/quote-balance',
            tr_id='FHPST01720000',
            params={
                'fid_vol_cnt': min_volume,
                'fid_cond_mrkt_div_code': market_code,
                'fid_cond_scr_div_code': '20172',
                'fid_input_iscd': input_iscd,
                'fid_rank_sort_cls_code': rank_sort_cls_code,
                'fid_div_cls_code': div_cls_code,
                'fid_trgt_cls_code': target_cls_code,
                'fid_trgt_exls_cls_code': target_exclude_cls_code,
                'fid_input_price_1': min_price,
                'fid_input_price_2': max_price,
            },
            output_keys=('output',),
            max_pages=max_pages,
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
