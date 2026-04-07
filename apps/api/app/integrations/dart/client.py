from __future__ import annotations

import asyncio
import io
import zipfile
from datetime import datetime, timedelta, timezone
from typing import Any
from xml.etree import ElementTree

import httpx

from app.core.audit_logging import log_fetch_event, request_with_audit
from app.core.settings import settings


class DartAPIError(RuntimeError):
    pass


class DartLookupError(LookupError):
    pass


class DartClient:
    def __init__(self) -> None:
        self.base_url = settings.dart_base_url.rstrip('/')
        self.api_key = settings.dart_api_key
        self._corp_codes: list[dict[str, str]] = []
        self._corp_codes_loaded_at: datetime | None = None
        self._corp_codes_lock = asyncio.Lock()

    async def fetch_company_overview(self, corp_code: str) -> dict[str, Any]:
        payload = await self._get_json('/company.json', {'corp_code': corp_code})
        log_fetch_event(
            service='dart',
            operation='fetch_company_overview',
            status='completed',
            corp_code=corp_code,
            response_status=payload.get('status'),
            corp_name=payload.get('corp_name'),
            stock_code=payload.get('stock_code'),
        )
        return payload

    async def fetch_single_company_financial_statements(
        self,
        *,
        corp_code: str,
        bsns_year: str,
        reprt_code: str,
        fs_div: str,
    ) -> dict[str, Any]:
        payload = await self._get_json(
            '/fnlttSinglAcntAll.json',
            {
                'corp_code': corp_code,
                'bsns_year': bsns_year,
                'reprt_code': reprt_code,
                'fs_div': fs_div,
            },
            allowed_statuses={'013'},
        )
        log_fetch_event(
            service='dart',
            operation='fetch_single_company_financial_statements',
            status='completed',
            corp_code=corp_code,
            business_year=bsns_year,
            report_code=reprt_code,
            fs_div=fs_div,
            response_status=payload.get('status'),
            row_count=len(payload.get('list', [])),
            response_message=payload.get('message'),
        )
        if payload.get('status') == '013':
            return {
                'status': '013',
                'message': payload.get('message'),
                'list': [],
            }
        return payload

    async def fetch_filings(
        self,
        *,
        corp_code: str,
        bgn_de: str,
        end_de: str,
        page_no: int = 1,
        page_count: int = 10,
        last_reprt_at: str = 'Y',
    ) -> dict[str, Any]:
        payload = await self.fetch_disclosure_list(
            corp_code=corp_code,
            bgn_de=bgn_de,
            end_de=end_de,
            page_no=page_no,
            page_count=page_count,
            last_reprt_at=last_reprt_at,
        )
        return payload

    async def fetch_disclosure_list(
        self,
        *,
        corp_code: str | None = None,
        bgn_de: str,
        end_de: str,
        page_no: int = 1,
        page_count: int = 10,
        last_reprt_at: str = 'N',
    ) -> dict[str, Any]:
        params = {
            'bgn_de': bgn_de,
            'end_de': end_de,
            'page_no': str(page_no),
            'page_count': str(page_count),
            'last_reprt_at': last_reprt_at,
            'sort': 'date',
            'sort_mth': 'desc',
        }
        if corp_code:
            params['corp_code'] = corp_code

        payload = await self._get_json(
            '/list.json',
            params,
            allowed_statuses={'013'},
        )
        log_fetch_event(
            service='dart',
            operation='fetch_disclosure_list',
            status='completed',
            corp_code=corp_code,
            window_start=bgn_de,
            window_end=end_de,
            page_no=page_no,
            page_count=page_count,
            last_report_only=last_reprt_at,
            response_status=payload.get('status'),
            total_count=payload.get('total_count'),
            item_count=len(payload.get('list', [])),
            response_message=payload.get('message'),
        )
        if payload.get('status') == '013':
            return {
                'status': '013',
                'message': payload.get('message'),
                'page_no': str(page_no),
                'page_count': '0',
                'total_count': '0',
                'list': [],
            }
        return payload

    async def fetch_financial_statements(
        self,
        *,
        corp_code: str,
        bsns_year: str,
        reprt_code: str,
        fs_div: str = 'CFS',
    ) -> dict[str, Any]:
        payload = await self._get_json(
            '/fnlttSinglAcntAll.json',
            {
                'corp_code': corp_code,
                'bsns_year': bsns_year,
                'reprt_code': reprt_code,
                'fs_div': fs_div,
            },
            allowed_statuses={'013'},
        )
        log_fetch_event(
            service='dart',
            operation='fetch_financial_statements',
            status='completed',
            corp_code=corp_code,
            business_year=bsns_year,
            report_code=reprt_code,
            fs_div=fs_div,
            response_status=payload.get('status'),
            row_count=len(payload.get('list', [])),
            response_message=payload.get('message'),
        )
        if payload.get('status') == '013':
            return {
                'status': '013',
                'message': payload.get('message'),
                'list': [],
            }
        return payload

    async def lookup_corp_code(
        self,
        *,
        corp_code: str | None = None,
        stock_code: str | None = None,
        corp_name: str | None = None,
    ) -> dict[str, str]:
        if corp_code:
            corp = await self._find_corp(lambda item: item['corp_code'] == corp_code)
            if corp:
                return corp
            raise DartLookupError(f'Unknown corp_code: {corp_code}')

        if stock_code:
            normalized_stock_code = stock_code.strip().zfill(6)
            corp = await self._find_corp(lambda item: item['stock_code'] == normalized_stock_code)
            if corp:
                return corp
            raise DartLookupError(f'Unknown stock_code: {normalized_stock_code}')

        if corp_name:
            normalized_name = corp_name.strip().lower()
            corp = await self._find_corp(lambda item: item['corp_name'].strip().lower() == normalized_name)
            if corp:
                return corp
            raise DartLookupError(f'Unknown corp_name: {corp_name}')

        raise DartLookupError('One of corp_code, stock_code, or corp_name is required')

    async def _find_corp(self, predicate) -> dict[str, str] | None:
        corp_codes = await self._load_corp_codes()
        for item in corp_codes:
            if predicate(item):
                return item
        return None

    async def _load_corp_codes(self) -> list[dict[str, str]]:
        now = datetime.now(timezone.utc)
        ttl = timedelta(seconds=settings.dart_corp_code_cache_ttl_seconds)
        if self._corp_codes and self._corp_codes_loaded_at and now - self._corp_codes_loaded_at < ttl:
            return self._corp_codes

        async with self._corp_codes_lock:
            now = datetime.now(timezone.utc)
            if self._corp_codes and self._corp_codes_loaded_at and now - self._corp_codes_loaded_at < ttl:
                return self._corp_codes

            payload = await self._get_bytes('/corpCode.xml', {})
            corp_codes = self._parse_corp_code_zip(payload)
            self._corp_codes = corp_codes
            self._corp_codes_loaded_at = now
            log_fetch_event(
                service='dart',
                operation='load_corp_codes',
                status='completed',
                cache_ttl_seconds=settings.dart_corp_code_cache_ttl_seconds,
                corp_code_count=len(corp_codes),
                payload_bytes=len(payload),
            )
            return corp_codes

    async def _get_json(
        self,
        path: str,
        params: dict[str, str],
        *,
        allowed_statuses: set[str] | None = None,
    ) -> dict[str, Any]:
        response = await self._request(path, params)
        payload = response.json()
        status = payload.get('status')
        if status and status != '000' and (allowed_statuses is None or status not in allowed_statuses):
            log_fetch_event(
                service='dart',
                operation='request_json',
                status='failed',
                path=path,
                response_status=status,
                error_message=payload.get('message'),
            )
            raise DartAPIError(f"DART request failed [{status}]: {payload.get('message', 'Unknown error')}")
        return payload

    async def _get_bytes(self, path: str, params: dict[str, str]) -> bytes:
        response = await self._request(path, params)
        return response.content

    async def _request(self, path: str, params: dict[str, str]) -> httpx.Response:
        if not self.api_key:
            raise DartAPIError('DART_API_KEY is not configured')

        request_params = {'crtfc_key': self.api_key, **params}
        async with httpx.AsyncClient(timeout=20.0) as client:
            response = await request_with_audit(
                client,
                'dart',
                'GET',
                f'{self.base_url}{path}',
                params=request_params,
            )
            if response.status_code >= 400:
                log_fetch_event(
                    service='dart',
                    operation='request_http',
                    status='failed',
                    path=path,
                    status_code=response.status_code,
                    error_message=response.text,
                )
                raise DartAPIError(f'DART request failed [{response.status_code}]: {response.text}')
            return response

    @staticmethod
    def _parse_corp_code_zip(payload: bytes) -> list[dict[str, str]]:
        try:
            with zipfile.ZipFile(io.BytesIO(payload)) as archive:
                first_member = archive.namelist()[0]
                xml_bytes = archive.read(first_member)
        except (IndexError, zipfile.BadZipFile) as exc:
            raise DartAPIError('Invalid DART corp code archive response') from exc

        root = ElementTree.fromstring(xml_bytes)
        corp_codes: list[dict[str, str]] = []
        for item in root.findall('list'):
            corp_code = (item.findtext('corp_code') or '').strip()
            corp_name = (item.findtext('corp_name') or '').strip()
            stock_code = (item.findtext('stock_code') or '').strip()
            modify_date = (item.findtext('modify_date') or '').strip()
            if not corp_code or not corp_name:
                continue
            corp_codes.append(
                {
                    'corp_code': corp_code,
                    'corp_name': corp_name,
                    'stock_code': stock_code,
                    'modify_date': modify_date,
                }
            )
        return corp_codes


dart_client = DartClient()
