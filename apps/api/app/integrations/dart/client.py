from __future__ import annotations

import asyncio
import io
import os
import re
import tempfile
from urllib.parse import unquote
import zipfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from xml.etree import ElementTree

import httpx

from app.core.audit_logging import log_fetch_event, request_read_with_retry_and_audit
from app.core.settings import settings


class DartAPIError(RuntimeError):
    pass


class DartLookupError(LookupError):
    pass


_FILENAME_STAR_PATTERN = re.compile(r"filename\*\s*=\s*([^']*)''([^;]+)")
_FILENAME_PATTERN = re.compile(r'filename\s*=\s*"?(?P<filename>[^";]+)"?')


class DartClient:
    def __init__(
        self,
        *,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        self.base_url = settings.dart_base_url.rstrip('/')
        self.api_key = settings.dart_api_key
        self.transport = transport
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

    async def download_document(self, *, rcept_no: str) -> dict[str, Any]:
        response = await self._request('/document.xml', {'rcept_no': rcept_no})
        self._raise_for_binary_error_envelope(response, path='/document.xml')
        payload = self._build_binary_payload(
            response,
            default_filename=f'dart-document-{rcept_no}.zip',
        )
        log_fetch_event(
            service='dart',
            operation='download_document',
            status='completed',
            receipt_number=rcept_no,
            content_type=payload['content_type'],
            file_name=payload['filename'],
            payload_bytes=payload['size_bytes'],
        )
        return payload

    async def download_xbrl(self, *, rcept_no: str, reprt_code: str | None = None) -> dict[str, Any]:
        params = {'rcept_no': rcept_no}
        if reprt_code:
            params['reprt_code'] = reprt_code

        response = await self._request('/fnlttXbrl.xml', params)
        self._raise_for_binary_error_envelope(response, path='/fnlttXbrl.xml')
        filename_suffix = f'-{reprt_code}' if reprt_code else ''
        payload = self._build_binary_payload(
            response,
            default_filename=f'dart-xbrl-{rcept_no}{filename_suffix}.zip',
        )
        log_fetch_event(
            service='dart',
            operation='download_xbrl',
            status='completed',
            receipt_number=rcept_no,
            report_code=reprt_code,
            content_type=payload['content_type'],
            file_name=payload['filename'],
            payload_bytes=payload['size_bytes'],
        )
        return payload

    async def has_supported_stock_code(self, stock_code: str) -> bool:
        normalized_stock_code = stock_code.strip().zfill(6)
        corp_codes = await self._load_corp_codes()
        return any(item['stock_code'] == normalized_stock_code for item in corp_codes)

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

            payload = b''
            content_type: str | None = None
            try:
                response = await self._request('/corpCode.xml', {})
                payload = response.content
                content_type = response.headers.get('content-type')
                self._raise_for_binary_error_envelope(response, path='/corpCode.xml')
                corp_codes = self._parse_corp_code_payload(payload, content_type=content_type)
                if not corp_codes:
                    raise DartAPIError('DART corp code archive contained no valid records')
            except (DartAPIError, httpx.HTTPError) as exc:
                persisted = self._load_persisted_corp_codes()
                if persisted is not None:
                    self._corp_codes = persisted
                    self._corp_codes_loaded_at = now
                    log_fetch_event(
                        service='dart',
                        operation='load_corp_codes',
                        status='completed',
                        source='persistent_fallback',
                        corp_code_count=len(persisted),
                        upstream_error_type=type(exc).__name__,
                    )
                    return persisted
                log_fetch_event(
                    service='dart',
                    operation='load_corp_codes',
                    status='failed',
                    content_type=content_type,
                    payload_bytes=len(payload),
                    error_message=str(exc),
                )
                raise

            self._persist_corp_code_payload(payload)
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

    @staticmethod
    def _corp_code_cache_path() -> Path | None:
        configured = settings.dart_corp_code_cache_file.strip()
        if not configured:
            return None
        return Path(configured).expanduser()

    def _load_persisted_corp_codes(self) -> list[dict[str, str]] | None:
        cache_path = self._corp_code_cache_path()
        if cache_path is None or not cache_path.exists():
            return None

        try:
            if cache_path.is_symlink() or not cache_path.is_file():
                raise DartAPIError('DART corp code cache must be a regular file')
            if cache_path.stat().st_size > 64 * 1024 * 1024:
                raise DartAPIError('DART corp code cache exceeds the 64 MiB safety limit')

            payload = cache_path.read_bytes()
            corp_codes = self._parse_corp_code_payload(payload)
            if not corp_codes:
                raise DartAPIError('DART corp code cache contained no valid records')
            return corp_codes
        except (DartAPIError, OSError) as exc:
            log_fetch_event(
                service='dart',
                operation='load_persisted_corp_codes',
                status='failed',
                cache_path=str(cache_path),
                error_message=str(exc),
            )
            return None

    def _persist_corp_code_payload(self, payload: bytes) -> None:
        cache_path = self._corp_code_cache_path()
        if cache_path is None:
            return

        temporary_path: str | None = None
        try:
            cache_path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
            file_descriptor, temporary_path = tempfile.mkstemp(
                prefix=f'.{cache_path.name}.',
                suffix='.tmp',
                dir=cache_path.parent,
            )
            try:
                os.fchmod(file_descriptor, 0o600)
                with os.fdopen(file_descriptor, 'wb') as cache_file:
                    cache_file.write(payload)
                    cache_file.flush()
                    os.fsync(cache_file.fileno())
            except Exception:
                try:
                    os.close(file_descriptor)
                except OSError:
                    pass
                raise
            os.replace(temporary_path, cache_path)
            temporary_path = None
            cache_path.chmod(0o600)
        except OSError as exc:
            log_fetch_event(
                service='dart',
                operation='persist_corp_codes',
                status='failed',
                cache_path=str(cache_path),
                error_message=str(exc),
            )
        finally:
            if temporary_path is not None:
                try:
                    Path(temporary_path).unlink(missing_ok=True)
                except OSError:
                    pass

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
        async with httpx.AsyncClient(timeout=20.0, transport=self.transport) as client:
            response = await request_read_with_retry_and_audit(
                client,
                'dart',
                'GET',
                f'{self.base_url}{path}',
                params=request_params,
                max_attempts=2,
            )
            if response.status_code >= 400:
                log_fetch_event(
                    service='dart',
                    operation='request_http',
                    status='failed',
                    path=path,
                    status_code=response.status_code,
                    error_message=f'DART HTTP {response.status_code} response body omitted from audit log',
                )
                raise DartAPIError(f'DART request failed [{response.status_code}]: {response.text}')
            return response

    @staticmethod
    def _parse_corp_code_payload(
        payload: bytes,
        *,
        content_type: str | None = None,
    ) -> list[dict[str, str]]:
        if zipfile.is_zipfile(io.BytesIO(payload)):
            return DartClient._parse_corp_code_zip(payload, content_type=content_type)

        xml_root = DartClient._try_parse_xml(payload)
        if xml_root is not None:
            if xml_root.findall('list'):
                return DartClient._parse_corp_code_xml_root(xml_root)

            status = (xml_root.findtext('.//status') or '').strip()
            message = (xml_root.findtext('.//message') or '').strip()
            if status and status != '000':
                DartClient._raise_for_binary_error_payload(
                    {'status': status, 'message': message},
                    path='/corpCode.xml',
                )

            raise DartAPIError(
                'Invalid DART corp code archive response '
                f'({DartClient._describe_binary_payload(payload, content_type=content_type, xml_root=xml_root)})'
            )

        raise DartAPIError(
            'Invalid DART corp code archive response '
            f'({DartClient._describe_binary_payload(payload, content_type=content_type)})'
        )

    @staticmethod
    def _parse_corp_code_zip(
        payload: bytes,
        *,
        content_type: str | None = None,
    ) -> list[dict[str, str]]:
        try:
            with zipfile.ZipFile(io.BytesIO(payload)) as archive:
                first_member = archive.namelist()[0]
                xml_bytes = archive.read(first_member)
        except (IndexError, zipfile.BadZipFile) as exc:
            raise DartAPIError(
                'Invalid DART corp code archive response '
                f'({DartClient._describe_binary_payload(payload, content_type=content_type)})'
            ) from exc

        root = DartClient._try_parse_xml(xml_bytes)
        if root is None:
            raise DartAPIError(
                'Invalid DART corp code archive response '
                f'({DartClient._describe_binary_payload(xml_bytes, content_type="application/xml")})'
            )
        return DartClient._parse_corp_code_xml_root(root)

    @staticmethod
    def _parse_corp_code_xml_root(root: ElementTree.Element) -> list[dict[str, str]]:
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

    @staticmethod
    def _try_parse_xml(payload: bytes) -> ElementTree.Element | None:
        try:
            return ElementTree.fromstring(payload)
        except ElementTree.ParseError:
            return None

    @staticmethod
    def _describe_binary_payload(
        payload: bytes,
        *,
        content_type: str | None = None,
        xml_root: ElementTree.Element | None = None,
    ) -> str:
        parts = [f'size_bytes={len(payload)}']
        if content_type:
            parts.append(f'content_type={content_type}')

        prefix = payload[:16].hex()
        if prefix:
            parts.append(f'prefix_hex={prefix}')

        if xml_root is not None:
            parts.append(f'xml_root={xml_root.tag}')

        return ', '.join(parts)

    @staticmethod
    def _build_binary_payload(response: httpx.Response, *, default_filename: str) -> dict[str, Any]:
        headers = dict(response.headers)
        return {
            'content': response.content,
            'content_type': response.headers.get('content-type', 'application/octet-stream'),
            'filename': DartClient._extract_filename(response.headers.get('content-disposition'), default_filename),
            'size_bytes': len(response.content),
            'headers': headers,
        }

    @staticmethod
    def _extract_filename(content_disposition: str | None, default_filename: str) -> str:
        if not content_disposition:
            return default_filename

        star_match = _FILENAME_STAR_PATTERN.search(content_disposition)
        if star_match:
            return unquote(star_match.group(2)) or default_filename

        match = _FILENAME_PATTERN.search(content_disposition)
        if match:
            return match.group('filename')

        return default_filename

    @staticmethod
    def _raise_for_binary_error_envelope(response: httpx.Response, *, path: str) -> None:
        content = response.content or b''
        if not content:
            return

        stripped = content.lstrip()
        content_type = (response.headers.get('content-type') or '').lower()

        if stripped.startswith(b'{') or 'json' in content_type:
            try:
                payload = response.json()
            except Exception:
                return
            DartClient._raise_for_binary_error_payload(payload, path=path)
            return

        if stripped.startswith(b'<') or 'xml' in content_type or 'text/' in content_type:
            try:
                root = ElementTree.fromstring(content)
            except ElementTree.ParseError:
                return
            payload = {
                'status': (root.findtext('.//status') or '').strip(),
                'message': (root.findtext('.//message') or '').strip(),
            }
            DartClient._raise_for_binary_error_payload(payload, path=path)

    @staticmethod
    def _raise_for_binary_error_payload(payload: dict[str, Any], *, path: str) -> None:
        status = str(payload.get('status') or '').strip()
        if status and status != '000':
            message = payload.get('message', 'Unknown error')
            log_fetch_event(
                service='dart',
                operation='request_binary',
                status='failed',
                path=path,
                response_status=status,
                error_message=message,
            )
            raise DartAPIError(f'DART request failed [{status}]: {message}')


dart_client = DartClient()
