from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
import hashlib
import html
import re
from typing import Any, Protocol

import httpx

from app.core.audit_logging import exception_message, log_fetch_event, request_read_with_retry_and_audit
from app.core.settings import settings

NAVER_NEWS_PROVIDER = 'naver_news_search'
NAVER_NEWS_BASE_URL = 'https://openapi.naver.com/v1/search/news.json'
_TAG_RE = re.compile(r'<[^>]+>')


@dataclass(frozen=True)
class ExternalNewsProviderStatus:
    provider: str
    enabled: bool
    status: str
    reason: str | None = None

    def as_dict(self) -> dict[str, Any]:
        return {
            'provider': self.provider,
            'enabled': self.enabled,
            'status': self.status,
            'reason': self.reason,
        }


@dataclass(frozen=True)
class ExternalNewsItem:
    provider: str
    provider_item_id: str | None
    title: str
    description: str | None
    url: str
    source_url: str | None
    published_at: datetime | None
    payload: dict[str, Any]


class ExternalNewsProvider(Protocol):
    provider_name: str

    def status(self) -> ExternalNewsProviderStatus: ...

    async def search_news(self, *, query: str, display: int = 10, start: int = 1) -> list[ExternalNewsItem]: ...


def _clean_text(value: Any) -> str:
    if value is None:
        return ''
    text = html.unescape(str(value))
    text = _TAG_RE.sub('', text)
    return ' '.join(text.split())


def _parse_pub_date(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        parsed = parsedate_to_datetime(str(value))
    except (TypeError, ValueError):
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _stable_item_id(*parts: str | None) -> str:
    material = '|'.join(part or '' for part in parts)
    return hashlib.sha256(material.encode('utf-8')).hexdigest()[:32]


class NaverNewsSearchProvider:
    provider_name = NAVER_NEWS_PROVIDER

    def __init__(self, *, transport: httpx.AsyncBaseTransport | None = None) -> None:
        self.base_url = settings.naver_search_base_url.rstrip('/')
        self.transport = transport

    def status(self) -> ExternalNewsProviderStatus:
        if not settings.naver_search_client_id or not settings.naver_search_client_secret:
            return ExternalNewsProviderStatus(
                provider=self.provider_name,
                enabled=False,
                status='disabled',
                reason='naver_search_credentials_missing',
            )
        return ExternalNewsProviderStatus(provider=self.provider_name, enabled=True, status='available')

    async def search_news(self, *, query: str, display: int = 10, start: int = 1) -> list[ExternalNewsItem]:
        provider_status = self.status()
        if not provider_status.enabled:
            raise RuntimeError(provider_status.reason or 'external_news_provider_disabled')

        headers = {
            'X-Naver-Client-Id': settings.naver_search_client_id,
            'X-Naver-Client-Secret': settings.naver_search_client_secret,
            'Accept': 'application/json',
        }
        params = {
            'query': query,
            'display': max(1, min(int(display), 100)),
            'start': max(1, min(int(start), 1000)),
            'sort': 'date',
        }
        try:
            async with httpx.AsyncClient(timeout=settings.external_news_request_timeout_seconds, transport=self.transport) as client:
                response = await request_read_with_retry_and_audit(
                    client,
                    self.provider_name,
                    'GET',
                    self.base_url,
                    headers=headers,
                    params=params,
                    secret_values=(settings.naver_search_client_id, settings.naver_search_client_secret),
                    max_attempts=2,
                )
        except httpx.HTTPError as exc:
            log_fetch_event(
                service=self.provider_name,
                operation='search_news',
                status='failed',
                query=query,
                error_message=exception_message(exc),
            )
            raise RuntimeError(f'Naver News Search transport error: {exception_message(exc)}') from exc

        if response.status_code >= 400:
            log_fetch_event(
                service=self.provider_name,
                operation='search_news',
                status='failed',
                query=query,
                status_code=response.status_code,
                error_message='Naver News Search request failed',
            )
            raise RuntimeError(f'Naver News Search request failed [{response.status_code}]')

        payload = response.json()
        raw_items = payload.get('items') if isinstance(payload, dict) else None
        if not isinstance(raw_items, list):
            raise RuntimeError('Naver News Search response did not contain an items list')

        items: list[ExternalNewsItem] = []
        for raw in raw_items:
            if not isinstance(raw, dict):
                continue
            link = str(raw.get('originallink') or raw.get('link') or '').strip()
            if not link:
                continue
            title = _clean_text(raw.get('title'))
            published_at = _parse_pub_date(raw.get('pubDate'))
            item_id = _stable_item_id(link, title, str(raw.get('pubDate') or ''))
            items.append(
                ExternalNewsItem(
                    provider=self.provider_name,
                    provider_item_id=item_id,
                    title=title or link,
                    description=_clean_text(raw.get('description')) or None,
                    url=link,
                    source_url=str(raw.get('link') or '').strip() or None,
                    published_at=published_at,
                    payload={
                        'provider': self.provider_name,
                        'query': query,
                        'access_status': 'headline_only',
                        'content_basis': 'search_result_title_and_snippet',
                        'raw': raw,
                    },
                )
            )

        log_fetch_event(
            service=self.provider_name,
            operation='search_news',
            status='completed',
            query=query,
            record_count=len(items),
        )
        return items
