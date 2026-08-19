from __future__ import annotations

import asyncio
from dataclasses import dataclass
import re
import time
from typing import Any, Awaitable, Callable

import httpx

from app.core.audit_logging import log_fetch_event, request_with_audit
from app.core.settings import settings


class SECEdgarError(RuntimeError):
    pass


class SECEdgarConfigurationError(SECEdgarError):
    pass


@dataclass(frozen=True, slots=True)
class SECCompanyTicker:
    cik: str
    name: str
    ticker: str
    exchange: str


_CIK_PATTERN = re.compile(r'^(?:CIK)?(?P<cik>\d{1,10})$', re.IGNORECASE)


def normalize_cik(value: int | str) -> str:
    match = _CIK_PATTERN.fullmatch(str(value).strip())
    if not match:
        raise ValueError(f'Invalid SEC CIK: {value}')
    return match.group('cik').zfill(10)


def parse_company_ticker_exchange_mapping(payload: dict[str, Any]) -> list[SECCompanyTicker]:
    fields = [str(field) for field in payload.get('fields') or []]
    required = {'cik', 'name', 'ticker', 'exchange'}
    if not required.issubset(fields):
        raise SECEdgarError('SEC ticker mapping is missing required fields')
    rows: list[SECCompanyTicker] = []
    for raw_row in payload.get('data') or []:
        if not isinstance(raw_row, list) or len(raw_row) != len(fields):
            continue
        item = dict(zip(fields, raw_row, strict=True))
        ticker = str(item['ticker'] or '').strip().upper()
        name = str(item['name'] or '').strip()
        exchange = str(item['exchange'] or '').strip()
        if not ticker or not name:
            continue
        rows.append(
            SECCompanyTicker(
                cik=normalize_cik(item['cik']),
                name=name,
                ticker=ticker,
                exchange=exchange,
            )
        )
    return sorted(rows, key=lambda item: (item.ticker, item.cik))


class _AsyncRateLimiter:
    def __init__(
        self,
        requests_per_second: float,
        *,
        clock: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> None:
        if requests_per_second <= 0 or requests_per_second > 10:
            raise ValueError('SEC requests_per_second must be greater than 0 and no more than 10')
        self.minimum_interval_seconds = 1.0 / requests_per_second
        self._clock = clock
        self._sleep = sleep
        self._lock = asyncio.Lock()
        self._last_request_at: float | None = None

    async def wait(self) -> None:
        async with self._lock:
            now = self._clock()
            if self._last_request_at is not None:
                remaining = self.minimum_interval_seconds - (now - self._last_request_at)
                if remaining > 0:
                    await self._sleep(remaining)
                    now = self._clock()
            self._last_request_at = now


class SECEdgarClient:
    def __init__(
        self,
        *,
        user_agent: str | None = None,
        data_base_url: str | None = None,
        www_base_url: str | None = None,
        timeout_seconds: float | None = None,
        requests_per_second: float | None = None,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        self.user_agent = (user_agent if user_agent is not None else settings.sec_edgar_user_agent).strip()
        if not self.user_agent:
            raise SECEdgarConfigurationError(
                'SEC_EDGAR_USER_AGENT must identify the requester and include an administrative contact email'
            )
        self.data_base_url = (data_base_url or settings.sec_edgar_data_base_url).rstrip('/')
        self.www_base_url = (www_base_url or settings.sec_edgar_www_base_url).rstrip('/')
        self.timeout_seconds = timeout_seconds or settings.sec_edgar_request_timeout_seconds
        self._transport = transport
        self._rate_limiter = _AsyncRateLimiter(
            requests_per_second or settings.sec_edgar_requests_per_second
        )

    async def fetch_submissions(self, cik: int | str) -> dict[str, Any]:
        normalized = normalize_cik(cik)
        return await self._get_json(f'{self.data_base_url}/submissions/CIK{normalized}.json')

    async def fetch_companyfacts(self, cik: int | str) -> dict[str, Any]:
        normalized = normalize_cik(cik)
        return await self._get_json(f'{self.data_base_url}/api/xbrl/companyfacts/CIK{normalized}.json')

    async def fetch_company_ticker_exchange_mapping(self) -> list[SECCompanyTicker]:
        payload = await self._get_json(f'{self.www_base_url}/files/company_tickers_exchange.json')
        return parse_company_ticker_exchange_mapping(payload)

    async def _get_json(self, url: str) -> dict[str, Any]:
        await self._rate_limiter.wait()
        headers = {
            'User-Agent': self.user_agent,
            'Accept': 'application/json',
            'Accept-Encoding': 'gzip, deflate',
        }
        async with httpx.AsyncClient(timeout=self.timeout_seconds, transport=self._transport) as client:
            response = await request_with_audit(client, 'sec_edgar', 'GET', url, headers=headers)
        if response.status_code >= 400:
            log_fetch_event(
                service='sec_edgar',
                operation='request_json',
                status='failed',
                status_code=response.status_code,
                url=url,
            )
            raise SECEdgarError(f'SEC EDGAR request failed [{response.status_code}]')
        try:
            payload = response.json()
        except ValueError as exc:
            raise SECEdgarError('SEC EDGAR returned invalid JSON') from exc
        if not isinstance(payload, dict):
            raise SECEdgarError('SEC EDGAR returned a non-object JSON payload')
        return payload
