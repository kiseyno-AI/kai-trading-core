from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
import inspect
from threading import Lock
from typing import Any, Protocol

import exchange_calendars as xcals
import pandas as pd

from app.brokers.kis.client import KISClient

MARKET_SESSION_OPEN = 'open'
MARKET_SESSION_CLOSED = 'closed'
MARKET_SESSION_WEEKEND = 'weekend'
MARKET_SESSION_HOLIDAY = 'holiday'
MARKET_SESSION_NOT_APPLICABLE = 'not_applicable'

RECOMMENDATION_BASIS_LIVE_INTRADAY = 'live_intraday'
RECOMMENDATION_BASIS_LATEST_COMPLETED_EOD = 'latest_completed_eod'

CALENDAR_SOURCE_KIS_CHK_HOLIDAY = 'kis_chk_holiday'
CALENDAR_SOURCE_EXCHANGE_CALENDARS = 'exchange_calendars'
CALENDAR_SOURCE_STATUS_OK = 'ok'
CALENDAR_SOURCE_STATUS_FALLBACK = 'fallback'
CALENDAR_SOURCE_STATUS_UNAVAILABLE = 'unavailable'
CALENDAR_SOURCE_STATUS_MISSING_DATE = 'missing_date'

KRX_EXCHANGE_CALENDAR = xcals.get_calendar('XKRX')
DEFAULT_KRX_CALENDAR_CACHE_TTL = timedelta(hours=6)
DEFAULT_KRX_CALENDAR_SCAN_DAYS = 370


class KrxCalendarUnavailableError(RuntimeError):
    pass


class KISCalendarClientProtocol(Protocol):
    def fetch_krx_holiday_calendar(
        self,
        *,
        base_date: str,
        ctx_area_nk: str = '',
        ctx_area_fk: str = '',
        tr_cont: str = '',
        max_pages: int = 10,
    ) -> Any:
        ...


@dataclass(frozen=True)
class KrxCalendarDay:
    date: date
    is_open: bool
    calendar_source: str
    calendar_source_status: str
    opnd_yn: str | None = None
    raw: dict[str, Any] | None = None
    calendar_source_error: str | None = None
    fetched_at: str | None = None

    def metadata(self) -> dict[str, Any]:
        return {
            'calendar_source': self.calendar_source,
            'calendar_source_status': self.calendar_source_status,
            'calendar_source_error': self.calendar_source_error,
            'opnd_yn': self.opnd_yn,
        }


@dataclass(frozen=True)
class _KrxCalendarCacheEntry:
    record: KrxCalendarDay
    expires_at: datetime


class InMemoryKrxCalendarCache:
    """Small repository-shaped cache; swap this boundary for DB persistence later."""

    def __init__(self, *, ttl: timedelta = DEFAULT_KRX_CALENDAR_CACHE_TTL) -> None:
        self.ttl = ttl
        self._entries: dict[date, _KrxCalendarCacheEntry] = {}
        self._lock = Lock()

    def load_day(self, value: date) -> KrxCalendarDay | None:
        now = datetime.now(timezone.utc)
        with self._lock:
            entry = self._entries.get(value)
            if entry is None:
                return None
            if entry.expires_at <= now:
                self._entries.pop(value, None)
                return None
            return entry.record

    def store_days(self, records: list[KrxCalendarDay]) -> None:
        if not records:
            return
        expires_at = datetime.now(timezone.utc) + self.ttl
        with self._lock:
            for record in records:
                self._entries[record.date] = _KrxCalendarCacheEntry(
                    record=record,
                    expires_at=expires_at,
                )

    def clear(self) -> None:
        with self._lock:
            self._entries.clear()


class KisKrxTradingCalendarProvider:
    def __init__(
        self,
        *,
        client: KISCalendarClientProtocol | None = None,
        cache: InMemoryKrxCalendarCache | None = None,
        max_pages: int = 1,
        max_scan_days: int = DEFAULT_KRX_CALENDAR_SCAN_DAYS,
    ) -> None:
        self._client = client
        self.cache = cache or InMemoryKrxCalendarCache()
        self.max_pages = max_pages
        self.max_scan_days = max_scan_days

    def lookup_day(self, value: date, *, allow_fallback: bool = True) -> KrxCalendarDay:
        cached = self.cache.load_day(value)
        if cached is not None:
            return cached

        try:
            self._fetch_and_cache(value)
        except Exception as exc:
            if allow_fallback:
                return self._exchange_calendar_fallback(
                    value,
                    status=CALENDAR_SOURCE_STATUS_FALLBACK,
                    error=str(exc),
                )
            raise KrxCalendarUnavailableError(f'KIS KRX calendar unavailable for {value.isoformat()}: {exc}') from exc

        cached = self.cache.load_day(value)
        if cached is not None:
            return cached

        message = f'KIS chk-holiday response did not include {value.isoformat()}'
        if allow_fallback:
            return self._exchange_calendar_fallback(
                value,
                status=CALENDAR_SOURCE_STATUS_MISSING_DATE,
                error=message,
            )
        raise KrxCalendarUnavailableError(message)

    async def lookup_day_async(self, value: date, *, allow_fallback: bool = True) -> KrxCalendarDay:
        cached = self.cache.load_day(value)
        if cached is not None:
            return cached

        try:
            await self._fetch_and_cache_async(value)
        except Exception as exc:
            if allow_fallback:
                return self._exchange_calendar_fallback(
                    value,
                    status=CALENDAR_SOURCE_STATUS_FALLBACK,
                    error=str(exc),
                )
            raise KrxCalendarUnavailableError(f'KIS KRX calendar unavailable for {value.isoformat()}: {exc}') from exc

        cached = self.cache.load_day(value)
        if cached is not None:
            return cached

        message = f'KIS chk-holiday response did not include {value.isoformat()}'
        if allow_fallback:
            return self._exchange_calendar_fallback(
                value,
                status=CALENDAR_SOURCE_STATUS_MISSING_DATE,
                error=message,
            )
        raise KrxCalendarUnavailableError(message)

    def clear_cache(self) -> None:
        self.cache.clear()

    def _client_for_request(self) -> KISCalendarClientProtocol:
        if self._client is None:
            # KIS documents chk-holiday as a real-domain domestic stock quotation API.
            self._client = KISClient(use_mock=False)
        return self._client

    def _fetch_and_cache(self, value: date) -> None:
        client = self._client_for_request()
        result = client.fetch_krx_holiday_calendar(
            base_date=value.strftime('%Y%m%d'),
            max_pages=self.max_pages,
        )
        payload = _run_maybe_async(result)
        self._cache_payload(payload)

    async def _fetch_and_cache_async(self, value: date) -> None:
        client = self._client_for_request()
        result = client.fetch_krx_holiday_calendar(
            base_date=value.strftime('%Y%m%d'),
            max_pages=self.max_pages,
        )
        payload = await _await_maybe_async(result)
        self._cache_payload(payload)

    def _cache_payload(self, payload: Any) -> None:
        rows = _extract_kis_calendar_rows(payload)
        fetched_at = datetime.now(timezone.utc).isoformat()
        records = [
            record
            for record in (_parse_kis_calendar_row(row, fetched_at=fetched_at) for row in rows)
            if record is not None
        ]
        self.cache.store_days(records)

    def _exchange_calendar_fallback(self, value: date, *, status: str, error: str | None) -> KrxCalendarDay:
        return get_krx_exchange_calendar_day(value, status=status, error=error)


def _run_maybe_async(value: Any) -> Any:
    if not inspect.isawaitable(value):
        return value
    try:
        running_loop = asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(value)
    if running_loop.is_running():
        if inspect.iscoroutine(value):
            value.close()
        raise RuntimeError('KIS KRX calendar lookup must be awaited from async callers')
    return asyncio.run(value)


async def _await_maybe_async(value: Any) -> Any:
    if inspect.isawaitable(value):
        return await value
    return value


def _extract_kis_calendar_rows(payload: Any) -> list[dict[str, Any]]:
    if not isinstance(payload, dict):
        return []
    output = payload.get('output')
    if isinstance(output, list):
        return [item for item in output if isinstance(item, dict)]
    if isinstance(output, dict):
        return [output]
    return []


def _parse_kis_calendar_row(row: dict[str, Any], *, fetched_at: str) -> KrxCalendarDay | None:
    row_date = _parse_kis_calendar_date(
        row.get('bass_dt')
        or row.get('BASS_DT')
        or row.get('base_dt')
        or row.get('BASE_DT')
    )
    if row_date is None:
        return None
    opnd_yn = _normalize_yn(row.get('opnd_yn') or row.get('OPND_YN'))
    is_open = _yn_to_bool(opnd_yn)
    if is_open is None:
        return None
    return KrxCalendarDay(
        date=row_date,
        is_open=is_open,
        calendar_source=CALENDAR_SOURCE_KIS_CHK_HOLIDAY,
        calendar_source_status=CALENDAR_SOURCE_STATUS_OK,
        opnd_yn=opnd_yn,
        raw=dict(row),
        fetched_at=fetched_at,
    )


def _normalize_yn(value: Any) -> str | None:
    if isinstance(value, bool):
        return 'Y' if value else 'N'
    normalized = str(value or '').strip().upper()
    if normalized in {'Y', 'YES', 'TRUE', '1'}:
        return 'Y'
    if normalized in {'N', 'NO', 'FALSE', '0'}:
        return 'N'
    return None


def _yn_to_bool(value: str | None) -> bool | None:
    if value == 'Y':
        return True
    if value == 'N':
        return False
    return None


def _parse_kis_calendar_date(value: Any) -> date | None:
    if isinstance(value, date):
        return value
    text = str(value or '').strip()
    if not text:
        return None
    if len(text) == 8 and text.isdigit():
        try:
            return date(int(text[:4]), int(text[4:6]), int(text[6:8]))
        except ValueError:
            return None
    try:
        return date.fromisoformat(text)
    except ValueError:
        return None


def _normalize_date(value: date | str) -> date:
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value))


def _exchange_calendar_is_open(value: date) -> bool:
    try:
        return bool(KRX_EXCHANGE_CALENDAR.is_session(pd.Timestamp(value.isoformat())))
    except Exception:
        return False


def get_krx_exchange_calendar_day(
    value: date | str,
    *,
    status: str = CALENDAR_SOURCE_STATUS_FALLBACK,
    error: str | None = None,
) -> KrxCalendarDay:
    normalized = _normalize_date(value)
    return KrxCalendarDay(
        date=normalized,
        is_open=_exchange_calendar_is_open(normalized),
        calendar_source=CALENDAR_SOURCE_EXCHANGE_CALENDARS,
        calendar_source_status=status,
        calendar_source_error=error,
    )


def latest_krx_exchange_calendar_trading_date(
    value: date | str,
    *,
    max_scan_days: int = DEFAULT_KRX_CALENDAR_SCAN_DAYS,
) -> date | None:
    normalized = _normalize_date(value)
    for offset in range(max_scan_days + 1):
        candidate = normalized - timedelta(days=offset)
        if _exchange_calendar_is_open(candidate):
            return candidate
    return None


_calendar_provider: KisKrxTradingCalendarProvider = KisKrxTradingCalendarProvider()


def set_krx_calendar_provider(provider: KisKrxTradingCalendarProvider) -> None:
    global _calendar_provider
    _calendar_provider = provider


def reset_krx_calendar_provider() -> None:
    global _calendar_provider
    _calendar_provider = KisKrxTradingCalendarProvider()


def clear_krx_calendar_cache() -> None:
    _calendar_provider.clear_cache()


def get_krx_calendar_day(value: date | str, *, allow_fallback: bool = True) -> KrxCalendarDay:
    return _calendar_provider.lookup_day(_normalize_date(value), allow_fallback=allow_fallback)


async def async_get_krx_calendar_day(value: date | str, *, allow_fallback: bool = True) -> KrxCalendarDay:
    return await _calendar_provider.lookup_day_async(_normalize_date(value), allow_fallback=allow_fallback)


def is_krx_trading_day(value: date | str, *, allow_fallback: bool = True) -> bool:
    return get_krx_calendar_day(value, allow_fallback=allow_fallback).is_open


async def async_is_krx_trading_day(value: date | str, *, allow_fallback: bool = True) -> bool:
    return (await async_get_krx_calendar_day(value, allow_fallback=allow_fallback)).is_open


def latest_krx_trading_date(
    value: date | str,
    *,
    allow_fallback: bool = True,
    max_scan_days: int = DEFAULT_KRX_CALENDAR_SCAN_DAYS,
) -> date | None:
    normalized = _normalize_date(value)
    for offset in range(max_scan_days + 1):
        candidate = normalized - timedelta(days=offset)
        if is_krx_trading_day(candidate, allow_fallback=allow_fallback):
            return candidate
    return None


async def async_latest_krx_trading_date(
    value: date | str,
    *,
    allow_fallback: bool = True,
    max_scan_days: int = DEFAULT_KRX_CALENDAR_SCAN_DAYS,
) -> date | None:
    normalized = _normalize_date(value)
    for offset in range(max_scan_days + 1):
        candidate = normalized - timedelta(days=offset)
        if await async_is_krx_trading_day(candidate, allow_fallback=allow_fallback):
            return candidate
    return None


def previous_krx_trading_date(
    value: date | str,
    *,
    allow_fallback: bool = True,
    max_scan_days: int = DEFAULT_KRX_CALENDAR_SCAN_DAYS,
) -> date | None:
    normalized = _normalize_date(value)
    for offset in range(1, max_scan_days + 1):
        candidate = normalized - timedelta(days=offset)
        if is_krx_trading_day(candidate, allow_fallback=allow_fallback):
            return candidate
    return None


async def async_previous_krx_trading_date(
    value: date | str,
    *,
    allow_fallback: bool = True,
    max_scan_days: int = DEFAULT_KRX_CALENDAR_SCAN_DAYS,
) -> date | None:
    normalized = _normalize_date(value)
    for offset in range(1, max_scan_days + 1):
        candidate = normalized - timedelta(days=offset)
        if await async_is_krx_trading_day(candidate, allow_fallback=allow_fallback):
            return candidate
    return None


def next_krx_trading_date(
    value: date | str,
    *,
    allow_fallback: bool = True,
    max_scan_days: int = DEFAULT_KRX_CALENDAR_SCAN_DAYS,
) -> date | None:
    normalized = _normalize_date(value)
    for offset in range(1, max_scan_days + 1):
        candidate = normalized + timedelta(days=offset)
        if is_krx_trading_day(candidate, allow_fallback=allow_fallback):
            return candidate
    return None


async def async_next_krx_trading_date(
    value: date | str,
    *,
    allow_fallback: bool = True,
    max_scan_days: int = DEFAULT_KRX_CALENDAR_SCAN_DAYS,
) -> date | None:
    normalized = _normalize_date(value)
    for offset in range(1, max_scan_days + 1):
        candidate = normalized + timedelta(days=offset)
        if await async_is_krx_trading_day(candidate, allow_fallback=allow_fallback):
            return candidate
    return None


def next_krx_open_date_on_or_after(
    value: date | str,
    *,
    allow_fallback: bool = True,
    max_scan_days: int = DEFAULT_KRX_CALENDAR_SCAN_DAYS,
) -> date | None:
    normalized = _normalize_date(value)
    if is_krx_trading_day(normalized, allow_fallback=allow_fallback):
        return normalized
    return next_krx_trading_date(
        normalized,
        allow_fallback=allow_fallback,
        max_scan_days=max_scan_days,
    )


async def async_next_krx_open_date_on_or_after(
    value: date | str,
    *,
    allow_fallback: bool = True,
    max_scan_days: int = DEFAULT_KRX_CALENDAR_SCAN_DAYS,
) -> date | None:
    normalized = _normalize_date(value)
    if await async_is_krx_trading_day(normalized, allow_fallback=allow_fallback):
        return normalized
    return await async_next_krx_trading_date(
        normalized,
        allow_fallback=allow_fallback,
        max_scan_days=max_scan_days,
    )


def market_session_status_for_date(value: date | str) -> str:
    normalized = _normalize_date(value)
    if is_krx_trading_day(normalized):
        return MARKET_SESSION_CLOSED
    if normalized.weekday() >= 5:
        return MARKET_SESSION_WEEKEND
    return MARKET_SESSION_HOLIDAY


async def async_market_session_status_for_date(value: date | str) -> str:
    normalized = _normalize_date(value)
    if await async_is_krx_trading_day(normalized):
        return MARKET_SESSION_CLOSED
    if normalized.weekday() >= 5:
        return MARKET_SESSION_WEEKEND
    return MARKET_SESSION_HOLIDAY


def build_market_session_metadata(
    *,
    requested_date: date | str,
    current_state: Any,
) -> dict[str, Any]:
    normalized_requested_date = _normalize_date(requested_date)
    current_calendar_date = date.fromisoformat(
        str(
            getattr(current_state, 'current_calendar_date', None)
            or str(getattr(current_state, 'server_time', ''))[:10]
        )
    )
    requested_is_current_calendar_date = normalized_requested_date == current_calendar_date
    requested_lookup = get_krx_calendar_day(normalized_requested_date)
    requested_is_trading_day = requested_lookup.is_open
    latest_valid_trading_date = latest_krx_trading_date(normalized_requested_date)

    current_market_state = str(getattr(current_state, 'state', '') or '')
    current_market_reason = str(getattr(current_state, 'reason', '') or '') or None
    current_market_session = str(
        getattr(current_state, 'market_session', None) or market_session_status_for_date(current_calendar_date)
    )

    if requested_is_current_calendar_date:
        status = current_market_session
        reason = current_market_reason
    else:
        status = market_session_status_for_date(normalized_requested_date)
        reason = 'historical_requested_date' if status == MARKET_SESSION_CLOSED else status

    intraday_data_expected = (
        requested_is_current_calendar_date
        and requested_is_trading_day
        and bool(getattr(current_state, 'is_open', False))
    )
    quote_data_expected = intraday_data_expected
    recommendation_basis = (
        RECOMMENDATION_BASIS_LIVE_INTRADAY
        if requested_is_current_calendar_date
        and requested_is_trading_day
        and bool(getattr(current_state, 'is_open', False))
        else RECOMMENDATION_BASIS_LATEST_COMPLETED_EOD
    )

    return {
        'requested_date': normalized_requested_date.isoformat(),
        'current_calendar_date': current_calendar_date.isoformat(),
        'latest_valid_trading_date': (
            latest_valid_trading_date.isoformat() if latest_valid_trading_date is not None else None
        ),
        'requested_date_is_current_calendar_date': requested_is_current_calendar_date,
        'is_trading_day': requested_is_trading_day,
        'requested_date_is_trading_day': requested_is_trading_day,
        'current_calendar_date_is_trading_day': bool(getattr(current_state, 'is_trading_day', False)),
        'status': status,
        'market_state': current_market_state if requested_is_current_calendar_date else None,
        'current_market_state': current_market_state,
        'current_market_session': current_market_session,
        'reason': reason,
        'intraday_data_expected': intraday_data_expected,
        'quote_data_expected': quote_data_expected,
        'recommendation_basis': recommendation_basis,
        'calendar_source': requested_lookup.calendar_source,
        'calendar_source_status': requested_lookup.calendar_source_status,
        'calendar_source_error': requested_lookup.calendar_source_error,
        'current_calendar_source': getattr(current_state, 'calendar_source', None),
        'current_calendar_source_status': getattr(current_state, 'calendar_source_status', None),
        'current_calendar_source_error': getattr(current_state, 'calendar_source_error', None),
    }


async def async_build_market_session_metadata(
    *,
    requested_date: date | str,
    current_state: Any,
) -> dict[str, Any]:
    normalized_requested_date = _normalize_date(requested_date)
    current_calendar_date = date.fromisoformat(
        str(
            getattr(current_state, 'current_calendar_date', None)
            or str(getattr(current_state, 'server_time', ''))[:10]
        )
    )
    requested_is_current_calendar_date = normalized_requested_date == current_calendar_date
    requested_lookup = await async_get_krx_calendar_day(normalized_requested_date)
    requested_is_trading_day = requested_lookup.is_open
    latest_valid_trading_date = await async_latest_krx_trading_date(normalized_requested_date)

    current_market_state = str(getattr(current_state, 'state', '') or '')
    current_market_reason = str(getattr(current_state, 'reason', '') or '') or None
    current_market_session = str(getattr(current_state, 'market_session', None) or '')
    if not current_market_session:
        current_market_session = await async_market_session_status_for_date(current_calendar_date)

    if requested_is_current_calendar_date:
        status = current_market_session
        reason = current_market_reason
    else:
        status = await async_market_session_status_for_date(normalized_requested_date)
        reason = 'historical_requested_date' if status == MARKET_SESSION_CLOSED else status

    intraday_data_expected = (
        requested_is_current_calendar_date
        and requested_is_trading_day
        and bool(getattr(current_state, 'is_open', False))
    )
    quote_data_expected = intraday_data_expected
    recommendation_basis = (
        RECOMMENDATION_BASIS_LIVE_INTRADAY
        if requested_is_current_calendar_date
        and requested_is_trading_day
        and bool(getattr(current_state, 'is_open', False))
        else RECOMMENDATION_BASIS_LATEST_COMPLETED_EOD
    )

    return {
        'requested_date': normalized_requested_date.isoformat(),
        'current_calendar_date': current_calendar_date.isoformat(),
        'latest_valid_trading_date': (
            latest_valid_trading_date.isoformat() if latest_valid_trading_date is not None else None
        ),
        'requested_date_is_current_calendar_date': requested_is_current_calendar_date,
        'is_trading_day': requested_is_trading_day,
        'requested_date_is_trading_day': requested_is_trading_day,
        'current_calendar_date_is_trading_day': bool(getattr(current_state, 'is_trading_day', False)),
        'status': status,
        'market_state': current_market_state if requested_is_current_calendar_date else None,
        'current_market_state': current_market_state,
        'current_market_session': current_market_session,
        'reason': reason,
        'intraday_data_expected': intraday_data_expected,
        'quote_data_expected': quote_data_expected,
        'recommendation_basis': recommendation_basis,
        'calendar_source': requested_lookup.calendar_source,
        'calendar_source_status': requested_lookup.calendar_source_status,
        'calendar_source_error': requested_lookup.calendar_source_error,
        'current_calendar_source': getattr(current_state, 'calendar_source', None),
        'current_calendar_source_status': getattr(current_state, 'calendar_source_status', None),
        'current_calendar_source_error': getattr(current_state, 'calendar_source_error', None),
    }
