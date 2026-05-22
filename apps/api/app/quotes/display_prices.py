from __future__ import annotations

import logging
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any

from app.market_data_store import load_current_quote, load_daily_bars_for_symbol
from app.quotes.cache import quote_cache

logger = logging.getLogger(__name__)

DISPLAY_PRICE_SOURCE_CACHED_QUOTE = 'cached_quote'
DISPLAY_PRICE_SOURCE_DAILY_CLOSE = 'daily_close'
DISPLAY_PRICE_SOURCE_BACKGROUND_REST_QUOTE = 'background_rest_quote'
DISPLAY_PRICE_SOURCE_REALTIME = 'kis_realtime_ws'

DISPLAY_PRICE_STATUS_AVAILABLE = 'available'
DISPLAY_PRICE_STATUS_FALLBACK = 'fallback'
DISPLAY_PRICE_STATUS_STALE = 'stale'
DISPLAY_PRICE_STATUS_LIMITED = 'limited'
DISPLAY_PRICE_STATUS_UNAVAILABLE = 'unavailable'

DISPLAY_FALLBACK_SOURCES = {
    DISPLAY_PRICE_SOURCE_CACHED_QUOTE,
    DISPLAY_PRICE_SOURCE_BACKGROUND_REST_QUOTE,
    DISPLAY_PRICE_SOURCE_DAILY_CLOSE,
}
MAX_RECENT_DISPLAY_FALLBACK_AGE_DAYS = 3


def _normalize_symbol(value: Any) -> str | None:
    normalized = ''.join(ch for ch in str(value or '').strip().upper() if ch.isalnum())
    return normalized or None


def _parse_datetime(value: Any) -> datetime | None:
    if not value:
        return None
    normalized = str(value).replace('Z', '+00:00')
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _parse_date(value: Any) -> date | None:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if not value:
        return None
    try:
        return date.fromisoformat(str(value)[:10])
    except ValueError:
        return None


def _safe_decimal(value: Any) -> Decimal | None:
    if value in (None, ''):
        return None
    if isinstance(value, Decimal):
        return value
    try:
        return Decimal(str(value).replace(',', ''))
    except (InvalidOperation, ValueError):
        return None


def _to_json_number(value: Decimal | None) -> int | float | None:
    if value is None:
        return None
    if value == value.to_integral():
        return int(value)
    return float(value)


def _positive_price(*values: Any) -> Decimal | None:
    for value in values:
        price = _safe_decimal(value)
        if price is not None and price > Decimal('0'):
            return price
    return None


def _coalesce_iso_datetime(*values: Any) -> str | None:
    for value in values:
        parsed = _parse_datetime(value)
        if parsed is not None:
            return parsed.isoformat()
    return None


def _empty_display_fields(
    *,
    realtime_fields: dict[str, Any],
    status: str,
) -> dict[str, Any]:
    return {
        **realtime_fields,
        'current_price': None,
        'price_as_of': None,
        'price_received_at': None,
        'price_source': None,
        'price_status': status,
        'display_price_available': False,
    }


def _normalize_realtime_fields(realtime_fields: dict[str, Any] | None) -> dict[str, Any]:
    fields = dict(realtime_fields or {})
    fields.setdefault('current_price', None)
    fields.setdefault('price_as_of', None)
    fields.setdefault('price_received_at', None)
    fields.setdefault('price_source', None)
    fields.setdefault('price_status', DISPLAY_PRICE_STATUS_UNAVAILABLE)
    fields.setdefault('realtime_subscribed', False)
    fields.setdefault('realtime_subscription_reason', None)
    fields.setdefault('display_price_available', fields.get('current_price') is not None)
    return fields


def _cached_quote_display_fields(
    symbol: str,
    *,
    market_date: date,
) -> dict[str, Any] | None:
    candidates: list[tuple[dict[str, Any], str | None, str]] = []
    memory_payload = quote_cache.get(symbol)
    if memory_payload:
        candidates.append((memory_payload, quote_cache.get_updated_at(symbol), 'in_memory_quote_cache'))

    try:
        persisted_payload = load_current_quote(symbol)
    except Exception as exc:
        logger.warning('Current quote display fallback lookup failed for %s: %s', symbol, exc)
        persisted_payload = None
    if persisted_payload:
        candidates.append(
            (
                persisted_payload,
                persisted_payload.get('cached_at') or persisted_payload.get('captured_at'),
                'current_quote_cache',
            )
        )

    for payload, cached_at, reason in candidates:
        price = _positive_price(payload.get('current_price'), payload.get('price'))
        if price is None:
            continue
        cached_timestamp = _parse_datetime(cached_at)
        trade_date = _parse_date(payload.get('trade_date'))
        fallback_date = trade_date or (cached_timestamp.date() if cached_timestamp else None)
        if fallback_date is None:
            continue
        age_days = (market_date - fallback_date).days
        if age_days < 0:
            age_days = 0
        if age_days > MAX_RECENT_DISPLAY_FALLBACK_AGE_DAYS:
            continue
        status = (
            DISPLAY_PRICE_STATUS_FALLBACK
            if fallback_date == market_date
            else DISPLAY_PRICE_STATUS_STALE
        )
        as_of = _coalesce_iso_datetime(
            payload.get('price_as_of'),
            payload.get('as_of'),
            payload.get('cached_at'),
            payload.get('captured_at'),
            cached_at,
        )
        return {
            'current_price': _to_json_number(price),
            'price_as_of': as_of or fallback_date.isoformat(),
            'price_received_at': _coalesce_iso_datetime(
                payload.get('price_received_at'),
                payload.get('cached_at'),
                payload.get('captured_at'),
                cached_at,
            ),
            'price_source': DISPLAY_PRICE_SOURCE_CACHED_QUOTE,
            'price_status': status,
            'display_price_available': True,
            'display_price_fallback_reason': reason,
        }
    return None


def _daily_close_display_fields(
    symbol: str,
    *,
    market_date: date,
) -> dict[str, Any] | None:
    try:
        bars = load_daily_bars_for_symbol(symbol, end_date=market_date, limit=2)
    except Exception as exc:
        logger.warning('Daily close display fallback lookup failed for %s: %s', symbol, exc)
        return None
    if not bars:
        return None

    latest_bar = bars[-1]
    latest_trade_date = _parse_date(latest_bar.get('trade_date'))
    if latest_trade_date is None:
        return None
    age_days = (market_date - latest_trade_date).days
    if age_days < 0 or age_days > MAX_RECENT_DISPLAY_FALLBACK_AGE_DAYS:
        return None
    price = _positive_price(latest_bar.get('close_price'))
    if price is None:
        return None
    return {
        'current_price': _to_json_number(price),
        'price_as_of': latest_trade_date.isoformat(),
        'price_received_at': latest_bar.get('captured_at'),
        'price_source': DISPLAY_PRICE_SOURCE_DAILY_CLOSE,
        'price_status': (
            DISPLAY_PRICE_STATUS_FALLBACK
            if latest_trade_date == market_date
            else DISPLAY_PRICE_STATUS_STALE
        ),
        'display_price_available': True,
        'display_price_fallback_reason': 'daily_price_bar_close',
    }


def build_display_price_fields(
    symbol: Any,
    *,
    market_date: date,
    realtime_fields: dict[str, Any] | None,
) -> dict[str, Any]:
    """Merge realtime diagnostic fields with dashboard-safe local fallback prices."""
    normalized_symbol = _normalize_symbol(symbol)
    fields = _normalize_realtime_fields(realtime_fields)
    realtime_price = _positive_price(fields.get('current_price'))
    if normalized_symbol and realtime_price is not None:
        fields['current_price'] = _to_json_number(realtime_price)
        fields['display_price_available'] = True
        fields.setdefault('display_price_fallback_reason', None)
        return fields

    if normalized_symbol:
        fallback = _cached_quote_display_fields(normalized_symbol, market_date=market_date)
        if fallback is None:
            fallback = _daily_close_display_fields(normalized_symbol, market_date=market_date)
        if fallback is not None:
            return {
                **fields,
                **fallback,
            }

    realtime_status = str(fields.get('price_status') or '').strip().lower()
    subscription_reason = str(fields.get('realtime_subscription_reason') or '').strip().lower()
    status = (
        DISPLAY_PRICE_STATUS_LIMITED
        if realtime_status == DISPLAY_PRICE_STATUS_LIMITED or subscription_reason == 'capacity_limited'
        else DISPLAY_PRICE_STATUS_UNAVAILABLE
    )
    return _empty_display_fields(realtime_fields=fields, status=status)
