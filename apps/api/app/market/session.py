from __future__ import annotations

from datetime import date
from typing import Any

import exchange_calendars as xcals
import pandas as pd

MARKET_SESSION_OPEN = 'open'
MARKET_SESSION_CLOSED = 'closed'
MARKET_SESSION_WEEKEND = 'weekend'
MARKET_SESSION_HOLIDAY = 'holiday'
MARKET_SESSION_NOT_APPLICABLE = 'not_applicable'

RECOMMENDATION_BASIS_LIVE_INTRADAY = 'live_intraday'
RECOMMENDATION_BASIS_LATEST_COMPLETED_EOD = 'latest_completed_eod'

KRX_EXCHANGE_CALENDAR = xcals.get_calendar('XKRX')


def _normalize_date(value: date | str) -> date:
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value))


def is_krx_trading_day(value: date | str) -> bool:
    normalized = _normalize_date(value)
    try:
        return bool(KRX_EXCHANGE_CALENDAR.is_session(pd.Timestamp(normalized.isoformat())))
    except Exception:
        return False


def latest_krx_trading_date(value: date | str) -> date | None:
    normalized = _normalize_date(value)
    label = pd.Timestamp(normalized.isoformat())
    try:
        if KRX_EXCHANGE_CALENDAR.is_session(label):
            return normalized
        return KRX_EXCHANGE_CALENDAR.date_to_session(label, direction='previous').date()
    except Exception:
        return None


def previous_krx_trading_date(value: date | str) -> date | None:
    normalized = _normalize_date(value)
    label = pd.Timestamp(normalized.isoformat())
    try:
        if KRX_EXCHANGE_CALENDAR.is_session(label):
            return KRX_EXCHANGE_CALENDAR.previous_session(label).date()
        return KRX_EXCHANGE_CALENDAR.date_to_session(label, direction='previous').date()
    except Exception:
        return None


def market_session_status_for_date(value: date | str) -> str:
    normalized = _normalize_date(value)
    if is_krx_trading_day(normalized):
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
    requested_is_trading_day = is_krx_trading_day(normalized_requested_date)
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

    intraday_data_expected = requested_is_trading_day and (
        not requested_is_current_calendar_date or bool(getattr(current_state, 'is_open', False))
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
    }
