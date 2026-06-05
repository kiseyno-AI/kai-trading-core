from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from zoneinfo import ZoneInfo

from app.market.session import get_krx_calendar_day, latest_krx_trading_date

KST = ZoneInfo('Asia/Seoul')


@dataclass
class MarketState:
    market: str
    state: str
    is_trading_day: bool
    is_open: bool
    server_time: str
    session_open_at: str | None
    session_close_at: str | None
    reason: str | None = None
    current_calendar_date: str | None = None
    latest_valid_trading_date: str | None = None
    market_session: str | None = None
    calendar_source: str | None = None
    calendar_source_status: str | None = None
    calendar_source_error: str | None = None


def get_market_state(now: datetime | None = None) -> MarketState:
    now = now.astimezone(KST) if now else datetime.now(KST)
    current_calendar_date = now.date().isoformat()
    weekday = now.weekday()
    session_open = now.replace(hour=9, minute=0, second=0, microsecond=0)
    session_close = now.replace(hour=15, minute=30, second=0, microsecond=0)
    calendar_day = get_krx_calendar_day(now.date(), allow_fallback=True)
    latest_valid_date = latest_krx_trading_date(now.date(), allow_fallback=True)
    latest_valid_trading_date = latest_valid_date.isoformat() if latest_valid_date is not None else None

    if not calendar_day.is_open:
        state = 'WEEKEND' if weekday >= 5 else 'HOLIDAY'
        reason = 'weekend' if weekday >= 5 else 'holiday'
        market_session = 'weekend' if weekday >= 5 else 'holiday'
        return MarketState(
            market='KRX',
            state=state,
            is_trading_day=False,
            is_open=False,
            server_time=now.isoformat(),
            session_open_at=None,
            session_close_at=None,
            reason=reason,
            current_calendar_date=current_calendar_date,
            latest_valid_trading_date=latest_valid_trading_date,
            market_session=market_session,
            calendar_source=calendar_day.calendar_source,
            calendar_source_status=calendar_day.calendar_source_status,
            calendar_source_error=calendar_day.calendar_source_error,
        )

    if now < session_open:
        state = 'PREOPEN'
        is_open = False
        market_session = 'closed'
    elif now <= session_close:
        state = 'OPEN'
        is_open = True
        market_session = 'open'
    else:
        state = 'CLOSED'
        is_open = False
        market_session = 'closed'

    return MarketState(
        market='KRX',
        state=state,
        is_trading_day=True,
        is_open=is_open,
        server_time=now.isoformat(),
        session_open_at=session_open.isoformat(),
        session_close_at=session_close.isoformat(),
        current_calendar_date=current_calendar_date,
        latest_valid_trading_date=latest_valid_trading_date,
        market_session=market_session,
        calendar_source=calendar_day.calendar_source,
        calendar_source_status=calendar_day.calendar_source_status,
        calendar_source_error=calendar_day.calendar_source_error,
    )
