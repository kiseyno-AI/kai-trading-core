from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, time
from zoneinfo import ZoneInfo

import pandas as pd

from app.market.session import KRX_EXCHANGE_CALENDAR

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


def get_market_state(now: datetime | None = None) -> MarketState:
    now = now.astimezone(KST) if now else datetime.now(KST)
    current_calendar_date = now.date().isoformat()
    weekday = now.weekday()
    session_open = now.replace(hour=9, minute=0, second=0, microsecond=0)
    session_close = now.replace(hour=15, minute=30, second=0, microsecond=0)
    current_label = pd.Timestamp(current_calendar_date)

    latest_valid_trading_date = None
    try:
        if KRX_EXCHANGE_CALENDAR.is_session(current_label):
            latest_valid_trading_date = current_calendar_date
        else:
            latest_valid_trading_date = KRX_EXCHANGE_CALENDAR.date_to_session(
                current_label,
                direction='previous',
            ).date().isoformat()
    except Exception:
        latest_valid_trading_date = None

    if weekday >= 5:
        return MarketState(
            market='KRX',
            state='WEEKEND',
            is_trading_day=False,
            is_open=False,
            server_time=now.isoformat(),
            session_open_at=None,
            session_close_at=None,
            reason='weekend',
            current_calendar_date=current_calendar_date,
            latest_valid_trading_date=latest_valid_trading_date,
            market_session='weekend',
        )

    if not KRX_EXCHANGE_CALENDAR.is_session(current_label):
        return MarketState(
            market='KRX',
            state='HOLIDAY',
            is_trading_day=False,
            is_open=False,
            server_time=now.isoformat(),
            session_open_at=None,
            session_close_at=None,
            reason='holiday',
            current_calendar_date=current_calendar_date,
            latest_valid_trading_date=latest_valid_trading_date,
            market_session='holiday',
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
    )
