from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, time
from zoneinfo import ZoneInfo

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


def get_market_state(now: datetime | None = None) -> MarketState:
    now = now.astimezone(KST) if now else datetime.now(KST)
    weekday = now.weekday()
    session_open = now.replace(hour=9, minute=0, second=0, microsecond=0)
    session_close = now.replace(hour=15, minute=30, second=0, microsecond=0)

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
        )

    if now < session_open:
        state = 'PREOPEN'
        is_open = False
    elif now <= session_close:
        state = 'OPEN'
        is_open = True
    else:
        state = 'CLOSED'
        is_open = False

    return MarketState(
        market='KRX',
        state=state,
        is_trading_day=True,
        is_open=is_open,
        server_time=now.isoformat(),
        session_open_at=session_open.isoformat(),
        session_close_at=session_close.isoformat(),
    )
