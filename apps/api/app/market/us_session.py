from dataclasses import dataclass
from datetime import date, datetime
from zoneinfo import ZoneInfo

import exchange_calendars


@dataclass(frozen=True)
class CompletedUSSession:
    session_date: date
    close_at: datetime
    previous_session_date: date
    previous_close_at: datetime


def latest_completed_us_session(*, as_of: datetime) -> CompletedUSSession:
    """Select scheduled XNYS closes; this does not certify provider data readiness."""
    if not isinstance(as_of, datetime) or as_of.utcoffset() is None:
        raise ValueError('as_of must be a timezone-aware datetime')
    calendar = exchange_calendars.get_calendar('XNYS')
    session = calendar.date_to_session(
        as_of.astimezone(ZoneInfo('America/New_York')).date(), direction='previous',
    )
    if calendar.session_close(session) > as_of:
        session = calendar.previous_session(session)
    previous = calendar.previous_session(session)
    return CompletedUSSession(
        session_date=session.date(),
        close_at=calendar.session_close(session).to_pydatetime(),
        previous_session_date=previous.date(),
        previous_close_at=calendar.session_close(previous).to_pydatetime(),
    )
