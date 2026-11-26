from dataclasses import dataclass
from datetime import date, datetime
from zoneinfo import ZoneInfo

import exchange_calendars


US_INDEX_HISTORY_RETURNS = 20


@dataclass(frozen=True)
class CompletedUSSession:
    session_date: date
    close_at: datetime
    previous_session_date: date
    previous_close_at: datetime


def _completed_session(calendar, session) -> CompletedUSSession:
    previous = calendar.previous_session(session)
    return CompletedUSSession(
        session_date=session.date(),
        close_at=calendar.session_close(session).to_pydatetime(),
        previous_session_date=previous.date(),
        previous_close_at=calendar.session_close(previous).to_pydatetime(),
    )


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
    return _completed_session(calendar, session)


def prior_completed_us_sessions(current: CompletedUSSession) -> tuple[CompletedUSSession, ...]:
    """Freeze the 20 immediately preceding XNYS return windows, oldest first."""
    calendar = exchange_calendars.get_calendar('XNYS')
    session = calendar.date_to_session(current.session_date)
    if _completed_session(calendar, session) != current:
        raise ValueError('Current US session window must match the exchange calendar')
    history = []
    for _ in range(US_INDEX_HISTORY_RETURNS):
        session = calendar.previous_session(session)
        history.append(_completed_session(calendar, session))
    return tuple(reversed(history))
