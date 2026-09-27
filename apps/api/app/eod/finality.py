from __future__ import annotations

from datetime import date, datetime, time, timedelta

from app.core.settings import settings
from app.market.clock import KST


KRX_REGULAR_SESSION_CLOSE = time(15, 30)


def current_krx_datetime() -> datetime:
    return datetime.now(KST)


def _normalize_date(value: str | date) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    raw = str(value).strip()
    for pattern in ('%Y-%m-%d', '%Y%m%d'):
        try:
            return datetime.strptime(raw, pattern).date()
        except ValueError:
            continue
    raise ValueError('KRX daily write date must use YYYY-MM-DD or YYYYMMDD')


def validate_finalized_krx_write_date(
    value: str | date,
    *,
    now: datetime | None = None,
    operation: str = 'KRX daily data write',
) -> date:
    """Reject future or not-yet-final same-day KRX daily writes."""

    target_date = _normalize_date(value)
    observed_at = now or current_krx_datetime()
    if observed_at.tzinfo is None:
        raise ValueError('KRX daily write clock must be timezone-aware')
    now_kst = observed_at.astimezone(KST)
    if target_date > now_kst.date():
        raise ValueError(f'{operation} cannot target a future date')
    if target_date < now_kst.date():
        return target_date

    eligible_at = datetime.combine(
        target_date,
        KRX_REGULAR_SESSION_CLOSE,
        tzinfo=KST,
    ) + timedelta(minutes=settings.eod_auto_pykrx_after_close_minutes)
    if now_kst < eligible_at:
        raise ValueError(
            f'same-day {operation} is blocked until {eligible_at.isoformat()}'
        )
    return target_date
