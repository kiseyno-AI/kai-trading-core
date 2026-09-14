from __future__ import annotations

from collections.abc import Mapping
from datetime import date, datetime, time, timezone
from typing import Any, Callable
from zoneinfo import ZoneInfo

from app.market.session import next_krx_trading_date


KRX_TIME_ZONE = ZoneInfo('Asia/Seoul')
KRX_REGULAR_SESSION_OPEN = time(9, 0)


def _snapshot_value(snapshot: Any, field: str) -> Any:
    if isinstance(snapshot, Mapping):
        return snapshot.get(field)
    return getattr(snapshot, field, None)


def decision_snapshot_observed_at_utc(snapshot: Any) -> datetime | None:
    """Return the canonical aware observation time, normalized to UTC."""
    observed_at = _snapshot_value(snapshot, 'feedback_received_at')
    if observed_at is None:
        observed_at = _snapshot_value(snapshot, 'created_at')
    if not isinstance(observed_at, datetime) or observed_at.utcoffset() is None:
        return None
    return observed_at.astimezone(timezone.utc)


def decision_snapshot_target_open_at_utc(target_trade_date: date) -> datetime:
    """Return the canonical KRX regular-session open for a trade date."""
    return datetime.combine(
        target_trade_date,
        KRX_REGULAR_SESSION_OPEN,
        tzinfo=KRX_TIME_ZONE,
    ).astimezone(timezone.utc)


def is_decision_snapshot_pre_target_open(
    snapshot: Any,
    *,
    target_trade_date: date,
) -> bool:
    """Accept a decision only when it existed strictly before the KRX open."""
    observed_at = decision_snapshot_observed_at_utc(snapshot)
    if observed_at is None:
        return False
    target_open_at = decision_snapshot_target_open_at_utc(target_trade_date)
    return observed_at < target_open_at


def is_decision_snapshot_valid_for_source_date(
    snapshot: Any,
    *,
    next_trading_date_resolver: Callable[[date], date | None] | None = None,
) -> bool:
    """Validate a snapshot against the open following its source market date."""
    source_market_date = _snapshot_value(snapshot, 'market_date')
    if (
        not isinstance(source_market_date, date)
        or isinstance(source_market_date, datetime)
    ):
        return False
    resolver = next_trading_date_resolver or next_krx_trading_date
    target_trade_date = resolver(source_market_date)
    if target_trade_date is None:
        return False
    return is_decision_snapshot_pre_target_open(
        snapshot,
        target_trade_date=target_trade_date,
    )
