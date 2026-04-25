from __future__ import annotations

from datetime import date, datetime, time, timedelta
from typing import Any
from zoneinfo import ZoneInfo

from sqlalchemy import func, select

from app.core.settings import settings
from app.dart.fundamentals_store import DartFundamentalsRepository
from app.db import session_scope
from app.db_models import DartDisclosureEvent, DartFundamentalsSnapshot, DailyPriceBar

SEOUL_TZ = ZoneInfo('Asia/Seoul')
FUNDAMENTALS_CAPTURE_FAILED = 'failed'
LATEST_AVAILABLE_FUNDAMENTALS_RUN_STATUSES = ('completed', 'partial', 'completed_with_errors')
_fundamentals_repository = DartFundamentalsRepository()


def _seoul_day_bounds(value: str | date) -> tuple[datetime, datetime]:
    market_date = date.fromisoformat(value) if isinstance(value, str) else value
    start = datetime.combine(market_date, time.min, tzinfo=SEOUL_TZ)
    return start, start + timedelta(days=1)


def _load_daily_fundamentals_stage_counts(market_date: str) -> dict[str, int]:
    start_at, end_at = _seoul_day_bounds(market_date)
    fs_div = settings.dart_fundamentals_default_fs_div.strip().upper()

    with session_scope() as session:
        succeeded_codes = set(
            session.execute(
                select(func.distinct(DartFundamentalsSnapshot.stock_code))
                .select_from(DartFundamentalsSnapshot)
                .where(DartFundamentalsSnapshot.fs_div == fs_div)
                .where(DartFundamentalsSnapshot.stock_code.is_not(None))
                .where(DartFundamentalsSnapshot.updated_at >= start_at)
                .where(DartFundamentalsSnapshot.updated_at < end_at)
            ).scalars()
        )
        failed_codes = set(
            session.execute(
                select(func.distinct(DartDisclosureEvent.stock_code))
                .select_from(DartDisclosureEvent)
                .where(DartDisclosureEvent.is_financial_report.is_(True))
                .where(DartDisclosureEvent.stock_code.is_not(None))
                .where(DartDisclosureEvent.fundamentals_capture_status == FUNDAMENTALS_CAPTURE_FAILED)
                .where(DartDisclosureEvent.fundamentals_last_attempt_at >= start_at)
                .where(DartDisclosureEvent.fundamentals_last_attempt_at < end_at)
            ).scalars()
        )

    return {
        'symbols_succeeded': len(succeeded_codes),
        'symbols_failed': len(failed_codes),
        'symbols_total': len(succeeded_codes | failed_codes),
    }


def _load_latest_fundamentals_refresh_run(
    *,
    market_date: str | None = None,
    statuses: tuple[str, ...] | None = None,
) -> dict[str, Any] | None:
    fs_div = settings.dart_fundamentals_default_fs_div.strip().upper()
    return _fundamentals_repository.get_latest_refresh_run(
        market_date=date.fromisoformat(market_date) if market_date is not None else None,
        fs_div=fs_div,
        statuses=statuses,
    )


def _load_today_fundamentals_refresh_run(market_date: str) -> dict[str, Any] | None:
    return _load_latest_fundamentals_refresh_run(market_date=market_date)


def _load_latest_available_fundamentals_refresh_run() -> dict[str, Any] | None:
    return _load_latest_fundamentals_refresh_run(statuses=LATEST_AVAILABLE_FUNDAMENTALS_RUN_STATUSES)


def _load_latest_price_bar_updated_at(trade_date: str) -> str | None:
    with session_scope() as session:
        updated_at = session.execute(
            select(func.max(DailyPriceBar.captured_at)).where(DailyPriceBar.trade_date == date.fromisoformat(trade_date))
        ).scalar_one_or_none()
    return updated_at.isoformat() if updated_at else None
