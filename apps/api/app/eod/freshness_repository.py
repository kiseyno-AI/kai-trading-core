from __future__ import annotations

from datetime import date, datetime, time, timedelta
from typing import Any
from zoneinfo import ZoneInfo

from sqlalchemy import func, select

from app.core.settings import settings
from app.db import session_scope
from app.db_models import DartDisclosureEvent, DartFundamentalsRefreshRun, DartFundamentalsSnapshot, DailyPriceBar

SEOUL_TZ = ZoneInfo('Asia/Seoul')
FUNDAMENTALS_CAPTURE_FAILED = 'failed'
LATEST_AVAILABLE_FUNDAMENTALS_RUN_STATUSES = ('completed', 'partial', 'completed_with_errors')


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


def _serialize_fundamentals_refresh_run(row: DartFundamentalsRefreshRun) -> dict[str, Any]:
    return {
        'id': row.id,
        'market_date': row.market_date.isoformat(),
        'trigger': row.trigger,
        'status': row.status,
        'fs_div': row.fs_div,
        'force': row.force,
        'symbol_limit': row.symbol_limit,
        'symbol_offset': row.symbol_offset,
        'symbols_total': row.symbols_total,
        'symbols_checked': row.symbols_checked,
        'symbols_missing': row.symbols_missing,
        'symbols_stale': row.symbols_stale,
        'symbols_fresh': row.symbols_fresh,
        'symbols_refreshed': row.symbols_refreshed,
        'symbols_failed': row.symbols_failed,
        'events_detected': row.events_detected,
        'events_captured': row.events_captured,
        'events_failed': row.events_failed,
        'error_message': row.error_message,
        'run_metadata': row.run_metadata,
        'started_at': row.started_at.isoformat() if row.started_at else None,
        'completed_at': row.completed_at.isoformat() if row.completed_at else None,
    }


def _load_latest_fundamentals_refresh_run(
    *,
    market_date: str | None = None,
    statuses: tuple[str, ...] | None = None,
) -> dict[str, Any] | None:
    fs_div = settings.dart_fundamentals_default_fs_div.strip().upper()
    with session_scope() as session:
        stmt = select(DartFundamentalsRefreshRun).where(DartFundamentalsRefreshRun.fs_div == fs_div)
        if market_date is not None:
            stmt = stmt.where(DartFundamentalsRefreshRun.market_date == date.fromisoformat(market_date))
        if statuses:
            stmt = stmt.where(DartFundamentalsRefreshRun.status.in_(statuses))
        row = session.execute(
            stmt.order_by(DartFundamentalsRefreshRun.started_at.desc(), DartFundamentalsRefreshRun.id.desc()).limit(1)
        ).scalar_one_or_none()
        if row is None:
            return None
        return _serialize_fundamentals_refresh_run(row)


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
