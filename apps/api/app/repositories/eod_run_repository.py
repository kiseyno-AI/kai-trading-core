from __future__ import annotations

from contextlib import contextmanager
from datetime import date, datetime
from typing import Any, Sequence

from sqlalchemy import func, select, text

from app.db import engine, session_scope
from app.db_models import DailyActionSummary, DailyPriceBar, DailyTechnicalSnapshot, DailyTrendingRank, EODCollectionRun, Symbol


def _parse_trade_date(value: str) -> date:
    return date.fromisoformat(value)


def _normalize_trade_date(value: str | date) -> date:
    if isinstance(value, date):
        return value
    return _parse_trade_date(value)


def _serialize_eod_collection_run(row: EODCollectionRun) -> dict[str, Any]:
    return {
        'id': row.id,
        'market': row.market,
        'trade_date': row.trade_date.isoformat(),
        'source': row.source,
        'trigger': row.trigger,
        'status': row.status,
        'symbols_total': row.symbols_total,
        'symbols_succeeded': row.symbols_succeeded,
        'symbols_failed': row.symbols_failed,
        'error_message': row.error_message,
        'started_at': row.started_at.isoformat() if row.started_at else None,
        'completed_at': row.completed_at.isoformat() if row.completed_at else None,
        'run_metadata': row.run_metadata,
    }


def create_eod_collection_run(
    *,
    market: str,
    trade_date: str,
    symbols_total: int,
    trigger: str,
    source: str = 'kis',
    run_metadata: dict | None = None,
) -> int:
    with session_scope() as session:
        run = EODCollectionRun(
            market=market,
            trade_date=_parse_trade_date(trade_date),
            source=source,
            trigger=trigger,
            status='running',
            symbols_total=symbols_total,
            run_metadata=run_metadata,
        )
        session.add(run)
        session.flush()
        return run.id


def load_latest_eod_collection_run(
    *,
    market: str,
    trade_date: str,
    source: str | None = None,
    statuses: Sequence[str] | None = None,
) -> dict | None:
    rows = list_eod_collection_runs(
        market=market,
        trade_date=trade_date,
        source=source,
        statuses=statuses,
        limit=1,
    )
    if not rows:
        return None
    return rows[0]


def list_eod_collection_runs(
    *,
    market: str,
    trade_date: str,
    source: str | None = None,
    statuses: Sequence[str] | None = None,
    limit: int = 10,
) -> list[dict[str, Any]]:
    with session_scope() as session:
        stmt = select(EODCollectionRun).where(
            EODCollectionRun.market == market,
            EODCollectionRun.trade_date == _parse_trade_date(trade_date),
        )
        if source:
            stmt = stmt.where(EODCollectionRun.source == source)
        if statuses:
            stmt = stmt.where(EODCollectionRun.status.in_(statuses))
        rows = session.execute(
            stmt.order_by(EODCollectionRun.started_at.desc(), EODCollectionRun.id.desc()).limit(limit)
        ).scalars()
        return [_serialize_eod_collection_run(row) for row in rows]


def load_most_complete_eod_collection_run(
    *,
    market: str,
    trade_date: str,
    source: str | None = None,
    statuses: Sequence[str] | None = None,
    min_symbols_succeeded: int = 1,
) -> dict[str, Any] | None:
    with session_scope() as session:
        stmt = (
            select(EODCollectionRun)
            .where(EODCollectionRun.market == market)
            .where(EODCollectionRun.trade_date == _parse_trade_date(trade_date))
            .where(EODCollectionRun.symbols_succeeded >= min_symbols_succeeded)
        )
        if source:
            stmt = stmt.where(EODCollectionRun.source == source)
        if statuses:
            stmt = stmt.where(EODCollectionRun.status.in_(statuses))
        row = session.execute(
            stmt.order_by(
                EODCollectionRun.symbols_succeeded.desc(),
                EODCollectionRun.symbols_total.desc(),
                EODCollectionRun.started_at.desc(),
                EODCollectionRun.id.desc(),
            ).limit(1)
        ).scalar_one_or_none()
        if row is None:
            return None
        return _serialize_eod_collection_run(row)


def count_eod_collection_runs(
    *,
    market: str,
    trade_date: str,
    source: str | None = None,
    trigger_prefix: str | None = None,
) -> int:
    with session_scope() as session:
        stmt = select(func.count(EODCollectionRun.id)).where(
            EODCollectionRun.market == market,
            EODCollectionRun.trade_date == _parse_trade_date(trade_date),
        )
        if source:
            stmt = stmt.where(EODCollectionRun.source == source)
        if trigger_prefix:
            escaped_prefix = trigger_prefix.replace('\\', '\\\\')
            escaped_prefix = escaped_prefix.replace('%', '\\%').replace('_', '\\_')
            stmt = stmt.where(EODCollectionRun.trigger.like(f'{escaped_prefix}%', escape='\\'))
        return int(session.execute(stmt).scalar_one())


def complete_eod_collection_run(
    run_id: int,
    *,
    status: str,
    symbols_succeeded: int,
    symbols_failed: int,
    error_message: str | None = None,
    run_metadata: dict | None = None,
) -> None:
    with session_scope() as session:
        run = session.get(EODCollectionRun, run_id)
        if run is None:
            return
        run.status = status
        run.symbols_succeeded = symbols_succeeded
        run.symbols_failed = symbols_failed
        run.error_message = error_message
        run.completed_at = datetime.utcnow()
        if run_metadata is not None:
            run.run_metadata = run_metadata


def update_eod_collection_run_progress(
    run_id: int,
    *,
    symbols_succeeded: int,
    symbols_failed: int,
    run_metadata: dict | None = None,
) -> None:
    with session_scope() as session:
        run = session.get(EODCollectionRun, run_id)
        if run is None:
            return
        run.symbols_succeeded = symbols_succeeded
        run.symbols_failed = symbols_failed
        if run_metadata is not None:
            run.run_metadata = run_metadata


def count_eod_run_persisted_symbols(run_id: int) -> int:
    with session_scope() as session:
        return int(
            session.execute(
                select(func.count(func.distinct(DailyPriceBar.symbol_id)))
                .select_from(DailyPriceBar)
                .where(DailyPriceBar.eod_run_id == run_id)
            ).scalar_one()
        )


def load_eod_run_persisted_symbols(run_id: int) -> list[str]:
    with session_scope() as session:
        rows = session.execute(
            select(Symbol.symbol)
            .join(DailyPriceBar, DailyPriceBar.symbol_id == Symbol.id)
            .where(DailyPriceBar.eod_run_id == run_id)
            .group_by(Symbol.symbol)
            .order_by(Symbol.symbol.asc())
        ).all()
        return [row[0] for row in rows]


def load_eod_run_stage_counts(run_id: int) -> dict[str, dict]:
    with session_scope() as session:
        price_persisted, price_updated_at = session.execute(
            select(
                func.count(func.distinct(DailyPriceBar.symbol_id)),
                func.max(DailyPriceBar.captured_at),
            )
            .select_from(DailyPriceBar)
            .where(DailyPriceBar.eod_run_id == run_id)
        ).one()
        technical_persisted, technical_updated_at = session.execute(
            select(
                func.count(func.distinct(DailyTechnicalSnapshot.symbol_id)),
                func.max(DailyTechnicalSnapshot.captured_at),
            )
            .select_from(DailyTechnicalSnapshot)
            .where(DailyTechnicalSnapshot.eod_run_id == run_id)
        ).one()
        action_persisted, action_updated_at = session.execute(
            select(
                func.count(func.distinct(DailyActionSummary.symbol_id)),
                func.max(DailyActionSummary.captured_at),
            )
            .select_from(DailyActionSummary)
            .where(DailyActionSummary.eod_run_id == run_id)
        ).one()
        trending_rows, trending_periods, trending_updated_at = session.execute(
            select(
                func.count(DailyTrendingRank.id),
                func.count(func.distinct(DailyTrendingRank.period)),
                func.max(DailyTrendingRank.captured_at),
            )
            .select_from(DailyTrendingRank)
            .where(DailyTrendingRank.eod_run_id == run_id)
        ).one()

    return {
        'price_collection': {
            'persisted': int(price_persisted or 0),
            'updated_at': price_updated_at.isoformat() if price_updated_at else None,
        },
        'technical_snapshots': {
            'persisted': int(technical_persisted or 0),
            'updated_at': technical_updated_at.isoformat() if technical_updated_at else None,
        },
        'action_summaries': {
            'persisted': int(action_persisted or 0),
            'updated_at': action_updated_at.isoformat() if action_updated_at else None,
        },
        'trending_ranks': {
            'persisted': int(trending_rows or 0),
            'periods_persisted': int(trending_periods or 0),
            'updated_at': trending_updated_at.isoformat() if trending_updated_at else None,
        },
    }


@contextmanager
def advisory_eod_collection_lock(*, market: str, trade_date: str, source: str):
    trade_key = int(trade_date.replace('-', ''))
    source_key = {
        ('KRX', 'pykrx'): 10_001,
        ('KRX', 'kis'): 10_002,
    }.get((market, source), 19_999)

    with engine.connect() as connection:
        acquired = bool(
            connection.execute(
                text('SELECT pg_try_advisory_lock(:source_key, :trade_key)'),
                {
                    'source_key': source_key,
                    'trade_key': trade_key,
                },
            ).scalar()
        )
        try:
            yield acquired
        finally:
            if acquired:
                connection.execute(
                    text('SELECT pg_advisory_unlock(:source_key, :trade_key)'),
                    {
                        'source_key': source_key,
                        'trade_key': trade_key,
                    },
                )


def load_recent_daily_price_trade_dates(
    *,
    end_date: str | date,
    limit: int,
) -> list[str]:
    if limit <= 0:
        return []

    normalized_end_date = _normalize_trade_date(end_date)
    with session_scope() as session:
        rows = session.execute(
            select(DailyPriceBar.trade_date)
            .where(DailyPriceBar.trade_date <= normalized_end_date)
            .group_by(DailyPriceBar.trade_date)
            .order_by(DailyPriceBar.trade_date.desc())
            .limit(limit)
        ).all()

    return [row.trade_date.isoformat() for row in reversed(rows)]


def count_daily_price_bars_for_date(trade_date: str | date) -> int:
    with session_scope() as session:
        return int(
            session.execute(
                select(func.count())
                .select_from(DailyPriceBar)
                .where(DailyPriceBar.trade_date == _normalize_trade_date(trade_date))
            ).scalar_one()
        )


def load_latest_available_daily_price_trade_date() -> str | None:
    with session_scope() as session:
        value = session.execute(select(func.max(DailyPriceBar.trade_date))).scalar_one()
        return value.isoformat() if value else None


def load_latest_available_daily_technical_snapshot_date() -> str | None:
    with session_scope() as session:
        value = session.execute(select(func.max(DailyTechnicalSnapshot.market_date))).scalar_one()
        return value.isoformat() if value else None


def load_latest_available_daily_action_summary_date() -> str | None:
    with session_scope() as session:
        value = session.execute(select(func.max(DailyActionSummary.market_date))).scalar_one()
        return value.isoformat() if value else None


def count_live_symbols() -> int:
    with session_scope() as session:
        return int(
            session.execute(
                select(func.count(Symbol.id))
                .select_from(Symbol)
                .where(Symbol.is_active.is_(True))
            ).scalar_one()
        )


def count_daily_price_bars_for_live_symbols(trade_date: str | date) -> int:
    with session_scope() as session:
        return int(
            session.execute(
                select(func.count(func.distinct(DailyPriceBar.symbol_id)))
                .select_from(DailyPriceBar)
                .join(Symbol, Symbol.id == DailyPriceBar.symbol_id)
                .where(DailyPriceBar.trade_date == _normalize_trade_date(trade_date))
                .where(Symbol.is_active.is_(True))
            ).scalar_one()
        )


def count_daily_technical_snapshots_for_live_symbols(market_date: str | date) -> int:
    with session_scope() as session:
        return int(
            session.execute(
                select(func.count(func.distinct(DailyTechnicalSnapshot.symbol_id)))
                .select_from(DailyTechnicalSnapshot)
                .join(Symbol, Symbol.id == DailyTechnicalSnapshot.symbol_id)
                .where(DailyTechnicalSnapshot.market_date == _normalize_trade_date(market_date))
                .where(Symbol.is_active.is_(True))
            ).scalar_one()
        )


def count_daily_action_summaries_for_live_symbols(market_date: str | date) -> int:
    with session_scope() as session:
        return int(
            session.execute(
                select(func.count(func.distinct(DailyActionSummary.symbol_id)))
                .select_from(DailyActionSummary)
                .join(Symbol, Symbol.id == DailyActionSummary.symbol_id)
                .where(DailyActionSummary.market_date == _normalize_trade_date(market_date))
                .where(Symbol.is_active.is_(True))
            ).scalar_one()
        )
