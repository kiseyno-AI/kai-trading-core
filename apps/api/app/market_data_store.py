from __future__ import annotations

from contextlib import contextmanager
from datetime import date, datetime, timedelta
from decimal import Decimal
from typing import Callable

from sqlalchemy import Numeric, cast, delete, func, select, text, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import aliased

from app.db import engine, session_scope
from app.db_models import (
    CurrentQuoteCache,
    DailyActionSummary,
    DailyPriceBar,
    DailySignalFeature,
    DailyTechnicalSnapshot,
    DailyTrendingRank,
    EODCollectionRun,
    FavoriteSymbol,
    MarketStatusSnapshot,
    QuoteSnapshot,
    Symbol,
    TrackedSymbol,
)
from app.market.clock import MarketState
from app.scanner.state import scanner_state


def _parse_trade_date(value: str) -> date:
    return date.fromisoformat(value)


def _normalize_trade_date(value: str | date) -> date:
    if isinstance(value, date):
        return value
    return _parse_trade_date(value)


def _parse_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    return datetime.fromisoformat(value)


def _as_decimal(value: float | int | str) -> Decimal:
    return Decimal(str(value))


def _decimal_to_float(value) -> float | None:
    if value is None:
        return None
    return float(value)


def _round_float(value, digits: int = 4) -> float | None:
    converted = _decimal_to_float(value)
    if converted is None:
        return None
    return round(converted, digits)


def _trend_direction(value: float | None) -> str:
    if value is None or value == 0:
        return 'flat'
    return 'up' if value > 0 else 'down'


DAY_BASED_TRENDING_PERIODS = ('1d', '5d', '20d')
TRENDING_RANK_REBUILD_LIMIT = 100000


def _get_symbol(session, symbol: str) -> Symbol | None:
    return session.execute(select(Symbol).where(Symbol.symbol == symbol)).scalar_one_or_none()


def _normalize_symbol_code(symbol: str) -> str:
    normalized = symbol.strip().upper()
    if not normalized:
        raise ValueError('symbol is required')
    return normalized


def ensure_symbol(
    symbol: str,
    *,
    market: str = 'KRX',
    name_ko: str | None = None,
    standard_code: str | None = None,
    instrument_type: str = 'equity',
    source: str = 'app',
) -> int:
    with session_scope() as session:
        record = _get_symbol(session, symbol)
        if record is None:
            record = Symbol(
                symbol=symbol,
                market=market,
                name_ko=name_ko or symbol,
                standard_code=standard_code,
                instrument_type=instrument_type,
                source=source,
                last_refreshed_at=datetime.utcnow(),
            )
            session.add(record)
            session.flush()
            return record.id

        if name_ko:
            record.name_ko = name_ko
        if standard_code:
            record.standard_code = standard_code
        if market:
            record.market = market
        record.instrument_type = instrument_type
        record.source = source
        record.is_active = True
        record.last_refreshed_at = datetime.utcnow()
        session.flush()
        return record.id


def upsert_symbol_universe(
    items: list,
    *,
    source: str,
    stale_sources: list[str] | None = None,
    stale_markets: list[str] | None = None,
    instrument_type: str = 'equity',
) -> int:
    if not items:
        return 0

    refreshed_at = datetime.utcnow()
    symbol_rows = [
        {
            'symbol': entry.symbol,
            'standard_code': getattr(entry, 'standard_code', None),
            'name_ko': entry.name_ko,
            'market': entry.market,
            'instrument_type': instrument_type,
            'source': source,
            'is_active': True,
            'last_refreshed_at': refreshed_at,
        }
        for entry in items
    ]

    with session_scope() as session:
        statement = pg_insert(Symbol).values(symbol_rows)
        session.execute(
            statement.on_conflict_do_update(
                index_elements=[Symbol.symbol],
                set_={
                    'standard_code': statement.excluded.standard_code,
                    'name_ko': statement.excluded.name_ko,
                    'market': statement.excluded.market,
                    'instrument_type': statement.excluded.instrument_type,
                    'source': statement.excluded.source,
                    'is_active': statement.excluded.is_active,
                    'last_refreshed_at': statement.excluded.last_refreshed_at,
                    'updated_at': refreshed_at,
                },
            )
        )
        stale_source_names = [source] if stale_sources is None else stale_sources
        if stale_source_names:
            target_markets = stale_markets or list({entry.market for entry in items if getattr(entry, 'market', None)})
            stale_stmt = (
                update(Symbol)
                .where(Symbol.source.in_(stale_source_names), ~Symbol.symbol.in_([entry.symbol for entry in items]))
                .values(is_active=False, last_refreshed_at=refreshed_at, updated_at=refreshed_at)
            )
            if target_markets:
                stale_stmt = stale_stmt.where(Symbol.market.in_(target_markets))
            session.execute(stale_stmt)
    return len(items)


def upsert_master_symbols(items: list) -> int:
    return upsert_symbol_universe(items, source='kis_master')


def load_symbols(
    *,
    is_active: bool | None = None,
    source: str | None = None,
    markets: list[str] | None = None,
    symbols: list[str] | None = None,
    limit: int | None = None,
) -> list[dict]:
    with session_scope() as session:
        stmt = select(
            Symbol.id,
            Symbol.symbol,
            Symbol.standard_code,
            Symbol.name_ko,
            Symbol.market,
            Symbol.instrument_type,
            Symbol.source,
            Symbol.is_active,
            Symbol.last_refreshed_at,
        ).order_by(Symbol.market.asc(), Symbol.symbol.asc())
        if is_active is not None:
            stmt = stmt.where(Symbol.is_active == is_active)
        if source:
            stmt = stmt.where(Symbol.source == source)
        if markets:
            stmt = stmt.where(Symbol.market.in_(markets))
        if symbols:
            stmt = stmt.where(Symbol.symbol.in_(symbols))
        if limit:
            stmt = stmt.limit(limit)
        rows = session.execute(stmt).all()
        return [dict(row._mapping) for row in rows]


def load_tracked_symbols() -> list[str]:
    with session_scope() as session:
        rows = session.execute(
            select(Symbol.symbol)
            .join(TrackedSymbol, TrackedSymbol.symbol_id == Symbol.id)
            .order_by(TrackedSymbol.sort_order.asc(), TrackedSymbol.id.asc())
        ).all()
        return [row[0] for row in rows]


def replace_tracked_symbols(symbols: list[str]) -> None:
    ordered_symbols = list(dict.fromkeys(symbols))
    with session_scope() as session:
        session.execute(delete(TrackedSymbol))
        for index, symbol in enumerate(ordered_symbols):
            record = _get_symbol(session, symbol)
            if record is None:
                record = Symbol(
                    symbol=symbol,
                    market='KRX',
                    name_ko=symbol,
                    instrument_type='equity',
                    source='scanner',
                    is_active=True,
                    last_refreshed_at=datetime.utcnow(),
                )
                session.add(record)
                session.flush()
            session.add(TrackedSymbol(symbol_id=record.id, sort_order=index))


def load_favorite_symbols() -> list[dict]:
    with session_scope() as session:
        rows = session.execute(
            select(
                Symbol.symbol,
                Symbol.standard_code,
                Symbol.name_ko,
                Symbol.market,
                Symbol.instrument_type,
                Symbol.is_active,
                TrackedSymbol.sort_order,
                TrackedSymbol.created_at,
            )
            .join(TrackedSymbol, TrackedSymbol.symbol_id == Symbol.id)
            .order_by(TrackedSymbol.sort_order.asc(), TrackedSymbol.id.asc())
        ).all()
        return [
            {
                'symbol': row.symbol,
                'standard_code': row.standard_code,
                'name_ko': row.name_ko,
                'market': row.market,
                'instrument_type': row.instrument_type,
                'is_active': row.is_active,
                'sort_order': row.sort_order,
                'favorited_at': row.created_at.isoformat() if row.created_at else None,
            }
            for row in rows
        ]


def remove_favorite_symbol(symbol: str) -> bool:
    normalized_symbol = _normalize_symbol_code(symbol)
    with session_scope() as session:
        favorite = session.execute(
            select(TrackedSymbol)
            .join(Symbol, Symbol.id == TrackedSymbol.symbol_id)
            .where(Symbol.symbol == normalized_symbol)
        ).scalar_one_or_none()
        if favorite is None:
            return False
        session.delete(favorite)

    scanner_state.update(load_tracked_symbols())
    return True


def persist_quote_snapshot(payload: dict) -> None:
    with session_scope() as session:
        symbol = _get_symbol(session, payload['symbol'])
        if symbol is None:
            symbol = Symbol(
                symbol=payload['symbol'],
                market='KRX',
                name_ko=payload.get('company_name') or payload['symbol'],
                instrument_type='equity',
                source='quote',
                is_active=True,
                last_refreshed_at=datetime.utcnow(),
            )
            session.add(symbol)
            session.flush()
        elif payload.get('company_name'):
            symbol.name_ko = payload['company_name']
            symbol.last_refreshed_at = datetime.utcnow()

        snapshot = QuoteSnapshot(
            symbol_id=symbol.id,
            market_state=payload['market_state'],
            price=payload['price'],
            change=payload['change'],
            change_pct=_as_decimal(payload['change_pct']),
            volume=payload['volume'],
            turnover=payload['turnover'],
            company_name=payload.get('company_name'),
            as_of=payload.get('as_of'),
            source=payload.get('source', 'app'),
            is_live=payload.get('is_live', False),
            fallback=payload.get('fallback', False),
            payload=payload,
        )
        session.add(snapshot)

        current = session.execute(select(CurrentQuoteCache).where(CurrentQuoteCache.symbol_id == symbol.id)).scalar_one_or_none()
        if current is None:
            current = CurrentQuoteCache(
                symbol_id=symbol.id,
                market_state=payload['market_state'],
                price=payload['price'],
                change=payload['change'],
                change_pct=_as_decimal(payload['change_pct']),
                volume=payload['volume'],
                turnover=payload['turnover'],
                company_name=payload.get('company_name'),
                as_of=payload.get('as_of'),
                source=payload.get('source', 'app'),
                is_live=payload.get('is_live', False),
                fallback=payload.get('fallback', False),
                payload=payload,
            )
            session.add(current)
            return

        current.market_state = payload['market_state']
        current.price = payload['price']
        current.change = payload['change']
        current.change_pct = _as_decimal(payload['change_pct'])
        current.volume = payload['volume']
        current.turnover = payload['turnover']
        current.company_name = payload.get('company_name')
        current.as_of = payload.get('as_of')
        current.source = payload.get('source', 'app')
        current.is_live = payload.get('is_live', False)
        current.fallback = payload.get('fallback', False)
        current.payload = payload
        current.captured_at = datetime.utcnow()


def load_current_quote(symbol: str) -> dict | None:
    with session_scope() as session:
        row = session.execute(
            select(CurrentQuoteCache)
            .join(Symbol, Symbol.id == CurrentQuoteCache.symbol_id)
            .where(Symbol.symbol == symbol)
        ).scalar_one_or_none()
        if row is None:
            return None
        payload = dict(row.payload)
        payload['cached_at'] = row.updated_at.isoformat()
        return payload


def persist_market_status_snapshot(state: MarketState, *, source: str = 'api') -> None:
    with session_scope() as session:
        session.add(
            MarketStatusSnapshot(
                market=state.market,
                state=state.state,
                is_trading_day=state.is_trading_day,
                is_open=state.is_open,
                reason=state.reason,
                server_time=_parse_datetime(state.server_time) or datetime.utcnow(),
                session_open_at=_parse_datetime(state.session_open_at),
                session_close_at=_parse_datetime(state.session_close_at),
                source=source,
            )
        )


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
    statuses: list[str] | None = None,
) -> dict | None:
    with session_scope() as session:
        stmt = select(EODCollectionRun).where(
            EODCollectionRun.market == market,
            EODCollectionRun.trade_date == _parse_trade_date(trade_date),
        )
        if source:
            stmt = stmt.where(EODCollectionRun.source == source)
        if statuses:
            stmt = stmt.where(EODCollectionRun.status.in_(statuses))
        stmt = stmt.order_by(EODCollectionRun.started_at.desc(), EODCollectionRun.id.desc()).limit(1)
        row = session.execute(stmt).scalars().first()
        if row is None:
            return None
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


def persist_daily_price_bar(snapshot: dict, *, run_id: int | None = None) -> None:
    with session_scope() as session:
        symbol = _get_symbol(session, snapshot['symbol'])
        if symbol is None:
            symbol = Symbol(
                symbol=snapshot['symbol'],
                market=snapshot.get('market') or 'KRX',
                name_ko=snapshot['symbol'],
                instrument_type='equity',
                source=snapshot.get('source', 'kis'),
                is_active=True,
                last_refreshed_at=datetime.utcnow(),
            )
            session.add(symbol)
            session.flush()

        row = session.execute(
            select(DailyPriceBar).where(
                DailyPriceBar.symbol_id == symbol.id,
                DailyPriceBar.trade_date == _parse_trade_date(snapshot['business_date']),
            )
        ).scalar_one_or_none()
        if row is None:
            row = DailyPriceBar(
                symbol_id=symbol.id,
                eod_run_id=run_id,
                trade_date=_parse_trade_date(snapshot['business_date']),
                open_price=snapshot['open_price'],
                high_price=snapshot['high_price'],
                low_price=snapshot['low_price'],
                close_price=snapshot['close_price'],
                volume=snapshot['volume'],
                turnover=snapshot['turnover'],
                source=snapshot.get('source', 'kis'),
            )
            session.add(row)
            return

        row.eod_run_id = run_id
        row.open_price = snapshot['open_price']
        row.high_price = snapshot['high_price']
        row.low_price = snapshot['low_price']
        row.close_price = snapshot['close_price']
        row.volume = snapshot['volume']
        row.turnover = snapshot['turnover']
        row.source = snapshot.get('source', 'kis')
        row.captured_at = datetime.utcnow()


def persist_daily_price_bars(
    symbol: str,
    bars: list[dict],
    *,
    market: str,
    source: str,
    run_id: int | None = None,
    name_ko: str | None = None,
    standard_code: str | None = None,
) -> int:
    if not bars:
        return 0

    refreshed_at = datetime.utcnow()
    with session_scope() as session:
        record = _get_symbol(session, symbol)
        if record is None:
            record = Symbol(
                symbol=symbol,
                market=market,
                name_ko=name_ko or symbol,
                standard_code=standard_code,
                instrument_type='equity',
                source=source,
                is_active=True,
                last_refreshed_at=refreshed_at,
            )
            session.add(record)
            session.flush()
        else:
            if name_ko:
                record.name_ko = name_ko
            if standard_code:
                record.standard_code = standard_code
            record.market = market
            record.instrument_type = 'equity'
            record.source = source
            record.is_active = True
            record.last_refreshed_at = refreshed_at

        values = [
            {
                'symbol_id': record.id,
                'eod_run_id': run_id,
                'trade_date': _normalize_trade_date(item['trade_date']),
                'open_price': int(item['open_price']),
                'high_price': int(item['high_price']),
                'low_price': int(item['low_price']),
                'close_price': int(item['close_price']),
                'volume': int(item['volume']),
                'turnover': int(item['turnover']),
                'source': source,
            }
            for item in bars
        ]
        statement = pg_insert(DailyPriceBar).values(values)
        session.execute(
            statement.on_conflict_do_update(
                index_elements=[DailyPriceBar.symbol_id, DailyPriceBar.trade_date],
                set_={
                    'eod_run_id': statement.excluded.eod_run_id,
                    'open_price': statement.excluded.open_price,
                    'high_price': statement.excluded.high_price,
                    'low_price': statement.excluded.low_price,
                    'close_price': statement.excluded.close_price,
                    'volume': statement.excluded.volume,
                    'turnover': statement.excluded.turnover,
                    'source': statement.excluded.source,
                    'captured_at': refreshed_at,
                },
            )
        )
    return len(values)


def list_symbols_with_daily_bar_on_date(trade_date: str) -> list[str]:
    with session_scope() as session:
        rows = session.execute(
            select(Symbol.symbol)
            .join(DailyPriceBar, DailyPriceBar.symbol_id == Symbol.id)
            .where(DailyPriceBar.trade_date == _normalize_trade_date(trade_date))
            .order_by(Symbol.symbol.asc())
        ).all()
        return [row[0] for row in rows]


def load_daily_bars_for_symbol(
    symbol: str,
    *,
    end_date: str | date,
    limit: int = 60,
) -> list[dict]:
    with session_scope() as session:
        rows = session.execute(
            select(
                DailyPriceBar.trade_date,
                DailyPriceBar.open_price,
                DailyPriceBar.high_price,
                DailyPriceBar.low_price,
                DailyPriceBar.close_price,
                DailyPriceBar.volume,
                DailyPriceBar.turnover,
            )
            .join(Symbol, Symbol.id == DailyPriceBar.symbol_id)
            .where(
                Symbol.symbol == symbol,
                DailyPriceBar.trade_date <= _normalize_trade_date(end_date),
            )
            .order_by(DailyPriceBar.trade_date.desc())
            .limit(limit)
        ).all()
        items = [
            {
                'trade_date': row.trade_date.isoformat(),
                'open_price': int(row.open_price),
                'high_price': int(row.high_price),
                'low_price': int(row.low_price),
                'close_price': int(row.close_price),
                'volume': int(row.volume),
                'turnover': int(row.turnover),
            }
            for row in reversed(rows)
        ]
        return items


def load_daily_bars_for_symbol_range(
    symbol: str,
    *,
    start_date: str | date | None = None,
    end_date: str | date | None = None,
    limit: int | None = None,
) -> list[dict]:
    with session_scope() as session:
        descending_window = limit is not None
        stmt = (
            select(
                DailyPriceBar.trade_date,
                DailyPriceBar.open_price,
                DailyPriceBar.high_price,
                DailyPriceBar.low_price,
                DailyPriceBar.close_price,
                DailyPriceBar.volume,
                DailyPriceBar.turnover,
                DailyPriceBar.source,
                DailyPriceBar.captured_at,
            )
            .join(Symbol, Symbol.id == DailyPriceBar.symbol_id)
            .where(Symbol.symbol == symbol)
            .order_by(DailyPriceBar.trade_date.desc() if descending_window else DailyPriceBar.trade_date.asc())
        )
        if start_date is not None:
            stmt = stmt.where(DailyPriceBar.trade_date >= _normalize_trade_date(start_date))
        if end_date is not None:
            stmt = stmt.where(DailyPriceBar.trade_date <= _normalize_trade_date(end_date))
        if limit is not None:
            stmt = stmt.limit(limit)

        rows = session.execute(stmt).all()
        ordered_rows = reversed(rows) if descending_window else rows
        return [
            {
                'trade_date': row.trade_date.isoformat(),
                'open_price': int(row.open_price),
                'high_price': int(row.high_price),
                'low_price': int(row.low_price),
                'close_price': int(row.close_price),
                'volume': int(row.volume),
                'turnover': int(row.turnover),
                'source': row.source,
                'captured_at': row.captured_at.isoformat() if row.captured_at else None,
            }
            for row in ordered_rows
        ]


def upsert_daily_technical_snapshot(snapshot: dict, *, run_id: int | None = None) -> None:
    refreshed_at = datetime.utcnow()
    with session_scope() as session:
        symbol = _get_symbol(session, snapshot['symbol'])
        if symbol is None:
            symbol = Symbol(
                symbol=snapshot['symbol'],
                market='KRX',
                name_ko=snapshot['symbol'],
                instrument_type='equity',
                source=snapshot.get('source', 'pykrx'),
                is_active=True,
                last_refreshed_at=refreshed_at,
            )
            session.add(symbol)
            session.flush()

        values = {
            'symbol_id': symbol.id,
            'eod_run_id': run_id,
            'market_date': _normalize_trade_date(snapshot['market_date']),
            'source': snapshot.get('source', 'pykrx'),
            'close_price': int(snapshot['close_price']),
            'volume': int(snapshot['volume']),
            'trend_state': snapshot['trend_state'],
            'ma_alignment': snapshot['ma_alignment'],
            'breakout_20d': bool(snapshot['breakout_20d']),
            'breakdown_20d': bool(snapshot['breakdown_20d']),
            'return_5d': snapshot.get('return_5d'),
            'return_20d': snapshot.get('return_20d'),
            'volume_ratio_20d': snapshot.get('volume_ratio_20d'),
            'atr_14': snapshot.get('atr_14'),
            'atr_pct_14': snapshot.get('atr_pct_14'),
            'signal_payload': snapshot['signal_payload'],
            'captured_at': refreshed_at,
            'updated_at': refreshed_at,
        }
        statement = pg_insert(DailyTechnicalSnapshot).values(values)
        session.execute(
            statement.on_conflict_do_update(
                constraint='uq_daily_technical_snapshots_symbol_market_date',
                set_={
                    'eod_run_id': statement.excluded.eod_run_id,
                    'source': statement.excluded.source,
                    'close_price': statement.excluded.close_price,
                    'volume': statement.excluded.volume,
                    'trend_state': statement.excluded.trend_state,
                    'ma_alignment': statement.excluded.ma_alignment,
                    'breakout_20d': statement.excluded.breakout_20d,
                    'breakdown_20d': statement.excluded.breakdown_20d,
                    'return_5d': statement.excluded.return_5d,
                    'return_20d': statement.excluded.return_20d,
                    'volume_ratio_20d': statement.excluded.volume_ratio_20d,
                    'atr_14': statement.excluded.atr_14,
                    'atr_pct_14': statement.excluded.atr_pct_14,
                    'signal_payload': statement.excluded.signal_payload,
                    'captured_at': statement.excluded.captured_at,
                    'updated_at': refreshed_at,
                },
            )
        )


def load_daily_technical_snapshots(
    *,
    market_date: str | date,
    symbols: list[str] | None = None,
    limit: int = 100,
    fallback_window_days: int = 0,
) -> list[dict]:
    normalized_market_date = _normalize_trade_date(market_date)
    with session_scope() as session:
        if fallback_window_days > 0:
            ranked = (
                select(
                    DailyTechnicalSnapshot.id.label('snapshot_id'),
                    func.row_number()
                    .over(
                        partition_by=Symbol.symbol,
                        order_by=(
                            DailyTechnicalSnapshot.market_date.desc(),
                            DailyTechnicalSnapshot.id.desc(),
                        ),
                    )
                    .label('symbol_rank'),
                )
                .join(Symbol, Symbol.id == DailyTechnicalSnapshot.symbol_id)
                .where(DailyTechnicalSnapshot.market_date <= normalized_market_date)
                .where(DailyTechnicalSnapshot.market_date >= normalized_market_date - timedelta(days=fallback_window_days))
            )
            if symbols:
                ranked = ranked.where(Symbol.symbol.in_(symbols))
            ranked_subquery = ranked.subquery()

            stmt = (
                select(DailyTechnicalSnapshot, Symbol.symbol)
                .join(Symbol, Symbol.id == DailyTechnicalSnapshot.symbol_id)
                .join(ranked_subquery, DailyTechnicalSnapshot.id == ranked_subquery.c.snapshot_id)
                .where(ranked_subquery.c.symbol_rank <= 1)
                .order_by(Symbol.symbol.asc())
                .limit(limit)
            )
        else:
            stmt = (
                select(DailyTechnicalSnapshot, Symbol.symbol)
                .join(Symbol, Symbol.id == DailyTechnicalSnapshot.symbol_id)
                .where(DailyTechnicalSnapshot.market_date == normalized_market_date)
                .order_by(Symbol.symbol.asc())
                .limit(limit)
            )
            if symbols:
                stmt = stmt.where(Symbol.symbol.in_(symbols))

        rows = session.execute(stmt).all()
        return [
            {
                'symbol': symbol,
                'market_date': snapshot.market_date.isoformat(),
                'source': snapshot.source,
                'close_price': int(snapshot.close_price),
                'volume': int(snapshot.volume),
                'trend_state': snapshot.trend_state,
                'ma_alignment': snapshot.ma_alignment,
                'breakout_20d': snapshot.breakout_20d,
                'breakdown_20d': snapshot.breakdown_20d,
                'return_5d': _decimal_to_float(snapshot.return_5d),
                'return_20d': _decimal_to_float(snapshot.return_20d),
                'volume_ratio_20d': _decimal_to_float(snapshot.volume_ratio_20d),
                'atr_14': _decimal_to_float(snapshot.atr_14),
                'atr_pct_14': _decimal_to_float(snapshot.atr_pct_14),
                'signal_payload': snapshot.signal_payload,
                'captured_at': snapshot.captured_at.isoformat() if snapshot.captured_at else None,
            }
            for snapshot, symbol in rows
        ]



def load_daily_technical_snapshot_inputs(
    *,
    market_date: str | date,
    symbols: list[str] | None = None,
    limit: int = 100,
) -> list[dict]:
    with session_scope() as session:
        stmt = (
            select(DailyTechnicalSnapshot, Symbol.symbol)
            .join(Symbol, Symbol.id == DailyTechnicalSnapshot.symbol_id)
            .where(DailyTechnicalSnapshot.market_date == _normalize_trade_date(market_date))
            .order_by(Symbol.symbol.asc())
            .limit(limit)
        )
        if symbols:
            stmt = stmt.where(Symbol.symbol.in_(symbols))

        rows = session.execute(stmt).all()
        return [
            {
                'technical_snapshot_id': snapshot.id,
                'symbol': symbol,
                'market_date': snapshot.market_date.isoformat(),
                'source': snapshot.source,
                'close_price': int(snapshot.close_price),
                'volume': int(snapshot.volume),
                'trend_state': snapshot.trend_state,
                'ma_alignment': snapshot.ma_alignment,
                'breakout_20d': snapshot.breakout_20d,
                'breakdown_20d': snapshot.breakdown_20d,
                'return_5d': _decimal_to_float(snapshot.return_5d),
                'return_20d': _decimal_to_float(snapshot.return_20d),
                'volume_ratio_20d': _decimal_to_float(snapshot.volume_ratio_20d),
                'atr_14': _decimal_to_float(snapshot.atr_14),
                'atr_pct_14': _decimal_to_float(snapshot.atr_pct_14),
                'signal_payload': snapshot.signal_payload,
                'captured_at': snapshot.captured_at.isoformat() if snapshot.captured_at else None,
            }
            for snapshot, symbol in rows
        ]


def upsert_daily_action_summary(summary: dict, *, run_id: int | None = None) -> None:
    refreshed_at = datetime.utcnow()
    with session_scope() as session:
        symbol = _get_symbol(session, summary['symbol'])
        if symbol is None:
            symbol = Symbol(
                symbol=summary['symbol'],
                market='KRX',
                name_ko=summary['symbol'],
                instrument_type='equity',
                source=summary.get('source', 'pykrx'),
                is_active=True,
                last_refreshed_at=refreshed_at,
            )
            session.add(symbol)
            session.flush()

        values = {
            'symbol_id': symbol.id,
            'technical_snapshot_id': summary.get('technical_snapshot_id'),
            'eod_run_id': run_id,
            'market_date': _normalize_trade_date(summary['market_date']),
            'source': summary.get('source', 'pykrx'),
            'action_label': summary['action_label'],
            'bias_direction': summary['bias_direction'],
            'signal_score': int(summary['signal_score']),
            'confidence_tier': summary['confidence_tier'],
            'risk_tier': summary['risk_tier'],
            'trigger_price': summary.get('trigger_price'),
            'invalidation_price': summary.get('invalidation_price'),
            'support_price': summary.get('support_price'),
            'resistance_price': summary.get('resistance_price'),
            'reasons': summary['reasons'],
            'summary_payload': summary['summary_payload'],
            'captured_at': refreshed_at,
            'updated_at': refreshed_at,
        }
        statement = pg_insert(DailyActionSummary).values(values)
        session.execute(
            statement.on_conflict_do_update(
                constraint='uq_daily_action_summaries_symbol_market_date',
                set_={
                    'technical_snapshot_id': statement.excluded.technical_snapshot_id,
                    'eod_run_id': statement.excluded.eod_run_id,
                    'source': statement.excluded.source,
                    'action_label': statement.excluded.action_label,
                    'bias_direction': statement.excluded.bias_direction,
                    'signal_score': statement.excluded.signal_score,
                    'confidence_tier': statement.excluded.confidence_tier,
                    'risk_tier': statement.excluded.risk_tier,
                    'trigger_price': statement.excluded.trigger_price,
                    'invalidation_price': statement.excluded.invalidation_price,
                    'support_price': statement.excluded.support_price,
                    'resistance_price': statement.excluded.resistance_price,
                    'reasons': statement.excluded.reasons,
                    'summary_payload': statement.excluded.summary_payload,
                    'captured_at': statement.excluded.captured_at,
                    'updated_at': refreshed_at,
                },
            )
        )


def load_daily_action_summaries(
    *,
    market_date: str | date,
    symbols: list[str] | None = None,
    limit: int = 100,
) -> list[dict]:
    with session_scope() as session:
        stmt = (
            select(DailyActionSummary, Symbol.symbol)
            .join(Symbol, Symbol.id == DailyActionSummary.symbol_id)
            .where(DailyActionSummary.market_date == _normalize_trade_date(market_date))
            .order_by(Symbol.symbol.asc())
            .limit(limit)
        )
        if symbols:
            stmt = stmt.where(Symbol.symbol.in_(symbols))

        rows = session.execute(stmt).all()
        return [
            {
                'symbol': symbol,
                'market_date': summary.market_date.isoformat(),
                'source': summary.source,
                'action_label': summary.action_label,
                'bias_direction': summary.bias_direction,
                'signal_score': int(summary.signal_score),
                'confidence_tier': summary.confidence_tier,
                'risk_tier': summary.risk_tier,
                'trigger_price': _decimal_to_float(summary.trigger_price),
                'invalidation_price': _decimal_to_float(summary.invalidation_price),
                'support_price': _decimal_to_float(summary.support_price),
                'resistance_price': _decimal_to_float(summary.resistance_price),
                'reasons': list(summary.reasons or []),
                'summary_payload': summary.summary_payload,
                'captured_at': summary.captured_at.isoformat() if summary.captured_at else None,
            }
            for summary, symbol in rows
        ]


def _trending_action_summary(row) -> dict | None:
    if row.action_label is None:
        return None
    return {
        'action_label': row.action_label,
        'bias_direction': row.bias_direction,
        'signal_score': int(row.signal_score) if row.signal_score is not None else None,
        'confidence_tier': row.confidence_tier,
        'risk_tier': row.risk_tier,
        'reasons': list(row.reasons or []),
        'source': row.action_source,
        'captured_at': row.action_captured_at.isoformat() if row.action_captured_at else None,
    }


def _trending_technical_context(row) -> dict:
    return {
        'close_price': int(row.close_price) if row.close_price is not None else None,
        'volume': int(row.volume) if row.volume is not None else None,
        'trend_state': row.trend_state,
        'ma_alignment': row.ma_alignment,
        'breakout_20d': row.breakout_20d,
        'breakdown_20d': row.breakdown_20d,
        'return_5d': _round_float(row.return_5d),
        'return_20d': _round_float(row.return_20d),
        'volume_ratio_20d': _round_float(row.volume_ratio_20d),
        'source': row.technical_source,
        'captured_at': row.technical_captured_at.isoformat() if row.technical_captured_at else None,
    }


def _apply_trending_order(stmt, metric_expr, *, direction: str):
    if direction == 'up':
        return stmt.where(metric_expr > 0).order_by(metric_expr.desc(), Symbol.symbol.asc())
    if direction == 'down':
        return stmt.where(metric_expr < 0).order_by(metric_expr.asc(), Symbol.symbol.asc())
    return stmt.order_by(func.abs(metric_expr).desc(), Symbol.symbol.asc())


def _load_trending_stocks_from_daily_price_bars(
    *,
    normalized_market_date: date,
    direction: str,
    limit: int,
    markets: list[str] | None,
) -> list[dict]:
    previous_bar = aliased(DailyPriceBar)
    previous_close_expr = (
        select(previous_bar.close_price)
        .where(previous_bar.symbol_id == DailyPriceBar.symbol_id)
        .where(previous_bar.trade_date < DailyPriceBar.trade_date)
        .order_by(previous_bar.trade_date.desc())
        .limit(1)
        .correlate(DailyPriceBar)
        .scalar_subquery()
    )
    previous_close_numeric = cast(previous_close_expr, Numeric(20, 6))
    close_numeric = cast(DailyPriceBar.close_price, Numeric(20, 6))
    change_expr = DailyPriceBar.close_price - previous_close_expr
    change_pct_expr = ((close_numeric - previous_close_numeric) / func.nullif(previous_close_numeric, 0)) * 100

    with session_scope() as session:
        stmt = (
            select(
                Symbol.symbol.label('symbol'),
                Symbol.name_ko.label('name_ko'),
                Symbol.market.label('market'),
                DailyPriceBar.trade_date.label('market_date'),
                DailyPriceBar.open_price.label('open_price'),
                DailyPriceBar.high_price.label('high_price'),
                DailyPriceBar.low_price.label('low_price'),
                DailyPriceBar.close_price.label('close_price'),
                DailyPriceBar.volume.label('volume'),
                DailyPriceBar.turnover.label('turnover'),
                DailyPriceBar.source.label('price_source'),
                DailyPriceBar.captured_at.label('price_captured_at'),
                previous_close_expr.label('previous_close_price'),
                change_expr.label('change'),
                change_pct_expr.label('metric_value'),
                DailyTechnicalSnapshot.trend_state.label('trend_state'),
                DailyTechnicalSnapshot.ma_alignment.label('ma_alignment'),
                DailyTechnicalSnapshot.breakout_20d.label('breakout_20d'),
                DailyTechnicalSnapshot.breakdown_20d.label('breakdown_20d'),
                DailyTechnicalSnapshot.return_5d.label('return_5d'),
                DailyTechnicalSnapshot.return_20d.label('return_20d'),
                DailyTechnicalSnapshot.volume_ratio_20d.label('volume_ratio_20d'),
                DailyTechnicalSnapshot.source.label('technical_source'),
                DailyTechnicalSnapshot.captured_at.label('technical_captured_at'),
                DailyActionSummary.action_label.label('action_label'),
                DailyActionSummary.bias_direction.label('bias_direction'),
                DailyActionSummary.signal_score.label('signal_score'),
                DailyActionSummary.confidence_tier.label('confidence_tier'),
                DailyActionSummary.risk_tier.label('risk_tier'),
                DailyActionSummary.reasons.label('reasons'),
                DailyActionSummary.source.label('action_source'),
                DailyActionSummary.captured_at.label('action_captured_at'),
            )
            .join(Symbol, Symbol.id == DailyPriceBar.symbol_id)
            .outerjoin(
                DailyTechnicalSnapshot,
                (DailyTechnicalSnapshot.symbol_id == DailyPriceBar.symbol_id)
                & (DailyTechnicalSnapshot.market_date == DailyPriceBar.trade_date),
            )
            .outerjoin(
                DailyActionSummary,
                (DailyActionSummary.symbol_id == DailyPriceBar.symbol_id)
                & (DailyActionSummary.market_date == DailyPriceBar.trade_date),
            )
            .where(DailyPriceBar.trade_date == normalized_market_date)
            .where(Symbol.is_active.is_(True))
            .where(previous_close_expr.isnot(None))
            .where(change_pct_expr.isnot(None))
        )
        if markets:
            stmt = stmt.where(Symbol.market.in_(markets))
        stmt = _apply_trending_order(stmt, change_pct_expr, direction=direction).limit(limit)
        rows = session.execute(stmt).all()

        items: list[dict] = []
        for row in rows:
            metric_value = _round_float(row.metric_value)
            items.append(
                {
                    'symbol': row.symbol,
                    'name_ko': row.name_ko,
                    'market': row.market,
                    'market_date': row.market_date.isoformat(),
                    'trend_direction': _trend_direction(metric_value),
                    'metric': {
                        'period': '1d',
                        'name': 'daily_change_pct',
                        'value': metric_value,
                    },
                    'price': {
                        'open_price': int(row.open_price),
                        'high_price': int(row.high_price),
                        'low_price': int(row.low_price),
                        'close_price': int(row.close_price),
                        'previous_close_price': int(row.previous_close_price),
                        'change': int(row.change),
                        'change_pct': metric_value,
                        'volume': int(row.volume),
                        'turnover': int(row.turnover),
                        'source': row.price_source,
                        'captured_at': row.price_captured_at.isoformat() if row.price_captured_at else None,
                    },
                    'technical': _trending_technical_context(row),
                    'action_summary': _trending_action_summary(row),
                }
            )
        return items


def _load_trending_stocks_from_snapshots(
    *,
    normalized_market_date: date,
    period: str,
    direction: str,
    limit: int,
    markets: list[str] | None,
) -> list[dict]:
    if period == '5d':
        metric_expr = DailyTechnicalSnapshot.return_5d
        metric_name = 'return_5d_pct'
    elif period == '20d':
        metric_expr = DailyTechnicalSnapshot.return_20d
        metric_name = 'return_20d_pct'
    else:
        metric_expr = DailyActionSummary.signal_score
        metric_name = 'signal_score'

    with session_scope() as session:
        if period == 'action_score':
            stmt = (
                select(
                    Symbol.symbol.label('symbol'),
                    Symbol.name_ko.label('name_ko'),
                    Symbol.market.label('market'),
                    DailyActionSummary.market_date.label('market_date'),
                    DailyTechnicalSnapshot.close_price.label('close_price'),
                    DailyTechnicalSnapshot.volume.label('volume'),
                    DailyTechnicalSnapshot.trend_state.label('trend_state'),
                    DailyTechnicalSnapshot.ma_alignment.label('ma_alignment'),
                    DailyTechnicalSnapshot.breakout_20d.label('breakout_20d'),
                    DailyTechnicalSnapshot.breakdown_20d.label('breakdown_20d'),
                    DailyTechnicalSnapshot.return_5d.label('return_5d'),
                    DailyTechnicalSnapshot.return_20d.label('return_20d'),
                    DailyTechnicalSnapshot.volume_ratio_20d.label('volume_ratio_20d'),
                    DailyTechnicalSnapshot.source.label('technical_source'),
                    DailyTechnicalSnapshot.captured_at.label('technical_captured_at'),
                    DailyActionSummary.action_label.label('action_label'),
                    DailyActionSummary.bias_direction.label('bias_direction'),
                    DailyActionSummary.signal_score.label('signal_score'),
                    DailyActionSummary.confidence_tier.label('confidence_tier'),
                    DailyActionSummary.risk_tier.label('risk_tier'),
                    DailyActionSummary.reasons.label('reasons'),
                    DailyActionSummary.source.label('action_source'),
                    DailyActionSummary.captured_at.label('action_captured_at'),
                    metric_expr.label('metric_value'),
                )
                .join(Symbol, Symbol.id == DailyActionSummary.symbol_id)
                .outerjoin(
                    DailyTechnicalSnapshot,
                    (DailyTechnicalSnapshot.symbol_id == DailyActionSummary.symbol_id)
                    & (DailyTechnicalSnapshot.market_date == DailyActionSummary.market_date),
                )
                .where(DailyActionSummary.market_date == normalized_market_date)
                .where(Symbol.is_active.is_(True))
                .where(metric_expr.isnot(None))
            )
        else:
            stmt = (
                select(
                    Symbol.symbol.label('symbol'),
                    Symbol.name_ko.label('name_ko'),
                    Symbol.market.label('market'),
                    DailyTechnicalSnapshot.market_date.label('market_date'),
                    DailyTechnicalSnapshot.close_price.label('close_price'),
                    DailyTechnicalSnapshot.volume.label('volume'),
                    DailyTechnicalSnapshot.trend_state.label('trend_state'),
                    DailyTechnicalSnapshot.ma_alignment.label('ma_alignment'),
                    DailyTechnicalSnapshot.breakout_20d.label('breakout_20d'),
                    DailyTechnicalSnapshot.breakdown_20d.label('breakdown_20d'),
                    DailyTechnicalSnapshot.return_5d.label('return_5d'),
                    DailyTechnicalSnapshot.return_20d.label('return_20d'),
                    DailyTechnicalSnapshot.volume_ratio_20d.label('volume_ratio_20d'),
                    DailyTechnicalSnapshot.source.label('technical_source'),
                    DailyTechnicalSnapshot.captured_at.label('technical_captured_at'),
                    DailyActionSummary.action_label.label('action_label'),
                    DailyActionSummary.bias_direction.label('bias_direction'),
                    DailyActionSummary.signal_score.label('signal_score'),
                    DailyActionSummary.confidence_tier.label('confidence_tier'),
                    DailyActionSummary.risk_tier.label('risk_tier'),
                    DailyActionSummary.reasons.label('reasons'),
                    DailyActionSummary.source.label('action_source'),
                    DailyActionSummary.captured_at.label('action_captured_at'),
                    metric_expr.label('metric_value'),
                )
                .join(Symbol, Symbol.id == DailyTechnicalSnapshot.symbol_id)
                .outerjoin(
                    DailyActionSummary,
                    (DailyActionSummary.symbol_id == DailyTechnicalSnapshot.symbol_id)
                    & (DailyActionSummary.market_date == DailyTechnicalSnapshot.market_date),
                )
                .where(DailyTechnicalSnapshot.market_date == normalized_market_date)
                .where(Symbol.is_active.is_(True))
                .where(metric_expr.isnot(None))
            )
        if markets:
            stmt = stmt.where(Symbol.market.in_(markets))
        stmt = _apply_trending_order(stmt, metric_expr, direction=direction).limit(limit)
        rows = session.execute(stmt).all()

        items: list[dict] = []
        for row in rows:
            metric_value = _round_float(row.metric_value)
            items.append(
                {
                    'symbol': row.symbol,
                    'name_ko': row.name_ko,
                    'market': row.market,
                    'market_date': row.market_date.isoformat(),
                    'trend_direction': _trend_direction(metric_value),
                    'metric': {
                        'period': period,
                        'name': metric_name,
                        'value': metric_value,
                    },
                    'price': None,
                    'technical': _trending_technical_context(row),
                    'action_summary': _trending_action_summary(row),
                }
            )
        return items


def build_daily_trending_rank_items(items: list[dict]) -> list[dict]:
    rank_entries: list[dict] = []
    for index, item in enumerate(items):
        metric = item.get('metric') or {}
        metric_value = metric.get('value')
        if metric_value is None:
            continue
        rank_entries.append(
            {
                'index': index,
                'symbol': item['symbol'],
                'metric_value': float(metric_value),
                'item': item,
            }
        )

    rank_by_index: dict[int, dict] = {
        entry['index']: {
            'absolute_rank': None,
            'up_rank': None,
            'down_rank': None,
        }
        for entry in rank_entries
    }

    absolute_entries = sorted(rank_entries, key=lambda entry: (-abs(entry['metric_value']), entry['symbol']))
    for rank, entry in enumerate(absolute_entries, start=1):
        rank_by_index[entry['index']]['absolute_rank'] = rank

    up_entries = sorted(
        [entry for entry in rank_entries if entry['metric_value'] > 0],
        key=lambda entry: (-entry['metric_value'], entry['symbol']),
    )
    for rank, entry in enumerate(up_entries, start=1):
        rank_by_index[entry['index']]['up_rank'] = rank

    down_entries = sorted(
        [entry for entry in rank_entries if entry['metric_value'] < 0],
        key=lambda entry: (entry['metric_value'], entry['symbol']),
    )
    for rank, entry in enumerate(down_entries, start=1):
        rank_by_index[entry['index']]['down_rank'] = rank

    ranked_items: list[dict] = []
    for entry in rank_entries:
        item = dict(entry['item'])
        item['stored_ranks'] = rank_by_index[entry['index']]
        ranked_items.append(item)
    return ranked_items


def replace_daily_trending_ranks(
    *,
    market_date: str | date,
    period: str,
    items: list[dict],
    source: str = 'pykrx',
    run_id: int | None = None,
) -> int:
    normalized_market_date = _normalize_trade_date(market_date)
    normalized_period = period.strip().lower()
    ranked_items = build_daily_trending_rank_items(items)
    symbols = [item['symbol'] for item in ranked_items]
    refreshed_at = datetime.utcnow()

    with session_scope() as session:
        session.execute(
            delete(DailyTrendingRank)
            .where(DailyTrendingRank.market_date == normalized_market_date)
            .where(DailyTrendingRank.period == normalized_period)
        )
        if not ranked_items:
            return 0

        symbol_rows = session.execute(select(Symbol.symbol, Symbol.id).where(Symbol.symbol.in_(symbols))).all()
        symbol_ids = {symbol: symbol_id for symbol, symbol_id in symbol_rows}
        values = []
        for item in ranked_items:
            symbol_id = symbol_ids.get(item['symbol'])
            if symbol_id is None:
                continue
            metric = item['metric']
            stored_ranks = item['stored_ranks']
            values.append(
                {
                    'symbol_id': symbol_id,
                    'eod_run_id': run_id,
                    'market_date': normalized_market_date,
                    'period': normalized_period,
                    'source': source,
                    'metric_name': metric['name'],
                    'metric_value': metric['value'],
                    'trend_direction': item['trend_direction'],
                    'absolute_rank': stored_ranks['absolute_rank'],
                    'up_rank': stored_ranks['up_rank'],
                    'down_rank': stored_ranks['down_rank'],
                    'price_payload': item.get('price'),
                    'technical_payload': item.get('technical'),
                    'action_summary_payload': item.get('action_summary'),
                    'captured_at': refreshed_at,
                    'created_at': refreshed_at,
                    'updated_at': refreshed_at,
                }
            )
        if not values:
            return 0
        session.execute(pg_insert(DailyTrendingRank).values(values))
        return len(values)


def compute_and_persist_daily_trending_ranks(
    *,
    market_date: str | date,
    periods: list[str] | tuple[str, ...] | None = None,
    source: str = 'pykrx',
    run_id: int | None = None,
    progress_callback: Callable[[dict], None] | None = None,
) -> dict:
    normalized_market_date = _normalize_trade_date(market_date)
    normalized_periods = [period.strip().lower() for period in (periods or DAY_BASED_TRENDING_PERIODS)]
    seen_periods: set[str] = set()
    deduped_periods: list[str] = []
    for period in normalized_periods:
        if period in seen_periods:
            continue
        if period not in DAY_BASED_TRENDING_PERIODS:
            raise ValueError('period must be one of 1d, 5d, or 20d')
        seen_periods.add(period)
        deduped_periods.append(period)

    period_results: list[dict] = []
    total_persisted = 0

    def _emit_progress(payload: dict) -> None:
        if not progress_callback:
            return
        try:
            progress_callback(payload)
        except Exception:
            # Progress persistence is best-effort and must not interrupt trending rank rebuilds.
            return

    if progress_callback:
        first_period = deduped_periods[0] if deduped_periods else None
        _emit_progress(
            {
                'status': 'running' if first_period else 'completed',
                'processed': 0,
                'succeeded': 0,
                'failed': 0,
                'total': len(deduped_periods),
                'current_period': first_period,
                'current_step': 'computing_trending_period' if first_period else 'trending_ranks_completed',
                'current_activity': (
                    f'Computing daily trending ranks for {first_period}'
                    if first_period
                    else 'No day-based trending periods configured'
                ),
                'details': {
                    'rows_persisted': 0,
                    'periods_persisted': 0,
                },
            }
        )
    for period in deduped_periods:
        if period == '1d':
            items = _load_trending_stocks_from_daily_price_bars(
                normalized_market_date=normalized_market_date,
                direction='both',
                limit=TRENDING_RANK_REBUILD_LIMIT,
                markets=None,
            )
        else:
            items = _load_trending_stocks_from_snapshots(
                normalized_market_date=normalized_market_date,
                period=period,
                direction='both',
                limit=TRENDING_RANK_REBUILD_LIMIT,
                markets=None,
            )
        persisted_count = replace_daily_trending_ranks(
            market_date=normalized_market_date,
            period=period,
            items=items,
            source=source,
            run_id=run_id,
        )
        total_persisted += persisted_count
        period_results.append(
            {
                'period': period,
                'candidates_seen': len(items),
                'trending_ranks_persisted': persisted_count,
            }
        )
        if progress_callback:
            processed_periods = len(period_results)
            next_period = deduped_periods[processed_periods] if processed_periods < len(deduped_periods) else None
            _emit_progress(
                {
                    'status': 'running' if next_period is not None else 'completed',
                    'processed': processed_periods,
                    'succeeded': processed_periods,
                    'failed': 0,
                    'total': len(deduped_periods),
                    'current_period': next_period,
                    'current_step': 'computing_trending_period' if next_period is not None else 'trending_ranks_completed',
                    'current_activity': (
                        f'Computing daily trending ranks for {next_period}'
                        if next_period is not None
                        else 'Completed daily trending ranks'
                    ),
                    'details': {
                        'rows_persisted': total_persisted,
                        'periods_persisted': processed_periods,
                    },
                }
            )

    return {
        'market_date': normalized_market_date.isoformat(),
        'source': source,
        'periods': period_results,
        'trending_ranks_persisted': total_persisted,
    }


def load_latest_available_daily_trending_rank_date(period: str) -> str | None:
    normalized_period = period.strip().lower()
    with session_scope() as session:
        value = session.execute(
            select(func.max(DailyTrendingRank.market_date)).where(DailyTrendingRank.period == normalized_period)
        ).scalar_one()
        return value.isoformat() if value else None


def load_persisted_trending_stocks(
    *,
    market_date: str | date,
    period: str,
    direction: str,
    limit: int = 20,
    markets: list[str] | None = None,
) -> list[dict]:
    normalized_market_date = _normalize_trade_date(market_date)
    normalized_period = period.strip().lower()
    normalized_markets = [market.upper() for market in markets] if markets else None

    with session_scope() as session:
        stmt = (
            select(
                DailyTrendingRank,
                Symbol.symbol.label('symbol'),
                Symbol.name_ko.label('name_ko'),
                Symbol.market.label('market'),
            )
            .join(Symbol, Symbol.id == DailyTrendingRank.symbol_id)
            .where(DailyTrendingRank.market_date == normalized_market_date)
            .where(DailyTrendingRank.period == normalized_period)
            .where(Symbol.is_active.is_(True))
        )
        if normalized_markets:
            stmt = stmt.where(Symbol.market.in_(normalized_markets))

        if direction == 'up':
            stmt = (
                stmt.where(DailyTrendingRank.up_rank.isnot(None))
                .order_by(DailyTrendingRank.up_rank.asc(), Symbol.symbol.asc())
            )
        elif direction == 'down':
            stmt = (
                stmt.where(DailyTrendingRank.down_rank.isnot(None))
                .order_by(DailyTrendingRank.down_rank.asc(), Symbol.symbol.asc())
            )
        else:
            stmt = stmt.order_by(DailyTrendingRank.absolute_rank.asc(), Symbol.symbol.asc())

        rows = session.execute(stmt.limit(limit)).all()
        items: list[dict] = []
        for rank, symbol, name_ko, market in rows:
            metric_value = _round_float(rank.metric_value)
            items.append(
                {
                    'symbol': symbol,
                    'name_ko': name_ko,
                    'market': market,
                    'market_date': rank.market_date.isoformat(),
                    'trend_direction': rank.trend_direction,
                    'metric': {
                        'period': rank.period,
                        'name': rank.metric_name,
                        'value': metric_value,
                    },
                    'price': rank.price_payload,
                    'technical': rank.technical_payload,
                    'action_summary': rank.action_summary_payload,
                    'stored_ranks': {
                        'absolute_rank': rank.absolute_rank,
                        'up_rank': rank.up_rank,
                        'down_rank': rank.down_rank,
                    },
                    'source': rank.source,
                    'captured_at': rank.captured_at.isoformat() if rank.captured_at else None,
                }
            )
        return items


def load_trending_stocks(
    *,
    market_date: str | date,
    period: str,
    direction: str,
    limit: int = 20,
    markets: list[str] | None = None,
) -> list[dict]:
    normalized_market_date = _normalize_trade_date(market_date)
    normalized_markets = [market.upper() for market in markets] if markets else None
    if period in DAY_BASED_TRENDING_PERIODS:
        return load_persisted_trending_stocks(
            market_date=normalized_market_date,
            period=period,
            direction=direction,
            limit=limit,
            markets=normalized_markets,
        )
    return _load_trending_stocks_from_snapshots(
        normalized_market_date=normalized_market_date,
        period=period,
        direction=direction,
        limit=limit,
        markets=normalized_markets,
    )


def upsert_daily_signal_feature(feature: dict) -> None:
    refreshed_at = datetime.utcnow()
    with session_scope() as session:
        symbol = _get_symbol(session, feature['symbol'])
        if symbol is None:
            symbol = Symbol(
                symbol=feature['symbol'],
                market=feature.get('market', 'KRX'),
                name_ko=feature.get('name_ko') or feature['symbol'],
                instrument_type='equity',
                source=feature.get('source', 'kis_ranking'),
                is_active=True,
                last_refreshed_at=refreshed_at,
            )
            session.add(symbol)
            session.flush()
        elif feature.get('name_ko') and symbol.name_ko == symbol.symbol:
            symbol.name_ko = feature['name_ko']
            symbol.last_refreshed_at = refreshed_at

        values = {
            'symbol_id': symbol.id,
            'market_date': _normalize_trade_date(feature['market_date']),
            'source': feature.get('source', 'kis_ranking'),
            'signal_count': int(feature.get('signal_count') or 0),
            'volume_power_rank': feature.get('volume_power_rank'),
            'volume_power': feature.get('volume_power'),
            'overtime_volume_rank': feature.get('overtime_volume_rank'),
            'overtime_volume': feature.get('overtime_volume'),
            'overtime_fluctuation_rank': feature.get('overtime_fluctuation_rank'),
            'overtime_fluctuation_rate': feature.get('overtime_fluctuation_rate'),
            'feature_payload': feature['feature_payload'],
            'captured_at': refreshed_at,
            'updated_at': refreshed_at,
        }
        statement = pg_insert(DailySignalFeature).values(values)
        session.execute(
            statement.on_conflict_do_update(
                constraint='uq_daily_signal_features_symbol_market_date',
                set_={
                    'source': statement.excluded.source,
                    'signal_count': statement.excluded.signal_count,
                    'volume_power_rank': statement.excluded.volume_power_rank,
                    'volume_power': statement.excluded.volume_power,
                    'overtime_volume_rank': statement.excluded.overtime_volume_rank,
                    'overtime_volume': statement.excluded.overtime_volume,
                    'overtime_fluctuation_rank': statement.excluded.overtime_fluctuation_rank,
                    'overtime_fluctuation_rate': statement.excluded.overtime_fluctuation_rate,
                    'feature_payload': statement.excluded.feature_payload,
                    'captured_at': statement.excluded.captured_at,
                    'updated_at': refreshed_at,
                },
            )
        )


def load_daily_signal_features(
    *,
    market_date: str | date,
    symbols: list[str] | None = None,
    limit: int = 100,
) -> list[dict]:
    with session_scope() as session:
        stmt = (
            select(DailySignalFeature, Symbol.symbol)
            .join(Symbol, Symbol.id == DailySignalFeature.symbol_id)
            .where(DailySignalFeature.market_date == _normalize_trade_date(market_date))
            .order_by(DailySignalFeature.signal_count.desc(), Symbol.symbol.asc())
            .limit(limit)
        )
        if symbols:
            stmt = stmt.where(Symbol.symbol.in_(symbols))

        rows = session.execute(stmt).all()
        return [
            {
                'symbol': symbol,
                'market_date': feature.market_date.isoformat(),
                'source': feature.source,
                'signal_count': int(feature.signal_count),
                'volume_power_rank': feature.volume_power_rank,
                'volume_power': _decimal_to_float(feature.volume_power),
                'overtime_volume_rank': feature.overtime_volume_rank,
                'overtime_volume': int(feature.overtime_volume) if feature.overtime_volume is not None else None,
                'overtime_fluctuation_rank': feature.overtime_fluctuation_rank,
                'overtime_fluctuation_rate': _decimal_to_float(feature.overtime_fluctuation_rate),
                'feature_payload': feature.feature_payload,
                'captured_at': feature.captured_at.isoformat() if feature.captured_at else None,
            }
            for feature, symbol in rows
        ]


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
