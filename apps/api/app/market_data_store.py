from __future__ import annotations

from contextlib import contextmanager
from datetime import date, datetime
from decimal import Decimal

from sqlalchemy import delete, func, select, text, update
from sqlalchemy.dialects.postgresql import insert as pg_insert

from app.db import engine, session_scope
from app.db_models import CurrentQuoteCache, DailyPriceBar, EODCollectionRun, MarketStatusSnapshot, QuoteSnapshot, Symbol, TrackedSymbol
from app.market.clock import MarketState


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


def _get_symbol(session, symbol: str) -> Symbol | None:
    return session.execute(select(Symbol).where(Symbol.symbol == symbol)).scalar_one_or_none()


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


def complete_eod_collection_run(
    run_id: int,
    *,
    status: str,
    symbols_succeeded: int,
    symbols_failed: int,
    error_message: str | None = None,
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


def update_eod_collection_run_progress(
    run_id: int,
    *,
    symbols_succeeded: int,
    symbols_failed: int,
) -> None:
    with session_scope() as session:
        run = session.get(EODCollectionRun, run_id)
        if run is None:
            return
        run.symbols_succeeded = symbols_succeeded
        run.symbols_failed = symbols_failed


def count_eod_run_persisted_symbols(run_id: int) -> int:
    with session_scope() as session:
        return int(
            session.execute(
                select(func.count(func.distinct(DailyPriceBar.symbol_id)))
                .select_from(DailyPriceBar)
                .where(DailyPriceBar.eod_run_id == run_id)
            ).scalar_one()
        )


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
