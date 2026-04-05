from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal

from sqlalchemy import delete, select

from app.db import session_scope
from app.db_models import CurrentQuoteCache, DailyPriceBar, EODCollectionRun, MarketStatusSnapshot, QuoteSnapshot, Symbol, TrackedSymbol
from app.market.clock import MarketState


def _parse_trade_date(value: str) -> date:
    return date.fromisoformat(value)


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


def upsert_master_symbols(items: list) -> int:
    refreshed_at = datetime.utcnow()
    with session_scope() as session:
        existing = {
            item.symbol: item
            for item in session.execute(select(Symbol).where(Symbol.symbol.in_([entry.symbol for entry in items]))).scalars()
        }
        seen_symbols: set[str] = set()
        for entry in items:
            seen_symbols.add(entry.symbol)
            record = existing.get(entry.symbol)
            if record is None:
                session.add(
                    Symbol(
                        symbol=entry.symbol,
                        standard_code=entry.standard_code,
                        name_ko=entry.name_ko,
                        market=entry.market,
                        instrument_type='equity',
                        source='kis_master',
                        is_active=True,
                        last_refreshed_at=refreshed_at,
                    )
                )
                continue

            record.standard_code = entry.standard_code
            record.name_ko = entry.name_ko
            record.market = entry.market
            record.instrument_type = 'equity'
            record.source = 'kis_master'
            record.is_active = True
            record.last_refreshed_at = refreshed_at
        if seen_symbols:
            for stale in session.execute(select(Symbol).where(Symbol.source == 'kis_master')).scalars():
                if stale.symbol not in seen_symbols:
                    stale.is_active = False
                    stale.last_refreshed_at = refreshed_at
        return len(items)


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
) -> int:
    with session_scope() as session:
        run = EODCollectionRun(
            market=market,
            trade_date=_parse_trade_date(trade_date),
            source=source,
            trigger=trigger,
            status='running',
            symbols_total=symbols_total,
            run_metadata=None,
        )
        session.add(run)
        session.flush()
        return run.id


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
