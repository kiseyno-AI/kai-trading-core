from __future__ import annotations

from contextlib import contextmanager
from datetime import date, datetime, timedelta
from decimal import Decimal
from typing import Callable

from sqlalchemy import Numeric, and_, case, cast, delete, func, select, text, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import aliased

from app.db import engine, session_scope
from app.db_models import (
    CurrentQuoteCache,
    DailyActionSummary,
    DailyAdvancedValueFactorResult,
    DailyBetaFactorResult,
    DailyCompositeFactorResult,
    DailyLowRiskFactorResult,
    DailyMarketRiskFilterResult,
    DailyMomentumFactorResult,
    DailyMarketParticipationContext,
    DailyPiotroskiFilterResult,
    DailyPriceBar,
    DailyPriceContext,
    DailySignalFeature,
    DailyTechnicalSnapshot,
    DailyTrendingRank,
    DailyValueFactorResult,
    EODCollectionRun,
    FavoriteSymbol,
    MarketStatusSnapshot,
    QuoteSnapshot,
    Symbol,
    TrackedSymbol,
)
from app.market.clock import MarketState
from app.repositories.eod_run_repository import (
    advisory_eod_collection_lock,
    complete_eod_collection_run,
    count_daily_action_summaries_for_live_symbols,
    count_daily_price_bars_for_date,
    count_daily_price_bars_for_live_symbols,
    count_daily_technical_snapshots_for_live_symbols,
    count_eod_collection_runs,
    count_eod_run_persisted_symbols,
    count_live_symbols,
    create_eod_collection_run,
    load_eod_run_persisted_symbols,
    load_eod_run_stage_counts,
    load_latest_available_daily_action_summary_date,
    load_latest_available_daily_price_trade_date,
    load_latest_available_daily_technical_snapshot_date,
    load_latest_eod_collection_run,
    load_recent_daily_price_trade_dates,
    update_eod_collection_run_progress,
)
from app.repositories.symbol_repository import (
    ensure_symbol,
    load_favorite_symbols,
    load_symbols,
    load_tracked_symbols,
    remove_favorite_symbol,
    replace_tracked_symbols,
    upsert_master_symbols,
    upsert_symbol_universe,
)
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
                per=_as_decimal(snapshot['per']) if snapshot.get('per') is not None else None,
                pbr=_as_decimal(snapshot['pbr']) if snapshot.get('pbr') is not None else None,
                bps=_as_decimal(snapshot['bps']) if snapshot.get('bps') is not None else None,
                eps=_as_decimal(snapshot['eps']) if snapshot.get('eps') is not None else None,
                dividend_yield=_as_decimal(snapshot['dividend_yield']) if snapshot.get('dividend_yield') is not None else None,
                dps=_as_decimal(snapshot['dps']) if snapshot.get('dps') is not None else None,
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
        row.per = _as_decimal(snapshot['per']) if snapshot.get('per') is not None else None
        row.pbr = _as_decimal(snapshot['pbr']) if snapshot.get('pbr') is not None else None
        row.bps = _as_decimal(snapshot['bps']) if snapshot.get('bps') is not None else None
        row.eps = _as_decimal(snapshot['eps']) if snapshot.get('eps') is not None else None
        row.dividend_yield = _as_decimal(snapshot['dividend_yield']) if snapshot.get('dividend_yield') is not None else None
        row.dps = _as_decimal(snapshot['dps']) if snapshot.get('dps') is not None else None
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
                'per': _as_decimal(item['per']) if item.get('per') is not None else None,
                'pbr': _as_decimal(item['pbr']) if item.get('pbr') is not None else None,
                'bps': _as_decimal(item['bps']) if item.get('bps') is not None else None,
                'eps': _as_decimal(item['eps']) if item.get('eps') is not None else None,
                'dividend_yield': _as_decimal(item['dividend_yield']) if item.get('dividend_yield') is not None else None,
                'dps': _as_decimal(item['dps']) if item.get('dps') is not None else None,
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
                    'per': statement.excluded.per,
                    'pbr': statement.excluded.pbr,
                    'bps': statement.excluded.bps,
                    'eps': statement.excluded.eps,
                    'dividend_yield': statement.excluded.dividend_yield,
                    'dps': statement.excluded.dps,
                    'source': statement.excluded.source,
                    'captured_at': refreshed_at,
                },
            )
        )
    return len(values)


def load_daily_price_bar_dates_for_symbol(
    symbol: str,
    *,
    start_date: str | date | None = None,
    end_date: str | date | None = None,
) -> list[date]:
    with session_scope() as session:
        stmt = (
            select(DailyPriceBar.trade_date)
            .join(Symbol, Symbol.id == DailyPriceBar.symbol_id)
            .where(Symbol.symbol == symbol)
            .order_by(DailyPriceBar.trade_date.asc())
        )
        if start_date is not None:
            stmt = stmt.where(DailyPriceBar.trade_date >= _normalize_trade_date(start_date))
        if end_date is not None:
            stmt = stmt.where(DailyPriceBar.trade_date <= _normalize_trade_date(end_date))
        rows = session.execute(stmt).all()
        return [row.trade_date for row in rows]


def update_daily_price_bar_fundamentals(
    symbol: str,
    fundamentals_by_date: dict[date, dict[str, float | None]],
    *,
    source: str = 'pykrx',
) -> int:
    if not fundamentals_by_date:
        return 0

    refreshed_at = datetime.utcnow()
    with session_scope() as session:
        record = _get_symbol(session, symbol)
        if record is None:
            return 0

        updated_count = 0
        for trade_date, fundamentals in fundamentals_by_date.items():
            result = session.execute(
                update(DailyPriceBar)
                .where(
                    and_(
                        DailyPriceBar.symbol_id == record.id,
                        DailyPriceBar.trade_date == _normalize_trade_date(trade_date),
                    )
                )
                .values(
                    per=_as_decimal(fundamentals['per']) if fundamentals.get('per') is not None else None,
                    pbr=_as_decimal(fundamentals['pbr']) if fundamentals.get('pbr') is not None else None,
                    bps=_as_decimal(fundamentals['bps']) if fundamentals.get('bps') is not None else None,
                    eps=_as_decimal(fundamentals['eps']) if fundamentals.get('eps') is not None else None,
                    dividend_yield=_as_decimal(fundamentals['dividend_yield']) if fundamentals.get('dividend_yield') is not None else None,
                    dps=_as_decimal(fundamentals['dps']) if fundamentals.get('dps') is not None else None,
                    source=source,
                    captured_at=refreshed_at,
                )
            )
            updated_count += int(result.rowcount or 0)
        return updated_count




def list_symbols_with_daily_bars_in_range(*, start_date: str | date | None = None, end_date: str | date | None = None) -> list[str]:
    with session_scope() as session:
        stmt = (
            select(Symbol.symbol)
            .join(DailyPriceBar, DailyPriceBar.symbol_id == Symbol.id)
            .group_by(Symbol.symbol)
            .order_by(Symbol.symbol.asc())
        )
        if start_date is not None:
            stmt = stmt.where(DailyPriceBar.trade_date >= _normalize_trade_date(start_date))
        if end_date is not None:
            stmt = stmt.where(DailyPriceBar.trade_date <= _normalize_trade_date(end_date))
        rows = session.execute(stmt).all()
        return [row.symbol for row in rows]

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
                DailyPriceBar.per,
                DailyPriceBar.pbr,
                DailyPriceBar.bps,
                DailyPriceBar.eps,
                DailyPriceBar.dividend_yield,
                DailyPriceBar.dps,
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
                'per': _round_float(row.per, 6),
                'pbr': _round_float(row.pbr, 6),
                'bps': _round_float(row.bps, 6),
                'eps': _round_float(row.eps, 6),
                'dividend_yield': _round_float(row.dividend_yield, 6),
                'dps': _round_float(row.dps, 6),
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
                DailyPriceBar.per,
                DailyPriceBar.pbr,
                DailyPriceBar.bps,
                DailyPriceBar.eps,
                DailyPriceBar.dividend_yield,
                DailyPriceBar.dps,
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
                'per': _round_float(row.per, 6),
                'pbr': _round_float(row.pbr, 6),
                'bps': _round_float(row.bps, 6),
                'eps': _round_float(row.eps, 6),
                'dividend_yield': _round_float(row.dividend_yield, 6),
                'dps': _round_float(row.dps, 6),
                'source': row.source,
                'captured_at': row.captured_at.isoformat() if row.captured_at else None,
            }
            for row in ordered_rows
        ]


def load_recent_price_bars_by_symbol(
    *,
    market_date: str | date,
    symbols: list[str],
    history_days: int = 2,
) -> dict[str, list[dict]]:
    if history_days <= 0 or not symbols:
        return {}

    normalized_symbols = list(dict.fromkeys(symbols))
    with session_scope() as session:
        ranked = (
            select(
                Symbol.symbol.label('symbol'),
                DailyPriceBar.trade_date.label('trade_date'),
                DailyPriceBar.open_price.label('open_price'),
                DailyPriceBar.high_price.label('high_price'),
                DailyPriceBar.low_price.label('low_price'),
                DailyPriceBar.close_price.label('close_price'),
                DailyPriceBar.volume.label('volume'),
                DailyPriceBar.turnover.label('turnover'),
                DailyPriceBar.per.label('per'),
                DailyPriceBar.pbr.label('pbr'),
                DailyPriceBar.bps.label('bps'),
                DailyPriceBar.eps.label('eps'),
                DailyPriceBar.dividend_yield.label('dividend_yield'),
                DailyPriceBar.dps.label('dps'),
                DailyPriceBar.source.label('source'),
                DailyPriceBar.captured_at.label('captured_at'),
                func.row_number()
                .over(partition_by=DailyPriceBar.symbol_id, order_by=DailyPriceBar.trade_date.desc())
                .label('bar_rank'),
            )
            .join(Symbol, Symbol.id == DailyPriceBar.symbol_id)
            .where(
                Symbol.symbol.in_(normalized_symbols),
                DailyPriceBar.trade_date <= _normalize_trade_date(market_date),
            )
            .subquery()
        )
        rows = session.execute(
            select(ranked)
            .where(ranked.c.bar_rank <= history_days)
            .order_by(ranked.c.symbol.asc(), ranked.c.trade_date.desc())
        ).all()

    grouped: dict[str, list[dict]] = {symbol: [] for symbol in normalized_symbols}
    for row in rows:
        mapping = row._mapping
        grouped.setdefault(mapping['symbol'], []).append(
            {
                'trade_date': mapping['trade_date'].isoformat() if mapping['trade_date'] else None,
                'open_price': int(mapping['open_price']) if mapping['open_price'] is not None else None,
                'high_price': int(mapping['high_price']) if mapping['high_price'] is not None else None,
                'low_price': int(mapping['low_price']) if mapping['low_price'] is not None else None,
                'close_price': int(mapping['close_price']) if mapping['close_price'] is not None else None,
                'volume': int(mapping['volume']) if mapping['volume'] is not None else None,
                'turnover': int(mapping['turnover']) if mapping['turnover'] is not None else None,
                'per': _round_float(mapping['per'], 6),
                'pbr': _round_float(mapping['pbr'], 6),
                'bps': _round_float(mapping['bps'], 6),
                'eps': _round_float(mapping['eps'], 6),
                'dividend_yield': _round_float(mapping['dividend_yield'], 6),
                'dps': _round_float(mapping['dps'], 6),
                'source': mapping['source'],
                'captured_at': mapping['captured_at'].isoformat() if mapping['captured_at'] else None,
            }
        )
    return grouped


def upsert_daily_price_context(item: dict, *, run_id: int | None = None) -> None:
    refreshed_at = datetime.utcnow()
    with session_scope() as session:
        symbol = _get_symbol(session, item['symbol'])
        if symbol is None:
            raise ValueError(f"Unknown symbol for daily price context: {item['symbol']}")

        values = {
            'symbol_id': symbol.id,
            'eod_run_id': run_id,
            'market_date': _normalize_trade_date(item['market_date']),
            'source': item.get('source', 'pykrx'),
            'context_payload': item['context_payload'],
            'captured_at': refreshed_at,
            'updated_at': refreshed_at,
        }
        statement = pg_insert(DailyPriceContext).values(values)
        session.execute(
            statement.on_conflict_do_update(
                constraint='uq_daily_price_contexts_symbol_market_date',
                set_={
                    'eod_run_id': statement.excluded.eod_run_id,
                    'source': statement.excluded.source,
                    'context_payload': statement.excluded.context_payload,
                    'captured_at': statement.excluded.captured_at,
                    'updated_at': refreshed_at,
                },
            )
        )


def load_daily_price_contexts(
    *,
    market_date: str | date,
    symbols: list[str] | None = None,
    limit: int = 100,
) -> list[dict]:
    normalized_market_date = _normalize_trade_date(market_date)
    with session_scope() as session:
        stmt = (
            select(DailyPriceContext, Symbol.symbol)
            .join(Symbol, Symbol.id == DailyPriceContext.symbol_id)
            .where(DailyPriceContext.market_date == normalized_market_date)
            .order_by(Symbol.symbol.asc())
            .limit(limit)
        )
        if symbols:
            stmt = stmt.where(Symbol.symbol.in_(symbols))

        rows = session.execute(stmt).all()
        return [
            {
                'symbol': symbol,
                'market_date': context.market_date.isoformat(),
                'source': context.source,
                'context_payload': context.context_payload,
                'captured_at': context.captured_at.isoformat() if context.captured_at else None,
            }
            for context, symbol in rows
        ]


def load_daily_price_context_history(
    *,
    market_date: str | date,
    symbols: list[str],
    history_days: int,
) -> list[dict]:
    if history_days <= 0 or not symbols:
        return []

    normalized_market_date = _normalize_trade_date(market_date)
    normalized_symbols = list(dict.fromkeys(symbols))
    with session_scope() as session:
        ranked = (
            select(
                DailyPriceContext.market_date.label('market_date'),
                DailyPriceContext.source.label('source'),
                DailyPriceContext.context_payload.label('context_payload'),
                DailyPriceContext.captured_at.label('captured_at'),
                Symbol.symbol.label('symbol'),
                func.row_number()
                .over(partition_by=DailyPriceContext.symbol_id, order_by=DailyPriceContext.market_date.desc())
                .label('context_rank'),
            )
            .join(Symbol, Symbol.id == DailyPriceContext.symbol_id)
            .where(
                Symbol.symbol.in_(normalized_symbols),
                DailyPriceContext.market_date <= normalized_market_date,
            )
            .subquery()
        )
        rows = session.execute(
            select(ranked)
            .where(ranked.c.context_rank <= history_days)
            .order_by(ranked.c.symbol.asc(), ranked.c.market_date.asc())
        ).all()

    return [
        {
            'symbol': row.symbol,
            'market_date': row.market_date.isoformat(),
            'source': row.source,
            'context_payload': row.context_payload,
            'captured_at': row.captured_at.isoformat() if row.captured_at else None,
        }
        for row in rows
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


def upsert_daily_market_risk_filter_result(result: dict) -> None:
    refreshed_at = datetime.utcnow()
    with session_scope() as session:
        symbol = _get_symbol(session, result['symbol'])
        if symbol is None:
            symbol = Symbol(
                symbol=result['symbol'],
                market=result.get('market', 'KRX'),
                name_ko=result.get('name_ko') or result['symbol'],
                instrument_type='equity',
                source=result.get('source', 'kis_market_risk_v1'),
                is_active=True,
                last_refreshed_at=refreshed_at,
            )
            session.add(symbol)
            session.flush()
        elif result.get('name_ko') and symbol.name_ko == symbol.symbol:
            symbol.name_ko = result['name_ko']
            symbol.last_refreshed_at = refreshed_at

        values = {
            'symbol_id': symbol.id,
            'eod_run_id': result.get('eod_run_id'),
            'market_date': _normalize_trade_date(result['market_date']),
            'source': result.get('source', 'kis_market_risk_v1'),
            'risk_state': result.get('risk_state', 'pass'),
            'passed_filter': bool(result.get('passed_filter')),
            'watchlist_flagged': bool(result.get('watchlist_flagged')),
            'market_warning_code': result.get('market_warning_code'),
            'market_warning_name': result.get('market_warning_name'),
            'is_management_item': bool(result.get('is_management_item')),
            'is_investment_caution': bool(result.get('is_investment_caution')),
            'is_short_overheated': bool(result.get('is_short_overheated')),
            'short_overheat_class_code': result.get('short_overheat_class_code'),
            'is_short_sale_overheated': bool(result.get('is_short_sale_overheated')),
            'vi_class_code': result.get('vi_class_code'),
            'is_vi_triggered': bool(result.get('is_vi_triggered')),
            'is_dishonest_disclosure': bool(result.get('is_dishonest_disclosure')),
            'is_trade_halted': bool(result.get('is_trade_halted')),
            'filter_payload': result['filter_payload'],
            'captured_at': refreshed_at,
            'updated_at': refreshed_at,
        }
        statement = pg_insert(DailyMarketRiskFilterResult).values(values)
        session.execute(
            statement.on_conflict_do_update(
                constraint='uq_daily_market_risk_filter_results_symbol_market_date',
                set_={
                    'eod_run_id': statement.excluded.eod_run_id,
                    'source': statement.excluded.source,
                    'risk_state': statement.excluded.risk_state,
                    'passed_filter': statement.excluded.passed_filter,
                    'watchlist_flagged': statement.excluded.watchlist_flagged,
                    'market_warning_code': statement.excluded.market_warning_code,
                    'market_warning_name': statement.excluded.market_warning_name,
                    'is_management_item': statement.excluded.is_management_item,
                    'is_investment_caution': statement.excluded.is_investment_caution,
                    'is_short_overheated': statement.excluded.is_short_overheated,
                    'short_overheat_class_code': statement.excluded.short_overheat_class_code,
                    'is_short_sale_overheated': statement.excluded.is_short_sale_overheated,
                    'vi_class_code': statement.excluded.vi_class_code,
                    'is_vi_triggered': statement.excluded.is_vi_triggered,
                    'is_dishonest_disclosure': statement.excluded.is_dishonest_disclosure,
                    'is_trade_halted': statement.excluded.is_trade_halted,
                    'filter_payload': statement.excluded.filter_payload,
                    'captured_at': statement.excluded.captured_at,
                    'updated_at': refreshed_at,
                },
            )
        )


def upsert_daily_piotroski_filter_result(result: dict) -> None:
    refreshed_at = datetime.utcnow()
    with session_scope() as session:
        symbol = _get_symbol(session, result['symbol'])
        if symbol is None:
            symbol = Symbol(
                symbol=result['symbol'],
                market='KRX',
                name_ko=(result.get('filter_payload') or {}).get('name_ko') or result['symbol'],
                instrument_type='equity',
                source=result.get('source', 'strategy_piotroski_v1'),
                is_active=True,
                last_refreshed_at=refreshed_at,
            )
            session.add(symbol)
            session.flush()

        values = {
            'symbol_id': symbol.id,
            'market_date': _normalize_trade_date(result['market_date']),
            'source': result.get('source', 'strategy_piotroski_v1'),
            'pbr': result.get('pbr'),
            'valuation_available': bool(result.get('valuation_available', True)),
            'valuation_source_status': result.get('valuation_source_status'),
            'pbr_percentile_rank': result.get('pbr_percentile_rank'),
            'is_low_pbr': bool(result.get('is_low_pbr')),
            'has_profitability_issue': bool(result.get('has_profitability_issue')),
            'has_capital_impairment': bool(result.get('has_capital_impairment')),
            'has_audit_issue': bool(result.get('has_audit_issue')),
            'piotroski_score': result.get('piotroski_score'),
            'passes_piotroski_score': bool(result.get('passes_piotroski_score')),
            'momentum_percentile_rank': result.get('momentum_percentile_rank'),
            'is_bottom_momentum': bool(result.get('is_bottom_momentum')),
            'passed_filter': bool(result.get('passed_filter')),
            'filter_payload': result['filter_payload'],
            'captured_at': refreshed_at,
            'updated_at': refreshed_at,
        }
        statement = pg_insert(DailyPiotroskiFilterResult).values(values)
        session.execute(
            statement.on_conflict_do_update(
                constraint='uq_daily_piotroski_filter_results_symbol_market_date',
                set_={
                    'source': statement.excluded.source,
                    'pbr': statement.excluded.pbr,
                    'valuation_available': statement.excluded.valuation_available,
                    'valuation_source_status': statement.excluded.valuation_source_status,
                    'pbr_percentile_rank': statement.excluded.pbr_percentile_rank,
                    'is_low_pbr': statement.excluded.is_low_pbr,
                    'has_profitability_issue': statement.excluded.has_profitability_issue,
                    'has_capital_impairment': statement.excluded.has_capital_impairment,
                    'has_audit_issue': statement.excluded.has_audit_issue,
                    'piotroski_score': statement.excluded.piotroski_score,
                    'passes_piotroski_score': statement.excluded.passes_piotroski_score,
                    'momentum_percentile_rank': statement.excluded.momentum_percentile_rank,
                    'is_bottom_momentum': statement.excluded.is_bottom_momentum,
                    'passed_filter': statement.excluded.passed_filter,
                    'filter_payload': statement.excluded.filter_payload,
                    'captured_at': statement.excluded.captured_at,
                    'updated_at': refreshed_at,
                },
            )
        )


def upsert_daily_momentum_factor_result(result: dict) -> None:
    refreshed_at = datetime.utcnow()
    with session_scope() as session:
        symbol = _get_symbol(session, result['symbol'])
        if symbol is None:
            payload = result.get('factor_payload') or {}
            symbol = Symbol(
                symbol=result['symbol'],
                market='KRX',
                name_ko=payload.get('name_ko') or result['symbol'],
                instrument_type='equity',
                source=result.get('source', 'daily_price_bars'),
                is_active=True,
                last_refreshed_at=refreshed_at,
            )
            session.add(symbol)
            session.flush()

        values = {
            'symbol_id': symbol.id,
            'market_date': _normalize_trade_date(result['market_date']),
            'source': result.get('source', 'daily_price_bars'),
            'close_price': int(result['close_price']),
            'ma_60': result.get('ma_60'),
            'ma_120': result.get('ma_120'),
            'momentum_12_1': result.get('momentum_12_1'),
            'return_5d': result.get('return_5d'),
            'has_sufficient_history': bool(result.get('has_sufficient_history')),
            'is_price_above_ma120': bool(result.get('is_price_above_ma120')),
            'is_ma60_above_ma120': bool(result.get('is_ma60_above_ma120')),
            'is_recent_overheated': bool(result.get('is_recent_overheated')),
            'momentum_rank': result.get('momentum_rank'),
            'momentum_percentile': result.get('momentum_percentile'),
            'is_momentum_eligible': bool(result.get('is_momentum_eligible')),
            'factor_payload': result['factor_payload'],
            'captured_at': refreshed_at,
            'updated_at': refreshed_at,
        }
        statement = pg_insert(DailyMomentumFactorResult).values(values)
        session.execute(
            statement.on_conflict_do_update(
                constraint='uq_daily_momentum_factor_results_symbol_market_date',
                set_={
                    'source': statement.excluded.source,
                    'close_price': statement.excluded.close_price,
                    'ma_60': statement.excluded.ma_60,
                    'ma_120': statement.excluded.ma_120,
                    'momentum_12_1': statement.excluded.momentum_12_1,
                    'return_5d': statement.excluded.return_5d,
                    'has_sufficient_history': statement.excluded.has_sufficient_history,
                    'is_price_above_ma120': statement.excluded.is_price_above_ma120,
                    'is_ma60_above_ma120': statement.excluded.is_ma60_above_ma120,
                    'is_recent_overheated': statement.excluded.is_recent_overheated,
                    'momentum_rank': statement.excluded.momentum_rank,
                    'momentum_percentile': statement.excluded.momentum_percentile,
                    'is_momentum_eligible': statement.excluded.is_momentum_eligible,
                    'factor_payload': statement.excluded.factor_payload,
                    'captured_at': statement.excluded.captured_at,
                    'updated_at': refreshed_at,
                },
            )
        )


def load_daily_momentum_factor_results(
    *,
    market_date: str | date,
    symbols: list[str] | None = None,
    eligible_only: bool | None = None,
    limit: int | None = 100,
) -> list[dict]:
    with session_scope() as session:
        stmt = (
            select(DailyMomentumFactorResult, Symbol.symbol)
            .join(Symbol, Symbol.id == DailyMomentumFactorResult.symbol_id)
            .where(DailyMomentumFactorResult.market_date == _normalize_trade_date(market_date))
            .order_by(
                DailyMomentumFactorResult.is_momentum_eligible.desc(),
                DailyMomentumFactorResult.momentum_rank.asc().nullslast(),
                Symbol.symbol.asc(),
            )
        )
        if limit is not None:
            stmt = stmt.limit(limit)
        if symbols:
            stmt = stmt.where(Symbol.symbol.in_(symbols))
        if eligible_only is not None:
            stmt = stmt.where(DailyMomentumFactorResult.is_momentum_eligible == eligible_only)

        rows = session.execute(stmt).all()
        return [
            {
                'symbol': symbol,
                'market_date': item.market_date.isoformat(),
                'source': item.source,
                'close_price': int(item.close_price),
                'ma_60': _round_float(item.ma_60, 6),
                'ma_120': _round_float(item.ma_120, 6),
                'momentum_12_1': _round_float(item.momentum_12_1, 6),
                'return_5d': _round_float(item.return_5d, 6),
                'has_sufficient_history': item.has_sufficient_history,
                'is_price_above_ma120': item.is_price_above_ma120,
                'is_ma60_above_ma120': item.is_ma60_above_ma120,
                'is_recent_overheated': item.is_recent_overheated,
                'momentum_rank': item.momentum_rank,
                'momentum_percentile': _round_float(item.momentum_percentile, 6),
                'is_momentum_eligible': item.is_momentum_eligible,
                'factor_payload': item.factor_payload,
                'captured_at': item.captured_at.isoformat() if item.captured_at else None,
            }
            for item, symbol in rows
        ]


def load_daily_market_risk_filter_results(
    *,
    market_date: str | date,
    symbols: list[str] | None = None,
    passed_filter: bool | None = None,
    limit: int | None = 100,
) -> list[dict]:
    with session_scope() as session:
        stmt = (
            select(DailyMarketRiskFilterResult, Symbol.symbol, Symbol.name_ko, Symbol.market)
            .join(Symbol, Symbol.id == DailyMarketRiskFilterResult.symbol_id)
            .where(DailyMarketRiskFilterResult.market_date == _normalize_trade_date(market_date))
            .order_by(
                DailyMarketRiskFilterResult.passed_filter.asc(),
                DailyMarketRiskFilterResult.watchlist_flagged.desc(),
                Symbol.symbol.asc(),
            )
        )
        if limit is not None:
            stmt = stmt.limit(limit)
        if symbols:
            stmt = stmt.where(Symbol.symbol.in_(symbols))
        if passed_filter is not None:
            stmt = stmt.where(DailyMarketRiskFilterResult.passed_filter == passed_filter)

        rows = session.execute(stmt).all()
        return [
            {
                'symbol': symbol,
                'name_ko': name_ko,
                'market': market,
                'eod_run_id': item.eod_run_id,
                'market_date': item.market_date.isoformat(),
                'source': item.source,
                'feature_version': item.source,
                'risk_state': item.risk_state,
                'passed_filter': item.passed_filter,
                'watchlist_flagged': item.watchlist_flagged,
                'market_warning_code': item.market_warning_code,
                'market_warning_name': item.market_warning_name,
                'is_management_item': item.is_management_item,
                'is_investment_caution': item.is_investment_caution,
                'is_short_overheated': item.is_short_overheated,
                'short_overheat_class_code': item.short_overheat_class_code,
                'is_short_sale_overheated': item.is_short_sale_overheated,
                'vi_class_code': item.vi_class_code,
                'is_vi_triggered': item.is_vi_triggered,
                'is_dishonest_disclosure': item.is_dishonest_disclosure,
                'is_trade_halted': item.is_trade_halted,
                'filter_payload': item.filter_payload,
                'captured_at': item.captured_at.isoformat() if item.captured_at else None,
            }
            for item, symbol, name_ko, market in rows
        ]


def load_latest_available_daily_market_risk_filter_result_date(
    *,
    market_date: str | date | None = None,
    symbols: list[str] | None = None,
) -> str | None:
    with session_scope() as session:
        stmt = select(func.max(DailyMarketRiskFilterResult.market_date)).select_from(DailyMarketRiskFilterResult)
        if symbols:
            stmt = (
                stmt.join(Symbol, Symbol.id == DailyMarketRiskFilterResult.symbol_id)
                .where(Symbol.symbol.in_(symbols))
            )
        if market_date is not None:
            stmt = stmt.where(DailyMarketRiskFilterResult.market_date <= _normalize_trade_date(market_date))

        value = session.execute(stmt).scalar_one()
        return value.isoformat() if value else None


def load_daily_piotroski_filter_results(
    *,
    market_date: str | date,
    symbols: list[str] | None = None,
    passed_filter: bool | None = None,
    limit: int | None = 100,
) -> list[dict]:
    with session_scope() as session:
        stmt = (
            select(DailyPiotroskiFilterResult, Symbol.symbol)
            .join(Symbol, Symbol.id == DailyPiotroskiFilterResult.symbol_id)
            .where(DailyPiotroskiFilterResult.market_date == _normalize_trade_date(market_date))
            .order_by(
                DailyPiotroskiFilterResult.passed_filter.desc(),
                DailyPiotroskiFilterResult.pbr_percentile_rank.asc().nullslast(),
                Symbol.symbol.asc(),
            )
        )
        if limit is not None:
            stmt = stmt.limit(limit)
        if symbols:
            stmt = stmt.where(Symbol.symbol.in_(symbols))
        if passed_filter is not None:
            stmt = stmt.where(DailyPiotroskiFilterResult.passed_filter == passed_filter)

        rows = session.execute(stmt).all()
        return [
            {
                'symbol': symbol,
                'market_date': item.market_date.isoformat(),
                'source': item.source,
                'pbr': _decimal_to_float(item.pbr),
                'pbr_percentile_rank': _round_float(item.pbr_percentile_rank, 6),
                'is_low_pbr': item.is_low_pbr,
                'has_profitability_issue': item.has_profitability_issue,
                'has_capital_impairment': item.has_capital_impairment,
                'has_audit_issue': item.has_audit_issue,
                'piotroski_score': item.piotroski_score,
                'passes_piotroski_score': item.passes_piotroski_score,
                'momentum_percentile_rank': _round_float(item.momentum_percentile_rank, 6),
                'is_bottom_momentum': item.is_bottom_momentum,
                'passed_filter': item.passed_filter,
                'filter_payload': item.filter_payload,
                'captured_at': item.captured_at.isoformat() if item.captured_at else None,
            }
            for item, symbol in rows
        ]


def load_latest_available_daily_piotroski_filter_result_date(
    *,
    market_date: str | date | None = None,
    symbols: list[str] | None = None,
) -> str | None:
    with session_scope() as session:
        stmt = select(func.max(DailyPiotroskiFilterResult.market_date)).select_from(DailyPiotroskiFilterResult)
        if symbols:
            stmt = (
                stmt.join(Symbol, Symbol.id == DailyPiotroskiFilterResult.symbol_id)
                .where(Symbol.symbol.in_(symbols))
            )
        if market_date is not None:
            stmt = stmt.where(DailyPiotroskiFilterResult.market_date <= _normalize_trade_date(market_date))

        value = session.execute(stmt).scalar_one()
        return value.isoformat() if value else None


def upsert_daily_low_risk_factor_result(result: dict) -> None:
    refreshed_at = datetime.utcnow()
    with session_scope() as session:
        symbol = _get_symbol(session, result['symbol'])
        if symbol is None:
            symbol = Symbol(
                symbol=result['symbol'],
                market=result.get('market', 'KRX'),
                name_ko=result.get('name_ko') or result['symbol'],
                instrument_type='equity',
                source=result.get('source', 'low_risk_v1'),
                is_active=True,
                last_refreshed_at=refreshed_at,
            )
            session.add(symbol)
            session.flush()

        values = {
            'symbol_id': symbol.id,
            'market_date': _normalize_trade_date(result['market_date']),
            'source': result.get('source', 'low_risk_v1'),
            'volatility_60d': result.get('volatility_60d'),
            'volatility_120d': result.get('volatility_120d'),
            'max_drawdown_120d': result.get('max_drawdown_120d'),
            'avg_trading_value_20d': result.get('avg_trading_value_20d'),
            'volatility_60d_rank': result.get('volatility_60d_rank'),
            'volatility_120d_rank': result.get('volatility_120d_rank'),
            'max_drawdown_120d_rank': result.get('max_drawdown_120d_rank'),
            'liquidity_rank': result.get('liquidity_rank'),
            'volatility_60d_percentile': result.get('volatility_60d_percentile'),
            'volatility_120d_percentile': result.get('volatility_120d_percentile'),
            'max_drawdown_120d_percentile': result.get('max_drawdown_120d_percentile'),
            'liquidity_percentile': result.get('liquidity_percentile'),
            'passes_liquidity_floor': result.get('passes_liquidity_floor'),
            'low_risk_score': result.get('low_risk_score'),
            'low_risk_classification': result.get('low_risk_classification', 'insufficient_history'),
            'is_low_risk_eligible': bool(result.get('is_low_risk_eligible')),
            'history_days_available': int(result.get('history_days_available') or 0),
            'factor_payload': result['factor_payload'],
            'captured_at': refreshed_at,
            'updated_at': refreshed_at,
        }
        statement = pg_insert(DailyLowRiskFactorResult).values(values)
        session.execute(
            statement.on_conflict_do_update(
                constraint='uq_daily_low_risk_factor_results_symbol_market_date',
                set_={
                    'source': statement.excluded.source,
                    'volatility_60d': statement.excluded.volatility_60d,
                    'volatility_120d': statement.excluded.volatility_120d,
                    'max_drawdown_120d': statement.excluded.max_drawdown_120d,
                    'avg_trading_value_20d': statement.excluded.avg_trading_value_20d,
                    'volatility_60d_rank': statement.excluded.volatility_60d_rank,
                    'volatility_120d_rank': statement.excluded.volatility_120d_rank,
                    'max_drawdown_120d_rank': statement.excluded.max_drawdown_120d_rank,
                    'liquidity_rank': statement.excluded.liquidity_rank,
                    'volatility_60d_percentile': statement.excluded.volatility_60d_percentile,
                    'volatility_120d_percentile': statement.excluded.volatility_120d_percentile,
                    'max_drawdown_120d_percentile': statement.excluded.max_drawdown_120d_percentile,
                    'liquidity_percentile': statement.excluded.liquidity_percentile,
                    'passes_liquidity_floor': statement.excluded.passes_liquidity_floor,
                    'low_risk_score': statement.excluded.low_risk_score,
                    'low_risk_classification': statement.excluded.low_risk_classification,
                    'is_low_risk_eligible': statement.excluded.is_low_risk_eligible,
                    'history_days_available': statement.excluded.history_days_available,
                    'factor_payload': statement.excluded.factor_payload,
                    'captured_at': statement.excluded.captured_at,
                    'updated_at': refreshed_at,
                },
            )
        )


def upsert_daily_beta_factor_result(result: dict) -> None:
    refreshed_at = datetime.utcnow()
    with session_scope() as session:
        symbol = _get_symbol(session, result['symbol'])
        if symbol is None:
            symbol = Symbol(
                symbol=result['symbol'],
                market=result.get('market', 'KRX'),
                name_ko=result.get('name_ko') or result['symbol'],
                instrument_type='equity',
                source=result.get('source', 'beta_v1'),
                is_active=True,
                last_refreshed_at=refreshed_at,
            )
            session.add(symbol)
            session.flush()

        values = {
            'symbol_id': symbol.id,
            'market_date': _normalize_trade_date(result['market_date']),
            'source': result.get('source', 'beta_v1'),
            'benchmark_id': result.get('benchmark_id', 'KRX_EQW'),
            'benchmark_market': result.get('benchmark_market', 'KRX'),
            'beta_60d': result.get('beta_60d'),
            'beta_120d': result.get('beta_120d'),
            'beta_60d_rank': result.get('beta_60d_rank'),
            'beta_120d_rank': result.get('beta_120d_rank'),
            'beta_60d_percentile': result.get('beta_60d_percentile'),
            'beta_120d_percentile': result.get('beta_120d_percentile'),
            'low_beta_score': result.get('low_beta_score'),
            'low_beta_classification': result.get('low_beta_classification', 'insufficient_history'),
            'is_low_beta_eligible': bool(result.get('is_low_beta_eligible')),
            'history_days_available': int(result.get('history_days_available') or 0),
            'factor_payload': result['factor_payload'],
            'captured_at': refreshed_at,
            'updated_at': refreshed_at,
        }
        statement = pg_insert(DailyBetaFactorResult).values(values)
        session.execute(
            statement.on_conflict_do_update(
                constraint='uq_daily_beta_factor_results_symbol_market_date',
                set_={
                    'source': statement.excluded.source,
                    'benchmark_id': statement.excluded.benchmark_id,
                    'benchmark_market': statement.excluded.benchmark_market,
                    'beta_60d': statement.excluded.beta_60d,
                    'beta_120d': statement.excluded.beta_120d,
                    'beta_60d_rank': statement.excluded.beta_60d_rank,
                    'beta_120d_rank': statement.excluded.beta_120d_rank,
                    'beta_60d_percentile': statement.excluded.beta_60d_percentile,
                    'beta_120d_percentile': statement.excluded.beta_120d_percentile,
                    'low_beta_score': statement.excluded.low_beta_score,
                    'low_beta_classification': statement.excluded.low_beta_classification,
                    'is_low_beta_eligible': statement.excluded.is_low_beta_eligible,
                    'history_days_available': statement.excluded.history_days_available,
                    'factor_payload': statement.excluded.factor_payload,
                    'captured_at': statement.excluded.captured_at,
                    'updated_at': refreshed_at,
                },
            )
        )


def load_daily_beta_factor_results(
    *,
    market_date: str | date,
    symbols: list[str] | None = None,
    is_low_beta_eligible: bool | None = None,
    limit: int | None = None,
) -> list[dict]:
    with session_scope() as session:
        stmt = (
            select(DailyBetaFactorResult, Symbol.symbol, Symbol.name_ko, Symbol.market)
            .join(Symbol, Symbol.id == DailyBetaFactorResult.symbol_id)
            .where(DailyBetaFactorResult.market_date == _normalize_trade_date(market_date))
            .order_by(
                DailyBetaFactorResult.is_low_beta_eligible.desc(),
                DailyBetaFactorResult.low_beta_score.desc().nullslast(),
                DailyBetaFactorResult.beta_120d.asc().nullslast(),
                Symbol.symbol.asc(),
            )
        )
        if limit is not None:
            stmt = stmt.limit(limit)
        if symbols:
            stmt = stmt.where(Symbol.symbol.in_(symbols))
        if is_low_beta_eligible is not None:
            stmt = stmt.where(DailyBetaFactorResult.is_low_beta_eligible == is_low_beta_eligible)

        rows = session.execute(stmt).all()
        return [
            {
                'symbol': symbol,
                'name_ko': name_ko,
                'market': market,
                'market_date': item.market_date.isoformat(),
                'source': item.source,
                'feature_version': item.source,
                'benchmark_id': item.benchmark_id,
                'benchmark_market': item.benchmark_market,
                'beta_60d': _round_float(item.beta_60d, 6),
                'beta_120d': _round_float(item.beta_120d, 6),
                'beta_60d_rank': item.beta_60d_rank,
                'beta_120d_rank': item.beta_120d_rank,
                'beta_60d_percentile': _round_float(item.beta_60d_percentile, 6),
                'beta_120d_percentile': _round_float(item.beta_120d_percentile, 6),
                'low_beta_score': _round_float(item.low_beta_score, 6),
                'low_beta_classification': item.low_beta_classification,
                'is_low_beta_eligible': item.is_low_beta_eligible,
                'history_days_available': item.history_days_available,
                'factor_payload': item.factor_payload,
                'captured_at': item.captured_at.isoformat() if item.captured_at else None,
            }
            for item, symbol, name_ko, market in rows
        ]


def load_daily_low_risk_factor_results(
    *,
    market_date: str | date,
    symbols: list[str] | None = None,
    is_low_risk_eligible: bool | None = None,
    limit: int | None = None,
) -> list[dict]:
    with session_scope() as session:
        stmt = (
            select(DailyLowRiskFactorResult, Symbol.symbol, Symbol.name_ko, Symbol.market)
            .join(Symbol, Symbol.id == DailyLowRiskFactorResult.symbol_id)
            .where(DailyLowRiskFactorResult.market_date == _normalize_trade_date(market_date))
            .order_by(
                DailyLowRiskFactorResult.is_low_risk_eligible.desc(),
                DailyLowRiskFactorResult.low_risk_score.desc().nullslast(),
                DailyLowRiskFactorResult.liquidity_percentile.desc().nullslast(),
                Symbol.symbol.asc(),
            )
        )
        if limit is not None:
            stmt = stmt.limit(limit)
        if symbols:
            stmt = stmt.where(Symbol.symbol.in_(symbols))
        if is_low_risk_eligible is not None:
            stmt = stmt.where(DailyLowRiskFactorResult.is_low_risk_eligible == is_low_risk_eligible)

        rows = session.execute(stmt).all()
        return [
            {
                'symbol': symbol,
                'name_ko': name_ko,
                'market': market,
                'market_date': item.market_date.isoformat(),
                'source': item.source,
                'feature_version': item.source,
                'volatility_60d': _round_float(item.volatility_60d, 6),
                'volatility_120d': _round_float(item.volatility_120d, 6),
                'max_drawdown_120d': _round_float(item.max_drawdown_120d, 6),
                'avg_trading_value_20d': _round_float(item.avg_trading_value_20d, 4),
                'volatility_60d_rank': item.volatility_60d_rank,
                'volatility_120d_rank': item.volatility_120d_rank,
                'max_drawdown_120d_rank': item.max_drawdown_120d_rank,
                'liquidity_rank': item.liquidity_rank,
                'volatility_60d_percentile': _round_float(item.volatility_60d_percentile, 6),
                'volatility_120d_percentile': _round_float(item.volatility_120d_percentile, 6),
                'max_drawdown_120d_percentile': _round_float(item.max_drawdown_120d_percentile, 6),
                'liquidity_percentile': _round_float(item.liquidity_percentile, 6),
                'passes_liquidity_floor': item.passes_liquidity_floor,
                'low_risk_score': _round_float(item.low_risk_score, 6),
                'low_risk_classification': item.low_risk_classification,
                'is_low_risk_eligible': item.is_low_risk_eligible,
                'history_days_available': item.history_days_available,
                'factor_payload': item.factor_payload,
                'captured_at': item.captured_at.isoformat() if item.captured_at else None,
            }
            for item, symbol, name_ko, market in rows
        ]


def upsert_daily_value_factor_result(result: dict) -> None:
    refreshed_at = datetime.utcnow()
    with session_scope() as session:
        symbol = _get_symbol(session, result['symbol'])
        if symbol is None:
            symbol = Symbol(
                symbol=result['symbol'],
                market=result.get('market', 'KRX'),
                name_ko=result.get('name_ko') or result['symbol'],
                instrument_type='equity',
                source=result.get('source', 'value_factor_v1'),
                is_active=True,
                last_refreshed_at=refreshed_at,
            )
            session.add(symbol)
            session.flush()

        values = {
            'symbol_id': symbol.id,
            'market_date': _normalize_trade_date(result['market_date']),
            'source': result.get('source', 'value_factor_v1'),
            'per': result.get('per'),
            'pbr': result.get('pbr'),
            'dividend_yield': result.get('dividend_yield'),
            'per_status': result.get('per_status', 'missing'),
            'per_rank': result.get('per_rank'),
            'pbr_rank': result.get('pbr_rank'),
            'dividend_yield_rank': result.get('dividend_yield_rank'),
            'per_percentile': result.get('per_percentile'),
            'pbr_percentile': result.get('pbr_percentile'),
            'dividend_yield_percentile': result.get('dividend_yield_percentile'),
            'value_score': result.get('value_score'),
            'value_rank': result.get('value_rank'),
            'value_percentile': result.get('value_percentile'),
            'value_status': result.get('value_status', 'missing_metric'),
            'factor_payload': result['factor_payload'],
            'captured_at': refreshed_at,
            'updated_at': refreshed_at,
        }
        statement = pg_insert(DailyValueFactorResult).values(values)
        session.execute(
            statement.on_conflict_do_update(
                constraint='uq_daily_value_factor_results_symbol_market_date',
                set_={
                    'source': statement.excluded.source,
                    'per': statement.excluded.per,
                    'pbr': statement.excluded.pbr,
                    'dividend_yield': statement.excluded.dividend_yield,
                    'per_status': statement.excluded.per_status,
                    'per_rank': statement.excluded.per_rank,
                    'pbr_rank': statement.excluded.pbr_rank,
                    'dividend_yield_rank': statement.excluded.dividend_yield_rank,
                    'per_percentile': statement.excluded.per_percentile,
                    'pbr_percentile': statement.excluded.pbr_percentile,
                    'dividend_yield_percentile': statement.excluded.dividend_yield_percentile,
                    'value_score': statement.excluded.value_score,
                    'value_rank': statement.excluded.value_rank,
                    'value_percentile': statement.excluded.value_percentile,
                    'value_status': statement.excluded.value_status,
                    'factor_payload': statement.excluded.factor_payload,
                    'captured_at': statement.excluded.captured_at,
                    'updated_at': refreshed_at,
                },
            )
        )


def load_daily_value_factor_results(
    *,
    market_date: str | date,
    symbols: list[str] | None = None,
    value_status: str | None = None,
    limit: int | None = None,
) -> list[dict]:
    with session_scope() as session:
        stmt = (
            select(DailyValueFactorResult, Symbol.symbol, Symbol.name_ko, Symbol.market)
            .join(Symbol, Symbol.id == DailyValueFactorResult.symbol_id)
            .where(DailyValueFactorResult.market_date == _normalize_trade_date(market_date))
            .order_by(
                DailyValueFactorResult.value_rank.asc().nullslast(),
                DailyValueFactorResult.value_score.desc().nullslast(),
                Symbol.symbol.asc(),
            )
        )
        if limit is not None:
            stmt = stmt.limit(limit)
        if symbols:
            stmt = stmt.where(Symbol.symbol.in_(symbols))
        if value_status is not None:
            stmt = stmt.where(DailyValueFactorResult.value_status == value_status)

        rows = session.execute(stmt).all()
        return [
            {
                'symbol': symbol,
                'name_ko': name_ko,
                'market': market,
                'market_date': item.market_date.isoformat(),
                'source': item.source,
                'feature_version': item.source,
                'per': _round_float(item.per, 6),
                'pbr': _round_float(item.pbr, 6),
                'dividend_yield': _round_float(item.dividend_yield, 6),
                'per_status': item.per_status,
                'per_rank': item.per_rank,
                'pbr_rank': item.pbr_rank,
                'dividend_yield_rank': item.dividend_yield_rank,
                'per_percentile': _round_float(item.per_percentile, 6),
                'pbr_percentile': _round_float(item.pbr_percentile, 6),
                'dividend_yield_percentile': _round_float(item.dividend_yield_percentile, 6),
                'value_score': _round_float(item.value_score, 6),
                'value_rank': item.value_rank,
                'value_percentile': _round_float(item.value_percentile, 6),
                'value_status': item.value_status,
                'factor_payload': item.factor_payload,
                'captured_at': item.captured_at.isoformat() if item.captured_at else None,
            }
            for item, symbol, name_ko, market in rows
        ]


def upsert_daily_advanced_value_factor_result(result: dict) -> None:
    refreshed_at = datetime.utcnow()
    with session_scope() as session:
        symbol = _get_symbol(session, result['symbol'])
        if symbol is None:
            symbol = Symbol(
                symbol=result['symbol'],
                market=result.get('market', 'KRX'),
                name_ko=result.get('name_ko') or result['symbol'],
                instrument_type='equity',
                source=result.get('source', 'advanced_value_v1'),
                is_active=True,
                last_refreshed_at=refreshed_at,
            )
            session.add(symbol)
            session.flush()

        values = {
            'symbol_id': symbol.id,
            'market_date': _normalize_trade_date(result['market_date']),
            'source': result.get('source', 'advanced_value_v1'),
            'market_cap_proxy': result.get('market_cap_proxy'),
            'enterprise_value': result.get('enterprise_value'),
            'ebitda': result.get('ebitda'),
            'free_cash_flow': result.get('free_cash_flow'),
            'fcf_yield': result.get('fcf_yield'),
            'ev_to_ebitda': result.get('ev_to_ebitda'),
            'fcf_yield_status': result.get('fcf_yield_status', 'missing_metric'),
            'ev_to_ebitda_status': result.get('ev_to_ebitda_status', 'missing_metric'),
            'fcf_yield_valid_for_scoring': bool(result.get('fcf_yield_valid_for_scoring')),
            'ev_to_ebitda_valid_for_scoring': bool(result.get('ev_to_ebitda_valid_for_scoring')),
            'fcf_yield_rank': result.get('fcf_yield_rank'),
            'ev_to_ebitda_rank': result.get('ev_to_ebitda_rank'),
            'fcf_yield_percentile': result.get('fcf_yield_percentile'),
            'ev_to_ebitda_percentile': result.get('ev_to_ebitda_percentile'),
            'advanced_value_score': result.get('advanced_value_score'),
            'advanced_value_rank': result.get('advanced_value_rank'),
            'advanced_value_percentile': result.get('advanced_value_percentile'),
            'advanced_value_components_used': int(result.get('advanced_value_components_used') or 0),
            'advanced_value_classification': result.get(
                'advanced_value_classification',
                'insufficient_components',
            ),
            'is_advanced_value_eligible': bool(result.get('is_advanced_value_eligible')),
            'factor_payload': result['factor_payload'],
            'captured_at': refreshed_at,
            'updated_at': refreshed_at,
        }
        statement = pg_insert(DailyAdvancedValueFactorResult).values(values)
        session.execute(
            statement.on_conflict_do_update(
                constraint='uq_daily_advanced_value_factor_results_symbol_market_date',
                set_={
                    'source': statement.excluded.source,
                    'market_cap_proxy': statement.excluded.market_cap_proxy,
                    'enterprise_value': statement.excluded.enterprise_value,
                    'ebitda': statement.excluded.ebitda,
                    'free_cash_flow': statement.excluded.free_cash_flow,
                    'fcf_yield': statement.excluded.fcf_yield,
                    'ev_to_ebitda': statement.excluded.ev_to_ebitda,
                    'fcf_yield_status': statement.excluded.fcf_yield_status,
                    'ev_to_ebitda_status': statement.excluded.ev_to_ebitda_status,
                    'fcf_yield_valid_for_scoring': statement.excluded.fcf_yield_valid_for_scoring,
                    'ev_to_ebitda_valid_for_scoring': statement.excluded.ev_to_ebitda_valid_for_scoring,
                    'fcf_yield_rank': statement.excluded.fcf_yield_rank,
                    'ev_to_ebitda_rank': statement.excluded.ev_to_ebitda_rank,
                    'fcf_yield_percentile': statement.excluded.fcf_yield_percentile,
                    'ev_to_ebitda_percentile': statement.excluded.ev_to_ebitda_percentile,
                    'advanced_value_score': statement.excluded.advanced_value_score,
                    'advanced_value_rank': statement.excluded.advanced_value_rank,
                    'advanced_value_percentile': statement.excluded.advanced_value_percentile,
                    'advanced_value_components_used': statement.excluded.advanced_value_components_used,
                    'advanced_value_classification': statement.excluded.advanced_value_classification,
                    'is_advanced_value_eligible': statement.excluded.is_advanced_value_eligible,
                    'factor_payload': statement.excluded.factor_payload,
                    'captured_at': statement.excluded.captured_at,
                    'updated_at': refreshed_at,
                },
            )
        )


def load_daily_advanced_value_factor_results(
    *,
    market_date: str | date,
    symbols: list[str] | None = None,
    classification: str | None = None,
    is_eligible: bool | None = None,
    limit: int | None = None,
) -> list[dict]:
    with session_scope() as session:
        stmt = (
            select(DailyAdvancedValueFactorResult, Symbol.symbol, Symbol.name_ko, Symbol.market)
            .join(Symbol, Symbol.id == DailyAdvancedValueFactorResult.symbol_id)
            .where(DailyAdvancedValueFactorResult.market_date == _normalize_trade_date(market_date))
            .order_by(
                DailyAdvancedValueFactorResult.advanced_value_rank.asc().nullslast(),
                DailyAdvancedValueFactorResult.advanced_value_score.desc().nullslast(),
                Symbol.symbol.asc(),
            )
        )
        if limit is not None:
            stmt = stmt.limit(limit)
        if symbols:
            stmt = stmt.where(Symbol.symbol.in_(symbols))
        if classification is not None:
            stmt = stmt.where(DailyAdvancedValueFactorResult.advanced_value_classification == classification)
        if is_eligible is not None:
            stmt = stmt.where(DailyAdvancedValueFactorResult.is_advanced_value_eligible == is_eligible)

        rows = session.execute(stmt).all()
        return [
            {
                'symbol': symbol,
                'name_ko': name_ko,
                'market': market,
                'market_date': item.market_date.isoformat(),
                'source': item.source,
                'feature_version': item.source,
                'market_cap_proxy': _round_float(item.market_cap_proxy, 4),
                'enterprise_value': _round_float(item.enterprise_value, 4),
                'ebitda': _round_float(item.ebitda, 4),
                'free_cash_flow': _round_float(item.free_cash_flow, 4),
                'fcf_yield': _round_float(item.fcf_yield, 6),
                'ev_to_ebitda': _round_float(item.ev_to_ebitda, 6),
                'fcf_yield_status': item.fcf_yield_status,
                'ev_to_ebitda_status': item.ev_to_ebitda_status,
                'fcf_yield_valid_for_scoring': item.fcf_yield_valid_for_scoring,
                'ev_to_ebitda_valid_for_scoring': item.ev_to_ebitda_valid_for_scoring,
                'fcf_yield_rank': item.fcf_yield_rank,
                'ev_to_ebitda_rank': item.ev_to_ebitda_rank,
                'fcf_yield_percentile': _round_float(item.fcf_yield_percentile, 6),
                'ev_to_ebitda_percentile': _round_float(item.ev_to_ebitda_percentile, 6),
                'advanced_value_score': _round_float(item.advanced_value_score, 6),
                'advanced_value_rank': item.advanced_value_rank,
                'advanced_value_percentile': _round_float(item.advanced_value_percentile, 6),
                'advanced_value_components_used': item.advanced_value_components_used,
                'advanced_value_classification': item.advanced_value_classification,
                'is_advanced_value_eligible': item.is_advanced_value_eligible,
                'factor_payload': item.factor_payload,
                'captured_at': item.captured_at.isoformat() if item.captured_at else None,
            }
            for item, symbol, name_ko, market in rows
        ]


def upsert_daily_composite_factor_result(result: dict) -> None:
    refreshed_at = datetime.utcnow()
    with session_scope() as session:
        symbol = _get_symbol(session, result['symbol'])
        if symbol is None:
            symbol = Symbol(
                symbol=result['symbol'],
                market=result.get('market', 'KRX'),
                name_ko=result.get('name_ko') or result['symbol'],
                instrument_type='equity',
                source=result.get('source', 'composite_v1'),
                is_active=True,
                last_refreshed_at=refreshed_at,
            )
            session.add(symbol)
            session.flush()

        values = {
            'symbol_id': symbol.id,
            'market_date': _normalize_trade_date(result['market_date']),
            'source': result.get('source', 'composite_v1'),
            'momentum_input': result.get('momentum_input'),
            'value_input': result.get('value_input'),
            'advanced_value_input': result.get('advanced_value_input'),
            'low_risk_input': result.get('low_risk_input'),
            'low_beta_input': result.get('low_beta_input'),
            'momentum_contribution': result.get('momentum_contribution'),
            'value_contribution': result.get('value_contribution'),
            'advanced_value_contribution': result.get('advanced_value_contribution'),
            'low_risk_contribution': result.get('low_risk_contribution'),
            'low_beta_contribution': result.get('low_beta_contribution'),
            'valid_factor_count': int(result.get('valid_factor_count') or 0),
            'available_base_weight': result.get('available_base_weight') or 0.0,
            'has_required_inputs': bool(result.get('has_required_inputs')),
            'is_hard_excluded': bool(result.get('is_hard_excluded')),
            'composite_score': result.get('composite_score'),
            'composite_rank': result.get('composite_rank'),
            'composite_percentile': result.get('composite_percentile'),
            'composite_classification': result.get('composite_classification', 'insufficient_inputs'),
            'is_composite_eligible': bool(result.get('is_composite_eligible')),
            'factor_payload': result['factor_payload'],
            'captured_at': refreshed_at,
            'updated_at': refreshed_at,
        }
        statement = pg_insert(DailyCompositeFactorResult).values(values)
        session.execute(
            statement.on_conflict_do_update(
                constraint='uq_daily_composite_factor_results_symbol_market_date',
                set_={
                    'source': statement.excluded.source,
                    'momentum_input': statement.excluded.momentum_input,
                    'value_input': statement.excluded.value_input,
                    'advanced_value_input': statement.excluded.advanced_value_input,
                    'low_risk_input': statement.excluded.low_risk_input,
                    'low_beta_input': statement.excluded.low_beta_input,
                    'momentum_contribution': statement.excluded.momentum_contribution,
                    'value_contribution': statement.excluded.value_contribution,
                    'advanced_value_contribution': statement.excluded.advanced_value_contribution,
                    'low_risk_contribution': statement.excluded.low_risk_contribution,
                    'low_beta_contribution': statement.excluded.low_beta_contribution,
                    'valid_factor_count': statement.excluded.valid_factor_count,
                    'available_base_weight': statement.excluded.available_base_weight,
                    'has_required_inputs': statement.excluded.has_required_inputs,
                    'is_hard_excluded': statement.excluded.is_hard_excluded,
                    'composite_score': statement.excluded.composite_score,
                    'composite_rank': statement.excluded.composite_rank,
                    'composite_percentile': statement.excluded.composite_percentile,
                    'composite_classification': statement.excluded.composite_classification,
                    'is_composite_eligible': statement.excluded.is_composite_eligible,
                    'factor_payload': statement.excluded.factor_payload,
                    'captured_at': statement.excluded.captured_at,
                    'updated_at': refreshed_at,
                },
            )
        )


def load_daily_composite_factor_results(
    *,
    market_date: str | date,
    symbols: list[str] | None = None,
    limit: int | None = None,
) -> list[dict]:
    classification_order = case(
        (DailyCompositeFactorResult.composite_classification == 'eligible', 0),
        (DailyCompositeFactorResult.composite_classification == 'watchlist', 1),
        (DailyCompositeFactorResult.composite_classification == 'ineligible', 2),
        (DailyCompositeFactorResult.composite_classification == 'hard_excluded', 3),
        else_=4,
    )
    with session_scope() as session:
        stmt = (
            select(DailyCompositeFactorResult, Symbol.symbol, Symbol.name_ko, Symbol.market)
            .join(Symbol, Symbol.id == DailyCompositeFactorResult.symbol_id)
            .where(DailyCompositeFactorResult.market_date == _normalize_trade_date(market_date))
            .order_by(
                classification_order.asc(),
                DailyCompositeFactorResult.composite_rank.asc().nullslast(),
                Symbol.symbol.asc(),
            )
        )
        if limit is not None:
            stmt = stmt.limit(limit)
        if symbols:
            stmt = stmt.where(Symbol.symbol.in_(symbols))

        rows = session.execute(stmt).all()
        items: list[dict] = []
        for item, symbol, name_ko, market in rows:
            payload = item.factor_payload or {}
            weights = payload.get('weights') if isinstance(payload, dict) else None
            gates = payload.get('gates') if isinstance(payload, dict) else None
            inputs = {
                'momentum_percentile': _round_float(item.momentum_input, 6),
                'value_score_v1': _round_float(item.value_input, 6),
                'advanced_value_score': _round_float(item.advanced_value_input, 6),
                'low_risk_score': _round_float(item.low_risk_input, 6),
                'low_beta_score': _round_float(item.low_beta_input, 6),
            }
            contributions = {
                'momentum': _round_float(item.momentum_contribution, 6),
                'value_v1': _round_float(item.value_contribution, 6),
                'advanced_value': _round_float(item.advanced_value_contribution, 6),
                'low_risk': _round_float(item.low_risk_contribution, 6),
                'beta': _round_float(item.low_beta_contribution, 6),
            }
            items.append(
                {
                    'symbol': symbol,
                    'name_ko': name_ko,
                    'market': market,
                    'market_date': item.market_date.isoformat(),
                    'source': item.source,
                    'feature_version': item.source,
                    'momentum_input': inputs['momentum_percentile'],
                    'value_input': inputs['value_score_v1'],
                    'advanced_value_input': inputs['advanced_value_score'],
                    'low_risk_input': inputs['low_risk_score'],
                    'low_beta_input': inputs['low_beta_score'],
                    'momentum_contribution': contributions['momentum'],
                    'value_contribution': contributions['value_v1'],
                    'advanced_value_contribution': contributions['advanced_value'],
                    'low_risk_contribution': contributions['low_risk'],
                    'low_beta_contribution': contributions['beta'],
                    'inputs': inputs,
                    'weights': weights,
                    'contributions': contributions,
                    'gates': gates,
                    'valid_factor_count': item.valid_factor_count,
                    'available_base_weight': _round_float(item.available_base_weight, 6),
                    'has_required_inputs': item.has_required_inputs,
                    'is_hard_excluded': item.is_hard_excluded,
                    'composite_score': _round_float(item.composite_score, 6),
                    'composite_rank': item.composite_rank,
                    'composite_percentile': _round_float(item.composite_percentile, 6),
                    'composite_classification': item.composite_classification,
                    'is_composite_eligible': item.is_composite_eligible,
                    'factor_payload': item.factor_payload,
                    'captured_at': item.captured_at.isoformat() if item.captured_at else None,
                }
            )
        return items


def load_daily_signal_feature_context_rows(
    *,
    market_date: str | date,
) -> list[dict]:
    normalized_market_date = _normalize_trade_date(market_date)
    with session_scope() as session:
        rows = session.execute(
            select(DailySignalFeature, Symbol)
            .join(Symbol, Symbol.id == DailySignalFeature.symbol_id)
            .where(DailySignalFeature.market_date == normalized_market_date)
            .order_by(
                DailySignalFeature.signal_count.desc(),
                DailySignalFeature.volume_power_rank.asc().nullslast(),
                DailySignalFeature.overtime_volume_rank.asc().nullslast(),
                DailySignalFeature.overtime_fluctuation_rank.asc().nullslast(),
                Symbol.symbol.asc(),
            )
        ).all()

        return [
            {
                'symbol': symbol.symbol,
                'name_ko': symbol.name_ko,
                'market': symbol.market,
                'signal_count': feature.signal_count,
                'volume_power_rank': feature.volume_power_rank,
                'volume_power': _decimal_to_float(feature.volume_power),
                'overtime_volume_rank': feature.overtime_volume_rank,
                'overtime_volume': int(feature.overtime_volume) if feature.overtime_volume is not None else None,
                'overtime_fluctuation_rank': feature.overtime_fluctuation_rank,
                'overtime_fluctuation_rate': _decimal_to_float(feature.overtime_fluctuation_rate),
                'feature_payload': feature.feature_payload or {},
            }
            for feature, symbol in rows
        ]


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


def upsert_daily_market_participation_context(
    *,
    market_date: str | date,
    context_payload: dict,
    market: str = 'KRX',
    source: str = 'kis_ranking',
) -> None:
    refreshed_at = datetime.utcnow()
    with session_scope() as session:
        values = {
            'market': market,
            'market_date': _normalize_trade_date(market_date),
            'source': source,
            'context_payload': context_payload,
            'captured_at': refreshed_at,
            'updated_at': refreshed_at,
        }
        statement = pg_insert(DailyMarketParticipationContext).values(values)
        session.execute(
            statement.on_conflict_do_update(
                constraint='uq_daily_market_participation_contexts_market_market_date',
                set_={
                    'source': statement.excluded.source,
                    'context_payload': statement.excluded.context_payload,
                    'captured_at': statement.excluded.captured_at,
                    'updated_at': refreshed_at,
                },
            )
        )


def load_daily_market_participation_context(
    *,
    market_date: str | date,
    market: str = 'KRX',
) -> dict | None:
    normalized_market_date = _normalize_trade_date(market_date)
    with session_scope() as session:
        row = session.execute(
            select(DailyMarketParticipationContext)
            .where(DailyMarketParticipationContext.market == market)
            .where(DailyMarketParticipationContext.market_date == normalized_market_date)
        ).scalar_one_or_none()
        if row is None:
            return None
        return {
            'market': row.market,
            'market_date': row.market_date.isoformat(),
            'source': row.source,
            'context_payload': row.context_payload,
            'captured_at': row.captured_at.isoformat() if row.captured_at else None,
        }


def load_daily_market_participation_context_history(
    *,
    market_date: str | date,
    market: str = 'KRX',
    history_days: int,
) -> list[dict]:
    if history_days <= 0:
        return []

    normalized_market_date = _normalize_trade_date(market_date)
    with session_scope() as session:
        rows = session.execute(
            select(DailyMarketParticipationContext)
            .where(DailyMarketParticipationContext.market == market)
            .where(DailyMarketParticipationContext.market_date <= normalized_market_date)
            .order_by(DailyMarketParticipationContext.market_date.desc())
            .limit(history_days)
        ).scalars().all()

    return [
        {
            'market': row.market,
            'market_date': row.market_date.isoformat(),
            'source': row.source,
            'context_payload': row.context_payload,
            'captured_at': row.captured_at.isoformat() if row.captured_at else None,
        }
        for row in reversed(rows)
    ]
