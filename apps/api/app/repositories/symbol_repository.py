from __future__ import annotations

from datetime import datetime, timezone
import logging

from sqlalchemy import case, delete, select, text, update
from sqlalchemy.dialects.postgresql import insert as pg_insert

from app.db import session_scope
from app.db_models import Symbol, TrackedSymbol
from app.scanner.state import scanner_state


logger = logging.getLogger(__name__)
SYMBOL_UNIVERSE_UPSERT_BATCH_SIZE = 1_000


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _iter_symbol_universe_chunks(rows: list[dict]):
    for start in range(0, len(rows), SYMBOL_UNIVERSE_UPSERT_BATCH_SIZE):
        yield rows[start:start + SYMBOL_UNIVERSE_UPSERT_BATCH_SIZE]


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
                last_refreshed_at=_utcnow(),
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
        record.last_refreshed_at = _utcnow()
        session.flush()
        return record.id


def _coerce_optional_text(value: object) -> str | None:
    if value is None:
        return None
    if isinstance(value, str):
        normalized = value.strip()
        return normalized or None
    return None


def _coerce_required_text(value: object) -> str | None:
    if isinstance(value, str):
        normalized = value.strip()
        return normalized or None
    return None


def _build_symbol_universe_rows(items: list, *, source: str, instrument_type: str, refreshed_at: datetime) -> tuple[list[dict], list[object]]:
    symbol_rows: list[dict] = []
    valid_items: list[object] = []
    skipped_items = 0

    for entry in items:
        symbol = _coerce_required_text(getattr(entry, 'symbol', None))
        market = _coerce_required_text(getattr(entry, 'market', None))
        if symbol:
            symbol = symbol.upper()
            if symbol.isdigit():
                symbol = symbol.zfill(6)
        name_ko = _coerce_optional_text(getattr(entry, 'name_ko', None)) or symbol
        name_en = _coerce_optional_text(getattr(entry, 'name_en', None))
        standard_code = _coerce_optional_text(getattr(entry, 'standard_code', None))
        country_code = _coerce_optional_text(getattr(entry, 'country_code', None))
        currency_code = _coerce_optional_text(getattr(entry, 'currency_code', None))
        exchange_code = _coerce_optional_text(getattr(entry, 'exchange_code', None))
        regulatory_id = _coerce_optional_text(getattr(entry, 'regulatory_id', None))
        raw_source_payload = getattr(entry, 'source_payload', None)
        source_payload = raw_source_payload if isinstance(raw_source_payload, dict) else {}
        if not symbol or not market:
            skipped_items += 1
            logger.warning(
                'Skipping invalid symbol universe row from %s: symbol_type=%s market_type=%s',
                source,
                type(getattr(entry, 'symbol', None)).__name__,
                type(getattr(entry, 'market', None)).__name__,
            )
            continue
        symbol_rows.append(
            {
                'symbol': symbol,
                'standard_code': standard_code,
                'name_ko': name_ko,
                'name_en': name_en,
                'market': market,
                'country_code': country_code,
                'currency_code': currency_code,
                'exchange_code': exchange_code,
                'regulatory_id': regulatory_id,
                'source_payload': source_payload,
                'instrument_type': instrument_type,
                'source': source,
                'is_active': True,
                'last_refreshed_at': refreshed_at,
            }
        )
        valid_items.append(entry)

    if skipped_items:
        logger.warning('Skipped %s invalid symbol universe rows from %s before DB upsert', skipped_items, source)
    return symbol_rows, valid_items


def upsert_symbol_universe(
    items: list,
    *,
    source: str,
    stale_sources: list[str] | None = None,
    stale_markets: list[str] | None = None,
    instrument_type: str = 'equity',
    preserve_existing_lifecycle: bool = False,
) -> int:
    if not items:
        return 0

    refreshed_at = _utcnow()
    symbol_rows, valid_items = _build_symbol_universe_rows(
        items,
        source=source,
        instrument_type=instrument_type,
        refreshed_at=refreshed_at,
    )
    if not symbol_rows:
        return 0

    with session_scope() as session:
        for symbol_chunk in _iter_symbol_universe_chunks(symbol_rows):
            statement = pg_insert(Symbol).values(symbol_chunk)
            conflict_updates = {
                'standard_code': statement.excluded.standard_code,
                'name_ko': statement.excluded.name_ko,
                'name_en': statement.excluded.name_en,
                'market': statement.excluded.market,
                'country_code': statement.excluded.country_code,
                'currency_code': statement.excluded.currency_code,
                'exchange_code': statement.excluded.exchange_code,
                'regulatory_id': statement.excluded.regulatory_id,
                'source_payload': statement.excluded.source_payload,
                'instrument_type': statement.excluded.instrument_type,
                'last_refreshed_at': statement.excluded.last_refreshed_at,
                'updated_at': refreshed_at,
            }
            if preserve_existing_lifecycle:
                existing_owned_by_source = Symbol.source == source
                conflict_updates.update(
                    {
                        'source': case(
                            (existing_owned_by_source, statement.excluded.source),
                            else_=Symbol.source,
                        ),
                        'is_active': case(
                            (existing_owned_by_source, statement.excluded.is_active),
                            else_=Symbol.is_active,
                        ),
                        'source_payload': case(
                            (existing_owned_by_source, statement.excluded.source_payload),
                            else_=Symbol.source_payload,
                        ),
                        'last_refreshed_at': case(
                            (
                                existing_owned_by_source,
                                statement.excluded.last_refreshed_at,
                            ),
                            else_=Symbol.last_refreshed_at,
                        ),
                    }
                )
            else:
                conflict_updates.update(
                    {
                        'source': statement.excluded.source,
                        'is_active': statement.excluded.is_active,
                    }
                )
            session.execute(
                statement.on_conflict_do_update(
                    index_elements=[Symbol.symbol],
                    set_=conflict_updates,
                )
            )
        stale_source_names = [source] if stale_sources is None else stale_sources
        if stale_source_names:
            valid_symbols = [row['symbol'] for row in symbol_rows]
            target_markets = stale_markets or list({row['market'] for row in symbol_rows if row.get('market')})
            stale_stmt = (
                update(Symbol)
                .where(Symbol.source.in_(stale_source_names), ~Symbol.symbol.in_(valid_symbols))
                .values(is_active=False, last_refreshed_at=refreshed_at, updated_at=refreshed_at)
            )
            if target_markets:
                stale_stmt = stale_stmt.where(Symbol.market.in_(target_markets))
            session.execute(stale_stmt)
    return len(valid_items)


def upsert_master_symbols(items: list) -> int:
    return upsert_symbol_universe(
        items,
        source='kis_master',
        preserve_existing_lifecycle=True,
    )


def load_symbols(
    *,
    is_active: bool | None = None,
    source: str | None = None,
    markets: list[str] | None = None,
    symbols: list[str] | None = None,
    limit: int | None = None,
) -> list[dict]:
    if symbols and len(symbols) > 500 and limit is None:
        items: list[dict] = []
        for start in range(0, len(symbols), 500):
            items.extend(
                load_symbols(
                    is_active=is_active,
                    source=source,
                    markets=markets,
                    symbols=symbols[start:start + 500],
                    limit=None,
                )
            )
        return sorted(items, key=lambda item: (item.get('market') or '', item.get('symbol') or ''))

    with session_scope() as session:
        session.execute(text("SET LOCAL statement_timeout = '60s'"))
        stmt = select(
            Symbol.id,
            Symbol.symbol,
            Symbol.standard_code,
            Symbol.name_ko,
            Symbol.name_en,
            Symbol.market,
            Symbol.country_code,
            Symbol.currency_code,
            Symbol.exchange_code,
            Symbol.regulatory_id,
            Symbol.source_payload,
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
                    last_refreshed_at=_utcnow(),
                )
                session.add(record)
                session.flush()
            session.add(TrackedSymbol(symbol_id=record.id, sort_order=index))


def load_favorite_symbols() -> list[dict]:
    with session_scope() as session:
        rows = session.execute(
            select(
                Symbol.name_ko,
                Symbol.standard_code,
                Symbol.symbol,
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
