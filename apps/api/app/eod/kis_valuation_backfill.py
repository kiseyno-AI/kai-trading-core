from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, Sequence

from sqlalchemy import and_, select, text, update

from app.brokers.kis.client import KISClient
from app.db import session_scope
from app.db_models import DailyPriceBar, Symbol

KIS_VALUATION_BACKFILL_SOURCE = 'kis_quote_valuation'


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _normalize_market_date(value: str | date) -> date:
    return value if isinstance(value, date) else date.fromisoformat(value)


def _normalize_symbol(value: str) -> str:
    text = str(value or '').strip().upper()
    return text.zfill(6) if text.isdigit() else text


def _to_decimal(value: Any) -> Decimal | None:
    if value in (None, ''):
        return None
    try:
        converted = Decimal(str(value).replace(',', '').strip())
    except (InvalidOperation, ValueError):
        return None
    return converted if converted == converted else None


def _to_float(value: Decimal | None) -> float | None:
    return float(value) if value is not None else None


@dataclass(frozen=True, slots=True)
class ValuationTarget:
    symbol: str
    market: str | None
    close_price: int
    prior_dps: Decimal | None
    prior_dividend_yield: Decimal | None
    prior_close_price: int | None


def _load_targets(
    *,
    market_date: date,
    symbols: Sequence[str] | None = None,
    limit: int | None = None,
) -> list[ValuationTarget]:
    normalized_symbols = [_normalize_symbol(symbol) for symbol in symbols or [] if str(symbol or '').strip()]
    if not normalized_symbols:
        with session_scope() as session:
            rows = session.execute(
                text(
                    """
                    with current_rows as (
                        select s.symbol, s.market, b.close_price, b.symbol_id
                        from daily_price_bars b
                        join symbols s on s.id = b.symbol_id
                        where b.trade_date = :market_date
                        order by s.symbol asc
                        limit :row_limit
                    ),
                    latest_valuation_date as (
                        select max(trade_date) as trade_date
                        from daily_price_bars
                        where trade_date < :market_date
                          and trade_date >= :lower_bound
                          and (dps is not null or dividend_yield is not null)
                    )
                    select
                        c.symbol,
                        c.market,
                        c.close_price,
                        p.dps as prior_dps,
                        p.dividend_yield as prior_dividend_yield,
                        p.close_price as prior_close_price
                    from current_rows c
                    left join daily_price_bars p
                      on p.symbol_id = c.symbol_id
                     and p.trade_date = (select trade_date from latest_valuation_date)
                    order by c.symbol asc
                    """
                ),
                {
                    'market_date': market_date,
                    'lower_bound': market_date - timedelta(days=60),
                    'row_limit': int(limit) if limit is not None else 1000000,
                },
            ).all()
            return [
                ValuationTarget(
                    symbol=row.symbol,
                    market=row.market,
                    close_price=int(row.close_price),
                    prior_dps=row.prior_dps,
                    prior_dividend_yield=row.prior_dividend_yield,
                    prior_close_price=int(row.prior_close_price) if row.prior_close_price is not None else None,
                )
                for row in rows
            ]
    with session_scope() as session:
        stmt = (
            select(Symbol.symbol, Symbol.market, DailyPriceBar.close_price)
            .join(DailyPriceBar, DailyPriceBar.symbol_id == Symbol.id)
            .where(DailyPriceBar.trade_date == market_date)
            .order_by(Symbol.symbol.asc())
        )
        if normalized_symbols:
            stmt = stmt.where(Symbol.symbol.in_(normalized_symbols))
        if limit is not None:
            stmt = stmt.limit(max(0, int(limit)))
        current_rows = session.execute(stmt).all()
        current_symbols = [row.symbol for row in current_rows]
        prior_by_symbol: dict[str, Any] = {}
        if current_symbols:
            current_symbol_set = set(current_symbols)
            prior_rows = session.execute(
                select(
                    Symbol.symbol,
                    DailyPriceBar.trade_date,
                    DailyPriceBar.dps,
                    DailyPriceBar.dividend_yield,
                    DailyPriceBar.close_price,
                )
                .join(Symbol, Symbol.id == DailyPriceBar.symbol_id)
                .where(
                    DailyPriceBar.trade_date < market_date,
                    DailyPriceBar.trade_date >= market_date - timedelta(days=60),
                    (
                        DailyPriceBar.dps.is_not(None)
                        | DailyPriceBar.dividend_yield.is_not(None)
                    ),
                )
                .order_by(Symbol.symbol.asc(), DailyPriceBar.trade_date.desc())
            ).all()
            for row in prior_rows:
                if row.symbol not in current_symbol_set:
                    continue
                prior_by_symbol.setdefault(row.symbol, row)

        targets: list[ValuationTarget] = []
        for symbol, market, close_price in current_rows:
            prior = prior_by_symbol.get(symbol)
            prior_dps = prior.dps if prior is not None else None
            prior_dividend_yield = prior.dividend_yield if prior is not None else None
            prior_close_price = int(prior.close_price) if prior is not None and prior.close_price is not None else None
            targets.append(
                ValuationTarget(
                    symbol=symbol,
                    market=market,
                    close_price=int(close_price),
                    prior_dps=prior_dps,
                    prior_dividend_yield=prior_dividend_yield,
                    prior_close_price=prior_close_price,
                )
            )
        return targets


def _derive_dps(target: ValuationTarget) -> Decimal | None:
    if target.prior_dps is not None:
        return target.prior_dps
    if target.prior_dividend_yield is None or not target.prior_close_price:
        return None
    return (Decimal(target.prior_close_price) * target.prior_dividend_yield / Decimal('100')).quantize(Decimal('0.000001'))


def _derive_dividend_yield(*, dps: Decimal | None, close_price: int) -> Decimal | None:
    if dps is None or close_price <= 0:
        return None
    return (dps / Decimal(close_price) * Decimal('100')).quantize(Decimal('0.000001'))


def _extract_valuation(raw: dict[str, Any], target: ValuationTarget) -> dict[str, Decimal | None]:
    output = raw.get('output') if isinstance(raw.get('output'), dict) else {}
    dps = _to_decimal(output.get('dps')) or _to_decimal(output.get('stck_dvdn_amt')) or _derive_dps(target)
    dividend_yield = (
        _to_decimal(output.get('dividend_yield'))
        or _to_decimal(output.get('dvd_yld'))
        or _to_decimal(output.get('stck_dvdn_yld'))
        or _derive_dividend_yield(dps=dps, close_price=target.close_price)
    )
    return {
        'per': _to_decimal(output.get('per')),
        'pbr': _to_decimal(output.get('pbr')),
        'bps': _to_decimal(output.get('bps')),
        'eps': _to_decimal(output.get('eps')),
        'dps': dps,
        'dividend_yield': dividend_yield,
    }


def _persist_valuation(*, market_date: date, symbol: str, valuation: dict[str, Decimal | None]) -> int:
    if valuation.get('per') is None or valuation.get('pbr') is None:
        return 0
    refreshed_at = _utcnow()
    with session_scope() as session:
        symbol_id = session.execute(select(Symbol.id).where(Symbol.symbol == symbol).limit(1)).scalar_one_or_none()
        if symbol_id is None:
            return 0
        result = session.execute(
            update(DailyPriceBar)
            .where(and_(DailyPriceBar.symbol_id == symbol_id, DailyPriceBar.trade_date == market_date))
            .values(
                per=valuation.get('per'),
                pbr=valuation.get('pbr'),
                bps=valuation.get('bps'),
                eps=valuation.get('eps'),
                dividend_yield=valuation.get('dividend_yield'),
                dps=valuation.get('dps'),
                source=KIS_VALUATION_BACKFILL_SOURCE,
                captured_at=refreshed_at,
            )
        )
        return int(result.rowcount or 0)


async def backfill_kis_quote_valuation_for_market_date(
    *,
    market_date: str | date,
    symbols: Sequence[str] | None = None,
    limit: int | None = None,
    sleep_seconds: float = 0.05,
    request_timeout_seconds: float = 12.0,
) -> dict[str, Any]:
    normalized_market_date = _normalize_market_date(market_date)
    targets = _load_targets(market_date=normalized_market_date, symbols=symbols, limit=limit)

    client = KISClient()
    fetched = 0
    updated = 0
    skipped = 0
    failed = 0
    failures: list[dict[str, str]] = []
    dividend_basis_rows = 0
    for target in targets:
        try:
            raw = await asyncio.wait_for(
                client.fetch_domestic_quote(symbol=target.symbol),
                timeout=max(1.0, float(request_timeout_seconds)),
            )
            fetched += 1
            valuation = _extract_valuation(raw, target)
            if valuation.get('dps') is not None:
                dividend_basis_rows += 1
            persisted = _persist_valuation(
                market_date=normalized_market_date,
                symbol=target.symbol,
                valuation=valuation,
            )
            if persisted:
                updated += persisted
            else:
                skipped += 1
        except Exception as exc:
            failed += 1
            if len(failures) < 20:
                failures.append({'symbol': target.symbol, 'error': str(exc)[:300]})
        if sleep_seconds > 0:
            await asyncio.sleep(sleep_seconds)

    return {
        'market_date': normalized_market_date.isoformat(),
        'source': KIS_VALUATION_BACKFILL_SOURCE,
        'targets': len(targets),
        'fetched': fetched,
        'updated': updated,
        'skipped': skipped,
        'failed': failed,
        'dividend_basis_rows': dividend_basis_rows,
        'failures': failures,
    }
