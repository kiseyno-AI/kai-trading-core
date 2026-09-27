from __future__ import annotations

import argparse
import asyncio
from collections.abc import Awaitable, Callable, Iterable, Mapping, Sequence
from contextlib import AbstractContextManager, ExitStack, contextmanager
from dataclasses import dataclass
from datetime import date, datetime, timezone
import json
import re
import sys
from typing import Any
from uuid import uuid4

from sqlalchemy import and_, bindparam, insert, select, update

from app.brokers.kis.provider import kis_real_client
from app.db import session_scope
from app.db_models import (
    DailyPriceBar,
    DailyPriceBarRepairAudit,
    DailyPriceBarRepairBatch,
    Symbol,
)
from app.market_data_store import (
    advisory_eod_collection_lock,
    advisory_eod_collection_locks,
)
from app.market.clock import KST
from app.eod.finality import validate_finalized_krx_write_date
from app.eod.price_bar_repair_state import (
    load_pending_price_bar_rebuild_batches,
)


MAX_REPAIR_CALENDAR_DAYS = 120
MAX_SYMBOL_LIMIT = 500
DEFAULT_SYMBOL_LIMIT = 100
MAX_CONCURRENCY = 4
PROVIDER_ATTEMPTS = 3
MAX_PRICE_DEPENDENT_EVALUATION_HORIZON = 10
LOCK_SOURCE = 'daily_price_bars'
SUPPORTED_MARKETS = ('KOSPI', 'KOSDAQ')
_SYMBOL_PATTERN = re.compile(r'[0-9A-Z]{6}')


@dataclass(frozen=True)
class StoredFinalBar:
    bar_id: int
    symbol: str
    trade_date: date
    open_price: int
    high_price: int
    low_price: int
    close_price: int
    volume: int
    turnover: int
    source: str = ''
    captured_at: datetime | None = None
    eod_run_id: int | None = None
    repair_origin_eod_run_id: int | None = None
    repair_origin_source: str | None = None

    @property
    def values(self) -> tuple[int, int, int, int, int, int]:
        return (
            self.open_price,
            self.high_price,
            self.low_price,
            self.close_price,
            self.volume,
            self.turnover,
        )


@dataclass(frozen=True)
class ProviderFinalBar:
    symbol: str
    trade_date: date
    open_price: int
    high_price: int
    low_price: int
    close_price: int
    volume: int
    turnover: int

    @property
    def values(self) -> tuple[int, int, int, int, int, int]:
        return (
            self.open_price,
            self.high_price,
            self.low_price,
            self.close_price,
            self.volume,
            self.turnover,
        )


@dataclass(frozen=True)
class TargetPage:
    symbols: tuple[str, ...]
    rows: tuple[StoredFinalBar, ...]
    has_more: bool


def _normalize_date(value: str | date) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    raw = str(value or '').strip()
    for pattern in ('%Y-%m-%d', '%Y%m%d'):
        try:
            return datetime.strptime(raw, pattern).date()
        except ValueError:
            continue
    raise ValueError('date must use YYYY-MM-DD or YYYYMMDD format')


def _normalize_bounds(
    start_date: str | date,
    end_date: str | date,
) -> tuple[date, date]:
    normalized_start = _normalize_date(start_date)
    normalized_end = _normalize_date(end_date)
    if normalized_start > normalized_end:
        raise ValueError('start date must not be after end date')
    calendar_days = (normalized_end - normalized_start).days + 1
    if calendar_days > MAX_REPAIR_CALENDAR_DAYS:
        raise ValueError(
            f'KIS final-bar repair is limited to {MAX_REPAIR_CALENDAR_DAYS} '
            'calendar days'
        )
    return normalized_start, normalized_end


def _normalize_symbol(value: object) -> str:
    symbol = str(value or '').strip().upper()
    if _SYMBOL_PATTERN.fullmatch(symbol) is None:
        raise ValueError(f'invalid KRX symbol: {value!r}')
    return symbol


def _normalize_cursor(value: str | None) -> str:
    if value in (None, ''):
        return ''
    return _normalize_symbol(value)


def _coerce_nonnegative_integer(
    value: object,
    *,
    symbol: str,
    field: str,
) -> int:
    if isinstance(value, bool):
        raise ValueError(f'{field} is not an integer for {symbol}')
    raw = str(value).strip() if value is not None else ''
    if not raw or not raw.isascii() or not raw.isdigit():
        raise ValueError(f'{field} is not an integer for {symbol}')
    normalized = int(raw)
    if normalized < 0:
        raise ValueError(f'{field} is negative for {symbol}')
    return normalized


def _validate_final_values(
    *,
    symbol: str,
    open_price: int,
    high_price: int,
    low_price: int,
    close_price: int,
    volume: int,
    turnover: int,
) -> None:
    values = (open_price, high_price, low_price, close_price)
    if volume > 0:
        if any(value <= 0 for value in values):
            raise ValueError(
                f'positive-volume KIS row has non-positive OHLC for {symbol}'
            )
        if turnover <= 0:
            raise ValueError(
                f'positive-volume KIS row has non-positive turnover for {symbol}'
            )
    elif turnover != 0:
        raise ValueError(
            f'zero-volume KIS row has non-zero turnover for {symbol}'
        )
    if any(values) and (
        high_price < max(values) or low_price > min(values)
    ):
        raise ValueError(f'KIS row has invalid OHLC geometry for {symbol}')


def extract_kis_final_bars(
    payload: object,
    *,
    symbol: str,
    start_date: date,
    end_date: date,
) -> dict[date, ProviderFinalBar]:
    normalized_symbol = _normalize_symbol(symbol)
    if not isinstance(payload, Mapping):
        raise ValueError(f'KIS payload is not an object for {normalized_symbol}')
    rows = payload.get('output2')
    if not isinstance(rows, list) or not rows:
        raise ValueError(f'KIS payload has no daily rows for {normalized_symbol}')

    bars: dict[date, ProviderFinalBar] = {}
    for row in rows:
        if not isinstance(row, Mapping):
            raise ValueError(
                f'KIS payload contains a non-object row for {normalized_symbol}'
            )
        raw_date = str(row.get('stck_bsop_date') or '').strip()
        try:
            trade_date = datetime.strptime(raw_date, '%Y%m%d').date()
        except ValueError as exc:
            raise ValueError(
                f'KIS payload has an invalid date for {normalized_symbol}'
            ) from exc
        if trade_date < start_date or trade_date > end_date:
            continue
        if trade_date in bars:
            raise ValueError(
                f'KIS payload has a duplicate date for {normalized_symbol}: '
                f'{trade_date.isoformat()}'
            )
        values = {
            'open_price': _coerce_nonnegative_integer(
                row.get('stck_oprc'), symbol=normalized_symbol, field='open_price'
            ),
            'high_price': _coerce_nonnegative_integer(
                row.get('stck_hgpr'), symbol=normalized_symbol, field='high_price'
            ),
            'low_price': _coerce_nonnegative_integer(
                row.get('stck_lwpr'), symbol=normalized_symbol, field='low_price'
            ),
            'close_price': _coerce_nonnegative_integer(
                row.get('stck_clpr'), symbol=normalized_symbol, field='close_price'
            ),
            'volume': _coerce_nonnegative_integer(
                row.get('acml_vol'), symbol=normalized_symbol, field='volume'
            ),
            'turnover': _coerce_nonnegative_integer(
                row.get('acml_tr_pbmn'), symbol=normalized_symbol, field='turnover'
            ),
        }
        _validate_final_values(symbol=normalized_symbol, **values)
        bars[trade_date] = ProviderFinalBar(
            symbol=normalized_symbol,
            trade_date=trade_date,
            **values,
        )
    if not bars:
        raise ValueError(
            f'KIS payload has no rows inside the requested range for '
            f'{normalized_symbol}'
        )
    return bars


def load_target_page(
    *,
    start_date: date,
    end_date: date,
    after_symbol: str = '',
    symbol_limit: int = DEFAULT_SYMBOL_LIMIT,
    selected_symbols: Sequence[str] | None = None,
) -> TargetPage:
    if not 1 <= symbol_limit <= MAX_SYMBOL_LIMIT:
        raise ValueError(
            f'symbol_limit must be between 1 and {MAX_SYMBOL_LIMIT}'
        )
    normalized_cursor = _normalize_cursor(after_symbol)
    with session_scope() as session:
        if selected_symbols is None:
            symbol_statement = (
                select(Symbol.symbol)
                .join(DailyPriceBar, DailyPriceBar.symbol_id == Symbol.id)
                .where(
                    DailyPriceBar.trade_date >= start_date,
                    DailyPriceBar.trade_date <= end_date,
                    Symbol.market.in_(SUPPORTED_MARKETS),
                    Symbol.symbol > normalized_cursor,
                )
                .distinct()
                .order_by(Symbol.symbol.asc())
                .limit(symbol_limit + 1)
            )
            candidates = [str(item) for item in session.scalars(symbol_statement)]
            has_more = len(candidates) > symbol_limit
            symbols = tuple(candidates[:symbol_limit])
        else:
            symbols = tuple(_normalize_symbol(item) for item in selected_symbols)
            if len(symbols) != len(set(symbols)):
                raise ValueError('selected_symbols must not contain duplicates')
            has_more = False
        if not symbols:
            return TargetPage(symbols=(), rows=(), has_more=has_more)

        row_statement = (
            select(
                DailyPriceBar.id.label('bar_id'),
                Symbol.symbol,
                DailyPriceBar.trade_date,
                DailyPriceBar.open_price,
                DailyPriceBar.high_price,
                DailyPriceBar.low_price,
                DailyPriceBar.close_price,
                DailyPriceBar.volume,
                DailyPriceBar.turnover,
                DailyPriceBar.source,
                DailyPriceBar.captured_at,
                DailyPriceBar.eod_run_id,
                DailyPriceBar.repair_origin_eod_run_id,
                DailyPriceBar.repair_origin_source,
            )
            .join(Symbol, DailyPriceBar.symbol_id == Symbol.id)
            .where(
                Symbol.symbol.in_(symbols),
                Symbol.market.in_(SUPPORTED_MARKETS),
                DailyPriceBar.trade_date >= start_date,
                DailyPriceBar.trade_date <= end_date,
            )
            .order_by(Symbol.symbol.asc(), DailyPriceBar.trade_date.asc())
        )
        rows = tuple(
            StoredFinalBar(
                bar_id=int(row.bar_id),
                symbol=str(row.symbol),
                trade_date=row.trade_date,
                open_price=int(row.open_price),
                high_price=int(row.high_price),
                low_price=int(row.low_price),
                close_price=int(row.close_price),
                volume=int(row.volume),
                turnover=int(row.turnover),
                source=str(row.source),
                captured_at=row.captured_at,
                eod_run_id=(int(row.eod_run_id) if row.eod_run_id is not None else None),
                repair_origin_eod_run_id=(
                    int(row.repair_origin_eod_run_id)
                    if row.repair_origin_eod_run_id is not None
                    else None
                ),
                repair_origin_source=(
                    str(row.repair_origin_source)
                    if row.repair_origin_source is not None
                    else None
                ),
            )
            for row in session.execute(row_statement).all()
        )
        return TargetPage(symbols=symbols, rows=rows, has_more=has_more)


def _required_rebuild_trade_dates(session: Any, *, from_date: date) -> tuple[date, ...]:
    statement = (
        select(DailyPriceBar.trade_date)
        .join(Symbol, DailyPriceBar.symbol_id == Symbol.id)
        .where(
            Symbol.market.in_(SUPPORTED_MARKETS),
            DailyPriceBar.trade_date >= from_date,
        )
        .distinct()
        .order_by(DailyPriceBar.trade_date.asc())
    )
    return tuple(session.scalars(statement).all())


def _required_evaluation_market_date_from(
    session: Any,
    *,
    first_repaired_date: date,
) -> date:
    prior_dates = tuple(
        session.scalars(
            select(DailyPriceBar.trade_date)
            .join(Symbol, DailyPriceBar.symbol_id == Symbol.id)
            .where(
                Symbol.market.in_(SUPPORTED_MARKETS),
                DailyPriceBar.trade_date < first_repaired_date,
            )
            .distinct()
            .order_by(DailyPriceBar.trade_date.desc())
            .limit(MAX_PRICE_DEPENDENT_EVALUATION_HORIZON)
        ).all()
    )
    return min(prior_dates) if prior_dates else first_repaired_date


def load_required_rebuild_trade_dates(*, from_date: date) -> tuple[date, ...]:
    """Return every stored KRX date whose derived values may depend on a repair."""

    with session_scope() as session:
        return _required_rebuild_trade_dates(session, from_date=from_date)


def load_pending_rebuild_batches() -> list[dict[str, Any]]:
    return load_pending_price_bar_rebuild_batches()


def _extension_block_reason(
    stored: StoredFinalBar,
    provider: ProviderFinalBar,
) -> str | None:
    if provider.volume < stored.volume:
        return 'provider_volume_decreased'
    if provider.turnover < stored.turnover:
        return 'provider_turnover_decreased'
    stored_ohlc = stored.values[:4]
    provider_ohlc = provider.values[:4]
    if stored.open_price > 0 and provider.open_price != stored.open_price:
        return 'provider_open_mismatch'
    if stored.open_price == 0 and any(stored.values):
        return 'invalid_partial_open'
    if stored.high_price > 0 and provider.high_price < stored.high_price:
        return 'provider_high_decreased'
    if stored.low_price > 0 and provider.low_price > stored.low_price:
        return 'provider_low_increased'
    if stored.volume > 0 and any(value <= 0 for value in stored_ohlc):
        return 'invalid_stored_ohlc'
    if (
        provider_ohlc != stored_ohlc
        and provider.volume == stored.volume
        and provider.turnover == stored.turnover
    ):
        return 'price_changed_without_liquidity_extension'
    return None


def plan_final_bar_repairs(
    *,
    stored_rows: Sequence[StoredFinalBar],
    provider_bars: Mapping[tuple[str, date], ProviderFinalBar],
) -> dict[str, Any]:
    stored_by_key: dict[tuple[str, date], StoredFinalBar] = {}
    for row in stored_rows:
        key = (row.symbol, row.trade_date)
        if key in stored_by_key:
            raise RuntimeError(f'duplicate stored final-bar key: {key}')
        stored_by_key[key] = row

    missing: list[tuple[str, date]] = []
    unsafe: list[dict[str, str]] = []
    unchanged: list[StoredFinalBar] = []
    updates: list[tuple[StoredFinalBar, ProviderFinalBar]] = []
    for key, stored in stored_by_key.items():
        provider = provider_bars.get(key)
        if provider is None:
            missing.append(key)
            continue
        reason = _extension_block_reason(stored, provider)
        if reason is not None:
            unsafe.append(
                {
                    'symbol': stored.symbol,
                    'trade_date': stored.trade_date.isoformat(),
                    'reason': reason,
                }
            )
            continue
        if stored.values == provider.values:
            unchanged.append(stored)
            continue
        updates.append((stored, provider))

    missing.sort()
    unsafe.sort(key=lambda item: (item['symbol'], item['trade_date']))
    return {
        'target_count': len(stored_by_key),
        'provider_matched_count': len(stored_by_key) - len(missing),
        'missing_provider_row_count': len(missing),
        'missing_provider_rows': [
            {'symbol': symbol, 'trade_date': trade_date.isoformat()}
            for symbol, trade_date in missing
        ],
        'unsafe_row_count': len(unsafe),
        'unsafe_rows': unsafe,
        'unchanged_count': len(unchanged),
        'safe_changed_count': len(updates),
        'safe_changed_symbols': sorted(
            {provider.symbol for _, provider in updates}
        ),
        'safe_changed_trade_dates': sorted(
            {provider.trade_date.isoformat() for _, provider in updates}
        ),
        'updates': updates,
    }


def _rows_fingerprint(
    rows: Sequence[StoredFinalBar],
) -> dict[tuple[str, date], tuple[Any, ...]]:
    return {
        (row.symbol, row.trade_date): (
            row.bar_id,
            row.values,
            row.source,
            row.captured_at,
            row.eod_run_id,
            row.repair_origin_eod_run_id,
            row.repair_origin_source,
        )
        for row in rows
    }


def _repair_root_lineage(row: StoredFinalBar) -> tuple[int | None, str | None]:
    if row.eod_run_id is not None:
        return row.eod_run_id, row.source
    if row.repair_origin_eod_run_id is not None:
        return row.repair_origin_eod_run_id, row.repair_origin_source
    return None, row.repair_origin_source or row.source or None


def repair_existing_final_bars(
    *,
    start_date: date,
    end_date: date,
    selected_symbols: Sequence[str],
    initial_rows: Sequence[StoredFinalBar],
    provider_bars: Mapping[tuple[str, date], ProviderFinalBar],
    apply: bool,
    repair_batch_id: str | None = None,
    required_rebuild_trade_dates: Sequence[str | date] | None = None,
) -> dict[str, Any]:
    normalized_symbols = tuple(_normalize_symbol(item) for item in selected_symbols)
    if not normalized_symbols:
        return {
            'status': 'completed' if apply else 'ready',
            'applied': apply,
            'target_count': 0,
            'provider_matched_count': 0,
            'missing_provider_row_count': 0,
            'missing_provider_rows': [],
            'unsafe_row_count': 0,
            'unsafe_rows': [],
            'unchanged_count': 0,
            'safe_changed_count': 0,
            'updated_count': 0,
            'audit_count': 0,
            'repair_batch_id': None,
        }

    with session_scope() as session:
        statement = (
            select(
                DailyPriceBar.id.label('bar_id'),
                Symbol.symbol,
                DailyPriceBar.trade_date,
                DailyPriceBar.open_price,
                DailyPriceBar.high_price,
                DailyPriceBar.low_price,
                DailyPriceBar.close_price,
                DailyPriceBar.volume,
                DailyPriceBar.turnover,
                DailyPriceBar.source,
                DailyPriceBar.captured_at,
                DailyPriceBar.eod_run_id,
                DailyPriceBar.repair_origin_eod_run_id,
                DailyPriceBar.repair_origin_source,
            )
            .join(Symbol, DailyPriceBar.symbol_id == Symbol.id)
            .where(
                Symbol.symbol.in_(normalized_symbols),
                Symbol.market.in_(SUPPORTED_MARKETS),
                DailyPriceBar.trade_date >= start_date,
                DailyPriceBar.trade_date <= end_date,
            )
            .order_by(Symbol.symbol.asc(), DailyPriceBar.trade_date.asc())
        )
        if apply:
            statement = statement.with_for_update(of=DailyPriceBar)
        current_rows = tuple(
            StoredFinalBar(
                bar_id=int(row.bar_id),
                symbol=str(row.symbol),
                trade_date=row.trade_date,
                open_price=int(row.open_price),
                high_price=int(row.high_price),
                low_price=int(row.low_price),
                close_price=int(row.close_price),
                volume=int(row.volume),
                turnover=int(row.turnover),
                source=str(row.source),
                captured_at=row.captured_at,
                eod_run_id=(int(row.eod_run_id) if row.eod_run_id is not None else None),
                repair_origin_eod_run_id=(
                    int(row.repair_origin_eod_run_id)
                    if row.repair_origin_eod_run_id is not None
                    else None
                ),
                repair_origin_source=(
                    str(row.repair_origin_source)
                    if row.repair_origin_source is not None
                    else None
                ),
            )
            for row in session.execute(statement).all()
        )
        if _rows_fingerprint(current_rows) != _rows_fingerprint(initial_rows):
            return {
                'status': 'blocked',
                'reason': 'stored_rows_changed_during_provider_fetch',
                'applied': False,
                'target_count': len(initial_rows),
                'updated_count': 0,
                'audit_count': 0,
                'repair_batch_id': None,
            }

        plan = plan_final_bar_repairs(
            stored_rows=current_rows,
            provider_bars=provider_bars,
        )
        updates = plan.pop('updates')
        status = (
            'partial'
            if plan['missing_provider_row_count'] or plan['unsafe_row_count']
            else ('completed' if apply else 'ready')
        )
        result = {
            'status': status,
            'reason': (
                'provider_rows_missing_or_not_monotonic'
                if status == 'partial'
                else None
            ),
            'applied': False,
            **plan,
            'updated_count': 0,
            'audit_count': 0,
            'repair_batch_id': None,
        }
        if not apply or not updates:
            result['applied'] = apply
            return result

        normalized_batch_id = str(repair_batch_id or uuid4())
        repaired_at = datetime.now(timezone.utc)
        changed_trade_dates = sorted(
            {provider.trade_date for _, provider in updates}
        )
        required_rebuild_dates = (
            tuple(
                sorted(
                    {
                        _normalize_date(item)
                        for item in required_rebuild_trade_dates
                    }
                )
            )
            if required_rebuild_trade_dates is not None
            else _required_rebuild_trade_dates(
                session,
                from_date=changed_trade_dates[0],
            )
        )
        if not required_rebuild_dates or not set(changed_trade_dates).issubset(
            required_rebuild_dates
        ):
            raise RuntimeError(
                'repair dates are missing from the durable rebuild scope'
            )
        affected_symbols = sorted({provider.symbol for _, provider in updates})
        evaluation_market_date_from = _required_evaluation_market_date_from(
            session,
            first_repaired_date=changed_trade_dates[0],
        )
        evaluation_market_date_to = changed_trade_dates[-1]
        session.execute(
            insert(DailyPriceBarRepairBatch.__table__),
            [
                {
                    'repair_batch_id': normalized_batch_id,
                    'status': 'pending',
                    'affected_symbols': affected_symbols,
                    'affected_trade_dates': [
                        item.isoformat() for item in changed_trade_dates
                    ],
                    'required_rebuild_trade_dates': [
                        item.isoformat() for item in required_rebuild_dates
                    ],
                    'required_evaluation_market_date_from': (
                        evaluation_market_date_from
                    ),
                    'required_evaluation_market_date_to': (
                        evaluation_market_date_to
                    ),
                    'evaluation_rebuilt_at': None,
                    'evaluation_rebuild_metadata': None,
                    'factor_rebuild_metadata': None,
                    'repaired_row_count': len(updates),
                    'created_at': repaired_at,
                    'completed_at': None,
                    'verification_metadata': None,
                }
            ],
        )
        audit_parameters = []
        for stored, provider in updates:
            if stored.captured_at is None or not stored.source:
                raise RuntimeError(
                    'stored row lineage is incomplete; refusing unaudited repair'
                )
            root_eod_run_id, root_source = _repair_root_lineage(stored)
            audit_parameters.append(
                {
                    'daily_price_bar_id': stored.bar_id,
                    'repair_batch_id': normalized_batch_id,
                    'symbol': stored.symbol,
                    'trade_date': stored.trade_date,
                    'provider': 'kis',
                    'provider_market_code': 'J',
                    'provider_adjusted_price': '1',
                    'provider_price_mode': 'original',
                    'old_open_price': stored.open_price,
                    'old_high_price': stored.high_price,
                    'old_low_price': stored.low_price,
                    'old_close_price': stored.close_price,
                    'old_volume': stored.volume,
                    'old_turnover': stored.turnover,
                    'new_open_price': provider.open_price,
                    'new_high_price': provider.high_price,
                    'new_low_price': provider.low_price,
                    'new_close_price': provider.close_price,
                    'new_volume': provider.volume,
                    'new_turnover': provider.turnover,
                    'original_source': stored.source,
                    'original_captured_at': stored.captured_at,
                    'original_eod_run_id': stored.eod_run_id,
                    'root_eod_run_id': root_eod_run_id,
                    'root_source': root_source,
                }
            )
        session.execute(
            insert(DailyPriceBarRepairAudit.__table__),
            audit_parameters,
        )

        parameters = [
            {
                'repair_bar_id': stored.bar_id,
                'old_open': stored.open_price,
                'old_high': stored.high_price,
                'old_low': stored.low_price,
                'old_close': stored.close_price,
                'old_volume': stored.volume,
                'old_turnover': stored.turnover,
                'new_open': provider.open_price,
                'new_high': provider.high_price,
                'new_low': provider.low_price,
                'new_close': provider.close_price,
                'new_volume': provider.volume,
                'new_turnover': provider.turnover,
                'new_source': 'kis_final_bar_repair',
                'new_captured_at': repaired_at,
                'new_repair_origin_eod_run_id': _repair_root_lineage(stored)[0],
                'new_repair_origin_source': _repair_root_lineage(stored)[1],
            }
            for stored, provider in updates
        ]
        table = DailyPriceBar.__table__
        update_statement = (
            update(table)
            .where(
                and_(
                    table.c.id == bindparam('repair_bar_id'),
                    table.c.open_price == bindparam('old_open'),
                    table.c.high_price == bindparam('old_high'),
                    table.c.low_price == bindparam('old_low'),
                    table.c.close_price == bindparam('old_close'),
                    table.c.volume == bindparam('old_volume'),
                    table.c.turnover == bindparam('old_turnover'),
                )
            )
            .values(
                open_price=bindparam('new_open'),
                high_price=bindparam('new_high'),
                low_price=bindparam('new_low'),
                close_price=bindparam('new_close'),
                volume=bindparam('new_volume'),
                turnover=bindparam('new_turnover'),
                source=bindparam('new_source'),
                captured_at=bindparam('new_captured_at'),
                eod_run_id=None,
                repair_origin_eod_run_id=bindparam(
                    'new_repair_origin_eod_run_id'
                ),
                repair_origin_source=bindparam('new_repair_origin_source'),
            )
        )
        update_result = session.execute(update_statement, parameters)
        rowcount = int(update_result.rowcount or 0)
        if rowcount >= 0 and rowcount != len(parameters):
            raise RuntimeError(
                'KIS final-bar update count changed after row locking'
            )

        updated_ids = [stored.bar_id for stored, _ in updates]
        observed = {
            int(row.id): (
                int(row.open_price),
                int(row.high_price),
                int(row.low_price),
                int(row.close_price),
                int(row.volume),
                int(row.turnover),
                str(row.source),
                row.captured_at,
                (int(row.eod_run_id) if row.eod_run_id is not None else None),
                (
                    int(row.repair_origin_eod_run_id)
                    if row.repair_origin_eod_run_id is not None
                    else None
                ),
                (
                    str(row.repair_origin_source)
                    if row.repair_origin_source is not None
                    else None
                ),
            )
            for row in session.execute(
                select(
                    DailyPriceBar.id,
                    DailyPriceBar.open_price,
                    DailyPriceBar.high_price,
                    DailyPriceBar.low_price,
                    DailyPriceBar.close_price,
                    DailyPriceBar.volume,
                    DailyPriceBar.turnover,
                    DailyPriceBar.source,
                    DailyPriceBar.captured_at,
                    DailyPriceBar.eod_run_id,
                    DailyPriceBar.repair_origin_eod_run_id,
                    DailyPriceBar.repair_origin_source,
                ).where(DailyPriceBar.id.in_(updated_ids))
            ).all()
        }
        expected = {
            stored.bar_id: (
                *provider.values,
                'kis_final_bar_repair',
                repaired_at,
                None,
                *_repair_root_lineage(stored),
            )
            for stored, provider in updates
        }
        if observed != expected:
            raise RuntimeError('KIS final-bar post-write verification failed')

        result.update(
            {
                'applied': True,
                'updated_count': len(parameters),
                'audit_count': len(audit_parameters),
                'repair_batch_id': normalized_batch_id,
                'repaired_at': repaired_at.isoformat(),
                'required_rebuild_trade_dates': [
                    item.isoformat() for item in required_rebuild_dates
                ],
                'required_evaluation_market_date_from': (
                    evaluation_market_date_from.isoformat()
                ),
                'required_evaluation_market_date_to': (
                    evaluation_market_date_to.isoformat()
                ),
            }
        )
        return result


async def _fetch_kis_payload(
    symbol: str,
    *,
    start_date: date,
    end_date: date,
) -> object:
    return await kis_real_client.fetch_domestic_daily_chart(
        symbol=symbol,
        market_code='J',
        start_date=start_date.strftime('%Y%m%d'),
        end_date=end_date.strftime('%Y%m%d'),
        adjusted_price='1',
    )


ProviderLoader = Callable[..., Awaitable[object]]
PageLoader = Callable[..., TargetPage]
Repairer = Callable[..., dict[str, Any]]
LockFactory = Callable[..., AbstractContextManager[bool]]
LockSetFactory = Callable[
    ...,
    AbstractContextManager[tuple[bool, str | None]],
]
RebuildDateLoader = Callable[..., Sequence[date]]
PendingBatchLoader = Callable[[], Sequence[Mapping[str, Any]]]


@contextmanager
def _adapt_single_lock_factory(
    lock_factory: LockFactory,
    *,
    market: str,
    trade_dates: Sequence[date],
    source: str,
):
    """Compatibility adapter for injected unit-test lock factories."""

    with ExitStack() as stack:
        for trade_date in sorted(set(trade_dates)):
            acquired = stack.enter_context(
                lock_factory(
                    market=market,
                    trade_date=trade_date.isoformat(),
                    source=source,
                )
            )
            if not acquired:
                yield False, trade_date.isoformat()
                return
        yield True, None


class KISFinalBarRepairService:
    def __init__(
        self,
        *,
        page_loader: PageLoader | None = None,
        provider_loader: ProviderLoader | None = None,
        repairer: Repairer | None = None,
        lock_factory: LockFactory | None = None,
        lock_set_factory: LockSetFactory | None = None,
        rebuild_date_loader: RebuildDateLoader | None = None,
        pending_batch_loader: PendingBatchLoader | None = None,
        now_factory: Callable[[], datetime] | None = None,
        concurrency: int = MAX_CONCURRENCY,
        attempts: int = PROVIDER_ATTEMPTS,
    ) -> None:
        if not 1 <= concurrency <= MAX_CONCURRENCY:
            raise ValueError(f'concurrency must be between 1 and {MAX_CONCURRENCY}')
        if not 1 <= attempts <= PROVIDER_ATTEMPTS:
            raise ValueError(f'attempts must be between 1 and {PROVIDER_ATTEMPTS}')
        self.page_loader = page_loader or load_target_page
        self.provider_loader = provider_loader or _fetch_kis_payload
        self.repairer = repairer or repair_existing_final_bars
        if lock_set_factory is not None:
            self.lock_set_factory = lock_set_factory
        elif lock_factory is not None:
            self.lock_set_factory = lambda **kwargs: _adapt_single_lock_factory(
                lock_factory,
                **kwargs,
            )
        else:
            self.lock_set_factory = advisory_eod_collection_locks
        self.rebuild_date_loader = (
            rebuild_date_loader or load_required_rebuild_trade_dates
        )
        self.pending_batch_loader = (
            pending_batch_loader or load_pending_rebuild_batches
        )
        self.now_factory = now_factory or (lambda: datetime.now(KST))
        self.concurrency = concurrency
        self.attempts = attempts

    def _with_pending_rebuild_state(
        self,
        payload: Mapping[str, Any],
        *,
        current_rebuild_dates: Sequence[date] = (),
    ) -> dict[str, Any]:
        """Merge durable repair obligations into every service response."""

        try:
            pending_batches = [dict(item) for item in self.pending_batch_loader()]
        except Exception as exc:  # noqa: BLE001
            return {
                **payload,
                'status': 'blocked',
                'reason': 'pending_rebuild_state_unavailable',
                'pending_rebuild_error_type': type(exc).__name__,
                'pending_rebuild_error_message': str(exc)[:500],
                'derived_rebuild_required': True,
            }

        rebuild_dates = {
            item.isoformat() for item in current_rebuild_dates
        }
        pending_batch_ids: list[str] = []
        for batch in pending_batches:
            batch_id = str(batch.get('repair_batch_id') or '').strip()
            if batch_id:
                pending_batch_ids.append(batch_id)
            for raw_date in batch.get('required_rebuild_trade_dates') or []:
                rebuild_dates.add(_normalize_date(raw_date).isoformat())

        rebuild_required = bool(
            payload.get('derived_rebuild_required') or pending_batches
        )
        status = str(payload.get('status') or '')
        if rebuild_required and status in {'ready', 'completed'}:
            status = 'rebuild_required'
        return {
            **payload,
            'status': status,
            'derived_rebuild_required': rebuild_required,
            'derived_rebuild_trade_dates': sorted(rebuild_dates),
            'pending_rebuild_batch_count': len(pending_batches),
            'pending_rebuild_batch_ids': pending_batch_ids,
            'pending_rebuild_batches': pending_batches,
        }

    async def _load_symbol(
        self,
        symbol: str,
        *,
        start_date: date,
        end_date: date,
        semaphore: asyncio.Semaphore,
    ) -> tuple[str, dict[date, ProviderFinalBar] | None, list[dict[str, str]]]:
        attempts: list[dict[str, str]] = []
        for attempt in range(1, self.attempts + 1):
            try:
                async with semaphore:
                    payload = await self.provider_loader(
                        symbol,
                        start_date=start_date,
                        end_date=end_date,
                    )
                bars = extract_kis_final_bars(
                    payload,
                    symbol=symbol,
                    start_date=start_date,
                    end_date=end_date,
                )
            except Exception as exc:  # noqa: BLE001
                attempts.append(
                    {
                        'attempt': str(attempt),
                        'error_type': type(exc).__name__,
                        'error_message': str(exc)[:500],
                    }
                )
                if attempt < self.attempts:
                    await asyncio.sleep(0.25 * (2 ** (attempt - 1)))
                continue
            return symbol, bars, attempts
        return symbol, None, attempts

    async def repair_page(
        self,
        *,
        start_date: str | date,
        end_date: str | date,
        after_symbol: str | None = None,
        symbols: Sequence[str] | None = None,
        symbol_limit: int = DEFAULT_SYMBOL_LIMIT,
        apply: bool = False,
    ) -> dict[str, Any]:
        normalized_start, normalized_end = _normalize_bounds(start_date, end_date)
        now = self.now_factory()
        if now.tzinfo is None:
            raise ValueError('now_factory must return a timezone-aware datetime')
        now_kst = now.astimezone(KST)
        if apply:
            validate_finalized_krx_write_date(
                normalized_end,
                now=now_kst,
                operation='KIS final-bar repair',
            )
        normalized_cursor = _normalize_cursor(after_symbol)
        normalized_symbols = (
            tuple(_normalize_symbol(symbol) for symbol in symbols)
            if symbols is not None
            else None
        )
        if normalized_symbols is not None:
            if normalized_cursor:
                raise ValueError('after_symbol and symbols are mutually exclusive')
            if not normalized_symbols:
                raise ValueError('symbols must not be empty')
            if len(normalized_symbols) != len(set(normalized_symbols)):
                raise ValueError('symbols must not contain duplicates')
            if len(normalized_symbols) > MAX_SYMBOL_LIMIT:
                raise ValueError(
                    f'symbols is limited to {MAX_SYMBOL_LIMIT} entries'
                )
        if not 1 <= symbol_limit <= MAX_SYMBOL_LIMIT:
            raise ValueError(
                f'symbol_limit must be between 1 and {MAX_SYMBOL_LIMIT}'
            )
        page = self.page_loader(
            start_date=normalized_start,
            end_date=normalized_end,
            after_symbol=normalized_cursor,
            symbol_limit=symbol_limit,
            selected_symbols=normalized_symbols,
        )
        base = {
            'mode': 'apply' if apply else 'dry_run',
            'provider': 'kis',
            'provider_market_code': 'J',
            'provider_adjusted_price': '1',
            'start_date': normalized_start.isoformat(),
            'end_date': normalized_end.isoformat(),
            'after_symbol': normalized_cursor or None,
            'explicit_symbols': (
                list(normalized_symbols)
                if normalized_symbols is not None
                else None
            ),
            'selected_symbol_count': len(page.symbols),
            'selected_symbols': list(page.symbols),
            'target_row_count': len(page.rows),
            'has_more': page.has_more,
            'next_after_symbol': (
                page.symbols[-1]
                if page.symbols and page.has_more and normalized_symbols is None
                else None
            ),
            'required_follow_up': [
                'rebuild_daily_price_contexts',
                'rebuild_daily_technical_snapshots',
                'rebuild_daily_action_summaries',
                'rebuild_daily_trending_ranks',
                'rebuild_price_dependent_factors_for_affected_and_downstream_dates',
                'recompute_existing_price_dependent_decision_evaluations',
                'verify_and_complete_pending_repair_batches',
            ],
        }
        if not page.symbols:
            return self._with_pending_rebuild_state(
                {
                    **base,
                    'status': 'completed' if apply else 'ready',
                    'applied': apply,
                    'provider_succeeded_symbol_count': 0,
                    'provider_failed_symbol_count': 0,
                    'provider_failed_symbols': [],
                    'retry_required': False,
                    'retry_symbols': [],
                    'derived_rebuild_required': False,
                    'updated_count': 0,
                }
            )

        semaphore = asyncio.Semaphore(self.concurrency)
        loaded = await asyncio.gather(
            *(
                self._load_symbol(
                    symbol,
                    start_date=normalized_start,
                    end_date=normalized_end,
                    semaphore=semaphore,
                )
                for symbol in page.symbols
            )
        )
        provider_bars: dict[tuple[str, date], ProviderFinalBar] = {}
        failed_symbols: list[dict[str, Any]] = []
        for symbol, bars, attempts in loaded:
            if bars is None:
                failed_symbols.append(
                    {'symbol': symbol, 'attempts': attempts}
                )
                continue
            for trade_date, bar in bars.items():
                provider_bars[(symbol, trade_date)] = bar

        if apply:
            target_trade_dates = sorted({row.trade_date for row in page.rows})
            downstream_lock_dates = tuple(
                self.rebuild_date_loader(from_date=min(target_trade_dates))
            )
            if (
                not downstream_lock_dates
                or not set(target_trade_dates).issubset(downstream_lock_dates)
            ):
                raise RuntimeError(
                    'KIS final-bar repair could not prove the complete '
                    'downstream rebuild lock scope'
                )
            repair_lock_dates = sorted(
                {*target_trade_dates, *downstream_lock_dates}
            )
            with self.lock_set_factory(
                market='KRX',
                trade_dates=repair_lock_dates,
                source=LOCK_SOURCE,
            ) as (locks_acquired, blocked_trade_date):
                if not locks_acquired:
                    return self._with_pending_rebuild_state(
                        {
                            **base,
                            'status': 'blocked',
                            'reason': 'advisory_lock_unavailable',
                            'blocked_trade_date': blocked_trade_date,
                            'blocked_lock_source': LOCK_SOURCE,
                            'locked_targets': [],
                            'required_lock_dates': [
                                item.isoformat() for item in repair_lock_dates
                            ],
                            'next_after_symbol': None,
                            'retry_after_symbol': normalized_cursor or None,
                            'applied': False,
                            'provider_succeeded_symbol_count': (
                                len(page.symbols) - len(failed_symbols)
                            ),
                            'provider_failed_symbol_count': len(failed_symbols),
                            'provider_failed_symbols': failed_symbols,
                            'retry_required': True,
                            'retry_symbols': list(page.symbols),
                            'derived_rebuild_required': False,
                            'updated_count': 0,
                        }
                    )
                repair_result = self.repairer(
                    start_date=normalized_start,
                    end_date=normalized_end,
                    selected_symbols=page.symbols,
                    initial_rows=page.rows,
                    provider_bars=provider_bars,
                    apply=True,
                    required_rebuild_trade_dates=repair_lock_dates,
                )
                observed_rebuild_scope = {
                    _normalize_date(item)
                    for item in repair_result.get(
                        'required_rebuild_trade_dates'
                    )
                    or []
                }
                if observed_rebuild_scope and not observed_rebuild_scope.issubset(
                    repair_lock_dates
                ):
                    raise RuntimeError(
                        'KIS final-bar repair rebuild scope expanded beyond '
                        'the acquired downstream date locks; the durable '
                        'pending batch remains fail-closed'
                    )
        else:
            repair_result = self.repairer(
                start_date=normalized_start,
                end_date=normalized_end,
                selected_symbols=page.symbols,
                initial_rows=page.rows,
                provider_bars=provider_bars,
                apply=False,
            )

        status = str(repair_result.get('status') or '')
        if failed_symbols and status in {'ready', 'completed'}:
            status = 'partial'
        retry_symbols = {
            str(item.get('symbol') or '').strip()
            for item in failed_symbols
            if str(item.get('symbol') or '').strip()
        }
        for key in ('missing_provider_rows', 'unsafe_rows'):
            for item in repair_result.get(key) or []:
                if isinstance(item, Mapping):
                    symbol = str(item.get('symbol') or '').strip()
                    if symbol:
                        retry_symbols.add(symbol)
        if status == 'blocked':
            retry_symbols.update(page.symbols)
        updated_count = int(repair_result.get('updated_count') or 0)
        derived_rebuild_required = bool(apply and updated_count > 0)
        changed_trade_dates = [
            _normalize_date(item)
            for item in repair_result.get('safe_changed_trade_dates') or []
        ]
        durable_rebuild_dates = tuple(
            _normalize_date(item)
            for item in repair_result.get('required_rebuild_trade_dates') or []
        )
        required_rebuild_dates = durable_rebuild_dates
        if (
            derived_rebuild_required
            and not required_rebuild_dates
            and changed_trade_dates
        ):
            required_rebuild_dates = tuple(
                self.rebuild_date_loader(from_date=min(changed_trade_dates))
            )
        if derived_rebuild_required and not required_rebuild_dates:
            raise RuntimeError(
                'updated bars have no stored downstream rebuild dates'
            )
        page_is_incomplete = status in {'partial', 'blocked'}
        return self._with_pending_rebuild_state(
            {
                **base,
                **repair_result,
                'status': status,
                'next_after_symbol': (
                    None if page_is_incomplete else base['next_after_symbol']
                ),
                'retry_after_symbol': (
                    (normalized_cursor or None) if page_is_incomplete else None
                ),
                'provider_succeeded_symbol_count': (
                    len(page.symbols) - len(failed_symbols)
                ),
                'provider_failed_symbol_count': len(failed_symbols),
                'provider_failed_symbols': failed_symbols,
                'retry_required': bool(retry_symbols),
                'retry_symbols': sorted(retry_symbols),
                'derived_rebuild_required': derived_rebuild_required,
            },
            current_rebuild_dates=required_rebuild_dates,
        )


kis_final_bar_repair_service = KISFinalBarRepairService()


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            'Finalize existing KRX daily bars from official KIS J history. '
            'Missing rows are never inserted.'
        )
    )
    parser.add_argument('--start-date', required=True)
    parser.add_argument('--end-date', required=True)
    parser.add_argument('--after-symbol')
    parser.add_argument(
        '--symbols',
        help='Comma-separated explicit symbols for a failed/unsafe retry page.',
    )
    parser.add_argument('--symbol-limit', type=int, default=DEFAULT_SYMBOL_LIMIT)
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument('--apply', action='store_true')
    mode.add_argument('--dry-run', action='store_false', dest='apply')
    parser.set_defaults(apply=False)
    return parser


def main(argv: Iterable[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(list(argv) if argv is not None else None)
    try:
        result = asyncio.run(
            kis_final_bar_repair_service.repair_page(
                start_date=args.start_date,
                end_date=args.end_date,
                after_symbol=args.after_symbol,
                symbols=(
                    [item.strip() for item in args.symbols.split(',') if item.strip()]
                    if args.symbols
                    else None
                ),
                symbol_limit=args.symbol_limit,
                apply=args.apply,
            )
        )
    except Exception as exc:  # noqa: BLE001
        result = {
            'status': 'failed',
            'applied': False,
            'error_type': type(exc).__name__,
            'error_message': str(exc),
        }
        print(json.dumps(result, ensure_ascii=False, sort_keys=True), file=sys.stderr)
        return 1
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    if result.get('status') in {'ready', 'completed'}:
        return 0
    if result.get('status') in {'partial', 'blocked', 'rebuild_required'}:
        return 2
    return 1


if __name__ == '__main__':
    raise SystemExit(main())
