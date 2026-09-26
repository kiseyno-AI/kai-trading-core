from __future__ import annotations

import argparse
from collections.abc import Callable, Iterable, Mapping, Sequence
from contextlib import AbstractContextManager
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
import json
import logging
import math
from numbers import Integral, Real
import sys
from typing import Any

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert

from app.db import session_scope
from app.db_models import DailyPriceBar, Symbol
from app.eod.pykrx_backfill import (
    PYKRX_BACKFILL_SERVICE,
    _get_market_ohlcv_by_date,
    _get_market_ohlcv_by_ticker,
)
from app.market.session import KRX_EXCHANGE_CALENDAR
from app.market_data_store import advisory_eod_collection_lock


logger = logging.getLogger(__name__)

SYMBOL_HISTORY_REPAIR_MARKETS = ('KOSPI', 'KOSDAQ')
MINIMUM_MARKET_MEMBERSHIP = {
    'KOSPI': 700,
    'KOSDAQ': 1200,
}
MAX_REPAIR_SYMBOLS = 10
MAX_REPAIR_CALENDAR_DAYS = 450
SYMBOL_RANGE_LOAD_ATTEMPTS = 3
EXACT_MARKET_LOAD_ATTEMPTS = 3
RANGE_OHLCV_COLUMNS = {
    '시가': 'open_price',
    '고가': 'high_price',
    '저가': 'low_price',
    '종가': 'close_price',
    '거래량': 'volume',
}
MARKET_TURNOVER_COLUMN = '거래대금'
MARKET_OHLCV_COLUMNS = {
    **RANGE_OHLCV_COLUMNS,
    MARKET_TURNOVER_COLUMN: 'turnover',
}


@dataclass(frozen=True)
class SymbolHistoryBar:
    symbol: str
    market: str
    trade_date: date
    open_price: int
    high_price: int
    low_price: int
    close_price: int
    volume: int
    turnover: int | None = None


@dataclass(frozen=True)
class _ExactMarketLoadResult:
    status: str
    members: dict[str, dict[str, int | None]]
    skipped_codes: tuple[str, ...]
    attempts: tuple[dict[str, Any], ...]


@dataclass(frozen=True)
class _SymbolRangeLoadResult:
    status: str
    bars: dict[date, SymbolHistoryBar]
    attempts: tuple[dict[str, Any], ...]


def _normalize_date(value: str | date) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    raw = str(value).strip()
    for pattern in ('%Y-%m-%d', '%Y%m%d'):
        try:
            return datetime.strptime(raw, pattern).date()
        except ValueError:
            continue
    raise ValueError('date must use YYYY-MM-DD or YYYYMMDD format')


def _normalize_date_bounds(
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
            f'symbol-history repair is limited to {MAX_REPAIR_CALENDAR_DAYS} '
            'calendar days'
        )
    return normalized_start, normalized_end


def _krx_trading_dates(start_date: date, end_date: date) -> tuple[date, ...]:
    sessions = KRX_EXCHANGE_CALENDAR.sessions_in_range(start_date, end_date)
    return tuple(_normalize_date(session) for session in sessions)


def _normalize_symbol(value: object) -> str:
    symbol = str(value or '').strip()
    if len(symbol) != 6 or not symbol.isdigit():
        raise ValueError(
            f'symbol-history repair requires an explicit 6-digit symbol: '
            f'{value!r}'
        )
    return symbol


def _normalize_symbols(symbols: Sequence[object]) -> tuple[str, ...]:
    normalized = tuple(_normalize_symbol(symbol) for symbol in symbols)
    if not normalized:
        raise ValueError('symbol-history repair requires at least one symbol')
    if len(normalized) > MAX_REPAIR_SYMBOLS:
        raise ValueError(
            f'symbol-history repair is limited to {MAX_REPAIR_SYMBOLS} symbols'
        )
    if len(normalized) != len(set(normalized)):
        raise ValueError('symbol-history repair symbols must not contain duplicates')
    return normalized


def _normalize_provider_symbol(value: object) -> str | None:
    raw = str(value or '').strip()
    normalized = raw.zfill(6)
    if len(normalized) != 6 or not normalized.isdigit():
        return None
    return normalized


def _provider_code_preview(value: object) -> str:
    raw = str(value or '').strip() or repr(value)
    return raw if len(raw) <= 64 else raw[:61] + '...'


def _coerce_nonnegative_integer(
    value: object,
    *,
    symbol: str,
    field: str,
) -> int:
    if value is None or isinstance(value, bool):
        raise ValueError(f'{field} is not an integer for {symbol}')
    if isinstance(value, Integral):
        normalized = int(value)
    elif isinstance(value, Decimal):
        if not value.is_finite() or value != value.to_integral_value():
            raise ValueError(f'{field} is not an integer for {symbol}')
        normalized = int(value)
    elif isinstance(value, Real):
        numeric = float(value)
        if not math.isfinite(numeric) or not numeric.is_integer():
            raise ValueError(f'{field} is not an integer for {symbol}')
        normalized = int(numeric)
    elif isinstance(value, str):
        raw = value.strip()
        if not raw:
            raise ValueError(f'{field} is not an integer for {symbol}')
        try:
            decimal_value = Decimal(raw)
        except InvalidOperation as exc:
            raise ValueError(f'{field} is not an integer for {symbol}') from exc
        if (
            not decimal_value.is_finite()
            or decimal_value != decimal_value.to_integral_value()
        ):
            raise ValueError(f'{field} is not an integer for {symbol}')
        normalized = int(decimal_value)
    else:
        scalar = getattr(value, 'item', None)
        if callable(scalar):
            scalar_value = scalar()
            if scalar_value is not value:
                return _coerce_nonnegative_integer(
                    scalar_value,
                    symbol=symbol,
                    field=field,
                )
        raise ValueError(
            f'{field} has unsupported type {type(value).__name__} for {symbol}'
        )
    if normalized < 0:
        raise ValueError(f'{field} is negative for {symbol}')
    return normalized


def _coerce_optional_turnover(value: object, *, symbol: str) -> int | None:
    if value is None:
        return None
    if isinstance(value, (Decimal, Real)) and not isinstance(value, bool):
        try:
            if math.isnan(float(value)):
                return None
        except (TypeError, ValueError, OverflowError):
            pass
    if isinstance(value, str) and value.strip().lower() in {
        '',
        'nan',
        'none',
        '<na>',
    }:
        return None
    return _coerce_nonnegative_integer(
        value,
        symbol=symbol,
        field='turnover',
    )


def _normalize_ohlcv_fields(
    raw_values: Mapping[str, object],
    *,
    symbol: str,
    context: str,
    require_turnover: bool,
) -> dict[str, int | None]:
    values: dict[str, int | None] = {
        field: _coerce_nonnegative_integer(
            raw_values.get(field),
            symbol=symbol,
            field=field,
        )
        for field in (
            'open_price',
            'high_price',
            'low_price',
            'close_price',
            'volume',
        )
    }
    if require_turnover:
        turnover = _coerce_optional_turnover(
            raw_values.get('turnover'),
            symbol=symbol,
        )
        if turnover is None:
            raise ValueError(f'{context} row has no turnover for {symbol}')
        values['turnover'] = turnover

    volume = int(values['volume'] or 0)
    ohlc = tuple(
        int(values[field] or 0)
        for field in ('open_price', 'high_price', 'low_price', 'close_price')
    )
    if volume > 0:
        if any(value <= 0 for value in ohlc):
            raise ValueError(
                f'{context} positive-volume row has non-positive OHLC for '
                f'{symbol}'
            )
        if require_turnover and int(values['turnover'] or 0) <= 0:
            raise ValueError(
                f'{context} positive-volume row has non-positive turnover '
                f'for {symbol}'
            )
    else:
        if require_turnover and values['turnover'] != 0:
            raise ValueError(
                f'{context} zero-volume row has non-zero turnover for {symbol}'
            )
        if any(ohlc) and any(value <= 0 for value in ohlc):
            raise ValueError(
                f'{context} zero-volume row has incomplete halted OHLC for '
                f'{symbol}'
            )

    if any(ohlc) and (
        ohlc[1] < max(ohlc) or ohlc[2] > min(ohlc)
    ):
        raise ValueError(f'{context} row has invalid OHLC geometry for {symbol}')
    return values


def _iter_frame_rows(frame: object, *, payload_name: str):
    if frame is None or bool(getattr(frame, 'empty', True)):
        raise ValueError(f'{payload_name} payload is empty')
    iterrows = getattr(frame, 'iterrows', None)
    if not callable(iterrows):
        raise ValueError(f'{payload_name} payload rows are not iterable')
    return iterrows()


def _extract_symbol_range(
    frame: object,
    *,
    symbol: str,
    start_date: date,
    end_date: date,
) -> dict[date, SymbolHistoryBar]:
    columns = set(getattr(frame, 'columns', ()))
    missing_columns = sorted(set(RANGE_OHLCV_COLUMNS).difference(columns))
    if missing_columns:
        raise ValueError(
            f'{symbol} range payload is missing columns: '
            f'{",".join(missing_columns)}'
        )

    bars: dict[date, SymbolHistoryBar] = {}
    for raw_date, row in _iter_frame_rows(
        frame,
        payload_name=f'{symbol} range OHLCV',
    ):
        trade_date = _normalize_date(raw_date)
        if trade_date < start_date or trade_date > end_date:
            raise ValueError(
                f'{symbol} range payload contains out-of-bounds date '
                f'{trade_date.isoformat()}'
            )
        if trade_date in bars:
            raise ValueError(
                f'{symbol} range payload contains duplicate date '
                f'{trade_date.isoformat()}'
            )
        values = _normalize_ohlcv_fields(
            {
                target: row[source]
                for source, target in RANGE_OHLCV_COLUMNS.items()
            },
            symbol=symbol,
            context='symbol range',
            require_turnover=False,
        )
        bars[trade_date] = SymbolHistoryBar(
            symbol=symbol,
            market='',
            trade_date=trade_date,
            **values,
        )

    if start_date not in bars or end_date not in bars:
        raise ValueError(
            f'{symbol} range payload does not cover both exact date bounds'
        )
    return bars


def _extract_market_membership(
    frame: object,
    *,
    market: str,
) -> tuple[dict[str, dict[str, int | None]], list[str]]:
    columns = set(getattr(frame, 'columns', ()))
    missing_columns = sorted(set(MARKET_OHLCV_COLUMNS).difference(columns))
    if missing_columns:
        raise ValueError(
            f'{market} market payload is missing columns: '
            + ','.join(missing_columns)
        )

    members: dict[str, dict[str, int | None]] = {}
    skipped_codes: list[str] = []
    for raw_symbol, row in _iter_frame_rows(
        frame,
        payload_name=f'{market} market OHLCV',
    ):
        symbol = _normalize_provider_symbol(raw_symbol)
        if symbol is None:
            skipped_codes.append(_provider_code_preview(raw_symbol))
            continue
        if symbol in members:
            raise ValueError(
                f'{market} market payload contains duplicate symbol {symbol}'
            )
        values = _normalize_ohlcv_fields(
            {
                target: row[source]
                for source, target in MARKET_OHLCV_COLUMNS.items()
            },
            symbol=symbol,
            context=f'{market} exact market',
            require_turnover=True,
        )
        members[symbol] = values
    if not members:
        raise ValueError(f'{market} market payload has no valid stock symbols')
    return members, skipped_codes


def repair_missing_symbol_history(
    *,
    symbols: Sequence[str],
    start_date: str | date,
    end_date: str | date,
    bars: Sequence[SymbolHistoryBar],
    apply: bool = False,
    source: str = 'pykrx',
) -> dict[str, Any]:
    """Insert only absent rows for a validated, bounded symbol/date scope."""

    normalized_symbols = _normalize_symbols(symbols)
    normalized_source = str(source or '').strip().lower()
    if normalized_source not in {'pykrx', 'kis'}:
        raise ValueError('symbol-history repair source must be pykrx or kis')
    normalized_start, normalized_end = _normalize_date_bounds(
        start_date,
        end_date,
    )
    normalized_bars = list(bars)
    if not normalized_bars:
        raise ValueError('symbol-history repair requires validated provider bars')

    target_symbol_set = set(normalized_symbols)
    bar_by_key: dict[tuple[str, date], SymbolHistoryBar] = {}
    dates_by_symbol: dict[str, set[date]] = {
        symbol: set() for symbol in normalized_symbols
    }
    for bar in normalized_bars:
        if bar.symbol not in target_symbol_set:
            raise ValueError(
                f'provider bar is outside requested symbols: {bar.symbol}'
            )
        if bar.market not in SYMBOL_HISTORY_REPAIR_MARKETS:
            raise ValueError(f'provider bar has unsupported market: {bar.market}')
        if not normalized_start <= bar.trade_date <= normalized_end:
            raise ValueError('provider bar is outside requested date bounds')
        values = _normalize_ohlcv_fields(
            {
                'open_price': bar.open_price,
                'high_price': bar.high_price,
                'low_price': bar.low_price,
                'close_price': bar.close_price,
                'volume': bar.volume,
                'turnover': bar.turnover,
            },
            symbol=bar.symbol,
            context='database repair',
            require_turnover=True,
        )
        normalized_bar = SymbolHistoryBar(
            symbol=bar.symbol,
            market=bar.market,
            trade_date=bar.trade_date,
            open_price=int(values['open_price'] or 0),
            high_price=int(values['high_price'] or 0),
            low_price=int(values['low_price'] or 0),
            close_price=int(values['close_price'] or 0),
            volume=int(values['volume'] or 0),
            turnover=int(values['turnover'] or 0),
        )
        key = (bar.symbol, bar.trade_date)
        if key in bar_by_key:
            raise ValueError('provider bars contain duplicate symbol/date rows')
        bar_by_key[key] = normalized_bar
        dates_by_symbol[bar.symbol].add(bar.trade_date)

    expected_dates: set[date] | None = None
    for symbol in normalized_symbols:
        symbol_dates = dates_by_symbol[symbol]
        if not symbol_dates:
            raise ValueError(f'provider bars contain no rows for {symbol}')
        if expected_dates is None:
            expected_dates = symbol_dates
        elif symbol_dates != expected_dates:
            raise ValueError('provider bars have incomplete cross-symbol coverage')
    assert expected_dates is not None
    if normalized_start not in expected_dates or normalized_end not in expected_dates:
        raise ValueError('provider bars do not cover both exact date bounds')

    expected_keys = set(bar_by_key)
    with session_scope() as session:
        symbol_rows = session.execute(
            select(Symbol.id, Symbol.symbol).where(
                Symbol.symbol.in_(normalized_symbols)
            )
        ).all()
        symbol_id_by_code = {
            str(row.symbol): int(row.id)
            for row in symbol_rows
        }
        missing_symbol_records = sorted(
            target_symbol_set.difference(symbol_id_by_code)
        )
        if missing_symbol_records:
            return {
                'status': 'blocked',
                'reason': 'requested_symbol_missing_from_symbols_table',
                'applied': False,
                'start_date': normalized_start.isoformat(),
                'end_date': normalized_end.isoformat(),
                'requested_symbols': list(normalized_symbols),
                'source': normalized_source,
                'missing_symbol_records': missing_symbol_records,
                'validated_bar_count': len(normalized_bars),
                'missing_bar_count': 0,
                'would_insert_count': 0,
                'inserted_count': 0,
            }

        stored_before = {
            (str(row.symbol), row.trade_date)
            for row in session.execute(
                select(Symbol.symbol, DailyPriceBar.trade_date)
                .join(DailyPriceBar, DailyPriceBar.symbol_id == Symbol.id)
                .where(
                    Symbol.symbol.in_(normalized_symbols),
                    DailyPriceBar.trade_date >= normalized_start,
                    DailyPriceBar.trade_date <= normalized_end,
                )
            ).all()
        }
        existing_expected = expected_keys.intersection(stored_before)
        missing_bars = [
            bar
            for key, bar in bar_by_key.items()
            if key not in stored_before
        ]
        result: dict[str, Any] = {
            'status': 'ready',
            'reason': None,
            'applied': False,
            'start_date': normalized_start.isoformat(),
            'end_date': normalized_end.isoformat(),
            'requested_symbols': list(normalized_symbols),
            'source': normalized_source,
            'trade_date_count': len(expected_dates),
            'validated_bar_count': len(normalized_bars),
            'existing_bar_count': len(existing_expected),
            'missing_bar_count': len(missing_bars),
            'would_insert_count': len(missing_bars),
            'inserted_count': 0,
        }
        if not apply:
            return result

        inserted_count = 0
        if missing_bars:
            values = [
                {
                    'symbol_id': symbol_id_by_code[bar.symbol],
                    'eod_run_id': None,
                    'trade_date': bar.trade_date,
                    'open_price': bar.open_price,
                    'high_price': bar.high_price,
                    'low_price': bar.low_price,
                    'close_price': bar.close_price,
                    'volume': bar.volume,
                    'turnover': bar.turnover,
                    'per': None,
                    'pbr': None,
                    'bps': None,
                    'eps': None,
                    'dividend_yield': None,
                    'dps': None,
                    'source': normalized_source,
                }
                for bar in missing_bars
            ]
            statement = (
                pg_insert(DailyPriceBar)
                .values(values)
                .on_conflict_do_nothing(
                    constraint='uq_daily_price_bars_symbol_trade_date'
                )
                .returning(DailyPriceBar.id)
            )
            inserted_count = len(session.execute(statement).scalars().all())
            if inserted_count != len(missing_bars):
                raise RuntimeError(
                    'symbol-history insert count changed after coverage '
                    'validation'
                )

        stored_after = {
            (str(row.symbol), row.trade_date)
            for row in session.execute(
                select(Symbol.symbol, DailyPriceBar.trade_date)
                .join(DailyPriceBar, DailyPriceBar.symbol_id == Symbol.id)
                .where(
                    Symbol.symbol.in_(normalized_symbols),
                    DailyPriceBar.trade_date >= normalized_start,
                    DailyPriceBar.trade_date <= normalized_end,
                )
            ).all()
        }
        if not expected_keys.issubset(stored_after):
            raise RuntimeError(
                'symbol-history coverage verification failed after insert'
            )

        result.update(
            {
                'status': 'completed',
                'applied': True,
                'inserted_count': inserted_count,
                'stored_after_count': len(expected_keys.intersection(stored_after)),
            }
        )
        return result


class SymbolHistoryRepairService:
    def __init__(
        self,
        *,
        trade_date_resolver: Callable[..., str] | None = None,
        trading_dates_resolver: (
            Callable[[date, date], Sequence[date]] | None
        ) = None,
        symbol_range_loader: Callable[[str, str, str], object] | None = None,
        market_ohlcv_loader: Callable[[str, str], object] | None = None,
        database_repairer: Callable[..., dict[str, Any]] | None = None,
        lock_factory: Callable[..., AbstractContextManager[bool]] | None = None,
        minimum_market_membership: Mapping[str, int] | None = None,
    ) -> None:
        self.trade_date_resolver = (
            trade_date_resolver
            or PYKRX_BACKFILL_SERVICE.resolve_latest_trade_date
        )
        self.trading_dates_resolver = (
            trading_dates_resolver or _krx_trading_dates
        )
        self.symbol_range_loader = (
            symbol_range_loader or _get_market_ohlcv_by_date
        )
        self.market_ohlcv_loader = (
            market_ohlcv_loader or _get_market_ohlcv_by_ticker
        )
        self.database_repairer = (
            database_repairer or repair_missing_symbol_history
        )
        self.lock_factory = lock_factory or advisory_eod_collection_lock
        self.minimum_market_membership = dict(
            minimum_market_membership or MINIMUM_MARKET_MEMBERSHIP
        )
        if set(self.minimum_market_membership) != set(
            SYMBOL_HISTORY_REPAIR_MARKETS
        ) or any(
            isinstance(value, bool) or not isinstance(value, int) or value <= 0
            for value in self.minimum_market_membership.values()
        ):
            raise ValueError(
                'minimum membership must define positive integer floors for '
                'KOSPI and KOSDAQ'
            )

    @staticmethod
    def _blocked_result(
        *,
        symbols: Sequence[str],
        start_date: date,
        end_date: date,
        apply: bool,
        reason: str,
        **details: Any,
    ) -> dict[str, Any]:
        return {
            'status': 'blocked',
            'reason': reason,
            'applied': False,
            'mode': 'apply' if apply else 'dry_run',
            'requested_symbols': list(symbols),
            'start_date': start_date.isoformat(),
            'end_date': end_date.isoformat(),
            **details,
        }

    def _load_exact_market_membership(
        self,
        *,
        compact_date: str,
        market: str,
    ) -> _ExactMarketLoadResult:
        attempts: list[dict[str, Any]] = []
        for attempt in range(1, EXACT_MARKET_LOAD_ATTEMPTS + 1):
            try:
                frame = self.market_ohlcv_loader(compact_date, market)
            except Exception as exc:  # noqa: BLE001
                attempts.append(
                    {
                        'attempt': attempt,
                        'outcome': 'loader_error',
                        'error_type': type(exc).__name__,
                        'error_message': str(exc)[:500],
                    }
                )
                continue

            if frame is None or bool(getattr(frame, 'empty', True)):
                attempts.append(
                    {
                        'attempt': attempt,
                        'outcome': 'empty_payload',
                    }
                )
                continue

            try:
                members, skipped_codes = _extract_market_membership(
                    frame,
                    market=market,
                )
            except Exception as exc:  # noqa: BLE001
                attempts.append(
                    {
                        'attempt': attempt,
                        'outcome': 'invalid_payload',
                        'error_type': type(exc).__name__,
                        'error_message': str(exc)[:500],
                    }
                )
                continue

            attempts.append({'attempt': attempt, 'outcome': 'success'})
            return _ExactMarketLoadResult(
                status='success',
                members=members,
                skipped_codes=tuple(skipped_codes),
                attempts=tuple(attempts),
            )

        status = (
            'empty'
            if attempts
            and all(item['outcome'] == 'empty_payload' for item in attempts)
            else 'invalid'
        )
        return _ExactMarketLoadResult(
            status=status,
            members={},
            skipped_codes=(),
            attempts=tuple(attempts),
        )

    def _load_symbol_range(
        self,
        *,
        compact_start: str,
        compact_end: str,
        symbol: str,
        start_date: date,
        end_date: date,
    ) -> _SymbolRangeLoadResult:
        attempts: list[dict[str, Any]] = []
        for attempt in range(1, SYMBOL_RANGE_LOAD_ATTEMPTS + 1):
            try:
                frame = self.symbol_range_loader(
                    compact_start,
                    compact_end,
                    symbol,
                )
            except Exception as exc:  # noqa: BLE001
                attempts.append(
                    {
                        'attempt': attempt,
                        'outcome': 'loader_error',
                        'error_type': type(exc).__name__,
                        'error_message': str(exc)[:500],
                    }
                )
                continue

            if frame is None or bool(getattr(frame, 'empty', True)):
                attempts.append(
                    {
                        'attempt': attempt,
                        'outcome': 'empty_payload',
                    }
                )
                continue

            try:
                bars = _extract_symbol_range(
                    frame,
                    symbol=symbol,
                    start_date=start_date,
                    end_date=end_date,
                )
            except Exception as exc:  # noqa: BLE001
                attempts.append(
                    {
                        'attempt': attempt,
                        'outcome': 'invalid_payload',
                        'error_type': type(exc).__name__,
                        'error_message': str(exc)[:500],
                    }
                )
                continue

            attempts.append({'attempt': attempt, 'outcome': 'success'})
            return _SymbolRangeLoadResult(
                status='success',
                bars=bars,
                attempts=tuple(attempts),
            )

        return _SymbolRangeLoadResult(
            status='invalid',
            bars={},
            attempts=tuple(attempts),
        )

    def repair_range(
        self,
        *,
        symbols: Sequence[str],
        start_date: str | date,
        end_date: str | date,
        apply: bool = False,
    ) -> dict[str, Any]:
        normalized_symbols = _normalize_symbols(symbols)
        normalized_start, normalized_end = _normalize_date_bounds(
            start_date,
            end_date,
        )
        compact_start = normalized_start.strftime('%Y%m%d')
        compact_end = normalized_end.strftime('%Y%m%d')

        with self.lock_factory(
            market='KRX',
            trade_date=normalized_end.isoformat(),
            source='pykrx',
        ) as lock_acquired:
            if not lock_acquired:
                return self._blocked_result(
                    symbols=normalized_symbols,
                    start_date=normalized_start,
                    end_date=normalized_end,
                    apply=apply,
                    reason='advisory_lock_unavailable',
                )

            try:
                resolved_start = _normalize_date(
                    self.trade_date_resolver(end_date=compact_start)
                )
                resolved_end = _normalize_date(
                    self.trade_date_resolver(end_date=compact_end)
                )
            except Exception as exc:  # noqa: BLE001
                return self._blocked_result(
                    symbols=normalized_symbols,
                    start_date=normalized_start,
                    end_date=normalized_end,
                    apply=apply,
                    reason='trade_date_resolution_failed',
                    error_type=type(exc).__name__,
                    error_message=str(exc),
                )
            if (
                resolved_start != normalized_start
                or resolved_end != normalized_end
            ):
                return self._blocked_result(
                    symbols=normalized_symbols,
                    start_date=normalized_start,
                    end_date=normalized_end,
                    apply=apply,
                    reason='date_bound_is_not_exact_trading_session',
                    resolved_start_date=resolved_start.isoformat(),
                    resolved_end_date=resolved_end.isoformat(),
                )

            try:
                calendar_candidate_dates = tuple(
                    sorted(
                        {
                            _normalize_date(item)
                            for item in self.trading_dates_resolver(
                                normalized_start,
                                normalized_end,
                            )
                        }
                    )
                )
            except Exception as exc:  # noqa: BLE001
                return self._blocked_result(
                    symbols=normalized_symbols,
                    start_date=normalized_start,
                    end_date=normalized_end,
                    apply=apply,
                    reason='trading_calendar_resolution_failed',
                    error_type=type(exc).__name__,
                    error_message=str(exc),
                )

            range_bars_by_symbol: dict[
                str,
                dict[date, SymbolHistoryBar],
            ] = {}
            range_date_union: set[date] = set()
            symbol_range_retry_count = 0
            symbol_range_retry_preview: list[dict[str, Any]] = []
            for symbol in normalized_symbols:
                load_result = self._load_symbol_range(
                    compact_start=compact_start,
                    compact_end=compact_end,
                    symbol=symbol,
                    start_date=normalized_start,
                    end_date=normalized_end,
                )
                retry_count = max(0, len(load_result.attempts) - 1)
                symbol_range_retry_count += retry_count
                if retry_count and len(symbol_range_retry_preview) < 20:
                    symbol_range_retry_preview.append(
                        {
                            'symbol': symbol,
                            'attempts': list(load_result.attempts),
                        }
                    )
                if load_result.status != 'success':
                    return self._blocked_result(
                        symbols=normalized_symbols,
                        start_date=normalized_start,
                        end_date=normalized_end,
                        apply=apply,
                        reason='invalid_symbol_range_payload',
                        failed_symbol=symbol,
                        load_attempt_count=len(load_result.attempts),
                        load_attempts=list(load_result.attempts),
                    )
                symbol_bars = load_result.bars
                range_bars_by_symbol[symbol] = symbol_bars
                range_date_union.update(symbol_bars)

            candidate_dates = tuple(
                sorted(set(calendar_candidate_dates).union(range_date_union))
            )

            symbol_markets: dict[str, str] = {}
            validated_bars: list[SymbolHistoryBar] = []
            skipped_codes: list[dict[str, str]] = []
            exact_market_retry_count = 0
            exact_market_retry_preview: list[dict[str, Any]] = []
            membership_min: dict[str, int | None] = {
                market: None for market in SYMBOL_HISTORY_REPAIR_MARKETS
            }
            membership_max: dict[str, int] = {
                market: 0 for market in SYMBOL_HISTORY_REPAIR_MARKETS
            }
            actual_trade_dates: list[date] = []
            provider_closed_calendar_dates: list[date] = []
            range_exact_ohlcv_mismatches: list[dict[str, Any]] = []

            for trade_date in candidate_dates:
                compact_date = trade_date.strftime('%Y%m%d')
                members_by_market: dict[
                    str,
                    dict[str, dict[str, int | None]],
                ] = {}
                all_members: set[str] = set()
                empty_markets: list[str] = []
                load_results: dict[str, _ExactMarketLoadResult] = {}
                for market in SYMBOL_HISTORY_REPAIR_MARKETS:
                    load_result = self._load_exact_market_membership(
                        compact_date=compact_date,
                        market=market,
                    )
                    load_results[market] = load_result
                    retry_count = max(0, len(load_result.attempts) - 1)
                    exact_market_retry_count += retry_count
                    if retry_count and len(exact_market_retry_preview) < 20:
                        exact_market_retry_preview.append(
                            {
                                'date': trade_date.isoformat(),
                                'market': market,
                                'attempts': list(load_result.attempts),
                            }
                        )
                    if load_result.status == 'invalid':
                        return self._blocked_result(
                            symbols=normalized_symbols,
                            start_date=normalized_start,
                            end_date=normalized_end,
                            apply=apply,
                            reason='invalid_exact_market_payload',
                            failed_market=market,
                            failed_date=trade_date.isoformat(),
                            load_attempt_count=len(load_result.attempts),
                            load_attempts=list(load_result.attempts),
                        )
                    if load_result.status == 'empty':
                        empty_markets.append(market)

                if empty_markets:
                    if (
                        len(empty_markets) == len(SYMBOL_HISTORY_REPAIR_MARKETS)
                        and trade_date not in range_date_union
                    ):
                        provider_closed_calendar_dates.append(trade_date)
                        continue
                    return self._blocked_result(
                        symbols=normalized_symbols,
                        start_date=normalized_start,
                        end_date=normalized_end,
                        apply=apply,
                        reason='empty_or_partial_exact_market_payload',
                        failed_date=trade_date.isoformat(),
                        empty_markets=empty_markets,
                        load_attempts_by_market={
                            market: list(load_results[market].attempts)
                            for market in SYMBOL_HISTORY_REPAIR_MARKETS
                        },
                    )

                for market in SYMBOL_HISTORY_REPAIR_MARKETS:
                    members = load_results[market].members
                    market_skipped_codes = load_results[market].skipped_codes
                    member_count = len(members)
                    membership_min[market] = (
                        member_count
                        if membership_min[market] is None
                        else min(membership_min[market], member_count)
                    )
                    membership_max[market] = max(
                        membership_max[market],
                        member_count,
                    )
                    minimum_count = self.minimum_market_membership[market]
                    if member_count < minimum_count:
                        return self._blocked_result(
                            symbols=normalized_symbols,
                            start_date=normalized_start,
                            end_date=normalized_end,
                            apply=apply,
                            reason='market_membership_below_safety_floor',
                            failed_market=market,
                            failed_date=trade_date.isoformat(),
                            observed_symbol_count=member_count,
                            minimum_symbol_count=minimum_count,
                        )
                    duplicate_members = sorted(
                        all_members.intersection(members)
                    )
                    if duplicate_members:
                        return self._blocked_result(
                            symbols=normalized_symbols,
                            start_date=normalized_start,
                            end_date=normalized_end,
                            apply=apply,
                            reason='duplicate_cross_market_membership',
                            failed_date=trade_date.isoformat(),
                            duplicate_symbols=duplicate_members[:20],
                        )
                    all_members.update(members)
                    members_by_market[market] = members
                    skipped_codes.extend(
                        {
                            'date': trade_date.isoformat(),
                            'market': market,
                            'code': code,
                        }
                        for code in market_skipped_codes
                    )

                has_positive_market_turnover = any(
                    member['turnover'] is not None
                    and member['turnover'] > 0
                    for members in members_by_market.values()
                    for member in members.values()
                )
                if not has_positive_market_turnover and trade_date in range_date_union:
                    return self._blocked_result(
                        symbols=normalized_symbols,
                        start_date=normalized_start,
                        end_date=normalized_end,
                        apply=apply,
                        reason='range_date_has_no_market_turnover',
                        failed_date=trade_date.isoformat(),
                    )
                if not has_positive_market_turnover:
                    provider_closed_calendar_dates.append(trade_date)
                    continue

                actual_trade_dates.append(trade_date)

                for symbol in normalized_symbols:
                    containing_markets = [
                        market
                        for market, members in members_by_market.items()
                        if symbol in members
                    ]
                    if len(containing_markets) != 1:
                        return self._blocked_result(
                            symbols=normalized_symbols,
                            start_date=normalized_start,
                            end_date=normalized_end,
                            apply=apply,
                            reason='target_absent_or_ambiguous_in_exact_membership',
                            failed_symbol=symbol,
                            failed_date=trade_date.isoformat(),
                            containing_markets=containing_markets,
                        )
                    market = containing_markets[0]
                    prior_market = symbol_markets.setdefault(symbol, market)
                    if prior_market != market:
                        return self._blocked_result(
                            symbols=normalized_symbols,
                            start_date=normalized_start,
                            end_date=normalized_end,
                            apply=apply,
                            reason='target_market_changed_across_range',
                            failed_symbol=symbol,
                            failed_date=trade_date.isoformat(),
                            prior_market=prior_market,
                            current_market=market,
                        )
                    exact_market_bar = members_by_market[market][symbol]
                    turnover = exact_market_bar['turnover']
                    if turnover is None:
                        return self._blocked_result(
                            symbols=normalized_symbols,
                            start_date=normalized_start,
                            end_date=normalized_end,
                            apply=apply,
                            reason='target_turnover_missing',
                            failed_symbol=symbol,
                            failed_date=trade_date.isoformat(),
                        )
                    bar = range_bars_by_symbol[symbol].get(trade_date)
                    if bar is None:
                        return self._blocked_result(
                            symbols=normalized_symbols,
                            start_date=normalized_start,
                            end_date=normalized_end,
                            apply=apply,
                            reason='incomplete_cross_symbol_date_coverage',
                            failed_symbol=symbol,
                            failed_date=trade_date.isoformat(),
                        )
                    exact_ohlcv = (
                        int(exact_market_bar['open_price'] or 0),
                        int(exact_market_bar['high_price'] or 0),
                        int(exact_market_bar['low_price'] or 0),
                        int(exact_market_bar['close_price'] or 0),
                        int(exact_market_bar['volume'] or 0),
                    )
                    range_ohlcv = (
                        bar.open_price,
                        bar.high_price,
                        bar.low_price,
                        bar.close_price,
                        bar.volume,
                    )
                    if exact_ohlcv != range_ohlcv:
                        range_exact_ohlcv_mismatches.append(
                            {
                                'symbol': symbol,
                                'trade_date': trade_date.isoformat(),
                                'range_ohlcv': range_ohlcv,
                                'exact_market_ohlcv': exact_ohlcv,
                            }
                        )
                    validated_bars.append(
                        SymbolHistoryBar(
                            symbol=symbol,
                            market=market,
                            trade_date=trade_date,
                            open_price=exact_ohlcv[0],
                            high_price=exact_ohlcv[1],
                            low_price=exact_ohlcv[2],
                            close_price=exact_ohlcv[3],
                            volume=exact_ohlcv[4],
                            turnover=turnover,
                        )
                    )
            try:
                database_result = self.database_repairer(
                    symbols=normalized_symbols,
                    start_date=normalized_start,
                    end_date=normalized_end,
                    bars=validated_bars,
                    apply=apply,
                    source='pykrx',
                )
            except Exception as exc:  # noqa: BLE001
                logger.exception(
                    'Missing-only symbol history repair failed for %s through %s',
                    normalized_start.isoformat(),
                    normalized_end.isoformat(),
                )
                return {
                    **self._blocked_result(
                        symbols=normalized_symbols,
                        start_date=normalized_start,
                        end_date=normalized_end,
                        apply=apply,
                        reason='database_repair_failed',
                        error_type=type(exc).__name__,
                        error_message=str(exc),
                    ),
                    'status': 'failed',
                }

            return {
                **database_result,
                'mode': 'apply' if apply else 'dry_run',
                'membership_basis': 'exact_market_date_ohlcv',
                'symbol_markets': symbol_markets,
                'provider_trade_date_count': len(actual_trade_dates),
                'calendar_candidate_date_count': len(
                    calendar_candidate_dates
                ),
                'provider_closed_calendar_date_count': len(
                    provider_closed_calendar_dates
                ),
                'provider_closed_calendar_dates': [
                    item.isoformat()
                    for item in provider_closed_calendar_dates[:20]
                ],
                'provider_skipped_code_count': len(skipped_codes),
                'provider_skipped_code_preview': skipped_codes[:20],
                'symbol_range_retry_count': symbol_range_retry_count,
                'symbol_range_retry_preview': symbol_range_retry_preview,
                'exact_market_retry_count': exact_market_retry_count,
                'exact_market_retry_preview': exact_market_retry_preview,
                'range_exact_ohlcv_mismatch_count': len(
                    range_exact_ohlcv_mismatches
                ),
                'range_exact_ohlcv_mismatch_preview': (
                    range_exact_ohlcv_mismatches[:20]
                ),
                'market_membership_count_range': {
                    market: {
                        'min': membership_min[market],
                        'max': membership_max[market],
                    }
                    for market in SYMBOL_HISTORY_REPAIR_MARKETS
                },
            }


symbol_history_repair_service = SymbolHistoryRepairService()


def _parse_symbols(raw: str) -> tuple[str, ...]:
    return tuple(item.strip() for item in raw.split(',') if item.strip())


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            'Validate a bounded symbol history range against exact-date '
            'KOSPI/KOSDAQ membership and insert only missing price bars.'
        )
    )
    parser.add_argument(
        '--symbols',
        required=True,
        help='Explicit comma-separated 6-digit KRX symbols.',
    )
    parser.add_argument('--start-date', required=True)
    parser.add_argument('--end-date', required=True)
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument(
        '--apply',
        action='store_true',
        help='Insert validated missing rows only.',
    )
    mode.add_argument(
        '--dry-run',
        action='store_false',
        dest='apply',
        help='Validate and report without writing (default).',
    )
    parser.set_defaults(apply=False)
    return parser


def main(argv: Iterable[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(list(argv) if argv is not None else None)
    try:
        result = symbol_history_repair_service.repair_range(
            symbols=_parse_symbols(args.symbols),
            start_date=args.start_date,
            end_date=args.end_date,
            apply=args.apply,
        )
    except Exception as exc:  # noqa: BLE001
        result = {
            'status': 'failed',
            'applied': False,
            'error_type': type(exc).__name__,
            'error_message': str(exc),
        }
        print(
            json.dumps(result, ensure_ascii=False, sort_keys=True),
            file=sys.stderr,
        )
        return 1

    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    if result.get('status') in {'ready', 'completed'}:
        return 0
    if result.get('status') == 'blocked':
        return 2
    return 1


if __name__ == '__main__':
    raise SystemExit(main())
