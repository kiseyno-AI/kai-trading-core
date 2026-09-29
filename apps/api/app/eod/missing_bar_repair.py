from __future__ import annotations

import argparse
from collections.abc import Callable, Iterable, Mapping, Sequence
from contextlib import AbstractContextManager
from dataclasses import dataclass, replace
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
from app.eod.market_membership import market_membership_sha256
from app.eod.finality import validate_finalized_krx_write_date
from app.eod.master import load_all_domestic_masters, _validate_master_universe
from app.eod.pykrx_backfill import (
    PYKRX_BACKFILL_SERVICE,
    _call_pykrx,
    _get_market_ohlcv_by_ticker,
    stock,
)
from app.market_data_store import advisory_eod_collection_lock


logger = logging.getLogger(__name__)

MISSING_BAR_REPAIR_MARKETS = ('KOSPI', 'KOSDAQ')
MISSING_BAR_INSERT_BATCH_SIZE = 200
MINIMUM_MARKET_MEMBERSHIP = {
    'KOSPI': 700,
    'KOSDAQ': 1200,
}
REQUIRED_OHLCV_COLUMNS = {
    '시가': 'open_price',
    '고가': 'high_price',
    '저가': 'low_price',
    '종가': 'close_price',
    '거래량': 'volume',
    '거래대금': 'turnover',
}
FUNDAMENTAL_COLUMNS = {
    'PER': 'per',
    'PBR': 'pbr',
    'BPS': 'bps',
    'EPS': 'eps',
    'DIV': 'dividend_yield',
    'DPS': 'dps',
}
FUNDAMENTAL_OPTIONAL_SECURITY_GROUPS = frozenset({'RT', 'IF', 'MF', 'DR', 'FS'})


def _missing_bar_insert_batches(
    values: Sequence[dict[str, Any]],
) -> Iterable[Sequence[dict[str, Any]]]:
    """Bound INSERT statement size while retaining one outer transaction."""

    for offset in range(0, len(values), MISSING_BAR_INSERT_BATCH_SIZE):
        yield values[offset : offset + MISSING_BAR_INSERT_BATCH_SIZE]


@dataclass(frozen=True)
class MissingDailyBar:
    symbol: str
    market: str
    open_price: int
    high_price: int
    low_price: int
    close_price: int
    volume: int
    turnover: int
    per: Decimal | None = None
    pbr: Decimal | None = None
    bps: Decimal | None = None
    eps: Decimal | None = None
    dividend_yield: Decimal | None = None
    dps: Decimal | None = None


def _normalize_market_date(value: str | date) -> date:
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
    raise ValueError('market date must use YYYY-MM-DD or YYYYMMDD format')


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


def _coerce_optional_decimal(value: object, *, symbol: str, field: str) -> Decimal | None:
    if value is None:
        return None
    if isinstance(value, bool):
        raise ValueError(f'{field} is not numeric for {symbol}')
    if isinstance(value, Decimal):
        normalized = value
    elif isinstance(value, (Integral, Real, str)):
        raw = str(value).strip()
        if not raw or raw.lower() in {'nan', 'none', '<na>'}:
            return None
        try:
            normalized = Decimal(raw)
        except InvalidOperation as exc:
            raise ValueError(f'{field} is not numeric for {symbol}') from exc
    else:
        scalar = getattr(value, 'item', None)
        if callable(scalar):
            scalar_value = scalar()
            if scalar_value is not value:
                return _coerce_optional_decimal(
                    scalar_value,
                    symbol=symbol,
                    field=field,
                )
        raise ValueError(
            f'{field} has unsupported type {type(value).__name__} for {symbol}'
        )
    if not normalized.is_finite():
        return None
    return normalized


def _iter_frame_rows(frame: object, *, payload_name: str):
    if frame is None or bool(getattr(frame, 'empty', True)):
        raise ValueError(f'{payload_name} payload is empty')
    iterrows = getattr(frame, 'iterrows', None)
    if not callable(iterrows):
        raise ValueError(f'{payload_name} payload rows are not iterable')
    return iterrows()


def _extract_market_ohlcv(
    frame: object,
    *,
    market: str,
) -> tuple[list[MissingDailyBar], list[str]]:
    columns = set(getattr(frame, 'columns', ()))
    missing_columns = sorted(set(REQUIRED_OHLCV_COLUMNS).difference(columns))
    if missing_columns:
        raise ValueError(
            f'{market} OHLCV payload is missing columns: {",".join(missing_columns)}'
        )

    bars: list[MissingDailyBar] = []
    skipped_codes: list[str] = []
    seen: set[str] = set()
    for raw_symbol, row in _iter_frame_rows(
        frame,
        payload_name=f'{market} OHLCV',
    ):
        symbol = _normalize_provider_symbol(raw_symbol)
        if symbol is None:
            skipped_codes.append(_provider_code_preview(raw_symbol))
            continue
        if symbol in seen:
            raise ValueError(f'{market} OHLCV payload has duplicate symbol {symbol}')
        seen.add(symbol)
        values = {
            target: _coerce_nonnegative_integer(
                row[source],
                symbol=symbol,
                field=target,
            )
            for source, target in REQUIRED_OHLCV_COLUMNS.items()
        }
        if values['volume'] > 0:
            if any(
                values[field] <= 0
                for field in (
                    'open_price',
                    'high_price',
                    'low_price',
                    'close_price',
                )
            ):
                raise ValueError(
                    f'positive-volume row has non-positive OHLC for {symbol}'
                )
            if values['high_price'] < max(
                values['open_price'],
                values['low_price'],
                values['close_price'],
            ):
                raise ValueError(f'invalid OHLC high price for {symbol}')
            if values['low_price'] > min(
                values['open_price'],
                values['high_price'],
                values['close_price'],
            ):
                raise ValueError(f'invalid OHLC low price for {symbol}')
            if values['turnover'] <= 0:
                raise ValueError(
                    f'positive-volume row has non-positive turnover for {symbol}'
                )
        elif values['turnover'] != 0:
            raise ValueError(
                f'zero-volume row has non-zero turnover for {symbol}'
            )
        elif any(
            values[field] != 0
            for field in ('open_price', 'high_price', 'low_price')
        ):
            if values['high_price'] < max(
                values['open_price'],
                values['low_price'],
                values['close_price'],
            ):
                raise ValueError(f'invalid OHLC high price for {symbol}')
            if values['low_price'] > min(
                values['open_price'],
                values['high_price'],
                values['close_price'],
            ):
                raise ValueError(f'invalid OHLC low price for {symbol}')
        bars.append(MissingDailyBar(symbol=symbol, market=market, **values))

    if not bars:
        raise ValueError(f'{market} OHLCV payload has no valid stock symbols')
    return bars, skipped_codes


def _extract_market_fundamentals(
    frame: object,
    *,
    market: str,
) -> tuple[dict[str, dict[str, Decimal | None]], list[str]]:
    columns = set(getattr(frame, 'columns', ()))
    missing_columns = sorted(set(FUNDAMENTAL_COLUMNS).difference(columns))
    if missing_columns:
        raise ValueError(
            f'{market} fundamental payload is missing columns: '
            + ','.join(missing_columns)
        )

    values_by_symbol: dict[str, dict[str, Decimal | None]] = {}
    skipped_codes: list[str] = []
    for raw_symbol, row in _iter_frame_rows(
        frame,
        payload_name=f'{market} fundamental',
    ):
        symbol = _normalize_provider_symbol(raw_symbol)
        if symbol is None:
            skipped_codes.append(_provider_code_preview(raw_symbol))
            continue
        if symbol in values_by_symbol:
            raise ValueError(
                f'{market} fundamental payload has duplicate symbol {symbol}'
            )
        values_by_symbol[symbol] = {
            FUNDAMENTAL_COLUMNS[column]: _coerce_optional_decimal(
                row[column],
                symbol=symbol,
                field=FUNDAMENTAL_COLUMNS[column],
            )
            for column in FUNDAMENTAL_COLUMNS
        }
    return values_by_symbol, skipped_codes


def _get_market_fundamental_by_ticker(trade_date: str, market: str):
    getter = getattr(stock, 'get_market_fundamental_by_ticker', None)
    if not callable(getter):
        raise RuntimeError('PyKRX market-wide fundamental API is unavailable')
    return _call_pykrx(getter, trade_date, market)


def _load_current_master_security_groups() -> dict[str, str]:
    symbols = load_all_domestic_masters()
    _validate_master_universe(symbols)
    return {item.symbol: item.security_group for item in symbols}


def repair_missing_daily_price_bars(
    *,
    market_date: str | date,
    bars: Sequence[MissingDailyBar],
    apply: bool = False,
) -> dict[str, Any]:
    """Insert only absent exact-date bars in one transaction.

    Existing ``daily_price_bars`` rows are never included in an UPDATE or an
    upsert update clause.  A concurrent conflict fails the count verification
    and rolls this transaction back instead of silently changing the repair
    scope after validation.
    """

    normalized_date = _normalize_market_date(market_date)
    normalized_bars = list(bars)
    if not normalized_bars:
        raise ValueError('missing-bar repair requires provider membership')
    expected_symbols = [bar.symbol for bar in normalized_bars]
    if any(_normalize_provider_symbol(symbol) != symbol for symbol in expected_symbols):
        raise ValueError('missing-bar repair contains an invalid stock symbol')
    if len(expected_symbols) != len(set(expected_symbols)):
        raise ValueError('missing-bar repair membership contains duplicate symbols')

    expected_set = set(expected_symbols)
    with session_scope() as session:
        symbol_rows = session.execute(
            select(Symbol.id, Symbol.symbol, Symbol.market).where(
                Symbol.symbol.in_(expected_symbols)
            )
        ).all()
        symbol_id_by_code = {
            str(row.symbol): int(row.id)
            for row in symbol_rows
        }
        missing_symbol_records = sorted(
            expected_set.difference(symbol_id_by_code)
        )
        if missing_symbol_records:
            return {
                'market_date': normalized_date.isoformat(),
                'status': 'blocked',
                'reason': 'provider_membership_missing_from_symbols_table',
                'applied': False,
                'expected_symbol_count': len(expected_symbols),
                'symbols_table_match_count': len(symbol_id_by_code),
                'missing_symbol_record_count': len(missing_symbol_records),
                'missing_symbol_records': missing_symbol_records[:20],
                'missing_bar_count': 0,
                'would_insert_count': 0,
                'inserted_count': 0,
            }

        provider_market_by_code = {
            bar.symbol: bar.market for bar in normalized_bars
        }
        current_market_mismatches = sorted(
            str(row.symbol)
            for row in symbol_rows
            if str(row.market or '').upper()
            != provider_market_by_code[str(row.symbol)]
        )

        stored_before = set(
            session.execute(
                select(Symbol.symbol)
                .join(DailyPriceBar, DailyPriceBar.symbol_id == Symbol.id)
                .where(
                    DailyPriceBar.trade_date == normalized_date,
                    Symbol.market.in_(MISSING_BAR_REPAIR_MARKETS),
                )
            ).scalars()
        )
        stored_outside_provider_membership = sorted(
            stored_before.difference(expected_set)
        )
        if stored_outside_provider_membership:
            return {
                'market_date': normalized_date.isoformat(),
                'status': 'blocked',
                'reason': 'stored_rows_outside_provider_membership',
                'applied': False,
                'expected_symbol_count': len(expected_symbols),
                'symbols_table_match_count': len(symbol_id_by_code),
                'stored_before_count': len(stored_before),
                'stored_outside_provider_membership_count': len(
                    stored_outside_provider_membership
                ),
                'stored_outside_provider_membership_symbols': (
                    stored_outside_provider_membership[:20]
                ),
                'current_market_mismatch_count': len(current_market_mismatches),
                'current_market_mismatch_symbols': current_market_mismatches[:20],
                'missing_bar_count': 0,
                'would_insert_count': 0,
                'inserted_count': 0,
            }
        missing_bars = [
            bar for bar in normalized_bars if bar.symbol not in stored_before
        ]
        result: dict[str, Any] = {
            'market_date': normalized_date.isoformat(),
            'status': 'ready',
            'reason': None,
            'applied': False,
            'expected_symbol_count': len(expected_symbols),
            'symbols_table_match_count': len(symbol_id_by_code),
            'stored_before_count': len(stored_before),
            'stored_outside_provider_membership_count': 0,
            'stored_outside_provider_membership_symbols': [],
            'current_market_mismatch_count': len(current_market_mismatches),
            'current_market_mismatch_symbols': current_market_mismatches[:20],
            'missing_bar_count': len(missing_bars),
            'missing_bar_symbols': sorted(bar.symbol for bar in missing_bars)[:20],
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
                    'trade_date': normalized_date,
                    'open_price': bar.open_price,
                    'high_price': bar.high_price,
                    'low_price': bar.low_price,
                    'close_price': bar.close_price,
                    'volume': bar.volume,
                    'turnover': bar.turnover,
                    'per': bar.per,
                    'pbr': bar.pbr,
                    'bps': bar.bps,
                    'eps': bar.eps,
                    'dividend_yield': bar.dividend_yield,
                    'dps': bar.dps,
                    'source': 'pykrx',
                }
                for bar in missing_bars
            ]
            for value_batch in _missing_bar_insert_batches(values):
                statement = (
                    pg_insert(DailyPriceBar)
                    .values(value_batch)
                    .on_conflict_do_nothing(
                        constraint='uq_daily_price_bars_symbol_trade_date'
                    )
                    .returning(DailyPriceBar.symbol_id)
                )
                inserted_count += len(
                    session.execute(statement).scalars().all()
                )
            if inserted_count != len(missing_bars):
                raise RuntimeError(
                    'missing-bar insert count changed after coverage validation'
                )

        stored_after = set(
            session.execute(
                select(Symbol.symbol)
                .join(DailyPriceBar, DailyPriceBar.symbol_id == Symbol.id)
                .where(
                    DailyPriceBar.trade_date == normalized_date,
                    Symbol.market.in_(MISSING_BAR_REPAIR_MARKETS),
                )
            ).scalars()
        )
        if stored_after != expected_set:
            raise RuntimeError(
                'missing-bar coverage verification failed after insert'
            )

        result.update(
            {
                'status': 'completed',
                'applied': True,
                'inserted_count': inserted_count,
                'stored_after_count': len(stored_after),
            }
        )
        return result


class MissingDailyBarRepairService:
    def __init__(
        self,
        *,
        trade_date_resolver: Callable[..., str] | None = None,
        market_ohlcv_loader: Callable[[str, str], object] | None = None,
        market_fundamental_loader: Callable[[str, str], object] | None = None,
        database_repairer: Callable[..., dict[str, Any]] | None = None,
        security_group_loader: Callable[[], Mapping[str, str]] | None = None,
        lock_factory: Callable[..., AbstractContextManager[bool]] | None = None,
        minimum_market_membership: Mapping[str, int] | None = None,
    ) -> None:
        self.trade_date_resolver = (
            trade_date_resolver
            or PYKRX_BACKFILL_SERVICE.resolve_latest_trade_date
        )
        self.market_ohlcv_loader = (
            market_ohlcv_loader or _get_market_ohlcv_by_ticker
        )
        self.market_fundamental_loader = (
            market_fundamental_loader or _get_market_fundamental_by_ticker
        )
        self.database_repairer = (
            database_repairer or repair_missing_daily_price_bars
        )
        self.security_group_loader = (
            security_group_loader or _load_current_master_security_groups
        )
        self.lock_factory = lock_factory or advisory_eod_collection_lock
        self.minimum_market_membership = dict(
            minimum_market_membership or MINIMUM_MARKET_MEMBERSHIP
        )
        if set(self.minimum_market_membership) != set(
            MISSING_BAR_REPAIR_MARKETS
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
        market_date: date,
        apply: bool,
        reason: str,
        **details: Any,
    ) -> dict[str, Any]:
        return {
            'market_date': market_date.isoformat(),
            'markets': list(MISSING_BAR_REPAIR_MARKETS),
            'mode': 'apply' if apply else 'dry_run',
            'status': 'blocked',
            'reason': reason,
            'applied': False,
            **details,
        }

    def repair_for_market_date(
        self,
        *,
        market_date: str | date,
        apply: bool = False,
    ) -> dict[str, Any]:
        normalized_date = _normalize_market_date(market_date)
        iso_date = normalized_date.isoformat()
        compact_date = normalized_date.strftime('%Y%m%d')
        if apply:
            validate_finalized_krx_write_date(
                normalized_date,
                operation='missing KRX bar repair',
            )

        with self.lock_factory(
            market='KRX',
            trade_date=iso_date,
            source='daily_price_bars',
        ) as lock_acquired:
            if not lock_acquired:
                return self._blocked_result(
                    market_date=normalized_date,
                    apply=apply,
                    reason='advisory_lock_unavailable',
                )

            try:
                resolved_date = _normalize_market_date(
                    self.trade_date_resolver(end_date=compact_date)
                )
            except Exception as exc:  # noqa: BLE001
                return self._blocked_result(
                    market_date=normalized_date,
                    apply=apply,
                    reason='trade_date_resolution_failed',
                    error_type=type(exc).__name__,
                    error_message=str(exc),
                )
            if resolved_date != normalized_date:
                return self._blocked_result(
                    market_date=normalized_date,
                    apply=apply,
                    reason='not_an_exact_trading_session',
                    resolved_market_date=resolved_date.isoformat(),
                )

            all_bars: list[MissingDailyBar] = []
            seen_symbols: set[str] = set()
            market_symbol_counts: dict[str, int] = {}
            skipped_ohlcv_codes: list[dict[str, str]] = []
            skipped_fundamental_codes: list[dict[str, str]] = []
            fundamental_missing_symbols: list[str] = []
            required_fundamental_missing_symbols: list[str] = []
            fundamental_extra_symbols: list[str] = []

            for market in MISSING_BAR_REPAIR_MARKETS:
                try:
                    ohlcv_frame = self.market_ohlcv_loader(compact_date, market)
                    market_bars, market_skipped_ohlcv = _extract_market_ohlcv(
                        ohlcv_frame,
                        market=market,
                    )
                except Exception as exc:  # noqa: BLE001
                    logger.warning(
                        'PyKRX missing-bar OHLCV validation failed for %s %s: %s',
                        market,
                        iso_date,
                        exc,
                    )
                    return self._blocked_result(
                        market_date=normalized_date,
                        apply=apply,
                        reason='invalid_ohlcv_payload',
                        failed_market=market,
                        error_type=type(exc).__name__,
                        error_message=str(exc),
                    )
                market_symbol_counts[market] = len(market_bars)
                skipped_ohlcv_codes.extend(
                    {'market': market, 'code': code}
                    for code in market_skipped_ohlcv
                )
                floor = self.minimum_market_membership[market]
                if len(market_bars) < floor:
                    return self._blocked_result(
                        market_date=normalized_date,
                        apply=apply,
                        reason='market_membership_below_safety_floor',
                        failed_market=market,
                        observed_symbol_count=len(market_bars),
                        minimum_symbol_count=floor,
                        market_symbol_counts=market_symbol_counts,
                        provider_skipped_ohlcv_code_count=len(
                            skipped_ohlcv_codes
                        ),
                        provider_skipped_ohlcv_code_preview=(
                            skipped_ohlcv_codes[:20]
                        ),
                    )

                market_symbols = {bar.symbol for bar in market_bars}
                duplicate_members = sorted(seen_symbols.intersection(market_symbols))
                if duplicate_members:
                    return self._blocked_result(
                        market_date=normalized_date,
                        apply=apply,
                        reason='duplicate_cross_market_membership',
                        duplicate_symbol_count=len(duplicate_members),
                        duplicate_symbols=duplicate_members[:20],
                    )
                seen_symbols.update(market_symbols)

                try:
                    fundamentals_frame = self.market_fundamental_loader(
                        compact_date,
                        market,
                    )
                    fundamentals, market_skipped_fundamentals = (
                        _extract_market_fundamentals(
                            fundamentals_frame,
                            market=market,
                        )
                    )
                except Exception as exc:  # noqa: BLE001
                    logger.warning(
                        'PyKRX missing-bar fundamental validation failed for %s %s: %s',
                        market,
                        iso_date,
                        exc,
                    )
                    return self._blocked_result(
                        market_date=normalized_date,
                        apply=apply,
                        reason='invalid_fundamental_payload',
                        failed_market=market,
                        error_type=type(exc).__name__,
                        error_message=str(exc),
                        market_symbol_counts=market_symbol_counts,
                    )
                skipped_fundamental_codes.extend(
                    {'market': market, 'code': code}
                    for code in market_skipped_fundamentals
                )
                fundamental_symbols = set(fundamentals)
                market_missing_fundamentals = sorted(
                    market_symbols.difference(fundamental_symbols)
                )
                fundamental_missing_symbols.extend(market_missing_fundamentals)
                fundamental_extra_symbols.extend(
                    sorted(fundamental_symbols.difference(market_symbols))
                )
                for bar in market_bars:
                    values = fundamentals.get(bar.symbol, {})
                    all_bars.append(
                        replace(
                            bar,
                            **{
                                field: values.get(field)
                                for field in FUNDAMENTAL_COLUMNS.values()
                            },
                        )
                    )

            membership_sha256 = market_membership_sha256(
                [bar.symbol for bar in all_bars]
            )
            if fundamental_missing_symbols:
                try:
                    security_group_by_symbol = {
                        str(symbol): str(group).upper()
                        for symbol, group in self.security_group_loader().items()
                    }
                except Exception as exc:  # noqa: BLE001
                    return self._blocked_result(
                        market_date=normalized_date,
                        apply=apply,
                        reason='master_security_group_validation_failed',
                        error_type=type(exc).__name__,
                        error_message=str(exc),
                        membership_basis='exact_market_date_ohlcv',
                        membership_sha256=membership_sha256,
                    )
                required_fundamental_missing_symbols.extend(
                    symbol
                    for symbol in fundamental_missing_symbols
                    if security_group_by_symbol.get(symbol)
                    not in FUNDAMENTAL_OPTIONAL_SECURITY_GROUPS
                )
            if required_fundamental_missing_symbols:
                return self._blocked_result(
                    market_date=normalized_date,
                    apply=apply,
                    reason='required_fundamental_rows_missing',
                    membership_basis='exact_market_date_ohlcv',
                    membership_sha256=membership_sha256,
                    market_symbol_counts=market_symbol_counts,
                    provider_missing_fundamental_count=len(
                        fundamental_missing_symbols
                    ),
                    provider_missing_fundamental_symbols=sorted(
                        fundamental_missing_symbols
                    )[:20],
                    required_missing_fundamental_count=len(
                        required_fundamental_missing_symbols
                    ),
                    required_missing_fundamental_symbols=sorted(
                        required_fundamental_missing_symbols
                    )[:20],
                )
            try:
                database_result = self.database_repairer(
                    market_date=normalized_date,
                    bars=all_bars,
                    apply=apply,
                )
            except Exception as exc:  # noqa: BLE001
                logger.exception(
                    'Missing-only daily bar repair failed for %s',
                    iso_date,
                )
                return {
                    **self._blocked_result(
                        market_date=normalized_date,
                        apply=apply,
                        reason='database_repair_failed',
                        error_type=type(exc).__name__,
                        error_message=str(exc),
                    ),
                    'status': 'failed',
                    'membership_sha256': membership_sha256,
                }

            return {
                **database_result,
                'markets': list(MISSING_BAR_REPAIR_MARKETS),
                'mode': 'apply' if apply else 'dry_run',
                'membership_basis': 'exact_market_date_ohlcv',
                'membership_sha256': membership_sha256,
                'market_symbol_counts': market_symbol_counts,
                'provider_skipped_ohlcv_code_count': len(skipped_ohlcv_codes),
                'provider_skipped_ohlcv_code_preview': skipped_ohlcv_codes[:20],
                'provider_skipped_fundamental_code_count': len(
                    skipped_fundamental_codes
                ),
                'provider_skipped_fundamental_code_preview': (
                    skipped_fundamental_codes[:20]
                ),
                'provider_missing_fundamental_count': len(
                    fundamental_missing_symbols
                ),
                'provider_missing_fundamental_symbols': (
                    sorted(fundamental_missing_symbols)[:20]
                ),
                'required_missing_fundamental_count': 0,
                'required_missing_fundamental_symbols': [],
                'provider_extra_fundamental_count': len(
                    fundamental_extra_symbols
                ),
                'provider_extra_fundamental_symbols': (
                    sorted(fundamental_extra_symbols)[:20]
                ),
            }


missing_daily_bar_repair_service = MissingDailyBarRepairService()


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            'Validate an exact KOSPI+KOSDAQ session and insert only missing '
            'daily price bars. Existing rows are never updated.'
        )
    )
    parser.add_argument(
        '--market-date',
        required=True,
        help='Exactly one KRX market date in YYYY-MM-DD or YYYYMMDD format.',
    )
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
        result = missing_daily_bar_repair_service.repair_for_market_date(
            market_date=args.market_date,
            apply=args.apply,
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
    if result.get('status') == 'blocked':
        return 2
    return 1


if __name__ == '__main__':
    raise SystemExit(main())
