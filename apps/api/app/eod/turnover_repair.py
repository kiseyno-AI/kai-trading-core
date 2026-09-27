from __future__ import annotations

import argparse
from collections.abc import Callable, Iterable, Mapping, Sequence
from contextlib import AbstractContextManager
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
import json
import logging
import math
from numbers import Integral, Real
import sys
from typing import Any

from app.eod.market_membership import market_membership_sha256
from app.eod.finality import validate_finalized_krx_write_date
from app.eod.pykrx_backfill import (
    PYKRX_BACKFILL_SERVICE,
    _get_market_ohlcv_by_ticker,
)
from app.market_data_store import (
    advisory_eod_collection_lock,
    repair_daily_price_bar_turnovers,
)


logger = logging.getLogger(__name__)
DEFAULT_TURNOVER_REPAIR_MARKETS = ('KOSPI', 'KOSDAQ')
MINIMUM_EXACT_MARKET_MEMBERSHIP = {
    'KOSPI': 700,
    'KOSDAQ': 1200,
}
SUPPORTED_TURNOVER_REPAIR_MARKETS = frozenset(
    MINIMUM_EXACT_MARKET_MEMBERSHIP
)
PYKRX_TURNOVER_COLUMN = '거래대금'
PYKRX_VOLUME_COLUMN = '거래량'
PYKRX_OHLC_COLUMNS = ('시가', '고가', '저가', '종가')


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


def _normalize_markets(markets: Sequence[str]) -> tuple[str, ...]:
    normalized = tuple(str(market).strip().upper() for market in markets)
    if not normalized or any(not market for market in normalized):
        raise ValueError('turnover repair requires at least one explicit market')
    if len(normalized) != len(set(normalized)):
        raise ValueError('turnover repair markets must not contain duplicates')
    unsupported = sorted(set(normalized).difference(SUPPORTED_TURNOVER_REPAIR_MARKETS))
    if unsupported:
        raise ValueError(
            'unsupported turnover repair market(s): ' + ','.join(unsupported)
        )
    return normalized


def _normalize_provider_stock_code(value: object) -> str | None:
    raw_code = str(value or '').strip().upper()
    normalized = raw_code.zfill(6)
    if not normalized.isdigit() or len(normalized) != 6:
        return None
    return normalized


def _provider_code_preview(value: object) -> str:
    raw_code = str(value or '').strip().upper() or repr(value)
    if len(raw_code) <= 64:
        return raw_code
    return raw_code[:61] + '...'


def _provider_skipped_code_summary(
    entries: Sequence[dict[str, str]],
    *,
    counts_by_market: Mapping[str, int],
) -> dict[str, Any]:
    return {
        'provider_skipped_code_count': len(entries),
        'provider_skipped_code_preview': list(entries[:20]),
        'provider_skipped_code_counts_by_market': dict(counts_by_market),
    }


def _coerce_provider_integer(
    value: object,
    *,
    symbol: str,
    field: str,
) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool):
        raise ValueError(f'boolean {field} is invalid for {symbol}')

    if isinstance(value, Integral):
        turnover = int(value)
    elif isinstance(value, Decimal):
        if value.is_nan():
            return None
        if not value.is_finite() or value != value.to_integral_value():
            raise ValueError(f'non-integral {field} is invalid for {symbol}')
        turnover = int(value)
    elif isinstance(value, Real):
        numeric = float(value)
        if math.isnan(numeric):
            return None
        if not math.isfinite(numeric) or not numeric.is_integer():
            raise ValueError(f'non-integral {field} is invalid for {symbol}')
        turnover = int(numeric)
    elif isinstance(value, str):
        raw = value.strip()
        if not raw or raw.lower() in {'nan', 'none', '<na>'}:
            return None
        try:
            decimal_value = Decimal(raw)
        except InvalidOperation as exc:
            raise ValueError(f'non-numeric {field} is invalid for {symbol}') from exc
        if (
            not decimal_value.is_finite()
            or decimal_value != decimal_value.to_integral_value()
        ):
            raise ValueError(f'non-integral {field} is invalid for {symbol}')
        turnover = int(decimal_value)
    else:
        scalar = getattr(value, 'item', None)
        if callable(scalar):
            normalized_scalar = scalar()
            if normalized_scalar is not value:
                return _coerce_provider_integer(
                    normalized_scalar,
                    symbol=symbol,
                    field=field,
                )
        raise ValueError(
            f'unsupported {field} value {type(value).__name__} for {symbol}'
        )

    if turnover < 0:
        raise ValueError(f'negative {field} is invalid for {symbol}')
    return turnover


def _extract_market_turnovers(
    frame: object,
    *,
    market: str,
) -> tuple[
    list[str],
    dict[str, int],
    dict[str, int],
    dict[str, tuple[int, int, int, int]],
    list[str],
    list[str],
]:
    if frame is None or bool(getattr(frame, 'empty', True)):
        raise ValueError(f'{market} market payload is empty')
    columns = getattr(frame, 'columns', ())
    missing_columns = {
        PYKRX_TURNOVER_COLUMN,
        PYKRX_VOLUME_COLUMN,
        *PYKRX_OHLC_COLUMNS,
    }.difference(columns)
    if missing_columns:
        raise ValueError(
            f'{market} market payload is missing '
            + ','.join(sorted(missing_columns))
        )
    turnover_series = frame[PYKRX_TURNOVER_COLUMN]
    turnover_items = getattr(turnover_series, 'items', None)
    volume_series = frame[PYKRX_VOLUME_COLUMN]
    volume_items = getattr(volume_series, 'items', None)
    if not callable(turnover_items) or not callable(volume_items):
        raise ValueError(f'{market} turnover column is not iterable')
    raw_volume_items = list(volume_items())
    raw_volume_by_symbol = dict(raw_volume_items)
    if len(raw_volume_by_symbol) != len(raw_volume_items):
        raise ValueError(f'{market} volume payload contains duplicate symbols')
    raw_ohlc_by_column: dict[str, dict[object, object]] = {}
    for column in PYKRX_OHLC_COLUMNS:
        items = getattr(frame[column], 'items', None)
        if not callable(items):
            raise ValueError(f'{market} {column} column is not iterable')
        raw_items = list(items())
        values_by_symbol = dict(raw_items)
        if len(values_by_symbol) != len(raw_items):
            raise ValueError(f'{market} {column} payload contains duplicate symbols')
        raw_ohlc_by_column[column] = values_by_symbol

    symbols: list[str] = []
    turnover_by_symbol: dict[str, int] = {}
    volume_by_symbol: dict[str, int] = {}
    ohlc_by_symbol: dict[str, tuple[int, int, int, int]] = {}
    missing_turnover_symbols: list[str] = []
    skipped_provider_codes: list[str] = []
    seen: set[str] = set()
    for raw_symbol, raw_turnover in turnover_items():
        symbol = _normalize_provider_stock_code(raw_symbol)
        if symbol is None:
            skipped_provider_codes.append(_provider_code_preview(raw_symbol))
            continue
        if symbol in seen:
            raise ValueError(
                f'{market} market payload contains duplicate symbol: {symbol}'
            )
        seen.add(symbol)
        symbols.append(symbol)
        if raw_symbol not in raw_volume_by_symbol:
            raise ValueError(f'{market} volume payload is missing {symbol}')
        turnover = _coerce_provider_integer(
            raw_turnover,
            symbol=symbol,
            field='turnover',
        )
        volume = _coerce_provider_integer(
            raw_volume_by_symbol[raw_symbol],
            symbol=symbol,
            field='volume',
        )
        if volume is None:
            raise ValueError(f'{market} volume is missing for {symbol}')
        raw_ohlc_values: list[int] = []
        for column in PYKRX_OHLC_COLUMNS:
            raw_values = raw_ohlc_by_column[column]
            if raw_symbol not in raw_values:
                raise ValueError(f'{market} {column} payload is missing {symbol}')
            value = _coerce_provider_integer(
                raw_values[raw_symbol],
                symbol=symbol,
                field=column,
            )
            if value is None:
                raise ValueError(f'{market} {column} is missing for {symbol}')
            raw_ohlc_values.append(value)
        ohlc = tuple(raw_ohlc_values)
        if volume > 0:
            if any(value <= 0 for value in ohlc):
                raise ValueError(
                    f'{market} positive-volume row has non-positive OHLC for {symbol}'
                )
            if ohlc[1] < max(ohlc) or ohlc[2] > min(ohlc):
                raise ValueError(f'{market} OHLC geometry is invalid for {symbol}')
        elif any(ohlc[:3]) and (
            ohlc[1] < max(ohlc) or ohlc[2] > min(ohlc)
        ):
            raise ValueError(f'{market} zero-volume OHLC geometry is invalid for {symbol}')
        if volume == 0 and turnover not in {None, 0}:
            raise ValueError(
                f'{market} turnover is positive while volume is zero for {symbol}'
            )
        volume_by_symbol[symbol] = volume
        ohlc_by_symbol[symbol] = ohlc
        if turnover is None:
            missing_turnover_symbols.append(symbol)
        else:
            turnover_by_symbol[symbol] = turnover

    if not symbols:
        raise ValueError(f'{market} market payload has no valid symbols')
    return (
        symbols,
        turnover_by_symbol,
        volume_by_symbol,
        ohlc_by_symbol,
        missing_turnover_symbols,
        skipped_provider_codes,
    )


class DailyTurnoverRepairService:
    def __init__(
        self,
        *,
        trade_date_resolver: Callable[..., str] | None = None,
        market_ohlcv_loader: Callable[[str, str], object] | None = None,
        turnover_patcher: Callable[..., dict[str, Any]] | None = None,
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
        self.turnover_patcher = (
            turnover_patcher or repair_daily_price_bar_turnovers
        )
        self.lock_factory = lock_factory or advisory_eod_collection_lock
        self.minimum_market_membership = dict(
            minimum_market_membership or MINIMUM_EXACT_MARKET_MEMBERSHIP
        )
        if set(self.minimum_market_membership) != set(
            SUPPORTED_TURNOVER_REPAIR_MARKETS
        ) or any(
            isinstance(minimum, bool) or not isinstance(minimum, int) or minimum <= 0
            for minimum in self.minimum_market_membership.values()
        ):
            raise ValueError(
                'minimum market membership must define positive integer floors '
                'for KOSPI and KOSDAQ'
            )

    @staticmethod
    def _blocked_result(
        *,
        market_date: date,
        markets: Sequence[str],
        apply: bool,
        reason: str,
        **details: Any,
    ) -> dict[str, Any]:
        return {
            'market_date': market_date.isoformat(),
            'markets': list(markets),
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
        markets: Sequence[str] = DEFAULT_TURNOVER_REPAIR_MARKETS,
        apply: bool = False,
    ) -> dict[str, Any]:
        normalized_date = _normalize_market_date(market_date)
        normalized_markets = _normalize_markets(markets)
        iso_date = normalized_date.isoformat()
        compact_date = normalized_date.strftime('%Y%m%d')
        if apply:
            validate_finalized_krx_write_date(
                normalized_date,
                operation='KRX turnover repair',
            )

        with self.lock_factory(
            market='KRX',
            trade_date=iso_date,
            source='daily_price_bars',
        ) as lock_acquired:
            if not lock_acquired:
                return self._blocked_result(
                    market_date=normalized_date,
                    markets=normalized_markets,
                    apply=apply,
                    reason='advisory_lock_unavailable',
                )

            try:
                resolved_date = _normalize_market_date(
                    self.trade_date_resolver(end_date=compact_date)
                )
            except Exception as exc:  # noqa: BLE001
                logger.warning(
                    'Unable to resolve exact KRX trade date for turnover repair %s: %s',
                    iso_date,
                    exc,
                )
                return self._blocked_result(
                    market_date=normalized_date,
                    markets=normalized_markets,
                    apply=apply,
                    reason='trade_date_resolution_failed',
                    error_type=type(exc).__name__,
                    error_message=str(exc),
                )
            if resolved_date != normalized_date:
                return self._blocked_result(
                    market_date=normalized_date,
                    markets=normalized_markets,
                    apply=apply,
                    reason='not_an_exact_trading_session',
                    resolved_market_date=resolved_date.isoformat(),
                )

            expected_symbols: list[str] = []
            turnover_by_symbol: dict[str, int] = {}
            volume_by_symbol: dict[str, int] = {}
            ohlc_by_symbol: dict[str, tuple[int, int, int, int]] = {}
            missing_provider_turnover_symbols: list[str] = []
            market_symbol_counts: dict[str, int] = {}
            skipped_provider_code_entries: list[dict[str, str]] = []
            skipped_provider_code_counts_by_market: dict[str, int] = {}
            seen_symbols: set[str] = set()
            for market in normalized_markets:
                try:
                    frame = self.market_ohlcv_loader(compact_date, market)
                except Exception as exc:  # noqa: BLE001
                    logger.warning(
                        'PyKRX turnover fetch failed for %s on %s: %s',
                        market,
                        iso_date,
                        exc,
                    )
                    return self._blocked_result(
                        market_date=normalized_date,
                        markets=normalized_markets,
                        apply=apply,
                        reason='market_fetch_failed',
                        failed_market=market,
                        error_type=type(exc).__name__,
                        error_message=str(exc),
                    )
                try:
                    (
                        market_symbols,
                        market_turnovers,
                        market_volumes,
                        market_ohlc,
                        market_missing_turnovers,
                        market_skipped_provider_codes,
                    ) = _extract_market_turnovers(frame, market=market)
                except Exception as exc:  # noqa: BLE001
                    return self._blocked_result(
                        market_date=normalized_date,
                        markets=normalized_markets,
                        apply=apply,
                        reason='invalid_market_payload',
                        failed_market=market,
                        error_type=type(exc).__name__,
                        error_message=str(exc),
                    )
                market_symbol_counts[market] = len(market_symbols)
                skipped_provider_code_counts_by_market[market] = len(
                    market_skipped_provider_codes
                )
                skipped_provider_code_entries.extend(
                    {
                        'market': market,
                        'code': code,
                    }
                    for code in market_skipped_provider_codes
                )
                minimum_symbol_count = self.minimum_market_membership[market]
                if len(market_symbols) < minimum_symbol_count:
                    return self._blocked_result(
                        market_date=normalized_date,
                        markets=normalized_markets,
                        apply=apply,
                        reason='market_membership_below_safety_floor',
                        failed_market=market,
                        observed_symbol_count=len(market_symbols),
                        minimum_symbol_count=minimum_symbol_count,
                        market_symbol_counts=market_symbol_counts,
                        **_provider_skipped_code_summary(
                            skipped_provider_code_entries,
                            counts_by_market=(
                                skipped_provider_code_counts_by_market
                            ),
                        ),
                    )
                duplicate_symbols = sorted(seen_symbols.intersection(market_symbols))
                if duplicate_symbols:
                    return self._blocked_result(
                        market_date=normalized_date,
                        markets=normalized_markets,
                        apply=apply,
                        reason='duplicate_cross_market_membership',
                        duplicate_symbol_count=len(duplicate_symbols),
                        duplicate_symbols=duplicate_symbols[:20],
                        **_provider_skipped_code_summary(
                            skipped_provider_code_entries,
                            counts_by_market=(
                                skipped_provider_code_counts_by_market
                            ),
                        ),
                    )
                seen_symbols.update(market_symbols)
                expected_symbols.extend(market_symbols)
                turnover_by_symbol.update(market_turnovers)
                volume_by_symbol.update(market_volumes)
                ohlc_by_symbol.update(market_ohlc)
                missing_provider_turnover_symbols.extend(
                    market_missing_turnovers
                )

            membership_sha256 = market_membership_sha256(expected_symbols)
            if missing_provider_turnover_symbols:
                return self._blocked_result(
                    market_date=normalized_date,
                    markets=normalized_markets,
                    apply=apply,
                    reason='provider_turnover_incomplete',
                    membership_basis='exact_market_date_ohlcv',
                    membership_sha256=membership_sha256,
                    provider_missing_turnover_count=len(
                        missing_provider_turnover_symbols
                    ),
                    provider_missing_turnover_symbols=sorted(
                        missing_provider_turnover_symbols
                    )[:20],
                    market_symbol_counts=market_symbol_counts,
                    **_provider_skipped_code_summary(
                        skipped_provider_code_entries,
                        counts_by_market=skipped_provider_code_counts_by_market,
                    ),
                )
            try:
                repair_result = self.turnover_patcher(
                    market_date=iso_date,
                    expected_symbols=expected_symbols,
                    turnover_by_symbol=turnover_by_symbol,
                    provider_volume_by_symbol=volume_by_symbol,
                    provider_ohlc_by_symbol=ohlc_by_symbol,
                    apply=apply,
                )
            except Exception as exc:  # noqa: BLE001
                logger.exception(
                    'Daily turnover repair transaction failed for %s',
                    iso_date,
                )
                return {
                    **self._blocked_result(
                        market_date=normalized_date,
                        markets=normalized_markets,
                        apply=apply,
                        reason='database_repair_failed',
                        error_type=type(exc).__name__,
                        error_message=str(exc),
                    ),
                    'status': 'failed',
                    'membership_basis': 'exact_market_date_ohlcv',
                    'membership_sha256': membership_sha256,
                    **_provider_skipped_code_summary(
                        skipped_provider_code_entries,
                        counts_by_market=skipped_provider_code_counts_by_market,
                    ),
                }

            return {
                **repair_result,
                'markets': list(normalized_markets),
                'mode': 'apply' if apply else 'dry_run',
                'membership_basis': 'exact_market_date_ohlcv',
                'membership_sha256': membership_sha256,
                'market_symbol_counts': market_symbol_counts,
                'provider_missing_turnover_count': len(
                    missing_provider_turnover_symbols
                ),
                'provider_missing_turnover_symbols': sorted(
                    missing_provider_turnover_symbols
                )[:20],
                **_provider_skipped_code_summary(
                    skipped_provider_code_entries,
                    counts_by_market=skipped_provider_code_counts_by_market,
                ),
            }


daily_turnover_repair_service = DailyTurnoverRepairService()


def _parse_markets(raw: str) -> tuple[str, ...]:
    return tuple(item.strip() for item in raw.split(',') if item.strip())


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            'Validate and optionally repair daily_price_bars volume and '
            'turnover as one paired update from exact-date PyKRX data.'
        )
    )
    parser.add_argument(
        '--market-date',
        required=True,
        help='Exactly one KRX market date in YYYY-MM-DD or YYYYMMDD format.',
    )
    parser.add_argument(
        '--markets',
        default=','.join(DEFAULT_TURNOVER_REPAIR_MARKETS),
        help='Explicit comma-separated PyKRX markets.',
    )
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument(
        '--apply',
        action='store_true',
        help='Apply the validated paired volume/turnover patch.',
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
        result = daily_turnover_repair_service.repair_for_market_date(
            market_date=args.market_date,
            markets=_parse_markets(args.markets),
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
