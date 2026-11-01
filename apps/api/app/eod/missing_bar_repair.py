from __future__ import annotations

import argparse
from collections.abc import Callable, Iterable, Mapping, Sequence
from contextlib import AbstractContextManager, ExitStack, contextmanager
from dataclasses import dataclass, replace
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation
import hashlib
import json
import logging
import math
from numbers import Integral, Real
import re
import socket
import sys
from typing import Any
from uuid import uuid4

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert

from app.db import session_scope
from app.db_models import (
    DailyPipelineRun,
    DailyPriceBar,
    DailyPriceBarRepairBatch,
    EODCollectionRun,
    Symbol,
)
from app.eod.market_membership import (
    build_market_membership_snapshot,
    market_membership_sha256,
    validate_market_membership_snapshot,
)
from app.eod.finality import validate_finalized_krx_write_date
from app.eod.master import load_all_domestic_masters, _validate_master_universe
from app.eod.pykrx_backfill import (
    PYKRX_BACKFILL_SERVICE,
    _call_pykrx,
    _get_market_ohlcv_by_ticker,
    stock,
)
from app.eod.price_bar_repair_state import (
    exact_daily_price_bar_scope_fingerprint,
)
from app.repositories.eod_run_repository import advisory_eod_collection_locks


logger = logging.getLogger(__name__)

MISSING_BAR_REPAIR_MARKETS = ('KOSPI', 'KOSDAQ', 'KONEX')
# Raw price collection is all three official markets; the existing derived
# strategy contract in price_bar_repair_state intentionally remains KOSPI/KOSDAQ.
PRICE_DEPENDENT_MARKETS = ('KOSPI', 'KOSDAQ')
MISSING_BAR_INSERT_BATCH_SIZE = 200
MINIMUM_MARKET_MEMBERSHIP = {
    'KOSPI': 700,
    'KOSDAQ': 1200,
    'KONEX': 50,
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
# PyKRX does not publish the market-wide fundamental table for KONEX.  Price
# membership and OHLCV remain mandatory; only the absent valuation columns are
# optional for this market.
FUNDAMENTAL_OPTIONAL_MARKETS = frozenset({'KONEX'})
MAX_PRICE_DEPENDENT_EVALUATION_HORIZON = 10
MISSING_BAR_REPAIR_LOCK_SOURCE = 'daily_price_bars'
ACTIVE_RUN_STATUSES = ('queued', 'running')
PROVIDER_PAYLOAD_SCHEMA_VERSION = 'missing_daily_bar_provider_payload_v2'
REPAIR_EVIDENCE_SCHEMA_VERSION = 'missing_bar_insert_v3'
DOWNSTREAM_MEMBERSHIP_BUNDLE_SCHEMA_VERSION = (
    'missing_bar_downstream_membership_v1'
)
EXCLUDED_PROVIDER_CODE_PATTERN = re.compile(r'^[0-9]{4}[0-9A-Z]{2}$')
CORE_API_HOST = '127.0.0.1'
CORE_API_PORT = 8000
CORE_API_PORT_CHECK_TIMEOUT_SECONDS = 0.25


def _canonical_json_sha256(payload: Mapping[str, object]) -> str:
    return hashlib.sha256(
        json.dumps(
            payload,
            ensure_ascii=False,
            sort_keys=True,
            separators=(',', ':'),
        ).encode('utf-8')
    ).hexdigest()


def _is_core_api_port_active() -> bool:
    """Fail closed unless localhost conclusively refuses the Core API port."""

    try:
        with socket.create_connection(
            (CORE_API_HOST, CORE_API_PORT),
            timeout=CORE_API_PORT_CHECK_TIMEOUT_SECONDS,
        ):
            return True
    except ConnectionRefusedError:
        return False
    except OSError as exc:
        logger.warning(
            'Could not prove Core API port %s:%s inactive: %s',
            CORE_API_HOST,
            CORE_API_PORT,
            exc,
        )
        return True


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


def _normalize_expected_membership_sha256(value: object) -> str | None:
    if value is None:
        return None
    normalized = str(value).strip().lower()
    if len(normalized) != 64 or any(
        character not in '0123456789abcdef' for character in normalized
    ):
        raise ValueError('expected membership SHA-256 must be 64 hex characters')
    return normalized


def _normalize_expected_sha256(
    value: object,
    *,
    field_name: str,
) -> str | None:
    if value is None:
        return None
    normalized = str(value).strip().lower()
    if len(normalized) != 64 or any(
        character not in '0123456789abcdef' for character in normalized
    ):
        raise ValueError(f'{field_name} must be 64 hex characters')
    return normalized


def _normalize_expected_membership_count(value: object) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool):
        raise ValueError('expected provider membership count must be positive')
    try:
        normalized = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(
            'expected provider membership count must be positive'
        ) from exc
    if normalized <= 0 or str(value).strip() != str(normalized):
        raise ValueError('expected provider membership count must be positive')
    return normalized


def _normalize_expected_nonnegative_count(
    value: object,
    *,
    field_name: str,
) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool):
        raise ValueError(f'{field_name} must be a non-negative integer')
    try:
        normalized = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(
            f'{field_name} must be a non-negative integer'
        ) from exc
    if normalized < 0 or str(value).strip() != str(normalized):
        raise ValueError(f'{field_name} must be a non-negative integer')
    return normalized


def _stable_json_sha256(payload: object) -> str:
    return hashlib.sha256(
        json.dumps(
            payload,
            ensure_ascii=True,
            separators=(',', ':'),
            sort_keys=True,
        ).encode('utf-8')
    ).hexdigest()


def _canonical_decimal(value: Decimal | None) -> str | None:
    if value is None:
        return None
    if value == 0:
        return '0'
    return format(value.normalize(), 'f')


def _provider_payload_sha256(
    *,
    market_date: date,
    bars: Sequence[MissingDailyBar],
    historical_symbol_identities: Mapping[str, Mapping[str, str]] | None = None,
    excluded_ohlcv_codes: Sequence[Mapping[str, str]] | None = None,
    excluded_fundamental_codes: Sequence[Mapping[str, str]] | None = None,
) -> str:
    rows = [
        {
            'bps': _canonical_decimal(bar.bps),
            'close_price': bar.close_price,
            'dividend_yield': _canonical_decimal(bar.dividend_yield),
            'dps': _canonical_decimal(bar.dps),
            'eps': _canonical_decimal(bar.eps),
            'high_price': bar.high_price,
            'low_price': bar.low_price,
            'market': bar.market,
            'open_price': bar.open_price,
            'pbr': _canonical_decimal(bar.pbr),
            'per': _canonical_decimal(bar.per),
            'symbol': bar.symbol,
            'turnover': bar.turnover,
            'volume': bar.volume,
        }
        for bar in sorted(bars, key=lambda item: (item.market, item.symbol))
    ]
    payload: dict[str, Any] = {
        'schema_version': PROVIDER_PAYLOAD_SCHEMA_VERSION,
        'market_date': market_date.isoformat(),
        'bars': rows,
    }
    if historical_symbol_identities:
        payload['historical_symbol_identities'] = [
            {
                'symbol': symbol,
                'market': str(identity['market']).strip().upper(),
                'name_ko': str(identity['name_ko']).strip(),
            }
            for symbol, identity in sorted(
                historical_symbol_identities.items()
            )
        ]
    if excluded_ohlcv_codes:
        payload['excluded_ohlcv_codes'] = _normalize_excluded_provider_codes(
            excluded_ohlcv_codes,
            payload_name='OHLCV',
        )
    if excluded_fundamental_codes:
        payload['excluded_fundamental_codes'] = (
            _normalize_excluded_provider_codes(
                excluded_fundamental_codes,
                payload_name='fundamental',
            )
        )
    return _stable_json_sha256(payload)


def _skipped_provider_codes_sha256(
    entries: Sequence[Mapping[str, str]],
) -> str:
    return _stable_json_sha256(
        sorted(
            (
                {
                    'market': str(entry.get('market') or '').strip().upper(),
                    'code': str(entry.get('code') or '').strip(),
                }
                for entry in entries
            ),
            key=lambda item: (item['market'], item['code']),
        )
    )


def _normalize_excluded_provider_codes(
    entries: Sequence[Mapping[str, str]],
    *,
    payload_name: str,
) -> list[dict[str, str]]:
    """Validate KRX short codes intentionally outside the numeric universe.

    The strategy universe is deliberately restricted to six-digit numeric
    stock codes.  PyKRX also returns canonical six-character short codes that
    contain an ASCII letter (for example, preferred/security variants).  They
    are not silently discarded: the exact market/code manifest is validated,
    sorted, and included in the reviewed provider-payload hash.
    """

    normalized: list[dict[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for entry in entries:
        market = str(entry.get('market') or '').strip().upper()
        raw_code = str(entry.get('code') or '').strip()
        code = raw_code.upper()
        if market not in MISSING_BAR_REPAIR_MARKETS:
            raise ValueError(
                f'{payload_name} excluded code has invalid market {market!r}'
            )
        if (
            raw_code != code
            or code.isdigit()
            or not EXCLUDED_PROVIDER_CODE_PATTERN.fullmatch(code)
        ):
            raise ValueError(
                f'{payload_name} excluded code is not a canonical KRX short '
                f'code: {raw_code!r}'
            )
        identity = (market, code)
        if identity in seen:
            raise ValueError(
                f'{payload_name} excluded code is duplicated: {market}:{code}'
            )
        seen.add(identity)
        normalized.append({'market': market, 'code': code})
    return sorted(normalized, key=lambda item: (item['market'], item['code']))


def _provider_membership_manifest(
    bars: Sequence[MissingDailyBar],
) -> dict[str, Any]:
    snapshot = build_market_membership_snapshot(
        symbols=bars,
        markets=MISSING_BAR_REPAIR_MARKETS,
    )
    all_symbols = [bar.symbol for bar in bars]
    return {
        **snapshot,
        'membership_sha256': market_membership_sha256(all_symbols),
    }


def _strict_stored_price_dependent_membership(
    run: EODCollectionRun,
    *,
    market_date: date,
) -> list[str]:
    """Read an immutable same-date KOSPI/KOSDAQ membership proof."""

    if (
        run.market != 'KRX'
        or run.trade_date != market_date
        or run.source != 'pykrx'
        or run.status not in {'completed', 'completed_with_errors'}
    ):
        raise ValueError('stored EOD run identity is not an exact terminal proof')
    metadata = run.run_metadata if isinstance(run.run_metadata, Mapping) else {}
    snapshot = metadata.get('universe_snapshot')
    if not isinstance(snapshot, Mapping):
        raise ValueError('stored EOD run has no universe snapshot')
    snapshot_markets = snapshot.get('markets')
    if (
        not isinstance(snapshot_markets, list)
        or len(snapshot_markets) != len(set(snapshot_markets))
        or not set(PRICE_DEPENDENT_MARKETS).issubset(snapshot_markets)
        or not set(snapshot_markets).issubset(MISSING_BAR_REPAIR_MARKETS)
    ):
        raise ValueError('stored universe snapshot markets are not exact')
    expected_status = {
        market: 'pykrx_market_ohlcv' for market in snapshot_markets
    }
    if (
        snapshot.get('complete') is not True
        or _normalize_market_date(snapshot.get('latest_trade_date'))
        != market_date
        or snapshot.get('universe_source') != 'pykrx_market_ohlcv'
        or snapshot.get('source') != 'pykrx'
        or snapshot.get('market_status') != expected_status
        or snapshot.get('market_errors') != []
        or snapshot.get('unavailable_markets') != []
        or snapshot.get('membership_basis') != 'end_date_snapshot'
    ):
        raise ValueError('stored universe snapshot is not an exact provider proof')

    membership = snapshot.get('market_membership')
    raw_entries = (
        membership.get('markets')
        if isinstance(membership, Mapping)
        and isinstance(membership.get('markets'), Mapping)
        else {}
    )
    requested_symbols = [
        str(symbol).strip().upper()
        for market in snapshot_markets
        for symbol in (
            raw_entries.get(market, {}).get('symbols', [])
            if isinstance(raw_entries.get(market), Mapping)
            else []
        )
    ]
    validated = validate_market_membership_snapshot(
        membership,
        requested_symbols=requested_symbols,
        markets=tuple(snapshot_markets),
    )
    if validated is None:
        raise ValueError('stored market membership proof is invalid')
    if (
        isinstance(snapshot.get('symbol_count'), bool)
        or snapshot.get('symbol_count') != len(requested_symbols)
    ):
        raise ValueError('stored universe symbol count does not match membership')

    symbols: list[str] = []
    for market in PRICE_DEPENDENT_MARKETS:
        if len(validated[market]) < MINIMUM_MARKET_MEMBERSHIP[market]:
            raise ValueError(
                f'{market} stored membership is below the safety floor'
            )
        for raw_symbol in validated[market]:
            normalized = str(raw_symbol).strip().upper()
            if _normalize_provider_symbol(normalized) != normalized:
                raise ValueError('stored market membership contains invalid symbol')
            symbols.append(normalized)
    if not symbols or len(symbols) != len(set(symbols)):
        raise ValueError('stored market membership is empty or duplicated')
    return sorted(symbols)


def _select_strict_stored_membership(
    session: Any,
    *,
    market_date: date,
    for_update: bool,
) -> tuple[EODCollectionRun | None, list[str] | None]:
    statement = (
        select(EODCollectionRun)
        .where(
            EODCollectionRun.market == 'KRX',
            EODCollectionRun.trade_date == market_date,
            EODCollectionRun.source == 'pykrx',
            EODCollectionRun.status.in_(
                ('completed', 'completed_with_errors')
            ),
        )
        .order_by(EODCollectionRun.id.desc())
    )
    if for_update:
        statement = statement.with_for_update()
    for candidate in session.scalars(statement).all():
        try:
            symbols = _strict_stored_price_dependent_membership(
                candidate,
                market_date=market_date,
            )
        except (TypeError, ValueError):
            continue
        return candidate, symbols
    return None, None


def _downstream_fallback_bundle_sha256(
    manifests: Mapping[str, Mapping[str, object]],
) -> str:
    return _canonical_json_sha256(
        {
            'schema_version': DOWNSTREAM_MEMBERSHIP_BUNDLE_SCHEMA_VERSION,
            'manifests': {
                market_date: dict(manifests[market_date])
                for market_date in sorted(manifests)
            },
        }
    )


def _validate_prepared_downstream_membership_manifest(
    raw_manifest: Mapping[str, object],
    *,
    market_date: date,
) -> dict[str, Any]:
    expected_date = market_date.isoformat()
    if (
        not isinstance(raw_manifest, Mapping)
        or raw_manifest.get('basis')
        != 'reviewed_live_exact_date_pykrx_membership'
        or raw_manifest.get('source') != 'pykrx'
        or raw_manifest.get('universe_source') != 'pykrx_market_ohlcv'
        or raw_manifest.get('market_date') != expected_date
        or raw_manifest.get('markets') != list(PRICE_DEPENDENT_MARKETS)
    ):
        raise ValueError(
            f'prepared downstream membership proof is invalid for '
            f'{expected_date}'
        )
    raw_symbols = raw_manifest.get('symbols')
    if not isinstance(raw_symbols, list):
        raise ValueError('prepared downstream membership symbols are invalid')
    symbols = sorted(str(symbol).strip().upper() for symbol in raw_symbols)
    if (
        not symbols
        or len(symbols) != len(set(symbols))
        or any(_normalize_provider_symbol(symbol) != symbol for symbol in symbols)
        or isinstance(raw_manifest.get('symbol_count'), bool)
        or raw_manifest.get('symbol_count') != len(symbols)
        or raw_manifest.get('membership_sha256')
        != market_membership_sha256(symbols)
    ):
        raise ValueError('prepared downstream membership scope is invalid')
    snapshot = raw_manifest.get('market_membership')
    validated = validate_market_membership_snapshot(
        snapshot,
        requested_symbols=symbols,
        markets=PRICE_DEPENDENT_MARKETS,
    )
    if validated is None:
        raise ValueError('prepared downstream market membership is invalid')
    for market in PRICE_DEPENDENT_MARKETS:
        if len(validated[market]) < MINIMUM_MARKET_MEMBERSHIP[market]:
            raise ValueError(
                f'{market} prepared membership is below the safety floor'
            )
    excluded_codes = _normalize_excluded_provider_codes(
        raw_manifest.get('excluded_ohlcv_codes') or (),
        payload_name='downstream OHLCV',
    )
    if raw_manifest.get('excluded_ohlcv_code_sha256') != (
        _skipped_provider_codes_sha256(excluded_codes)
    ):
        raise ValueError('prepared downstream excluded-code hash is invalid')
    input_sha256 = _normalize_expected_sha256(
        raw_manifest.get('daily_price_input_sha256'),
        field_name='prepared downstream daily price input SHA-256',
    )
    input_count = raw_manifest.get('daily_price_input_count')
    if (
        input_sha256 is None
        or isinstance(input_count, bool)
        or input_count != len(symbols)
    ):
        raise ValueError('prepared downstream daily price input is invalid')
    return {
        'basis': 'reviewed_live_exact_date_pykrx_membership',
        'eod_run_id': None,
        'eod_run_source': 'pykrx',
        'source': 'pykrx',
        'universe_source': 'pykrx_market_ohlcv',
        'market_date': expected_date,
        'markets': list(PRICE_DEPENDENT_MARKETS),
        'market_membership': snapshot,
        'symbols': symbols,
        'symbol_count': len(symbols),
        'membership_sha256': market_membership_sha256(symbols),
        'excluded_ohlcv_codes': excluded_codes,
        'excluded_ohlcv_code_sha256': _skipped_provider_codes_sha256(
            excluded_codes
        ),
        'daily_price_input_count': input_count,
        'daily_price_input_sha256': input_sha256,
    }


def _load_downstream_membership_manifests(
    session: Any,
    *,
    target_date: date,
    required_rebuild_dates: Sequence[date],
    prepared_fallback_manifests: Mapping[
        str,
        Mapping[str, object],
    ]
    | None = None,
) -> dict[str, dict[str, Any]]:
    """Freeze every downstream date from strict persisted provider evidence."""

    manifests: dict[str, dict[str, Any]] = {}
    prepared = dict(prepared_fallback_manifests or {})
    consumed_prepared_dates: set[str] = set()
    for market_date in required_rebuild_dates:
        if market_date == target_date:
            continue
        selected_run, symbols = _select_strict_stored_membership(
            session,
            market_date=market_date,
            for_update=True,
        )
        if selected_run is None or symbols is None:
            prepared_key = market_date.isoformat()
            raw_prepared = prepared.get(prepared_key)
            if raw_prepared is None:
                raise RuntimeError(
                    'no strict same-date PyKRX membership proof for downstream '
                    f'rebuild date {prepared_key}'
                )
            normalized_prepared = (
                _validate_prepared_downstream_membership_manifest(
                    raw_prepared,
                    market_date=market_date,
                )
            )
            symbols = normalized_prepared['symbols']
            selected_run = None
            consumed_prepared_dates.add(prepared_key)
        input_sha256, input_count = exact_daily_price_bar_scope_fingerprint(
            session,
            market_date=market_date,
            symbols=symbols,
        )
        if input_count != len(symbols):
            raise RuntimeError(
                'downstream membership is not fully backed by stored price '
                f'bars for {market_date.isoformat()}'
            )
        if selected_run is None:
            if (
                input_count != normalized_prepared['daily_price_input_count']
                or input_sha256
                != normalized_prepared['daily_price_input_sha256']
            ):
                raise RuntimeError(
                    'prepared downstream price input changed for '
                    f'{market_date.isoformat()}'
                )
            manifests[market_date.isoformat()] = normalized_prepared
        else:
            manifests[market_date.isoformat()] = {
                'basis': 'validated_eod_run_market_membership',
                'eod_run_id': selected_run.id,
                'eod_run_source': selected_run.source,
                'symbols': symbols,
                'symbol_count': len(symbols),
                'membership_sha256': market_membership_sha256(symbols),
                'daily_price_input_count': input_count,
                'daily_price_input_sha256': input_sha256,
            }
    if consumed_prepared_dates != set(prepared):
        raise RuntimeError(
            'prepared downstream membership scope does not match proof gaps'
        )
    return manifests


def prepare_downstream_fallback_membership_bundle(
    *,
    target_date: date,
    required_rebuild_dates: Sequence[date],
    trade_date_resolver: Callable[..., str],
    market_ohlcv_loader: Callable[[str, str], object],
    minimum_market_membership: Mapping[str, int],
) -> dict[str, Any]:
    """Freeze exact live PyKRX proof only where durable EOD proof is absent."""

    proof_gap_dates: list[date] = []
    with session_scope() as session:
        for market_date in required_rebuild_dates:
            if market_date == target_date:
                continue
            stored_run, stored_symbols = _select_strict_stored_membership(
                session,
                market_date=market_date,
                for_update=False,
            )
            if stored_run is None or stored_symbols is None:
                proof_gap_dates.append(market_date)

    manifests: dict[str, dict[str, Any]] = {}
    for market_date in proof_gap_dates:
        compact_date = market_date.strftime('%Y%m%d')
        resolved_date = _normalize_market_date(
            trade_date_resolver(end_date=compact_date)
        )
        if resolved_date != market_date:
            raise RuntimeError(
                'downstream fallback date is not an exact trading session: '
                f'{market_date.isoformat()}'
            )

        members: list[MissingDailyBar] = []
        seen_symbols: set[str] = set()
        excluded_codes: list[dict[str, str]] = []
        for market in PRICE_DEPENDENT_MARKETS:
            market_frame = market_ohlcv_loader(compact_date, market)
            market_bars, market_excluded_codes = _extract_market_ohlcv(
                market_frame,
                market=market,
            )
            floor = minimum_market_membership[market]
            if len(market_bars) < floor:
                raise RuntimeError(
                    f'{market} downstream fallback membership is below '
                    f'the safety floor for {market_date.isoformat()}'
                )
            market_symbols = {bar.symbol for bar in market_bars}
            duplicates = sorted(seen_symbols.intersection(market_symbols))
            if duplicates:
                raise RuntimeError(
                    'downstream fallback contains duplicate cross-market '
                    f'membership for {market_date.isoformat()}: '
                    + ','.join(duplicates[:20])
                )
            seen_symbols.update(market_symbols)
            members.extend(market_bars)
            excluded_codes.extend(
                {'market': market, 'code': code}
                for code in market_excluded_codes
            )

        normalized_excluded_codes = _normalize_excluded_provider_codes(
            excluded_codes,
            payload_name='downstream OHLCV',
        )
        symbols = sorted(member.symbol for member in members)
        membership = build_market_membership_snapshot(
            symbols=members,
            markets=PRICE_DEPENDENT_MARKETS,
        )
        manifests[market_date.isoformat()] = {
            'basis': 'reviewed_live_exact_date_pykrx_membership',
            'source': 'pykrx',
            'universe_source': 'pykrx_market_ohlcv',
            'market_date': market_date.isoformat(),
            'markets': list(PRICE_DEPENDENT_MARKETS),
            'market_membership': membership,
            'symbols': symbols,
            'symbol_count': len(symbols),
            'membership_sha256': market_membership_sha256(symbols),
            'excluded_ohlcv_codes': normalized_excluded_codes,
            'excluded_ohlcv_code_sha256': (
                _skipped_provider_codes_sha256(normalized_excluded_codes)
            ),
        }

    with session_scope() as session:
        for raw_market_date, manifest in manifests.items():
            market_date = _normalize_market_date(raw_market_date)
            input_sha256, input_count = exact_daily_price_bar_scope_fingerprint(
                session,
                market_date=market_date,
                symbols=manifest['symbols'],
            )
            if input_count != manifest['symbol_count']:
                raise RuntimeError(
                    'downstream fallback membership is not fully backed by '
                    f'stored price bars for {raw_market_date}'
                )
            manifest['daily_price_input_count'] = input_count
            manifest['daily_price_input_sha256'] = input_sha256
            _validate_prepared_downstream_membership_manifest(
                manifest,
                market_date=market_date,
            )

    return {
        'schema_version': DOWNSTREAM_MEMBERSHIP_BUNDLE_SCHEMA_VERSION,
        'manifest_count': len(manifests),
        'bundle_sha256': _downstream_fallback_bundle_sha256(manifests),
        'manifests': manifests,
    }


def load_missing_bar_required_rebuild_trade_dates(
    *,
    from_date: date,
) -> tuple[date, ...]:
    """Return the complete stored raw-date suffix independent of classification."""

    with session_scope() as session:
        return tuple(
            session.scalars(
                select(DailyPriceBar.trade_date)
                .where(DailyPriceBar.trade_date >= from_date)
                .distinct()
                .order_by(DailyPriceBar.trade_date.asc())
            ).all()
        )


@contextmanager
def _adapt_single_lock_factory(
    lock_factory: Callable[..., AbstractContextManager[bool]],
    *,
    market: str,
    trade_dates: Sequence[date],
    source: str,
    exclusive_admission: bool = False,
):
    """Compatibility adapter for injected unit-test single-date locks."""

    with ExitStack() as stack:
        del exclusive_admission
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


def _provider_code_preview(value: object) -> str:
    raw = str(value or '').strip() or repr(value)
    return raw if len(raw) <= 64 else raw[:61] + '...'


def _provider_code_identity(value: object) -> str:
    return str(value or '').strip() or repr(value)


def _skipped_code_preview(
    entries: Sequence[Mapping[str, str]],
) -> list[dict[str, str]]:
    return [
        {
            'market': str(entry.get('market') or '').strip().upper(),
            'code': _provider_code_preview(entry.get('code')),
        }
        for entry in entries[:20]
    ]


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
            skipped_codes.append(_provider_code_identity(raw_symbol))
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
            skipped_codes.append(_provider_code_identity(raw_symbol))
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


def _is_optional_market_empty_fundamental_error(
    exc: Exception,
    *,
    market: str,
) -> bool:
    """Recognize PyKRX's empty-table failure without hiding provider errors."""

    if market not in FUNDAMENTAL_OPTIONAL_MARKETS or not isinstance(exc, KeyError):
        return False
    message = str(exc)
    return (
        'None of [Index(' in message
        and all(column in message for column in FUNDAMENTAL_COLUMNS)
    )


def _get_market_fundamental_by_ticker(trade_date: str, market: str):
    getter = getattr(stock, 'get_market_fundamental_by_ticker', None)
    if not callable(getter):
        raise RuntimeError('PyKRX market-wide fundamental API is unavailable')
    return _call_pykrx(getter, trade_date, market)


def _get_symbol_fundamental_by_date(
    trade_date: str,
    symbol: str,
):
    getter = getattr(stock, 'get_market_fundamental_by_date', None)
    if not callable(getter):
        raise RuntimeError('PyKRX symbol fundamental API is unavailable')
    return _call_pykrx(getter, trade_date, trade_date, symbol)


def _extract_symbol_fundamental(
    frame: object,
    *,
    symbol: str,
) -> dict[str, Decimal | None] | None:
    """Read an exact-date per-symbol fallback, preserving explicit absence."""

    columns = set(getattr(frame, 'columns', ()))
    if bool(getattr(frame, 'empty', False)):
        return None
    missing_columns = sorted(set(FUNDAMENTAL_COLUMNS).difference(columns))
    if missing_columns:
        raise ValueError(
            f'{symbol} exact-date fundamental payload is missing columns: '
            + ','.join(missing_columns)
        )
    rows = list(
        _iter_frame_rows(
            frame,
            payload_name=f'{symbol} exact-date fundamental',
        )
    )
    if len(rows) != 1:
        raise ValueError(
            f'{symbol} exact-date fundamental payload must contain one row'
        )
    _, row = rows[0]
    return {
        FUNDAMENTAL_COLUMNS[column]: _coerce_optional_decimal(
            row[column],
            symbol=symbol,
            field=FUNDAMENTAL_COLUMNS[column],
        )
        for column in FUNDAMENTAL_COLUMNS
    }


def _load_current_master_security_groups() -> dict[str, str]:
    symbols = load_all_domestic_masters()
    _validate_master_universe(symbols)
    return {item.symbol: item.security_group for item in symbols}


def _prepare_missing_historical_symbol_identities(
    *,
    bars: Sequence[MissingDailyBar],
    market_date: date,
) -> dict[str, dict[str, str]]:
    """Resolve only exact-date members absent from the durable symbol table."""

    symbols = [bar.symbol for bar in bars]
    with session_scope() as session:
        existing_symbols = set(
            session.scalars(
                select(Symbol.symbol).where(Symbol.symbol.in_(symbols))
            ).all()
        )
    missing_bars = [bar for bar in bars if bar.symbol not in existing_symbols]
    identities: dict[str, dict[str, str]] = {}
    for bar in missing_bars:
        raw_name = _call_pykrx(stock.get_market_ticker_name, bar.symbol)
        if not isinstance(raw_name, str):
            raise ValueError(
                f'PyKRX returned non-string name for historical symbol '
                f'{bar.symbol}'
            )
        name_ko = raw_name.strip()
        if not name_ko or name_ko == bar.symbol:
            raise ValueError(
                f'PyKRX returned no authoritative name for historical symbol '
                f'{bar.symbol}'
            )
        identities[bar.symbol] = {
            'symbol': bar.symbol,
            'market': bar.market,
            'name_ko': name_ko,
            'market_date': market_date.isoformat(),
        }
    return identities


def _normalize_historical_symbol_identities(
    identities: Mapping[str, Mapping[str, object]] | None,
    *,
    market_by_symbol: Mapping[str, str],
    market_date: date,
) -> dict[str, dict[str, str]]:
    normalized: dict[str, dict[str, str]] = {}
    for raw_symbol, raw_identity in (identities or {}).items():
        symbol = _normalize_provider_symbol(raw_symbol)
        if symbol is None or symbol not in market_by_symbol:
            raise ValueError(
                'historical symbol identity is outside provider membership'
            )
        if not isinstance(raw_identity, Mapping):
            raise ValueError('historical symbol identity must be an object')
        market = str(raw_identity.get('market') or '').strip().upper()
        name_ko = str(raw_identity.get('name_ko') or '').strip()
        identity_date = _normalize_market_date(
            str(raw_identity.get('market_date') or '')
        )
        if market != market_by_symbol[symbol] or identity_date != market_date:
            raise ValueError(
                f'historical symbol identity scope mismatch for {symbol}'
            )
        if not name_ko or name_ko == symbol:
            raise ValueError(
                f'historical symbol identity name is unavailable for {symbol}'
            )
        normalized[symbol] = {
            'symbol': symbol,
            'market': market,
            'name_ko': name_ko,
            'market_date': identity_date.isoformat(),
        }
    return normalized


def repair_missing_daily_price_bars(
    *,
    market_date: str | date,
    bars: Sequence[MissingDailyBar],
    apply: bool = False,
    expected_provider_membership_count: int | None = None,
    expected_membership_sha256: str | None = None,
    expected_provider_payload_sha256: str | None = None,
    expected_missing_bar_count: int | None = None,
    expected_missing_membership_sha256: str | None = None,
    required_rebuild_trade_dates: Sequence[str | date] | None = None,
    historical_symbol_identities: Mapping[
        str,
        Mapping[str, object],
    ]
    | None = None,
    excluded_ohlcv_codes: Sequence[Mapping[str, str]] | None = None,
    excluded_fundamental_codes: Sequence[Mapping[str, str]] | None = None,
    prepared_downstream_membership_manifests: Mapping[
        str,
        Mapping[str, object],
    ]
    | None = None,
    expected_downstream_fallback_manifest_count: int | None = None,
    expected_downstream_fallback_bundle_sha256: str | None = None,
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
    provider_market_by_code = {
        bar.symbol: bar.market for bar in normalized_bars
    }
    normalized_historical_identities = (
        _normalize_historical_symbol_identities(
            historical_symbol_identities,
            market_by_symbol=provider_market_by_code,
            market_date=normalized_date,
        )
    )
    normalized_excluded_ohlcv_codes = _normalize_excluded_provider_codes(
        excluded_ohlcv_codes or (),
        payload_name='OHLCV',
    )
    normalized_excluded_fundamental_codes = (
        _normalize_excluded_provider_codes(
            excluded_fundamental_codes or (),
            payload_name='fundamental',
        )
    )
    provider_membership_count = len(expected_symbols)
    provider_membership_sha256 = market_membership_sha256(expected_symbols)
    provider_payload_sha256 = _provider_payload_sha256(
        market_date=normalized_date,
        bars=normalized_bars,
        historical_symbol_identities=normalized_historical_identities,
        excluded_ohlcv_codes=normalized_excluded_ohlcv_codes,
        excluded_fundamental_codes=normalized_excluded_fundamental_codes,
    )
    provider_manifest = _provider_membership_manifest(normalized_bars)
    normalized_expected_count = _normalize_expected_membership_count(
        expected_provider_membership_count
    )
    normalized_expected_sha256 = _normalize_expected_membership_sha256(
        expected_membership_sha256
    )
    normalized_expected_payload_sha256 = _normalize_expected_sha256(
        expected_provider_payload_sha256,
        field_name='expected provider payload SHA-256',
    )
    normalized_expected_missing_count = _normalize_expected_nonnegative_count(
        expected_missing_bar_count,
        field_name='expected missing bar count',
    )
    normalized_expected_missing_sha256 = _normalize_expected_sha256(
        expected_missing_membership_sha256,
        field_name='expected missing membership SHA-256',
    )
    normalized_expected_fallback_count = (
        _normalize_expected_nonnegative_count(
            expected_downstream_fallback_manifest_count,
            field_name='expected downstream fallback manifest count',
        )
    )
    normalized_expected_fallback_sha256 = _normalize_expected_sha256(
        expected_downstream_fallback_bundle_sha256,
        field_name='expected downstream fallback bundle SHA-256',
    )
    normalized_prepared_fallbacks = dict(
        prepared_downstream_membership_manifests or {}
    )
    prepared_fallback_count = len(normalized_prepared_fallbacks)
    prepared_fallback_sha256 = _downstream_fallback_bundle_sha256(
        normalized_prepared_fallbacks
    )
    if apply and (
        normalized_expected_count is None
        or normalized_expected_sha256 is None
        or normalized_expected_payload_sha256 is None
        or normalized_expected_missing_count is None
        or normalized_expected_missing_sha256 is None
    ):
        raise ValueError(
            'apply requires reviewed provider membership, provider payload, '
            'and exact missing-scope count/SHA-256'
        )
    if (
        normalized_expected_count is not None
        and normalized_expected_count != provider_membership_count
    ):
        raise ValueError(
            'provider membership count changed after dry-run review'
        )
    if (
        normalized_expected_sha256 is not None
        and normalized_expected_sha256 != provider_membership_sha256
    ):
        raise ValueError(
            'provider membership SHA-256 changed after dry-run review'
        )
    if (
        normalized_expected_payload_sha256 is not None
        and normalized_expected_payload_sha256 != provider_payload_sha256
    ):
        raise ValueError('provider payload SHA-256 changed after dry-run review')
    if (
        normalized_expected_fallback_count is not None
        and normalized_expected_fallback_count != prepared_fallback_count
    ):
        raise ValueError(
            'downstream fallback manifest count changed after dry-run review'
        )
    if (
        normalized_expected_fallback_sha256 is not None
        and normalized_expected_fallback_sha256 != prepared_fallback_sha256
    ):
        raise ValueError(
            'downstream fallback bundle SHA-256 changed after dry-run review'
        )
    if apply and prepared_downstream_membership_manifests is not None and (
        normalized_expected_fallback_count is None
        or normalized_expected_fallback_sha256 is None
    ):
        raise ValueError(
            'apply requires reviewed downstream fallback count/SHA-256'
        )

    normalized_locked_dates = tuple(
        sorted(
            {
                _normalize_market_date(item)
                for item in required_rebuild_trade_dates or ()
            }
        )
    )
    if apply and (
        not normalized_locked_dates
        or normalized_date not in normalized_locked_dates
    ):
        raise ValueError(
            'apply requires the target and complete downstream date lock scope'
        )

    with session_scope() as session:
        observed_rebuild_dates = tuple(
            session.scalars(
                select(DailyPriceBar.trade_date)
                .where(DailyPriceBar.trade_date >= normalized_date)
                .distinct()
                .order_by(DailyPriceBar.trade_date.asc())
            ).all()
        )
        if apply and observed_rebuild_dates != normalized_locked_dates:
            raise RuntimeError(
                'missing-bar repair downstream rebuild scope changed after '
                'the date locks were acquired'
            )

        active_eod_run_ids: tuple[int, ...] = ()
        active_pipeline_run_ids: tuple[int, ...] = ()
        if apply:
            active_eod_run_ids = tuple(
                session.scalars(
                    select(EODCollectionRun.id)
                    .where(
                        EODCollectionRun.market == 'KRX',
                        EODCollectionRun.source == 'pykrx',
                        EODCollectionRun.trade_date >= normalized_date,
                        EODCollectionRun.status.in_(ACTIVE_RUN_STATUSES),
                    )
                    .order_by(EODCollectionRun.id.asc())
                    .with_for_update()
                ).all()
            )
            active_pipeline_run_ids = tuple(
                session.scalars(
                    select(DailyPipelineRun.id)
                    .where(
                        DailyPipelineRun.market_date >= normalized_date,
                        DailyPipelineRun.status.in_(ACTIVE_RUN_STATUSES),
                    )
                    .order_by(DailyPipelineRun.id.asc())
                    .with_for_update()
                ).all()
            )
            if active_eod_run_ids or active_pipeline_run_ids:
                return {
                    'market_date': normalized_date.isoformat(),
                    'status': 'blocked',
                    'reason': 'official_daily_or_pykrx_task_active',
                    'applied': False,
                    'active_pykrx_eod_run_ids': list(active_eod_run_ids),
                    'active_daily_pipeline_run_ids': list(
                        active_pipeline_run_ids
                    ),
                    'required_rebuild_trade_dates': [
                        item.isoformat() for item in normalized_locked_dates
                    ],
                    'missing_bar_count': 0,
                    'would_insert_count': 0,
                    'inserted_count': 0,
                }

        downstream_membership_by_date: dict[str, dict[str, Any]] = {}

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
        unresolved_symbol_records = sorted(
            set(missing_symbol_records).difference(
                normalized_historical_identities
            )
        )
        if unresolved_symbol_records:
            return {
                'market_date': normalized_date.isoformat(),
                'status': 'blocked',
                'reason': 'provider_membership_missing_from_symbols_table',
                'applied': False,
                'expected_symbol_count': len(expected_symbols),
                'symbols_table_match_count': len(symbol_id_by_code),
                'missing_symbol_record_count': len(unresolved_symbol_records),
                'missing_symbol_records': unresolved_symbol_records[:20],
                'missing_bar_count': 0,
                'would_insert_count': 0,
                'inserted_count': 0,
            }

        if apply and missing_symbol_records:
            historical_refreshed_at = datetime.now(timezone.utc)
            session.execute(
                pg_insert(Symbol)
                .values(
                    [
                        {
                            'symbol': symbol,
                            'name_ko': normalized_historical_identities[
                                symbol
                            ]['name_ko'],
                            'market': normalized_historical_identities[
                                symbol
                            ]['market'],
                            'source_payload': {
                                'identity_basis': (
                                    'exact_date_pykrx_membership_with_'
                                    'repair_time_name_lookup'
                                ),
                                'identity_market_basis': (
                                    'exact_date_pykrx_membership'
                                ),
                                'identity_name_basis': (
                                    'pykrx_ticker_name_lookup_at_repair_time'
                                ),
                                'identity_market_date': (
                                    normalized_date.isoformat()
                                ),
                                'provider_membership_sha256': (
                                    provider_membership_sha256
                                ),
                            },
                            'instrument_type': 'equity',
                            'source': 'pykrx_historical_repair',
                            'is_active': False,
                            'last_refreshed_at': historical_refreshed_at,
                        }
                        for symbol in missing_symbol_records
                    ]
                )
                .on_conflict_do_nothing(index_elements=[Symbol.symbol])
            )
            symbol_rows = session.execute(
                select(Symbol.id, Symbol.symbol, Symbol.market).where(
                    Symbol.symbol.in_(expected_symbols)
                )
            ).all()
            symbol_id_by_code = {
                str(row.symbol): int(row.id) for row in symbol_rows
            }
            still_missing = sorted(
                expected_set.difference(symbol_id_by_code)
            )
            if still_missing:
                raise RuntimeError(
                    'historical symbol identity insert did not converge: '
                    + ','.join(still_missing[:20])
                )
        historical_market_transitions = sorted(
            (
                {
                    'symbol': str(row.symbol),
                    'current_market': str(row.market or '').upper(),
                    'historical_market': provider_market_by_code[
                        str(row.symbol)
                    ],
                }
                for row in symbol_rows
                if str(row.market or '').upper()
                != provider_market_by_code[str(row.symbol)]
            ),
            key=lambda item: item['symbol'],
        )

        stored_before = set(
            session.execute(
                select(Symbol.symbol)
                .join(DailyPriceBar, DailyPriceBar.symbol_id == Symbol.id)
                .where(
                    DailyPriceBar.trade_date == normalized_date,
                    Symbol.symbol.in_(expected_symbols),
                )
            ).scalars()
        )
        current_market_stored = set(
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
            current_market_stored.difference(expected_set)
        )
        missing_bars = [
            bar for bar in normalized_bars if bar.symbol not in stored_before
        ]
        missing_symbols = sorted(bar.symbol for bar in missing_bars)
        missing_membership_sha256 = market_membership_sha256(missing_symbols)
        if (
            normalized_expected_missing_count is not None
            and normalized_expected_missing_count != len(missing_symbols)
        ):
            raise ValueError(
                'missing bar count changed after dry-run review'
            )
        if (
            normalized_expected_missing_sha256 is not None
            and normalized_expected_missing_sha256
            != missing_membership_sha256
        ):
            raise ValueError(
                'missing membership SHA-256 changed after dry-run review'
            )
        result: dict[str, Any] = {
            'market_date': normalized_date.isoformat(),
            'status': 'ready',
            'reason': None,
            'applied': False,
            'provider_membership_count': provider_membership_count,
            'provider_membership_sha256': provider_membership_sha256,
            'provider_payload_sha256': provider_payload_sha256,
            'expected_provider_membership_count': normalized_expected_count,
            'expected_membership_sha256': normalized_expected_sha256,
            'expected_provider_payload_sha256': (
                normalized_expected_payload_sha256
            ),
            'expected_missing_bar_count': normalized_expected_missing_count,
            'expected_missing_membership_sha256': (
                normalized_expected_missing_sha256
            ),
            'downstream_fallback_manifest_count': prepared_fallback_count,
            'downstream_fallback_bundle_sha256': prepared_fallback_sha256,
            'expected_downstream_fallback_manifest_count': (
                normalized_expected_fallback_count
            ),
            'expected_downstream_fallback_bundle_sha256': (
                normalized_expected_fallback_sha256
            ),
            'expected_symbol_count': len(expected_symbols),
            'symbols_table_match_count': len(symbol_id_by_code),
            'historical_symbol_create_count': (
                len(missing_symbol_records) if apply else 0
            ),
            'would_create_historical_symbol_count': len(
                missing_symbol_records
            ),
            'would_create_historical_symbols': missing_symbol_records[:20],
            'stored_before_count': len(stored_before),
            'stored_outside_provider_membership_count': len(
                stored_outside_provider_membership
            ),
            'stored_outside_provider_membership_symbols': (
                stored_outside_provider_membership[:20]
            ),
            'historical_market_transition_count': len(
                historical_market_transitions
            ),
            'historical_market_transition_preview': (
                historical_market_transitions[:20]
            ),
            'provider_excluded_ohlcv_code_count': len(
                normalized_excluded_ohlcv_codes
            ),
            'provider_excluded_ohlcv_code_sha256': (
                _skipped_provider_codes_sha256(
                    normalized_excluded_ohlcv_codes
                )
            ),
            'provider_excluded_fundamental_code_count': len(
                normalized_excluded_fundamental_codes
            ),
            'provider_excluded_fundamental_code_sha256': (
                _skipped_provider_codes_sha256(
                    normalized_excluded_fundamental_codes
                )
            ),
            'missing_bar_count': len(missing_bars),
            'missing_membership_sha256': missing_membership_sha256,
            'missing_bar_symbols': missing_symbols,
            'missing_bar_symbol_preview': missing_symbols[:20],
            'would_insert_count': len(missing_bars),
            'inserted_count': 0,
            'repair_batch_id': None,
            'rebuild_debt_recorded': False,
            'required_rebuild_trade_dates': [
                item.isoformat() for item in normalized_locked_dates
            ],
        }
        if not apply:
            return result

        inserted_count = 0
        if missing_bars:
            downstream_membership_by_date = (
                _load_downstream_membership_manifests(
                    session,
                    target_date=normalized_date,
                    required_rebuild_dates=normalized_locked_dates,
                    prepared_fallback_manifests=(
                        normalized_prepared_fallbacks
                    ),
                )
            )
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
                    Symbol.symbol.in_(expected_symbols),
                )
            ).scalars()
        )
        if stored_after != expected_set:
            raise RuntimeError(
                'missing-bar coverage verification failed after insert'
            )

        repair_batch_id: str | None = None
        repair_evidence: dict[str, Any] | None = None
        if missing_bars:
            inserted_symbols = missing_symbols
            inserted_price_input_sha256, inserted_scope_count = (
                exact_daily_price_bar_scope_fingerprint(
                    session,
                    market_date=normalized_date,
                    symbols=inserted_symbols,
                )
            )
            if inserted_scope_count != inserted_count:
                raise RuntimeError(
                    'missing-bar durable evidence count changed after insert'
                )
            provider_price_input_sha256, provider_scope_count = (
                exact_daily_price_bar_scope_fingerprint(
                    session,
                    market_date=normalized_date,
                    symbols=expected_symbols,
                )
            )
            if provider_scope_count != provider_membership_count:
                raise RuntimeError(
                    'missing-bar full provider scope changed after insert'
                )
            required_rebuild_dates = normalized_locked_dates
            prior_dates = tuple(
                session.scalars(
                    select(DailyPriceBar.trade_date)
                    .where(DailyPriceBar.trade_date < normalized_date)
                    .distinct()
                    .order_by(DailyPriceBar.trade_date.desc())
                    .limit(MAX_PRICE_DEPENDENT_EVALUATION_HORIZON)
                ).all()
            )
            evaluation_market_date_from = (
                min(prior_dates) if prior_dates else normalized_date
            )
            repair_batch_id = f'missing-bar-{uuid4()}'
            recorded_at = datetime.now(timezone.utc)
            price_dependent_symbols = sorted(
                bar.symbol
                for bar in normalized_bars
                if bar.market in PRICE_DEPENDENT_MARKETS
            )
            price_dependent_input_sha256, price_dependent_input_count = (
                exact_daily_price_bar_scope_fingerprint(
                    session,
                    market_date=normalized_date,
                    symbols=price_dependent_symbols,
                )
            )
            if price_dependent_input_count != len(price_dependent_symbols):
                raise RuntimeError(
                    'target price-dependent membership changed after insert'
                )
            price_dependent_membership_by_date = {
                normalized_date.isoformat(): {
                    'basis': 'reviewed_live_provider_membership',
                    'eod_run_id': None,
                    'eod_run_source': 'pykrx',
                    'symbols': price_dependent_symbols,
                    'symbol_count': len(price_dependent_symbols),
                    'membership_sha256': market_membership_sha256(
                        price_dependent_symbols
                    ),
                    'daily_price_input_sha256': (
                        price_dependent_input_sha256
                    ),
                },
                **downstream_membership_by_date,
            }
            repair_evidence = {
                'schema_version': REPAIR_EVIDENCE_SCHEMA_VERSION,
                'repair_kind': 'missing_bar_insert',
                'provider': 'pykrx',
                'market_date': normalized_date.isoformat(),
                'expected_provider_membership_count': (
                    provider_membership_count
                ),
                'expected_provider_membership_sha256': (
                    provider_membership_sha256
                ),
                'expected_provider_payload_sha256': provider_payload_sha256,
                'provider_membership_manifest': provider_manifest,
                'provider_excluded_ohlcv_codes': (
                    normalized_excluded_ohlcv_codes
                ),
                'provider_excluded_ohlcv_code_sha256': (
                    _skipped_provider_codes_sha256(
                        normalized_excluded_ohlcv_codes
                    )
                ),
                'provider_excluded_fundamental_codes': (
                    normalized_excluded_fundamental_codes
                ),
                'provider_excluded_fundamental_code_sha256': (
                    _skipped_provider_codes_sha256(
                        normalized_excluded_fundamental_codes
                    )
                ),
                'downstream_fallback_manifest_count': (
                    prepared_fallback_count
                ),
                'downstream_fallback_bundle_sha256': (
                    prepared_fallback_sha256
                ),
                'historical_market_transitions': (
                    historical_market_transitions
                ),
                'provider_price_input_sha256': (
                    provider_price_input_sha256
                ),
                'price_dependent_membership_symbols': (
                    price_dependent_symbols
                ),
                'price_dependent_membership_count': len(
                    price_dependent_symbols
                ),
                'price_dependent_membership_sha256': (
                    market_membership_sha256(price_dependent_symbols)
                ),
                'price_dependent_membership_by_date': (
                    price_dependent_membership_by_date
                ),
                'reviewed_missing_symbol_count': len(missing_symbols),
                'reviewed_missing_membership_sha256': (
                    missing_membership_sha256
                ),
                'inserted_symbol_count': inserted_count,
                'inserted_symbols': inserted_symbols,
                'inserted_membership_sha256': market_membership_sha256(
                    inserted_symbols
                ),
                'inserted_price_input_sha256': (
                    inserted_price_input_sha256
                ),
                'recorded_at': recorded_at.isoformat(),
            }
            session.add(
                DailyPriceBarRepairBatch(
                    repair_batch_id=repair_batch_id,
                    status='pending',
                    affected_symbols=inserted_symbols,
                    affected_trade_dates=[normalized_date.isoformat()],
                    required_rebuild_trade_dates=[
                        item.isoformat() for item in required_rebuild_dates
                    ],
                    required_evaluation_market_date_from=(
                        evaluation_market_date_from
                    ),
                    required_evaluation_market_date_to=normalized_date,
                    evaluation_rebuilt_at=None,
                    evaluation_rebuild_metadata=None,
                    factor_rebuild_metadata=None,
                    repaired_row_count=inserted_count,
                    created_at=recorded_at,
                    completed_at=None,
                    verification_metadata={
                        'repair_evidence': repair_evidence,
                    },
                )
            )
            session.flush()

        result.update(
            {
                'status': 'completed',
                'applied': True,
                'inserted_count': inserted_count,
                'stored_after_count': len(stored_after),
                'repair_batch_id': repair_batch_id,
                'rebuild_debt_recorded': repair_batch_id is not None,
                'repair_evidence': repair_evidence,
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
        symbol_fundamental_loader: Callable[[str, str], object] | None = None,
        database_repairer: Callable[..., dict[str, Any]] | None = None,
        security_group_loader: Callable[[], Mapping[str, str]] | None = None,
        historical_symbol_identity_preparer: Callable[
            ...,
            Mapping[str, Mapping[str, object]],
        ]
        | None = None,
        lock_factory: Callable[..., AbstractContextManager[bool]] | None = None,
        lock_set_factory: Callable[
            ...,
            AbstractContextManager[tuple[bool, str | None]],
        ]
        | None = None,
        rebuild_date_loader: Callable[..., Sequence[date]] | None = None,
        downstream_membership_bundle_preparer: Callable[..., dict[str, Any]]
        | None = None,
        core_api_active_checker: Callable[[], bool] | None = None,
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
        self.symbol_fundamental_loader = (
            symbol_fundamental_loader
            if symbol_fundamental_loader is not None
            else (
                _get_symbol_fundamental_by_date
                if database_repairer is None
                else None
            )
        )
        self.database_repairer = (
            database_repairer or repair_missing_daily_price_bars
        )
        self.historical_symbol_identity_preparer = (
            historical_symbol_identity_preparer
            if historical_symbol_identity_preparer is not None
            else (
                _prepare_missing_historical_symbol_identities
                if database_repairer is None
                else None
            )
        )
        self.security_group_loader = (
            security_group_loader or _load_current_master_security_groups
        )
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
            rebuild_date_loader
            or load_missing_bar_required_rebuild_trade_dates
        )
        self.downstream_membership_bundle_preparer = (
            downstream_membership_bundle_preparer
            if downstream_membership_bundle_preparer is not None
            else (
                prepare_downstream_fallback_membership_bundle
                if database_repairer is None
                else None
            )
        )
        self.core_api_active_checker = (
            core_api_active_checker or _is_core_api_port_active
        )
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
                'KOSPI, KOSDAQ, and KONEX'
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
        expected_provider_membership_count: int | None = None,
        expected_membership_sha256: str | None = None,
        expected_provider_payload_sha256: str | None = None,
        expected_missing_bar_count: int | None = None,
        expected_missing_membership_sha256: str | None = None,
        expected_downstream_fallback_manifest_count: int | None = None,
        expected_downstream_fallback_bundle_sha256: str | None = None,
    ) -> dict[str, Any]:
        normalized_date = _normalize_market_date(market_date)
        iso_date = normalized_date.isoformat()
        compact_date = normalized_date.strftime('%Y%m%d')
        try:
            normalized_expected_count = _normalize_expected_membership_count(
                expected_provider_membership_count
            )
            normalized_expected_sha256 = (
                _normalize_expected_membership_sha256(
                    expected_membership_sha256
                )
            )
            normalized_expected_payload_sha256 = _normalize_expected_sha256(
                expected_provider_payload_sha256,
                field_name='expected provider payload SHA-256',
            )
            normalized_expected_missing_count = (
                _normalize_expected_nonnegative_count(
                    expected_missing_bar_count,
                    field_name='expected missing bar count',
                )
            )
            normalized_expected_missing_sha256 = _normalize_expected_sha256(
                expected_missing_membership_sha256,
                field_name='expected missing membership SHA-256',
            )
            normalized_expected_fallback_count = (
                _normalize_expected_nonnegative_count(
                    expected_downstream_fallback_manifest_count,
                    field_name=(
                        'expected downstream fallback manifest count'
                    ),
                )
            )
            normalized_expected_fallback_sha256 = (
                _normalize_expected_sha256(
                    expected_downstream_fallback_bundle_sha256,
                    field_name=(
                        'expected downstream fallback bundle SHA-256'
                    ),
                )
            )
        except ValueError as exc:
            return self._blocked_result(
                market_date=normalized_date,
                apply=apply,
                reason='invalid_reviewed_apply_evidence',
                error_message=str(exc),
            )
        if apply and (
            normalized_expected_count is None
            or normalized_expected_sha256 is None
            or normalized_expected_payload_sha256 is None
            or normalized_expected_missing_count is None
            or normalized_expected_missing_sha256 is None
        ):
            return self._blocked_result(
                market_date=normalized_date,
                apply=apply,
                reason='reviewed_apply_evidence_required',
                expected_provider_membership_count=(
                    normalized_expected_count
                ),
                expected_membership_sha256=normalized_expected_sha256,
                expected_provider_payload_sha256=(
                    normalized_expected_payload_sha256
                ),
                expected_missing_bar_count=normalized_expected_missing_count,
                expected_missing_membership_sha256=(
                    normalized_expected_missing_sha256
                ),
            )
        if apply:
            try:
                core_api_active = self.core_api_active_checker()
            except Exception as exc:  # noqa: BLE001
                return self._blocked_result(
                    market_date=normalized_date,
                    apply=apply,
                    reason='core_api_port_check_failed',
                    core_api_host=CORE_API_HOST,
                    core_api_port=CORE_API_PORT,
                    error_type=type(exc).__name__,
                    error_message=str(exc),
                )
            if not isinstance(core_api_active, bool):
                return self._blocked_result(
                    market_date=normalized_date,
                    apply=apply,
                    reason='core_api_port_check_failed',
                    core_api_host=CORE_API_HOST,
                    core_api_port=CORE_API_PORT,
                    error_type='TypeError',
                    error_message='Core API active checker must return bool',
                )
            if core_api_active:
                return self._blocked_result(
                    market_date=normalized_date,
                    apply=apply,
                    reason='core_api_port_active',
                    core_api_host=CORE_API_HOST,
                    core_api_port=CORE_API_PORT,
                )
            validate_finalized_krx_write_date(
                normalized_date,
                operation='missing KRX bar repair',
            )

        lock_dates = (normalized_date,)
        if apply or self.downstream_membership_bundle_preparer is not None:
            lock_dates = tuple(
                sorted(
                    {
                        _normalize_market_date(item)
                        for item in self.rebuild_date_loader(
                            from_date=normalized_date
                        )
                    }
                )
            )
            if not lock_dates or normalized_date not in lock_dates:
                return self._blocked_result(
                    market_date=normalized_date,
                    apply=apply,
                    reason='downstream_rebuild_lock_scope_unproven',
                    required_lock_dates=[
                        item.isoformat() for item in lock_dates
                    ],
                )
        if (
            apply
            and self.downstream_membership_bundle_preparer is not None
            and (
                normalized_expected_fallback_count is None
                or normalized_expected_fallback_sha256 is None
            )
        ):
            return self._blocked_result(
                market_date=normalized_date,
                apply=apply,
                reason='reviewed_downstream_fallback_evidence_required',
                expected_downstream_fallback_manifest_count=(
                    normalized_expected_fallback_count
                ),
                expected_downstream_fallback_bundle_sha256=(
                    normalized_expected_fallback_sha256
                ),
            )

        with self.lock_set_factory(
            market='KRX',
            trade_dates=lock_dates,
            source=MISSING_BAR_REPAIR_LOCK_SOURCE,
            exclusive_admission=apply,
        ) as (locks_acquired, blocked_trade_date):
            if not locks_acquired:
                return self._blocked_result(
                    market_date=normalized_date,
                    apply=apply,
                    reason='advisory_lock_unavailable',
                    blocked_trade_date=blocked_trade_date,
                    required_lock_dates=[
                        item.isoformat() for item in lock_dates
                    ],
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
            market_by_symbol: dict[str, str] = {}
            market_symbol_counts: dict[str, int] = {}
            skipped_ohlcv_codes: list[dict[str, str]] = []
            skipped_fundamental_codes: list[dict[str, str]] = []
            fundamental_unavailable_markets: list[str] = []
            fundamental_missing_symbols: list[str] = []
            required_fundamental_missing_symbols: list[str] = []
            fundamental_extra_symbols: list[str] = []
            symbol_fundamental_fallback_symbols: list[str] = []
            symbol_fundamental_unavailable_symbols: list[str] = []

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
                        provider_skipped_ohlcv_code_sha256=(
                            _skipped_provider_codes_sha256(
                                skipped_ohlcv_codes
                            )
                        ),
                        provider_skipped_ohlcv_code_preview=(
                            _skipped_code_preview(skipped_ohlcv_codes)
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
                market_by_symbol.update(
                    {symbol: market for symbol in market_symbols}
                )

                try:
                    fundamentals_frame = self.market_fundamental_loader(
                        compact_date,
                        market,
                    )
                except Exception as exc:  # noqa: BLE001
                    if _is_optional_market_empty_fundamental_error(
                        exc,
                        market=market,
                    ):
                        logger.info(
                            'PyKRX publishes no market-wide fundamentals for '
                            '%s %s; retaining strict OHLCV membership with '
                            'nullable valuation fields',
                            market,
                            iso_date,
                        )
                        fundamentals = {}
                        market_skipped_fundamentals = []
                        fundamental_unavailable_markets.append(market)
                    else:
                        logger.warning(
                            'PyKRX missing-bar fundamental load failed for '
                            '%s %s: %s',
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
                else:
                    try:
                        fundamentals, market_skipped_fundamentals = (
                            _extract_market_fundamentals(
                                fundamentals_frame,
                                market=market,
                            )
                        )
                    except Exception as exc:  # noqa: BLE001
                        logger.warning(
                            'PyKRX missing-bar fundamental validation failed '
                            'for %s %s: %s',
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

            try:
                excluded_ohlcv_codes = _normalize_excluded_provider_codes(
                    skipped_ohlcv_codes,
                    payload_name='OHLCV',
                )
                excluded_fundamental_codes = (
                    _normalize_excluded_provider_codes(
                        skipped_fundamental_codes,
                        payload_name='fundamental',
                    )
                )
            except ValueError as exc:
                return self._blocked_result(
                    market_date=normalized_date,
                    apply=apply,
                    reason='invalid_excluded_provider_code',
                    error_message=str(exc),
                    provider_skipped_ohlcv_code_preview=(
                        _skipped_code_preview(skipped_ohlcv_codes)
                    ),
                    provider_skipped_fundamental_code_preview=(
                        _skipped_code_preview(skipped_fundamental_codes)
                    ),
                )

            membership_sha256 = market_membership_sha256(
                [bar.symbol for bar in all_bars]
            )
            provider_membership_count = len(all_bars)
            historical_symbol_identities: Mapping[
                str,
                Mapping[str, object],
            ] = {}
            if self.historical_symbol_identity_preparer is not None:
                try:
                    historical_symbol_identities = (
                        self.historical_symbol_identity_preparer(
                            bars=all_bars,
                            market_date=normalized_date,
                        )
                    )
                except Exception as exc:  # noqa: BLE001
                    return self._blocked_result(
                        market_date=normalized_date,
                        apply=apply,
                        reason=(
                            'historical_symbol_identity_resolution_failed'
                        ),
                        error_type=type(exc).__name__,
                        error_message=str(exc),
                        membership_sha256=membership_sha256,
                        provider_membership_count=provider_membership_count,
                    )
            excluded_ohlcv_code_sha256 = _skipped_provider_codes_sha256(
                excluded_ohlcv_codes
            )
            if (
                normalized_expected_count is not None
                and normalized_expected_count != provider_membership_count
            ):
                return self._blocked_result(
                    market_date=normalized_date,
                    apply=apply,
                    reason='provider_membership_count_mismatch',
                    provider_membership_count=provider_membership_count,
                    membership_sha256=membership_sha256,
                    expected_provider_membership_count=(
                        normalized_expected_count
                    ),
                    expected_membership_sha256=(
                        normalized_expected_sha256
                    ),
                    market_symbol_counts=market_symbol_counts,
                )
            if (
                normalized_expected_sha256 is not None
                and normalized_expected_sha256 != membership_sha256
            ):
                return self._blocked_result(
                    market_date=normalized_date,
                    apply=apply,
                    reason='provider_membership_sha256_mismatch',
                    provider_membership_count=provider_membership_count,
                    membership_sha256=membership_sha256,
                    expected_provider_membership_count=(
                        normalized_expected_count
                    ),
                    expected_membership_sha256=(
                        normalized_expected_sha256
                    ),
                    market_symbol_counts=market_symbol_counts,
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
                    if (
                        market_by_symbol.get(symbol)
                        not in FUNDAMENTAL_OPTIONAL_MARKETS
                        and security_group_by_symbol.get(symbol)
                        not in FUNDAMENTAL_OPTIONAL_SECURITY_GROUPS
                    )
                )
            if (
                required_fundamental_missing_symbols
                and self.symbol_fundamental_loader is not None
            ):
                bar_position_by_symbol = {
                    bar.symbol: index for index, bar in enumerate(all_bars)
                }
                unresolved_required: list[str] = []
                for symbol in required_fundamental_missing_symbols:
                    try:
                        fallback_frame = self.symbol_fundamental_loader(
                            compact_date,
                            symbol,
                        )
                        fallback_values = _extract_symbol_fundamental(
                            fallback_frame,
                            symbol=symbol,
                        )
                    except Exception as exc:  # noqa: BLE001
                        return self._blocked_result(
                            market_date=normalized_date,
                            apply=apply,
                            reason='symbol_fundamental_fallback_failed',
                            failed_symbol=symbol,
                            error_type=type(exc).__name__,
                            error_message=str(exc),
                            membership_basis='exact_market_date_ohlcv',
                            membership_sha256=membership_sha256,
                        )
                    if fallback_values is None:
                        symbol_fundamental_unavailable_symbols.append(symbol)
                        continue
                    position = bar_position_by_symbol[symbol]
                    all_bars[position] = replace(
                        all_bars[position],
                        **fallback_values,
                    )
                    symbol_fundamental_fallback_symbols.append(symbol)
                    try:
                        fundamental_missing_symbols.remove(symbol)
                    except ValueError:
                        unresolved_required.append(symbol)
                required_fundamental_missing_symbols = unresolved_required
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
            provider_payload_sha256 = _provider_payload_sha256(
                market_date=normalized_date,
                bars=all_bars,
                historical_symbol_identities=(
                    historical_symbol_identities
                ),
                excluded_ohlcv_codes=excluded_ohlcv_codes,
                excluded_fundamental_codes=excluded_fundamental_codes,
            )
            if (
                normalized_expected_payload_sha256 is not None
                and normalized_expected_payload_sha256
                != provider_payload_sha256
            ):
                return self._blocked_result(
                    market_date=normalized_date,
                    apply=apply,
                    reason='provider_payload_sha256_mismatch',
                    provider_membership_count=provider_membership_count,
                    membership_sha256=membership_sha256,
                    provider_payload_sha256=provider_payload_sha256,
                    expected_provider_payload_sha256=(
                        normalized_expected_payload_sha256
                    ),
                    market_symbol_counts=market_symbol_counts,
                )
            downstream_fallback_bundle = {
                'schema_version': (
                    DOWNSTREAM_MEMBERSHIP_BUNDLE_SCHEMA_VERSION
                ),
                'manifest_count': 0,
                'bundle_sha256': _downstream_fallback_bundle_sha256({}),
                'manifests': {},
            }
            if self.downstream_membership_bundle_preparer is not None:
                try:
                    downstream_fallback_bundle = (
                        self.downstream_membership_bundle_preparer(
                            target_date=normalized_date,
                            required_rebuild_dates=lock_dates,
                            trade_date_resolver=self.trade_date_resolver,
                            market_ohlcv_loader=self.market_ohlcv_loader,
                            minimum_market_membership=(
                                self.minimum_market_membership
                            ),
                        )
                    )
                    fallback_manifests = downstream_fallback_bundle.get(
                        'manifests'
                    )
                    if not isinstance(fallback_manifests, Mapping):
                        raise ValueError(
                            'downstream fallback manifests must be an object'
                        )
                    fallback_count = len(fallback_manifests)
                    fallback_sha256 = _downstream_fallback_bundle_sha256(
                        fallback_manifests
                    )
                    if (
                        downstream_fallback_bundle.get('schema_version')
                        != DOWNSTREAM_MEMBERSHIP_BUNDLE_SCHEMA_VERSION
                        or downstream_fallback_bundle.get('manifest_count')
                        != fallback_count
                        or downstream_fallback_bundle.get('bundle_sha256')
                        != fallback_sha256
                    ):
                        raise ValueError(
                            'downstream fallback bundle metadata is invalid'
                        )
                except Exception as exc:  # noqa: BLE001
                    return self._blocked_result(
                        market_date=normalized_date,
                        apply=apply,
                        reason='downstream_membership_preparation_failed',
                        error_type=type(exc).__name__,
                        error_message=str(exc),
                    )
            fallback_manifests = downstream_fallback_bundle['manifests']
            fallback_count = downstream_fallback_bundle['manifest_count']
            fallback_sha256 = downstream_fallback_bundle['bundle_sha256']
            if (
                normalized_expected_fallback_count is not None
                and normalized_expected_fallback_count != fallback_count
            ):
                return self._blocked_result(
                    market_date=normalized_date,
                    apply=apply,
                    reason='downstream_fallback_manifest_count_mismatch',
                    downstream_fallback_manifest_count=fallback_count,
                    downstream_fallback_bundle_sha256=fallback_sha256,
                    expected_downstream_fallback_manifest_count=(
                        normalized_expected_fallback_count
                    ),
                    expected_downstream_fallback_bundle_sha256=(
                        normalized_expected_fallback_sha256
                    ),
                )
            if (
                normalized_expected_fallback_sha256 is not None
                and normalized_expected_fallback_sha256 != fallback_sha256
            ):
                return self._blocked_result(
                    market_date=normalized_date,
                    apply=apply,
                    reason='downstream_fallback_bundle_sha256_mismatch',
                    downstream_fallback_manifest_count=fallback_count,
                    downstream_fallback_bundle_sha256=fallback_sha256,
                    expected_downstream_fallback_manifest_count=(
                        normalized_expected_fallback_count
                    ),
                    expected_downstream_fallback_bundle_sha256=(
                        normalized_expected_fallback_sha256
                    ),
                )
            try:
                database_result = self.database_repairer(
                    market_date=normalized_date,
                    bars=all_bars,
                    apply=apply,
                    expected_provider_membership_count=(
                        normalized_expected_count
                    ),
                    expected_membership_sha256=(
                        normalized_expected_sha256
                    ),
                    expected_provider_payload_sha256=(
                        normalized_expected_payload_sha256
                    ),
                    expected_missing_bar_count=(
                        normalized_expected_missing_count
                    ),
                    expected_missing_membership_sha256=(
                        normalized_expected_missing_sha256
                    ),
                    required_rebuild_trade_dates=(
                        lock_dates if apply else None
                    ),
                    historical_symbol_identities=(
                        historical_symbol_identities
                    ),
                    excluded_ohlcv_codes=excluded_ohlcv_codes,
                    excluded_fundamental_codes=(
                        excluded_fundamental_codes
                    ),
                    prepared_downstream_membership_manifests=(
                        fallback_manifests
                    ),
                    expected_downstream_fallback_manifest_count=(
                        normalized_expected_fallback_count
                    ),
                    expected_downstream_fallback_bundle_sha256=(
                        normalized_expected_fallback_sha256
                    ),
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
                'provider_payload_sha256': provider_payload_sha256,
                'provider_membership_count': provider_membership_count,
                'expected_provider_membership_count': (
                    normalized_expected_count
                ),
                'expected_membership_sha256': normalized_expected_sha256,
                'expected_provider_payload_sha256': (
                    normalized_expected_payload_sha256
                ),
                'expected_missing_bar_count': normalized_expected_missing_count,
                'expected_missing_membership_sha256': (
                    normalized_expected_missing_sha256
                ),
                'downstream_fallback_manifest_count': fallback_count,
                'downstream_fallback_bundle_sha256': fallback_sha256,
                'expected_downstream_fallback_manifest_count': (
                    normalized_expected_fallback_count
                ),
                'expected_downstream_fallback_bundle_sha256': (
                    normalized_expected_fallback_sha256
                ),
                'market_symbol_counts': market_symbol_counts,
                'provider_excluded_ohlcv_code_count': len(
                    excluded_ohlcv_codes
                ),
                'provider_excluded_ohlcv_code_sha256': (
                    excluded_ohlcv_code_sha256
                ),
                'provider_excluded_ohlcv_code_preview': _skipped_code_preview(
                    excluded_ohlcv_codes
                ),
                'provider_excluded_fundamental_code_count': len(
                    excluded_fundamental_codes
                ),
                'provider_excluded_fundamental_code_sha256': (
                    _skipped_provider_codes_sha256(
                        excluded_fundamental_codes
                    )
                ),
                'provider_excluded_fundamental_code_preview': (
                    _skipped_code_preview(excluded_fundamental_codes)
                ),
                'provider_fundamental_unavailable_markets': sorted(
                    fundamental_unavailable_markets
                ),
                'provider_missing_fundamental_count': len(
                    fundamental_missing_symbols
                ),
                'provider_missing_fundamental_symbols': (
                    sorted(fundamental_missing_symbols)[:20]
                ),
                'provider_symbol_fundamental_fallback_count': len(
                    symbol_fundamental_fallback_symbols
                ),
                'provider_symbol_fundamental_fallback_symbols': sorted(
                    symbol_fundamental_fallback_symbols
                )[:20],
                'provider_symbol_fundamental_unavailable_count': len(
                    symbol_fundamental_unavailable_symbols
                ),
                'provider_symbol_fundamental_unavailable_symbols': sorted(
                    symbol_fundamental_unavailable_symbols
                )[:20],
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
            'Validate an exact KOSPI+KOSDAQ+KONEX session and insert only '
            'missing daily price bars. Existing rows are never updated.'
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
    parser.add_argument(
        '--expected-provider-membership-count',
        type=int,
        help=(
            'Required with --apply: provider membership count copied from '
            'the reviewed dry-run output.'
        ),
    )
    parser.add_argument(
        '--expected-membership-sha256',
        help=(
            'Required with --apply: exact membership SHA-256 copied from '
            'the reviewed dry-run output.'
        ),
    )
    parser.add_argument(
        '--expected-provider-payload-sha256',
        help=(
            'Required with --apply: canonical full provider payload SHA-256 '
            'copied from the reviewed dry-run output.'
        ),
    )
    parser.add_argument(
        '--expected-missing-bar-count',
        type=int,
        help=(
            'Required with --apply: exact full missing-bar count copied from '
            'the reviewed dry-run output.'
        ),
    )
    parser.add_argument(
        '--expected-missing-membership-sha256',
        help=(
            'Required with --apply: exact full missing-symbol SHA-256 copied '
            'from the reviewed dry-run output.'
        ),
    )
    parser.add_argument(
        '--expected-downstream-fallback-manifest-count',
        type=int,
        help=(
            'Required with --apply: exact live downstream proof-gap count '
            'copied from the reviewed dry-run output.'
        ),
    )
    parser.add_argument(
        '--expected-downstream-fallback-bundle-sha256',
        help=(
            'Required with --apply: canonical downstream proof-gap bundle '
            'SHA-256 copied from the reviewed dry-run output.'
        ),
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
            expected_provider_membership_count=(
                args.expected_provider_membership_count
            ),
            expected_membership_sha256=args.expected_membership_sha256,
            expected_provider_payload_sha256=(
                args.expected_provider_payload_sha256
            ),
            expected_missing_bar_count=args.expected_missing_bar_count,
            expected_missing_membership_sha256=(
                args.expected_missing_membership_sha256
            ),
            expected_downstream_fallback_manifest_count=(
                args.expected_downstream_fallback_manifest_count
            ),
            expected_downstream_fallback_bundle_sha256=(
                args.expected_downstream_fallback_bundle_sha256
            ),
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
