from __future__ import annotations

import io
import re
import zipfile
from dataclasses import dataclass
from datetime import datetime
from typing import Iterable, Optional

from app.core.audit_logging import download_bytes_with_audit
from app.eod.master_state import master_refresh_state
from app.market_data_store import upsert_master_symbols
from app.market.clock import get_market_state
from app.repositories.eod_run_repository import advisory_eod_collection_lock

KOSPI_MASTER_URL = 'https://new.real.download.dws.co.kr/common/master/kospi_code.mst.zip'
KOSDAQ_MASTER_URL = 'https://new.real.download.dws.co.kr/common/master/kosdaq_code.mst.zip'
KOSPI_TRAILER_WIDTH = 227
KOSDAQ_TRAILER_WIDTH = 221
STOCK_SECURITY_GROUP_CODES = frozenset({'ST', 'RT', 'IF', 'MF', 'DR', 'FS'})
# Deliberately below the live baseline (~916/~1,766) while still rejecting
# truncated or format-shifted master files before they can deactivate symbols.
MIN_MASTER_STOCK_COUNTS = {'KOSPI': 700, 'KOSDAQ': 1_200}
ISIN_PATTERN = re.compile(r'^[A-Z]{2}[A-Z0-9]{9}[0-9]$')


@dataclass
class MasterSymbol:
    symbol: str
    standard_code: str
    name_ko: str
    market: str
    security_group: str

    @property
    def source_payload(self) -> dict[str, str]:
        return {'security_group': self.security_group}


def _download_zip_bytes(url: str) -> bytes:
    return download_bytes_with_audit(service='kis_master', url=url, timeout=30)


def _read_zip_member_bytes(payload: bytes, member_suffix: str) -> bytes:
    with zipfile.ZipFile(io.BytesIO(payload)) as zf:
        target = next(name for name in zf.namelist() if name.endswith(member_suffix))
        with zf.open(target) as fp:
            return fp.read()


def _is_valid_isin(value: str) -> bool:
    """Return whether ``value`` is a structurally and checksum-valid ISIN."""

    if not ISIN_PATTERN.fullmatch(value):
        return False
    expanded = ''.join(
        str(ord(character) - ord('A') + 10)
        if character.isalpha()
        else character
        for character in value
    )
    checksum = 0
    for index, character in enumerate(reversed(expanded)):
        digit = int(character)
        if index % 2 == 1:
            digit *= 2
        checksum += (digit // 10) + (digit % 10)
    return checksum % 10 == 0


def _parse_master_lines(raw_bytes: bytes, *, trailer_width: int, market: str) -> list[MasterSymbol]:
    rows: list[MasterSymbol] = []
    for line_number, line in enumerate(raw_bytes.splitlines(), start=1):
        if not line.strip():
            continue
        if len(line) <= trailer_width:
            raise ValueError(
                f'{market} master line {line_number} is shorter than its trailer'
            )
        head = line[:-trailer_width]
        trailer = line[-trailer_width:]
        try:
            symbol = head[0:9].decode('ascii').strip()
            standard_code = head[9:21].decode('ascii').strip().upper()
            name_ko = head[21:].decode('cp949').strip()
            security_group = trailer[0:2].decode('ascii').strip().upper()
        except UnicodeDecodeError as exc:
            raise ValueError(
                f'{market} master line {line_number} contains invalid encoded fields'
            ) from exc
        if security_group not in STOCK_SECURITY_GROUP_CODES:
            continue
        # Alphanumeric and non-six-character short codes are intentional
        # non-common-share classes outside this system's numeric stock scope.
        if len(symbol) != 6 or not symbol.isdigit():
            continue
        if not _is_valid_isin(standard_code) or not name_ko:
            raise ValueError(
                f'{market} master line {line_number} has invalid stock identity fields'
            )
        rows.append(
            MasterSymbol(
                symbol=symbol,
                standard_code=standard_code,
                name_ko=name_ko,
                market=market,
                security_group=security_group,
            )
        )
    return rows


def load_kospi_master() -> list[MasterSymbol]:
    payload = _download_zip_bytes(KOSPI_MASTER_URL)
    raw = _read_zip_member_bytes(payload, 'kospi_code.mst')
    return _parse_master_lines(raw, trailer_width=KOSPI_TRAILER_WIDTH, market='KOSPI')


def load_kosdaq_master() -> list[MasterSymbol]:
    payload = _download_zip_bytes(KOSDAQ_MASTER_URL)
    raw = _read_zip_member_bytes(payload, 'kosdaq_code.mst')
    return _parse_master_lines(raw, trailer_width=KOSDAQ_TRAILER_WIDTH, market='KOSDAQ')


def load_all_domestic_masters() -> list[MasterSymbol]:
    return [*load_kospi_master(), *load_kosdaq_master()]


def _validate_master_universe(symbols: list[MasterSymbol]) -> None:
    counts = {market: 0 for market in MIN_MASTER_STOCK_COUNTS}
    symbol_counts: dict[str, int] = {}
    standard_code_counts: dict[str, int] = {}
    for item in symbols:
        if item.market in counts:
            counts[item.market] += 1
        symbol_counts[item.symbol] = symbol_counts.get(item.symbol, 0) + 1
        if not _is_valid_isin(item.standard_code):
            raise ValueError(
                'Invalid KIS domestic master snapshot: invalid standard code '
                f'{item.standard_code!r} for {item.symbol}'
            )
        standard_code_counts[item.standard_code] = (
            standard_code_counts.get(item.standard_code, 0) + 1
        )

    errors = [
        f'{market} master count {counts[market]} is below safe minimum {minimum}'
        for market, minimum in MIN_MASTER_STOCK_COUNTS.items()
        if counts[market] < minimum
    ]
    duplicates = sorted(symbol for symbol, count in symbol_counts.items() if count > 1)
    if duplicates:
        errors.append(
            'duplicate symbols: '
            + ','.join(duplicates[:10])
            + ('...' if len(duplicates) > 10 else '')
        )
    duplicate_standard_codes = sorted(
        standard_code
        for standard_code, count in standard_code_counts.items()
        if count > 1
    )
    if duplicate_standard_codes:
        errors.append(
            'duplicate standard codes: '
            + ','.join(duplicate_standard_codes[:10])
            + ('...' if len(duplicate_standard_codes) > 10 else '')
        )
    if errors:
        raise ValueError('Invalid KIS domestic master snapshot: ' + '; '.join(errors))


def refresh_master_if_due(now: Optional[datetime] = None, *, force: bool = False) -> list[MasterSymbol]:
    market = get_market_state(now)
    market_date = market.server_time[:10]

    if not force and master_refresh_state.market_date == market_date and master_refresh_state.last_status == 'completed':
        return master_refresh_state.symbols

    with advisory_eod_collection_lock(
        market='KRX',
        trade_date=market_date,
        source='kis_master',
    ) as lock_acquired:
        if not lock_acquired:
            raise RuntimeError('KIS master refresh lock is already held')
        if (
            not force
            and master_refresh_state.market_date == market_date
            and master_refresh_state.last_status == 'completed'
        ):
            return master_refresh_state.symbols

        master_refresh_state.start(market_date)
        try:
            symbols = load_all_domestic_masters()
            _validate_master_universe(symbols)
            # Do not publish an in-memory "completed" snapshot until the durable
            # symbol universe is in sync.  Otherwise a transient database failure
            # suppresses every same-day retry while readers observe a state that
            # was never persisted.
            upsert_master_symbols(symbols)
            master_refresh_state.complete(symbols)
            return symbols
        except Exception as exc:
            master_refresh_state.fail(exc)
            raise


def get_master_symbols(now: Optional[datetime] = None) -> list[MasterSymbol]:
    market = get_market_state(now)
    market_date = market.server_time[:10]
    if master_refresh_state.market_date == market_date and master_refresh_state.last_status == 'completed':
        return master_refresh_state.symbols
    return refresh_master_if_due(now)


def find_master_symbol(symbol: str, now: Optional[datetime] = None) -> Optional[MasterSymbol]:
    for item in get_master_symbols(now):
        if item.symbol == symbol:
            return item
    return None


def iter_all_domestic_masters() -> Iterable[MasterSymbol]:
    return iter(get_master_symbols())
