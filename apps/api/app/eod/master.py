from __future__ import annotations

import io
import logging
import zipfile
from dataclasses import dataclass
from datetime import datetime
from typing import Iterable, Optional

from app.core.audit_logging import download_bytes_with_audit
from app.eod.master_state import master_refresh_state
from app.market_data_store import upsert_master_symbols
from app.market.clock import get_market_state

KOSPI_MASTER_URL = 'https://new.real.download.dws.co.kr/common/master/kospi_code.mst.zip'
KOSDAQ_MASTER_URL = 'https://new.real.download.dws.co.kr/common/master/kosdaq_code.mst.zip'
logger = logging.getLogger(__name__)


@dataclass
class MasterSymbol:
    symbol: str
    standard_code: str
    name_ko: str
    market: str


def _download_zip_bytes(url: str) -> bytes:
    return download_bytes_with_audit(service='kis_master', url=url, timeout=30)


def _read_zip_member_bytes(payload: bytes, member_suffix: str) -> bytes:
    with zipfile.ZipFile(io.BytesIO(payload)) as zf:
        target = next(name for name in zf.namelist() if name.endswith(member_suffix))
        with zf.open(target) as fp:
            return fp.read()


def _parse_master_lines(raw_bytes: bytes, *, trailer_width: int, market: str) -> list[MasterSymbol]:
    rows: list[MasterSymbol] = []
    text = raw_bytes.decode('cp949', errors='ignore')
    for line in text.splitlines():
        if not line.strip():
            continue
        head = line[:-trailer_width] if len(line) > trailer_width else line
        symbol = head[0:9].strip()
        standard_code = head[9:21].strip()
        name_ko = head[21:].strip()
        if not symbol or not name_ko:
            continue
        rows.append(MasterSymbol(symbol=symbol, standard_code=standard_code, name_ko=name_ko, market=market))
    return rows


def load_kospi_master() -> list[MasterSymbol]:
    payload = _download_zip_bytes(KOSPI_MASTER_URL)
    raw = _read_zip_member_bytes(payload, 'kospi_code.mst')
    return _parse_master_lines(raw, trailer_width=228, market='KOSPI')


def load_kosdaq_master() -> list[MasterSymbol]:
    payload = _download_zip_bytes(KOSDAQ_MASTER_URL)
    raw = _read_zip_member_bytes(payload, 'kosdaq_code.mst')
    return _parse_master_lines(raw, trailer_width=222, market='KOSDAQ')


def load_all_domestic_masters() -> list[MasterSymbol]:
    return [*load_kospi_master(), *load_kosdaq_master()]


def refresh_master_if_due(now: Optional[datetime] = None, *, force: bool = False) -> list[MasterSymbol]:
    market = get_market_state(now)
    market_date = market.server_time[:10]

    if not force and master_refresh_state.market_date == market_date and master_refresh_state.last_status == 'completed':
        return master_refresh_state.symbols

    master_refresh_state.start(market_date)
    try:
        symbols = load_all_domestic_masters()
        master_refresh_state.complete(symbols)
        try:
            upsert_master_symbols(symbols)
        except Exception as exc:
            logger.warning('Symbol master persistence failed: %s', exc)
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
