from __future__ import annotations

import io
import zipfile
from dataclasses import dataclass
from typing import Iterable
from urllib.request import urlopen


KOSPI_MASTER_URL = 'https://new.real.download.dws.co.kr/common/master/kospi_code.mst.zip'
KOSDAQ_MASTER_URL = 'https://new.real.download.dws.co.kr/common/master/kosdaq_code.mst.zip'


@dataclass
class MasterSymbol:
    symbol: str
    standard_code: str
    name_ko: str
    market: str


def _download_zip_bytes(url: str) -> bytes:
    with urlopen(url, timeout=30) as response:
        return response.read()


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


def iter_all_domestic_masters() -> Iterable[MasterSymbol]:
    return iter(load_all_domestic_masters())
