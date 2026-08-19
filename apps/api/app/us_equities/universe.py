from __future__ import annotations

import csv
from dataclasses import dataclass
from io import StringIO
import re
from typing import Iterable


_OTHER_EXCHANGE_CODES = {
    'A': 'NYSE_AMERICAN',
    'N': 'NYSE',
    'P': 'NYSE_ARCA',
    'Z': 'CBOE_BZX',
    'V': 'IEX',
}
_SUPPORTED_PRIMARY_EXCHANGES = frozenset({'NASDAQ', 'NYSE', 'NYSE_AMERICAN'})
_EXCHANGE_PRIORITY = {'NASDAQ': 0, 'NYSE': 1, 'NYSE_AMERICAN': 2}
_SECURITY_TYPE_PATTERNS = (
    ('warrant', re.compile(r'\bwarrants?\b', re.IGNORECASE)),
    ('right', re.compile(r'\brights?\b', re.IGNORECASE)),
    ('unit', re.compile(r'\bunits?\b', re.IGNORECASE)),
    ('preferred', re.compile(r'\bpreferred\b|\bpfd\b', re.IGNORECASE)),
    ('note', re.compile(r'\bnotes?\b|\bdebentures?\b', re.IGNORECASE)),
)
_ADR_PATTERN = re.compile(r'\bADR\b|American Depositary|Depositary Shares?', re.IGNORECASE)


@dataclass(frozen=True, slots=True)
class USListedSecurity:
    symbol: str
    name: str
    exchange: str
    is_etf: bool
    is_test_issue: bool
    financial_status: str | None
    round_lot_size: int | None = None
    source: str = 'nasdaq_trader'


@dataclass(frozen=True, slots=True)
class USUniverseFilterResult:
    eligible: tuple[USListedSecurity, ...]
    exclusions: dict[str, tuple[str, ...]]


def _parse_bool_flag(value: str | None) -> bool:
    return str(value or '').strip().upper() == 'Y'


def _parse_round_lot(value: str | None) -> int | None:
    normalized = str(value or '').strip()
    if not normalized:
        return None
    try:
        return int(normalized)
    except ValueError:
        return None


def _records(payload: str) -> Iterable[dict[str, str]]:
    reader = csv.DictReader(StringIO(payload), delimiter='|')
    for row in reader:
        first_value = next(iter(row.values()), '')
        if str(first_value or '').startswith('File Creation Time:'):
            continue
        yield {str(key): str(value or '').strip() for key, value in row.items() if key is not None}


def parse_nasdaq_listed(payload: str) -> list[USListedSecurity]:
    securities: list[USListedSecurity] = []
    for row in _records(payload):
        symbol = row.get('Symbol', '').strip().upper()
        name = row.get('Security Name', '').strip()
        if not symbol or not name:
            continue
        securities.append(
            USListedSecurity(
                symbol=symbol,
                name=name,
                exchange='NASDAQ',
                is_etf=_parse_bool_flag(row.get('ETF')),
                is_test_issue=_parse_bool_flag(row.get('Test Issue')),
                financial_status=(row.get('Financial Status') or None),
                round_lot_size=_parse_round_lot(row.get('Round Lot Size')),
            )
        )
    return securities


def parse_other_listed(payload: str) -> list[USListedSecurity]:
    securities: list[USListedSecurity] = []
    for row in _records(payload):
        symbol = (row.get('ACT Symbol') or row.get('NASDAQ Symbol') or '').strip().upper()
        name = row.get('Security Name', '').strip()
        if not symbol or not name:
            continue
        exchange_code = row.get('Exchange', '').strip().upper()
        securities.append(
            USListedSecurity(
                symbol=symbol,
                name=name,
                exchange=_OTHER_EXCHANGE_CODES.get(exchange_code, f'UNKNOWN_{exchange_code or "EMPTY"}'),
                is_etf=_parse_bool_flag(row.get('ETF')),
                is_test_issue=_parse_bool_flag(row.get('Test Issue')),
                financial_status=None,
                round_lot_size=_parse_round_lot(row.get('Round Lot Size')),
            )
        )
    return securities


def _exclusion_reasons(security: USListedSecurity) -> tuple[str, ...]:
    reasons: list[str] = []
    if security.exchange not in _SUPPORTED_PRIMARY_EXCHANGES:
        reasons.append(f'unsupported_exchange_{security.exchange.lower()}')
    if security.is_test_issue:
        reasons.append('test_issue')
    if security.is_etf:
        reasons.append('etf')
    financial_status = (security.financial_status or '').strip().upper()
    if financial_status and financial_status != 'N':
        reasons.append(f'financial_status_{financial_status}')
    if _ADR_PATTERN.search(security.name):
        reasons.append('adr')
    for label, pattern in _SECURITY_TYPE_PATTERNS:
        if pattern.search(security.name):
            reasons.append(f'unsupported_security_type_{label}')
            break
    return tuple(reasons)


def filter_us_equity_universe(securities: Iterable[USListedSecurity]) -> USUniverseFilterResult:
    ordered = sorted(
        securities,
        key=lambda item: (
            item.symbol,
            _EXCHANGE_PRIORITY.get(item.exchange, 100),
            item.exchange,
            item.name,
        ),
    )
    selected: dict[str, USListedSecurity] = {}
    exclusions: dict[str, tuple[str, ...]] = {}
    for security in ordered:
        if security.symbol in selected or security.symbol in exclusions:
            continue
        reasons = _exclusion_reasons(security)
        if reasons:
            exclusions[security.symbol] = reasons
        else:
            selected[security.symbol] = security
    return USUniverseFilterResult(
        eligible=tuple(selected[symbol] for symbol in sorted(selected)),
        exclusions={symbol: exclusions[symbol] for symbol in sorted(exclusions)},
    )
