from __future__ import annotations

from dataclasses import dataclass
import re
from typing import Iterable

from app.integrations.kis_overseas_master.client import KISOverseasMasterSecurity
from app.integrations.sec_edgar.client import SECCompanyTicker
from app.us_equities.universe import USListedSecurity, filter_us_equity_universe


_CLASS_SEPARATOR_PATTERN = re.compile(r'[-/]')


def canonical_us_symbol(value: str) -> str:
    normalized = value.strip().upper()
    if not normalized:
        raise ValueError('US symbol is required')
    return _CLASS_SEPARATOR_PATTERN.sub('.', normalized)


@dataclass(frozen=True, slots=True)
class USIntegratedSymbol:
    symbol: str
    kis_symbol: str
    realtime_symbol: str
    name: str
    korean_name: str | None
    exchange: str
    currency: str
    cik: str | None
    sec_mapping_status: str
    decimal_places: int | None
    bid_order_size: str
    ask_order_size: str
    industry_code: str | None
    source_payload: dict

    @property
    def market(self) -> str:
        return self.exchange

    @property
    def name_ko(self) -> str:
        return self.korean_name or self.name

    @property
    def name_en(self) -> str:
        return self.name

    @property
    def standard_code(self) -> str:
        return self.kis_symbol

    @property
    def country_code(self) -> str:
        return 'US'

    @property
    def currency_code(self) -> str:
        return self.currency

    @property
    def exchange_code(self) -> str:
        return self.exchange

    @property
    def regulatory_id(self) -> str | None:
        return self.cik


@dataclass(frozen=True, slots=True)
class USIntegratedUniverseResult:
    symbols: tuple[USIntegratedSymbol, ...]
    exclusions: dict[str, tuple[str, ...]]


def build_us_symbol_universe(
    listed_securities: Iterable[USListedSecurity],
    kis_securities: Iterable[KISOverseasMasterSecurity],
    sec_tickers: Iterable[SECCompanyTicker],
) -> USIntegratedUniverseResult:
    listed_result = filter_us_equity_universe(listed_securities)
    kis_by_scope: dict[tuple[str, str], KISOverseasMasterSecurity] = {}
    for item in sorted(kis_securities, key=lambda row: (canonical_us_symbol(row.symbol), row.exchange, row.symbol)):
        kis_by_scope.setdefault((canonical_us_symbol(item.symbol), item.exchange), item)
    sec_by_symbol: dict[str, SECCompanyTicker] = {}
    for item in sorted(sec_tickers, key=lambda row: (canonical_us_symbol(row.ticker), row.cik)):
        sec_by_symbol.setdefault(canonical_us_symbol(item.ticker), item)

    integrated: list[USIntegratedSymbol] = []
    exclusions = dict(listed_result.exclusions)
    for listed in listed_result.eligible:
        canonical_symbol = canonical_us_symbol(listed.symbol)
        kis = kis_by_scope.get((canonical_symbol, listed.exchange))
        if kis is None:
            exclusions[canonical_symbol] = ('not_supported_by_kis',)
            continue
        reasons: list[str] = []
        if kis.security_type != '2':
            reasons.append(f'kis_security_type_{kis.security_type or "missing"}')
        if kis.currency != 'USD':
            reasons.append(f'kis_currency_{kis.currency or "missing"}')
        if kis.is_depositary_receipt:
            reasons.append('kis_depositary_receipt')
        if reasons:
            exclusions[canonical_symbol] = tuple(reasons)
            continue
        sec = sec_by_symbol.get(canonical_symbol)
        integrated.append(
            USIntegratedSymbol(
                symbol=canonical_symbol,
                kis_symbol=kis.symbol,
                realtime_symbol=kis.realtime_symbol,
                name=kis.english_name or listed.name,
                korean_name=kis.korean_name or None,
                exchange=listed.exchange,
                currency='USD',
                cik=sec.cik if sec else None,
                sec_mapping_status='matched' if sec else 'missing',
                decimal_places=kis.decimal_places,
                bid_order_size=kis.bid_order_size,
                ask_order_size=kis.ask_order_size,
                industry_code=kis.industry_code,
                source_payload={
                    'nasdaq_trader_symbol': listed.symbol,
                    'kis_symbol': kis.symbol,
                    'kis_realtime_symbol': kis.realtime_symbol,
                    'sec_ticker': sec.ticker if sec else None,
                    'sec_exchange': sec.exchange if sec else None,
                    'sec_mapping_status': 'matched' if sec else 'missing',
                    'decimal_places': kis.decimal_places,
                    'bid_order_size': kis.bid_order_size,
                    'ask_order_size': kis.ask_order_size,
                },
            )
        )
    return USIntegratedUniverseResult(
        symbols=tuple(sorted(integrated, key=lambda item: item.symbol)),
        exclusions={symbol: exclusions[symbol] for symbol in sorted(exclusions)},
    )
