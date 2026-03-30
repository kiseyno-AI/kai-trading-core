from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional

from app.brokers.kis.client import KISClient


def _extract_company_name(raw: dict, symbol: str) -> Optional[str]:
    output = raw.get('output', {}) if isinstance(raw, dict) else {}
    for key in (
        'prdt_abrv_name',
        'prdt_name',
        'hts_kor_isnm',
        'prdt_eng_abrv_name',
        'prdt_eng_name',
        'stck_shrn_iscd_name',
        'hts_shrn_isnm',
    ):
        value = output.get(key)
        if isinstance(value, str) and value.strip() and value.strip() != symbol:
            return value.strip()
    return None


@dataclass
class SymbolNameCache:
    names: dict[str, str] = field(default_factory=dict)

    def get(self, symbol: str) -> Optional[str]:
        return self.names.get(symbol)

    def set(self, symbol: str, company_name: str) -> None:
        self.names[symbol] = company_name


symbol_name_cache = SymbolNameCache()


class SymbolNameService:
    def __init__(self) -> None:
        self.client = KISClient()

    async def resolve(self, symbol: str) -> str:
        cached = symbol_name_cache.get(symbol)
        if cached:
            return cached

        for fetcher in (
            self.client.fetch_domestic_stock_info,
            self.client.fetch_domestic_product_info,
            self.client.fetch_domestic_quote,
        ):
            try:
                raw = await fetcher(symbol)
            except Exception:
                continue

            company_name = _extract_company_name(raw, symbol)
            if company_name:
                symbol_name_cache.set(symbol, company_name)
                return company_name

        return symbol
