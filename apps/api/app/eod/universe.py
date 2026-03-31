from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

from app.eod.master import get_master_symbols
from app.scanner.state import scanner_state


@dataclass
class UniverseSymbol:
    symbol: str
    market: str = 'J'
    market_name: str | None = None
    standard_code: str | None = None
    name_ko: str | None = None


class UniverseService:
    def get_symbols(self, *, use_master: bool = True) -> list[UniverseSymbol]:
        if use_master:
            try:
                return [
                    UniverseSymbol(
                        symbol=item.symbol,
                        market='J',
                        market_name=item.market,
                        standard_code=item.standard_code,
                        name_ko=item.name_ko,
                    )
                    for item in get_master_symbols()
                    if item.symbol.isdigit() and len(item.symbol) == 6
                ]
            except Exception:
                pass

        return [UniverseSymbol(symbol=symbol) for symbol in scanner_state.tracked_symbols]

    def get_symbol_codes(self) -> list[str]:
        return [item.symbol for item in self.get_symbols()]

    def iter_symbols(self) -> Iterable[UniverseSymbol]:
        return iter(self.get_symbols())


universe_service = UniverseService()
