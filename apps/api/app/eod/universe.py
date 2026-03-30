from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

from app.scanner.state import scanner_state


@dataclass
class UniverseSymbol:
    symbol: str
    market: str = 'J'


class UniverseService:
    def get_symbols(self) -> list[UniverseSymbol]:
        # TODO: replace scanner-state fallback with full KRX/KOSDAQ/KONEX master universe loader.
        return [UniverseSymbol(symbol=symbol) for symbol in scanner_state.tracked_symbols]

    def get_symbol_codes(self) -> list[str]:
        return [item.symbol for item in self.get_symbols()]

    def iter_symbols(self) -> Iterable[UniverseSymbol]:
        return iter(self.get_symbols())


universe_service = UniverseService()
