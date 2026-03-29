from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime


@dataclass
class QuoteCache:
    items: dict[str, dict] = field(default_factory=dict)

    def get(self, symbol: str) -> dict | None:
        return self.items.get(symbol)

    def set(self, symbol: str, payload: dict) -> dict:
        self.items[symbol] = payload
        return payload

    def batch(self, symbols: list[str]) -> list[dict]:
        return [self.items[s] for s in symbols if s in self.items]


quote_cache = QuoteCache()
