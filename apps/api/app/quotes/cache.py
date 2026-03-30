from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone


@dataclass
class QuoteCache:
    items: dict[str, dict] = field(default_factory=dict)
    updated_at: dict[str, str] = field(default_factory=dict)

    def get(self, symbol: str) -> dict | None:
        return self.items.get(symbol)

    def get_updated_at(self, symbol: str) -> str | None:
        return self.updated_at.get(symbol)

    def set(self, symbol: str, payload: dict) -> dict:
        self.items[symbol] = payload
        self.updated_at[symbol] = datetime.now(timezone.utc).isoformat()
        return payload

    def batch(self, symbols: list[str]) -> list[dict]:
        return [self.items[s] for s in symbols if s in self.items]


quote_cache = QuoteCache()
