from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone


@dataclass
class ScannerState:
    tracked_symbols: list[str] = field(default_factory=lambda: ['005930', '000660'])
    last_updated_at: str | None = None

    def snapshot(self) -> dict:
        return {
            'tracked_symbols': self.tracked_symbols,
            'count': len(self.tracked_symbols),
            'last_updated_at': self.last_updated_at,
        }

    def update(self, symbols: list[str]) -> dict:
        self.tracked_symbols = symbols
        # Same wire format as before (naive UTC ISO + 'Z') without the
        # deprecated datetime.utcnow().
        self.last_updated_at = datetime.now(timezone.utc).replace(tzinfo=None).isoformat() + 'Z'
        return self.snapshot()

    def hydrate(self, symbols: list[str], *, last_updated_at: str | None = None) -> dict:
        self.tracked_symbols = symbols
        self.last_updated_at = last_updated_at
        return self.snapshot()


scanner_state = ScannerState()
