from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime


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
        self.last_updated_at = datetime.utcnow().isoformat() + 'Z'
        return self.snapshot()


scanner_state = ScannerState()
