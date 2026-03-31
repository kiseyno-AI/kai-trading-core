from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Optional


@dataclass
class MasterRefreshState:
    market_date: Optional[str] = None
    last_started_at: Optional[str] = None
    last_completed_at: Optional[str] = None
    last_status: str = 'idle'
    last_error: Optional[str] = None
    symbol_count: int = 0
    kospi_count: int = 0
    kosdaq_count: int = 0
    symbols: list[Any] = field(default_factory=list)

    def snapshot(self) -> dict:
        return {
            'market_date': self.market_date,
            'last_started_at': self.last_started_at,
            'last_completed_at': self.last_completed_at,
            'last_status': self.last_status,
            'last_error': self.last_error,
            'symbol_count': self.symbol_count,
            'kospi_count': self.kospi_count,
            'kosdaq_count': self.kosdaq_count,
        }

    def start(self, market_date: str) -> None:
        self.market_date = market_date
        self.last_started_at = datetime.now(timezone.utc).isoformat()
        self.last_completed_at = None
        self.last_status = 'running'
        self.last_error = None

    def complete(self, symbols: list[Any]) -> None:
        self.symbols = symbols
        self.symbol_count = len(symbols)
        self.kospi_count = sum(1 for item in symbols if item.market == 'KOSPI')
        self.kosdaq_count = sum(1 for item in symbols if item.market == 'KOSDAQ')
        self.last_status = 'completed'
        self.last_completed_at = datetime.now(timezone.utc).isoformat()
        self.last_error = None

    def fail(self, error: Exception) -> None:
        self.last_status = 'failed'
        self.last_error = str(error)
        self.last_completed_at = datetime.now(timezone.utc).isoformat()


master_refresh_state = MasterRefreshState()
