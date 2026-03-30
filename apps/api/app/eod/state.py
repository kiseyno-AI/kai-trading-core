from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional


@dataclass
class EODJobState:
    last_market_date: Optional[str] = None
    last_started_at: Optional[str] = None
    last_completed_at: Optional[str] = None
    last_status: str = 'idle'
    last_error: Optional[str] = None
    symbols_total: int = 0
    symbols_processed: int = 0

    def snapshot(self) -> dict:
        return {
            'last_market_date': self.last_market_date,
            'last_started_at': self.last_started_at,
            'last_completed_at': self.last_completed_at,
            'last_status': self.last_status,
            'last_error': self.last_error,
            'symbols_total': self.symbols_total,
            'symbols_processed': self.symbols_processed,
        }

    def start(self, market_date: str, symbols_total: int) -> None:
        self.last_market_date = market_date
        self.last_started_at = datetime.now(timezone.utc).isoformat()
        self.last_completed_at = None
        self.last_status = 'running'
        self.last_error = None
        self.symbols_total = symbols_total
        self.symbols_processed = 0

    def tick(self) -> None:
        self.symbols_processed += 1

    def fail(self, error: Exception) -> None:
        self.last_status = 'failed'
        self.last_error = str(error)
        self.last_completed_at = datetime.now(timezone.utc).isoformat()

    def complete(self) -> None:
        self.last_status = 'completed'
        self.last_completed_at = datetime.now(timezone.utc).isoformat()
        self.last_error = None


job_state = EODJobState()
