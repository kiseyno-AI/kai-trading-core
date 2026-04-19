from __future__ import annotations

import threading
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from math import ceil
from typing import Any


@dataclass
class RuntimeFeedbackRunState:
    run_id: str
    market_date: date
    requested_symbols: list[str]
    requested_roles: list[str]
    role_sequence: list[str]
    parallel_per_role: int
    batch_size: int
    started_at: datetime
    status: str = 'running'
    current_role: str | None = None
    completed_at: datetime | None = None
    buffered_feedback_rows: list[dict[str, Any]] = field(default_factory=list)
    buffered_failures: list[dict[str, Any]] = field(default_factory=list)
    completed_symbols_by_role: dict[str, set[str]] = field(default_factory=dict)
    failed_symbols_by_role: dict[str, set[str]] = field(default_factory=dict)
    in_flight_slots_by_role: dict[str, dict[int, dict[str, Any]]] = field(default_factory=dict)
    last_activity_at: datetime | None = None

    def to_progress_summary(self) -> dict[str, Any]:
        expected_feedback_count = len(self.requested_symbols) * len(self.requested_roles)
        completed_feedback_count = len(self.buffered_feedback_rows)
        completed_symbols = 0
        in_progress_symbols = 0
        pending_symbols = 0
        for symbol in self.requested_symbols:
            role_hits = sum(1 for role in self.requested_roles if symbol in self.completed_symbols_by_role.get(role, set()))
            if role_hits == len(self.requested_roles):
                completed_symbols += 1
            elif role_hits > 0:
                in_progress_symbols += 1
            else:
                pending_symbols += 1
        processed_symbols = completed_symbols + in_progress_symbols
        progress_ratio = (processed_symbols / len(self.requested_symbols)) if self.requested_symbols else 0.0
        now = datetime.now(timezone.utc)
        elapsed_seconds = max(0, int((now - self.started_at.astimezone(timezone.utc)).total_seconds()))
        seconds_since_last_feedback = None
        if self.last_activity_at is not None:
            seconds_since_last_feedback = max(0, int((now - self.last_activity_at.astimezone(timezone.utc)).total_seconds()))
        completed_feedback_per_minute = None
        estimated_remaining_seconds = None
        estimated_completion_at = None
        eta_confidence = None
        if elapsed_seconds > 0 and completed_feedback_count > 0:
            rate = completed_feedback_count / elapsed_seconds
            completed_feedback_per_minute = round(rate * 60, 2)
            remaining = max(0, expected_feedback_count - completed_feedback_count)
            if remaining > 0 and rate > 0:
                estimated_remaining_seconds = ceil(remaining / rate)
                estimated_completion_at = (now + __import__('datetime').timedelta(seconds=estimated_remaining_seconds)).isoformat()
                eta_confidence = 'low'
        return {
            'run_id': self.run_id,
            'run_status': self.status,
            'run_table_status': self.status,
            'is_live': self.status == 'running',
            'run_started_at': self.started_at.isoformat(),
            'run_completed_at': self.completed_at.isoformat() if self.completed_at else None,
            'current_role': self.current_role,
            'parallel_per_role': self.parallel_per_role,
            'first_feedback_at': self.started_at.isoformat(),
            'latest_feedback_at': self.last_activity_at.isoformat() if self.last_activity_at else None,
            'elapsed_seconds': elapsed_seconds,
            'seconds_since_last_feedback': seconds_since_last_feedback,
            'stalled_threshold_seconds': 300,
            'total_symbols': len(self.requested_symbols),
            'processed_symbols': processed_symbols,
            'completed_symbols': completed_symbols,
            'in_progress_symbols': in_progress_symbols,
            'pending_symbols': pending_symbols,
            'progress_ratio': round(progress_ratio, 6),
            'progress_percent': round(progress_ratio * 100, 2),
            'failed_items': len(self.buffered_failures),
            'completed_feedback_count': completed_feedback_count,
            'expected_feedback_count': expected_feedback_count,
            'completed_feedback_per_minute': completed_feedback_per_minute,
            'completed_symbols_per_minute': None,
            'estimated_remaining_seconds': estimated_remaining_seconds,
            'estimated_completion_at': estimated_completion_at,
            'eta_confidence': eta_confidence,
        }


class RuntimeFeedbackStateStore:
    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._runs_by_market_date: dict[str, RuntimeFeedbackRunState] = {}

    def start_run(self, state: RuntimeFeedbackRunState) -> None:
        with self._lock:
            self._runs_by_market_date[state.market_date.isoformat()] = state

    def get_run(self, market_date: str | date) -> RuntimeFeedbackRunState | None:
        key = market_date if isinstance(market_date, str) else market_date.isoformat()
        with self._lock:
            return self._runs_by_market_date.get(key)

    def update_run(self, market_date: str | date, updater) -> RuntimeFeedbackRunState | None:
        key = market_date if isinstance(market_date, str) else market_date.isoformat()
        with self._lock:
            state = self._runs_by_market_date.get(key)
            if state is None:
                return None
            updater(state)
            return state


runtime_feedback_state_store = RuntimeFeedbackStateStore()
