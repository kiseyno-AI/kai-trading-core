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
    pending_feedback_rows: list[dict[str, Any]] = field(default_factory=list)
    pending_failures: list[dict[str, Any]] = field(default_factory=list)
    completed_symbols_by_role: dict[str, set[str]] = field(default_factory=dict)
    failed_symbols_by_role: dict[str, set[str]] = field(default_factory=dict)
    reused_symbols_by_role: dict[str, set[str]] = field(default_factory=dict)
    in_flight_slots_by_role: dict[str, dict[int, dict[str, Any]]] = field(default_factory=dict)
    last_activity_at: datetime | None = None
    logical_total_symbols: int | None = None
    logical_expected_feedback_count: int | None = None
    logical_completed_symbols: int | None = None
    logical_in_progress_symbols: int | None = None
    logical_pending_symbols: int | None = None
    logical_completed_feedback_count: int | None = None
    logical_failed_items: int | None = None
    normalized_response_count: int | None = None
    flushed_feedback_count: int = 0
    flushed_failure_count: int = 0

    def to_progress_summary(self) -> dict[str, Any]:
        expected_feedback_count = self.logical_expected_feedback_count if self.logical_expected_feedback_count is not None else len(self.requested_symbols) * len(self.requested_roles)
        completed_feedback_count = (
            self.logical_completed_feedback_count
            if self.logical_completed_feedback_count is not None
            else sum(len(self.completed_symbols_by_role.get(role, set())) for role in self.requested_roles)
        )
        total_symbols = self.logical_total_symbols if self.logical_total_symbols is not None else len(self.requested_symbols)

        if self.logical_completed_symbols is not None or self.logical_in_progress_symbols is not None or self.logical_pending_symbols is not None:
            completed_symbols = max(0, int(self.logical_completed_symbols or 0))
            in_progress_symbols = max(0, int(self.logical_in_progress_symbols or 0))
            pending_symbols = max(0, int(self.logical_pending_symbols or 0))
            if self.logical_total_symbols is None:
                total_symbols = completed_symbols + in_progress_symbols + pending_symbols
        else:
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
        progress_ratio = (processed_symbols / total_symbols) if total_symbols else 0.0

        role_progress: dict[str, dict[str, Any]] = {}
        for role in self.requested_roles:
            completed_for_role = len(self.completed_symbols_by_role.get(role, set()))
            failed_for_role = len(self.failed_symbols_by_role.get(role, set()))
            reused_for_role = len(self.reused_symbols_by_role.get(role, set()))
            in_flight_symbols = set()
            for slot in self.in_flight_slots_by_role.get(role, {}).values():
                for symbol in slot.get('symbols') or []:
                    if symbol not in self.completed_symbols_by_role.get(role, set()) and symbol not in self.failed_symbols_by_role.get(role, set()):
                        in_flight_symbols.add(symbol)
            processed_for_role = min(total_symbols, completed_for_role + len(in_flight_symbols) + failed_for_role)
            pending_for_role = max(0, total_symbols - processed_for_role)
            progress_ratio_for_role = (processed_for_role / total_symbols) if total_symbols else 0.0
            role_status = 'pending'
            if pending_for_role == 0 and not in_flight_symbols:
                if failed_for_role == 0:
                    role_status = 'completed'
                elif completed_for_role > 0:
                    role_status = 'partial'
                else:
                    role_status = 'failed'
            elif processed_for_role > 0 or in_flight_symbols:
                role_status = 'running'
            role_progress[role] = {
                'role': role,
                'status': role_status,
                'total_symbols': total_symbols,
                'processed_symbols': processed_for_role,
                'completed_symbols': completed_for_role,
                'reused_symbols': reused_for_role,
                'in_progress_symbols': len(in_flight_symbols),
                'pending_symbols': pending_for_role,
                'failed_items': failed_for_role,
                'progress_ratio': round(progress_ratio_for_role, 6),
                'progress_percent': round(progress_ratio_for_role * 100, 2),
            }

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
            'total_symbols': total_symbols,
            'processed_symbols': processed_symbols,
            'completed_symbols': completed_symbols,
            'in_progress_symbols': in_progress_symbols,
            'pending_symbols': pending_symbols,
            'progress_ratio': round(progress_ratio, 6),
            'progress_percent': round(progress_ratio * 100, 2),
            'failed_items': self.logical_failed_items if self.logical_failed_items is not None else len(self.buffered_failures),
            'completed_feedback_count': completed_feedback_count,
            'expected_feedback_count': expected_feedback_count,
            'buffered_feedback_count': len(self.buffered_feedback_rows),
            'buffered_failure_count': len(self.buffered_failures),
            'pending_feedback_count': len(self.pending_feedback_rows),
            'pending_failure_count': len(self.pending_failures),
            'flushed_feedback_count': self.flushed_feedback_count,
            'flushed_failure_count': self.flushed_failure_count,
            'completed_feedback_per_minute': completed_feedback_per_minute,
            'completed_symbols_per_minute': None,
            'estimated_remaining_seconds': estimated_remaining_seconds,
            'estimated_completion_at': estimated_completion_at,
            'eta_confidence': eta_confidence,
            'role_progress': role_progress,
        }


class RuntimeFeedbackStateStore:
    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._runs_by_market_date: dict[str, dict[str, RuntimeFeedbackRunState]] = {}
        self._active_run_id_by_market_date: dict[str, str] = {}
        self._logical_run_id_by_market_date: dict[str, str] = {}
        self._max_runs_per_market_date = 8

    def start_run(self, state: RuntimeFeedbackRunState, *, replace: bool = True, set_active: bool = True, logical: bool = False) -> RuntimeFeedbackRunState:
        with self._lock:
            market_key = state.market_date.isoformat()
            runs = self._runs_by_market_date.setdefault(market_key, {})
            if not replace and state.run_id in runs:
                return runs[state.run_id]
            runs[state.run_id] = state
            if set_active:
                self._active_run_id_by_market_date[market_key] = state.run_id
            if logical:
                self._logical_run_id_by_market_date[market_key] = state.run_id
                self._active_run_id_by_market_date[market_key] = state.run_id
            self._prune_runs_locked(market_key)
            return state

    def get_run(self, market_date: str | date, run_id: str | None = None, logical: bool = False) -> RuntimeFeedbackRunState | None:
        market_key = market_date if isinstance(market_date, str) else market_date.isoformat()
        with self._lock:
            runs = self._runs_by_market_date.get(market_key, {})
            resolved_run_id = run_id
            if resolved_run_id is None and logical:
                resolved_run_id = self._logical_run_id_by_market_date.get(market_key)
            if resolved_run_id is None:
                resolved_run_id = self._active_run_id_by_market_date.get(market_key)
            if resolved_run_id is None:
                return None
            return runs.get(resolved_run_id)

    def update_run(self, market_date: str | date, updater, run_id: str | None = None, logical: bool = False) -> RuntimeFeedbackRunState | None:
        market_key = market_date if isinstance(market_date, str) else market_date.isoformat()
        with self._lock:
            runs = self._runs_by_market_date.get(market_key, {})
            resolved_run_id = run_id
            if resolved_run_id is None and logical:
                resolved_run_id = self._logical_run_id_by_market_date.get(market_key)
            if resolved_run_id is None:
                resolved_run_id = self._active_run_id_by_market_date.get(market_key)
            if resolved_run_id is None:
                return None
            state = runs.get(resolved_run_id)
            if state is None:
                return None
            updater(state)
            self._prune_runs_locked(market_key)
            return state

    def _prune_runs_locked(self, market_key: str) -> None:
        runs = self._runs_by_market_date.get(market_key)
        if not runs or len(runs) <= self._max_runs_per_market_date:
            return
        protected = {
            self._active_run_id_by_market_date.get(market_key),
            self._logical_run_id_by_market_date.get(market_key),
        }
        removable = [
            run for run in runs.values()
            if run.run_id not in protected and run.completed_at is not None
        ]
        removable.sort(key=lambda run: run.completed_at or run.started_at)
        while len(runs) > self._max_runs_per_market_date and removable:
            victim = removable.pop(0)
            runs.pop(victim.run_id, None)


runtime_feedback_state_store = RuntimeFeedbackStateStore()
