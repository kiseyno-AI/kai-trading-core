from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timezone
import threading
from typing import Any

from app.quotes.kis_realtime_prices import KISRealtimePriceTick


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


@dataclass(frozen=True)
class WaitRecheckSubscriptionSource:
    """Empty compatibility result for the retired WAIT recheck runtime."""

    symbols: tuple[str, ...]
    source_version: datetime | None
    membership_complete: bool


class SignalDeciderWaitRecheckService:
    """Side-effect-free tombstone for the retired WAIT recheck runtime.

    WAIT trigger geometry is consumed by the normal execution path. A quote
    touch must never open a second decider loop, persist a recheck event,
    subscribe an extra symbol, or grant order authority.
    """

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._last_cycle_at: datetime | None = None
        self._last_cycle_result: dict[str, Any] | None = None
        self._candidate_cache_epoch = 0

    def runtime_snapshot(self) -> dict[str, Any]:
        with self._lock:
            return {
                'enabled': False,
                'candidate_symbols': [],
                'candidate_count': 0,
                'pending_count': 0,
                'reserved_count': 0,
                'rolling_evidence_symbols': [],
                'rolling_evidence_sample_count': 0,
                'wake_notification_count': 0,
                'reward_risk_prefilter_rejection_count': 0,
                'evidence_rejection_count': 0,
                'material_change_rejection_count': 0,
                'last_source_refresh_at': None,
                'last_cycle_at': (
                    self._last_cycle_at.isoformat()
                    if self._last_cycle_at is not None
                    else None
                ),
                'last_cycle_result': self._last_cycle_result,
                'decider_dispatch_enabled': False,
                'direct_order_authority': False,
            }

    def candidate_cache_epoch(self) -> int:
        with self._lock:
            return self._candidate_cache_epoch

    def clear_candidate_cache(self) -> bool:
        with self._lock:
            self._candidate_cache_epoch += 1
        return False

    def observe_realtime_tick(
        self,
        tick: KISRealtimePriceTick,
        *,
        subscribed: bool,
        limited: bool,
        now: datetime | None = None,
    ) -> bool:
        del tick, subscribed, limited, now
        return False

    def load_subscription_source(
        self,
        *,
        target_trade_date: date,
        expected_cache_epoch: int | None = None,
    ) -> WaitRecheckSubscriptionSource:
        del target_trade_date, expected_cache_epoch
        return WaitRecheckSubscriptionSource(
            symbols=(),
            source_version=_utcnow(),
            membership_complete=True,
        )

    def run_open_cycle(
        self,
        *,
        market: Any,
        now: datetime | None = None,
        max_events: int | None = None,
    ) -> dict[str, Any]:
        del market, max_events
        cycle_result = {
            'status': 'disabled',
            'events_processed': 0,
            'results': [],
            'follow_up_required': False,
            'direct_order_authority': False,
        }
        with self._lock:
            self._last_cycle_at = now or _utcnow()
            self._last_cycle_result = cycle_result
        return cycle_result


signal_decider_wait_recheck_service = SignalDeciderWaitRecheckService()
