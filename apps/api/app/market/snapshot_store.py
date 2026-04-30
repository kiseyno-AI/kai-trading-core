from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from sqlalchemy import select

from app.db import session_scope
from app.db_models import CurrentMarketSnapshotCache

MARKET_SNAPSHOT_CACHE_KEY = 'dashboard'


def _normalize_datetime(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return value
    if not isinstance(value, str):
        return None
    normalized = value.strip()
    if not normalized:
        return None
    if normalized.endswith('Z'):
        normalized = normalized[:-1] + '+00:00'
    try:
        return datetime.fromisoformat(normalized)
    except ValueError:
        return None


@dataclass(frozen=True)
class MarketSnapshotCacheRecord:
    payload: dict[str, Any] | None
    snapshot_status: str | None
    stale_reason: str | None
    as_of: datetime | None
    last_refresh_attempted_at: datetime | None
    last_refresh_succeeded_at: datetime | None
    last_refresh_status: str
    last_refresh_error: str | None
    refresh_trigger: str | None
    created_at: datetime | None
    updated_at: datetime | None


class MarketSnapshotCacheStore:
    def load(self) -> MarketSnapshotCacheRecord | None:
        with session_scope() as session:
            row = session.execute(
                select(CurrentMarketSnapshotCache).where(
                    CurrentMarketSnapshotCache.cache_key == MARKET_SNAPSHOT_CACHE_KEY
                )
            ).scalar_one_or_none()
            if row is None:
                return None
            return self._to_record(row)

    def save(
        self,
        *,
        payload: dict[str, Any] | None,
        last_refresh_attempted_at: datetime | None,
        last_refresh_succeeded_at: datetime | None,
        last_refresh_status: str,
        last_refresh_error: str | None,
        refresh_trigger: str | None,
    ) -> MarketSnapshotCacheRecord:
        with session_scope() as session:
            row = session.execute(
                select(CurrentMarketSnapshotCache).where(
                    CurrentMarketSnapshotCache.cache_key == MARKET_SNAPSHOT_CACHE_KEY
                )
            ).scalar_one_or_none()
            if row is None:
                row = CurrentMarketSnapshotCache(cache_key=MARKET_SNAPSHOT_CACHE_KEY)
                session.add(row)
                session.flush()

            stored_payload = deepcopy(payload) if isinstance(payload, dict) else None
            row.payload = stored_payload
            row.snapshot_status = stored_payload.get('status') if stored_payload is not None else None
            row.stale_reason = stored_payload.get('stale_reason') if stored_payload is not None else None
            row.as_of = _normalize_datetime(stored_payload.get('as_of')) if stored_payload is not None else None
            row.last_refresh_attempted_at = last_refresh_attempted_at
            row.last_refresh_succeeded_at = last_refresh_succeeded_at
            row.last_refresh_status = last_refresh_status
            row.last_refresh_error = last_refresh_error
            row.refresh_trigger = refresh_trigger
            session.flush()
            return self._to_record(row)

    def _to_record(self, row: CurrentMarketSnapshotCache) -> MarketSnapshotCacheRecord:
        payload = deepcopy(row.payload) if isinstance(row.payload, dict) else None
        return MarketSnapshotCacheRecord(
            payload=payload,
            snapshot_status=row.snapshot_status,
            stale_reason=row.stale_reason,
            as_of=row.as_of,
            last_refresh_attempted_at=row.last_refresh_attempted_at,
            last_refresh_succeeded_at=row.last_refresh_succeeded_at,
            last_refresh_status=row.last_refresh_status,
            last_refresh_error=row.last_refresh_error,
            refresh_trigger=row.refresh_trigger,
            created_at=row.created_at,
            updated_at=row.updated_at,
        )


market_snapshot_cache_store = MarketSnapshotCacheStore()
