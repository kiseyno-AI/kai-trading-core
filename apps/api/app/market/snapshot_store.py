from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import select, text

from app.db import session_scope
from app.db_models import CurrentMarketSnapshotCache

MARKET_SNAPSHOT_CACHE_KEY = 'dashboard'
MARKET_SNAPSHOT_CACHE_WRITE_LOCK_KEY = 0x4B41494D534E4150
MARKET_SNAPSHOT_OBSERVATION_REGRESSED = 'market_snapshot_observation_regressed'
MARKET_SNAPSHOT_OBSERVATION_MISSING = 'market_snapshot_provider_observation_missing'

_SNAPSHOT_QUALITY_RANK = {
    'ready': 4,
    'partial': 3,
    'stale': 2,
    'missing': 1,
    'unavailable': 1,
}


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


def _comparable_datetime(value: Any) -> datetime | None:
    parsed = _normalize_datetime(value)
    if parsed is None or parsed.tzinfo is None or parsed.utcoffset() is None:
        return None
    return parsed.astimezone(timezone.utc)


def _payload_observation_order(
    payload: dict[str, Any] | None,
) -> tuple[datetime, datetime] | None:
    if not isinstance(payload, dict):
        return None
    provider_observed_at = _comparable_datetime(payload.get('as_of'))
    if provider_observed_at is None:
        return None
    collected_at = _comparable_datetime(payload.get('collected_at'))
    return provider_observed_at, collected_at or provider_observed_at


def _payload_market_observations(
    payload: dict[str, Any] | None,
) -> dict[str, datetime] | None:
    if not isinstance(payload, dict):
        return {}
    markets = payload.get('markets')
    if markets is None:
        return {}
    if not isinstance(markets, dict):
        return None

    observations: dict[str, datetime] = {}
    seen_market_keys: set[str] = set()
    for raw_market_key, raw_market in markets.items():
        market_key = str(raw_market_key).strip().lower()
        if (
            not market_key
            or market_key in seen_market_keys
            or not isinstance(raw_market, dict)
        ):
            return None
        seen_market_keys.add(market_key)
        raw_observed_at = raw_market.get('provider_observed_at')
        if raw_observed_at in (None, ''):
            continue
        observed_at = _comparable_datetime(raw_observed_at)
        if observed_at is None:
            return None
        observations[market_key] = observed_at
    return observations


def _market_observations_do_not_regress(
    *,
    current: dict[str, Any],
    candidate: dict[str, Any],
) -> bool:
    current_observations = _payload_market_observations(current)
    candidate_observations = _payload_market_observations(candidate)
    if candidate_observations is None:
        return False
    if not current_observations:
        return True
    return all(
        candidate_observations.get(market_key) is not None
        and candidate_observations[market_key] >= current_observed_at
        for market_key, current_observed_at in current_observations.items()
    )


def _snapshot_quality(payload: dict[str, Any] | None) -> int:
    if not isinstance(payload, dict):
        return 0
    return _SNAPSHOT_QUALITY_RANK.get(
        str(payload.get('status') or '').strip().lower(),
        0,
    )


def _payload_can_replace(
    *,
    current: dict[str, Any] | None,
    candidate: dict[str, Any] | None,
) -> bool:
    current_order = _payload_observation_order(current)
    candidate_order = _payload_observation_order(candidate)
    if current is None:
        return candidate is None or candidate_order is not None
    if candidate is None:
        return current_order is None
    if not _market_observations_do_not_regress(
        current=current,
        candidate=candidate,
    ):
        return False
    if current_order is None:
        return candidate_order is not None
    if candidate_order is None:
        return False
    if candidate_order != current_order:
        return candidate_order > current_order
    current_quality = _snapshot_quality(current)
    candidate_quality = _snapshot_quality(candidate)
    if candidate_quality != current_quality:
        return candidate_quality > current_quality
    return candidate == current


def _failed_refresh_overlay(
    *,
    current: dict[str, Any] | None,
    candidate: dict[str, Any] | None,
    current_attempt: datetime | None,
    candidate_attempt: datetime | None,
    current_success: datetime | None,
    last_refresh_status: str,
) -> dict[str, Any] | None:
    current_order = _payload_observation_order(current)
    candidate_order = _payload_observation_order(candidate)
    if (
        current is None
        or candidate is None
        or str(last_refresh_status).strip().lower() != 'error'
        or str(candidate.get('status') or '').strip().lower() != 'stale'
        or not str(candidate.get('stale_reason') or '').strip()
        or candidate_attempt is None
        or (current_attempt is not None and candidate_attempt < current_attempt)
        or (current_success is not None and candidate_attempt <= current_success)
        or current_order is None
        or candidate_order != current_order
    ):
        return None
    current_market_observations = _payload_market_observations(current)
    candidate_market_observations = _payload_market_observations(candidate)
    if (
        current_market_observations is None
        or candidate_market_observations is None
        or candidate_market_observations != current_market_observations
    ):
        return None

    overlay = deepcopy(current)
    overlay['status'] = 'stale'
    overlay['stale_reason'] = candidate['stale_reason']
    return overlay


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
    @staticmethod
    def _lock_snapshot_row(session) -> CurrentMarketSnapshotCache | None:
        session.execute(
            text('SELECT pg_advisory_xact_lock(:lock_key)'),
            {'lock_key': MARKET_SNAPSHOT_CACHE_WRITE_LOCK_KEY},
        )
        return session.execute(
            select(CurrentMarketSnapshotCache)
            .where(CurrentMarketSnapshotCache.cache_key == MARKET_SNAPSHOT_CACHE_KEY)
            .with_for_update()
        ).scalar_one_or_none()

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

    def try_claim_refresh_bucket(
        self,
        *,
        bucket_started_at: datetime,
        attempted_at: datetime,
        refresh_trigger: str | None,
    ) -> bool:
        comparable_bucket = _comparable_datetime(bucket_started_at)
        comparable_attempt = _comparable_datetime(attempted_at)
        if comparable_bucket is None or comparable_attempt is None:
            raise ValueError('Market snapshot refresh claims require timezone-aware timestamps')
        if comparable_attempt < comparable_bucket:
            raise ValueError('Market snapshot refresh attempt cannot precede its bucket')

        with session_scope() as session:
            row = self._lock_snapshot_row(session)
            if row is None:
                row = CurrentMarketSnapshotCache(cache_key=MARKET_SNAPSHOT_CACHE_KEY)
                session.add(row)
                session.flush()
            current_attempt = _comparable_datetime(row.last_refresh_attempted_at)
            if current_attempt is not None and current_attempt >= comparable_bucket:
                return False
            row.last_refresh_attempted_at = attempted_at
            row.refresh_trigger = refresh_trigger
            session.flush()
            return True

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
            row = self._lock_snapshot_row(session)
            if row is None:
                row = CurrentMarketSnapshotCache(cache_key=MARKET_SNAPSHOT_CACHE_KEY)
                session.add(row)
                session.flush()

            stored_payload = deepcopy(payload) if isinstance(payload, dict) else None
            current_attempt = _comparable_datetime(row.last_refresh_attempted_at)
            candidate_attempt = _comparable_datetime(last_refresh_attempted_at)
            if current_attempt is not None and (
                candidate_attempt is None or candidate_attempt < current_attempt
            ):
                return self._to_record(row)

            current_payload = deepcopy(row.payload) if isinstance(row.payload, dict) else None
            current_success = _comparable_datetime(row.last_refresh_succeeded_at)
            failed_overlay = _failed_refresh_overlay(
                current=current_payload,
                candidate=stored_payload,
                current_attempt=current_attempt,
                candidate_attempt=candidate_attempt,
                current_success=current_success,
                last_refresh_status=last_refresh_status,
            )
            if failed_overlay is not None:
                stored_payload = failed_overlay
            elif not _payload_can_replace(
                current=current_payload,
                candidate=stored_payload,
            ):
                row.last_refresh_attempted_at = last_refresh_attempted_at
                row.last_refresh_status = 'error'
                row.last_refresh_error = (
                    MARKET_SNAPSHOT_OBSERVATION_MISSING
                    if isinstance(stored_payload, dict)
                    and _payload_observation_order(stored_payload) is None
                    else MARKET_SNAPSHOT_OBSERVATION_REGRESSED
                )
                row.refresh_trigger = refresh_trigger
                session.flush()
                return self._to_record(row)

            row.payload = stored_payload
            row.snapshot_status = stored_payload.get('status') if stored_payload is not None else None
            row.stale_reason = stored_payload.get('stale_reason') if stored_payload is not None else None
            row.as_of = _normalize_datetime(stored_payload.get('as_of')) if stored_payload is not None else None
            row.last_refresh_attempted_at = last_refresh_attempted_at
            candidate_success = _comparable_datetime(last_refresh_succeeded_at)
            row.last_refresh_succeeded_at = (
                last_refresh_succeeded_at
                if candidate_success is not None
                and (current_success is None or candidate_success >= current_success)
                else row.last_refresh_succeeded_at
            )
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
