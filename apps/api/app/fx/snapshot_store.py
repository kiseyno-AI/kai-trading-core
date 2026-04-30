from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any

from sqlalchemy import select

from app.db import session_scope
from app.db_models import CurrentFXRateSnapshot

KOREAEXIM_PROVIDER = 'koreaexim'
USD_KRW_PAIR = 'USD/KRW'


def _normalize_date(value: Any) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if isinstance(value, str):
        normalized = value.strip()
        if len(normalized) == 8 and normalized.isdigit():
            return date.fromisoformat(f'{normalized[:4]}-{normalized[4:6]}-{normalized[6:8]}')
        return date.fromisoformat(normalized)
    raise ValueError(f'Unsupported date value: {value!r}')


def _normalize_datetime(value: Any) -> datetime:
    if isinstance(value, datetime):
        return value
    if not isinstance(value, str):
        raise ValueError(f'Unsupported datetime value: {value!r}')
    normalized = value.strip()
    if not normalized:
        raise ValueError('Datetime value cannot be empty')
    if normalized.endswith('Z'):
        normalized = normalized[:-1] + '+00:00'
    return datetime.fromisoformat(normalized)


@dataclass(frozen=True)
class FXRateSnapshotRecord:
    provider: str
    currency_pair: str
    base_currency: str
    quote_currency: str
    exchange_rate: float
    requested_date: date
    effective_date: date
    fallback_applied: bool
    snapshot_status: str
    stale_reason: str | None
    detail: str | None
    payload: dict[str, Any] | None
    captured_at: datetime
    created_at: datetime | None
    updated_at: datetime | None

    def to_market_snapshot_payload(self) -> dict[str, Any]:
        return {
            'pair': self.currency_pair,
            'rate': self.exchange_rate,
            'effective_date': self.effective_date.isoformat(),
            'fallback_applied': self.fallback_applied,
            'status': self.snapshot_status,
            'source': 'cache',
            'stale_reason': self.stale_reason,
            'detail': self.detail,
            'captured_at': self.captured_at.isoformat(),
        }


class FXRateSnapshotStore:
    def load_latest_usable(
        self,
        *,
        provider: str,
        currency_pair: str,
    ) -> FXRateSnapshotRecord | None:
        with session_scope() as session:
            row = session.execute(
                select(CurrentFXRateSnapshot).where(
                    CurrentFXRateSnapshot.provider == provider,
                    CurrentFXRateSnapshot.currency_pair == currency_pair,
                )
            ).scalar_one_or_none()
            if row is None:
                return None
            return self._to_record(row)

    def upsert_snapshot(
        self,
        *,
        provider: str,
        currency_pair: str,
        base_currency: str,
        quote_currency: str,
        exchange_rate: float,
        requested_date: date | str,
        effective_date: date | str,
        fallback_applied: bool,
        snapshot_status: str,
        stale_reason: str | None,
        detail: str | None,
        payload: dict[str, Any] | None,
        captured_at: datetime | str,
    ) -> FXRateSnapshotRecord:
        normalized_requested_date = _normalize_date(requested_date)
        normalized_effective_date = _normalize_date(effective_date)
        normalized_captured_at = _normalize_datetime(captured_at)
        if exchange_rate is None:
            raise ValueError('exchange_rate is required for FX snapshot persistence')

        with session_scope() as session:
            row = session.execute(
                select(CurrentFXRateSnapshot).where(
                    CurrentFXRateSnapshot.provider == provider,
                    CurrentFXRateSnapshot.currency_pair == currency_pair,
                )
            ).scalar_one_or_none()
            if row is None:
                row = CurrentFXRateSnapshot(provider=provider, currency_pair=currency_pair)
                session.add(row)

            row.base_currency = base_currency
            row.quote_currency = quote_currency
            row.exchange_rate = exchange_rate
            row.requested_date = normalized_requested_date
            row.effective_date = normalized_effective_date
            row.fallback_applied = fallback_applied
            row.snapshot_status = snapshot_status
            row.stale_reason = stale_reason
            row.detail = detail
            row.payload = deepcopy(payload) if isinstance(payload, dict) else None
            row.captured_at = normalized_captured_at
            session.flush()
            return self._to_record(row)

    def _to_record(self, row: CurrentFXRateSnapshot) -> FXRateSnapshotRecord:
        return FXRateSnapshotRecord(
            provider=row.provider,
            currency_pair=row.currency_pair,
            base_currency=row.base_currency,
            quote_currency=row.quote_currency,
            exchange_rate=float(row.exchange_rate),
            requested_date=row.requested_date,
            effective_date=row.effective_date,
            fallback_applied=row.fallback_applied,
            snapshot_status=row.snapshot_status,
            stale_reason=row.stale_reason,
            detail=row.detail,
            payload=deepcopy(row.payload) if isinstance(row.payload, dict) else None,
            captured_at=row.captured_at,
            created_at=row.created_at,
            updated_at=row.updated_at,
        )


fx_snapshot_store = FXRateSnapshotStore()
