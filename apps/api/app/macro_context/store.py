from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Any, Sequence

from sqlalchemy import func, select
from sqlalchemy.dialects.postgresql import insert

from app.db import session_scope
from app.db_models import MacroContextObservation


def _normalize_date(value: Any) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if isinstance(value, str):
        normalized = value.strip()
        if len(normalized) == 8 and normalized.isdigit():
            return date.fromisoformat(f'{normalized[:4]}-{normalized[4:6]}-{normalized[6:8]}')
        return date.fromisoformat(normalized[:10])
    raise ValueError(f'Unsupported date value: {value!r}')


def _normalize_datetime(value: Any) -> datetime:
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value
    if isinstance(value, str):
        normalized = value.strip()
        if normalized.endswith('Z'):
            normalized = normalized[:-1] + '+00:00'
        parsed = datetime.fromisoformat(normalized)
        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=timezone.utc)
        return parsed
    raise ValueError(f'Unsupported datetime value: {value!r}')


def _to_float(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        normalized = value.strip().replace(',', '')
        if not normalized:
            return None
        return float(normalized)
    return None


@dataclass(frozen=True)
class MacroContextObservationRecord:
    id: int | None
    source: str
    series_id: str
    series_name: str | None
    country_code: str | None
    observation_period: str
    observation_date: date
    value: float
    unit: str | None
    frequency: str | None
    fetched_at: datetime
    source_status: str
    status_detail: str | None
    payload: dict[str, Any] | None
    created_at: datetime | None
    updated_at: datetime | None

    def to_dict(self) -> dict[str, Any]:
        return {
            'id': self.id,
            'source': self.source,
            'series_id': self.series_id,
            'series_name': self.series_name,
            'country_code': self.country_code,
            'observation_period': self.observation_period,
            'observation_date': self.observation_date.isoformat(),
            'value': self.value,
            'unit': self.unit,
            'frequency': self.frequency,
            'fetched_at': self.fetched_at.isoformat(),
            'source_status': self.source_status,
            'status_detail': self.status_detail,
            'payload': deepcopy(self.payload) if isinstance(self.payload, dict) else None,
            'created_at': self.created_at.isoformat() if self.created_at else None,
            'updated_at': self.updated_at.isoformat() if self.updated_at else None,
        }


class MacroContextObservationStore:
    def upsert_observation(
        self,
        *,
        source: str,
        series_id: str,
        series_name: str | None,
        country_code: str | None,
        observation_period: str,
        observation_date: date | str,
        value: float,
        unit: str | None,
        frequency: str | None,
        fetched_at: datetime | str,
        source_status: str,
        status_detail: str | None = None,
        payload: dict[str, Any] | None = None,
    ) -> MacroContextObservationRecord:
        normalized_value = _to_float(value)
        if normalized_value is None:
            raise ValueError('Macro context observation value is required')
        normalized_observation_date = _normalize_date(observation_date)
        normalized_fetched_at = _normalize_datetime(fetched_at)
        values = {
            'source': str(source),
            'series_id': str(series_id),
            'series_name': series_name,
            'country_code': country_code,
            'observation_period': str(observation_period),
            'observation_date': normalized_observation_date,
            'value': normalized_value,
            'unit': unit,
            'frequency': frequency,
            'fetched_at': normalized_fetched_at,
            'source_status': str(source_status),
            'status_detail': status_detail,
            'payload': deepcopy(payload) if isinstance(payload, dict) else None,
        }
        with session_scope() as session:
            statement = insert(MacroContextObservation).values(**values)
            statement = statement.on_conflict_do_update(
                constraint='uq_macro_context_observations_source_series_period',
                set_={
                    'series_name': statement.excluded.series_name,
                    'country_code': statement.excluded.country_code,
                    'observation_date': statement.excluded.observation_date,
                    'value': statement.excluded.value,
                    'unit': statement.excluded.unit,
                    'frequency': statement.excluded.frequency,
                    'fetched_at': statement.excluded.fetched_at,
                    'source_status': statement.excluded.source_status,
                    'status_detail': statement.excluded.status_detail,
                    'payload': statement.excluded.payload,
                },
            ).returning(MacroContextObservation)
            row = session.execute(statement).scalar_one()
            session.flush()
            return self._to_record(row)

    def load_latest_by_series(
        self,
        *,
        series_keys: Sequence[tuple[str, str]],
        as_of_date: date | str | None = None,
    ) -> dict[tuple[str, str], MacroContextObservationRecord]:
        normalized_as_of = _normalize_date(as_of_date) if as_of_date is not None else None
        loaded: dict[tuple[str, str], MacroContextObservationRecord] = {}
        if not series_keys:
            return loaded

        with session_scope() as session:
            for source, series_id in series_keys:
                query = (
                    select(MacroContextObservation)
                    .where(
                        MacroContextObservation.source == source,
                        MacroContextObservation.series_id == series_id,
                    )
                    .order_by(
                        MacroContextObservation.observation_date.desc(),
                        MacroContextObservation.fetched_at.desc(),
                        MacroContextObservation.id.desc(),
                    )
                    .limit(1)
                )
                if normalized_as_of is not None:
                    query = query.where(MacroContextObservation.observation_date <= normalized_as_of)
                row = session.execute(query).scalar_one_or_none()
                if row is not None:
                    loaded[(source, series_id)] = self._to_record(row)
        return loaded

    def latest_fetched_at(self) -> datetime | None:
        with session_scope() as session:
            value = session.execute(select(func.max(MacroContextObservation.fetched_at))).scalar_one()
            return value

    def count_for_date(self, *, as_of_date: date | str) -> dict[str, Any]:
        normalized_as_of = _normalize_date(as_of_date)
        with session_scope() as session:
            count, updated_at = session.execute(
                select(
                    func.count(MacroContextObservation.id),
                    func.max(MacroContextObservation.fetched_at),
                ).where(MacroContextObservation.observation_date <= normalized_as_of)
            ).one()
        return {
            'persisted': int(count or 0),
            'updated_at': updated_at.isoformat() if updated_at else None,
        }

    def _to_record(self, row: MacroContextObservation) -> MacroContextObservationRecord:
        value = _to_float(row.value)
        if value is None:
            raise ValueError(f'Macro context observation has null value: {row.source}/{row.series_id}')
        return MacroContextObservationRecord(
            id=row.id,
            source=row.source,
            series_id=row.series_id,
            series_name=row.series_name,
            country_code=row.country_code,
            observation_period=row.observation_period,
            observation_date=row.observation_date,
            value=value,
            unit=row.unit,
            frequency=row.frequency,
            fetched_at=row.fetched_at,
            source_status=row.source_status,
            status_detail=row.status_detail,
            payload=deepcopy(row.payload) if isinstance(row.payload, dict) else None,
            created_at=row.created_at,
            updated_at=row.updated_at,
        )


macro_context_observation_store = MacroContextObservationStore()
