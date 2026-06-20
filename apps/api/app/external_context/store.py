from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from app.external_context.models import ExternalContextSourceStatus


@dataclass(frozen=True)
class ExternalContextCacheRecord:
    provider_name: str
    key: str
    payload: Any
    source_status: ExternalContextSourceStatus
    source_url: str | None
    upstream_endpoint: str | None
    fetched_at: datetime | None
    expires_at: datetime | None
    last_attempt_at: datetime | None
    last_success_at: datetime | None
    last_error_at: datetime | None
    last_error_message: str | None
    last_error_type: str | None
    created_at: datetime
    updated_at: datetime


class InMemoryExternalContextStore:
    def __init__(self) -> None:
        self._records: dict[tuple[str, str], ExternalContextCacheRecord] = {}

    def load(self, *, provider_name: str, key: str) -> ExternalContextCacheRecord | None:
        record = self._records.get((provider_name, key))
        return deepcopy(record) if record is not None else None

    def save_success(
        self,
        *,
        provider_name: str,
        key: str,
        payload: Any,
        source_status: ExternalContextSourceStatus,
        source_url: str | None,
        upstream_endpoint: str | None,
        fetched_at: datetime,
        expires_at: datetime,
    ) -> ExternalContextCacheRecord:
        previous = self._records.get((provider_name, key))
        created_at = previous.created_at if previous is not None else fetched_at
        record = ExternalContextCacheRecord(
            provider_name=provider_name,
            key=key,
            payload=deepcopy(payload),
            source_status=source_status,
            source_url=source_url,
            upstream_endpoint=upstream_endpoint,
            fetched_at=fetched_at,
            expires_at=expires_at,
            last_attempt_at=fetched_at,
            last_success_at=fetched_at,
            last_error_at=previous.last_error_at if previous is not None else None,
            last_error_message=previous.last_error_message if previous is not None else None,
            last_error_type=previous.last_error_type if previous is not None else None,
            created_at=created_at,
            updated_at=fetched_at,
        )
        self._records[(provider_name, key)] = record
        return deepcopy(record)

    def record_error(
        self,
        *,
        provider_name: str,
        key: str,
        attempted_at: datetime,
        error_message: str,
        error_type: str,
    ) -> ExternalContextCacheRecord:
        previous = self._records.get((provider_name, key))
        created_at = previous.created_at if previous is not None else attempted_at
        record = ExternalContextCacheRecord(
            provider_name=provider_name,
            key=key,
            payload=deepcopy(previous.payload) if previous is not None else None,
            source_status=previous.source_status if previous is not None else 'provider_error',
            source_url=previous.source_url if previous is not None else None,
            upstream_endpoint=previous.upstream_endpoint if previous is not None else None,
            fetched_at=previous.fetched_at if previous is not None else None,
            expires_at=previous.expires_at if previous is not None else None,
            last_attempt_at=attempted_at,
            last_success_at=previous.last_success_at if previous is not None else None,
            last_error_at=attempted_at,
            last_error_message=error_message,
            last_error_type=error_type,
            created_at=created_at,
            updated_at=attempted_at,
        )
        self._records[(provider_name, key)] = record
        return deepcopy(record)
