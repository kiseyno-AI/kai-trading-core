from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Literal, Mapping

ExternalContextSourceStatus = Literal[
    'available',
    'disabled',
    'provider_error',
    'stale',
    'empty',
    'rate_limited',
]
ExternalContextFreshnessStatus = Literal['fresh', 'stale', 'missing']


def isoformat_or_none(value: datetime | None) -> str | None:
    return value.isoformat() if isinstance(value, datetime) else None


@dataclass(frozen=True)
class ExternalContextRequest:
    key: str
    params: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class ExternalContextProviderStatus:
    provider: str
    enabled: bool
    status: ExternalContextSourceStatus
    reason: str | None = None
    last_success_at: datetime | None = None
    last_error_at: datetime | None = None
    source_url: str | None = None
    upstream_endpoint: str | None = None

    def as_dict(self) -> dict[str, Any]:
        return {
            'provider': self.provider,
            'enabled': self.enabled,
            'status': self.status,
            'reason': self.reason,
            'last_success_at': isoformat_or_none(self.last_success_at),
            'last_error_at': isoformat_or_none(self.last_error_at),
            'source_url': self.source_url,
            'upstream_endpoint': self.upstream_endpoint,
        }


@dataclass(frozen=True)
class ExternalContextFetchResult:
    payload: Any
    source_status: ExternalContextSourceStatus = 'available'
    source_url: str | None = None
    upstream_endpoint: str | None = None


@dataclass(frozen=True)
class ExternalContextError:
    error_type: str
    message: str
    recorded_at: datetime
    reason: str | None = None

    def as_dict(self) -> dict[str, Any]:
        return {
            'type': self.error_type,
            'message': self.message,
            'recorded_at': isoformat_or_none(self.recorded_at),
            'reason': self.reason,
        }


@dataclass(frozen=True)
class ExternalContextFreshness:
    status: ExternalContextFreshnessStatus
    served_from_cache: bool
    cache_hit: bool
    ttl_seconds: int
    min_refresh_interval_seconds: int
    force_refresh: bool
    refresh_reason: str
    stale_reason: str | None = None
    fetched_at: datetime | None = None
    expires_at: datetime | None = None
    age_seconds: float | None = None
    last_attempt_at: datetime | None = None
    last_success_at: datetime | None = None
    last_error_at: datetime | None = None
    next_refresh_allowed_at: datetime | None = None

    def as_dict(self) -> dict[str, Any]:
        return {
            'status': self.status,
            'served_from_cache': self.served_from_cache,
            'cache_hit': self.cache_hit,
            'ttl_seconds': self.ttl_seconds,
            'min_refresh_interval_seconds': self.min_refresh_interval_seconds,
            'force_refresh': self.force_refresh,
            'refresh_reason': self.refresh_reason,
            'stale_reason': self.stale_reason,
            'fetched_at': isoformat_or_none(self.fetched_at),
            'expires_at': isoformat_or_none(self.expires_at),
            'age_seconds': self.age_seconds,
            'last_attempt_at': isoformat_or_none(self.last_attempt_at),
            'last_success_at': isoformat_or_none(self.last_success_at),
            'last_error_at': isoformat_or_none(self.last_error_at),
            'next_refresh_allowed_at': isoformat_or_none(self.next_refresh_allowed_at),
        }


@dataclass(frozen=True)
class ExternalContextResult:
    key: str
    provider: str | None
    source_status: ExternalContextSourceStatus
    payload: Any
    freshness: ExternalContextFreshness
    source: Mapping[str, Any]
    error: ExternalContextError | None = None

    def as_dict(self) -> dict[str, Any]:
        return {
            'key': self.key,
            'provider': self.provider,
            'source_status': self.source_status,
            'payload': self.payload,
            'freshness': self.freshness.as_dict(),
            'source': dict(self.source),
            'error': self.error.as_dict() if self.error is not None else None,
        }
