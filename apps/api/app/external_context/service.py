from __future__ import annotations

import json
from collections.abc import Mapping as MappingABC
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Mapping

from app.external_context.models import (
    ExternalContextError,
    ExternalContextFetchResult,
    ExternalContextFreshnessStatus,
    ExternalContextFreshness,
    ExternalContextProviderStatus,
    ExternalContextRequest,
    ExternalContextResult,
    ExternalContextSourceStatus,
)
from app.external_context.registry import ExternalContextProvider, ExternalContextProviderRegistry
from app.external_context.store import ExternalContextCacheRecord, InMemoryExternalContextStore


Clock = Callable[[], datetime]


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _age_seconds(*, now: datetime, fetched_at: datetime | None) -> float | None:
    if fetched_at is None:
        return None
    return max(0.0, (now - fetched_at).total_seconds())


def _stable_param_sort_key(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(',', ':'))


def _stable_param_value(value: Any) -> Any:
    if isinstance(value, MappingABC):
        return {
            '__type__': 'mapping',
            'items': [
                [_stable_param_value(item_key), _stable_param_value(item_value)]
                for item_key, item_value in sorted(
                    value.items(),
                    key=lambda item: _stable_param_sort_key(_stable_param_value(item[0])),
                )
            ],
        }
    if isinstance(value, (set, frozenset)):
        normalized_items = [_stable_param_value(item) for item in value]
        return {'__type__': 'set', 'items': sorted(normalized_items, key=_stable_param_sort_key)}
    if isinstance(value, tuple):
        return {'__type__': 'tuple', 'items': [_stable_param_value(item) for item in value]}
    if isinstance(value, list):
        return [_stable_param_value(item) for item in value]
    if isinstance(value, datetime):
        return {'__type__': 'datetime', 'value': value.isoformat()}
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    return {'__type__': value.__class__.__qualname__, 'value': str(value)}


def _stable_params_identity(params: Mapping[str, Any]) -> str:
    if not params:
        return ''
    return json.dumps(
        _stable_param_value(params),
        sort_keys=True,
        separators=(',', ':'),
    )


class ExternalContextService:
    def __init__(
        self,
        *,
        registry: ExternalContextProviderRegistry | None = None,
        store: InMemoryExternalContextStore | None = None,
        clock: Clock | None = None,
    ) -> None:
        self.registry = registry or ExternalContextProviderRegistry()
        self.store = store or InMemoryExternalContextStore()
        self.clock = clock or _utcnow

    async def get_context(
        self,
        *,
        key: str,
        params: Mapping[str, Any] | None = None,
        force_refresh: bool = False,
    ) -> ExternalContextResult:
        now = _as_utc(self.clock())
        request = ExternalContextRequest(key=key, params=params or {})
        provider = self.registry.resolve(request)
        if provider is None:
            return self._missing_provider_result(key=key, now=now, force_refresh=force_refresh)

        params_identity = _stable_params_identity(request.params)
        ttl_seconds = max(0, int(provider.ttl_seconds))
        min_refresh_interval_seconds = max(0, int(provider.min_refresh_interval_seconds))
        provider_status = provider.status()
        cached = self.store.load(
            provider_name=provider.provider_name,
            key=key,
            params_identity=params_identity,
        )

        if not provider_status.enabled:
            return self._result_from_record(
                key=key,
                provider=provider,
                provider_status=provider_status,
                record=cached,
                now=now,
                ttl_seconds=ttl_seconds,
                min_refresh_interval_seconds=min_refresh_interval_seconds,
                source_status='disabled',
                served_from_cache=cached is not None and cached.payload is not None,
                cache_hit=cached is not None and cached.payload is not None,
                force_refresh=force_refresh,
                refresh_reason='provider_disabled',
                error=ExternalContextError(
                    error_type='ProviderDisabled',
                    message=provider_status.reason or 'external_context_provider_disabled',
                    recorded_at=now,
                    reason=provider_status.reason or 'provider_disabled',
                ),
            )

        if cached is not None and not force_refresh and self._record_is_fresh(cached, now):
            return self._result_from_record(
                key=key,
                provider=provider,
                provider_status=provider_status,
                record=cached,
                now=now,
                ttl_seconds=ttl_seconds,
                min_refresh_interval_seconds=min_refresh_interval_seconds,
                source_status=cached.source_status,
                served_from_cache=True,
                cache_hit=True,
                force_refresh=False,
                refresh_reason='ttl_cache_hit',
            )

        rate_limited_result = self._rate_limited_result(
            key=key,
            provider=provider,
            provider_status=provider_status,
            record=cached,
            now=now,
            ttl_seconds=ttl_seconds,
            min_refresh_interval_seconds=min_refresh_interval_seconds,
            force_refresh=force_refresh,
        )
        if rate_limited_result is not None:
            return rate_limited_result

        refresh_reason = self._refresh_reason(cached=cached, force_refresh=force_refresh, now=now)
        try:
            fetched = await provider.fetch(request)
        except Exception as exc:
            record = self.store.record_error(
                provider_name=provider.provider_name,
                key=key,
                params_identity=params_identity,
                attempted_at=now,
                error_message=str(exc),
                error_type=exc.__class__.__name__,
            )
            return self._result_from_record(
                key=key,
                provider=provider,
                provider_status=provider_status,
                record=record,
                now=now,
                ttl_seconds=ttl_seconds,
                min_refresh_interval_seconds=min_refresh_interval_seconds,
                source_status='provider_error',
                served_from_cache=record.payload is not None,
                cache_hit=record.payload is not None,
                force_refresh=force_refresh,
                refresh_reason='provider_error',
                stale_reason='refresh_failed' if record.payload is not None else None,
                error=ExternalContextError(
                    error_type=exc.__class__.__name__,
                    message=str(exc),
                    recorded_at=now,
                    reason='provider_error',
                ),
            )

        record = self._save_success(
            provider=provider,
            key=key,
            params_identity=params_identity,
            fetched=fetched,
            fetched_at=now,
            ttl_seconds=ttl_seconds,
            provider_status=provider_status,
        )
        return self._result_from_record(
            key=key,
            provider=provider,
            provider_status=provider_status,
            record=record,
            now=now,
            ttl_seconds=ttl_seconds,
            min_refresh_interval_seconds=min_refresh_interval_seconds,
            source_status=fetched.source_status,
            served_from_cache=False,
            cache_hit=False,
            force_refresh=force_refresh,
            refresh_reason=refresh_reason,
        )

    async def get(
        self,
        *,
        key: str,
        params: Mapping[str, Any] | None = None,
        force_refresh: bool = False,
    ) -> ExternalContextResult:
        return await self.get_context(key=key, params=params, force_refresh=force_refresh)

    def _missing_provider_result(self, *, key: str, now: datetime, force_refresh: bool) -> ExternalContextResult:
        return ExternalContextResult(
            key=key,
            provider=None,
            source_status='disabled',
            payload=None,
            source={
                'provider': None,
                'key_pattern': None,
                'provider_status': {
                    'provider': None,
                    'enabled': False,
                    'status': 'disabled',
                    'reason': 'no_provider_registered',
                },
            },
            freshness=ExternalContextFreshness(
                status='missing',
                served_from_cache=False,
                cache_hit=False,
                ttl_seconds=0,
                min_refresh_interval_seconds=0,
                force_refresh=force_refresh,
                refresh_reason='no_provider_registered',
            ),
            error=ExternalContextError(
                error_type='ProviderNotFound',
                message=f'No external context provider registered for key: {key}',
                recorded_at=now,
                reason='no_provider_registered',
            ),
        )

    def _rate_limited_result(
        self,
        *,
        key: str,
        provider: ExternalContextProvider,
        provider_status: ExternalContextProviderStatus,
        record: ExternalContextCacheRecord | None,
        now: datetime,
        ttl_seconds: int,
        min_refresh_interval_seconds: int,
        force_refresh: bool,
    ) -> ExternalContextResult | None:
        if record is None or record.last_attempt_at is None or min_refresh_interval_seconds <= 0:
            return None
        next_refresh_allowed_at = record.last_attempt_at + timedelta(seconds=min_refresh_interval_seconds)
        if now >= next_refresh_allowed_at:
            return None
        return self._result_from_record(
            key=key,
            provider=provider,
            provider_status=provider_status,
            record=record,
            now=now,
            ttl_seconds=ttl_seconds,
            min_refresh_interval_seconds=min_refresh_interval_seconds,
            source_status='rate_limited',
            served_from_cache=record.payload is not None,
            cache_hit=record.payload is not None,
            force_refresh=force_refresh,
            refresh_reason='min_refresh_interval_active',
            stale_reason='refresh_rate_limited' if not self._record_is_fresh(record, now) else None,
            next_refresh_allowed_at=next_refresh_allowed_at,
            error=ExternalContextError(
                error_type='RateLimited',
                message='External context provider minimum refresh interval has not elapsed',
                recorded_at=now,
                reason='min_refresh_interval_active',
            ),
        )

    def _save_success(
        self,
        *,
        provider: ExternalContextProvider,
        key: str,
        params_identity: str,
        fetched: ExternalContextFetchResult,
        fetched_at: datetime,
        ttl_seconds: int,
        provider_status: ExternalContextProviderStatus,
    ) -> ExternalContextCacheRecord:
        return self.store.save_success(
            provider_name=provider.provider_name,
            key=key,
            params_identity=params_identity,
            payload=fetched.payload,
            source_status=fetched.source_status,
            source_url=fetched.source_url or provider_status.source_url,
            upstream_endpoint=fetched.upstream_endpoint or provider_status.upstream_endpoint,
            fetched_at=fetched_at,
            expires_at=fetched_at + timedelta(seconds=ttl_seconds),
        )

    def _result_from_record(
        self,
        *,
        key: str,
        provider: ExternalContextProvider,
        provider_status: ExternalContextProviderStatus,
        record: ExternalContextCacheRecord | None,
        now: datetime,
        ttl_seconds: int,
        min_refresh_interval_seconds: int,
        source_status: ExternalContextSourceStatus,
        served_from_cache: bool,
        cache_hit: bool,
        force_refresh: bool,
        refresh_reason: str,
        stale_reason: str | None = None,
        next_refresh_allowed_at: datetime | None = None,
        error: ExternalContextError | None = None,
    ) -> ExternalContextResult:
        payload = record.payload if record is not None else None
        freshness_status = self._freshness_status(record, now)
        if source_status == 'provider_error' and payload is not None:
            freshness_status = 'stale'
        if source_status == 'disabled' and payload is None:
            freshness_status = 'missing'
        if stale_reason is None and freshness_status == 'stale':
            stale_reason = 'ttl_expired'

        provider_status_payload = provider_status.as_dict()
        if record is not None:
            provider_status_payload['last_success_at'] = (
                provider_status_payload.get('last_success_at') or record.last_success_at.isoformat()
                if record.last_success_at is not None
                else provider_status_payload.get('last_success_at')
            )
            provider_status_payload['last_error_at'] = (
                provider_status_payload.get('last_error_at') or record.last_error_at.isoformat()
                if record.last_error_at is not None
                else provider_status_payload.get('last_error_at')
            )

        freshness = ExternalContextFreshness(
            status=freshness_status,
            served_from_cache=served_from_cache,
            cache_hit=cache_hit,
            ttl_seconds=ttl_seconds,
            min_refresh_interval_seconds=min_refresh_interval_seconds,
            force_refresh=force_refresh,
            refresh_reason=refresh_reason,
            stale_reason=stale_reason,
            fetched_at=record.fetched_at if record is not None else None,
            expires_at=record.expires_at if record is not None else None,
            age_seconds=_age_seconds(now=now, fetched_at=record.fetched_at if record is not None else None),
            last_attempt_at=record.last_attempt_at if record is not None else None,
            last_success_at=record.last_success_at if record is not None else None,
            last_error_at=record.last_error_at if record is not None else None,
            next_refresh_allowed_at=next_refresh_allowed_at,
        )
        source_url = record.source_url if record is not None else provider_status.source_url
        upstream_endpoint = record.upstream_endpoint if record is not None else provider_status.upstream_endpoint
        return ExternalContextResult(
            key=key,
            provider=provider.provider_name,
            source_status=source_status,
            payload=payload,
            freshness=freshness,
            source={
                'provider': provider.provider_name,
                'key_pattern': provider.key_pattern,
                'source_url': source_url,
                'upstream_endpoint': upstream_endpoint,
                'provider_status': provider_status_payload,
            },
            error=error,
        )

    def _record_is_fresh(self, record: ExternalContextCacheRecord, now: datetime) -> bool:
        return record.expires_at is not None and record.expires_at > now

    def _freshness_status(
        self,
        record: ExternalContextCacheRecord | None,
        now: datetime,
    ) -> ExternalContextFreshnessStatus:
        if record is None or record.payload is None or record.fetched_at is None:
            return 'missing'
        return 'fresh' if self._record_is_fresh(record, now) else 'stale'

    def _refresh_reason(
        self,
        *,
        cached: ExternalContextCacheRecord | None,
        force_refresh: bool,
        now: datetime,
    ) -> str:
        if force_refresh:
            return 'force_refresh'
        if cached is None:
            return 'cache_miss'
        if not self._record_is_fresh(cached, now):
            return 'ttl_expired'
        return 'refresh_requested'


external_context_registry = ExternalContextProviderRegistry()
external_context_service = ExternalContextService(registry=external_context_registry)
