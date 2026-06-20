from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import unittest

from app.external_context import (
    ExternalContextFetchResult,
    ExternalContextProviderRegistry,
    ExternalContextProviderStatus,
    ExternalContextRequest,
    ExternalContextService,
    InMemoryExternalContextStore,
)


@dataclass
class MutableClock:
    current: datetime

    def __call__(self) -> datetime:
        return self.current

    def advance(self, *, seconds: int) -> None:
        self.current += timedelta(seconds=seconds)


class FakeExternalContextProvider:
    provider_name = 'fake_external_context'
    key_pattern = 'news:symbol:*'

    def __init__(
        self,
        *,
        ttl_seconds: int = 60,
        min_refresh_interval_seconds: int = 0,
        enabled: bool = True,
        responses: list[ExternalContextFetchResult | Exception] | None = None,
    ) -> None:
        self.ttl_seconds = ttl_seconds
        self.min_refresh_interval_seconds = min_refresh_interval_seconds
        self.enabled = enabled
        self.responses = list(responses or [])
        self.requests: list[ExternalContextRequest] = []

    def supports(self, request: ExternalContextRequest) -> bool:
        return request.key.startswith('news:symbol:')

    def status(self) -> ExternalContextProviderStatus:
        return ExternalContextProviderStatus(
            provider=self.provider_name,
            enabled=self.enabled,
            status='available' if self.enabled else 'disabled',
            reason=None if self.enabled else 'fake_provider_disabled',
            source_url='https://example.test/source',
            upstream_endpoint='https://example.test/api/context',
        )

    async def fetch(self, request: ExternalContextRequest) -> ExternalContextFetchResult:
        self.requests.append(request)
        if not self.responses:
            raise AssertionError('No fake external context response configured')
        response = self.responses.pop(0)
        if isinstance(response, Exception):
            raise response
        return response


def _build_service(provider: FakeExternalContextProvider, clock: MutableClock) -> ExternalContextService:
    registry = ExternalContextProviderRegistry()
    registry.register(provider)
    return ExternalContextService(
        registry=registry,
        store=InMemoryExternalContextStore(),
        clock=clock,
    )


def _available_payload(version: int) -> ExternalContextFetchResult:
    return ExternalContextFetchResult(
        payload={'version': version},
        source_status='available',
        source_url=f'https://example.test/context/{version}',
        upstream_endpoint='https://example.test/api/context',
    )


class ExternalContextServiceTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.clock = MutableClock(datetime(2026, 5, 8, 0, 0, tzinfo=timezone.utc))

    async def test_fresh_cache_hit_uses_cached_payload_without_refetch(self) -> None:
        provider = FakeExternalContextProvider(
            ttl_seconds=60,
            responses=[_available_payload(1), _available_payload(2)],
        )
        service = _build_service(provider, self.clock)

        first = await service.get_context(key='news:symbol:005930')
        self.clock.advance(seconds=10)
        second = await service.get_context(key='news:symbol:005930')

        self.assertEqual(first.payload, {'version': 1})
        self.assertEqual(second.payload, {'version': 1})
        self.assertEqual(len(provider.requests), 1)
        self.assertFalse(first.freshness.served_from_cache)
        self.assertTrue(second.freshness.served_from_cache)
        self.assertEqual(second.source_status, 'available')
        self.assertEqual(second.freshness.status, 'fresh')
        self.assertEqual(second.freshness.refresh_reason, 'ttl_cache_hit')

    async def test_cache_identity_includes_params_for_ttl_hits(self) -> None:
        provider = FakeExternalContextProvider(
            ttl_seconds=60,
            responses=[_available_payload(1), _available_payload(2)],
        )
        service = _build_service(provider, self.clock)

        first = await service.get_context(key='news:symbol:005930', params={'window': '1d'})
        self.clock.advance(seconds=10)
        second = await service.get_context(key='news:symbol:005930', params={'window': '5d'})
        third = await service.get_context(key='news:symbol:005930', params={'window': '1d'})

        self.assertEqual(first.payload, {'version': 1})
        self.assertEqual(second.payload, {'version': 2})
        self.assertEqual(third.payload, {'version': 1})
        self.assertEqual(len(provider.requests), 2)
        self.assertFalse(second.freshness.cache_hit)
        self.assertFalse(second.freshness.served_from_cache)
        self.assertEqual(second.freshness.refresh_reason, 'cache_miss')
        self.assertTrue(third.freshness.cache_hit)
        self.assertEqual(third.freshness.refresh_reason, 'ttl_cache_hit')

    async def test_equivalent_param_mappings_share_cache_identity(self) -> None:
        provider = FakeExternalContextProvider(
            ttl_seconds=60,
            responses=[_available_payload(1), _available_payload(2)],
        )
        service = _build_service(provider, self.clock)

        first = await service.get_context(
            key='news:symbol:005930',
            params={
                'locale': 'ko',
                'filters': {'market': 'KOSPI', 'source': 'news'},
            },
        )
        second = await service.get_context(
            key='news:symbol:005930',
            params={
                'filters': {'source': 'news', 'market': 'KOSPI'},
                'locale': 'ko',
            },
        )

        self.assertEqual(first.payload, {'version': 1})
        self.assertEqual(second.payload, {'version': 1})
        self.assertEqual(len(provider.requests), 1)
        self.assertTrue(second.freshness.cache_hit)
        self.assertEqual(second.freshness.refresh_reason, 'ttl_cache_hit')

    async def test_stale_cache_refreshes_after_ttl_expiry(self) -> None:
        provider = FakeExternalContextProvider(
            ttl_seconds=5,
            responses=[_available_payload(1), _available_payload(2)],
        )
        service = _build_service(provider, self.clock)

        await service.get_context(key='news:symbol:005930')
        self.clock.advance(seconds=6)
        refreshed = await service.get_context(key='news:symbol:005930')

        self.assertEqual(refreshed.payload, {'version': 2})
        self.assertEqual(len(provider.requests), 2)
        self.assertFalse(refreshed.freshness.served_from_cache)
        self.assertEqual(refreshed.freshness.refresh_reason, 'ttl_expired')
        self.assertEqual(refreshed.freshness.status, 'fresh')

    async def test_provider_error_preserves_last_good_payload_with_stale_metadata(self) -> None:
        provider = FakeExternalContextProvider(
            ttl_seconds=5,
            responses=[_available_payload(1), RuntimeError('upstream timeout')],
        )
        service = _build_service(provider, self.clock)

        await service.get_context(key='news:symbol:005930')
        self.clock.advance(seconds=6)
        result = await service.get_context(key='news:symbol:005930')

        self.assertEqual(result.payload, {'version': 1})
        self.assertEqual(result.source_status, 'provider_error')
        self.assertEqual(result.freshness.status, 'stale')
        self.assertEqual(result.freshness.stale_reason, 'refresh_failed')
        self.assertTrue(result.freshness.served_from_cache)
        self.assertEqual(result.error.error_type, 'RuntimeError')
        self.assertEqual(result.error.message, 'upstream timeout')
        self.assertEqual(result.freshness.last_success_at.isoformat(), '2026-05-08T00:00:00+00:00')
        self.assertEqual(result.freshness.last_error_at.isoformat(), '2026-05-08T00:00:06+00:00')

    async def test_provider_error_does_not_return_payload_cached_for_different_params(self) -> None:
        provider = FakeExternalContextProvider(
            ttl_seconds=0,
            responses=[_available_payload(1), RuntimeError('upstream timeout')],
        )
        service = _build_service(provider, self.clock)

        await service.get_context(key='news:symbol:005930', params={'window': '1d'})
        self.clock.advance(seconds=1)
        result = await service.get_context(key='news:symbol:005930', params={'window': '5d'})

        self.assertIsNone(result.payload)
        self.assertEqual(result.source_status, 'provider_error')
        self.assertEqual(result.freshness.status, 'missing')
        self.assertFalse(result.freshness.served_from_cache)
        self.assertFalse(result.freshness.cache_hit)
        self.assertEqual(result.error.error_type, 'RuntimeError')
        self.assertEqual(len(provider.requests), 2)

    async def test_disabled_provider_returns_disabled_status_without_fetch(self) -> None:
        provider = FakeExternalContextProvider(enabled=False, responses=[_available_payload(1)])
        service = _build_service(provider, self.clock)

        result = await service.get_context(key='news:symbol:005930')

        self.assertIsNone(result.payload)
        self.assertEqual(result.source_status, 'disabled')
        self.assertEqual(result.freshness.status, 'missing')
        self.assertEqual(result.freshness.refresh_reason, 'provider_disabled')
        self.assertEqual(result.error.reason, 'fake_provider_disabled')
        self.assertEqual(provider.requests, [])

    async def test_force_refresh_bypasses_fresh_ttl(self) -> None:
        provider = FakeExternalContextProvider(
            ttl_seconds=60,
            responses=[_available_payload(1), _available_payload(2)],
        )
        service = _build_service(provider, self.clock)

        await service.get_context(key='news:symbol:005930')
        self.clock.advance(seconds=10)
        result = await service.get_context(key='news:symbol:005930', force_refresh=True)

        self.assertEqual(result.payload, {'version': 2})
        self.assertEqual(len(provider.requests), 2)
        self.assertFalse(result.freshness.served_from_cache)
        self.assertTrue(result.freshness.force_refresh)
        self.assertEqual(result.freshness.refresh_reason, 'force_refresh')

    async def test_force_refresh_respects_provider_min_refresh_interval(self) -> None:
        provider = FakeExternalContextProvider(
            ttl_seconds=60,
            min_refresh_interval_seconds=30,
            responses=[_available_payload(1), _available_payload(2)],
        )
        service = _build_service(provider, self.clock)

        await service.get_context(key='news:symbol:005930')
        self.clock.advance(seconds=10)
        result = await service.get_context(key='news:symbol:005930', force_refresh=True)

        self.assertEqual(result.payload, {'version': 1})
        self.assertEqual(result.source_status, 'rate_limited')
        self.assertEqual(result.freshness.status, 'fresh')
        self.assertEqual(result.freshness.refresh_reason, 'min_refresh_interval_active')
        self.assertEqual(result.freshness.next_refresh_allowed_at.isoformat(), '2026-05-08T00:00:30+00:00')
        self.assertTrue(result.freshness.served_from_cache)
        self.assertEqual(len(provider.requests), 1)

    async def test_min_refresh_interval_is_scoped_to_params_identity(self) -> None:
        provider = FakeExternalContextProvider(
            ttl_seconds=0,
            min_refresh_interval_seconds=30,
            responses=[_available_payload(1), _available_payload(2)],
        )
        service = _build_service(provider, self.clock)

        await service.get_context(key='news:symbol:005930', params={'window': '1d'})
        self.clock.advance(seconds=10)
        result = await service.get_context(key='news:symbol:005930', params={'window': '5d'})

        self.assertEqual(result.payload, {'version': 2})
        self.assertEqual(result.source_status, 'available')
        self.assertEqual(result.freshness.refresh_reason, 'cache_miss')
        self.assertFalse(result.freshness.served_from_cache)
        self.assertEqual(len(provider.requests), 2)

    async def test_empty_provider_result_uses_consistent_result_shape(self) -> None:
        provider = FakeExternalContextProvider(
            ttl_seconds=60,
            responses=[ExternalContextFetchResult(payload=[], source_status='empty')],
        )
        service = _build_service(provider, self.clock)

        result = await service.get_context(key='news:symbol:005930')
        payload = result.as_dict()

        self.assertEqual(payload['source_status'], 'empty')
        self.assertEqual(payload['payload'], [])
        self.assertEqual(payload['freshness']['status'], 'fresh')
        self.assertEqual(payload['source']['provider'], 'fake_external_context')
        self.assertEqual(payload['source']['provider_status']['status'], 'available')
        self.assertIsNone(payload['error'])
