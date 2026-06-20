from __future__ import annotations

from typing import Protocol

from app.external_context.models import ExternalContextFetchResult, ExternalContextProviderStatus, ExternalContextRequest


class ExternalContextProvider(Protocol):
    provider_name: str
    key_pattern: str
    ttl_seconds: int
    min_refresh_interval_seconds: int

    def supports(self, request: ExternalContextRequest) -> bool: ...

    def status(self) -> ExternalContextProviderStatus: ...

    async def fetch(self, request: ExternalContextRequest) -> ExternalContextFetchResult: ...


class ExternalContextProviderRegistry:
    def __init__(self) -> None:
        self._providers: dict[str, ExternalContextProvider] = {}

    def register(self, provider: ExternalContextProvider) -> None:
        if not provider.provider_name:
            raise ValueError('external context provider_name is required')
        if provider.provider_name in self._providers:
            raise ValueError(f'external context provider already registered: {provider.provider_name}')
        self._providers[provider.provider_name] = provider

    def get(self, provider_name: str) -> ExternalContextProvider | None:
        return self._providers.get(provider_name)

    def resolve(self, request: ExternalContextRequest) -> ExternalContextProvider | None:
        for provider in self._providers.values():
            if provider.supports(request):
                return provider
        return None

    def providers(self) -> list[ExternalContextProvider]:
        return list(self._providers.values())

    def status_payloads(self) -> list[dict]:
        return [provider.status().as_dict() for provider in self.providers()]
