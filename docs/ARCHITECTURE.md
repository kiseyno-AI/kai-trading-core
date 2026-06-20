# Architecture Docs

Use this file as the architecture index.

## Primary docs

- [`docs/REPOSITORY_ARCHITECTURE.md`](./REPOSITORY_ARCHITECTURE.md) - concrete repository walkthrough with Mermaid diagrams for layers, EOD flow, DART flow, technical/action-summary flow, agent-context composition, and API/CLI/job placement
- [`../ARCHITECTURE.md`](../ARCHITECTURE.md) - short top-level summary of current direction and major module groups
- [`docs/API_BUILD_ORDER.md`](./API_BUILD_ORDER.md) - implemented and planned HTTP surface
- [`docs/EXTERNAL_CONTEXT_SOURCE_CATALOG.md`](./EXTERNAL_CONTEXT_SOURCE_CATALOG.md) - external macro/news/research/market/risk source catalog with access, TTL, intended use, and direct-signal policy
- [`../RUNBOOK.md`](../RUNBOOK.md) - operational start/run/smoke-test guidance

## What to read for structure

If you want to understand how the implemented backend is organized today, start with:

1. [`docs/REPOSITORY_ARCHITECTURE.md`](./REPOSITORY_ARCHITECTURE.md)
2. [`../ARCHITECTURE.md`](../ARCHITECTURE.md)
3. [`docs/API_BUILD_ORDER.md`](./API_BUILD_ORDER.md)

## Scope reminder

The detailed repository architecture doc is intentionally implementation-aligned. It describes the current modular-monolith backend and its persisted data/read-model layers, not an aspirational future system.
