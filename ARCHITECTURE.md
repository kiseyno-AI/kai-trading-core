# ARCHITECTURE.md

## Current Direction
- Modular monolith first
- KR broker: direct KIS REST integration
- US data and guarded direct execution paths are implemented in Core; the always-on broker lifecycle belongs to `kai-trading-exec`
- Backend/API: **FastAPI** (`apps/api`)
- Data/storage: PostgreSQL; Redis and Parquet/DuckDB remain roadmap items
- Research: vectorbt (roadmap)

## API service layout (`apps/api`)
- **`app/main.py`** — FastAPI app, CORS, bearer middleware, router registration, lifespan (`background_runner`).
- **`app/core/settings.py`** — Pydantic Settings / `.env` (KIS, BOK, Korea Exim, `UPSTREAM_BEARER_TOKEN`, etc.).
- **`app/core/audit_logging.py`** — rotating JSONL audit logs for inbound API requests and outbound upstream calls with request-id propagation and redaction.
- **`app/db.py` + `db_migrations/`** — SQLAlchemy engine/session helpers and Alembic schema migrations.
- **`app/api/routes_*.py`** — HTTP routers (KIS, macro, FX, scanner, market, quotes, symbols, EOD).
- **`app/dart/`** — DART company snapshot logic plus incremental disclosure-event sync, fundamentals capture, and DART persistence/repository code.
- **`app/brokers/kis/`** — KIS client/provider/schemas.
- **`app/orders/next_open_execution.py`** — persisted next-open execution planner that derives signal-decider buy candidates, applies account/allocation safety gates, and submits through the durable order flow at market open.
- **`app/orders/intraday_auto_sell.py`** — persisted intraday watcher for linked next-open positions that evaluates live target/stop thresholds, suppresses duplicate exits, and submits sell orders through the durable order flow.
- **`app/integrations/`** — BOK, FX (Korea Exim), etc.
- **`app/fx/`** — Korea Exim FX service-level normalization, filtering, and date fallback behavior.
- **`app/macro_context/`** — persisted slow-moving macro context refresh/read model for decider inputs; contextual only, not a direct trading signal.
- **`app/external_context/`** — foundational provider registry/service for slow-moving external context keys with TTL, minimum refresh interval, source status, freshness, and last-good cache metadata. It is internal-only until a concrete macro/news/research provider is explicitly wired.
- **`app/quotes/`**, **`app/market/`**, **`app/scanner/`**, **`app/eod/`**, **`app/signals/`** — domain services and background behavior, including pykrx-based historical EOD backfill and daily KIS ranking signal features.

## Documentation
- **`docs/API_BUILD_ORDER.md`** — implemented vs planned HTTP routes (keep in sync with code).
- **`docs/EXTERNAL_CONTEXT_SOURCE_CATALOG.md`** — approved external context source catalog, access requirements, TTL expectations, and direct-signal policy.
- **`docs/ARCHITECTURE.md`**, **`docs/KIS_API_ONBOARDING.md`**, **`docs/scanner/`** — deeper notes.

## Tooling and quality
- **Codex:** [`.codex/config.toml`](./.codex/config.toml), [`.codex/skills/`](./.codex/skills/), [`.codex/agents/`](./.codex/agents/).
- **CI:** [`.github/workflows/ci.yml`](./.github/workflows/ci.yml) — Python 3.11/3.12 compile, import, migration, and full PostgreSQL-backed test gates.
- **Ops:** [RUNBOOK.md](./RUNBOOK.md) — start script, health and EOD endpoints.

## Core Areas
- market-data and disclosure collection pipelines
- deterministic factors, agent context, calibration, and replay
- guarded plan generation and Exec handoff/readback
- compatibility-only direct KIS execution paths
- FastAPI read and operations surface
