# ARCHITECTURE.md

## Current Direction

- Modular monolith first
- KR broker: direct KIS REST integration
- US data, screening, and agent planning are implemented in Core. The current US execution-plan builder is preview-only; delegated broker execution belongs to `kai-trading-exec` (see [US boundary](docs/architecture/README.md#us-equities))
- Backend/API: **FastAPI** (`apps/api`)
- Data/storage: PostgreSQL; Redis and Parquet/DuckDB remain roadmap items
- Research: vectorbt (roadmap)

## API service layout (`apps/api`)

- **`app/agent_context/wait_paper_ledger.py`** — independent observation-only WAIT lifecycles for both deciders, with frozen quantities/costs, target/stop exits and explicit recovery gaps; no broker submission. See [WAIT paper ledger](docs/WAIT_PAPER_LEDGER.md).
- **`app/main.py`** — FastAPI app, CORS, bearer middleware, router registration, lifespan (`background_runner`).
- **`app/core/settings.py`** — Pydantic Settings / `.env` (KIS, BOK, Korea Exim, `UPSTREAM_BEARER_TOKEN`, etc.).
- **`app/core/audit_logging.py`** — rotating JSONL audit logs for inbound API requests and outbound upstream calls with request-id propagation and redaction.
- **`app/db.py` + `db_migrations/`** — SQLAlchemy engine/session helpers and Alembic schema migrations.
- **`app/api/routes_*.py`** — HTTP routers (KIS, macro, FX, scanner, market, quotes, symbols, EOD).
- **`app/dart/`** — DART company snapshot logic plus incremental disclosure-event sync, fundamentals capture, and DART persistence/repository code.
- **`app/brokers/kis/`** — KIS client/provider/schemas.
- **`app/orders/next_open_execution.py`** — persisted next-open planner and safety checks. Core-local next-open BUY submission is disabled in every mode; `app/orders/exec_integration.py` owns Core-side plan registration, activation, and readback (see [execution boundary](docs/architecture/README.md#execution)).
- **`app/orders/core_quantity_sizing.py` + `core_quantity_refresh.py`** — deterministic account/cohort sizing and durable preopen/open/two-hour quantity revisions. Core owns quantities; Exec owns orders and execution-time checks. Decider prices and filled inventory remain unchanged (see [quantity planning](docs/CORE_QUANTITY_SIZING.md)).
- **`app/orders/intraday_auto_sell.py`** — retained compatibility watcher for linked positions. It checks target/stop thresholds, current quantities, duplicate exits, and dry-run settings before the durable sell flow; disabled by default (see [auto-sell](docs/architecture/README.md#auto-sell)).
- **`app/integrations/`** — BOK, FX (Korea Exim), etc.
- **`app/fx/`** — Korea Exim FX service-level normalization, filtering, and date fallback behavior.
- **`app/macro_context/`** — persisted slow-moving macro context refresh/read model for decider inputs; contextual only, not a direct trading signal.
- **`app/forecast_context/`** — optional TimesFM 3.0 personal-study evidence prepared before both final deciders inside the daily `signal_decider` stage, with a visible `timesfm_forecast` sub-step. The native Apple Silicon launcher enables a provisioned worker unless explicitly disabled. Uses stored KRX close histories, an isolated MLX worker, input hashes, and existing request snapshots; see [setup and timing](docs/TIMESFM3_INTEGRATION.md).
- **`app/external_context/`** — foundational provider registry/service for slow-moving external context keys with TTL, minimum refresh interval, source status, freshness, and last-good cache metadata. It is internal-only until a concrete macro/news/research provider is explicitly wired.
- **`app/market/us_index_snapshot_store.py`** — append-only KIS US-index input evidence with exact prices, frozen calendar windows, receipt timestamps and content hashes; internal save/read only, without collection scheduling or trading.
- **`app/market/us_index_collector.py`** — independent manual KIS index fetch/validate/store orchestration; `app.internal.us_index_collect` provides the CLI with per-index failure isolation and no daily-pipeline or trading caller.
- **`app/market/us_index_preopen.py`** — isolated, opt-in preopen collection slots with durable attempt claims and committed-capture recovery; a standalone CLI/launchd job, without daily-runner or trading integration. See [operations and limits](docs/US_INDEX_PREOPEN_COLLECTION.md).
- **`app/market/us_index_input_freeze.py`** — immutable, file-backed selection and seal for the exact three committed preopen index captures; the standalone CLI attempts freezing after complete collection, and internal reads replay the sealed IDs without provider or calendar calls. See [cutoff and recovery contract](docs/US_INDEX_INPUT_FREEZE.md).
- **`app/market/us_index_features.py`** — pure descriptive returns, acceleration, return spreads and lag-only 20-session statistics from explicit sealed inputs; `app.internal.us_index_features` reads exact dates without collection or trading integration. Missing history remains unavailable. See [formulas and replay](docs/US_INDEX_FEATURES.md).
- **[Opt-in US-index history](docs/US_INDEX_HISTORY.md)** — 22 exact closes in append-only schema-2 captures, a separate history seal and version-2 descriptive features; shares existing request claims and cutoff, with all two-close defaults unchanged.
- **`app/overnight_rebound/`** — disabled-by-default, independent US-rebound observations using current history seals and exploratory rules; durable evaluation IDs, standalone CLI/launchd polling and `GET /strategies/us-rebound/status`. No paper/live order connection or daily-agent dependency. See [activation, evidence and rollback](docs/US_REBOUND_OPERATION.md).
- **`app/quotes/`**, **`app/market/`**, **`app/scanner/`**, **`app/eod/`**, **`app/signals/`** — domain services and background behavior, including pykrx-based historical EOD backfill and daily KIS ranking signal features.
- **`app/analytics/`** — read-only signal evaluation and deterministic improvement findings; the opt-in weekly improvement script proposes bounded contract-note PRs or code issues, with merge and deployment remaining separate (see [signal lab](docs/architecture/README.md#signal-lab) and [improvement loop](docs/architecture/README.md#agent-improvement)).

## Documentation

- **[System and feature architecture](docs/architecture/README.md)** — English source-backed diagrams, per-feature boundaries, API coverage, and Graphify source-relationship counts; editable source in `docs/architecture/model.json`.
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
