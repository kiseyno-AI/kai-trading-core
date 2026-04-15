# trading-core (kai-trading-core)

Core trading platform repository for the KAI system. This repo is the backend and data-service layer, not the web UI.

## What this project is

| Area | Status | Notes |
|------|--------|--------|
| **HTTP API** | Active | FastAPI app in `apps/api`, default `http://127.0.0.1:8000` |
| **KIS (Korea Investment & Securities)** | Integrated | Token, health, domestic quotes; mock vs production via env |
| **Market clock & quotes** | Active | Market state, tracked quotes, symbol search from master list |
| **Scanner** | Active | In-memory tracked symbol list backed by DB persistence |
| **EOD bars pipeline** | Active | Master refresh, scheduled capture hooks, pykrx backfill, status endpoints |
| **Post-EOD technical snapshots** | Active | DB-backed deterministic signal layer keyed by symbol/day |
| **Deterministic daily action summaries** | Active | DB-backed machine-friendly action context keyed by symbol/day |
| **DART disclosures + fundamentals history** | Active | Sync, capture, history retrieval, current-universe backfill |
| **Agent aggregate read layer** | Active | Structured symbol/day context for downstream agents |
| **Macro (BOK ECOS)** | Integrated | Statistic series proxy |
| **FX (Korea Exim)** | Integrated | Exchange rate lookup |
| **Execution engine / order persistence** | Roadmap | Trading and execution boundaries remain future work |

**Tech stack:** Python 3, FastAPI, SQLAlchemy, Alembic, HTTPX, Pydantic Settings, Postgres.

**Primary in-repo docs:** `AGENTS.md`, `ARCHITECTURE.md`, `RUNBOOK.md`, `docs/REPOSITORY_ARCHITECTURE.md`, `docs/API_BUILD_ORDER.md`, `docs/ARCHITECTURE.md`, `docs/KIS_API_ONBOARDING.md`, `docs/scanner/`.

## Architecture docs

- Repository walkthrough with diagrams: `docs/REPOSITORY_ARCHITECTURE.md`
- Docs index: `docs/ARCHITECTURE.md`
- Top-level summary: `ARCHITECTURE.md`
- API surface inventory: `docs/API_BUILD_ORDER.md`

## Current implemented architecture

The current backend is a modular monolith centered on the FastAPI service in `apps/api`. It now has five practical layers:

1. External integrations for KIS, DART, BOK/ECOS, and Korea Exim.
2. Persistence for symbol master data, intraday quote snapshots, market-status snapshots, EOD bars, DART disclosure events, and DART fundamentals snapshots.
3. Post-EOD deterministic computation layers for technical snapshots and action summaries.
4. Operator-facing jobs and HTTP routes for ingestion, capture, backfill, and inspection.
5. Agent-facing aggregate read APIs that package stored context for symbol/day analysis without making final ranking decisions.

The intended split is deliberate:

- Core services persist normalized facts and deterministic computed context.
- The read layer returns structured blocks for agents and subagents.
- Prioritization, ranking, and interpretation stay outside the core API.

## Authentication

If `UPSTREAM_BEARER_TOKEN` is set, every request except `OPTIONS` must include:

```http
Authorization: Bearer <same token>
```

If `UPSTREAM_BEARER_TOKEN` is unset or empty, the API accepts requests without a bearer token, which is typical for local development.

For local orchestration on the same machine, prefer direct internal commands from `apps/api` instead of looping back through `http://127.0.0.1:8000`:

```bash
python3.11 -m app.internal.ops market-status
python3.11 -m app.internal.ops quotes-tracked
python3.11 -m app.internal.ops scanner-tracked-symbols
python3.11 -m app.internal.ops eod-status
python3.11 -m app.internal.ops eod-pykrx-daily-run-if-due --market-date 2026-04-08 --force
```

Those commands call the repo services directly, so they do not fail on bearer middleware. If an internal integration must stay on HTTP, use `app.internal.http_client.InternalAPIClient`, which automatically sends the configured bearer token.

## Data flow overview

### 1. Instrument master and quote layer

Persisted tables in this layer:

- `symbols`
- `tracked_symbols`
- `quote_snapshots`
- `current_quote_cache`
- `market_status_snapshots`

These power the market, scanner, quotes, and symbols endpoints.

### 2. EOD bars flow

The repository currently supports both:

- KIS-driven EOD capture for a target `market_date`
- pykrx-based historical and daily backfill for broader bar coverage

Persisted EOD tables:

- `eod_collection_runs` for run audit and progress
- `daily_price_bars` for durable OHLCV history keyed by `symbol_id + trade_date`

Operationally, the current flow supports:

- master refresh before collection
- due-check based capture
- direct capture for a specific market date
- pykrx universe refresh
- pykrx historical backfill
- pykrx daily run-if-due

### 3. Post-EOD technical snapshot layer

After daily bars exist for a symbol/day, the system can compute and persist a deterministic technical context row in `daily_technical_snapshots`.

Current snapshot fields include:

- close and volume
- MA alignment
- trend state
- 5d and 20d returns
- 20d breakout and breakdown flags
- 20d volume ratio
- ATR and ATR percent
- compact `signal_payload` context

This layer is intentionally not a ranking model. It is a reusable, deterministic summary of chart state derived from stored bars.

### 4. Deterministic daily action summary layer

Once a technical snapshot exists, the system can compute and persist a deterministic action-summary object in `daily_action_summaries`.

Current action summary fields include:

- `action_label`
- `bias_direction`
- `signal_score`
- `confidence_tier`
- `risk_tier`
- `reasons`
- trigger, invalidation, support, and resistance levels
- compact `summary_payload`

This is still not the final agent decision layer. It is a stable, rules-based read model that agents can consume consistently.

### 5. DART disclosures and fundamentals ingestion/history

Implemented DART pieces:

- company snapshot lookup
- disclosure event sync for recent filings
- persistent `dart_disclosure_events` and `dart_disclosure_sync_runs`
- fundamentals capture from synced financial-report disclosures
- persistent `dart_fundamentals_snapshots`, `dart_fundamentals_rows`, and highlight rows
- stored fundamentals history retrieval across one or many symbols
- current-active-universe fundamentals backfill operator
- filing document and XBRL download routes

In practice, the DART pipeline works like this:

1. Sync recent disclosure events from Open DART.
2. Persist the financial-report events the fundamentals flow depends on.
3. Capture statement snapshots from those events.
4. Persist both normalized highlights and full statement rows.
5. Serve stored history for later read-side aggregation.

### 6. Agent context aggregate read layer

The latest implemented read layer is:

- `GET /agent-context/symbol-day`

This route supports one or many symbols for a given `market_date` and returns a compact machine-friendly payload that aggregates:

- company identity and symbol metadata
- same-day technical snapshot when available
- same-day deterministic action summary when available
- latest stored DART fundamentals snapshot summary and short history summary
- recent stored DART disclosure events up to the requested market date

This route is specifically for agent consumption. It does not emit final prioritization or ranking decisions.

## HTTP API reference

Base URL (local default): `http://127.0.0.1:8000`

Interactive docs when the server is running:

- `GET /docs`
- `GET /redoc`

### Core and health

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/health` | Liveness: app, env, database status |

### Broker - KIS (`/broker/kis`)

| Method | Path | Query / body | Description |
|--------|------|----------------|-------------|
| `GET` | `/broker/kis/health` | - | KIS connectivity check |
| `POST` | `/broker/kis/token` | `force_refresh` | Issue or refresh KIS access token |
| `GET` | `/broker/kis/quote` | `symbol`, `market_code` | Domestic quote via KIS |

### Market and quotes

| Method | Path | Query / body | Description |
|--------|------|----------------|-------------|
| `GET` | `/market/status` | - | Current market state |
| `GET` | `/quotes/tracked` | - | Quotes for tracked scanner symbols with explicit fallback provenance |
| `GET` | `/quotes/{symbol}` | path symbol | Single-symbol quote with explicit fallback provenance |

Quote fallback policy:
- Prefer live KIS quote data when the market is open and the upstream call succeeds.
- If live data is unavailable, prefer DB-backed `daily_price_bars` for the current market date.
- In `PREOPEN` or `WEEKEND`, allow the most recent `daily_price_bars` row only when it is within 3 calendar days of the Seoul market date.
- Same-day in-memory or `current_quote_cache` fallbacks are allowed only when their `cached_at` timestamp matches the current Seoul market date.
- Multi-day stale quote cache is rejected. When no acceptable fresh fallback exists, quote responses return `available: false`, `source: "unavailable"`, and an `unavailable_reason` instead of misleading stale numbers.

### Scanner (`/scanner`)

| Method | Path | Query / body | Description |
|--------|------|----------------|-------------|
| `GET` | `/scanner/tracked-symbols` | - | Current tracked symbol snapshot |
| `POST` | `/scanner/tracked-symbols` | JSON `{ "symbols": [...] }` | Replace tracked symbols |

### Symbols (`/symbols`)

| Method | Path | Query / body | Description |
|--------|------|----------------|-------------|
| `GET` | `/symbols/search` | `q`, `limit` | Search master symbols by code or Korean name |
| `GET` | `/symbols/{symbol}/price-bars` | `start_date`, `end_date`, `limit` | Read stored daily OHLCV history from `daily_price_bars` |

### Macro - Bank of Korea ECOS (`/macro/bok`)

| Method | Path | Query / body | Description |
|--------|------|----------------|-------------|
| `GET` | `/macro/bok/series` | `series_id`, `start_period`, `end_period`, `cycle` | ECOS statistic series proxy |

### FX - Korea Exim (`/fx/koreaexim`)

| Method | Path | Query / body | Description |
|--------|------|----------------|-------------|
| `GET` | `/fx/koreaexim/rates` | `search_date`, `data` | Exchange rates |

### DART (`/disclosures/dart`)

| Method | Path | Query / body | Description |
|--------|------|----------------|-------------|
| `GET` | `/disclosures/dart/company-snapshot` | `corp_code` or `stock_code` or `corp_name` | Company lookup plus recent filing snapshot |
| `POST` | `/disclosures/dart/sync/events` | `days` | Sync recent disclosure events |
| `GET` | `/disclosures/dart/sync/inspect` | `runs_limit`, `events_limit`, `financial_only` | Inspect recent sync runs and stored events |
| `GET` | `/disclosures/dart/fundamentals` | `business_year` or `fiscal_year`, lookup params, period params | Direct single-company DART fundamentals fetch |
| `POST` | `/disclosures/dart/fundamentals/capture` | `limit`, `force`, `fs_div` | Capture fundamentals from stored disclosure events |
| `GET` | `/disclosures/dart/fundamentals/history` | repeatable `symbols`, date window, `fs_div` | Read stored DART fundamentals history |
| `GET` | `/disclosures/dart/fundamentals/capture/inspect` | `runs_limit`, `events_limit`, `capture_status` | Inspect capture runs and capture status |
| `POST` | `/disclosures/dart/fundamentals/backfill/current-universe` | `symbol_limit`, `symbol_offset`, `capture_limit`, `force`, `fs_div`, date window | Batch backfill current active universe fundamentals |
| `GET` | `/disclosures/dart/filings/document` | `rcept_no` | Download filing artifact |
| `GET` | `/disclosures/dart/filings/xbrl` | `rcept_no`, `report_code` | Download filing XBRL artifact |

### EOD (`/eod`)

| Method | Path | Query / body | Description |
|--------|------|----------------|-------------|
| `GET` | `/eod/status` | - | EOD job, master refresh, and pykrx-auto snapshot |
| `GET` | `/eod/freshness` | - | Current trade-date EOD freshness plus latest derived-data dates |
| `POST` | `/eod/master/refresh-if-due` | `force` | Refresh symbol master if due |
| `POST` | `/eod/capture-if-due` | - | Run EOD capture when due |
| `POST` | `/eod/capture` | `market_date` | Capture EOD for a specific market date |
| `POST` | `/eod/pykrx/universe/refresh` | `markets`, `limit` | Refresh pykrx active universe |
| `POST` | `/eod/pykrx/backfill` | `start_date`, `end_date`, `markets`, `limit`, `symbol`, `refresh_universe` | Historical pykrx OHLCV backfill |
| `POST` | `/eod/pykrx/daily/run-if-due` | `market_date`, `markets`, `force`, `refresh_universe` | pykrx daily collection run |
| `POST` | `/eod/technical-snapshots/compute` | `market_date`, `symbols`, `source` | Compute and persist technical snapshots |
| `GET` | `/eod/technical-snapshots` | `market_date`, `symbols`, `limit` | Read persisted technical snapshots |
| `POST` | `/eod/action-summaries/compute` | `market_date`, `symbols`, `source` | Compute and persist action summaries |
| `GET` | `/eod/action-summaries` | `market_date`, `symbols`, `limit` | Read persisted action summaries |

`GET /eod/freshness` returns a stable, web-oriented payload:

```json
{
  "today": {
    "trade_date": "2026-04-10",
    "eod_status": "completed",
    "symbols_total": 950,
    "symbols_succeeded": 950,
    "symbols_failed": 0,
    "received": true
  },
  "latest": {
    "price_trade_date": "2026-04-10",
    "technical_market_date": "2026-04-10",
    "action_market_date": "2026-04-10"
  }
}
```

Behavior notes:
- `today.trade_date` comes from the current Seoul market server date.
- `eod_status` resolves to `running`, `completed`, `completed_with_errors`, or `missing`.
- If same-day bars exist without a matching stored run record, `received` is still `true` and symbol totals remain `null`.

### Agent Context (`/agent-context`)

| Method | Path | Query / body | Description |
|--------|------|----------------|-------------|
| `GET` | `/agent-context/symbol-day` | `market_date`, repeatable `symbols`, optional `fs_div`, `disclosures_limit` | Aggregate symbol/day agent context from stored metadata, technicals, action summaries, DART fundamentals, and recent disclosures |

## Run the API locally

Application root is `apps/api` and the Python package import root is `app`.

```bash
cd apps/api
pip install -r requirements.txt
# copy .env.example to .env and fill secrets
./start-api.sh
# or: uvicorn app.main:app --reload --host 127.0.0.1 --port 8000
```

## Database bootstrap and schema overview

The project uses Alembic-backed schema bootstrap under `apps/api/db_migrations/`. The migration set is the source of truth, and startup can apply pending revisions automatically when `DB_AUTO_UPGRADE=true`.

Core tables currently cover:

- `symbols` for the normalized instrument master
- `tracked_symbols` for persisted scanner membership
- `quote_snapshots` and `current_quote_cache` for intraday quote history and latest quote state
- `market_status_snapshots` for persisted market-clock observations
- `eod_collection_runs` and `daily_price_bars` for EOD collection history and OHLCV bars
- `daily_technical_snapshots` and `daily_action_summaries` for post-EOD computed read models
- `dart_disclosure_sync_runs`, `dart_disclosure_events`, `dart_fundamentals_capture_runs`, `dart_fundamentals_snapshots`, `dart_fundamentals_rows`, and highlights for DART ingestion and history

For a fresh machine or reset local database, use this flow from `apps/api`:

```bash
pip install -r requirements.txt
cp .env.example .env
# update .env for your local Postgres instance
createdb kai_trading
alembic upgrade head
python -c "from app.main import app"
```

Practical notes:

- Run `alembic current` before and after bootstrapping if you want explicit revision visibility.
- `alembic upgrade head` is the predictable explicit bootstrap path.
- If `DB_AUTO_UPGRADE=true`, startup can also apply pending migrations.

## Implemented jobs, APIs, and operator flows

### HTTP APIs

Implemented and currently useful endpoints include:

- broker/KIS health, token, and quote APIs
- market status, tracked quotes, tracked symbols, and symbol search APIs
- BOK/ECOS macro and Korea Exim FX APIs
- DART company snapshot, disclosure sync and inspect, fundamentals fetch, fundamentals capture and inspect, stored fundamentals history, current-universe backfill, and filing artifact download APIs
- EOD status, master refresh, capture, pykrx backfill and daily refresh, technical snapshot compute and inspect, and action summary compute and inspect APIs
- agent-context symbol/day aggregate read API
- agent feedback dispatch, intake, and read APIs for the three summarizer roles, with role-specific JSON payload construction

### CLI and module entry points

Operator-friendly Python module entry points from `apps/api` include:

- `python3.11 -m app.eod.technical_snapshots --market-date YYYY-MM-DD`
- `python3.11 -m app.eod.action_summaries --market-date YYYY-MM-DD`
- `python3.11 -m app.dart.backfill_current_universe ...`
- `python3.11 -m app.dart.backfill_current_universe_runner ...`

### Background behavior

- API startup initializes the DB and hydrates tracked symbols.
- `background_runner` keeps the long-running EOD scheduling loop alive while the service is up.
- `DB_AUTO_UPGRADE=true` can apply Alembic migrations during normal startup.

## How the implemented pieces connect

For a future engineer or agent landing in the repo cold, the important mental model is:

1. Symbol master and EOD bars are the base market-data facts.
2. Technical snapshots derive from stored daily bars.
3. Action summaries derive from stored technical snapshots.
4. DART sync and fundamentals capture build a separate persisted fundamentals history.
5. The agent-context route reads across these persisted layers and returns one compact symbol/day context object per symbol.

That means downstream agents should generally prefer:

- stored read models over ad hoc recomputation
- aggregate read APIs over stitching multiple raw endpoints into prompts
- their own prioritization logic over pushing ranking logic into the core API too early

## Example requests

Agent context read API examples:

```bash
curl "http://127.0.0.1:8000/agent-context/symbol-day?market_date=2026-04-08&symbols=005930"
```

```bash
curl "http://127.0.0.1:8000/agent-context/symbol-day?market_date=2026-04-08&symbols=005930&symbols=000660&fs_div=CFS&disclosures_limit=5"
```

Agent feedback orchestration examples (current scope: `eod-summarizer`, `technical-summarizer`, `fundamental-summarizer` only):

```bash
curl -X POST "http://127.0.0.1:8000/agent-context/feedback/dispatch" \
  -H "Content-Type: application/json" \
  -d '{"eod_run_id": 158}'
```

```bash
curl -X POST "http://127.0.0.1:8000/agent-context/feedback/dispatch" \
  -H "Content-Type: application/json" \
  -d '{"eod_run_id": 158, "roles": ["eod-summarizer", "technical-summarizer"]}'
```

```bash
curl "http://127.0.0.1:8000/agent-context/feedback?market_date=2026-04-15&agent_name=eod-summarizer&limit=20"
```

Agent feedback read/status API usage:

```bash
curl "http://127.0.0.1:8000/agent-context/feedback/payloads?market_date=2026-04-15&symbols=005930"
```

```bash
curl "http://127.0.0.1:8000/agent-context/feedback/requests?market_date=2026-04-15&symbols=005930"
```

```bash
curl "http://127.0.0.1:8000/agent-context/feedback/status?market_date=2026-04-15&symbols=005930&symbols=000660&symbols=012450"
```

Notes:

- `feedback/payloads` returns what the agents answered, using stored `feedback_payload` rows.
- `feedback/requests` returns what Core sent to the agents, using stored `context_payload` rows.
- `feedback/status` returns workflow state for the expected three-agent set: `eod-summarizer`, `technical-summarizer`, `fundamental-summarizer`.
- Symbol status becomes `pending`, `in_progress`, `partial`, or `completed` based on how many of those three agent rows have been stored.
- These endpoints are historical reads over `agent_context_feedback`, newest rows first.

Related manual post-EOD flows:

```bash
curl -X POST "http://127.0.0.1:8000/eod/technical-snapshots/compute?market_date=2026-04-08&symbols=005930,000660"
curl -X POST "http://127.0.0.1:8000/eod/action-summaries/compute?market_date=2026-04-08&symbols=005930,000660"
```

## Runtime direction

- The core API is intended to run 24/7.
- A background runner polls market state and drives pre-market master refresh and post-close EOD behavior while the process is up.
- CORS is currently permissive for development and should be tightened for production.

## OpenAI Codex

Agent instructions live in `.codex/config.toml`, `.codex/skills/`, and `.codex/agents/`. Trust the project in Codex so they load.

Quick smoke check from `apps/api`:

```bash
pip install -r requirements.txt
python -c "from app.main import app"
```

See `AGENTS.md` for repo-specific working rules.

## Related repos

- `trading-web` (Next.js) may proxy this API. Keep paths and JSON shapes aligned when changing either side.

## What remains future or planned

The repo is materially beyond scaffolding, but several major pieces are still intentionally future work:

- final agent ranking and portfolio-prioritization logic
- execution engine and order persistence
- account balance and positions APIs
- richer DART normalized analytics beyond the current disclosure and fundamentals backbone
- broader research and data-lake layers such as Redis, Parquet, and DuckDB workflows
- more explicit retention and archival policy as historical datasets grow

See `docs/API_BUILD_ORDER.md` for the next planned slices.
