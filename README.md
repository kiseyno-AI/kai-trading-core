# trading-core (kai-trading-core)

Core trading platform repository — **backend API and services** for the KAI system. It is **not** the web UI (see **Related repos**).

---

## What this project is

| Area | Status | Notes |
|------|--------|--------|
| **HTTP API** | Active | FastAPI app in `apps/api`, default `http://127.0.0.1:8000` |
| **KIS (Korea Investment & Securities)** | Integrated | Token, health, domestic quotes; mock vs production via env |
| **Market clock & quotes** | Active | Market state, tracked quotes, symbol search from master list |
| **Scanner** | Active | In-memory tracked symbol list (get/update) |
| **EOD pipeline** | Active | Master refresh, scheduled capture hooks, status endpoints |
| **Macro (BOK ECOS)** | Integrated | Statistic series proxy |
| **FX (Korea Exim)** | Integrated | Exchange rate lookup |
| **Market-data persistence** | Active | Alembic-backed Postgres schema for symbols, tracking, quotes, market status, and EOD bars |
| **Execution engine / order persistence** | Roadmap | Trading and execution boundaries remain future work in `ARCHITECTURE.md` |

**Tech stack:** Python 3, FastAPI, Uvicorn, Pydantic Settings, HTTPX for outbound calls. Configuration via `apps/api/.env` (see `apps/api/.env.example`).

**Documentation in-repo:** `ARCHITECTURE.md`, `RUNBOOK.md`, `docs/API_BUILD_ORDER.md`, `docs/ARCHITECTURE.md`, `docs/KIS_API_ONBOARDING.md`, scanner notes under `docs/scanner/`.

---

## Authentication

If **`UPSTREAM_BEARER_TOKEN`** is set in the environment, every request except **`OPTIONS`** must include:

```http
Authorization: Bearer <same token>
```

If the variable is unset or empty, the API accepts requests without a bearer token (typical for local development).

---

## HTTP API reference

Base URL (local default): `http://127.0.0.1:8000`

Interactive docs (when the server is running): **`GET /docs`** (Swagger UI), **`GET /redoc`**.

### Core & health

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/health` | Liveness: `status`, `app`, `env` |

### Broker — KIS (`/broker/kis`)

| Method | Path | Query / body | Description |
|--------|------|----------------|-------------|
| `GET` | `/broker/kis/health` | — | KIS connectivity / health check |
| `POST` | `/broker/kis/token` | Query: `force_refresh` (bool, default false) | Issue or refresh KIS access token |
| `GET` | `/broker/kis/quote` | `symbol` (required), `market_code` (default `J`) | Domestic quote via KIS |

### Market & quotes

| Method | Path | Query / body | Description |
|--------|------|----------------|-------------|
| `GET` | `/market/status` | — | Current market state (clock / session) |
| `GET` | `/quotes/tracked` | — | Quotes for all tracked scanner symbols |
| `GET` | `/quotes/{symbol}` | Path: `symbol` | Single-symbol quote |

### Scanner (`/scanner`)

| Method | Path | Query / body | Description |
|--------|------|----------------|-------------|
| `GET` | `/scanner/tracked-symbols` | — | Current tracked symbol list snapshot |
| `POST` | `/scanner/tracked-symbols` | JSON body: `{ "symbols": ["005930", ...] }` | Replace tracked symbols |

### Symbols (`/symbols`)

| Method | Path | Query / body | Description |
|--------|------|----------------|-------------|
| `GET` | `/symbols/search` | `q` (string, optional), `limit` (1–100, default 20) | Search master symbols by code or Korean name |

### Macro — Bank of Korea ECOS (`/macro/bok`)

| Method | Path | Query / body | Description |
|--------|------|----------------|-------------|
| `GET` | `/macro/bok/series` | `series_id`, `start_period`, `end_period`, `cycle` (default `D`) | ECOS statistic series (proxy) |

### FX — Korea Exim (`/fx/koreaexim`)

| Method | Path | Query / body | Description |
|--------|------|----------------|-------------|
| `GET` | `/fx/koreaexim/rates` | `search_date` (optional, `YYYYMMDD`), `data` (default `AP01`) | Exchange rates |

### EOD (`/eod`)

| Method | Path | Query / body | Description |
|--------|------|----------------|-------------|
| `GET` | `/eod/status` | — | EOD job + master refresh state snapshot |
| `POST` | `/eod/master/refresh-if-due` | Query: `force` (bool, default false) | Refresh symbol master if due (or forced) |
| `POST` | `/eod/capture-if-due` | — | Run EOD capture when due |
| `POST` | `/eod/capture` | Query: `market_date` | Capture EOD for a specific `market_date` |

Errors from upstream services are generally returned as **502** with a `detail` message; invalid bearer token returns **401** when `UPSTREAM_BEARER_TOKEN` is configured.

---

## Run the API locally

- **`apps/api`** — application root; Python package is **`app`**.
- **`apps/api/start-api.sh`** — installs requirements, loads `.env`, starts Uvicorn (see **RUNBOOK.md**).
- Environment template: **`apps/api/.env.example`**.

```bash
cd apps/api
pip install -r requirements.txt
# copy .env.example to .env and fill secrets
./start-api.sh
# or: uvicorn app.main:app --reload --host 127.0.0.1 --port 8000
```

---

## Database bootstrap and schema overview

The project now uses **Alembic-backed schema bootstrap** for the market-data database. The source of truth is the migration set under `apps/api/db_migrations/`, and startup can apply pending revisions automatically when `DB_AUTO_UPGRADE=true`.

At a high level, the current core tables cover:

- `symbols` for the normalized instrument master: symbol/code identity, Korean display name, market, instrument type, source, active flag, and refresh timestamps
- `tracked_symbols` for the scanner/watchlist set: a foreign-key link to `symbols` plus stable sort order
- `quote_snapshots` and `current_quote_cache` for quote history plus the latest cached quote per symbol: market state, price/change fields, volume/turnover, source flags, capture timestamps, and raw payloads
- `market_status_snapshots` for persisted market-clock observations: market/state values, trading-day and open flags, reason text, and server/session timestamps
- `eod_collection_runs` and `daily_price_bars` for end-of-day collection tracking and OHLCV bars: run status/counters/metadata plus per-symbol trade-date open, high, low, close, volume, and turnover values

For a fresh machine or a reset local database, use this bootstrap flow from `apps/api`:

```bash
pip install -r requirements.txt
cp .env.example .env
# update .env with the local Postgres database/user/password you want to use
createdb kai_trading            # or create the database with your preferred Postgres workflow
alembic upgrade head
python -c "from app.main import app"
```

Practical notes for newcomers:

- To see what your environment is currently on before changing anything, run `alembic current` from `apps/api`.
- `alembic upgrade head` is the explicit, predictable way to apply the schema before first startup.
- If `DB_AUTO_UPGRADE=true`, `./start-api.sh` will also run Alembic upgrades during API startup.
- The database name and credentials can differ per machine; what matters is that `.env` points at a reachable Postgres instance and Alembic reaches `head`.
- After bootstrapping, rerun `alembic current` to confirm your database revision matches the repo's current head.

### Core tables in practical terms

Use this as the quick mental model:

- `symbols` is the master list of instruments.
- `tracked_symbols` is the subset the scanner/watchlist currently cares about.
- `quote_snapshots` and `current_quote_cache` store intraday quotes.
- `market_status_snapshots` stores what the app believed the market clock was.
- `eod_collection_runs` and `daily_price_bars` store end-of-day collection history and the resulting daily bars.

| Table | What it is for | Key attributes and why they matter |
|------|-----------------|------------------------------------|
| `symbols` | Canonical instrument master used by the rest of the schema. Other tables reference `symbol_id` here instead of repeating symbol text everywhere. | `symbol` is the actual trading code such as `005930`. `name_ko` is the Korean display name. `market` tells you which market bucket it belongs to. `standard_code` keeps the upstream reference code when available. `instrument_type` is the asset class label. `source` tells you whether the row came from the KIS master refresh, scanner, or quote flow. `is_active` marks whether the master entry is still current. `last_refreshed_at` shows when the metadata was last refreshed. |
| `tracked_symbols` | Persisted watchlist/scanner membership. This is the small set returned by `/scanner/tracked-symbols` and used by tracked-quote flows. | `symbol_id` points to the instrument in `symbols`. `sort_order` preserves the caller-facing order. `created_at` tells you when that tracked row was added. |
| `quote_snapshots` | Append-only intraday quote history. Each persisted quote event creates a new row. | `symbol_id` ties the quote to an instrument. `market_state` captures the market session context. `price`, `change`, `change_pct`, `volume`, and `turnover` are the normalized quote fields most downstream code will read first. `as_of` stores the upstream quote time string. `source` records which provider or app path supplied the data. `is_live` distinguishes a live quote from a non-live path. `fallback` marks restart-safe fallback data. `payload` keeps the raw normalized quote payload for debugging and later reprocessing. `captured_at` is when the snapshot was written. |
| `current_quote_cache` | The latest known quote per symbol. Use this when you want current state, not history. | It intentionally mirrors the main quote fields from `quote_snapshots`, but `symbol_id` is unique so there is only one current row per symbol. `captured_at` tells you when the cached quote was observed. `updated_at` tells you when that cache row was last rewritten. |
| `market_status_snapshots` | Persisted market-clock observations over time. This is the history behind "what market state did the app think we were in?" | `market` names the market scope. `state` stores values like pre-open, open, or closed. `is_trading_day` and `is_open` are the first booleans most callers care about. `reason` explains special cases such as holidays. `server_time` is the reference timestamp used to evaluate the state. `session_open_at` and `session_close_at` are the expected trading-session bounds. `source` identifies the writer. `captured_at` is when the row was persisted. |
| `eod_collection_runs` | Audit trail for EOD collection jobs. One row represents one attempt to collect a market day's daily bars. | `trade_date` is the market date being collected. `market`, `source`, and `trigger` explain what was collected and why the run started. `status` shows whether the run is still running, completed, or completed with errors. `symbols_total`, `symbols_succeeded`, and `symbols_failed` are the main progress counters. `error_message` stores a run-level failure summary. `started_at` and `completed_at` tell you how long the run took. `run_metadata` is JSON for extra context that does not justify a dedicated column yet. |
| `daily_price_bars` | One daily OHLCV row per symbol per trade date. This is the durable end-of-day price history table. | `symbol_id` identifies the instrument. `trade_date` is the date of the bar. `open_price`, `high_price`, `low_price`, and `close_price` are the daily OHLC values. `volume` and `turnover` capture activity for that day. `eod_run_id` links the bar back to the collection run that produced or associated it. `source` records the upstream provider. `captured_at` shows when the row was stored. |

---

## Runtime direction

- The core API is intended to run **24/7**.
- A **background runner** (see `app/eod/runner.py`, **RUNBOOK.md**) polls market state and drives pre-market master refresh and post-close EOD behavior while the process is up.
- **CORS** is currently permissive (`allow_origins=['*']`) for development; tighten for production deployments.

---

## OpenAI Codex

Agent instructions: **`.codex/config.toml`**, **`.codex/skills/`**, **`.codex/agents/`**. Trust the project in Codex so they load.

Quick smoke check from `apps/api`:

```bash
pip install -r requirements.txt
python -c "from app.main import app"
```

More detail: **[AGENTS.md](./AGENTS.md)**.

---

## Related repos

- **trading-web** (Next.js) — may proxy this API (e.g. `/market/status`, `/quotes/tracked`, `/scanner/tracked-symbols`). Keep paths and JSON shapes aligned when you change either side.

---

## Planned API surface (not necessarily implemented)

See **`docs/API_BUILD_ORDER.md`** for the intended build order and ideas such as KIS balance, positions, order skeletons, and news abstractions.
