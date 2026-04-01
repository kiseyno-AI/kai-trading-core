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
| **Execution engine / DB persistence** | Roadmap | Described in `ARCHITECTURE.md`; DB not fully wired yet |

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
