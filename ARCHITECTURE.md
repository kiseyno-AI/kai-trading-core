# ARCHITECTURE.md

## Current Direction
- Modular monolith first
- KR broker: KIS + python-kis
- US broker: IBKR official API (planned / future)
- Backend/API: **FastAPI** (`apps/api`)
- Data/storage: PostgreSQL + Redis + Parquet/DuckDB (roadmap)
- Research: vectorbt (roadmap)

## API service layout (`apps/api`)
- **`app/main.py`** — FastAPI app, CORS, bearer middleware, router registration, lifespan (`background_runner`).
- **`app/core/settings.py`** — Pydantic Settings / `.env` (KIS, BOK, Korea Exim, `UPSTREAM_BEARER_TOKEN`, etc.).
- **`app/core/audit_logging.py`** — rotating JSONL audit logs for inbound API requests and outbound upstream calls with request-id propagation and redaction.
- **`app/db.py` + `alembic/`** — SQLAlchemy engine/session helpers and Alembic schema migrations.
- **`app/api/routes_*.py`** — HTTP routers (KIS, macro, FX, scanner, market, quotes, symbols, EOD).
- **`app/brokers/kis/`** — KIS client/provider/schemas.
- **`app/integrations/`** — BOK, FX (Korea Exim), etc.
- **`app/fx/`** — Korea Exim FX service-level normalization, filtering, and date fallback behavior.
- **`app/quotes/`**, **`app/market/`**, **`app/scanner/`**, **`app/eod/`** — domain services and background behavior, including pykrx-based historical EOD backfill.

## Documentation
- **`docs/API_BUILD_ORDER.md`** — implemented vs planned HTTP routes (keep in sync with code).
- **`docs/ARCHITECTURE.md`**, **`docs/KIS_API_ONBOARDING.md`**, **`docs/scanner/`** — deeper notes.

## Tooling and quality
- **Codex:** [`.codex/config.toml`](./.codex/config.toml), [`.codex/skills/`](./.codex/skills/), [`.codex/agents/`](./.codex/agents/).
- **CI:** [`.github/workflows/ci.yml`](./.github/workflows/ci.yml) — smoke import for `app.main`.
- **Ops:** [RUNBOOK.md](./RUNBOOK.md) — start script, health and EOD endpoints.

## Core Areas
- execution engine (future)
- data pipeline (future)
- shared models (future)
- FastAPI integration surface (current)
- docs
