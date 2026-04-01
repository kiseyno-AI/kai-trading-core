# trading-core (kai-trading-core)

Core trading platform repository — **backend API and services**, not the web UI.

## Scope
- Execution engine (roadmap)
- Data pipeline (roadmap)
- Shared domain models (roadmap)
- **FastAPI service** in `apps/api` (current)
- Docs and research direction

## API bootstrap
- **`apps/api`** — FastAPI app; KIS and other credentials come from **environment variables** (`apps/api/.env`), not from code.
- **`apps/api/start-api.sh`** — installs requirements, loads `.env`, runs uvicorn (default `127.0.0.1:8000`).
- Set **`UPSTREAM_BEARER_TOKEN`** to a non-empty value to require `Authorization: Bearer <token>` on incoming API requests; leave unset for open local dev.

See **`apps/api/.env.example`** for all variables.

## Runtime direction
- The core API is intended to run **24/7**.
- Pre-market master refresh and EOD logic are driven by a **background runner** inside the API process (see **RUNBOOK.md**).
- Database persistence is not fully wired yet.

## OpenAI Codex
Agent instructions for this repo: **`.codex/config.toml`**, **`.codex/skills/`**, **`.codex/agents/`**. Trust the project in Codex so they apply.

Quick verification from `apps/api`:
```bash
pip install -r requirements.txt
python -c "from app.main import app"
```

Details: **[AGENTS.md](./AGENTS.md)**.

## Related repos
- **trading-web** (Next.js) — consumes this API via its BFF/proxy; keep route and JSON contracts aligned when both change.
