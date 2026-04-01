# AGENTS.md

## Purpose
This repository is part of the KAI trading system — **backend / core API** (FastAPI), not the web frontend.

## Working Rules
- Read this file before making changes.
- Prefer simple, practical architecture over premature complexity.
- Keep modules loosely coupled and clearly named.
- Update docs when structure or design changes.
- Do not add live-trading or order-execution behavior without explicit approval.
- Commit clear, scoped changes.
- **Python service root is `apps/api/`** — run installs, uvicorn, and `python -c "from app.main import app"` there (import path is `app`, not `apps.api`).

## OpenAI Codex
- **Project layer:** [`.codex/config.toml`](./.codex/config.toml) loads when the workspace is **trusted**. See [Codex configuration](https://developers.openai.com/codex/config-reference).
- **Skills:** [`.codex/skills/*/SKILL.md`](./.codex/skills/) — `codex-workflow`, `fastapi-trading-api`, `kis-broker-integration`, `trading-core-quality`.
- **Subagents:** [`.codex/agents/*.toml`](./.codex/agents/) — includes `trading_core_api_specialist`, `backend-developer`, `fintech-engineer`, and others.
- **Quality loop:** From `apps/api`: install requirements if needed, then `python -c "from app.main import app"`; use `GET /health` and [RUNBOOK.md](./RUNBOOK.md) for runtime checks.
- **Secrets:** Use `apps/api/.env` (see `.env.example`); never commit keys. `UPSTREAM_BEARER_TOKEN` enables bearer auth on incoming API requests.

## Files to Read First
- AGENTS.md
- RULES.md
- ARCHITECTURE.md
- TASKS.md
- RUNBOOK.md
- docs/API_BUILD_ORDER.md
