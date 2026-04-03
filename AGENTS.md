# AGENTS.md

## Purpose
This repository is part of the KAI trading system — **backend / core API** (FastAPI), not the web frontend.

## Working Rules
- Read this file before making changes.
- Before doing task work, create and switch to a dedicated branch for that task. Writable agents own branch creation, switching, cleanup, and PR workflow; read-only agents must stay within the provided branch context without performing checkout or branch mutation steps themselves.
- Prefer simple, practical architecture over premature complexity.
- Keep modules loosely coupled and clearly named.
- Update docs when structure or design changes.
- Do not add live-trading or order-execution behavior without explicit approval.
- Commit clear, scoped changes.
- After finishing task work, restore the main workspace checkout to `main`.
- After the PR is merged or the work is cancelled, delete the task branch locally and remotely unless there is a specific reason to keep it.
- **Python service root is `apps/api/`** — run installs, uvicorn, and `python -c "from app.main import app"` there (import path is `app`, not `apps.api`).

## OpenAI Codex
- **Project layer:** [`.codex/config.toml`](./.codex/config.toml) loads when the workspace is **trusted**. See [Codex configuration](https://developers.openai.com/codex/config-reference).
- **Skills:** [`.codex/skills/*/SKILL.md`](./.codex/skills/) — `codex-workflow`, `fastapi-trading-api`, `kis-broker-integration`, `trading-core-quality`.
- **Subagents:** [`.codex/agents/*.toml`](./.codex/agents/) — includes `trading_core_api_specialist`, `backend-developer`, `fintech-engineer`, and others.
- **ACP sessions:** Default ACP coding work to fresh thread-spawned ACP sessions. Treat `acpx exited with code 1`, empty spawned-session messages, and gateway errors such as `queue owner unavailable`, `Resource not found`, or `repairing dead named session` as dead-session symptoms. Silent run fallback is forbidden. Required recovery is stale ACP session cleanup, gateway restart if needed, then a fresh spawned ACP session retry.
- **Quality loop:** From `apps/api`: install requirements if needed, then `python -c "from app.main import app"`; use `GET /health` and [RUNBOOK.md](./RUNBOOK.md) for runtime checks.
- **Secrets:** Use `apps/api/.env` (see `.env.example`); never commit keys. `UPSTREAM_BEARER_TOKEN` enables bearer auth on incoming API requests.

## Files to Read First
- AGENTS.md
- RULES.md
- ARCHITECTURE.md
- TASKS.md
- RUNBOOK.md
- docs/API_BUILD_ORDER.md
