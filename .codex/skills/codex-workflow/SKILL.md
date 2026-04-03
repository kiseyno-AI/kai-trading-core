---
name: codex-workflow
description: Default Codex workflow for kai-trading-core—monorepo layout, apps/api working directory, safety, and verification. Use for general backend tasks when no narrower skill fits.
---

# Codex workflow (kai-trading-core)

## Layout

- **Service root:** `apps/api/` — run shell commands and Python there. Imports are `from app....` (not `from apps.api...`).
- **Repo root:** docs (`docs/`, `ARCHITECTURE.md`, `RUNBOOK.md`), orchestration scripts (`start-api.sh`).

## Before larger changes

1. Read `AGENTS.md`, `RULES.md`, `ARCHITECTURE.md`, `TASKS.md`, `RUNBOOK.md`.
2. Trust the project in Codex so `.codex/config.toml` loads ([Codex config](https://developers.openai.com/codex/config-reference)).
3. Create and switch to a dedicated branch before doing task work.

## Safety

- No live trading or order placement without explicit approval.
- Do not commit credentials; use `apps/api/.env` locally (see `.env.example`).
- When `UPSTREAM_BEARER_TOKEN` is set, document that clients must send `Authorization: Bearer`.
- After finishing task work, restore the main workspace checkout to `main`.
- After the PR is merged or the work is cancelled, delete the task branch locally and remotely unless there is a specific reason to keep it.

## Quick verification (from `apps/api`)

```bash
pip install -r requirements.txt
python -c "from app.main import app"
```

For runtime checks: `./start-api.sh` or uvicorn per `RUNBOOK.md`, then `GET /health`.

## Specialist agents

See `.codex/agents/` — e.g. `trading_core_api_specialist`, `backend-developer`, `fintech-engineer`.
