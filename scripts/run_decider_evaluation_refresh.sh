#!/usr/bin/env bash
# Nightly decider-evaluation refresh (explicit maintenance, outside the Core runner).
set -euo pipefail
cd "$(dirname "$0")/../apps/api"
set -a
[ -f .env ] && . ./.env
set +a
exec .venv/bin/python -m app.agent_context.evaluation_refresh
