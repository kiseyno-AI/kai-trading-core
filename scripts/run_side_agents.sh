#!/bin/bash
# Run whatever side agents are due. Idempotent; called every 30 minutes by launchd.
# Reads apps/api/.env inside this shell only (never printed).
set -u
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT/apps/api" || exit 1
LOG_DIR="$ROOT/var/side-agents/logs"
mkdir -p "$LOG_DIR"
if [ -f .env ]; then set -a; . ./.env; set +a; fi
PY="$ROOT/apps/api/.venv/bin/python"
[ -x "$PY" ] || PY="$(command -v python3)"
"$PY" -m app.side_agents run-due --only market-context-analyst --only news-event-analyst >> "$LOG_DIR/run-due.log" 2>&1
"$PY" -m app.side_agents run-daily-review >> "$LOG_DIR/daily-review.log" 2>&1
