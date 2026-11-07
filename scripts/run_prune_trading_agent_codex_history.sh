#!/bin/zsh
# launchd entry point: weekly offline prune of trading-agent Codex history.
# Guards live in the Python script (KST weekend, no active Core pipeline,
# free space below --min-free-gib). Output is appended to var/log.
set -u
REPO_ROOT="${0:A:h:h}"
LOG_DIR="$REPO_ROOT/apps/api/var/log"
mkdir -p "$LOG_DIR"
if [[ -f "$REPO_ROOT/apps/api/.env" ]]; then
  set -a; source "$REPO_ROOT/apps/api/.env"; set +a
fi
/usr/bin/python3 "$REPO_ROOT/scripts/prune_trading_agent_codex_history.py" --apply "$@" \
  >> "$LOG_DIR/prune-trading-agent-codex-history.log" 2>&1
