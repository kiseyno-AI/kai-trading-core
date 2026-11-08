#!/bin/bash
# Weekly signal lab + agent calibration, run on this Mac (not on GitHub Actions).
# Core's background runner starts it (app/analytics/improvement_loop_schedule.py);
# it does real work once per ISO week, on Saturday from 09:00 KST, using the live
# main checkout. Numbers stay under var/alpha-lab.
# Applying (branch + PR + issues) is opt-in: set AGENT_RULES_AUTO_PR=true in
# var/alpha-lab/loop.env, or pass --apply. Pass --force to ignore the schedule.
set -u
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
OUT="$ROOT/var/alpha-lab"
mkdir -p "$OUT"
APPLY=false
FORCE=false
for arg in "$@"; do
  case "$arg" in
    --apply) APPLY=true ;;
    --force) FORCE=true ;;
  esac
done
[ -f "$OUT/loop.env" ] && . "$OUT/loop.env"
[ "${AGENT_RULES_AUTO_PR:-false}" = "true" ] && APPLY=true

WEEK="$(TZ=Asia/Seoul date +%G-W%V)"
DOW="$(TZ=Asia/Seoul date +%u)"   # 6 = Saturday
HOUR="$(TZ=Asia/Seoul date +%H)"
MARKER="$OUT/.done-$WEEK"
if [ "$FORCE" != "true" ]; then
  [ "$DOW" = "6" ] || exit 0
  [ "$((10#$HOUR))" -ge 9 ] || exit 0
  [ -f "$MARKER" ] && exit 0
fi

PY="$ROOT/apps/api/.venv/bin/python"
RUN_ID="$(date +%Y%m%d-%H%M)"
LOG="$OUT/loop-$RUN_ID.log"
{
  echo "run $RUN_ID week $WEEK apply=$APPLY"
  (
    set -a; [ -f "$ROOT/apps/api/.env" ] && . "$ROOT/apps/api/.env"; set +a
    cd "$ROOT/apps/api" && DB_AUTO_UPGRADE=false PYTHONPATH="$PWD" "$PY" -m app.analytics.signal_lab --out-dir "$OUT"
  ) > "$OUT/last-run.log" 2>&1 || { echo "signal lab failed; see last-run.log"; exit 1; }
  REPORT="$(ls -t "$OUT"/report-*.json | head -1)"
  FINDINGS="$OUT/findings-$RUN_ID.json"
  ( cd "$ROOT/apps/api" && PYTHONPATH="$PWD" "$PY" -m app.analytics.improvement_findings \
      --report "$REPORT" --history "$OUT/history.jsonl" --run-id "$RUN_ID" \
      --changelog "$ROOT/agent_rules/CHANGELOG.md" --out "$FINDINGS" ) > "$OUT/last-findings.log" 2>&1 \
    || { echo "findings failed; see last-findings.log"; exit 1; }
  ARGS=()
  [ "$APPLY" = "true" ] && ARGS+=(--apply)
  ( cd "$ROOT" && "$PY" scripts/agent_improvement_run.py --findings "$FINDINGS" --run-id "$RUN_ID" ${ARGS[@]+"${ARGS[@]}"} ) \
    > "$OUT/last-improvement.log" 2>&1 || { echo "improvement run failed; see last-improvement.log"; exit 1; }
  touch "$MARKER"
  echo "done"
} >> "$LOG" 2>&1
exit 0
