#!/usr/bin/env bash
# Independent data-only tick; never starts Core or runs daily stages.
set -euo pipefail
resolve_only=false
if [[ "${1:-}" == "--resolve-python" ]]; then
  if [[ $# -ne 1 ]]; then exit 2; fi
  resolve_only=true
fi
# Avoid Python/calendar startup outside the window; Python rechecks before requests.
if [[ "$resolve_only" == false ]]; then
  kst_hhmm=$(TZ=Asia/Seoul date +%H%M)
  if [[ "$kst_hhmm" < "0645" || "$kst_hhmm" > "1530" ]]; then exit 0; fi
fi
cd "$(dirname "$0")/../apps/api"
inherited_python_set=${PYTHON_BIN+x}
inherited_python=${PYTHON_BIN-}
set -a
[ -f .env ] && . ./.env
set +a
if [[ "$inherited_python_set" == x ]]; then PYTHON_BIN=$inherited_python; fi
if [[ -n "${PYTHON_BIN:-}" ]]; then
  :
elif [[ -n "${VIRTUAL_ENV:-}" && -x "$VIRTUAL_ENV/bin/python" ]]; then
  PYTHON_BIN="$VIRTUAL_ENV/bin/python"
elif [[ -x .venv/bin/python ]]; then
  PYTHON_BIN="$PWD/.venv/bin/python"
else
  PYTHON_BIN=python3.12
fi
if [[ "$resolve_only" == true ]]; then
  resolved_python=$(command -v "$PYTHON_BIN")
  if [[ "$resolved_python" != /* ]]; then resolved_python="$PWD/$resolved_python"; fi
  printf '%s\n' "$resolved_python"
  exit
fi
exec "$PYTHON_BIN" -m app.internal.us_rebound "$@"
