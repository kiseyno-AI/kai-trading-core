#!/usr/bin/env bash
# Independent data-only tick; never starts Core or runs daily stages.
set -euo pipefail
# Avoid Python/calendar startup outside the window; Python rechecks before requests.
kst_hhmm=$(TZ=Asia/Seoul date +%H%M)
if [[ "$kst_hhmm" < "0700" || "$kst_hhmm" > "0711" ]]; then
  exit 0
fi
cd "$(dirname "$0")/../apps/api"
set -a
[ -f .env ] && . ./.env
set +a
exec .venv/bin/python -m app.internal.us_index_preopen "$@"
