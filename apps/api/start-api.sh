#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

if [ ! -f .env ]; then
  echo "Missing $SCRIPT_DIR/.env" >&2
  exit 1
fi

PYTHON_BIN="${PYTHON_BIN:-/opt/homebrew/bin/python3.11}"
HOST="${HOST:-0.0.0.0}"
PORT="${PORT:-8000}"

if ! command -v "$PYTHON_BIN" >/dev/null 2>&1; then
  echo "Python not found: $PYTHON_BIN" >&2
  exit 1
fi

is_truthy() {
  local value
  value="$(printf '%s' "${1:-}" | tr '[:upper:]' '[:lower:]')"
  case "$value" in
    1|true|yes|on) return 0 ;;
    *) return 1 ;;
  esac
}

NEEDS_BOOTSTRAP=0
if ! "$PYTHON_BIN" - <<'PYCODE' >/dev/null 2>&1
import fastapi, uvicorn, httpx, pydantic_settings, sqlalchemy, psycopg, alembic, pykrx
PYCODE
then
  NEEDS_BOOTSTRAP=1
fi

if [ "${INSTALL_REQUIREMENTS_ON_START:-auto}" = "true" ] ||    { [ "${INSTALL_REQUIREMENTS_ON_START:-auto}" = "auto" ] && [ "$NEEDS_BOOTSTRAP" = "1" ]; }; then
  "$PYTHON_BIN" -m pip install -r requirements.txt
fi

set -a
source .env
set +a
export PATH="/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin:${PATH:-}"

if is_truthy "${DB_AUTO_UPGRADE:-true}"; then
  "$PYTHON_BIN" -m app.internal.deploy_hygiene preflight --format text
fi

exec "$PYTHON_BIN" -m uvicorn app.main:app --host "$HOST" --port "$PORT"
