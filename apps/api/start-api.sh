#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

if [ ! -f .env ]; then
  echo "Missing $SCRIPT_DIR/.env" >&2
  exit 1
fi

# Load launcher controls before selecting or probing Python. Preserve explicit
# process-environment overrides, which take precedence over .env values.
INHERITED_PYTHON_BIN_SET="${PYTHON_BIN+x}"
INHERITED_PYTHON_BIN="${PYTHON_BIN-}"
INHERITED_HOST_SET="${HOST+x}"
INHERITED_HOST="${HOST-}"
INHERITED_PORT_SET="${PORT+x}"
INHERITED_PORT="${PORT-}"
INHERITED_INSTALL_REQUIREMENTS_SET="${INSTALL_REQUIREMENTS_ON_START+x}"
INHERITED_INSTALL_REQUIREMENTS="${INSTALL_REQUIREMENTS_ON_START-}"
INHERITED_DB_AUTO_UPGRADE_SET="${DB_AUTO_UPGRADE+x}"
INHERITED_DB_AUTO_UPGRADE="${DB_AUTO_UPGRADE-}"
INHERITED_UPSTREAM_BEARER_TOKEN_SET="${UPSTREAM_BEARER_TOKEN+x}"
INHERITED_UPSTREAM_BEARER_TOKEN="${UPSTREAM_BEARER_TOKEN-}"
INHERITED_APP_DEBUG_SET="${APP_DEBUG+x}"
INHERITED_APP_DEBUG="${APP_DEBUG-}"

set -a
source .env
set +a

if [ "$INHERITED_PYTHON_BIN_SET" = "x" ]; then PYTHON_BIN="$INHERITED_PYTHON_BIN"; fi
if [ "$INHERITED_HOST_SET" = "x" ]; then HOST="$INHERITED_HOST"; fi
if [ "$INHERITED_PORT_SET" = "x" ]; then PORT="$INHERITED_PORT"; fi
if [ "$INHERITED_INSTALL_REQUIREMENTS_SET" = "x" ]; then
  INSTALL_REQUIREMENTS_ON_START="$INHERITED_INSTALL_REQUIREMENTS"
fi
if [ "$INHERITED_DB_AUTO_UPGRADE_SET" = "x" ]; then DB_AUTO_UPGRADE="$INHERITED_DB_AUTO_UPGRADE"; fi
if [ "$INHERITED_UPSTREAM_BEARER_TOKEN_SET" = "x" ]; then
  UPSTREAM_BEARER_TOKEN="$INHERITED_UPSTREAM_BEARER_TOKEN"
fi
if [ "$INHERITED_APP_DEBUG_SET" = "x" ]; then APP_DEBUG="$INHERITED_APP_DEBUG"; fi

export PATH="/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin:${PATH:-}"

if [ -n "${PYTHON_BIN:-}" ]; then
  : # Explicit operator override wins.
elif [ -n "${VIRTUAL_ENV:-}" ] && [ -x "${VIRTUAL_ENV}/bin/python" ]; then
  PYTHON_BIN="${VIRTUAL_ENV}/bin/python"
elif [ -x "$SCRIPT_DIR/.venv/bin/python" ]; then
  PYTHON_BIN="$SCRIPT_DIR/.venv/bin/python"
else
  PYTHON_BIN="python3.12"
fi
HOST="${HOST:-127.0.0.1}"
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

if [ "${INSTALL_REQUIREMENTS_ON_START:-auto}" = "true" ] || \
   { [ "${INSTALL_REQUIREMENTS_ON_START:-auto}" = "auto" ] && [ "$NEEDS_BOOTSTRAP" = "1" ]; }; then
  "$PYTHON_BIN" -m pip install -r requirements.txt
fi

is_loopback_host() {
  case "${1:-}" in
    127.0.0.1|localhost|::1|'[::1]') return 0 ;;
    *) return 1 ;;
  esac
}

if ! is_loopback_host "$HOST"; then
  if [ -z "${UPSTREAM_BEARER_TOKEN//[[:space:]]/}" ]; then
    echo "Refusing non-loopback API bind without UPSTREAM_BEARER_TOKEN" >&2
    exit 1
  fi
  export APP_DEBUG=false
fi

if [ "${DB_STATEMENT_TIMEOUT_MS:-0}" -lt 600000 ] 2>/dev/null; then
  export DB_STATEMENT_TIMEOUT_MS=600000
fi
if [ "${DB_LOCK_TIMEOUT_MS:-0}" -lt 60000 ] 2>/dev/null; then
  export DB_LOCK_TIMEOUT_MS=60000
fi

if is_truthy "${DB_AUTO_UPGRADE:-true}"; then
  PREFLIGHT_JSON=""
  if ! PREFLIGHT_JSON="$("$PYTHON_BIN" -m app.internal.deploy_hygiene preflight --format json)"; then
    "$PYTHON_BIN" -m app.internal.deploy_hygiene preflight --format text
    exit 1
  fi

  PREFLIGHT_AT_HEAD=0
  if printf '%s' "$PREFLIGHT_JSON" | tr -d '[:space:]' | grep -q '"at_head":true'; then
    PREFLIGHT_AT_HEAD=1
  fi

  if [ "$PREFLIGHT_AT_HEAD" != "1" ]; then
    "$PYTHON_BIN" -m alembic upgrade head
  fi

  "$PYTHON_BIN" -m app.internal.deploy_hygiene preflight --require-head --format text
  export DB_AUTO_UPGRADE=false
fi

exec "$PYTHON_BIN" -m uvicorn app.main:app --host "$HOST" --port "$PORT"
