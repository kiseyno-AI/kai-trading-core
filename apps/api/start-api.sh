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

if [ "${INSTALL_REQUIREMENTS_ON_START:-false}" = "true" ]; then
  "$PYTHON_BIN" -m pip install -r requirements.txt
fi
set -a
source .env
set +a
exec "$PYTHON_BIN" -m uvicorn app.main:app --host "$HOST" --port "$PORT"
