#!/usr/bin/env bash
# Poll in any host timezone; Python enforces the daily phases and cutoffs.
set -euo pipefail
if [[ $# -gt 1 || ( $# -eq 1 && "$1" != "--print" ) ]]; then
  echo "usage: $0 [--print]" >&2
  exit 2
fi
label=com.kiseyno.kai-us-rebound
repo=$(cd "$(dirname "$0")/.." && pwd)
runtime_python=$(bash "$repo/scripts/run_us_rebound.sh" --resolve-python)
if [[ "${1:-}" == "--print" ]]; then
  plist=/dev/stdout
else
  plist="$HOME/Library/LaunchAgents/$label.plist"
  mkdir -p "$repo/apps/api/var/log" "$(dirname "$plist")"
fi
"$runtime_python" - "$repo" "$plist" "$label" "$runtime_python" <<'PY'
from pathlib import Path
import plistlib
import sys

repo, output, label, runtime_python = sys.argv[1:]
configuration = {
    'Label': label,
    'ProgramArguments': ['/bin/bash', str(Path(repo) / 'scripts/run_us_rebound.sh')],
    'StartInterval': 60,
    'RunAtLoad': True,
    'KeepAlive': False,
    'EnvironmentVariables': {'PYTHON_BIN': runtime_python},
    'StandardOutPath': str(Path(repo) / 'apps/api/var/log/us-rebound.log'),
    'StandardErrorPath': str(Path(repo) / 'apps/api/var/log/us-rebound-error.log'),
}
with open(output, 'wb') as handle:
    plistlib.dump(configuration, handle)
PY
if [[ "${1:-}" == "--print" ]]; then
  exit 0
fi
domain="gui/$(id -u)"
launchctl enable "$domain/$label"
launchctl bootout "$domain/$label" 2>/dev/null || true
launchctl bootstrap "$domain" "$plist"
# Keep two-close polling disabled across logins after successful replacement.
launchctl disable "$domain/com.kiseyno.kai-us-index-preopen"
launchctl bootout "$domain/com.kiseyno.kai-us-index-preopen" 2>/dev/null || true
echo "installed $label -> $plist"
