#!/usr/bin/env bash
# Poll in any host timezone; Python enforces KST slots and the hard cutoff.
set -euo pipefail
if [[ $# -gt 1 || ( $# -eq 1 && "$1" != "--print" ) ]]; then
  echo "usage: $0 [--print]" >&2
  exit 2
fi
label=com.kiseyno.kai-us-index-preopen
repo=$(cd "$(dirname "$0")/.." && pwd)
if [[ "${1:-}" == "--print" ]]; then
  plist=/dev/stdout
else
  plist="$HOME/Library/LaunchAgents/$label.plist"
  mkdir -p "$repo/apps/api/var/log" "$(dirname "$plist")"
fi
"${PYTHON_BIN:-python3}" - "$repo" "$plist" "$label" <<'PY'
from pathlib import Path
import plistlib
import sys

repo, output, label = sys.argv[1:]
configuration = {
    'Label': label,
    'ProgramArguments': ['/bin/bash', str(Path(repo) / 'scripts/run_us_index_preopen.sh')],
    'StartInterval': 60,
    'RunAtLoad': True,
    'KeepAlive': False,
    'StandardOutPath': str(Path(repo) / 'apps/api/var/log/us-index-preopen.log'),
    'StandardErrorPath': str(Path(repo) / 'apps/api/var/log/us-index-preopen-error.log'),
}
with open(output, 'wb') as handle:
    plistlib.dump(configuration, handle)
PY
if [[ "${1:-}" == "--print" ]]; then
  exit 0
fi
launchctl bootout "gui/$(id -u)/$label" 2>/dev/null || true
launchctl bootstrap "gui/$(id -u)" "$plist"
echo "installed $label -> $plist"
