#!/bin/bash
# Install (or --uninstall) the per-user launchd job for the weekly agent improvement loop.
set -eu
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
LABEL="com.kai.agent-improvement"
PLIST="$HOME/Library/LaunchAgents/$LABEL.plist"
DOMAIN="gui/$(id -u)"
if [ "${1:-}" = "--uninstall" ]; then
  launchctl bootout "$DOMAIN/$LABEL" 2>/dev/null || true
  rm -f "$PLIST"
  echo "uninstalled $LABEL"
  exit 0
fi
mkdir -p "$HOME/Library/LaunchAgents" "$ROOT/var/alpha-lab"
sed -e "s#__ROOT__#$ROOT#g" -e "s#__LABEL__#$LABEL#g" "$ROOT/scripts/launchd/com.kai.agent-improvement.plist.template" > "$PLIST"
plutil -lint "$PLIST" >/dev/null
launchctl bootout "$DOMAIN/$LABEL" 2>/dev/null || true
launchctl bootstrap "$DOMAIN" "$PLIST"
echo "installed $LABEL (hourly check, runs Saturday from 09:00 KST once per week)"
