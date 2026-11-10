#!/bin/bash
# Install the side agents or their separate --daily-review job. Add --uninstall to remove the selected job.
set -eu
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
LABEL="com.kai.side-agents"
if [ "${1:-}" = "--daily-review" ]; then
  LABEL="com.kai.daily-review"
  shift
fi
PLIST="$HOME/Library/LaunchAgents/$LABEL.plist"
DOMAIN="gui/$(id -u)"
if [ "${1:-}" = "--uninstall" ]; then
  launchctl bootout "$DOMAIN/$LABEL" 2>/dev/null || true
  rm -f "$PLIST"
  echo "uninstalled $LABEL"
  exit 0
fi
mkdir -p "$HOME/Library/LaunchAgents" "$ROOT/var/side-agents/logs"
sed -e "s#__ROOT__#$ROOT#g" -e "s#__LABEL__#$LABEL#g" "$ROOT/scripts/launchd/com.kai.side-agents.plist.template" > "$PLIST"
plutil -lint "$PLIST" >/dev/null
launchctl bootout "$DOMAIN/$LABEL" 2>/dev/null || true
launchctl bootstrap "$DOMAIN" "$PLIST"
echo "installed $LABEL (every 30 minutes); logs: $ROOT/var/side-agents/logs/"
