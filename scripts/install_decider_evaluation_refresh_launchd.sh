#!/usr/bin/env bash
# Check every 30 minutes so the KRX evening window works in any host timezone.
# The command refreshes each report once per KRX session. Remove with:
#   launchctl bootout gui/$(id -u)/com.kiseyno.kai-decider-evaluation-refresh
set -euo pipefail
label=com.kiseyno.kai-decider-evaluation-refresh
repo=$(cd "$(dirname "$0")/.." && pwd)
plist="$HOME/Library/LaunchAgents/$label.plist"
log_dir="$repo/apps/api/var/log"
mkdir -p "$log_dir" "$(dirname "$plist")"
cat > "$plist" <<PLIST
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0"><dict>
  <key>Label</key><string>$label</string>
  <key>ProgramArguments</key><array><string>/bin/bash</string><string>$repo/scripts/run_decider_evaluation_refresh.sh</string></array>
  <key>StartInterval</key><integer>1800</integer>
  <key>StandardOutPath</key><string>$log_dir/decider-evaluation-refresh.log</string>
  <key>StandardErrorPath</key><string>$log_dir/decider-evaluation-refresh.log</string>
</dict></plist>
PLIST
launchctl bootout "gui/$(id -u)/$label" 2>/dev/null || true
launchctl bootstrap "gui/$(id -u)" "$plist"
echo "installed $label -> $plist"
