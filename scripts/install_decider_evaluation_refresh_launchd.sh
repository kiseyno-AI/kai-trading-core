#!/usr/bin/env bash
# Install a launchd job that runs the decider-evaluation refresh at 23:00 and
# 23:45 local time daily (the command itself decides whether it is due and
# refreshes once per KRX session). Remove with:
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
  <key>StartCalendarInterval</key><array>
    <dict><key>Hour</key><integer>23</integer><key>Minute</key><integer>0</integer></dict>
    <dict><key>Hour</key><integer>23</integer><key>Minute</key><integer>45</integer></dict>
  </array>
  <key>StandardOutPath</key><string>$log_dir/decider-evaluation-refresh.log</string>
  <key>StandardErrorPath</key><string>$log_dir/decider-evaluation-refresh.log</string>
</dict></plist>
PLIST
launchctl bootout "gui/$(id -u)/$label" 2>/dev/null || true
launchctl bootstrap "gui/$(id -u)" "$plist"
echo "installed $label -> $plist"
