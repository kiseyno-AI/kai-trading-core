#!/bin/zsh
# Install the weekly prune job (Saturday and Sunday 01:00 host-local time;
# the script itself only acts on a KST weekend with an idle pipeline and
# low free space). Usage: scripts/install_prune_trading_agent_codex_history_launchd.sh
set -eu
REPO_ROOT="${0:A:h:h}"
LABEL="com.kiseyno.kai-trading-agent-history-prune"
PLIST="$HOME/Library/LaunchAgents/$LABEL.plist"
cat > "$PLIST" <<PLIST
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key><string>$LABEL</string>
  <key>ProgramArguments</key>
  <array>
    <string>/bin/zsh</string>
    <string>$REPO_ROOT/scripts/run_prune_trading_agent_codex_history.sh</string>
  </array>
  <key>StartCalendarInterval</key>
  <array>
    <dict><key>Weekday</key><integer>6</integer><key>Hour</key><integer>1</integer><key>Minute</key><integer>0</integer></dict>
    <dict><key>Weekday</key><integer>0</integer><key>Hour</key><integer>1</integer><key>Minute</key><integer>0</integer></dict>
  </array>
  <key>ProcessType</key><string>Background</string>
  <key>StandardOutPath</key><string>/dev/null</string>
  <key>StandardErrorPath</key><string>/dev/null</string>
</dict>
</plist>
PLIST
launchctl bootout "gui/$(id -u)/$LABEL" 2>/dev/null || true
launchctl bootstrap "gui/$(id -u)" "$PLIST"
echo "installed $PLIST"
