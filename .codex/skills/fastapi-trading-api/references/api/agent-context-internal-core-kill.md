# POST /agent-context/internal/core/control

## Summary
Kills or restarts the local core API process that is listening on the requested port.

## Request

```json
{
  "action": "restart",
  "signal_name": "SIGKILL",
  "port": 8000
}
```

## Response

```json
{
  "status": "restarted",
  "port": 8000,
  "signal": "SIGKILL",
  "killed_pids": [12345],
  "launchctl_stdout": "",
  "launchctl_stderr": ""
}
```
