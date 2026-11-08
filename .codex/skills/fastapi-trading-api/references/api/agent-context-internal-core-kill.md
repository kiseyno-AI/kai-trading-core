# POST /agent-context/internal/core/control

## Summary
Kills or restarts the local core API process. The route only acts on the port that this API instance is serving.

- `port` is optional. When omitted it defaults to the port the request arrived on.
- A `port` that differs from the served port is rejected with `422` and no process is inspected or signaled. The route can no longer be pointed at other local services such as the Exec runtime.
- Only the process **listening** on that port is targeted (`lsof -nP -ti TCP:<port> -sTCP:LISTEN`). Processes that merely hold a client connection to the port are never signaled.
- `restart` runs `launchctl bootout/bootstrap` for the Core API plist; `killed_pids` in that response lists the listener PIDs observed, not processes it signaled.

## Request

```json
{
  "action": "restart",
  "signal_name": "SIGKILL"
}
```

`action` is `kill` or `restart` (default `kill`); `signal_name` is `SIGTERM` or `SIGKILL` (default `SIGKILL`).

## Response

```json
{
  "status": "restarted",
  "port": 8000,
  "signal": "SIGKILL",
  "killed_pids": [12345],
  "self_targeted": true,
  "launchctl_stdout": "",
  "launchctl_stderr": "",
  "plist_path": "apps/api/com.kiseyno.kai-trading-core-api.plist"
}
```

## Errors

```json
{ "detail": "port must match the port this API is serving (8000)" }
```
