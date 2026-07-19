# GET /execution/runtime/status

## Summary
Read the web-facing exec runtime summary from core. The route normalizes core↔exec readiness into concise UI states such as `ready`, `degraded`, `offline`, `paused`, and `kill_switched`, while keeping liveness and safety issues explicit.

## Contract source
Status literals and serialized payload keys are validated by `apps/api/app/orders/execution_handoff_contract.py`. Core owns this display read model, Exec owns execution truth and safety state, and Web consumes the payload for display only.

## Example request
```http
GET /execution/runtime/status?refresh=true
```

## Example response
```json
{
  "status": "ready",
  "mode": "mock",
  "exec_connected": true,
  "automation_enabled": true,
  "paused": false,
  "kill_switch_enabled": false,
  "monitoring_alive": true,
  "last_heartbeat_at": "2026-04-30T00:05:00+00:00",
  "open_order_count": 1,
  "active_execution_job_count": 1,
  "issues": [],
  "core_integration_mode": "dispatch",
  "delegate_execution": true
}
```
