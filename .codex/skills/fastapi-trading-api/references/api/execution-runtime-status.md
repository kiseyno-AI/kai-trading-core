# GET /execution/runtime/status

## Summary
Read the web-facing exec runtime summary from core. The route normalizes core↔exec readiness into concise UI states such as `ready`, `degraded`, `offline`, `paused`, and `kill_switched`, while keeping liveness and safety issues explicit. Current Exec autonomous monitoring fields under `liveness.autonomous.monitoring` are normalized alongside legacy top-level monitoring fields.

`daily_pnl`, `weekly_pnl`, and `monthly_pnl` remain Exec-owned dictionaries. `pnl_coverage` reports whether each period was provided and computable upstream. Missing values remain `null` for backward compatibility, with an explicit `status_reason` such as `not_provided_by_exec`, `not_computable_by_exec`, `runtime_payload_not_loaded`, or `runtime_unavailable`.

## Contract source
Status literals and serialized payload keys are validated by `apps/api/app/orders/execution_handoff_contract.py`. Core owns this display read model, Exec owns execution truth and safety state, and Web consumes the payload for display only.

## Example request
```http
GET /execution/runtime/status
```

The default read is local/cache-only. Pass `refresh=true` only for an explicit operator refresh that should contact the exec runtime.

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
  "daily_pnl": {
    "daily_realized_pnl": "8800.0000",
    "pnl_method": "fifo_internal_fills"
  },
  "weekly_pnl": null,
  "monthly_pnl": null,
  "pnl_coverage": {
    "daily": {
      "status": "available",
      "status_reason": "provided_by_exec",
      "source": "exec_runtime_status"
    },
    "weekly": {
      "status": "unavailable",
      "status_reason": "not_provided_by_exec",
      "source": "exec_runtime_status"
    },
    "monthly": {
      "status": "unavailable",
      "status_reason": "not_provided_by_exec",
      "source": "exec_runtime_status"
    }
  },
  "issues": [],
  "core_integration_mode": "dispatch",
  "delegate_execution": true
}
```
