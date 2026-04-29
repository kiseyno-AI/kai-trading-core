# GET /trading/orders/exec/runtime

## Summary
Read the current `kai-trading-exec` runtime readiness from core. The response keeps integration configuration, auth presence, and remote runtime failure states explicit so operators can distinguish `disabled`, `not_configured`, `unavailable`, and contract-level failures without querying exec directly.

## Example request
```http
GET /trading/orders/exec/runtime?refresh=true
```

## Example response
```json
{
  "integration": {
    "mode": "dispatch",
    "enabled": true,
    "delegate_execution": true,
    "config_status": "ready",
    "base_url": "http://127.0.0.1:8010",
    "bearer_token_configured": true,
    "dispatch_source": "kai-trading-core",
    "runtime_mode": "mock",
    "issues": []
  },
  "runtime": {
    "status": "ok",
    "payload": {
      "mode": "mock",
      "ready": true,
      "readiness": {
        "reasons": []
      },
      "safety": {
        "paused": false,
        "kill_switch_enabled": false,
        "automation_enabled": true
      },
      "counts": {
        "open_orders": 1,
        "active_execution_jobs": 2
      }
    },
    "error": null
  }
}
```
