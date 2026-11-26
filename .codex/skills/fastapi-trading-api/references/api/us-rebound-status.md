# GET /strategies/us-rebound/status

## Summary

Reads persisted US-rebound observation evidence for `target_trade_date` (ISO date,
default today in KST). The current route reads the `real` namespace from
`US_REBOUND_STATE_DIR`. It makes no collection, provider, paper-ledger or order
calls. Existing global bearer authentication applies when configured.

`status` describes persistence/liveness (`not_started`, `not_due`, `collecting`,
`unavailable`, `failed`, `completed`). `evaluation.status`, when present, is the
exploratory `WATCH`, `NO_SIGNAL` or `UNAVAILABLE` outcome. `completed` grants no
order authority. Configuration and schedule fields do not establish that the
optional launchd job is installed or currently running; inspect `last_tick`.
No account or quantity fields are returned.

## Example request

```http
GET /strategies/us-rebound/status?target_trade_date=2026-11-27
Authorization: Bearer <configured-token>
```

## Example response

Example before a persisted tick, with observation configured:

```json
{
  "strategy_id": "krx_us_rebound",
  "target_trade_date": "2026-11-27",
  "source_environment": "real",
  "status": "not_started",
  "evaluation": null,
  "last_tick": null,
  "server_time": "2026-11-27T06:45:00+09:00",
  "mode": "observe",
  "enabled_from": "2026-11-27",
  "order_execution_enabled": false,
  "execution_connection": "not_connected",
  "schedule_kst": {
    "collection": ["07:00", "07:05", "07:10"],
    "evaluation": "07:15",
    "deadline": "08:40"
  }
}
```

Completed evaluations include an immutable `evaluation_id`, rule/input/feature
identities when available, outcome reasons and `order_authority: false`.
`last_tick` includes the latest persisted `checked_at` and liveness status.

## Errors

Malformed dates return FastAPI validation error 422. Corrupt, mismatched or
unreadable persisted state returns 503 without file contents, paths or secrets:

```json
{
  "detail": {
    "code": "us_rebound_status_unavailable",
    "error_type": "ValueError"
  }
}
```

See [operation, activation and rollback](../../../../../docs/US_REBOUND_OPERATION.md).
