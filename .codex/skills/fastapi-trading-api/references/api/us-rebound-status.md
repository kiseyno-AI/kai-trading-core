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

Optional `execution` reads only the current session's persisted safe operation
projection. It includes status/reasons and per-symbol status with an actual
`last_confirmed_at` when available; it excludes policy paths, tokens, prices,
account-derived claim IDs and private evidence. `order_execution_enabled` is true
only for today's KST target when current paper mode and true execution opt-in match persisted
confirmed active watch authority. It is not a fresh broker check; market and
Exec pause/kill gates still apply.
`execution_connection=rule_v4` requires persisted registration/activation proof;
otherwise it remains `not_connected`. Observation never performs execution HTTP.
Configuration failures update `execution.status`/`reason` while retaining valid
current-session items, their persisted mode and unchanged `last_confirmed_at`.
Known active items can therefore coexist with a failed job status. Corrupt or
mis-scoped previous state returns 503 rather than losing its evidence.

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
  "execution": null,
  "server_time": "2026-11-27T06:45:00+09:00",
  "mode": "observe",
  "enabled_from": "2026-11-27",
  "order_execution_enabled": false,
  "execution_connection": "not_connected",
  "schedule_kst": {
    "collection": ["07:00", "07:05", "07:10"],
    "evaluation": "07:15",
    "execution_evidence": ["08:20", "08:30"],
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
