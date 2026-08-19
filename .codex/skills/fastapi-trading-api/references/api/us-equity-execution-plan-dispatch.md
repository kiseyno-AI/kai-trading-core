# POST /us-equities/execution-plans/dispatch

## Summary

Build a KIS-routed, whole-share USD execution plan from the specified agent run. `dry_run` defaults to `true`; non-preview dispatch requires both `US_EQUITY_EXECUTION_ENABLED=true` and the current `agent_run_id`.

## Example request

```http
POST /us-equities/execution-plans/dispatch
Content-Type: application/json

{
  "source_market_date": "2026-08-19",
  "target_trade_date": "2026-08-20",
  "max_candidates": 10,
  "dry_run": true,
  "agent_run_id": "us-symbol-run-2026-08-19-a1b2c3d4e5"
}
```

## Example response

```json
{
  "status": "ready",
  "dry_run": true,
  "source_market_date": "2026-08-19",
  "target_trade_date": "2026-08-20",
  "accepted_candidate_count": 1,
  "rejected_candidate_count": 0,
  "rejections": [],
  "payload": {
    "plan_id": "core-us-signal-decider-20260820-kis",
    "strategy_run_id": "us-symbol-run-2026-08-19-a1b2c3d4e5",
    "items": [{"symbol":"AAPL","provider":"kis","venue":"kis","exchange":"NASD","currency":"USD"}],
    "metadata": {"schema_version":"signal_decider_exec_plan_v3","market":"US","currency":"USD"}
  }
}
```
