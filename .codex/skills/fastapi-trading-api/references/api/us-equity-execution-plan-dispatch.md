# POST /us-equities/execution-plans/dispatch

## Summary

Build an inspectable, whole-share USD execution-plan proposal from one exact US
agent run. `agent_run_id` is required and `dry_run` defaults to `true`. The
current Exec v4 delegated-sizing capability is KIS/KRX/KRW-only, so this route
always returns a preview and never transmits a USD plan. A non-preview request
returns HTTP `409` even when `US_EQUITY_EXECUTION_ENABLED=true`.

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
  "status": "preview_only",
  "execution_supported": false,
  "execution_block_reason": "exec_currency_aware_sizing_capability_unavailable",
  "dry_run": true,
  "source_market_date": "2026-08-19",
  "target_trade_date": "2026-08-20",
  "accepted_candidate_count": 1,
  "rejected_candidate_count": 0,
  "rejections": [],
  "payload": null,
  "preview_payload": {
    "plan_id": "core-us-signal-decider-20260820-kis-us-symbol-run-2026-08-19-a1b2c3d4e5",
    "source": "kai-trading-core",
    "mode": "paper",
    "market_date": "2026-08-20",
    "strategy_run_id": "us-symbol-run-2026-08-19-a1b2c3d4e5",
    "requested_at": "2026-08-19T21:00:00+00:00",
    "items": [
      {
        "symbol": "AAPL",
        "provider": "kis",
        "venue": "kis",
        "exchange": "NASD",
        "currency": "USD"
      }
    ],
    "metadata": {
      "schema_version": "signal_decider_exec_plan_v4",
      "required_capability": "delegated_final_full_cost_gate_recheck_v1",
      "source_market_date": "2026-08-19",
      "target_trade_date": "2026-08-20"
    }
  }
}
```
