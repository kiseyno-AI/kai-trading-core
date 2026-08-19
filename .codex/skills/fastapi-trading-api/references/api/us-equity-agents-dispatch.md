# POST /us-equities/agents/dispatch

## Summary

Prepare bounded candidate contexts and role scopes, then run the US agent chain in dependency order. Downstream roles are skipped per symbol when required upstream feedback is missing. The response supplies the `run_id` required for a non-preview execution dispatch.

## Example request

```http
POST /us-equities/agents/dispatch
Content-Type: application/json

{"market_date":"2026-08-19","roles":["signal-decider"],"persist":true,"batch_size":5}
```

## Example response

```json
{
  "status": "completed",
  "market_date": "2026-08-19",
  "run_id": "us-symbol-run-2026-08-19-a1b2c3d4e5",
  "persist": true,
  "role_counts": {"eod-summarizer": 150, "technical-summarizer": 120, "fundamental-summarizer": 100, "signal-decider": 80},
  "completed": 450,
  "failed": 0,
  "items": [],
  "failures": []
}
```
