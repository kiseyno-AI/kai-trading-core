# GET /execution/runtime/monitored-symbols

## Summary
Read the current symbol-level exec monitoring view from core. Core groups same-day execution rows by symbol and exposes dashboard-friendly monitoring states without requiring the web to call exec directly.

## Example request
```http
GET /execution/runtime/monitored-symbols?market_date=2026-04-30&refresh=true
```

## Example response
```json
{
  "market_date": "2026-04-30",
  "runtime": {
    "status": "ready",
    "mode": "mock"
  },
  "summary": {
    "items_total": 1,
    "watching_target_stop": 1,
    "watching_fill": 0,
    "reconciling": 0,
    "idle": 0
  },
  "items": [
    {
      "symbol": "005930",
      "name": "Samsung Electronics",
      "market": "KOSPI",
      "monitoring_state": "watching_target_stop",
      "position_side": "long",
      "planned_intents": [
        "stop_sell",
        "target_sell"
      ],
      "decision_agent_names": [
        "short-term-decider",
        "signal-decider"
      ],
      "last_event_at": "2026-04-30T00:07:00+00:00",
      "account_references": [
        "KIS:12345678-01"
      ],
      "status_counts": {
        "filled": 1,
        "submitted": 1
      }
    }
  ]
}
```
