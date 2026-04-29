# GET /execution/runtime/daily-board

## Summary
Read the same-day execution board from core. The route combines persisted core planning rows with exec runtime/order/execution/position readback and normalizes each row into concise dashboard states.

## Example request
```http
GET /execution/runtime/daily-board?market_date=2026-04-30&refresh=true
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
    "planned": 0,
    "accepted": 0,
    "scheduled": 0,
    "submitted": 0,
    "partial": 0,
    "filled": 1,
    "cancelled": 0,
    "failed": 0
  },
  "items": [
    {
      "plan_item_id": 81,
      "account_reference": "KIS:12345678-01",
      "symbol": "005930",
      "name": "Samsung Electronics",
      "market": "KOSPI",
      "intent": "next_open_buy",
      "status": "filled",
      "monitoring_state": "watching_target_stop",
      "planned_quantity": 5,
      "filled_quantity": 5,
      "open_quantity": 5,
      "planned_price": 50000,
      "avg_fill_price": 50100,
      "decision_source": "signal-decider",
      "decision_snapshot_id": 144,
      "execution_plan_id": "core-signal-decider-20260430-KIS_12345678_01",
      "broker_order_id": "order-1",
      "last_updated_at": "2026-04-30T00:07:00+00:00",
      "failure_reason": null
    }
  ]
}
```
