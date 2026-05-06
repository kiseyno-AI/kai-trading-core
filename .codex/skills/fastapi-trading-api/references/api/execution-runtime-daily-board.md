# GET /execution/runtime/daily-board

## Summary
Read the same-day execution board from core. The route combines persisted core planning rows with exec runtime/order/execution/position readback, normalizes each row into concise dashboard states, and preserves explicit decision-source metadata so same-symbol rows from different deciders stay distinguishable.

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
      "agent_name": "short-term-decider",
      "decision_agent_name": "short-term-decider",
      "intent": "next_open_buy",
      "status": "filled",
      "monitoring_state": "watching_target_stop",
      "planned_quantity": 5,
      "filled_quantity": 5,
      "open_quantity": 5,
      "planned_price": 51500,
      "avg_fill_price": 50600,
      "decision_source": "short-term-decider",
      "decision_snapshot_id": 145,
      "source_feedback_id": 702,
      "decision_run_id": "short-run-145",
      "execution_plan_id": "core-short_term_decider-20260430-KIS_12345678_01",
      "broker_order_id": "order-1",
      "last_updated_at": "2026-04-30T00:07:00+00:00",
      "failure_reason": null,
      "trigger_price": 50500,
      "chase_limit_price": 51500,
      "expected_holding_period": "1-2 trading days",
      "entry_execution_mode": "pending_trigger_limit"
    }
  ]
}
```
