# GET /execution/runtime/symbols/{symbol}

## Summary
Read the per-symbol execution detail from core for dashboard/operator pages. The route preserves the normalized daily-board status while adding account-scoped dispatch/readback detail and the planning context needed for debugging.

## Example request
```http
GET /execution/runtime/symbols/005930?market_date=2026-04-30&refresh=true
```

## Example response
```json
{
  "market_date": "2026-04-30",
  "symbol": "005930",
  "name": "Samsung Electronics",
  "market": "KOSPI",
  "runtime": {
    "status": "ready",
    "mode": "mock"
  },
  "planning": {
    "decision_source": "signal-decider",
    "decision_snapshot_ids": [
      144
    ],
    "policy_versions": [
      "signal_decider_policy_v1"
    ],
    "account_references": [
      "KIS:12345678-01"
    ]
  },
  "monitoring": {
    "state": "watching_target_stop",
    "position_side": "long",
    "planned_intents": [
      "stop_sell",
      "target_sell"
    ],
    "last_event_at": "2026-04-30T00:07:00+00:00",
    "runtime_status": "ready"
  },
  "summary": {
    "items_total": 1,
    "account_count": 1,
    "status_counts": {
      "filled": 1
    },
    "planned_quantity_total": 5,
    "filled_quantity_total": 5,
    "open_quantity_total": 5,
    "last_updated_at": "2026-04-30T00:07:00+00:00"
  },
  "items": [
    {
      "plan_item_id": 81,
      "account_reference": "KIS:12345678-01",
      "symbol": "005930",
      "status": "filled",
      "monitoring_state": "watching_target_stop",
      "dispatch": {
        "status": "accepted",
        "plan_id": "core-signal-decider-20260430-KIS_12345678_01"
      },
      "remote_state": {
        "derived_status": "open",
        "last_synced_at": "2026-04-30T00:07:00+00:00"
      },
      "orders": [
        {
          "id": "order-1",
          "status": "submitted"
        }
      ],
      "executions": [
        {
          "broker_order_id": "order-1",
          "side": "buy",
          "quantity": "5"
        }
      ],
      "positions": [
        {
          "symbol": "005930",
          "quantity": "5"
        }
      ]
    }
  ]
}
```
