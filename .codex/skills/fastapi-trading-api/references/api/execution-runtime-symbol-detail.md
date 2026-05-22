# GET /execution/runtime/symbols/{symbol}

## Summary
Read the per-symbol execution detail from core for dashboard/operator pages. The route preserves the normalized daily-board status while adding account-scoped dispatch/readback detail and the planning context needed for debugging, including one-or-many decider provenance when the same symbol is active from multiple deciders.

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
  "current_price": 71500,
  "price_as_of": "2026-04-30T00:15:30+00:00",
  "price_received_at": "2026-04-30T00:15:31+00:00",
  "price_source": "kis_realtime_ws",
  "price_status": "available",
  "runtime": {
    "status": "ready",
    "mode": "mock"
  },
  "planning": {
    "decision_source": "multiple",
    "decision_agent_names": [
      "short-term-decider",
      "signal-decider"
    ],
    "decision_snapshot_ids": [
      144,
      145
    ],
    "decision_run_ids": [
      "short-run-145",
      "signal-run-144"
    ],
    "source_feedback_ids": [
      701,
      702
    ],
    "policy_versions": [
      "short_term_decider_contract_v1",
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
    "items_total": 2,
    "account_count": 1,
    "decision_agent_counts": {
      "short-term-decider": 1,
      "signal-decider": 1
    },
    "status_counts": {
      "filled": 1,
      "submitted": 1
    },
    "planned_quantity_total": 10,
    "filled_quantity_total": 5,
    "open_quantity_total": 5,
    "last_updated_at": "2026-04-30T00:07:00+00:00"
  },
  "items": [
    {
      "plan_item_id": 81,
      "account_reference": "KIS:12345678-01",
      "symbol": "005930",
      "agent_name": "signal-decider",
      "decision_agent_name": "signal-decider",
      "status": "filled",
      "current_price": 71500,
      "price_as_of": "2026-04-30T00:15:30+00:00",
      "price_received_at": "2026-04-30T00:15:31+00:00",
      "price_source": "kis_realtime_ws",
      "price_status": "available",
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
    },
    {
      "plan_item_id": 82,
      "account_reference": "KIS:12345678-01",
      "symbol": "005930",
      "agent_name": "short-term-decider",
      "decision_agent_name": "short-term-decider",
      "status": "submitted",
      "monitoring_state": "watching_fill",
      "dispatch": {
        "status": "accepted",
        "plan_id": "core-short_term_decider-20260430-KIS_12345678_01"
      },
      "remote_state": {
        "derived_status": "open",
        "last_synced_at": "2026-04-30T00:07:00+00:00"
      },
      "validation": {
        "short_term": {
          "tactical_action": "watch_for_trigger",
          "trigger_price": 50500,
          "chase_limit_price": 51500
        }
      },
      "traceability": {
        "decision_run_id": "short-run-145",
        "source_feedback_id": 702
      },
      "orders": [
        {
          "id": "order-2",
          "status": "submitted"
        }
      ],
      "executions": [],
      "positions": []
    }
  ]
}
```
