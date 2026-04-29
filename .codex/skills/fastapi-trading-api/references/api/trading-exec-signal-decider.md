# GET /trading/orders/exec/signal-decider

## Summary
Read the core-owned aggregated core↔exec view for signal-decider execution. The route starts from persisted `signal_decider_execution_plan_items`, overlays stored exec dispatch linkage and audit state, and with `refresh=true` calls the exec northbound runtime/order/execution/position APIs before persisting the latest matched readback snapshot back onto the durable plan rows.

## Example request
```http
GET /trading/orders/exec/signal-decider?target_trade_date=2026-04-30&symbol=005930&refresh=true
```

## Example response
```json
{
  "request": {
    "target_trade_date": "2026-04-30",
    "account_reference": null,
    "symbol": "005930",
    "refresh": true
  },
  "integration": {
    "mode": "dispatch",
    "enabled": true,
    "delegate_execution": true,
    "config_status": "ready"
  },
  "runtime": {
    "status": "ok",
    "payload": {
      "mode": "mock",
      "ready": true
    }
  },
  "summary": {
    "items_total": 1,
    "eligible_count": 1,
    "blocked_count": 0,
    "dispatch_status_counts": {
      "accepted": 1
    },
    "remote_status_counts": {
      "open": 1
    },
    "remote_open_count": 1,
    "remote_closed_count": 0
  },
  "items": [
    {
      "plan_item_id": 81,
      "symbol": "005930",
      "account_reference": "KIS:12345678-01",
      "decision_snapshot_id": 144,
      "target_price": 54000,
      "stop_loss_price": 47000,
      "dispatch": {
        "status": "accepted",
        "plan_id": "core-signal-decider-20260430-KIS_12345678_01",
        "item_links": [
          {
            "intent": "next_open_buy",
            "exec_plan_item_id": "exec-item-1"
          },
          {
            "intent": "target_sell",
            "exec_plan_item_id": "exec-item-2"
          },
          {
            "intent": "stop_sell",
            "exec_plan_item_id": "exec-item-3"
          }
        ]
      },
      "remote_state": {
        "last_synced_at": "2026-04-30T00:06:00+00:00",
        "derived_status": "open",
        "entry_filled_quantity": "5",
        "exit_filled_quantity": null,
        "open_quantity": "5",
        "closed_quantity": "0",
        "orders": [
          {
            "id": "order-1",
            "execution_plan_item_id": "exec-item-1",
            "symbol": "005930",
            "status": "submitted"
          }
        ],
        "executions": [
          {
            "broker_order_id": "order-1",
            "symbol": "005930",
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
    }
  ]
}
```
