# GET /trading/orders/intraday-auto-sell and POST /trading/orders/intraday-auto-sell/run-if-due

## Summary
Inspect or execute the persisted intraday auto-sell watcher for positions linked to submitted signal-decider next-open buys. The watcher refreshes live account positions plus KIS quotes, compares them to persisted `target_price` and `stop_loss_price` thresholds, and submits market sell orders with duplicate suppression and cooldown-aware retry behavior.

## Example request
```http
GET /trading/orders/intraday-auto-sell?account_reference=KIS:12345678-01&symbol=005930&refresh=true
```

```http
POST /trading/orders/intraday-auto-sell/run-if-due
Content-Type: application/json

{
  "account_reference": "KIS:12345678-01",
  "symbol": "005930",
  "dry_run": false,
  "force": false
}
```

## Example response
```json
{
  "summary": {
    "items_total": 1,
    "ready_count": 1,
    "target_ready_count": 1,
    "submitted_count": 1
  },
  "items": [
    {
      "plan_item_id": 112,
      "symbol": "005930",
      "account_reference": "KIS:12345678-01",
      "decision_snapshot_id": 144,
      "execution_status": "submitted",
      "client_order_id": "SDIS-20260430-005930-TGT-A1",
      "validation": {
        "trigger": {
          "rule": "target_hit",
          "live_price": 55000,
          "target_price": 54000,
          "stop_loss_price": 47000,
          "watched_quantity": "5"
        }
      },
      "traceability": {
        "linked_next_open_plan_item_id": 81,
        "linked_next_open_client_order_id": "SDNO-20260430-005930"
      }
    }
  ]
}
```
