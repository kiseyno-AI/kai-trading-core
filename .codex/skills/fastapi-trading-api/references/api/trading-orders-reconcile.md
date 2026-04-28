# POST /trading/orders/reconcile and POST /trading/orders/{client_order_id}/reconcile

## Summary
Trigger broker-truth reconciliation for recent persisted KIS orders or force the repair path for one known `client_order_id`.

## Example request
```http
POST /trading/orders/reconcile
Content-Type: application/json

{
  "account_reference": "KIS:12345678-01",
  "symbol": "005930",
  "limit": 25,
  "reason": "manual-audit",
  "include_orders": true
}
```

## Example response
```json
{
  "triggered": true,
  "trigger": "manual",
  "reason": "manual-audit",
  "selected_count": 1,
  "reconciled_count": 1,
  "repaired_count": 1,
  "mismatch_count": 1,
  "stale_pending_count": 0,
  "missing_fill_detail_count": 0,
  "error_count": 0,
  "orders": [
    {
      "client_order_id": "ORDER-20260428-001",
      "order": {
        "broker_order_id": "0000123456",
        "current_status": "filled",
        "last_reconcile_status": "repaired"
      },
      "reconcile": {
        "broker_truth_found": true,
        "repair_applied": true,
        "mismatch_reasons": ["status", "filled_quantity", "remaining_quantity"]
      }
    }
  ]
}
```
