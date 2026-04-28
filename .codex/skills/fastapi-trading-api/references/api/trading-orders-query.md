# GET /trading/orders/{client_order_id} and GET /trading/orders

## Summary
Read the persisted request snapshot, current broker-order snapshot, and event history for one order or list recent orders for an account.

## Example request
```http
GET /trading/orders/ORDER-20260428-001?account_reference=KIS:12345678-01&refresh=true
```

## Example response
```json
{
  "operation": "get",
  "broker_name": "kis",
  "account_reference": "KIS:12345678-01",
  "client_order_id": "ORDER-20260428-001",
  "request": {
    "submission_status": "submitted"
  },
  "order": {
    "broker_order_id": "0000123456",
    "symbol": "005930",
    "current_status": "partial_fill",
    "filled_quantity": "4.00000000",
    "remaining_quantity": "6.00000000"
  },
  "events": [
    {
      "event_type": "submit_request"
    },
    {
      "event_type": "submit_response"
    },
    {
      "event_type": "partial_fill"
    }
  ],
  "idempotent_replay": false,
  "broker_inquiry": {
    "matched_daily_order": {
      "odno": "0000123456",
      "tot_ccld_qty": "4",
      "rmn_qty": "6"
    }
  }
}
```
