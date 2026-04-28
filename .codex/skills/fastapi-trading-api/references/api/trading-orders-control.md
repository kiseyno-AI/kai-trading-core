# POST /trading/orders/{client_order_id}/cancel and POST /trading/orders/{client_order_id}/amend

## Summary
Control an existing KIS domestic cash order by canceling the remaining quantity or amending the live order after account-scoped inquiry validation.

## Example request
```http
POST /trading/orders/ORDER-20260428-001/cancel
Content-Type: application/json

{
  "account_reference": "KIS:12345678-01",
  "all_remaining": true,
  "exchange_id": "KRX"
}
```

## Example response
```json
{
  "operation": "cancel",
  "broker_name": "kis",
  "account_reference": "KIS:12345678-01",
  "client_order_id": "ORDER-20260428-001",
  "idempotent_replay": false,
  "order": {
    "broker_order_id": "0000123456",
    "current_status": "cancel_pending"
  },
  "events": [
    {
      "event_type": "cancel_request",
      "source": "local_request"
    },
    {
      "event_type": "cancel_request",
      "source": "rest_submit"
    }
  ]
}
```
