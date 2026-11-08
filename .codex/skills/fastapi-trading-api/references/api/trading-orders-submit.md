# POST /trading/orders/buy and POST /trading/orders/sell

## Summary
Submit an internal KIS domestic cash buy or sell order with durable request, order, and event persistence plus client-order idempotency.

## Example request
```http
POST /trading/orders/buy
Content-Type: application/json

{
  "client_order_id": "ORDER-20260428-001",
  "account_number": "12345678",
  "account_product_code": "01",
  "symbol": "005930",
  "quantity": "10",
  "price": "70000",
  "order_type": "limit",
  "time_in_force": "day",
  "exchange_id": "KRX",
  "source": "api"
}
```

## Example response
```json
{
  "operation": "submit",
  "broker_name": "kis",
  "account_reference": "KIS:12345678-01",
  "client_order_id": "ORDER-20260428-001",
  "created": true,
  "idempotent_replay": false,
  "request": {
    "submission_status": "submitted",
    "submission_attempts": 1
  },
  "order": {
    "broker_order_id": "0000123456",
    "symbol": "005930",
    "side": "buy",
    "current_status": "submitted",
    "requested_quantity": "10.00000000",
    "requested_price": "70000.00000000"
  },
  "events": [
    {
      "event_type": "submit_request",
      "source": "local_request"
    },
    {
      "event_type": "submit_response",
      "source": "rest_submit"
    }
  ]
}
```

## Availability

`POST /trading/orders/buy`, `POST /trading/orders/sell`, and `POST /trading/orders/{client_order_id}/amend` are disabled by default and return:

```json
{
  "detail": {
    "error": "direct_order_api_disabled",
    "message": "Direct broker order routes are disabled on Core. ..."
  }
}
```

with status `403`, before the request body is validated. Set `TRADING_DIRECT_ORDER_API_ENABLED=true` to enable them. Cancel is not gated.
