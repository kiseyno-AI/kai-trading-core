# GET /trading/orders/{client_order_id}/timeline

## Summary
Read one KIS order with a normalized event timeline, labeled event metadata, and the latest lifecycle summary for web detail views and operator audits.

## Example request
```http
GET /trading/orders/ORDER-20260428-001/timeline?account_reference=KIS:12345678-01&refresh=true
```

## Example response
```json
{
  "operation": "timeline",
  "client_order_id": "ORDER-20260428-001",
  "summary": {
    "broker_order_id": "0000123456",
    "symbol": "005930",
    "current_status": "filled",
    "status_label": "Filled",
    "filled_quantity": "10",
    "average_fill_price": "70120",
    "attention_flags": []
  },
  "timeline": {
    "count": 3,
    "latest_event": {
      "event_type": "fill",
      "event_label": "Filled",
      "source": "poll_reconcile",
      "is_execution_event": true,
      "is_reconciliation_event": true
    },
    "items": [
      {
        "event_type": "submit_request",
        "event_label": "Submit requested"
      },
      {
        "event_type": "submit_response",
        "event_label": "Submit acknowledged"
      },
      {
        "event_type": "fill",
        "event_label": "Filled"
      }
    ]
  },
  "reconcile": {
    "reconcile_status": "matched",
    "broker_truth_found": true
  }
}
```
