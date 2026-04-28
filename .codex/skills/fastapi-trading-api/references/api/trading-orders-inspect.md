# GET /trading/orders/inspect

## Summary
Inspect the in-process reconciliation scheduler state plus the current mismatch, stale-pending, and missing-fill-detail order buckets for KIS order recovery work.

## Example request
```http
GET /trading/orders/inspect?account_reference=KIS:12345678-01&limit=25&stale_after_minutes=30
```

## Example response
```json
{
  "filters": {
    "account_reference": "KIS:12345678-01",
    "limit": 25,
    "stale_after_minutes": 30
  },
  "reconciliation": {
    "enabled": true,
    "interval_seconds": 300,
    "last_trigger": "periodic"
  },
  "mismatches": {
    "count": 1,
    "items": [
      {
        "client_order_id": "ORDER-20260428-001",
        "last_reconcile_status": "repaired"
      }
    ]
  },
  "stale_pending_orders": {
    "count": 1,
    "items": [
      {
        "client_order_id": "ORDER-20260428-002",
        "current_status": "submitted"
      }
    ]
  },
  "missing_fill_detail_orders": {
    "count": 0,
    "items": []
  }
}
```
