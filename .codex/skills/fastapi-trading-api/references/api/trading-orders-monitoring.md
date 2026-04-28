# GET /trading/orders/lifecycle, /mismatches, /stale-pending, and /realtime-health

## Summary
Web-facing KIS order monitoring reads for lifecycle buckets, reconcile mismatch review, stale active submissions, and realtime order-notice stream health.

## Example request
```http
GET /trading/orders/lifecycle?account_reference=KIS:12345678-01&limit=25&stale_after_minutes=30
```

## Example response
```json
{
  "trade_date": "2026-04-28",
  "summary": {
    "today_orders": 4,
    "open_orders": 2,
    "partially_filled_orders": 1,
    "filled_orders": 1,
    "canceled_or_rejected_orders": 0,
    "replaced_orders": 0,
    "mismatch_count": 1,
    "stale_pending_count": 1,
    "missing_fill_detail_count": 0,
    "realtime_healthy": true,
    "realtime_status": "subscribed"
  },
  "buckets": {
    "open_orders": {
      "count": 2,
      "items": [
        {
          "client_order_id": "ORDER-20260428-001",
          "symbol": "005930",
          "current_status": "accepted",
          "status_label": "Accepted",
          "lifecycle_bucket": "open_orders",
          "attention_flags": []
        }
      ]
    }
  },
  "attention": {
    "mismatch_count": 1,
    "stale_pending_count": 1,
    "missing_fill_detail_count": 0,
    "realtime_attention_flags": []
  }
}
```

## Related requests
```http
GET /trading/orders/mismatches?account_reference=KIS:12345678-01&limit=50
GET /trading/orders/stale-pending?account_reference=KIS:12345678-01&limit=50
GET /trading/orders/realtime-health
```
