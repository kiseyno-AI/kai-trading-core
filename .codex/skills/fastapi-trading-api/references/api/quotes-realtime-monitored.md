# GET /quotes/realtime/monitored and /stream

## Summary
Read or stream Core's process-local realtime price snapshot for execution-runtime monitored symbols. Core owns the KIS websocket and fans out the latest in-memory ticks; this route does not call KIS REST and does not persist realtime dashboard prices. The route is a realtime diagnostic surface; execution-runtime dashboard endpoints add local cache/daily display fallback fields for web cards.

## Example request
```http
GET /quotes/realtime/monitored?refresh_symbols=false
```

## Example response
```json
{
  "status": "subscribed",
  "as_of": "2026-04-30T00:15:31+00:00",
  "summary": {
    "items_total": 1,
    "subscribed_count": 1,
    "limited_count": 0,
    "price_status_counts": {
      "available": 1
    }
  },
  "items": [
    {
      "symbol": "005930",
      "current_price": 71500,
      "price_as_of": "2026-04-30T00:15:30+00:00",
      "price_received_at": "2026-04-30T00:15:31+00:00",
      "price_source": "kis_realtime_ws",
      "price_status": "available",
      "display_price_available": true,
      "realtime_subscribed": true,
      "realtime_subscription_reason": "subscribed"
    }
  ],
  "health": {
    "status": "subscribed",
    "tr_id": "H0STCNT0",
    "subscribed_symbols": ["005930"],
    "limited_symbols": []
  }
}
```

## Stream request
```http
GET /quotes/realtime/monitored/stream
```

## Stream event
```text
event: prices
data: {"status":"subscribed","summary":{"items_total":1},"items":[{"symbol":"005930","current_price":71500,"price_status":"available"}]}
```
