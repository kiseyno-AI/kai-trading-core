# GET /quotes/realtime/monitored and /stream

## Summary
Read or stream Core's process-local realtime price snapshot for execution-runtime monitored symbols. Core owns the KIS websocket and fans out the latest in-memory ticks. The existing background WAIT multi-quote sampler also supplies display-only current prices and best bid/ask for candidates without a fresh websocket tick, including subscription-capacity-limited symbols; the route itself does not call KIS REST or persist these display samples. The route is a realtime diagnostic surface; execution-runtime dashboard endpoints add local cache/daily display fallback fields for web cards.

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

When WAIT orderbook sampling is active, `health.wait_orderbook_observation`
reports `requested_count`, `sampled_count`, `failed_batches`, and
`last_sample_at` for the latest regular-session sampling round. The
sampled count measures orderbook coverage separately from trade subscriptions.

## WAIT display samples

Fresh background samples use `price_source=kis_rest_multi_quote`,
`price_status=available`, and `display_price_available=true` even when
`realtime_subscribed=false` and `realtime_subscription_reason=capacity_limited`.
They expose `current_price`, `bid_price`, `ask_price`, `previous_close_price`,
`change`, and `change_pct`. Invalid/crossed bid/ask pairs are omitted as null.
Both `price_as_of` and `price_received_at` are the REST receipt time, identified
by `price_time_basis=received_at`: KIS does not supply an exchange trade time
in this response. These samples expire under the existing realtime stale
threshold and are hidden outside a verified active regular session. They are
never websocket ticks, trade-touch evidence, or orderbook-depth events.
The stream includes display samples using the same snapshot serializer.
