# GET /execution/runtime/monitored-symbols

## Summary
Read the current symbol-level exec monitoring view from core. The route is dashboard-safe and cache-first: it groups the local daily-board read model by symbol and returns immediately, including an empty item list when no symbols are monitored. `refresh=true` schedules a bounded, debounced background exec refresh instead of making the request wait on exec/KIS readback.

## Example request
```http
GET /execution/runtime/monitored-symbols?market_date=2026-04-30&refresh=true
```

## Example response
```json
{
  "market_date": "2026-04-30",
  "runtime": {
    "status": "ready",
    "mode": "mock"
  },
  "freshness": {
    "schema_version": "execution_runtime_dashboard_read_model_v1",
    "endpoint": "monitored_symbols",
    "market_date": "2026-04-30",
    "status": "refreshing",
    "served_from_cache": false,
    "read_model_source": "persisted_exec_read_model",
    "generated_at": "2026-04-30T00:15:35+00:00",
    "cached_at": "2026-04-30T00:15:35+00:00",
    "cache_age_seconds": 0,
    "cache_ttl_seconds": 5,
    "is_stale": true,
    "is_partial": false,
    "refreshing": true,
    "last_readback_synced_at": "2026-04-30T00:07:00+00:00",
    "readback_age_seconds": 515,
    "readback_synced_count": 1,
    "readback_missing_count": 0,
    "overview_item_count": 1,
    "output_item_count": 1
  },
  "refresh": {
    "requested": true,
    "status": "scheduled",
    "background": true,
    "in_flight": true,
    "timeout_seconds": 2.0,
    "debounce_seconds": 15.0,
    "last_error": null
  },
  "summary": {
    "items_total": 1,
    "watching_target_stop": 1,
    "watching_fill": 0,
    "reconciling": 0,
    "idle": 0
  },
  "items": [
    {
      "symbol": "005930",
      "name": "Samsung Electronics",
      "market": "KOSPI",
      "current_price": 71500,
      "price_as_of": "2026-04-30T00:15:30+00:00",
      "price_received_at": "2026-04-30T00:15:31+00:00",
      "price_source": "kis_realtime_ws",
      "price_status": "available",
      "monitoring_state": "watching_target_stop",
      "position_side": "long",
      "planned_intents": [
        "stop_sell",
        "target_sell"
      ],
      "decision_agent_names": [
        "short-term-decider",
        "signal-decider"
      ],
      "last_event_at": "2026-04-30T00:07:00+00:00",
      "account_references": [
        "KIS:12345678-01"
      ],
      "status_counts": {
        "filled": 1,
        "submitted": 1
      }
    }
  ]
}
```
