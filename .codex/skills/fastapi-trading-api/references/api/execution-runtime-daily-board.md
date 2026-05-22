# GET /execution/runtime/daily-board

## Summary
Read the same-day execution board from core. The route is dashboard-safe and cache-first: it returns the best local read model immediately from persisted Core planning/readback state and a short in-process snapshot cache. `refresh=true` schedules a bounded, debounced background exec refresh instead of blocking the HTTP request. Consumers should use `freshness` and `refresh` to label stale, partial, or refreshing data honestly.

## Example request
```http
GET /execution/runtime/daily-board?market_date=2026-04-30&refresh=true
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
    "endpoint": "daily_board",
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
    "planned": 0,
    "accepted": 0,
    "scheduled": 0,
    "submitted": 0,
    "partial": 0,
    "filled": 1,
    "cancelled": 0,
    "failed": 0
  },
  "items": [
    {
      "plan_item_id": 81,
      "account_reference": "KIS:12345678-01",
      "symbol": "005930",
      "name": "Samsung Electronics",
      "market": "KOSPI",
      "agent_name": "short-term-decider",
      "decision_agent_name": "short-term-decider",
      "intent": "intraday_watch_buy",
      "status": "filled",
      "current_price": 71500,
      "price_as_of": "2026-04-30T00:15:30+00:00",
      "price_received_at": "2026-04-30T00:15:31+00:00",
      "price_source": "kis_realtime_ws",
      "price_status": "available",
      "monitoring_state": "watching_target_stop",
      "planned_quantity": 5,
      "filled_quantity": 5,
      "open_quantity": 5,
      "planned_price": 51500,
      "avg_fill_price": 50600,
      "decision_source": "short-term-decider",
      "decision_snapshot_id": 145,
      "source_feedback_id": 702,
      "decision_run_id": "short-run-145",
      "execution_plan_id": "core-short_term_decider-20260430-KIS_12345678_01",
      "broker_order_id": "order-1",
      "last_updated_at": "2026-04-30T00:07:00+00:00",
      "failure_reason": null,
      "trigger_price": 50500,
      "chase_limit_price": 51500,
      "expected_holding_period": "1-2 trading days",
      "entry_execution_mode": "pending_trigger_limit"
    }
  ]
}
```
