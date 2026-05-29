# GET /execution/runtime/daily-board

## Summary
Read the same-day execution board from core. The route is dashboard-safe and cache-first: it returns the best local read model immediately from persisted Core planning/readback state and a short in-process snapshot cache. `refresh=true` schedules a bounded, debounced background exec refresh instead of blocking the HTTP request. Consumers should use `freshness` and `refresh` to label stale, partial, or refreshing data honestly.

Each row includes compact `today_activity` facts for the requested `market_date`. Use these side-aware fields for Today bought/sold sections instead of inferring from lifecycle `intent` or aggregate `status`. Buy/sell quantities are counted only from timestamped compact execution/readback facts for the selected date; side facts without timestamps are marked unavailable rather than guessed. Short-term rows also expose raw tactical action, generic action, handoff intent, and approval/monitoring state at the top level.

Price fields are display-oriented and cache-first: realtime websocket prices are preferred, but rows can fall back to local current-quote/cache or recent daily close with explicit source, freshness, and realtime subscription metadata.

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
      "raw_action": "watch_for_trigger",
      "tactical_action": "watch_for_trigger",
      "generic_action": "buy",
      "handoff_intent": "intraday_watch_buy",
      "handoff_active": true,
      "approval_state": "approved",
      "status": "filled",
      "current_price": 71500,
      "price_as_of": "2026-04-30T00:15:30+00:00",
      "price_received_at": "2026-04-30T00:15:31+00:00",
      "price_source": "kis_realtime_ws",
      "price_status": "available",
      "display_price_available": true,
      "display_price_fallback_reason": null,
      "realtime_subscribed": true,
      "realtime_subscription_reason": "subscribed",
      "monitoring_state": "watching_target_stop",
      "planned_quantity": 5,
      "filled_quantity": 5,
      "exit_filled_quantity": 0,
      "open_quantity": 5,
      "planned_price": 51500,
      "avg_fill_price": 50600,
      "today_activity": {
        "schema_version": "execution_runtime_today_activity_v1",
        "market_date": "2026-04-30",
        "buy_activity_status": "available",
        "buy_activity_source": "executions",
        "buy_filled_quantity_today": 5,
        "buy_avg_fill_price_today": 50600,
        "buy_last_filled_at": "2026-04-30T00:07:00+00:00",
        "buy_broker_order_ids": [
          "order-1"
        ],
        "buy_broker_order_ids_omitted_count": 0,
        "sell_activity_status": "none",
        "sell_activity_source": null,
        "sell_filled_quantity_today": 0,
        "sell_avg_fill_price_today": null,
        "sell_last_filled_at": null,
        "sell_broker_order_ids": [],
        "sell_broker_order_ids_omitted_count": 0,
        "net_open_quantity": 5
      },
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
