# GET /execution/runtime/monitored-symbols

## Summary
Read the current symbol-level exec monitoring view from core. The route is dashboard-safe and cache-first: it groups the local daily-board read model by symbol and returns immediately, including an empty item list when no symbols are monitored. `refresh=true` schedules a bounded, debounced background exec refresh instead of making the request wait on exec/KIS readback.

Each item includes explicit exec-runtime ownership fields so dashboard clients do not infer behavior from labels. Use `exec_runtime_showable=true` for the exec-owned card. Rows are showable when an exec-agent approved watch candidate has active monitoring or when an account/exec position is currently held. Core-only, rejected, expired, failed, cancelled, or stale rows carry `exec_runtime_exclusion_reason` when present in the broader monitored read model.

Price fields are display-oriented: realtime websocket prices are preferred, then local current-quote/cache fallback, then recent daily close. Fallback prices are labeled with `price_source`, `price_status`, `display_price_available`, and realtime subscription metadata so capacity-limited symbols do not look like true data gaps.

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
    "idle": 0,
    "desired_symbol_count": 1,
    "realtime_subscribed_count": 0,
    "capacity_limited_count": 1,
    "cached_fallback_covered_count": 1,
    "truly_unavailable_count": 0,
    "display_price_available_count": 1,
    "price_status_counts": {
      "fallback": 1
    },
    "price_source_counts": {
      "cached_quote": 1
    }
  },
  "realtime_subscription": {
    "desired_symbols": [
      "005930"
    ],
    "desired_symbol_count": 1,
    "max_subscriptions": 40
  },
  "items": [
    {
      "symbol": "005930",
      "name": "Samsung Electronics",
      "market": "KOSPI",
      "current_price": 71500,
      "price_as_of": "2026-04-30T00:10:00+00:00",
      "price_received_at": "2026-04-30T00:10:01+00:00",
      "price_source": "cached_quote",
      "price_status": "fallback",
      "display_price_available": true,
      "display_price_fallback_reason": "current_quote_cache",
      "realtime_subscribed": false,
      "realtime_subscription_reason": "capacity_limited",
      "monitoring_state": "watching_target_stop",
      "exec_monitoring_active": true,
      "exec_runtime_showable": true,
      "exec_runtime_state": "held_position",
      "exec_runtime_exclusion_reason": null,
      "exec_runtime_exclusion_reasons": [],
      "exec_agent_approved": false,
      "exec_agent_action": null,
      "exec_agent_reason_code": null,
      "exec_agent_target_profile": null,
      "exec_agent_target_tag": null,
      "exec_agent_special_tactical_scalp": null,
      "held_position": true,
      "position_quantity": 5,
      "position_average_price": 70100,
      "approved_quantity": null,
      "approved_notional": null,
      "monitoring_statuses": [
        "stop_sell_monitoring",
        "target_sell_monitoring"
      ],
      "position_side": "long",
      "planned_intents": [
        "stop_sell",
        "target_sell"
      ],
      "target_price": "73500",
      "stop_loss_price": "69000",
      "trigger_price": null,
      "chase_limit_price": null,
      "expected_holding_period": "3-10 trading days",
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
