# GET /broker/kis/health

## Summary
Broker connectivity and configuration health for the KIS adapter, including realtime domestic order-notice and monitored-price websocket status.

## Example request
```http
GET /broker/kis/health
```

## Example response
```json
{
  "broker": "kis",
  "base_url": "https://openapivts.koreainvestment.com:29443",
  "websocket_url": "ws://ops.koreainvestment.com:31000",
  "environment": "demo",
  "use_mock": true,
  "app_key_configured": true,
  "app_secret_configured": true,
  "hts_id_configured": true,
  "realtime_order_notice_enabled": true,
  "realtime_price_enabled": true,
  "market_snapshot_runtime": {
    "ready": true,
    "effective_kis_environment": "real",
    "blocking_reasons": []
  },
  "portfolio_summary_runtime": {
    "ready": true,
    "app_env": "production",
    "kis_environment": "real",
    "kis_use_mock": false,
    "portfolio_account_references_configured": true,
    "portfolio_account_references_valid": true,
    "configured_account_count": 2,
    "blocking_reasons": [],
    "cache": {
      "has_snapshot": true,
      "is_stale": false,
      "last_refresh_status": "success",
      "refresh_interval_seconds": 60
    }
  },
  "intraday_snapshot_runtime": {
    "enabled": true,
    "ready": true,
    "credentials_configured": true,
    "kis_use_mock": true,
    "market_code": "J",
    "max_ranking_pages": 2,
    "news_limit": 5,
    "blocking_reasons": []
  },
  "realtime_order_notices": {
    "enabled": true,
    "configured": true,
    "status": "subscribed",
    "tr_id": "H0STCNI9",
    "last_message_received_at": "2026-04-28T00:01:00+00:00",
    "last_message_age_seconds": 2.314,
    "approval": {
      "status": "succeeded",
      "updated_at": "2026-04-28T00:00:55+00:00",
      "message": "approval_key_issued"
    },
    "subscription": {
      "status": "succeeded",
      "updated_at": "2026-04-28T00:00:56+00:00",
      "message": "SUBSCRIBE SUCCESS",
      "aes_key_active": true,
      "aes_key_received_at": "2026-04-28T00:00:56+00:00"
    },
    "reconnect_attempts": 0,
    "consecutive_failures": 0,
    "last_error": null,
    "metrics": {
      "messages_received": 7,
      "system_messages": 2,
      "notices_received": 5,
      "notices_ingested": 4,
      "duplicate_notices": 1,
      "unmatched_notices": 0,
      "parse_errors": 0
    }
  },
  "realtime_prices": {
    "enabled": true,
    "configured": true,
    "status": "subscribed",
    "websocket_connected": true,
    "market_state": "OPEN",
    "market_session_active": true,
    "market_calendar_source": "kis_chk_holiday",
    "market_calendar_source_status": "ok",
    "market_calendar_source_error": null,
    "market_session_open_at": "2026-04-30T09:00:00+09:00",
    "market_session_close_at": "2026-04-30T15:30:00+09:00",
    "tr_id": "H0STCNT0",
    "max_subscriptions": 40,
    "desired_symbols": ["005930"],
    "subscribed_symbols": ["005930"],
    "limited_symbols": [],
    "last_price_received_at": "2026-04-30T00:15:31+00:00",
    "metrics": {
      "messages_received": 12,
      "price_messages_received": 10,
      "prices_ingested": 10,
      "subscribe_requests": 1,
      "unsubscribe_requests": 0
    }
  }
}
```

## Notes
- `realtime_prices` is process-local live dashboard state. It does not imply realtime ticks are persisted to the database.
- `realtime_prices.enabled` reports configuration, not an active socket. `market_session_active=true` requires an authoritative KIS `chk-holiday` OPEN day and both server/local clocks inside the regular KRX session. `websocket_connected` is the direct socket signal.
- Outside a verified OPEN session, realtime prices stay disconnected without approval or reconnect churn. Expected idle statuses are `idle_market_preopen`, `idle_market_closed`, `idle_market_weekend`, `idle_market_holiday`, and `idle_market_unverified`. The replaceable WAIT-recheck partition is empty while base holding/Exec membership remains available for the next verified open.
- `market_snapshot_runtime` and `portfolio_summary_runtime` are route-specific readiness summaries for deploy and operator checks.
- `intraday_snapshot_runtime` is the readiness summary for the separate KIS intraday execution-context capture path. `ready=false` means Core will safely return `status=disabled` from the collect route instead of attempting live KIS calls.
- `portfolio_summary_runtime.ready=false` means `/portfolio/summary` is still blocked by config such as missing `PORTFOLIO_ACCOUNT_REFERENCES`, missing KIS secrets, or `KIS_USE_MOCK=true` in `APP_ENV=production`.
- `portfolio_summary_runtime.cache` exposes whether Core currently has a cached portfolio snapshot, whether it is stale, and the last refresh status/error without forcing a live broker read.
