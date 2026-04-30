# GET /broker/kis/health

## Summary
Broker connectivity and configuration health for the KIS adapter, including realtime domestic order-notice websocket status.

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
  }
}
```

## Notes
- `market_snapshot_runtime` and `portfolio_summary_runtime` are route-specific readiness summaries for deploy and operator checks.
- `portfolio_summary_runtime.ready=false` means `/portfolio/summary` is still blocked by config such as missing `PORTFOLIO_ACCOUNT_REFERENCES`, missing KIS secrets, or `KIS_USE_MOCK=true` in `APP_ENV=production`.
- `portfolio_summary_runtime.cache` exposes whether Core currently has a cached portfolio snapshot, whether it is stale, and the last refresh status/error without forcing a live broker read.
