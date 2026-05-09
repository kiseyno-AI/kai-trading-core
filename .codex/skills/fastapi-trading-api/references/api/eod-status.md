# GET /eod/status

## Summary
Returns the current in-process EOD scheduler snapshots plus current market/session metadata used by the EOD routes.

## Example request
```http
GET /eod/status
```

## Example response
```json
{
  "job": {
    "running": true,
    "market": "KRX"
  },
  "master": {
    "last_attempt_at": "2026-04-15T06:00:00+09:00"
  },
  "pykrx_auto": {
    "enabled": true
  },
  "market": {
    "market": "KRX",
    "state": "OPEN",
    "is_trading_day": true,
    "is_open": true,
    "server_time": "2026-04-15T09:31:12+09:00",
    "session_open_at": "2026-04-15T09:00:00+09:00",
    "session_close_at": "2026-04-15T15:30:00+09:00",
    "reason": null,
    "current_calendar_date": "2026-04-15",
    "latest_valid_trading_date": "2026-04-15",
    "market_session": "open"
  },
  "market_session": {
    "requested_date": "2026-04-15",
    "current_calendar_date": "2026-04-15",
    "latest_valid_trading_date": "2026-04-15",
    "status": "open",
    "is_trading_day": true,
    "intraday_data_expected": true,
    "quote_data_expected": true,
    "recommendation_basis": "live_intraday"
  }
}
```
