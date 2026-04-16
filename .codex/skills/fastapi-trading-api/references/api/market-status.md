# GET /market/status

## Summary
Returns the current market/session state used by web and operators.

## Example request
```http
GET /market/status
```

## Example response
```json
{
  "market": "KRX",
  "state": "OPEN",
  "is_trading_day": true,
  "is_open": true,
  "server_time": "2026-04-16T09:31:12+09:00",
  "session_open_at": "2026-04-16T09:00:00+09:00",
  "session_close_at": "2026-04-16T15:30:00+09:00",
  "source": "app"
}
```
