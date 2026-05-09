# GET /market/status

## Summary
Returns the current KRX market clock state used by web and operators, including the latest valid trading date and explicit weekend or holiday session semantics.

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
  "reason": null,
  "current_calendar_date": "2026-04-16",
  "latest_valid_trading_date": "2026-04-16",
  "market_session": "open"
}
```
