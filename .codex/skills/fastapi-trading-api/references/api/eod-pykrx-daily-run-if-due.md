# POST /eod/pykrx/daily/run-if-due

## Summary
Runs the daily pykrx EOD pipeline only when due.

## Example request
```http
POST /eod/pykrx/daily/run-if-due?market_date=2026-04-15
```

## Example response
```json
{
  "status": "ok",
  "started": true,
  "market_date": "2026-04-15"
}
```
