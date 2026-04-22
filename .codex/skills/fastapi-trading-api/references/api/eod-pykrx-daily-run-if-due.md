# POST /eod/pykrx/daily/run-if-due

## Summary
Runs the daily pykrx EOD pipeline only when due, including persisted price-context refresh after bar writes and the chained DART daily fundamentals refresh that follows successful daily-auto completion.

## Example request
```http
POST /eod/pykrx/daily/run-if-due?market_date=2026-04-15
```

## Example response
```json
{
  "triggered": true,
  "status": "completed",
  "latest_available_trade_date": "2026-04-15",
  "price_contexts": {
    "market_date": "2026-04-15",
    "contexts_persisted": 912,
    "contexts_failed": 0
  },
  "fundamentals_followup": {
    "status": "completed",
    "market_date": "2026-04-15",
    "trigger": "eod_pykrx_followup"
  }
}
```
