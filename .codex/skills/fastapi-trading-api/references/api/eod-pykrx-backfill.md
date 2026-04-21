# POST /eod/pykrx/backfill

## Summary
Runs historical pykrx daily-bar backfill over a date window and persists per-symbol daily price context after the bar write stage.

## Example request
```http
POST /eod/pykrx/backfill?start_date=20150101&end_date=20260408&markets=KOSPI,KOSDAQ
```

## Example response
```json
{
  "status": "completed",
  "start_date": "20150101",
  "requested_end_date": "20260408",
  "latest_available_trade_date": "2026-04-08",
  "markets": ["KOSPI", "KOSDAQ"],
  "price_contexts": {
    "market_date": "2026-04-08",
    "contexts_persisted": 912,
    "contexts_failed": 0
  }
}
```
