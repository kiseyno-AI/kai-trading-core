# POST /eod/pykrx/backfill

## Summary
Runs historical pykrx daily-bar backfill over a date window.

## Example request
```http
POST /eod/pykrx/backfill?start_date=20150101&end_date=20260408&markets=KOSPI,KOSDAQ
```

## Example response
```json
{
  "status": "ok",
  "start_date": "2015-01-01",
  "end_date": "2026-04-08",
  "markets": ["KOSPI", "KOSDAQ"]
}
```
