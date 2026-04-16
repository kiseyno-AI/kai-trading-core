# POST /eod/capture

## Summary
Starts a direct EOD capture for a requested market date.

## Example request
```http
POST /eod/capture?market_date=2026-04-15
```

## Example response
```json
{
  "status": "ok",
  "run_id": 159,
  "market_date": "2026-04-15"
}
```
