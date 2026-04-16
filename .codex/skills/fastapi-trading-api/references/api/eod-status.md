# GET /eod/status

## Summary
Returns current EOD run status and recent collection metadata.

## Example request
```http
GET /eod/status
```

## Example response
```json
{
  "latest_run": {"id": 158, "trade_date": "2026-04-15", "status": "running", "symbols_total": 3579, "symbols_succeeded": 3314, "symbols_failed": 0}
}
```
