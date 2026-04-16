# POST /eod/master/refresh-if-due

## Summary
Refreshes the market master only when due, or immediately when forced.

## Example request
```http
POST /eod/master/refresh-if-due?force=false
```

## Example response
```json
{
  "status": "ok",
  "refreshed": false,
  "reason": "not_due"
}
```
