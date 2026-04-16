# POST /eod/capture-if-due

## Summary
Starts EOD capture when the due-check says a run should happen.

## Example request
```http
POST /eod/capture-if-due
```

## Example response
```json
{
  "status": "ok",
  "started": true,
  "run_id": 159
}
```
