# POST /agent-context/feedback/dispatch

## Summary
Dispatches market-level agent feedback for a completed or partially completed EOD run.

## Example request
```http
POST /agent-context/feedback/dispatch
Content-Type: application/json

{
  "eod_run_id": 158,
  "roles": ["eod-summarizer", "technical-summarizer", "fundamental-summarizer"]
}
```

## Example response
```json
{
  "request": {"eod_run_id": 158, "trade_date": "2026-04-15", "request_run_id": "eod-run-158-ab12cd34ef"},
  "summary": {"status": "completed", "completed": 3, "failed": 0},
  "items": []
}
```
