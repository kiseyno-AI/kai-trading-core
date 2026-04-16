# POST /agent-context/feedback/dispatch/symbols

## Summary
Dispatches symbol-level summarizer feedback collection with optional freshness-aware reuse.

## Example request
```http
POST /agent-context/feedback/dispatch/symbols
Content-Type: application/json

{
  "market_date": "2026-04-15",
  "symbols": ["005930", "000660"],
  "roles": ["eod-summarizer", "technical-summarizer", "fundamental-summarizer"],
  "reuse_existing": true
}
```

## Example response
```json
{
  "request": {
    "market_date": "2026-04-15",
    "request_run_id": "symbol-run-2026-04-15-ab12cd34ef",
    "roles": ["eod-summarizer", "technical-summarizer", "fundamental-summarizer"],
    "symbol_chunk_size": 5,
    "parallel_subtasks": 6,
    "reuse_existing": true
  },
  "summary": {
    "status": "completed",
    "completed": 6,
    "failed": 0
  },
  "items": []
}
```
