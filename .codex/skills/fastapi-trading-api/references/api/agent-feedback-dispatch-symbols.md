# POST /agent-context/feedback/dispatch/symbols

## Summary
Dispatches symbol-level summarizer feedback collection with optional freshness-aware reuse.

Core dispatches only the subset of requested symbols whose persisted `daily_piotroski_filter_results` row has `passed_filter=true` for the requested `market_date`. Requested symbols outside that universe are returned in `request.skipped_symbols` and are not dispatched.

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
    "symbols": ["005930"],
    "requested_symbols": ["005930", "000660"],
    "request_run_id": "symbol-run-2026-04-15-ab12cd34ef",
    "roles": ["eod-summarizer", "technical-summarizer", "fundamental-summarizer"],
    "scope": "piotroski_passed_symbols_only",
    "skipped_symbols": ["000660"],
    "symbol_chunk_size": 5,
    "parallel_subtasks": 6,
    "reuse_existing": true
  },
  "summary": {
    "status": "completed",
    "completed": 3,
    "failed": 0,
    "skipped": 1
  },
  "items": []
}
```
