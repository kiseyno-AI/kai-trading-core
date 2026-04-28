# POST /agent-context/feedback/dispatch/symbols

## Summary
Dispatches symbol-level summarizer feedback collection with optional freshness-aware reuse.

Core dispatches only the subset of requested symbols that sit in the intersection of the persisted active pre-filters for the requested `market_date`: `daily_market_risk_filter_results.passed_filter=true` and `daily_piotroski_filter_results.passed_filter=true`. Requested symbols outside that universe are returned in `request.skipped_symbols` and are not dispatched.
Each normal dispatch also creates a durable `agent_context_feedback_runs` row, checkpoints role progress as the run advances, and finalizes that run row even when the cycle exits partially. When `wait_for_completion=false`, Core now creates the durable queued run row before returning and reuses the same `request_run_id` when the background work starts.

## Example request
```http
POST /agent-context/feedback/dispatch/symbols
Content-Type: application/json

{
  "market_date": "2026-04-15",
  "symbols": ["005930", "000660"],
  "roles": ["eod-summarizer", "technical-summarizer", "fundamental-summarizer"],
  "reuse_existing": true,
  "wait_for_completion": false
}
```

## Example response
```json
{
  "request": {
    "market_date": "2026-04-15",
    "symbols": ["005930", "000660"],
    "requested_symbols": ["005930", "000660"],
    "request_run_id": "symbol-run-2026-04-15-ab12cd34ef",
    "roles": ["eod-summarizer", "technical-summarizer", "fundamental-summarizer"],
    "reuse_existing": true,
    "scope": "requested_symbols_intersected_with_active_pre_filters",
    "eligibility_rule": "market_risk_filter_passed_and_piotroski_passed",
    "eligibility_filters": ["market_risk_filter", "piotroski_filter"],
    "wait_for_completion": false
  },
  "summary": {
    "status": "queued",
    "queued": 2
  }
}
```
