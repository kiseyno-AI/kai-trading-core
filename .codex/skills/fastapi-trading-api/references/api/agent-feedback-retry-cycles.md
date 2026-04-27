# POST /agent-context/feedback/retry-cycles

## Summary
Runs bounded retry passes over incomplete symbol-level agent feedback.

The retry universe is the persisted passed-filter symbol set for the requested `market_date`, derived from `daily_piotroski_filter_results` rows where `passed_filter=true`. The retry path does not fan out to the full stored date universe.

Callers can optionally provide `symbols` to restrict the real daily retry cycle to one explicit symbol or a small explicit symbol list. Core still runs the same summarizer and decider sequence, persists through the same durable tables, and keeps the run inspectable through the normal progress/latest/read surfaces. Requested symbols outside the passed-filter universe are reported as `skipped_symbols`.

The response includes cycle and batch details plus a fresh `progress` snapshot using the same response shape as `GET /agent-context/feedback/progress`. Consumers can read `progress.summary.first_feedback_at`, `elapsed_seconds`, rate fields, and ETA fields immediately after the retry request completes.

## Example request
```http
POST /agent-context/feedback/retry-cycles
Content-Type: application/json

{
  "market_date": "2026-04-16",
  "symbols": ["005930"],
  "roles": ["eod-summarizer", "technical-summarizer", "fundamental-summarizer"],
  "max_cycles": 3,
  "outer_batch_size": 20,
  "reuse_existing": true
}
```

## Example response
```json
{
  "market_date": "2026-04-16",
  "requested_symbols": ["005930"],
  "eligible_symbols": ["005930"],
  "skipped_symbols": [],
  "roles": ["eod-summarizer", "technical-summarizer", "fundamental-summarizer"],
  "max_cycles": 3,
  "outer_batch_size": 20,
  "reuse_existing": true,
  "cycles": [
    {
      "cycle": 1,
      "status": "partial",
      "start_pending": 84,
      "end_pending": 12,
      "batches": []
    }
  ],
  "final_pending": 12,
  "completed": false,
  "progress": {
    "request": {
      "market_date": "2026-04-16",
      "symbols": ["005930"],
      "expected_agents": ["eod-summarizer", "technical-summarizer", "fundamental-summarizer"]
    },
    "summary": {
      "run_status": "running",
      "is_live": true,
      "first_feedback_at": "2026-04-16T01:00:00+00:00",
      "latest_feedback_at": "2026-04-16T01:05:00+00:00",
      "elapsed_seconds": 300,
      "total_symbols": 84,
      "processed_symbols": 72,
      "completed_symbols": 70,
      "in_progress_symbols": 2,
      "pending_symbols": 12,
      "progress_percent": 85.71,
      "completed_feedback_count": 210,
      "expected_feedback_count": 252,
      "completed_feedback_per_minute": 42.0,
      "completed_symbols_per_minute": 14.0,
      "estimated_remaining_seconds": 17,
      "estimated_completion_at": "2026-04-16T01:05:17+00:00",
      "eta_confidence": "high"
    }
  }
}
```
