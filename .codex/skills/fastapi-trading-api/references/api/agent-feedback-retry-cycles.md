# POST /agent-context/feedback/retry-cycles

## Summary
Runs bounded retry passes over incomplete symbol-level agent feedback.

The response includes cycle and batch details plus a fresh `progress` snapshot using the same response shape as `GET /agent-context/feedback/progress`. Consumers can read `progress.summary.first_feedback_at`, `elapsed_seconds`, rate fields, and ETA fields immediately after the retry request completes.

## Example request
```http
POST /agent-context/feedback/retry-cycles
Content-Type: application/json

{
  "market_date": "2026-04-16",
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
  "roles": ["eod-summarizer", "technical-summarizer", "fundamental-summarizer"],
  "max_cycles": 3,
  "outer_batch_size": 20,
  "reuse_existing": true,
  "cycles": [
    {
      "cycle": 1,
      "status": "partial",
      "start_pending": 120,
      "end_pending": 18,
      "batches": []
    }
  ],
  "final_pending": 18,
  "completed": false,
  "progress": {
    "request": {
      "market_date": "2026-04-16",
      "symbols": null,
      "expected_agents": ["eod-summarizer", "technical-summarizer", "fundamental-summarizer"]
    },
    "summary": {
      "run_status": "running",
      "is_live": true,
      "first_feedback_at": "2026-04-16T01:00:00+00:00",
      "latest_feedback_at": "2026-04-16T01:05:00+00:00",
      "elapsed_seconds": 300,
      "total_symbols": 4331,
      "processed_symbols": 4313,
      "completed_symbols": 4200,
      "in_progress_symbols": 113,
      "pending_symbols": 18,
      "progress_percent": 99.58,
      "completed_feedback_count": 12713,
      "expected_feedback_count": 12993,
      "completed_feedback_per_minute": 2542.6,
      "completed_symbols_per_minute": 840.0,
      "estimated_remaining_seconds": 7,
      "estimated_completion_at": "2026-04-16T01:05:07+00:00",
      "eta_confidence": "high"
    }
  }
}
```
