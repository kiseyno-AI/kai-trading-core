# GET /agent-context/feedback/progress

## Summary
Reports aggregate progress for the three-role symbol feedback workflow.

The response is now a stable coverage and liveness snapshot:
- `total_symbols` uses the current active symbol universe (or the requested symbol subset), not partial `daily_price_bars` coverage for the day
- `run_status` indicates whether feedback appears to be actively moving, stalled, pending, or completed
- `latest_feedback_at` and `seconds_since_last_feedback` help operators tell whether a run is still alive

## Example request
```http
GET /agent-context/feedback/progress?market_date=2026-04-16
```

## Example response
```json
{
  "request": {
    "market_date": "2026-04-16",
    "symbols": null,
    "expected_agents": [
      "eod-summarizer",
      "technical-summarizer",
      "fundamental-summarizer"
    ]
  },
  "summary": {
    "run_status": "stalled",
    "is_live": false,
    "latest_feedback_at": "2026-04-17T07:16:28.988041+09:00",
    "seconds_since_last_feedback": 5231,
    "stalled_threshold_seconds": 300,
    "total_symbols": 4331,
    "processed_symbols": 397,
    "completed_symbols": 0,
    "in_progress_symbols": 397,
    "pending_symbols": 3934,
    "progress_ratio": 0.091665,
    "progress_percent": 9.17,
    "failed_items": 0,
    "completed_feedback_count": 402,
    "expected_feedback_count": 12993
  }
}
```
