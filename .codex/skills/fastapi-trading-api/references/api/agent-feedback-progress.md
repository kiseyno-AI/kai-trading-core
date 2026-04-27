# GET /agent-context/feedback/progress

## Summary
Reports aggregate progress for the default four-role symbol feedback workflow.

The response is now a stable coverage and liveness snapshot:
- `total_symbols` uses the active symbol set with `daily_price_bars` for the requested market date, optionally narrowed to requested symbols
- `result.market_date` echoes the requested market date, and `result.roles[role]` reports durable persisted `requested_count`, `completed_count`, and `error_count`
- `run_id` is optional and lets operators inspect the exact durable run returned by dispatch or retry endpoints
- when a matching retry-cycle `agent_context_feedback_runs` row exists, aggregate counts stay grounded in persisted feedback and failure rows while live lifecycle fields such as `current_role`, `completed_roles`, and `cycle_status` still come from the active run metadata
- `run_status` indicates whether feedback appears to be actively moving, stalled, pending, or completed
- `latest_feedback_at` and `seconds_since_last_feedback` help operators tell whether a run is still alive
- `first_feedback_at`, `elapsed_seconds`, rates, and ETA fields are calculated only from feedback rows for the same target symbol universe used by the progress counts
- `estimated_completion_at` is an ETA for active runs and the latest feedback timestamp for already completed runs

## Example request
```http
GET /agent-context/feedback/progress?market_date=2026-04-16&run_id=feedback-cycle-2026-04-16-ab12cd34ef
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
    ],
    "run_id": "feedback-cycle-2026-04-16-ab12cd34ef"
  },
  "result": {
    "market_date": "2026-04-16",
    "roles": {
      "eod-summarizer": {
        "requested_count": 4331,
        "completed_count": 134,
        "error_count": 0
      },
      "technical-summarizer": {
        "requested_count": 4331,
        "completed_count": 134,
        "error_count": 2
      },
      "fundamental-summarizer": {
        "requested_count": 4331,
        "completed_count": 134,
        "error_count": 1
      }
    }
  },
  "summary": {
    "run_status": "stalled",
    "is_live": false,
    "first_feedback_at": "2026-04-17T06:58:10.141902+09:00",
    "latest_feedback_at": "2026-04-17T07:16:28.988041+09:00",
    "elapsed_seconds": 6330,
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
    "expected_feedback_count": 12993,
    "completed_feedback_per_minute": 3.81,
    "completed_symbols_per_minute": 0.0,
    "estimated_remaining_seconds": 198267,
    "estimated_completion_at": "2026-04-19T15:59:15.210342+09:00",
    "eta_confidence": "medium"
  }
}
```
