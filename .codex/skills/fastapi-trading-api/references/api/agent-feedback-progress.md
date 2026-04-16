# GET /agent-context/feedback/progress

## Summary
Reports aggregate progress for the three-role symbol feedback workflow.

## Example request
```http
GET /agent-context/feedback/progress?market_date=2026-04-15
```

## Example response
```json
{
  "request": {
    "market_date": "2026-04-15",
    "symbols": null,
    "expected_agents": [
      "eod-summarizer",
      "technical-summarizer",
      "fundamental-summarizer"
    ]
  },
  "summary": {
    "total_symbols": 3579,
    "processed_symbols": 3314,
    "completed_symbols": 3314,
    "in_progress_symbols": 0,
    "pending_symbols": 265,
    "progress_ratio": 0.926,
    "progress_percent": 92.6,
    "failed_items": 0,
    "completed_feedback_count": 9942,
    "expected_feedback_count": 10737
  }
}
```
