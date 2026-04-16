# POST /disclosures/dart/fundamentals/daily/run-if-due

## Summary
Runs the daily incremental DART fundamentals refresh for the requested market date, and can continue retryable capture events for multiple in-run cycles. When `retry_cycles` is omitted, the service uses the configured daily automatic retry count.

## Example request
`POST /disclosures/dart/fundamentals/daily/run-if-due?market_date=2026-04-16&capture_limit=500&force=false&fs_div=CFS`

## Example response
```json
{
  "triggered": true,
  "run_id": 13,
  "status": "partial",
  "market_date": "2026-04-16",
  "fs_div": "CFS",
  "events_detected": 13738,
  "events_captured": 120,
  "events_failed": 0,
  "retry_cycles_requested": 3,
  "retry_cycles_completed": 3,
  "remaining_retryable_events": 620,
  "capture": {
    "run_id": 266,
    "status": "completed",
    "events_seen": 1500,
    "events_captured": 120,
    "events_failed": 0,
    "events_skipped": 1380,
    "retry_cycles_requested": 3,
    "retry_cycles_completed": 3,
    "runs": []
  },
  "partial_reasons": ["remaining_retryable_events"]
}
```
