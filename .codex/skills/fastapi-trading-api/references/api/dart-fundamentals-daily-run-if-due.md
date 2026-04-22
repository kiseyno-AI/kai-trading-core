# POST /disclosures/dart/fundamentals/daily/run-if-due

## Summary
Runs the daily incremental DART fundamentals refresh for the requested market date, and can continue retryable capture events for multiple in-run cycles. When `retry_cycles` is omitted, the service uses the configured daily automatic retry count. A successful run also rebuilds persisted daily Piotroski filter rows for the same market date and returns that follow-up summary.

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
  "piotroski_filter": {
    "market_date": "2026-04-16",
    "source": "strategy_piotroski_v1",
    "symbols_evaluated": 3512,
    "symbols_passed": 84,
    "symbols_not_passed": 3428,
    "results_persisted": 3512,
    "results_failed": 0,
    "failed_symbols": []
  },
  "piotroski_filter_error": null,
  "partial_reasons": ["remaining_retryable_events"]
}
```
