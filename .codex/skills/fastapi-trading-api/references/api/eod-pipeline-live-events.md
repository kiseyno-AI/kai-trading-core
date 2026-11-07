# GET /eod/pipeline/live/events

## Summary
Incremental daily-pipeline live events held in the Core API process memory (last 1,000). Page with `since=<next_since>` from the previous response: `next_since` is the last event on that page, so a truncated page (`truncated: true`) loses nothing. `last_seq` is the newest event overall. `gap: true` means events between `since` and the oldest kept event fell out of memory. Events from a separate operator CLI process are not in API memory; read them from `apps/api/var/log/daily-pipeline-live.jsonl` or use `GET /eod/pipeline/live` (snapshot file fallback).

Event kinds: `run`, `stage`, `stage_progress`, `substep`, `collection`. Progress events are logged at milestones (status or step change, ~5% steps) so the stream stays small while memory always holds the latest counts.

## Example request
```http
GET /eod/pipeline/live/events?since=10&limit=200
Authorization: Bearer <token>
```

## Example response
```json
{
  "schema_version": "daily_pipeline_live_v1",
  "last_seq": 12,
  "next_since": 12,
  "truncated": false,
  "gap": false,
  "events": [
    {"seq": 11, "at": "2026-11-07T20:47:01.532162+00:00", "kind": "stage", "run_id": 634, "market_date": "2026-11-06", "stage_id": "action_summaries", "status": "completed", "processed": 2769, "total": 2769, "succeeded": 2769, "failed": 0, "current_step": "action_summaries_completed"},
    {"seq": 12, "at": "2026-11-07T20:47:01.539781+00:00", "kind": "run", "run_id": 634, "market_date": "2026-11-06", "trigger": "internal_ops_cli", "status": "completed", "current_stage": "action_summaries"}
  ]
}
```
