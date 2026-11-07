# GET /eod/pipeline/live

## Summary
DB-free real-time view of the daily pipeline for dashboards and agents.

Every write the daily pipeline makes to its run state (run start/finish, stage start, per-item progress, heartbeat, finish), the grouped PyKRX collection's per-symbol progress, derived-stage sub-steps (for example `technical_snapshots` → `price_contexts` then `technical_snapshots`) and in-memory per-symbol callbacks (technical snapshots, action summaries, external news collection) are mirrored into `app/eod/live_progress.py`. Nothing here queries the database, so it is safe to poll every second.

Sources, newest wins:
- Core API process memory;
- `apps/api/var/pipeline-live/current.json` (override with `DAILY_PIPELINE_LIVE_DIR`), written atomically at most once per second and on every status change by whichever process runs the pipeline (including `python -m app.internal.ops eod-pipeline-run`). `origin` says which one was returned.

Milestone events (status changes, step changes, ~5% progress steps) are appended to `apps/api/var/log/daily-pipeline-live.jsonl` (override with `DAILY_PIPELINE_LIVE_LOG`, rotated at 20 MB) for `tail -f` / log shippers.

Read-time fields: `percent`, `elapsed_seconds`, `rate_per_second` and `eta_seconds` (running stages with counts), `seconds_since_update`, and `stale_suspected` (running run with no update or heartbeat for 15 minutes, e.g. a killed process). `pending_stage_ids` and `stage_progress` come from the run's resolved stage order. During a grouped `pykrx_universe_refresh → price_collection → pykrx_enrichment` call, `collection_stage` and the mirrored `price_collection` entry (`source: eod_collection_run`) show the part that is actually running.

## Example request
```http
GET /eod/pipeline/live?events=3
Authorization: Bearer <token>
```

## Example response
```json
{
  "schema_version": "daily_pipeline_live_v1",
  "generated_at": "2026-11-07T20:46:50.120000+00:00",
  "updated_at": "2026-11-07T20:46:49.980000+00:00",
  "origin": "snapshot_file",
  "active": true,
  "active_run": {
    "run_id": 634,
    "market_date": "2026-11-06",
    "trigger": "internal_ops_cli",
    "status": "running",
    "current_stage": "technical_snapshots",
    "started_at": "2026-11-08T05:46:37.400211+09:00",
    "stage_order": ["technical_snapshots", "action_summaries"],
    "pending_stage_ids": ["action_summaries"],
    "stage_progress": {"done": 0, "total": 2},
    "elapsed_seconds": 12.7,
    "seconds_since_update": 0.1,
    "stale_suspected": false,
    "stages": [
      {
        "stage_id": "technical_snapshots",
        "status": "running",
        "processed": 1840,
        "total": 2769,
        "succeeded": 1840,
        "failed": 0,
        "current_step": "computing_symbol_snapshot",
        "current_symbol": "214450",
        "current_substep": "technical_snapshots",
        "substeps": {
          "price_contexts": {"status": "completed", "detail": {"scope_symbols": 2769}},
          "technical_snapshots": {"status": "running", "processed": 1840, "total": 2769}
        },
        "elapsed_seconds": 12.6,
        "percent": 66.5,
        "rate_per_second": 146.0,
        "eta_seconds": 6.4,
        "source": "pipeline_stage"
      }
    ]
  },
  "runs": ["…same shape, newest first, up to 6…"],
  "collections": [],
  "last_seq": 7,
  "recent_events": [
    {"seq": 5, "at": "2026-11-07T20:46:41.669814+00:00", "kind": "substep", "run_id": 634, "stage_id": "technical_snapshots", "step": "technical_snapshots", "status": "running"},
    {"seq": 6, "at": "2026-11-07T20:46:45.010000+00:00", "kind": "stage_progress", "run_id": 634, "stage_id": "technical_snapshots", "status": "running", "processed": 1385, "total": 2769},
    {"seq": 7, "at": "2026-11-07T20:46:49.980000+00:00", "kind": "stage_progress", "run_id": 634, "stage_id": "technical_snapshots", "status": "running", "processed": 1840, "total": 2769}
  ]
}
```
