# GET /eod/pipeline/status

## Summary
Operator-facing live status API for the post-close EOD pipeline.

Canonical semantics for this route and `GET /eod/freshness` live in `docs/EOD_STATUS_SEMANTICS_CONTRACT.md`.

Unlike `GET /eod/freshness`, this route focuses on in-flight work. It reports the current stage status for pykrx daily price collection, technical snapshot compute, action summary compute, stored trending-rank rebuild, and the latest DART fundamentals daily refresh run for the requested `market_date`.

The default `market_date` is the current Seoul calendar date from `server_time[:10]`. On weekends, holidays, or other break-days, callers must pass `market_date` explicitly if they want the previous trading day's pipeline view instead of the current calendar date.

## Example request
```http
GET /eod/pipeline/status?market_date=2026-04-21&fs_div=CFS
```

## Example response
```json
{
  "market_date": "2026-04-21",
  "fs_div": "CFS",
  "status": "running",
  "stage_order": [
    "price_collection",
    "technical_snapshots",
    "action_summaries",
    "trending_ranks",
    "fundamentals_daily_refresh"
  ],
  "stages": {
    "price_collection": {
      "status": "completed",
      "processed": 3579,
      "total": 3579,
      "failed": 0,
      "source_run_ids": {
        "eod_run_id": 184
      }
    },
    "technical_snapshots": {
      "status": "running",
      "processed": 1284,
      "total": 3579,
      "failed": 3,
      "current_step": "computing_symbol_snapshot",
      "current_activity": "Computing daily technical snapshots for 005930",
      "current_symbol": "005930",
      "source_run_ids": {
        "eod_run_id": 184
      }
    },
    "fundamentals_daily_refresh": {
      "status": "running",
      "processed": 120,
      "total": 200,
      "failed": 2,
      "current_step": "capturing_fundamentals",
      "current_activity": "Capturing DART fundamentals for 75 events, retry cycle 2/3, batch 1/2",
      "source_run_ids": {
        "refresh_run_id": 55,
        "sync_run_ids": [880, 881],
        "capture_run_id": 700,
        "capture_run_ids": [699, 700]
      }
    }
  }
}
```
