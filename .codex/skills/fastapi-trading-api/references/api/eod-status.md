# GET /eod/status

## Summary
Returns the current in-process EOD scheduler snapshots plus current market/session metadata used by the EOD routes.

## Example request
```http
GET /eod/status
```

## Example response
```json
{
  "job": {
    "running": true,
    "market": "KRX",
    "label": "Legacy/manual EOD capture",
    "scope": "legacy_manual_capture"
  },
  "master": {
    "last_attempt_at": "2026-04-15T06:00:00+09:00"
  },
  "pykrx_auto": {
    "enabled": true,
    "label": "Current daily pipeline",
    "scope": "automatic_daily_pipeline"
  },
  "background_runner": {
    "enabled": true,
    "status": "running",
    "running": true,
    "task_alive": true,
    "interval_seconds": 60,
    "heartbeat_at": "2026-04-15T00:31:01+00:00",
    "last_tick": {
      "count": 42,
      "started_at": "2026-04-15T00:31:00+00:00",
      "completed_at": "2026-04-15T00:31:01+00:00",
      "succeeded_at": "2026-04-15T00:31:01+00:00",
      "error": null
    },
    "staleness": {
      "age_seconds": 11,
      "stale_after_seconds": 180,
      "is_stale": false
    },
    "daily_bar_reconciliation": {
      "market_date": "2026-04-14",
      "checked_at": "2026-04-15T08:01:00+09:00",
      "status": "ok",
      "warnings": [],
      "compared_count": 2769,
      "mismatch_count": 0,
      "mismatch_by_field": {"open": 0, "high": 0, "low": 0, "close": 0, "volume": 0, "turnover": 0},
      "mismatch_symbols": [],
      "official_not_stored_count": 12,
      "attempt": 1,
      "collection_price_source": {"official_market_frame_bars": 2769, "per_symbol_bars": 0}
    },
    "label": "Background orchestration runner",
    "scope": "runtime_background_orchestration"
  },
  "market": {
    "market": "KRX",
    "state": "OPEN",
    "is_trading_day": true,
    "is_open": true,
    "server_time": "2026-04-15T09:31:12+09:00",
    "session_open_at": "2026-04-15T09:00:00+09:00",
    "session_close_at": "2026-04-15T15:30:00+09:00",
    "reason": null,
    "current_calendar_date": "2026-04-15",
    "latest_valid_trading_date": "2026-04-15",
    "market_session": "open"
  },
  "market_session": {
    "requested_date": "2026-04-15",
    "current_calendar_date": "2026-04-15",
    "latest_valid_trading_date": "2026-04-15",
    "status": "open",
    "is_trading_day": true,
    "intraday_data_expected": true,
    "quote_data_expected": true,
    "recommendation_basis": "live_intraday"
  }
}
```

`job` is the legacy/manual capture state and must not be used as the health
signal for the current automatic daily pipeline. Use `pykrx_auto` for the
daily-pipeline scheduler and `background_runner` for process-local orchestration
health. A runner with `status="stale"`, `staleness.is_stale=true`, or
`task_alive=false` while enabled needs operator investigation.

`background_runner.daily_bar_reconciliation` is the next-day comparison of the
latest finalized session's stored bars with the official KRX table (`null` until
the first check in this process). `status` is `ok`, `mismatch` (stored bars differ;
restate them, see `docs/TARGETED_REPAIR_TOOLING.md`) or `reference_unavailable`.
`warnings` may include `collection_used_per_symbol_fallback` (the collection fell
back to provisional per-symbol bars) and `collection_price_source_unrecorded`.
