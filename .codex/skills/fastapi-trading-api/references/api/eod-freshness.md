# GET /eod/freshness

## Summary
Compact web-facing freshness read API for EOD ingestion and downstream dashboards.

The `today` block is scoped to the current Seoul trade date and is derived from persisted same-day data. Price, technical-analysis, and action-summary counts use the active symbol universe; fundamentals counts use DART fundamentals snapshots and failed financial-report capture attempts updated during the Seoul day. The `latest` block keeps the latest stored price date, preserves the legacy `price_trade_date` and `symbols_*` fields, and now also exposes top-level `trade_date`, `updated_at`, `succeeded`, `failed`, and `total` values for the web.

`latest.summary.basis` names the count basis. `latest_eod_run_symbol_universe` means the summary comes from the selected latest `eod_collection_runs` row for that trade date. `stored_price_bars_for_latest_trade_date` is the fallback when stored bars exist but no matching EOD run row is available.

## Example request
```http
GET /eod/freshness
```

## Example response
```json
{
  "today": {
    "trade_date": "2026-04-16",
    "price": {
      "status": "ready",
      "symbols_total": 3579,
      "symbols_succeeded": 3314,
      "symbols_failed": 265
    },
    "technical_analysis": {
      "status": "missing",
      "symbols_total": 3579,
      "symbols_succeeded": 0,
      "symbols_failed": 0
    },
    "action_summary": {
      "status": "missing",
      "symbols_total": 3579,
      "symbols_succeeded": 0,
      "symbols_failed": 0
    },
    "fundamentals": {
      "status": "ready",
      "symbols_total": 42,
      "symbols_succeeded": 42,
      "symbols_failed": 1
    }
  },
  "latest": {
    "trade_date": "2026-04-16",
    "price_trade_date": "2026-04-16",
    "eod_status": "running",
    "updated_at": "2026-04-16T15:05:12+00:00",
    "succeeded": 3314,
    "failed": 0,
    "total": 3579,
    "summary": {
      "basis": "latest_eod_run_symbol_universe",
      "updated_at": "2026-04-16T15:05:12+00:00",
      "succeeded": 3314,
      "failed": 0,
      "total": 3579
    },
    "symbols_total": 3579,
    "symbols_succeeded": 3314,
    "symbols_failed": 0,
    "technical_market_date": "2026-04-15",
    "action_market_date": "2026-04-15"
  }
}
```
