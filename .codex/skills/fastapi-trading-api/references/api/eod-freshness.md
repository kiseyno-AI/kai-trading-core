# GET /eod/freshness

## Summary
Compact web-facing freshness read API for EOD ingestion and downstream dashboards.

The `today` block is scoped to the current Seoul trade date and is derived from persisted same-day data. Price, technical-analysis, and action-summary counts use the active symbol universe; fundamentals counts use DART fundamentals snapshots and failed financial-report capture attempts updated during the Seoul day, with same-day succeeded symbols excluded from the failed bucket. The `latest` block keeps the latest stored price date and its EOD run metadata.

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
      "symbols_total": 43,
      "symbols_succeeded": 42,
      "symbols_failed": 1
    }
  },
  "latest": {
    "price_trade_date": "2026-04-16",
    "eod_status": "running",
    "symbols_total": 3579,
    "symbols_succeeded": 3314,
    "symbols_failed": 0,
    "technical_market_date": "2026-04-15",
    "action_market_date": "2026-04-15"
  }
}
```
