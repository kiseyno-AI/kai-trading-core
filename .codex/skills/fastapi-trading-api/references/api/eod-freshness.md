# GET /eod/freshness

## Summary
Compact web-facing freshness read API for EOD ingestion and downstream dashboards.

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
      "status": "running",
      "symbols_total": 3579,
      "symbols_succeeded": 3314,
      "symbols_failed": 0
    },
    "technical_analysis": {
      "status": "running",
      "symbols_total": 3579,
      "symbols_succeeded": 0,
      "symbols_failed": 0
    },
    "action_summary": {
      "status": "running",
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
    "price_trade_date": "2026-04-15",
    "technical_market_date": "2026-04-15",
    "action_market_date": "2026-04-15"
  }
}
```
