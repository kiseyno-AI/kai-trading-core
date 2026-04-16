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
    "eod_status": "running",
    "symbols_total": 3579,
    "symbols_succeeded": 3314,
    "symbols_failed": 0
  },
  "latest": {
    "price_trade_date": "2026-04-15",
    "technical_market_date": "2026-04-15",
    "action_market_date": "2026-04-15"
  }
}
```
