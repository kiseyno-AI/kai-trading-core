# POST /us-equities/prices/collect-and-screen

## Summary

Collect adjusted KIS overseas daily prices for the bounded US universe, calculate liquidity/history filters, and persist the `price_screen` stage. This route does not submit orders.

## Example request

```http
POST /us-equities/prices/collect-and-screen
Content-Type: application/json

{"market_date":"2026-08-19","persist":true}
```

## Example response

```json
{
  "status": "completed",
  "market_date": "2026-08-19",
  "run_id": 41,
  "persist": true,
  "counts": {
    "universe": 4944,
    "ranking_prefilter": 600,
    "histories_collected": 600,
    "price_eligible_before_cap": 340,
    "price_selected": 300,
    "errors": 0
  },
  "exclusion_reason_counts": {},
  "selected_symbols": ["AAPL"],
  "error_preview": {}
}
```
