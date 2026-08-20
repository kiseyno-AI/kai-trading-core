# POST /us-equities/prices/collect-and-screen

## Summary

Collect adjusted KIS overseas daily prices for the bounded US universe, calculate liquidity/history filters, and persist the `price_screen` stage. This route does not submit orders.

`USPriceCollectionRequest.prefilter_cap` is an optional integer from 1 through 600. Omitting it preserves the 600-symbol cap; lower values bound expensive history collection for canary and operational runs. Final selection stays capped at 300 symbols.

## Example request

```http
POST /us-equities/prices/collect-and-screen
Content-Type: application/json

{"market_date":"2026-08-19","persist":true,"prefilter_cap":5}
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
    "ranking_prefilter": 5,
    "histories_collected": 5,
    "price_eligible_before_cap": 3,
    "price_selected": 3,
    "errors": 0
  },
  "exclusion_reason_counts": {"insufficient_history": 2},
  "selected_symbols": ["AAPL", "MSFT", "NVDA"],
  "error_preview": {}
}
```
