# POST /us-equities/fundamentals/collect-and-screen

## Summary

Collect SEC EDGAR submissions and company facts for price-screen candidates, apply fundamental/data-quality filters, and persist the `fundamental_screen` stage. A compliant `SEC_EDGAR_USER_AGENT` is required.

## Example request

```http
POST /us-equities/fundamentals/collect-and-screen
Content-Type: application/json

{"market_date":"2026-08-19","persist":true}
```

## Example response

```json
{
  "status": "completed",
  "market_date": "2026-08-19",
  "persist": true,
  "counts": {
    "price_candidates": 300,
    "sec_snapshots_collected": 150,
    "fundamental_eligible_before_cap": 95,
    "agent_candidates": 80,
    "errors": 0
  },
  "exclusion_reason_counts": {},
  "selected_symbols": ["AAPL"],
  "error_preview": {}
}
```
