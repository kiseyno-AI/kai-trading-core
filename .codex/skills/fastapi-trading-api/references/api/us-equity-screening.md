# GET /us-equities/screening/{market_date}

## Summary

Read persisted US price/fundamental screening stages for one market date. Results include rank, score, eligibility, metrics, and the explicit filtering decision.

## Example request

```http
GET /us-equities/screening/2026-08-19?eligible_for_agent=true&limit=150
```

## Example response

```json
{
  "market_date": "2026-08-19",
  "count": 1,
  "items": [{
    "symbol": "AAPL",
    "market": "NASDAQ",
    "stage": "fundamental_screen",
    "eligible_for_agent": true,
    "rank": 1,
    "composite_score": "0.912000",
    "metrics_payload": {},
    "decision_payload": {}
  }]
}
```
