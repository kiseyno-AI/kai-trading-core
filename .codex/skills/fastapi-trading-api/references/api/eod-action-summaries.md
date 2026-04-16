# POST /eod/action-summaries/compute and GET /eod/action-summaries

## Summary
Computes or reads deterministic action-summary rows for symbol/day slices.

## Example compute request
```http
POST /eod/action-summaries/compute?market_date=2026-04-15&symbols=005930,000660
```

## Example read request
```http
GET /eod/action-summaries?market_date=2026-04-15&symbols=005930,000660
```

## Example read response
```json
{
  "items": [{"symbol": "005930", "market_date": "2026-04-15", "action_label": "hold", "signal_score": 67}]
}
```
