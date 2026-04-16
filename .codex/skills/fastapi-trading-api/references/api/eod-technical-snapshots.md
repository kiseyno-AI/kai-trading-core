# POST /eod/technical-snapshots/compute and GET /eod/technical-snapshots

## Summary
Computes or reads deterministic technical snapshots for symbol/day slices.

## Example compute request
```http
POST /eod/technical-snapshots/compute?market_date=2026-04-15&symbols=005930,000660
```

## Example read request
```http
GET /eod/technical-snapshots?market_date=2026-04-15&symbols=005930,000660
```

## Example read response
```json
{
  "items": [{"symbol": "005930", "market_date": "2026-04-15", "trend_state": "uptrend", "ma_alignment": "bullish"}]
}
```
