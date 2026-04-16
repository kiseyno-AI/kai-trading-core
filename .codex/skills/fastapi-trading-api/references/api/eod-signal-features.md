# POST /eod/signal-features/collect and GET /eod/signal-features

## Summary
Collects or reads KIS ranking-derived daily signal features.

## Example collect request
```http
POST /eod/signal-features/collect?market_date=2026-04-15&sources=volume-power,overtime-volume,overtime-fluctuation
```

## Example read request
```http
GET /eod/signal-features?market_date=2026-04-15&symbols=005930,000660
```

## Example read response
```json
{
  "items": [{"symbol": "005930", "market_date": "2026-04-15", "source_features": {}}]
}
```
