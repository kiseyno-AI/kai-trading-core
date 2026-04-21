# POST /eod/signal-features/collect and GET /eod/signal-features

## Summary
Collects or reads KIS ranking-derived daily signal features. The collect path also refreshes the persisted market-wide participation context used by market summarizer payloads.

## Example collect request
```http
POST /eod/signal-features/collect?market_date=2026-04-15&sources=volume-power,overtime-volume,overtime-fluctuation
```

## Example collect response
```json
{
  "market_date": "2026-04-15",
  "source": "kis_ranking",
  "features_persisted": 2,
  "features_failed": 0,
  "participation_context": {
    "market": "KRX",
    "market_date": "2026-04-15",
    "source": "kis_ranking",
    "captured_symbol_count": 2
  }
}
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
