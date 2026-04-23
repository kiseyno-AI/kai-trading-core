# POST /eod/momentum-factors/compute and GET /eod/momentum-factors

## Summary
Computes or reads persisted price-based momentum factor rows built from stored `daily_price_bars`.

The v1 slice stores 12-1 momentum using exact stored trading-bar offsets (`close[t-21] / close[t-252] - 1`), 60-day and 120-day moving averages, trend-confirmation flags, a configurable 5-day overheat exclusion rule, and cross-sectional rank or percentile across the requested symbol universe.

## Example compute request
```http
POST /eod/momentum-factors/compute?market_date=2026-04-23&symbols=005930,000660
```

## Example read request
```http
GET /eod/momentum-factors?market_date=2026-04-23&symbols=005930,000660
```

## Example read response
```json
{
  "market_date": "2026-04-23",
  "feature_version": "price_momentum_v1",
  "summary": {
    "entries": 2,
    "requested_symbols": ["005930", "000660"],
    "matched_symbols": ["005930", "000660"],
    "missing_symbols": [],
    "ranked_symbols": 2,
    "eligible_symbols": 1
  },
  "items": [
    {
      "symbol": "005930",
      "market_date": "2026-04-23",
      "source": "daily_price_bars",
      "close_price": 81200,
      "ma_60": 77650.3,
      "ma_120": 74220.8,
      "momentum_12_1": 0.284,
      "return_5d": 0.031,
      "is_price_above_ma120": true,
      "is_ma60_above_ma120": true,
      "is_recent_overheated": false,
      "momentum_rank": 142,
      "momentum_percentile": 0.96,
      "is_momentum_eligible": true
    }
  ]
}
```
