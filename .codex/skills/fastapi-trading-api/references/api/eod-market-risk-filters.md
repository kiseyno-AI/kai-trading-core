# POST /eod/market-risk-filters/compute and GET /eod/market-risk-filters

## Summary
Computes or reads the persisted KIS-backed market-risk status filter for Korean equities.

The compute path uses KIS `inquire-price-2` per symbol, normalizes deterministic `passed_filter`, `watchlist_flagged`, and `risk_state` fields, and stores one row per symbol/date in `daily_market_risk_filter_results`.

## Example compute request
```http
POST /eod/market-risk-filters/compute?market_date=2026-04-15&symbols=005930,000660&market_code=J
```

## Example compute response
```json
{
  "market_date": "2026-04-15",
  "source": "kis_market_risk_v1",
  "results_persisted": 2,
  "results_failed": 0,
  "summary": {
    "matched_symbol_count": 2,
    "evaluated_count": 2,
    "passed_count": 1,
    "watchlist_count": 1,
    "failed_count": 0
  },
  "items": [
    {
      "symbol": "005930",
      "risk_state": "watchlist",
      "passed_filter": true,
      "watchlist_flagged": true,
      "market_warning_code": "01"
    }
  ]
}
```

## Example read requests
```http
GET /eod/market-risk-filters?market_date=2026-04-15&symbol=005930
GET /eod/market-risk-filters
```

## Example read response
```json
{
  "requested_market_date": "2026-04-15",
  "resolved_market_date": "2026-04-15",
  "market_date": "2026-04-15",
  "feature_version": "kis_market_risk_v1",
  "summary": {
    "entries": 1,
    "requested_symbol": "005930",
    "matched_symbols": ["005930"],
    "missing_symbols": [],
    "passed_count": 1,
    "watchlist_count": 1,
    "failed_count": 0
  },
  "item": {
    "symbol": "005930",
    "risk_state": "watchlist",
    "passed_filter": true,
    "watchlist_flagged": true,
    "filter_payload": {
      "blocking_reasons": [],
      "watchlist_reasons": ["market_warning_01", "investment_caution"]
    }
  }
}
```
