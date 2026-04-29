# POST /eod/momentum-factors/compute and GET /eod/momentum-factors

## Summary
Computes or reads persisted price-based momentum factor rows built from stored `daily_price_bars`.

The v1 slice stores 12-1 momentum using exact stored trading-bar offsets (`close[t-21] / close[t-252] - 1`), 60-day and 120-day moving averages, an explicit top-percentile eligibility gate (default top 20% / `momentum_percentile >= 0.80`), and a `momentum_overheat_v2` exclusion rule that combines cross-sectional 5-day return percentile, a volatility-adjusted 5-day shock metric, and a minimum absolute 5-day return gate. Compute and read responses expose the percentile cutoff, its pass/fail result, the primary eligibility status, and the overheat audit fields directly, while preserving the full rule metadata inside `factor_payload`.

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
  "eligibility_rule_version": "momentum_top_percentile_v1",
  "momentum_percentile_threshold": 0.8,
  "summary": {
    "entries": 2,
    "requested_symbols": ["005930", "000660"],
    "matched_symbols": ["005930", "000660"],
    "missing_symbols": [],
    "ranked_symbols": 2,
    "top_percentile_symbols": 1,
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
      "return_5d": 0.134337,
      "return_5d_rank": 1,
      "return_5d_percentile": 1.0,
      "return_5d_realized_volatility_20d": 0.019851,
      "return_5d_vol_adjusted": 3.026114,
      "is_return_5d_extreme": true,
      "is_volatility_adjusted_overheated": true,
      "is_price_above_ma120": true,
      "is_ma60_above_ma120": true,
      "is_recent_overheated": true,
      "overheat_rule_version": "momentum_overheat_v2",
      "momentum_rank": 142,
      "momentum_percentile": 0.96,
      "momentum_percentile_threshold": 0.8,
      "passes_momentum_top_percentile": true,
      "momentum_eligibility_status": "recently_overheated",
      "is_momentum_eligible": false,
      "factor_payload": {
        "eligibility_rule": {
          "rule_version": "momentum_top_percentile_v1"
        },
        "ranking": {
          "passes_momentum_top_percentile": true
        },
        "eligibility": {
          "momentum_eligibility_status": "recently_overheated",
          "failed_requirements": ["not_recently_overheated"]
        },
        "overheat_rule": {
          "rule_version": "momentum_overheat_v2"
        },
        "overheat": {
          "has_sufficient_cross_sectional_context": true,
          "passes_minimum_absolute_return": true
        }
      }
    }
  ]
}
```
