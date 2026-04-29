# GET /eod/screening-results

## Summary
Compact dashboard-facing screening read over persisted factor and filter result tables.

This route exists because the family-specific inspect endpoints are detail-oriented: they hydrate full rows and include heavyweight `factor_payload` or `filter_payload` JSON blocks. That is useful for single-symbol inspection, but it is unnecessarily expensive for interactive list views.

`GET /eod/screening-results` uses the existing canonical result tables and keeps the optimization read-side only:
- resolve the latest available `market_date` when one is not supplied
- aggregate summary counts in SQL
- project only list-needed columns
- page results with a default `limit=100`

Supported `family` values:
- `market_risk_filter`
- `piotroski_filter`
- `momentum`
- `value`
- `low_risk`
- `beta`
- `advanced_value`
- `unified_value`
- `composite`

Use the family-specific routes when the client needs the full payload block or a single-symbol detail shape.

For `family=momentum`, compact rows now surface the explicit 12-1 eligibility gates directly: `momentum_percentile_threshold`, `passes_momentum_top_percentile`, `momentum_eligibility_status`, `is_price_above_ma120`, `is_ma60_above_ma120`, `is_recent_overheated`, and `is_momentum_eligible`.

## Query parameters
- `family` (required): screening family identifier.
- `market_date` (optional): ISO trade date. When omitted, the route uses the latest available persisted date for that family.
- `limit` (optional): page size. Defaults to `100`, max `500`.
- `offset` (optional): zero-based page offset. Defaults to `0`.

## Example request
```http
GET /eod/screening-results?family=composite&market_date=2026-04-24&limit=50&offset=0
```

## Example response
```json
{
  "family": "composite",
  "detail_route": "/eod/composite-factors",
  "read_mode": "screening_compact",
  "requested_market_date": "2026-04-24",
  "resolved_market_date": "2026-04-24",
  "market_date": "2026-04-24",
  "latest_mode": false,
  "is_latest_fallback": false,
  "feature_version": "composite_v1",
  "summary": {
    "entries": 1674,
    "scored_count": 1674,
    "eligible_count": 214,
    "watchlist_count": 381,
    "ineligible_count": 941,
    "hard_excluded_count": 77,
    "insufficient_inputs_count": 61
  },
  "pagination": {
    "limit": 50,
    "offset": 0,
    "returned": 50,
    "total": 1674,
    "has_more": true,
    "next_offset": 50
  },
  "items": [
    {
      "symbol": "005930",
      "name_ko": "삼성전자",
      "market": "KOSPI",
      "market_date": "2026-04-24",
      "source": "composite_v1",
      "composite_rank": 81,
      "composite_score": 0.860134,
      "composite_percentile": 0.951584,
      "composite_classification": "eligible",
      "is_composite_eligible": true,
      "valid_factor_count": 5,
      "available_base_weight": 1.0,
      "captured_at": "2026-04-24T15:31:00+00:00"
    }
  ]
}
```
