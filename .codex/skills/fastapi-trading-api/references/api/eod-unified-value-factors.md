# POST /eod/unified-value-factors/compute and GET /eod/unified-value-factors

## Summary
Computes or reads persisted unified valuation rows built from stored `daily_price_bars` plus the latest stored annual DART fundamentals snapshot on or before the requested `market_date`.

`unified_value_v1` is a separate full-universe scoring slice. It keeps the existing value v1 and advanced-value outputs unchanged while combining their five valuation components into one higher-is-better score:
- lower `per`
- lower `pbr`
- higher `dividend_yield`
- higher `fcf_yield`
- lower `ev_to_ebitda`

The final `unified_value_score` is the equal-weight mean of the five component percentiles and is only stored when all five component percentiles are available. Rows still persist when the score is missing so consumers can inspect component statuses, partial rankings, and audit payloads.

## Compute query parameters
- `market_date` (required): ISO trade date to score.
- `symbols` (optional): comma-separated symbol subset. When omitted, the route uses the full stored symbol universe with a bar on `market_date`. Subset reruns still rank against the full stored market-date universe.
- `source` (optional): persisted feature-version string. Defaults to `unified_value_v1`.

If `symbols` is provided but trims down to an explicit empty subset, the route returns a no-op response with zero persisted rows.

## Example compute request
```http
POST /eod/unified-value-factors/compute?market_date=2026-04-24&symbols=005930,000660&source=unified_value_v1
```

## Example compute response
```json
{
  "market_date": "2026-04-24",
  "source": "unified_value_v1",
  "feature_version": "unified_value_v1",
  "requested_symbols": ["005930", "000660"],
  "matched_symbols": ["005930", "000660"],
  "missing_symbols": [],
  "summary": {
    "requested_symbol_count": 2,
    "matched_symbol_count": 2,
    "missing_symbol_count": 0,
    "symbols_with_market_date_bar": 2,
    "symbols_with_annual_fundamentals": 2,
    "per_valid_count": 2,
    "pbr_valid_count": 2,
    "dividend_yield_valid_count": 2,
    "fcf_yield_valid_count": 2,
    "ev_to_ebitda_valid_count": 2,
    "scored_count": 2,
    "invalid_per_count": 0,
    "missing_metric_count": 0,
    "insufficient_components_count": 0,
    "insufficient_fundamentals_count": 0,
    "missing_market_date_bar_count": 0,
    "ranking_universe_counts": {
      "per": 2417,
      "pbr": 2410,
      "dividend_yield": 2086,
      "fcf_yield": 2144,
      "ev_to_ebitda": 1738,
      "unified_value_score": 1684
    }
  },
  "results_persisted": 2,
  "results_failed": 0,
  "failed_symbols": [],
  "failed_symbol_errors": [],
  "items": [
    {
      "symbol": "005930",
      "name_ko": "삼성전자",
      "market": "KOSPI",
      "market_date": "2026-04-24",
      "source": "unified_value_v1",
      "feature_version": "unified_value_v1",
      "per": 12.418,
      "pbr": 1.142,
      "dividend_yield": 0.024,
      "fcf_yield": 0.070118,
      "ev_to_ebitda": 5.63591,
      "per_status": "available",
      "pbr_status": "available",
      "dividend_yield_status": "available",
      "fcf_yield_status": "available",
      "ev_to_ebitda_status": "available",
      "per_rank": 412,
      "pbr_rank": 295,
      "dividend_yield_rank": 188,
      "fcf_yield_rank": 301,
      "ev_to_ebitda_rank": 248,
      "per_percentile": 0.829339,
      "pbr_percentile": 0.877126,
      "dividend_yield_percentile": 0.910024,
      "fcf_yield_percentile": 0.85972,
      "ev_to_ebitda_percentile": 0.900058,
      "unified_value_score": 0.875253,
      "unified_value_rank": 104,
      "unified_value_percentile": 0.93872,
      "components_used": 5,
      "unified_value_status": "scored",
      "factor_payload": {
        "feature_version": "unified_value_v1",
        "methodology": {
          "score_method": "equal_weight_mean_of_five_component_percentiles",
          "score_requirement": "all_five_components_required"
        }
      }
    }
  ]
}
```

## Read query parameters
- `market_date` (required): ISO date for the persisted factor snapshot.
- `symbol` (optional): six-digit stock code to narrow the read to one symbol.
- `limit` (optional): maximum number of rows to return when reading the full date. Allowed range is `1..20000`.

## Example read requests
```http
GET /eod/unified-value-factors?market_date=2026-04-24&limit=100
GET /eod/unified-value-factors?market_date=2026-04-24&symbol=005930
```

## Example read response (single symbol)
```json
{
  "market_date": "2026-04-24",
  "feature_version": "unified_value_v1",
  "summary": {
    "entries": 1,
    "requested_symbol": "005930",
    "matched_symbols": ["005930"],
    "missing_symbols": [],
    "scored_count": 1,
    "invalid_per_count": 0,
    "missing_metric_count": 0,
    "insufficient_components_count": 0,
    "insufficient_fundamentals_count": 0,
    "missing_market_date_bar_count": 0
  },
  "item": {
    "symbol": "005930",
    "market_date": "2026-04-24",
    "unified_value_score": 0.875253,
    "unified_value_rank": 104,
    "unified_value_percentile": 0.93872,
    "components_used": 5,
    "unified_value_status": "scored",
    "factor_payload": {
      "feature_version": "unified_value_v1",
      "component_statuses": {
        "per": "available",
        "pbr": "available",
        "dividend_yield": "available",
        "fcf_yield": "available",
        "ev_to_ebitda": "available"
      }
    }
  }
}
```
