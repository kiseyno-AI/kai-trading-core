# POST /eod/quality-factors/compute and GET /eod/quality-factors

## Summary
Computes or reads persisted practical quality rows built from the full stored market-date universe plus the latest stored annual DART fundamentals snapshot on or before the requested `market_date`.

`quality_v1` is intentionally the first practical quality model, not the final richer one. It uses only the components that are currently feasible from stored data:
- higher `roe`
- higher `roa`
- higher `gross_profit_to_assets`
- higher `operating_cash_flow_to_net_income`
- lower `debt_ratio`
- lower `accruals_ratio`

The final `quality_score` is the equal-weight mean of the available component percentiles for that symbol. `components_possible` is fixed at `6` for this first-pass model, `components_used` records how many component percentiles were actually used, and `is_quality_score_complete` is only `true` when all six were available.

## Compute query parameters
- `market_date` (required): ISO trade date to score.
- `symbols` (optional): comma-separated symbol subset. When omitted, the route uses the full stored symbol universe with a bar on `market_date`. Subset reruns still rank against the full stored market-date universe.
- `source` (optional): persisted feature-version string. Defaults to `quality_v1`.

If `symbols` is provided but trims down to an explicit empty subset, the route returns a no-op response with zero persisted rows.

## Example compute request
```http
POST /eod/quality-factors/compute?market_date=2026-04-24&symbols=005930,000660&source=quality_v1
```

## Example compute response
```json
{
  "market_date": "2026-04-24",
  "source": "quality_v1",
  "feature_version": "quality_v1",
  "requested_symbols": ["005930", "000660"],
  "matched_symbols": ["005930", "000660"],
  "missing_symbols": [],
  "summary": {
    "requested_symbol_count": 2,
    "matched_symbol_count": 2,
    "missing_symbol_count": 0,
    "symbols_with_market_date_bar": 2,
    "symbols_with_annual_fundamentals": 2,
    "roe_valid_count": 2,
    "roa_valid_count": 2,
    "gross_profit_to_assets_valid_count": 2,
    "operating_cash_flow_to_net_income_valid_count": 1,
    "debt_ratio_valid_count": 2,
    "accruals_ratio_valid_count": 2,
    "scored_complete_count": 1,
    "scored_partial_count": 1,
    "scored_count": 2,
    "invalid_assets_count": 0,
    "invalid_equity_count": 0,
    "invalid_net_income_count": 0,
    "missing_metric_count": 0,
    "insufficient_components_count": 0,
    "insufficient_fundamentals_count": 0,
    "missing_market_date_bar_count": 0,
    "ranking_universe_counts": {
      "roe": 2098,
      "roa": 2096,
      "gross_profit_to_assets": 2255,
      "operating_cash_flow_to_net_income": 2099,
      "debt_ratio": 2568,
      "accruals_ratio": 2098,
      "quality_score": 2314
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
      "source": "quality_v1",
      "feature_version": "quality_v1",
      "roe": 0.128411,
      "roa": 0.091772,
      "gross_profit_to_assets": 0.346812,
      "operating_cash_flow_to_net_income": 1.104883,
      "debt_ratio": 0.338204,
      "accruals_ratio": -0.009623,
      "roe_status": "available",
      "roa_status": "available",
      "gross_profit_to_assets_status": "available",
      "operating_cash_flow_to_net_income_status": "available",
      "debt_ratio_status": "available",
      "accruals_ratio_status": "available",
      "roe_percentile": 0.889312,
      "roa_percentile": 0.875179,
      "gross_profit_to_assets_percentile": 0.842102,
      "operating_cash_flow_to_net_income_percentile": 0.814197,
      "debt_ratio_percentile": 0.803664,
      "accruals_ratio_percentile": 0.924118,
      "quality_score": 0.858095,
      "quality_rank": 141,
      "quality_percentile": 0.93903,
      "components_possible": 6,
      "components_used": 6,
      "is_quality_score_complete": true,
      "quality_status": "scored_complete",
      "factor_payload": {
        "feature_version": "quality_v1",
        "availability": {
          "components_possible": 6,
          "components_used": 6,
          "is_quality_score_complete": true
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
GET /eod/quality-factors?market_date=2026-04-24&limit=100
GET /eod/quality-factors?market_date=2026-04-24&symbol=005930
```

## Example read response (single symbol)
```json
{
  "market_date": "2026-04-24",
  "feature_version": "quality_v1",
  "summary": {
    "entries": 1,
    "requested_symbol": "005930",
    "matched_symbols": ["005930"],
    "missing_symbols": [],
    "scored_complete_count": 0,
    "scored_partial_count": 1,
    "invalid_assets_count": 0,
    "invalid_equity_count": 0,
    "invalid_net_income_count": 0,
    "missing_metric_count": 0,
    "insufficient_components_count": 0,
    "insufficient_fundamentals_count": 0,
    "missing_market_date_bar_count": 0
  },
  "item": {
    "symbol": "005930",
    "market_date": "2026-04-24",
    "quality_score": 0.802531,
    "quality_rank": 221,
    "quality_percentile": 0.901995,
    "components_possible": 6,
    "components_used": 5,
    "is_quality_score_complete": false,
    "quality_status": "scored_partial",
    "factor_payload": {
      "feature_version": "quality_v1",
      "component_statuses": {
        "roe": "available",
        "roa": "available",
        "gross_profit_to_assets": "missing_gross_margin",
        "operating_cash_flow_to_net_income": "available",
        "debt_ratio": "available",
        "accruals_ratio": "available"
      }
    }
  }
}
```
