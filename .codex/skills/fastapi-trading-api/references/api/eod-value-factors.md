# POST /eod/value-factors/compute and GET /eod/value-factors

## Summary
Computes or reads persisted value-factor rows built from stored `daily_price_bars`.

The v1 slice ranks three stored valuation inputs cross-sectionally on the requested `market_date`:
- lower `per` is better
- lower `pbr` is better
- higher `dividend_yield` is better

`value_score` is the equal-weight mean of the three component percentiles. Rows with missing or invalid inputs still persist with a non-scored `value_status` so downstream consumers can distinguish `scored`, `invalid_per`, `missing_metric`, and `missing_market_date_bar`.

## Compute query parameters
- `market_date` (required): ISO trade date to score from stored bars.
- `symbols` (optional): comma-separated symbol subset. When omitted, the route uses the full stored symbol universe with a bar on `market_date`.
- `source` (optional): persisted feature-version string. Defaults to `value_factor_v1`.

If `symbols` is provided but trims down to an explicit empty subset, the route returns a no-op response with zero persisted rows.

## Example compute request
```http
POST /eod/value-factors/compute?market_date=2026-04-24&symbols=005930,000660&source=value_factor_v1
```

## Example compute response
```json
{
  "market_date": "2026-04-24",
  "source": "value_factor_v1",
  "feature_version": "value_factor_v1",
  "requested_symbols": ["005930", "000660"],
  "matched_symbols": ["005930", "000660"],
  "missing_symbols": [],
  "summary": {
    "requested_symbol_count": 2,
    "matched_symbol_count": 2,
    "missing_symbol_count": 0,
    "symbols_with_market_date_bar": 2,
    "missing_market_date_bar_count": 0,
    "invalid_per_count": 0,
    "missing_metric_count": 0,
    "scored_count": 2,
    "ranking_universe_counts": {
      "per": 2417,
      "pbr": 2410,
      "dividend_yield": 2086,
      "composite_score": 2081
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
      "source": "value_factor_v1",
      "feature_version": "value_factor_v1",
      "per": 12.418,
      "pbr": 1.142,
      "dividend_yield": 0.024,
      "per_status": "available",
      "per_rank": 412,
      "pbr_rank": 295,
      "dividend_yield_rank": 188,
      "per_percentile": 0.829339,
      "pbr_percentile": 0.877126,
      "dividend_yield_percentile": 0.910024,
      "value_score": 0.872163,
      "value_rank": 96,
      "value_percentile": 0.953868,
      "value_status": "scored",
      "factor_payload": {
        "feature_version": "value_factor_v1",
        "methodology": {
          "input_table": "daily_price_bars",
          "score_method": "equal_weight_mean_of_component_percentiles",
          "ranking_directions": {
            "per": "lower_better_positive_only",
            "pbr": "lower_better_positive_only",
            "dividend_yield": "higher_better_non_negative",
            "value_score": "higher_better"
          },
          "tie_breaker": "symbol_asc"
        },
        "metric_statuses": {
          "per": "available",
          "pbr": "available",
          "dividend_yield": "available"
        },
        "ranking": {
          "per_rank": 412,
          "pbr_rank": 295,
          "dividend_yield_rank": 188,
          "value_rank": 96,
          "per_percentile": 0.829339,
          "pbr_percentile": 0.877126,
          "dividend_yield_percentile": 0.910024,
          "value_percentile": 0.953868,
          "ranking_universe_counts": {
            "per": 2417,
            "pbr": 2410,
            "dividend_yield": 2086,
            "composite_score": 2081
          }
        },
        "audit": {
          "trade_date": "2026-04-24",
          "name_ko": "삼성전자",
          "market": "KOSPI",
          "bar_source": "pykrx",
          "bar_captured_at": "2026-04-24T15:40:12+09:00"
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
GET /eod/value-factors?market_date=2026-04-24&limit=100
GET /eod/value-factors?market_date=2026-04-24&symbol=005930
```

## Example read response (single symbol)
```json
{
  "market_date": "2026-04-24",
  "feature_version": "value_factor_v1",
  "summary": {
    "entries": 1,
    "requested_symbol": "005930",
    "matched_symbols": ["005930"],
    "missing_symbols": [],
    "scored_count": 1,
    "invalid_per_count": 0,
    "missing_metric_count": 0
  },
  "item": {
    "symbol": "005930",
    "name_ko": "삼성전자",
    "market": "KOSPI",
    "market_date": "2026-04-24",
    "source": "value_factor_v1",
    "feature_version": "value_factor_v1",
    "per": 12.418,
    "pbr": 1.142,
    "dividend_yield": 0.024,
    "per_status": "available",
    "per_rank": 412,
    "pbr_rank": 295,
    "dividend_yield_rank": 188,
    "per_percentile": 0.829339,
    "pbr_percentile": 0.877126,
    "dividend_yield_percentile": 0.910024,
    "value_score": 0.872163,
    "value_rank": 96,
    "value_percentile": 0.953868,
    "value_status": "scored",
    "factor_payload": {
      "feature_version": "value_factor_v1",
      "methodology": {
        "input_table": "daily_price_bars",
        "score_method": "equal_weight_mean_of_component_percentiles"
      }
    },
    "captured_at": "2026-04-24T06:40:13.215842+00:00"
  }
}
```
