# POST /eod/composite-factors/compute and GET /eod/composite-factors

## Summary
Computes or reads persisted multi-factor composite rows built from the stored momentum, value v1, advanced value, low-risk, and beta slices.

`composite_v1` normalizes every consumed input to a common "higher is better" basis:
- `momentum_percentile`
- `value_score`
- `advanced_value_score`
- `low_risk_score`
- `low_beta_score`

The first-pass base weights are explicit and auditable:
- momentum: `0.25`
- value v1: `0.15`
- advanced value: `0.20`
- low-risk: `0.20`
- beta: `0.20`

Missing-input behavior is explicit. The route renormalizes weights only when a row has at least three valid factor inputs, at least one valuation-family input (`value_v1` or `advanced_value`), and at least `0.60` total base-weight coverage. Otherwise the row persists with `composite_score=null` and `composite_classification=insufficient_inputs`.

Hard exclusions are also explicit:
- `momentum_recent_overheat`
- `low_risk_liquidity_floor`

Those hard exclusions do not erase the integrated score; they mark the row `hard_excluded` while preserving score and ranking for inspection.

## Compute query parameters
- `market_date` (required): ISO trade date whose stored factor rows should be combined.
- `symbols` (optional): comma-separated symbol subset. When omitted, the route uses the full stored composite-eligible factor universe for that date.
- `source` (optional): persisted feature-version string. Defaults to `composite_v1`.

If `symbols` is provided but trims down to an explicit empty subset, the route returns a no-op response with zero persisted rows.

## Example compute request
```http
POST /eod/composite-factors/compute?market_date=2026-04-24&symbols=005930,000660&source=composite_v1
```

## Example compute response
```json
{
  "market_date": "2026-04-24",
  "source": "composite_v1",
  "feature_version": "composite_v1",
  "requested_symbols": ["005930", "000660"],
  "matched_symbols": ["005930", "000660"],
  "missing_symbols": [],
  "summary": {
    "requested_symbol_count": 2,
    "matched_symbol_count": 2,
    "missing_symbol_count": 0,
    "symbols_with_any_input": 2,
    "sufficient_input_count": 2,
    "scored_count": 2,
    "eligible_count": 1,
    "watchlist_count": 1,
    "ineligible_count": 0,
    "hard_excluded_count": 0,
    "insufficient_inputs_count": 0,
    "hard_exclusion_counts": {
      "momentum_recent_overheat": 0,
      "low_risk_liquidity_floor": 0
    },
    "input_universe_counts": {
      "momentum": 2413,
      "value_v1": 2081,
      "advanced_value": 1711,
      "low_risk": 2398,
      "beta": 2386,
      "composite_score": 1674
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
      "source": "composite_v1",
      "feature_version": "composite_v1",
      "inputs": {
        "momentum_percentile": 0.912114,
        "value_score_v1": 0.872163,
        "advanced_value_score": 0.879889,
        "low_risk_score": 0.781205,
        "low_beta_score": 0.845311
      },
      "weights": {
        "weight_model_version": "composite_weights_v1",
        "base": {
          "momentum": 0.25,
          "value_v1": 0.15,
          "advanced_value": 0.2,
          "low_risk": 0.2,
          "beta": 0.2
        },
        "applied": {
          "momentum": 0.25,
          "value_v1": 0.15,
          "advanced_value": 0.2,
          "low_risk": 0.2,
          "beta": 0.2
        }
      },
      "contributions": {
        "momentum": 0.228029,
        "value_v1": 0.130824,
        "advanced_value": 0.175978,
        "low_risk": 0.156241,
        "beta": 0.169062
      },
      "gates": {
        "has_required_inputs": true,
        "valid_input_count": 5,
        "minimum_valid_input_count": 3,
        "available_base_weight": 1.0,
        "minimum_available_weight": 0.6,
        "has_valuation_input": true,
        "requires_any_of_factors": ["advanced_value", "value_v1"],
        "is_recent_overheated": false,
        "passes_liquidity_floor": true,
        "is_hard_excluded": false,
        "hard_exclusion_reasons": []
      },
      "composite_score": 0.860134,
      "composite_rank": 81,
      "composite_percentile": 0.951584,
      "composite_classification": "eligible",
      "is_composite_eligible": true,
      "factor_payload": {
        "feature_version": "composite_v1",
        "weight_model_version": "composite_weights_v1"
      }
    }
  ]
}
```

## Read query parameters
- `market_date` (required): ISO date for the persisted composite snapshot.
- `symbol` (optional): six-digit stock code to narrow the read to one symbol.
- `limit` (optional): maximum number of rows to return when reading the full date. Allowed range is `1..20000`.

## Example read requests
```http
GET /eod/composite-factors?market_date=2026-04-24&limit=100
GET /eod/composite-factors?market_date=2026-04-24&symbol=005930
```

## Example read response (single symbol)
```json
{
  "market_date": "2026-04-24",
  "feature_version": "composite_v1",
  "summary": {
    "entries": 1,
    "requested_symbol": "005930",
    "matched_symbols": ["005930"],
    "missing_symbols": [],
    "scored_count": 1,
    "eligible_count": 1,
    "watchlist_count": 0,
    "ineligible_count": 0,
    "hard_excluded_count": 0,
    "insufficient_inputs_count": 0
  },
  "item": {
    "symbol": "005930",
    "market_date": "2026-04-24",
    "composite_score": 0.860134,
    "composite_rank": 81,
    "composite_percentile": 0.951584,
    "composite_classification": "eligible",
    "is_composite_eligible": true,
    "inputs": {
      "momentum_percentile": 0.912114,
      "value_score_v1": 0.872163,
      "advanced_value_score": 0.879889,
      "low_risk_score": 0.781205,
      "low_beta_score": 0.845311
    },
    "gates": {
      "has_required_inputs": true,
      "is_hard_excluded": false,
      "hard_exclusion_reasons": []
    },
    "factor_payload": {
      "feature_version": "composite_v1",
      "weight_model_version": "composite_weights_v1"
    }
  }
}
```
