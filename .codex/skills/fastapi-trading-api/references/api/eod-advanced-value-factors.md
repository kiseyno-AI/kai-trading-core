# POST /eod/advanced-value-factors/compute and GET /eod/advanced-value-factors

## Summary
Computes or reads persisted advanced value-factor rows built from stored `daily_price_bars` and stored annual DART fundamentals snapshots.

`advanced_value_v1` is a follow-up layer to value v1, not a replacement for it. The slice scores:
- higher `fcf_yield` as better
- lower `ev_to_ebitda` as better

The compute path uses the latest stored annual `CFS` DART snapshot on or before the requested `market_date` (`report_code=11011`, `period_type=annual`). The first-pass formulas are explicit:
- `free_cash_flow = operating_cash_flow - capex`
- `fcf_yield = free_cash_flow / market_cap_proxy`
- `market_cap_proxy = pbr * total_equity`
- `enterprise_value = market_cap_proxy + total_liabilities - cash_and_cash_equivalents`
- `ebitda = operating_income + depreciation_and_amortization`
- `ev_to_ebitda = enterprise_value / ebitda`

Rows with incomplete or invalid inputs still persist with explicit component statuses and `advanced_value_classification`.

## Compute query parameters
- `market_date` (required): ISO trade date to score.
- `symbols` (optional): comma-separated symbol subset. When omitted, the route uses the full stored symbol universe with a bar on `market_date`.
- `source` (optional): persisted feature-version string. Defaults to `advanced_value_v1`.

If `symbols` is provided but trims down to an explicit empty subset, the route returns a no-op response with zero persisted rows.

## Example compute request
```http
POST /eod/advanced-value-factors/compute?market_date=2026-04-24&symbols=005930,000660&source=advanced_value_v1
```

## Example compute response
```json
{
  "market_date": "2026-04-24",
  "source": "advanced_value_v1",
  "feature_version": "advanced_value_v1",
  "requested_symbols": ["005930", "000660"],
  "matched_symbols": ["005930", "000660"],
  "missing_symbols": [],
  "summary": {
    "requested_symbol_count": 2,
    "matched_symbol_count": 2,
    "missing_symbol_count": 0,
    "symbols_with_market_date_bar": 2,
    "symbols_with_annual_fundamentals": 2,
    "fcf_yield_valid_count": 2,
    "ev_to_ebitda_valid_count": 2,
    "eligible_count": 1,
    "watchlist_count": 1,
    "ineligible_count": 0,
    "insufficient_components_count": 0,
    "insufficient_fundamentals_count": 0,
    "missing_market_date_bar_count": 0,
    "ranking_universe_counts": {
      "fcf_yield": 2144,
      "ev_to_ebitda": 1738,
      "advanced_value_score": 1711
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
      "source": "advanced_value_v1",
      "feature_version": "advanced_value_v1",
      "market_cap_proxy": 425000000000000.0,
      "enterprise_value": 452000000000000.0,
      "ebitda": 80200000000000.0,
      "free_cash_flow": 29800000000000.0,
      "fcf_yield": 0.070118,
      "ev_to_ebitda": 5.63591,
      "fcf_yield_status": "available",
      "ev_to_ebitda_status": "available",
      "fcf_yield_valid_for_scoring": true,
      "ev_to_ebitda_valid_for_scoring": true,
      "fcf_yield_rank": 301,
      "ev_to_ebitda_rank": 248,
      "fcf_yield_percentile": 0.85972,
      "ev_to_ebitda_percentile": 0.900058,
      "advanced_value_score": 0.879889,
      "advanced_value_rank": 119,
      "advanced_value_percentile": 0.930451,
      "advanced_value_components_used": 2,
      "advanced_value_classification": "eligible",
      "is_advanced_value_eligible": true,
      "factor_payload": {
        "feature_version": "advanced_value_v1",
        "methodology": {
          "financial_snapshot_policy": {
            "report_code": "11011",
            "period_type": "annual",
            "fs_div": "CFS",
            "selection_rule": "latest_receipt_on_or_before_market_date"
          }
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
GET /eod/advanced-value-factors?market_date=2026-04-24&limit=100
GET /eod/advanced-value-factors?market_date=2026-04-24&symbol=005930
```

## Example read response (single symbol)
```json
{
  "market_date": "2026-04-24",
  "feature_version": "advanced_value_v1",
  "summary": {
    "entries": 1,
    "requested_symbol": "005930",
    "matched_symbols": ["005930"],
    "missing_symbols": [],
    "eligible_count": 1,
    "watchlist_count": 0,
    "ineligible_count": 0,
    "insufficient_components_count": 0
  },
  "item": {
    "symbol": "005930",
    "market_date": "2026-04-24",
    "advanced_value_score": 0.879889,
    "advanced_value_classification": "eligible",
    "fcf_yield": 0.070118,
    "ev_to_ebitda": 5.63591,
    "factor_payload": {
      "feature_version": "advanced_value_v1",
      "availability": {
        "fcf_yield_status": "available",
        "ev_to_ebitda_status": "available"
      }
    }
  }
}
```
