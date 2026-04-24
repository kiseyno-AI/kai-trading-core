# GET /eod/piotroski-filter-results

## Summary
Read persisted daily Piotroski filter results for an exact date or the latest available stored date.

When `market_date` is omitted, the endpoint resolves the latest stored Piotroski result date for the current request scope. When `market_date` is provided, the endpoint keeps the exact date when rows exist and otherwise falls back to the latest stored date on or before the requested date. The response makes that resolution explicit with `requested_market_date`, `resolved_market_date`, `latest_mode`, `is_latest_fallback`, and `fallback_policy`.

When `symbol` is omitted, the endpoint returns stored rows for the resolved date, optionally capped by `limit`. Rows are ordered with passed names first and then lower `pbr_percentile_rank` values first. When `symbol` is provided, the endpoint returns the single stored result for that symbol and resolved date, or `null` if none exists.

These persisted rows are also the source of truth for symbol-level agent feedback eligibility: only rows with `passed_filter=true` are eligible for daily feedback dispatch and retry on that same `market_date`.

## Query parameters
- `market_date` (optional): ISO date for the persisted filter snapshot to inspect. If omitted, the route resolves the latest stored date for the request scope.
- `symbol` (optional): six-digit stock code to narrow the result to one symbol.
- `limit` (optional): maximum number of rows to return when reading the whole date. Allowed range is `1..20000`.

## Example requests
```http
GET /eod/piotroski-filter-results
GET /eod/piotroski-filter-results?market_date=2026-04-21&limit=100
GET /eod/piotroski-filter-results?market_date=2026-04-24&symbol=005930
```

## Example response (latest mode, all rows)
```json
{
  "market_date": "2026-04-21",
  "requested_market_date": null,
  "resolved_market_date": "2026-04-21",
  "latest_mode": true,
  "is_latest_fallback": false,
  "fallback_policy": "latest_on_or_before_requested_market_date",
  "summary": {
    "entries": 100,
    "requested_symbol": null,
    "matched_symbols": ["005930"],
    "missing_symbols": [],
    "passed_count": 98,
    "not_passed_count": 2
  },
  "items": [
    {
      "symbol": "005930",
      "market_date": "2026-04-21",
      "pbr": 0.91,
      "pbr_percentile_rank": 0.083125,
      "is_low_pbr": true,
      "has_profitability_issue": false,
      "has_capital_impairment": false,
      "has_audit_issue": false,
      "passed_filter": true,
      "piotroski_score": 7,
      "passes_piotroski_score": true,
      "momentum_percentile_rank": 0.176403,
      "is_bottom_momentum": false,
      "source": "strategy_piotroski_v1",
      "filter_payload": {
        "low_pbr_threshold": 0.2,
        "momentum_threshold": 0.2,
        "signal_count": 8
      },
      "captured_at": "2026-04-21T15:42:09.184522+09:00"
    }
  ]
}
```

## Example response (requested date fell back to older stored date)
```json
{
  "market_date": "2026-04-23",
  "requested_market_date": "2026-04-24",
  "resolved_market_date": "2026-04-23",
  "latest_mode": false,
  "is_latest_fallback": true,
  "fallback_policy": "latest_on_or_before_requested_market_date",
  "summary": {
    "entries": 1,
    "requested_symbol": "005930",
    "matched_symbols": ["005930"],
    "missing_symbols": [],
    "passed_count": 1,
    "not_passed_count": 0
  },
  "item": {
    "symbol": "005930",
    "market_date": "2026-04-23",
    "pbr": 0.91,
    "pbr_percentile_rank": 0.083125,
    "is_low_pbr": true,
    "has_profitability_issue": false,
    "has_capital_impairment": false,
    "has_audit_issue": false,
    "passed_filter": true,
    "piotroski_score": 7,
    "passes_piotroski_score": true,
    "momentum_percentile_rank": 0.176403,
    "is_bottom_momentum": false,
    "source": "strategy_piotroski_v1",
    "filter_payload": {
      "low_pbr_threshold": 0.2,
      "momentum_threshold": 0.2,
      "signal_count": 8
    },
    "captured_at": "2026-04-21T15:42:09.184522+09:00"
  }
}
```
