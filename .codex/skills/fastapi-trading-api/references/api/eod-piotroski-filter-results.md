# GET /eod/piotroski-filter-results

## Summary
Read persisted daily Piotroski filter results for a required market date.

When `symbol` is omitted, the endpoint returns stored rows for the requested date, optionally capped by `limit`. Rows are ordered with passed names first and then lower `pbr_percentile_rank` values first. When `symbol` is provided, the endpoint returns the single stored result for that symbol and date, or `null` if none exists.

## Query parameters
- `market_date` (required): ISO date for the persisted filter snapshot to inspect.
- `symbol` (optional): six-digit stock code to narrow the result to one symbol.
- `limit` (optional): maximum number of rows to return when reading the whole date. Allowed range is `1..20000`.

## Example requests
```http
GET /eod/piotroski-filter-results?market_date=2026-04-21&limit=100
GET /eod/piotroski-filter-results?market_date=2026-04-21&symbol=005930
```

## Example response (all rows)
```json
{
  "market_date": "2026-04-21",
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

## Example response (single symbol)
```json
{
  "market_date": "2026-04-21",
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
}
```
