# GET /eod/piotroski-filter-results

## Summary
Read persisted daily Piotroski filter results for a required market date.

When `symbol` is omitted, the endpoint returns all stored rows for the requested date. When `symbol` is provided, it returns the single stored result for that symbol and date, or `null` if none exists.

## Query parameters
- `market_date` (required): ISO date for the persisted filter snapshot to inspect.
- `symbol` (optional): six-digit stock code to narrow the result to one symbol.

## Example requests
```http
GET /eod/piotroski-filter-results?market_date=2026-04-21
GET /eod/piotroski-filter-results?market_date=2026-04-21&symbol=005930
```

## Example response (all rows)
```json
{
  "market_date": "2026-04-21",
  "summary": {
    "entries": 3577,
    "passed_count": 98,
    "not_passed_count": 3479
  },
  "items": [
    {
      "symbol": "005930",
      "market_date": "2026-04-21",
      "passed_filter": true,
      "piotroski_score": 7,
      "source": "strategy_piotroski_v1",
      "filter_payload": {
        "filter_name": "piotroski_filter_d"
      }
    }
  ]
}
```

## Example response (single symbol)
```json
{
  "market_date": "2026-04-21",
  "symbol": "005930",
  "item": {
    "symbol": "005930",
    "market_date": "2026-04-21",
    "passed_filter": true,
    "piotroski_score": 7,
    "source": "strategy_piotroski_v1",
    "filter_payload": {
      "filter_name": "piotroski_filter_d"
    }
  }
}
```
