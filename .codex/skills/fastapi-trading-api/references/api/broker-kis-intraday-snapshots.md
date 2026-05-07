# GET /broker/kis/intraday-snapshots

## Summary
Read the latest persisted KIS intraday execution-context snapshot cache.

## Example request
```http
GET /broker/kis/intraday-snapshots?symbols=005930&limit=5
```

## Example response
```json
{
  "runtime": {
    "enabled": true,
    "ready": true,
    "credentials_configured": true,
    "kis_use_mock": true,
    "market_code": "J",
    "max_ranking_pages": 2,
    "news_limit": 5,
    "blocking_reasons": []
  },
  "requested_symbols": ["005930"],
  "summary": {
    "entries": 1,
    "available_count": 1,
    "partial_count": 0,
    "unavailable_count": 0,
    "fresh_count": 1,
    "stale_count": 0
  },
  "items": [
    {
      "symbol": "005930",
      "market_code": "J",
      "source": "kis_intraday_snapshot_v1",
      "feature_version": "kis_intraday_snapshot_v1",
      "availability_status": "available",
      "freshness_status": "fresh",
      "as_of_market_date": "2026-05-07",
      "as_of_timestamp": "2026-05-07T09:57:00+09:00",
      "quote": {
        "price": 71550,
        "spread_points": 100
      },
      "orderbook": {
        "imbalance": 0.076923
      },
      "liquidity": {
        "volume_power_rank": 3
      },
      "investor_program_activity": {
        "status": "available"
      },
      "news_hints": {
        "status": "available",
        "item_count": 1
      }
    }
  ]
}
```

## Notes
- This read path returns the current cache, not the full append-only history table.
- `availability_status` and `freshness_status` let operators distinguish partial/stale intraday context from completed EOD fact tables.
