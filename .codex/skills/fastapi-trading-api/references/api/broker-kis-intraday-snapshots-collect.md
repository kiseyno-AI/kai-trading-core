# POST /broker/kis/intraday-snapshots/collect

## Summary
Collect and persist KIS intraday execution-context snapshots, separate from the EOD PyKRX/DART jobs.

## Example request
```http
POST /broker/kis/intraday-snapshots/collect?symbols=005930,000660&market_code=J&max_ranking_pages=2&max_news_items=5&include_news=true
```

## Example response
```json
{
  "status": "ok",
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
  "market_state": "OPEN",
  "market_date": "2026-05-07",
  "market_code": "J",
  "source": "kis_intraday_snapshot_v1",
  "feature_version": "kis_intraday_snapshot_v1",
  "requested_symbols": ["005930"],
  "summary": {
    "matched_symbol_count": 1,
    "collected_count": 1,
    "available_count": 1,
    "partial_count": 0,
    "unavailable_count": 0
  },
  "snapshots_persisted": 1,
  "snapshots_failed": 0,
  "failed_symbols": [],
  "failed_symbol_errors": [],
  "items": [
    {
      "symbol": "005930",
      "market_code": "J",
      "availability_status": "available",
      "freshness_status": "fresh",
      "as_of_market_date": "2026-05-07",
      "as_of_timestamp": "2026-05-07T09:57:00+09:00",
      "coverage": {
        "status": "available",
        "available_blocks": 5,
        "partial_blocks": 0,
        "total_blocks": 5
      },
      "quote": {
        "price": 71550,
        "spread_points": 100
      },
      "orderbook": {
        "imbalance": 0.076923
      },
      "liquidity": {
        "volume_power_rank": 3,
        "quote_balance_rank": 5
      },
      "investor_program_activity": {
        "status": "available",
        "program_net_quantity": 12000
      },
      "news_hints": {
        "status": "available",
        "item_count": 1,
        "event_tags": ["contract", "earnings"]
      }
    }
  ]
}
```

## Notes
- When KIS credentials are missing or the feature toggle is off, the route returns `status=disabled` with `runtime.blocking_reasons`.
- The stored payload is the same normalized execution-context block surfaced later through `GET /broker/kis/intraday-snapshots` and `GET /agent-context/symbol-day`.
