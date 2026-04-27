# POST /eod/market-risk-snapshots/collect and GET /eod/market-risk-snapshots

## Summary
Collects or reads the persisted KIS market-risk status snapshot for Korean equities.

The collect path calls KIS `inquire-price-2` for the requested symbol/date scope, normalizes only the source status fields, and stores one reusable snapshot row per symbol/date in `daily_market_risk_snapshots`.

The read path is intentionally snapshot-first: it returns normalized KIS status flags and raw payload provenance without mixing in downstream filter policy outputs.

## Example collect request
```http
POST /eod/market-risk-snapshots/collect?market_date=2026-04-15&symbols=005930,000660&market_code=J
```

## Example collect response
```json
{
  "market_date": "2026-04-15",
  "source": "kis_quote2_snapshot_v1",
  "snapshots_persisted": 2,
  "snapshots_failed": 0,
  "summary": {
    "matched_symbol_count": 2,
    "collected_count": 2,
    "management_item_count": 0,
    "warning_count": 1,
    "trade_halted_count": 0
  },
  "items": [
    {
      "symbol": "005930",
      "market_warning_code": "01",
      "is_investment_caution": true,
      "is_trade_halted": false,
      "source_endpoint": "inquire-price-2"
    }
  ]
}
```

## Example read requests
```http
GET /eod/market-risk-snapshots?market_date=2026-04-15&symbol=005930
GET /eod/market-risk-snapshots
```

## Example read response
```json
{
  "requested_market_date": "2026-04-15",
  "resolved_market_date": "2026-04-15",
  "market_date": "2026-04-15",
  "feature_version": "kis_quote2_snapshot_v1",
  "summary": {
    "entries": 1,
    "requested_symbol": "005930",
    "matched_symbols": ["005930"],
    "missing_symbols": [],
    "management_item_count": 0,
    "warning_count": 1,
    "trade_halted_count": 0
  },
  "item": {
    "symbol": "005930",
    "market_warning_code": "01",
    "market_warning_name": "투자주의",
    "is_investment_caution": true,
    "is_trade_halted": false,
    "snapshot_payload": {
      "market_code": "J",
      "source_endpoint": "inquire-price-2"
    }
  }
}
```
