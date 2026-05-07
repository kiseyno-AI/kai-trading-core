# POST /eod/pykrx/enrichment/collect and GET /eod/pykrx/enrichment

## Summary
Collects and inspects the persisted non-live PyKRX enrichment store for one market date. The current normalized categories cover sector classification, market breadth, investor flow, short-selling, valuation snapshot fields, and foreign-ownership / exhaustion rates.

## Example collect request
```http
POST /eod/pykrx/enrichment/collect?market_date=2026-04-21&markets=KOSPI,KOSDAQ&categories=sector_classification,investor_flow_symbol,short_selling
```

## Example inspect request
```http
GET /eod/pykrx/enrichment?market_date=2026-04-21&markets=KOSPI&symbols=005930&categories=sector_classification,investor_flow_symbol&limit=20
```

## Example response
```json
{
  "market_date": "2026-04-21",
  "categories_requested": [
    "sector_classification",
    "investor_flow_symbol"
  ],
  "coverage": {
    "market_date": "2026-04-21",
    "requested_symbol_count": 1,
    "requested_market_count": 1,
    "categories": {
      "sector_classification": {
        "scope_type": "symbol",
        "present_rows": 1,
        "expected_rows": 1,
        "coverage_ratio": 1.0,
        "coverage_status": "complete"
      },
      "investor_flow_symbol": {
        "scope_type": "symbol",
        "present_rows": 1,
        "expected_rows": 1,
        "coverage_ratio": 1.0,
        "coverage_status": "complete"
      }
    }
  },
  "items": [
    {
      "market_date": "2026-04-21",
      "market": "KOSPI",
      "category": "sector_classification",
      "scope_type": "symbol",
      "scope_key": "005930",
      "symbol": "005930",
      "name_ko": "삼성전자",
      "payload": {
        "sector_name": "반도체",
        "market_cap": 420000000000
      }
    }
  ]
}
```
