# GET /market/snapshot

## Summary
Returns a compact market-condition payload for the dashboard card, aggregating:
- KOSPI/KOSDAQ current snapshot from KIS
- market-level foreign/institutional flow from KIS
- USD/KRW from the existing Korea Exim service

Runtime requirements:
- KIS reads resolve to the real domain by default through `MARKET_SNAPSHOT_KIS_USE_REAL=true`, because the selected KIS index and investor-flow APIs are real-domain only
- required secrets: `KIS_APP_KEY`, `KIS_APP_SECRET`, `KOREAEXIM_API_KEY`
- `GET /broker/kis/health` exposes `market_snapshot_runtime` for readiness/debugging

## Example request
```http
GET /market/snapshot
```

## Example response
```json
{
  "as_of": "2026-04-29T10:05:12+09:00",
  "markets": {
    "kospi": {
      "label": "KOSPI",
      "index_code": "0001",
      "value": 2642.07,
      "change": 2.78,
      "change_percent": 0.11,
      "open": 2651.63,
      "high": 2653.87,
      "low": 2634.29,
      "volume": 305715,
      "trading_value": 4736153,
      "advancers": 512,
      "unchanged": 94,
      "decliners": 330,
      "foreign_net_quantity": -5290,
      "foreign_net_value": 148656,
      "institution_net_quantity": 720,
      "institution_net_value": -55532
    },
    "kosdaq": {
      "label": "KOSDAQ",
      "index_code": "1001",
      "value": 857.6,
      "change": -1.61,
      "change_percent": -0.19,
      "open": 863.69,
      "high": 864.24,
      "low": 854.72,
      "volume": 1312496,
      "trading_value": 11507962,
      "advancers": 828,
      "unchanged": 94,
      "decliners": 716,
      "foreign_net_quantity": -18565,
      "foreign_net_value": -597490,
      "institution_net_quantity": -4738,
      "institution_net_value": -150685
    }
  },
  "usd_krw": {
    "pair": "USD/KRW",
    "rate": 1444.5,
    "effective_date": "2026-04-28",
    "fallback_applied": true
  }
}
```
