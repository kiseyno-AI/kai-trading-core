# GET /market/snapshot

## Summary
Returns a compact market-condition payload for the dashboard card, aggregating:
- KOSPI/KOSDAQ current snapshot from KIS
- market-level foreign/institutional flow from KIS
- USD/KRW from the existing Korea Exim service
- explicit degradation metadata when the USD/KRW leg is stale or unavailable

Runtime requirements:
- KIS reads resolve to the real domain by default through `MARKET_SNAPSHOT_KIS_USE_REAL=true`, because the selected KIS index and investor-flow APIs are real-domain only
- required secrets: `KIS_APP_KEY`, `KIS_APP_SECRET`, `KOREAEXIM_API_KEY`
- `GET /broker/kis/health` exposes `market_snapshot_runtime` for readiness/debugging

Degradation behavior:
- if Korea Exim returns `result=4` or another USD/KRW-only failure, the endpoint still returns `200` with preserved KIS market rows
- top-level `status`, `missing_fields`, and `stale_reason` describe whether the snapshot is `ready`, `stale`, or `partial`
- `usd_krw.status` is `ready`, `stale`, or `missing`; `usd_krw.source` is `live`, `cache`, or `unavailable`
- when a prior USD/KRW fetch succeeded, the service reuses that cached value and marks the FX leg stale instead of failing the whole snapshot
- when no cached USD/KRW value exists, the snapshot becomes partial and `missing_fields` includes the missing FX fields

## Example request
```http
GET /market/snapshot
```

## Example response
```json
{
  "as_of": "2026-04-30T13:15:00+09:00",
  "status": "stale",
  "missing_fields": [],
  "stale_reason": "koreaexim_quota_exhausted",
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
    "effective_date": "2026-04-29",
    "fallback_applied": false,
    "status": "stale",
    "source": "cache",
    "stale_reason": "koreaexim_quota_exhausted",
    "detail": "Korea Exim returned result=4",
    "captured_at": "2026-04-30T09:02:11+09:00"
  }
}
```

When no cached USD/KRW value exists, the response keeps the same market rows but shifts to:
- `status: "partial"`
- `missing_fields: ["usd_krw.rate", "usd_krw.effective_date"]`
- `usd_krw.status: "missing"`
- `usd_krw.source: "unavailable"`
