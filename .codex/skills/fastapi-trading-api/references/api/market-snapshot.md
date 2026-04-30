# GET /market/snapshot

## Summary
Returns a compact market-condition payload for the dashboard card from Core’s persisted market snapshot cache, aggregating:
- KOSPI/KOSDAQ current snapshot from KIS
- market-level foreign/institutional flow from KIS
- USD/KRW from the existing Korea Exim service
- explicit degradation metadata when the USD/KRW leg is stale or unavailable
- explicit cache freshness metadata for the current persisted read model

Runtime requirements:
- KIS reads resolve to the real domain by default through `MARKET_SNAPSHOT_KIS_USE_REAL=true`, because the selected KIS index and investor-flow APIs are real-domain only
- required secrets: `KIS_APP_KEY`, `KIS_APP_SECRET`, `KOREAEXIM_API_KEY`
- cache refresh cadence is controlled by `MARKET_SNAPSHOT_REFRESH_INTERVAL_SECONDS` (default `3600`)
- `GET /broker/kis/health` exposes `market_snapshot_runtime` for readiness/debugging

Degradation behavior:
- request-time reads do not call KIS or Korea Exim directly; Core serves the last persisted snapshot
- if Korea Exim returns `result=4` or another USD/KRW-only failure during a background refresh, the endpoint still returns `200` with preserved KIS market rows
- top-level `status`, `missing_fields`, and `stale_reason` describe whether the snapshot is `ready`, `stale`, or `partial`
- `usd_krw.status` is `ready`, `stale`, or `missing`; `usd_krw.source` is `live`, `cache`, or `unavailable`
- when a prior USD/KRW fetch succeeded, the service reuses that cached value and marks the FX leg stale instead of failing the whole snapshot
- when no cached USD/KRW value exists, the snapshot becomes partial and `missing_fields` includes the missing FX fields
- `freshness` reports the current cache cadence, last refresh attempt/success timestamps, next due time, and whether the last refresh was `success`, `degraded`, or `error`

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
  },
  "freshness": {
    "served_from_cache": true,
    "refresh_interval_seconds": 3600,
    "last_refresh_attempted_at": "2026-04-30T13:15:00+09:00",
    "last_refresh_succeeded_at": "2026-04-30T13:15:00+09:00",
    "next_refresh_due_at": "2026-04-30T14:15:00+09:00",
    "last_refresh_status": "degraded",
    "last_refresh_error": "Korea Exim returned result=4"
  }
}
```

When no cached USD/KRW value exists, the response keeps the same market rows but shifts to:
- `status: "partial"`
- `missing_fields: ["usd_krw.rate", "usd_krw.effective_date"]`
- `usd_krw.status: "missing"`
- `usd_krw.source: "unavailable"`
