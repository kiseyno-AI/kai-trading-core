# GET /portfolio/summary

## Summary
Read the aggregate portfolio payload for dashboard and portfolio-card consumers. The route resolves deployment-configured KIS account references, reuses the existing account inquiry services, and serves a short-lived cached summary by default so repeated dashboard reads do not fan out to live KIS account inquiries on every request.

## Example request
```http
GET /portfolio/summary?refresh=false
```

## Example response
```json
{
  "asOf": "2026-04-29T01:23:45+00:00",
  "source": "kis_portfolio_summary_v1",
  "sync": {
    "status": "stale",
    "lastSyncedAt": "2026-04-29T01:23:45+00:00",
    "source": "kis_account_inquiry_v1",
    "message": "Portfolio summary is serving cached data while a refresh runs in background."
  },
  "freshness": {
    "servedFromCache": true,
    "hasCachedSnapshot": true,
    "cachedAt": "2026-04-29T01:23:45+00:00",
    "cacheAgeSeconds": 74,
    "isStale": true,
    "staleReason": "refresh_due",
    "refreshInFlight": true,
    "refreshIntervalSeconds": 60,
    "lastRefreshAttemptedAt": "2026-04-29T01:24:59+00:00",
    "lastRefreshSucceededAt": "2026-04-29T01:23:45+00:00",
    "nextRefreshDueAt": "2026-04-29T01:24:45+00:00",
    "lastRefreshStatus": "success",
    "lastRefreshError": null
  },
  "accounts": [
    {
      "id": "KIS:12345678-01",
      "label": "Main",
      "broker": "kis",
      "accountType": "Product 01",
      "status": "synced",
      "baseCurrency": "KRW",
      "lastSyncedAt": "2026-04-29T01:23:45+00:00",
      "summary": {
        "totalAssets": {"amount": 3072000, "currency": "KRW"},
        "cash": {"amount": 2400000, "currency": "KRW"},
        "buyingPower": {"amount": 2500000, "currency": "KRW"},
        "investedValue": {"amount": 572000, "currency": "KRW"},
        "pnl": {
          "unrealized": {"amount": 22000, "currency": "KRW"},
          "total": {"amount": 22000, "currency": "KRW"},
          "totalPercent": 4.0
        }
      },
      "holdings": [
        {
          "id": "KIS:12345678-01:005930",
          "symbol": "005930",
          "name": "Samsung Electronics",
          "market": "KRX",
          "quantity": 10,
          "marketValue": {"amount": 572000, "currency": "KRW"},
          "unrealizedPnlPercent": 4.0,
          "allocationPercent": 18.62
        }
      ],
      "allocation": [
        {
          "id": "KIS:12345678-01:005930",
          "label": "Samsung Electronics",
          "value": {"amount": 572000, "currency": "KRW"},
          "percent": 18.62
        }
      ],
      "recentActivity": []
    }
  ],
  "notices": [
    "Portfolio summary is partially stale: 1 of 2 configured account summaries synced successfully."
  ]
}
```

## Notes
- `refresh=true` forces an immediate live rebuild of the portfolio summary. The default `refresh=false` path serves the cached snapshot when available and falls back to stale-while-revalidate behavior once the cache TTL expires.
- `freshness` exposes whether the response was served from cache, whether the cache is currently stale, the cache TTL, the last refresh attempt/success timestamps, and any last refresh error.
- `PORTFOLIO_ACCOUNT_REFERENCES` accepts `KIS:<account_number>-<product_code>` entries, with optional `Label=...` prefixes.
- Production portfolio aggregation also requires `KIS_APP_KEY`, `KIS_APP_SECRET`, and `KIS_USE_MOCK=false`.
- When the deployment is still missing portfolio configuration, the route returns `200` with `sync.status = "not_configured"` and explicit notices instead of pretending the portfolio is empty.
- `GET /broker/kis/health` exposes `portfolio_summary_runtime` so operators can confirm readiness before relying on the full payload.
