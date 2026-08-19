# POST /us-equities/universe/refresh and GET /us-equities/universe

## Summary

Build and inspect the KIS-tradable US equity universe using official Nasdaq Trader, KIS overseas master, and SEC EDGAR mapping sources.

The refresh is deterministic and records explicit exclusion reasons. Internal symbols use dot class separators while `standard_code` preserves the KIS order symbol. No order is submitted by either route.

`SEC_EDGAR_USER_AGENT` must contain the requester identity and an administrative contact email before refresh can run. Use `persist=false` to validate source availability without changing the database.

## Example refresh

```http
POST /us-equities/universe/refresh
Content-Type: application/json

{
  "persist": false,
  "preview_limit": 20
}
```

The response includes source row counts, integrated/SEC-matched counts, exclusion reason counts, persisted count, and a bounded symbol preview.

## Example read

```http
GET /us-equities/universe?limit=100
```
