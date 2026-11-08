# GET /market/live-sector-projection

## Summary
Returns a conservative current-session sector proxy for one KRX symbol without treating stored EOD performance as live authority.

The endpoint:
- resolves sector identity from the exact requested PyKRX mapping date;
- requires complete market-wide PyKRX membership count and SHA-256 proof;
- selects at most eight top-market-cap members and reports sector/cohort coverage;
- uses fresh same-session KIS websocket ticks or real-domain KIS quote timestamps for member returns;
- uses the fresh same-session KIS index leg from the Core market snapshot as the market benchmark; and
- publishes the oldest contributing provider observation as `observed_at`.

`status` is:
- `ready` only when selected and live quote market-cap coverage are each at least 60%;
- `pending_current_session` before the regular session has produced live evidence; or
- `unavailable` for closed sessions, stale/wrong-date evidence, incomplete mapping proof, or insufficient coverage.

Stored PyKRX `change_pct`, close, and other EOD performance fields are never used to calculate the live return.

## Example request
```http
GET /market/live-sector-projection?symbol=005930&source_market_date=2026-11-04
```

## Example ready response
```json
{
  "schema_version": "live_sector_projection_v1",
  "status": "ready",
  "reason": null,
  "symbol": "005930",
  "market": "KOSPI",
  "market_date": "2026-11-05",
  "source_market_date": "2026-11-04",
  "observed_at": "2026-11-05T09:57:42+09:00",
  "source": "core_verified_pykrx_sector_plus_kis_current_session",
  "sector_identity": {
    "status": "available",
    "sector_name": "전기전자",
    "mapping_date": "2026-11-04",
    "mapping_source": "pykrx"
  },
  "cohort": {
    "basis": "prior_session_market_cap_top_cohort",
    "selected_symbol_count": 8,
    "market_cap_coverage_ratio": 0.74,
    "live_symbol_count": 7,
    "live_market_cap_coverage_ratio": 0.68,
    "symbols": []
  },
  "benchmark": {
    "market": "KOSPI",
    "source": "kis_market_snapshot_index",
    "observed_at": "2026-11-05T09:58:00+09:00",
    "change_percent": -0.21
  },
  "projection": {
    "status": "ready",
    "basis": "prior_session_market_cap_weighted_live_quote_proxy_vs_live_market_index",
    "sector_return_pct": -0.05,
    "benchmark_return_pct": -0.21,
    "relative_return_pct": 0.16,
    "live_market_cap_coverage_ratio": 0.68
  },
  "missing_fields": [],
  "served_from_cache": false
}
```

## Example pending response
```json
{
  "schema_version": "live_sector_projection_v1",
  "status": "pending_current_session",
  "reason": "current_session_sector_performance_pending_market_open",
  "symbol": "005930",
  "market": "KOSPI",
  "market_date": "2026-11-05",
  "source_market_date": "2026-11-04",
  "observed_at": null,
  "source": "core_verified_pykrx_sector_plus_kis_current_session",
  "sector_identity": {"status": "available", "sector_name": "전기전자"},
  "cohort": {"market_cap_coverage_ratio": 0.74, "symbols": []},
  "benchmark": null,
  "projection": {
    "status": "pending_current_session",
    "sector_return_pct": null,
    "benchmark_return_pct": null,
    "relative_return_pct": null
  },
  "missing_fields": ["current_session_quotes", "current_session_benchmark"],
  "served_from_cache": false
}
```
