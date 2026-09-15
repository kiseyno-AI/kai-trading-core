# POST /eod/market-risk-filters/compute and GET /eod/market-risk-filters

## Summary
Computes or reads the persisted KIS-backed market-risk status filter for Korean equities.

The compute path now runs in two layers:
- collect or reuse stored KIS `inquire-price-2` snapshots first
- evaluate focused policy rules second and store the downstream result rows in `daily_market_risk_filter_results`

Both layers are current-date only. For a prior `market_date`, the POST route returns HTTP 200 with an auditable safe-skip response and neither calls KIS nor writes filter rows from current observations. Use the GET route to inspect rows already persisted for that historical date.

The async POST route resolves the current Seoul market date through the asynchronous market clock before applying that fence, so a cold calendar cache cannot move synchronous KIS calendar I/O onto the FastAPI event loop.

The compute route also creates a durable daily pipeline run for `market_risk_filter`. `/eod/pipeline/status` can show the stage as `running` and update `processed`, `total`, and `current_symbol` while KIS calls are still underway, before filter rows are persisted.

The read path is intentionally filter-focused: it returns policy state plus explicit blocking or watchlist reasons, not the full normalized snapshot field set. These persisted rows are also one half of agent-feedback eligibility: symbol-level feedback now requires `passed_filter=true` here and in the same-day Piotroski rows.

## Example compute request
```http
POST /eod/market-risk-filters/compute?market_date=2026-04-15&symbols=005930,000660&market_code=J
```

## Example compute response
```json
{
  "pipeline_run_id": 812,
  "market_date": "2026-04-15",
  "source": "kis_market_risk_v1",
  "snapshot_collection": {
    "feature_version": "kis_quote2_snapshot_v1",
    "snapshots_persisted": 2,
    "snapshots_failed": 0
  },
  "results_persisted": 2,
  "results_failed": 0,
  "summary": {
    "matched_symbol_count": 2,
    "evaluated_count": 2,
    "passed_count": 1,
    "watchlist_count": 1,
    "failed_count": 0
  },
  "items": [
    {
      "symbol": "005930",
      "risk_state": "watchlist",
      "passed_filter": true,
      "watchlist_flagged": true,
      "filter_payload": {
        "blocking_reasons": [],
        "watchlist_reasons": ["market_warning_01", "investment_caution"]
      }
    }
  ]
}
```

## Prior-date safe-skip response

```json
{
  "triggered": false,
  "skipped": true,
  "safe_skip": true,
  "status": "skipped",
  "reason": "point_in_time_recovery_current_only_stage",
  "market_date": "2026-04-15",
  "stage": "market_risk_filter"
}
```

## Example read requests
```http
GET /eod/market-risk-filters?market_date=2026-04-15&symbol=005930
GET /eod/market-risk-filters
```

## Example read response
```json
{
  "requested_market_date": "2026-04-15",
  "resolved_market_date": "2026-04-15",
  "market_date": "2026-04-15",
  "feature_version": "kis_market_risk_v1",
  "summary": {
    "entries": 1,
    "requested_symbol": "005930",
    "matched_symbols": ["005930"],
    "missing_symbols": [],
    "passed_count": 1,
    "watchlist_count": 1,
    "failed_count": 0
  },
  "item": {
    "symbol": "005930",
    "risk_state": "watchlist",
    "passed_filter": true,
    "watchlist_flagged": true,
    "filter_payload": {
      "blocking_reasons": [],
      "watchlist_reasons": ["market_warning_01", "investment_caution"]
    }
  }
}
```
