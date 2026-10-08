# GET /eod/freshness

## Summary
Compact web-facing freshness read API for EOD ingestion and downstream dashboards.

Canonical semantics for this route and `GET /eod/pipeline/status` live in `docs/EOD_STATUS_SEMANTICS_CONTRACT.md`.

The `today` block is scoped to the current Seoul trade date and is derived from persisted same-day data. Price, technical-analysis, and action-summary counts use the active symbol universe. `today.fundamentals` is intentionally an activity view: its `basis` is `same_day_snapshot_updates`, and `current_run` shows the latest refresh-run progress for the current market date when one exists.

The `latest` block keeps the latest stored price date, preserves the legacy `price_trade_date` and `symbols_*` fields, and exposes top-level `trade_date`, `updated_at`, `succeeded`, `failed`, and `total` values for the web. `latest.fundamentals` is the latest-available fundamentals coverage view sourced from the latest completed or partial `dart_fundamentals_refresh_runs` row, so clients can keep showing the last completed refresh result until a newer completed run supersedes it.

Both `today.pykrx_enrichment` and `latest.pykrx_enrichment` expose PyKRX enrichment coverage from `daily_pykrx_enrichment_rows`, including expected and present row counts by category plus `missing_categories` and `partial_categories`.

`latest.summary.basis` names the count basis. `latest_eod_run_symbol_universe` means the summary comes from the selected latest `eod_collection_runs` row for that trade date. `stored_price_bars_for_latest_trade_date` is the fallback when stored bars exist but no matching EOD run row is available.

For a date with a completed full-universe run followed by a narrow retry, the
read layer does not let the newer retry delta replace the representative daily
summary. When the newest candidate covers fewer symbols than the stored
same-date price-bar population, Core selects the more complete same-date run by
run total or success count. For example, a 1-symbol retry cannot replace a
2,679-symbol daily result.

The response now also includes a top-level `market_session` block with the requested/current calendar date, latest valid trading date, session status, data-expected flags, and `recommendation_basis`. On weekends and holidays, it makes the latest completed trading session explicit without backshifting `today.trade_date`.

## Example request
```http
GET /eod/freshness
```

## Example response
```json
{
  "market_session": {
    "requested_date": "2026-04-16",
    "current_calendar_date": "2026-04-16",
    "latest_valid_trading_date": "2026-04-16",
    "status": "closed",
    "is_trading_day": true,
    "intraday_data_expected": false,
    "quote_data_expected": false,
    "recommendation_basis": "latest_completed_eod"
  },
  "today": {
    "trade_date": "2026-04-16",
    "price": {
      "status": "ready",
      "symbols_total": 3579,
      "symbols_succeeded": 3314,
      "symbols_failed": 265
    },
    "technical_analysis": {
      "status": "missing",
      "symbols_total": 3579,
      "symbols_succeeded": 0,
      "symbols_failed": 0
    },
    "action_summary": {
      "status": "missing",
      "symbols_total": 3579,
      "symbols_succeeded": 0,
      "symbols_failed": 0
    },
    "fundamentals": {
      "status": "running",
      "basis": "same_day_snapshot_updates",
      "symbols_total": 42,
      "symbols_succeeded": 5,
      "symbols_failed": 1,
      "current_run": {
        "basis": "market_date_refresh_run",
        "market_date": "2026-04-16",
        "status": "running",
        "fs_div": "CFS",
        "trigger": "eod_pykrx_followup",
        "updated_at": "2026-04-16T15:20:00+00:00",
        "completed_at": null,
        "symbols_total": 200,
        "symbols_checked": 42,
        "symbols_succeeded": 37,
        "symbols_fresh": 32,
        "symbols_refreshed": 5,
        "symbols_failed": 1,
        "symbols_remaining": 158,
        "events_detected": 12,
        "events_captured": 5,
        "events_failed": 1,
        "error_message": null
      }
    },
    "pykrx_enrichment": {
      "status": "partial",
      "market_date": "2026-04-16",
      "updated_at": "2026-04-16T15:40:00+09:00",
      "requested_symbol_count": 3579,
      "requested_market_count": 2,
      "category_count": 7,
      "complete_category_count": 6,
      "missing_categories": [
        "investor_flow_symbol"
      ],
      "partial_categories": [],
      "categories": {
        "investor_flow_symbol": {
          "scope_type": "symbol",
          "present_rows": 0,
          "expected_rows": 3579,
          "coverage_ratio": 0.0,
          "coverage_status": "missing",
          "latest_captured_at": null
        },
        "short_selling": {
          "scope_type": "symbol",
          "present_rows": 3579,
          "expected_rows": 3579,
          "coverage_ratio": 1.0,
          "coverage_status": "complete",
          "latest_captured_at": "2026-04-16T15:40:00+09:00"
        }
      }
    }
  },
  "latest": {
    "trade_date": "2026-04-16",
    "price_trade_date": "2026-04-16",
    "eod_status": "running",
    "updated_at": "2026-04-16T15:05:12+00:00",
    "succeeded": 3314,
    "failed": 0,
    "total": 3579,
    "summary": {
      "basis": "latest_eod_run_symbol_universe",
      "updated_at": "2026-04-16T15:05:12+00:00",
      "succeeded": 3314,
      "failed": 0,
      "total": 3579
    },
    "symbols_total": 3579,
    "symbols_succeeded": 3314,
    "symbols_failed": 0,
    "technical_market_date": "2026-04-15",
    "action_market_date": "2026-04-15",
    "fundamentals": {
      "basis": "latest_available_refresh_run",
      "market_date": "2026-04-15",
      "status": "completed",
      "fs_div": "CFS",
      "trigger": "eod_pykrx_followup",
      "updated_at": "2026-04-15T16:05:00+00:00",
      "completed_at": "2026-04-15T16:05:00+00:00",
      "symbols_total": 200,
      "symbols_checked": 200,
      "symbols_succeeded": 200,
      "symbols_fresh": 189,
      "symbols_refreshed": 11,
      "symbols_failed": 0,
      "symbols_remaining": 0,
      "events_detected": 24,
      "events_captured": 11,
      "events_failed": 0,
      "error_message": null
    },
    "pykrx_enrichment": {
      "status": "ready",
      "market_date": "2026-04-15",
      "updated_at": "2026-04-15T15:40:00+09:00",
      "requested_symbol_count": 3579,
      "requested_market_count": 2,
      "category_count": 7,
      "complete_category_count": 7,
      "missing_categories": [],
      "partial_categories": [],
      "categories": {
        "sector_classification": {
          "scope_type": "symbol",
          "present_rows": 3579,
          "expected_rows": 3579,
          "coverage_ratio": 1.0,
          "coverage_status": "complete",
          "latest_captured_at": "2026-04-15T15:40:00+09:00"
        }
      }
    }
  }
}
```
