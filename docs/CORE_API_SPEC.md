# Core API Specification

## Purpose
This document describes the currently implemented `kai-trading-core` API surface in a consumer-friendly way for web, UX, GUI, and agent integration work.

It focuses on:
- what the API currently serves,
- how requests are shaped,
- what responses roughly look like,
- how to interpret freshness, nullability, and partial coverage.

This document is intentionally based on the current implemented system, not on hypothetical future endpoints.

---

## Base Concepts

### Authentication
Most external read/write API routes are protected.
Typical access pattern:
- `Authorization: Bearer <token>`

Internal automation may use separate internal call paths, but UX/web consumers should assume bearer-protected upstream APIs unless explicitly proxied through the web layer.

### Date Formats
Dates generally use ISO calendar date format:
- `YYYY-MM-DD`

Examples:
- `2026-04-09`
- `2015-01-01`

### Timestamps
Timestamps generally use ISO 8601 with timezone offsets.

Example:
- `2026-04-10T02:51:10.622651+00:00`

### Symbols
Symbols are Korean equity codes represented as strings.

Examples:
- `005930`
- `071050`

Do not assume numeric semantics in clients. Treat them as string identifiers.

---

# 1. Market / Quote APIs

## 1.1 `GET /market/status`
Returns current market state and related session timing metadata.

### Example request
```http
GET /market/status
Authorization: Bearer <token>
```

### Example response
```json
{
  "market": "KRX",
  "state": "CLOSED",
  "as_of": "2026-04-10T15:20:11+09:00",
  "session_label": "after_close",
  "is_open": false
}
```

### Notes
- `state` is used by the web to decide whether some screens should keep polling.
- Common consumer interpretation:
  - `OPEN` means live market behavior
  - `CLOSED` means post-close behavior
- Exact extra fields may evolve, but `state` is the key UX field.

---

## 1.2 `GET /market/snapshot`
Returns a compact market-condition payload for the dashboard card.

### Example request
```http
GET /market/snapshot
Authorization: Bearer <token>
```

### Example response
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
      "trading_value": 4736153,
      "trading_value_unit": "million_krw",
      "foreign_net_quantity": -5290,
      "foreign_net_value": -1455933000000,
      "institution_net_quantity": 720,
      "institution_net_value": 283792000000,
      "investor_flow_value_unit": "krw"
    },
    "kosdaq": {
      "label": "KOSDAQ",
      "index_code": "1001",
      "value": 857.6,
      "change": -1.61,
      "change_percent": -0.19,
      "trading_value": 11507962,
      "trading_value_unit": "million_krw",
      "foreign_net_quantity": -18565,
      "foreign_net_value": -211205000000,
      "institution_net_quantity": -4738,
      "institution_net_value": -304502000000,
      "investor_flow_value_unit": "krw"
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

### Notes
- This route is intentionally compact and card-oriented rather than chart-oriented.
- KOSPI/KOSDAQ index data comes from KIS `inquire-index-price`.
- Market-level investor flow comes from KIS `inquire-investor-time-by-market`.
- `trading_value` preserves KIS `acml_tr_pbmn` scale and is expressed in million KRW; see `trading_value_unit`.
- `foreign_net_value` and `institution_net_value` are normalized from KIS `*_tr_pbmn` fields into raw KRW; see `investor_flow_value_unit`.
- USD/KRW comes from the existing Korea Exim integration, including its fallback-date behavior, and Core persists the latest usable USD/KRW row so refresh-time fallback survives process restarts.
- The selected KIS endpoints are real-domain only, so this route resolves KIS requests against the real domain by default via `MARKET_SNAPSHOT_KIS_USE_REAL=true`.
- Required secrets are `KIS_APP_KEY`, `KIS_APP_SECRET`, and `KOREAEXIM_API_KEY`.
- Runtime readiness is exposed through `GET /broker/kis/health` as `market_snapshot_runtime`, and deploy smoke-check now includes this route.

---

## 1.3 `GET /quotes/{symbol}`
Returns a quote payload for a single symbol.

### Example request
```http
GET /quotes/005930
Authorization: Bearer <token>
```

### Example response
```json
{
  "symbol": "005930",
  "name": "삼성전자",
  "price": 81200,
  "change": -300,
  "change_pct": -0.37,
  "market_state": "CLOSED",
  "available": true,
  "source": "daily_bar",
  "as_of": "2026-04-09"
}
```

### Important semantics
- `market_state` is used with `/market/status` to decide whether symbol-detail polling should stop.
- `available: false` is a valid state and should not be converted into synthetic zero values.
- `source` communicates provenance, for example:
  - live quote source,
  - daily bar fallback,
  - cache.
- `bid_price` and `ask_price` may appear on live KIS-backed rows when the upstream quote included spread fields; fallback rows can omit them.

### UX guidance
If `available` is false:
- do not render fake price values,
- show an explicit unavailable state.

---

## 1.4 `GET /quotes/tracked`
Returns tracked-universe quote data for dashboard-like surfaces.

### Example request
```http
GET /quotes/tracked
Authorization: Bearer <token>
```

### Example response
```json
{
  "items": [
    {
      "symbol": "005930",
      "name": "삼성전자",
      "price": 81200,
      "change": -300,
      "change_pct": -0.37,
      "market_state": "CLOSED",
      "available": true
    },
    {
      "symbol": "000660",
      "name": "SK하이닉스",
      "price": 203500,
      "change": 4200,
      "change_pct": 2.11,
      "market_state": "CLOSED",
      "available": true
    }
  ]
}
```

### Notes
- Used primarily by tracked/dashboard surfaces.
- Not the same as symbol-detail quote polling.

---

## 1.5 `GET /portfolio/summary`
Returns the aggregate portfolio payload used by the dashboard and portfolio pages.

### Example request
```http
GET /portfolio/summary?refresh=false
Authorization: Bearer <token>
```

### Example response
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
          "averagePrice": {"amount": 55000, "currency": "KRW"},
          "lastPrice": {"amount": 57200, "currency": "KRW"},
          "marketValue": {"amount": 572000, "currency": "KRW"},
          "unrealizedPnl": {"amount": 22000, "currency": "KRW"},
          "unrealizedPnlPercent": 4.0,
          "allocationPercent": 18.62,
          "currency": "KRW",
          "updatedAt": "2026-04-29T01:23:45+00:00"
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
      "recentActivity": [
        {
          "id": "ORDER-1",
          "symbol": "005930",
          "side": "buy",
          "status": "filled",
          "quantity": 5,
          "filledQuantity": 5,
          "averageFillPrice": {"amount": 57100, "currency": "KRW"},
          "submittedAt": "2026-04-29T01:20:00+00:00",
          "updatedAt": "2026-04-29T01:21:00+00:00"
        }
      ]
    }
  ],
  "notices": []
}
```

### Notes
- The route resolves its account scope from `PORTFOLIO_ACCOUNT_REFERENCES`, a comma-separated list of `KIS:<account_number>-<product_code>` values. Optional labels can be supplied as `Label=KIS:...`.
- This is a portfolio-level aggregation layer over the account-specific KIS inquiry routes. It reuses the same KIS account-reference model as `/trading/accounts/{account_reference}/summary` and `/positions`.
- The default `refresh=false` path is a short-lived cached read model with stale-while-revalidate semantics. `refresh=true` forces an immediate live rebuild for manual sync paths.
- Production portfolio aggregation also requires `KIS_APP_KEY`, `KIS_APP_SECRET`, and `KIS_USE_MOCK=false`. In `APP_ENV=production`, mock-KIS portfolio wiring is treated as `not_configured`.
- `sync.status` is the primary UX state. Current intended values are:
  - `synced` when every configured account summary and holdings read succeeds
  - `stale` when at least one account succeeds but one or more configured accounts fail
  - `unavailable` when no configured account can be read from KIS
  - `not_configured` when account resolution or KIS deployment configuration is missing or invalid
- `freshness` exposes the cache hit state, cache TTL, cache age, last refresh attempt/success timestamps, `lastRefreshStatus`, `lastRefreshError`, and whether a background refresh is currently in flight.
- On degraded accounts, missing sections may be omitted instead of returned as misleading empty arrays, while `notices` explains which account or subsection failed.
- Percent fields in this route use whole-percent values for direct UI rendering, for example `4.0` means `4.00%`.
- Runtime readiness is exposed through `GET /broker/kis/health` as `portfolio_summary_runtime`. Deploy preflight reports the same status under `runtime_checks.portfolio_summary`, and deploy smoke-check now expects `/portfolio/summary` to leave the `not_configured` state after restart.

---

## 1.6 `GET /scanner/tracked-symbols`
Returns tracked/scanner symbol universe information used by dashboard/tracker-like surfaces.

### Example request
```http
GET /scanner/tracked-symbols
Authorization: Bearer <token>
```

### Example response
```json
{
  "symbols": [
    "005930",
    "000660",
    "035420"
  ],
  "count": 3
}
```

### Notes
- This is used for tracked-universe/dashboard logic.
- Symbol detail should not depend on this route.

---

## 1.7 `GET /symbols/search`
Searches symbol metadata.

### Example request
```http
GET /symbols/search?q=삼성
Authorization: Bearer <token>
```

### Example response
```json
{
  "query": "삼성",
  "items": [
    {
      "symbol": "005930",
      "name": "삼성전자",
      "market": "KOSPI"
    },
    {
      "symbol": "207940",
      "name": "삼성바이오로직스",
      "market": "KOSPI"
    }
  ]
}
```

### Notes
- Used in search UI and occasional route metadata recovery flows.

---

## 1.6 `GET /symbols/{symbol}/price-bars`
Returns historical price bars for charting.

### Example request
```http
GET /symbols/005930/price-bars?start_date=2025-04-09&end_date=2026-04-09&limit=250
Authorization: Bearer <token>
```

### Example response
```json
{
  "symbol": "005930",
  "count": 2,
  "start_date": "2026-04-07",
  "end_date": "2026-04-08",
  "requested_start_date": "2025-04-09",
  "requested_end_date": "2026-04-09",
  "items": [
    {
      "trade_date": "2026-04-07",
      "open": 79800,
      "high": 81000,
      "low": 79400,
      "close": 80700,
      "volume": 15234567
    },
    {
      "trade_date": "2026-04-08",
      "open": 80800,
      "high": 82000,
      "low": 80400,
      "close": 81500,
      "volume": 17567890
    }
  ]
}
```

### Notes
- Intended for chart rendering.
- Date ordering should be treated as explicit rather than assumed.

---

# 2. EOD and Derived Data APIs

## 2.1 `GET /eod/status`
Returns nested EOD automation status snapshots.

### Example request
```http
GET /eod/status
Authorization: Bearer <token>
```

### Example response
```json
{
  "job": {
    "last_market_date": "2026-04-09",
    "last_started_at": "2026-04-09T09:42:00+00:00",
    "last_completed_at": "2026-04-09T09:51:21+00:00",
    "last_status": "completed_with_errors",
    "last_error": null,
    "symbols_total": 3584,
    "symbols_processed": 3584,
    "symbols_succeeded": 3581,
    "symbols_failed": 3,
    "last_failed_symbols": ["444920", "458610", "473370"]
  },
  "master": {
    "market_date": "2026-04-09",
    "last_started_at": "2026-04-09T08:59:00+00:00",
    "last_completed_at": "2026-04-09T08:59:07+00:00",
    "last_status": "completed",
    "last_error": null,
    "symbol_count": 3584,
    "kospi_count": 958,
    "kosdaq_count": 2626
  },
  "pykrx_auto": {
    "enabled": true,
    "active_task": false,
    "last_market_state": "CLOSED",
    "last_market_date": "2026-04-09",
    "last_checked_at": "2026-04-09T09:52:00+00:00",
    "last_started_at": "2026-04-09T09:42:00+00:00",
    "last_completed_at": "2026-04-09T09:51:21+00:00",
    "last_status": "completed",
    "last_reason": "completed_with_errors",
    "last_result": {"triggered": true}
  }
}
```

### Notes
- `job` reflects the primary EOD collection job snapshot.
- `master` reflects symbol-master refresh state.
- `pykrx_auto` reflects automatic scheduler state.
- Use `/eod/freshness` when the consumer wants a compact web-facing freshness summary instead of these nested operational snapshots.

---

## 2.2 `GET /eod/freshness`
Returns UX-friendly EOD and derived-data freshness information.

Canonical semantics for `/eod/freshness` and `/eod/pipeline/status` live in `docs/EOD_STATUS_SEMANTICS_CONTRACT.md`.

### Example request
```http
GET /eod/freshness
Authorization: Bearer <token>
```

### Example response
```json
{
  "today": {
    "trade_date": "2026-04-10",
    "price": {
      "status": "missing",
      "symbols_total": 3584,
      "symbols_succeeded": 0,
      "symbols_failed": 0
    },
    "technical_analysis": {
      "status": "missing",
      "symbols_total": 3584,
      "symbols_succeeded": 0,
      "symbols_failed": 0
    },
    "action_summary": {
      "status": "missing",
      "symbols_total": 3584,
      "symbols_succeeded": 0,
      "symbols_failed": 0
    },
    "fundamentals": {
      "status": "missing",
      "symbols_total": 0,
      "symbols_succeeded": 0,
      "symbols_failed": 0
    }
  },
  "latest": {
    "trade_date": "2026-04-09",
    "price_trade_date": "2026-04-09",
    "eod_status": "completed_with_errors",
    "updated_at": "2026-04-09T07:31:55+00:00",
    "succeeded": 3581,
    "failed": 3,
    "total": 3584,
    "summary": {
      "basis": "latest_eod_run_symbol_universe",
      "updated_at": "2026-04-09T07:31:55+00:00",
      "succeeded": 3581,
      "failed": 3,
      "total": 3584
    },
    "symbols_total": 3584,
    "symbols_succeeded": 3581,
    "symbols_failed": 3,
    "technical_market_date": "2026-04-09",
    "action_market_date": "2026-04-09"
  }
}
```

### Today's stage status meanings
- `ready`: same-day persisted data exists for the stage.
- `missing`: no same-day persisted data exists for the stage.

`latest.eod_status` keeps the legacy EOD run statuses: `running`, `completed`, `completed_with_errors`, and `missing`.

### UX use
This route is the preferred API for expressing:
- whether today’s price, technical-analysis, action-summary, and fundamentals stages have same-day persisted data,
- whether it was partial,
- whether derived layers lag behind price data,
- latest available dates for key data groups.

The `today` block is always scoped to the current Seoul trade date. Price, technical-analysis, and action-summary totals use the active symbol universe; their succeeded counts come from same-day persisted rows for active symbols, and their failed counts represent active symbols still missing that persisted data once the stage has same-day rows. Fundamentals succeeded counts use distinct DART fundamentals snapshot symbols updated during the current Seoul day, and fundamentals failed counts use distinct financial-report symbols with failed capture attempts during that day. When no rows exist for a stage, that stage remains `missing` even if `latest.*` points to a prior available date. Use `latest.symbols_total`, `latest.symbols_succeeded`, `latest.symbols_failed`, and `latest.eod_status` for the run metadata tied to `latest.price_trade_date`. Bar-availability checks for `latest.eod_status` use all stored bars for `latest.price_trade_date`, not only symbols still active in the current universe.

`latest.trade_date`, `latest.updated_at`, `latest.succeeded`, `latest.failed`, and `latest.total` mirror `latest.summary` for easier web consumption. `latest.summary.basis=latest_eod_run_symbol_universe` means those counts come from the selected `eod_collection_runs` row for `latest.trade_date`; if bars exist without a matching run row, `basis=stored_price_bars_for_latest_trade_date` and the fallback uses stored bar counts with `failed=0`.

The current implementation keeps `today.trade_date` on the current Seoul calendar date even on weekends or other break-days; callers should use `latest.*` for previous stored availability instead of expecting `today` to backshift automatically.

---

## 2.3 `GET /eod/technical-snapshots`
Returns stored technical snapshot data.

### Example request
```http
GET /eod/technical-snapshots?market_date=2026-04-09&symbols=005930
Authorization: Bearer <token>
```

### Example response
```json
{
  "market_date": "2026-04-09",
  "items": [
    {
      "symbol": "005930",
      "trend_state": "range",
      "ma_alignment": "mixed",
      "breakout_20d": true,
      "breakdown_20d": false,
      "return_5d": 11.0232,
      "return_20d": 10.7895,
      "volume_ratio_20d": 1.3315,
      "atr_pct_14": 5.436
    }
  ]
}
```

### Important nullability
Some fields can be null for legitimate historical reasons.
Examples:
- `return_5d` needs enough lookback,
- `return_20d` needs enough lookback,
- `volume_ratio_20d` needs enough lookback,
- `atr_pct_14` needs enough lookback.

Nulls in these fields do not automatically mean ingestion failure.

---

## 2.4 `GET /eod/action-summaries`
Returns stored action-summary data derived from technical snapshots.

### Example request
```http
GET /eod/action-summaries?market_date=2026-04-09&symbols=005930
Authorization: Bearer <token>
```

### Example response
```json
{
  "market_date": "2026-04-09",
  "items": [
    {
      "symbol": "005930",
      "action_label": "watch_long",
      "signal_score": 40,
      "confidence_tier": "medium",
      "risk_tier": "medium",
      "reasons": [
        "20-day breakout detected",
        "MA alignment is mixed"
      ]
    }
  ]
}
```

### Notes
- Action summaries are deterministic derived summaries, not free-form LLM output.
- Good for ranking/display layers, but consumers should still preserve nuance.

---

## 2.5 `GET /eod/signal-features`
Returns stored daily KIS ranking signal features plus owned-data fallback coverage metadata.

### Example request
```http
GET /eod/signal-features?market_date=2026-04-14&symbols=005930
Authorization: Bearer <token>
```

### Example response
```json
{
  "market_date": "2026-04-14",
  "summary": {
    "entries": 1,
    "entries_for_market_date": 76,
    "requested_symbols": ["005930"],
    "matched_symbols": ["005930"],
    "missing_symbols": [],
    "coverage": {
      "market_date": "2026-04-14",
      "persisted_row_count": 76,
      "direct_signal_source_row_count": 12,
      "fallback_only_row_count": 64,
      "row_kind_counts": {
        "direct_plus_fallback": 12,
        "fallback_only": 64
      },
      "active_universe": {
        "label": "active_daily_bar_universe",
        "availability": "available",
        "symbol_count": 76,
        "present_rows": 76,
        "missing_rows": 0,
        "coverage_ratio": 1.0,
        "coverage_status": "complete",
        "missing_symbols_preview": []
      },
      "candidate_universe": {
        "label": "agent_feedback_candidate_universe",
        "availability": "available",
        "symbol_count": 18,
        "present_rows": 18,
        "missing_rows": 0,
        "coverage_ratio": 1.0,
        "coverage_status": "complete",
        "missing_symbols_preview": []
      },
      "requested_scope": {
        "label": "requested_symbol_scope",
        "availability": "available",
        "symbol_count": 1,
        "present_rows": 1,
        "missing_rows": 0,
        "coverage_ratio": 1.0,
        "coverage_status": "complete",
        "missing_symbols_preview": []
      }
    }
  },
  "items": [
    {
      "symbol": "005930",
      "name_ko": "삼성전자",
      "market": "KOSPI",
      "market_date": "2026-04-14",
      "source": "kis_ranking",
      "signal_count": 2,
      "volume_power_rank": 3,
      "volume_power": 140.5,
      "overtime_volume_rank": 7,
      "overtime_volume": 120000,
      "overtime_fluctuation_rank": null,
      "overtime_fluctuation_rate": null,
      "row_kind": "direct_plus_fallback",
      "provenance": {
        "row_kind": "direct_plus_fallback",
        "direct_signal_sources": ["overtime-volume", "volume-power"],
        "direct_signal_source_count": 2,
        "fallback_groups_present": [
          "price_action",
          "liquidity",
          "returns",
          "volatility",
          "factor_scores",
          "quote_snapshot"
        ],
        "active_universe_member": true,
        "candidate_universe_member": true
      },
      "freshness": {
        "market_date": "2026-04-14",
        "direct_sources_collected_at": "2026-04-14T15:42:10+00:00",
        "price_bars": {
          "trade_date": "2026-04-14",
          "trade_date_relation_to_market_date": "same_market_date"
        },
        "quote_snapshot": {
          "trade_date": "2026-04-14",
          "trade_date_relation_to_market_date": "same_market_date"
        },
        "factor_scores": {
          "families_present": ["quality", "momentum", "low_risk"],
          "latest_captured_at": "2026-04-14T15:31:00+00:00"
        }
      },
      "fallback_features": {
        "price_action": {
          "close_return_1d_pct": 1.287553,
          "candle_direction": "bullish",
          "close_location_label": "near_high"
        },
        "liquidity": {
          "turnover_source": "provider",
          "turnover_is_estimated": false
        },
        "returns": {
          "close_return_1d_pct": 1.287553,
          "return_5d": 4.921771
        },
        "quote_snapshot": {
          "available": true,
          "source": "kis",
          "fallback": false
        }
      }
    }
  ]
}
```

### Notes
- Stored rows are keyed by symbol and market date.
- The current persisted sources are `volume-power`, `overtime-volume`, and `overtime-fluctuation`.
- `POST /eod/signal-features/collect` is the operator path that calls KIS when available, expands the row set across the active daily-bar universe and candidate universe, and upserts the merged rows.
- Each stored row can be `direct_only`, `direct_plus_fallback`, or `fallback_only`.
- `fallback_features` come from already-owned bars, persisted factor rows, and cached quote data. They are contextual fallback values, not replacement live ranking truth.
- The collect response also returns `source_errors`, `coverage`, `items_total`, and `items_returned`.
- The same collect path also refreshes the persisted market-wide participation block used by the EOD summarizer payload, so participation context is no longer derived from signal rows during payload build time.

---

# 3. DART APIs

## 3.1 `GET /disclosures/dart/fundamentals/history`
Returns stored DART fundamentals history for one or more symbols.

### Example request
```http
GET /disclosures/dart/fundamentals/history?symbols=071050&start_date=2024-01-01&end_date=2026-04-09&fs_div=CFS
Authorization: Bearer <token>
```

### Example response
```json
{
  "request": {
    "symbols": ["071050"],
    "start_date": "2024-01-01",
    "end_date": "2026-04-09",
    "fs_div": "CFS"
  },
  "companies": [
    {
      "symbol": "071050",
      "name": "한국금융지주우",
      "entries": [
        {
          "filing": {
            "rcept_no": "20260319001006",
            "report_nm": "사업보고서 (2025.12)",
            "receipt_date": "2026-03-19"
          },
          "period": {
            "business_year": 2025,
            "report_code": "11011"
          },
          "statement": {
            "fs_div": "CFS"
          },
          "highlights": [
            {
              "metric_key": "revenue",
              "label": "매출액",
              "value": 123456789
            }
          ],
          "statements": [
            {
              "account_name": "매출액",
              "current_amount": 123456789
            }
          ]
        }
      ]
    }
  ]
}
```

### Notes
- This is stored-history access, not a live raw DART passthrough.
- Coverage may be partial by symbol, filing, and period.

---

## 3.2 `GET /disclosures/dart/filings/document`
Returns filing package/binary content for a DART filing.

### Example request
```http
GET /disclosures/dart/filings/document?rcept_no=20260319001006
Authorization: Bearer <token>
```

### Notes
- Intended for filing retrieval/download flows.
- Binary success and error envelope handling are explicitly guarded in current implementation.

---

## 3.3 `GET /disclosures/dart/filings/xbrl`
Returns XBRL-related filing content.

### Example request
```http
GET /disclosures/dart/filings/xbrl?rcept_no=20260319001006&report_code=11011
Authorization: Bearer <token>
```

### Notes
- Intended for consumers that want structured filing package access.

---

# 4. Aggregate Context API

## 4.1 `GET /agent-context/symbol-day`
Returns a symbol/day aggregate payload combining multiple stored datasets.

### Example request
```http
GET /agent-context/symbol-day?market_date=2026-04-09&symbols=005930&fs_div=CFS&disclosures_limit=5
Authorization: Bearer <token>
```

### Example response
```json
{
  "request": {
    "market_date": "2026-04-09",
    "symbols": ["005930"],
    "fs_div": "CFS",
    "fs_label": "consolidated",
    "disclosures_limit": 5
  },
  "summary": {
    "requested_symbol_count": 1,
    "matched_symbol_count": 1,
    "quote_context_count": 1,
    "missing_symbols": [],
    "price_context_count": 1,
    "technical_snapshot_count": 1,
    "action_summary_count": 1,
    "signal_feature_count": 1,
    "factor_row_counts": {
      "unified_value": 1,
      "quality": 1,
      "momentum": 1,
      "advanced_value": 1,
      "low_risk": 1,
      "beta": 1,
      "composite": 1
    },
    "fundamentals_count": 0,
    "recent_disclosure_symbol_count": 0,
    "context_completeness": {
      "signal_features": {
        "present_rows": 1,
        "missing_rows": 0,
        "coverage_status": "complete"
      }
    }
  },
  "items": [
    {
      "symbol": "005930",
      "market_date": "2026-04-09",
      "company": {
        "symbol": "005930",
        "name_ko": "삼성전자",
        "market": "KOSPI",
        "instrument_type": "EQUITY",
        "dart": {
          "corp_code": null,
          "corp_name": null
        }
      },
      "quote_context": {
        "available": true,
        "source": "kis",
        "market_state": "OPEN",
        "price": 71550,
        "trade_date": "2026-04-09",
        "cached_at": "2026-04-09T09:57:00+09:00",
        "freshness": {
          "trade_date_relation_to_request": "same_market_date",
          "same_market_date_as_request": true
        },
        "spread": {
          "available": true,
          "bid_price": 71500,
          "ask_price": 71600,
          "spread_points": 100,
          "spread_bps_of_price": 13.9762
        },
        "freshness_provenance": {
          "freshness_status": "fresh",
          "provenance_status": "provider_sourced",
          "issue_types": [],
          "requested_market_date": "2026-04-09",
          "as_of_market_date": "2026-04-09",
          "latest_available_market_date": "2026-04-09",
          "source_tables": ["current_quote_cache", "quote_snapshots"],
          "source_provider": "kis"
        }
      },
      "price_context": {
        "trade_date": "2026-04-09",
        "source": "pykrx",
        "open_price": 70000,
        "high_price": 72000,
        "low_price": 69500,
        "close_price": 71500,
        "liquidity": {
          "turnover_krw": 8800000000,
          "turnover_source": "provider",
          "turnover_is_estimated": false
        },
        "price_bar_quality": {
          "open_price_zero": false,
          "turnover_missing_or_zero": false,
          "gap_unreliable_due_to_open": false,
          "issue_codes": []
        },
        "candle": {
          "direction": "bullish",
          "body_points": 1500,
          "body_percent_of_open": 2.1429,
          "body_percent_of_range": 60.0,
          "body_size": "large",
          "upper_wick_points": 500,
          "lower_wick_points": 500,
          "wick_bias": "balanced"
        },
        "session_range": {
          "points": 2500,
          "percent_of_open": 3.5714,
          "close_position_in_range": 0.8,
          "close_position_label": "near_high"
        },
        "gap": {
          "previous_trade_date": "2026-04-08",
          "previous_close_price": 69800,
          "gap_points": 200,
          "gap_percent": 0.2865,
          "direction": "up",
          "filled_intraday": true,
          "reliable": true
        },
        "freshness_provenance": {
          "freshness_status": "fresh",
          "provenance_status": "derived_from_provider_data",
          "issue_types": [],
          "requested_market_date": "2026-04-09",
          "as_of_market_date": "2026-04-09",
          "latest_available_market_date": "2026-04-09",
          "source_tables": ["daily_price_contexts", "daily_price_bars"],
          "source_provider": "pykrx"
        }
      },
      "price_context_history": [
        {
          "market_date": "2026-04-08",
          "source": "pykrx",
          "captured_at": "2026-04-08T15:30:00+09:00",
          "price_context": {
            "trade_date": "2026-04-08",
            "close_price": 69800,
            "candle": {"direction": "bullish"}
          }
        },
        {
          "market_date": "2026-04-09",
          "source": "pykrx",
          "captured_at": "2026-04-09T15:30:00+09:00",
          "price_context": {
            "trade_date": "2026-04-09",
            "close_price": 71500,
            "candle": {"direction": "bullish"}
          }
        }
      ],
      "technical_snapshot": {
        "symbol": "005930",
        "market_date": "2026-04-09",
        "trend_state": "range",
        "ma_alignment": "mixed",
        "breakout_20d": true,
        "return_5d": 11.0232,
        "return_20d": 10.7895,
        "volume_ratio_20d": 1.3315,
        "atr_pct_14": 5.436
      },
      "action_summary": {
        "symbol": "005930",
        "market_date": "2026-04-09",
        "action_label": "watch_long",
        "signal_score": 40,
        "confidence_tier": "medium",
        "risk_tier": "medium"
      },
      "signal_features": {
        "symbol": "005930",
        "market_date": "2026-04-09",
        "source": "kis_ranking",
        "signal_count": 2,
        "volume_power_rank": 3,
        "overtime_volume_rank": 7,
        "row_kind": "direct_plus_fallback",
        "provenance": {
          "row_kind": "direct_plus_fallback",
          "direct_signal_source_count": 2,
          "fallback_groups_present": [
            "price_action",
            "liquidity",
            "returns",
            "volatility",
            "factor_scores",
            "quote_snapshot"
          ]
        },
        "freshness": {
          "market_date": "2026-04-09",
          "price_bars": {
            "trade_date": "2026-04-09",
            "trade_date_relation_to_market_date": "same_market_date"
          },
          "quote_snapshot": {
            "trade_date": "2026-04-09",
            "trade_date_relation_to_market_date": "same_market_date"
          }
        },
        "fallback_features": {
          "price_action": {
            "close_return_1d_pct": 2.43553,
            "candle_direction": "bullish",
            "close_location_label": "near_high"
          },
          "factor_scores": {
            "quality": {
              "score": 0.741,
              "status": "partial"
            },
            "momentum": {
              "score": 0.914
            }
          }
        },
        "freshness_provenance": {
          "freshness_status": "fresh",
          "provenance_status": "provider_plus_fallback",
          "issue_types": ["derived_or_fallback"],
          "requested_market_date": "2026-04-09",
          "as_of_market_date": "2026-04-09",
          "latest_available_market_date": "2026-04-09",
          "source_tables": ["daily_signal_features", "daily_price_bars", "current_quote_cache"],
          "source_provider": "kis_ranking"
        }
      },
      "market_relative_context": {
        "status": "available",
        "market": "KOSPI",
        "breadth": {
          "up_count": 470,
          "down_count": 260,
          "flat_count": 80
        },
        "relative_strength": {
          "return_percentile": 0.95,
          "volume_percentile": 0.93
        }
      },
      "persisted_factor_rows": {
        "advanced_value": {
          "advanced_value_score": 0.81,
          "advanced_value_classification": "eligible",
          "freshness_provenance": {
            "freshness_status": "fresh",
            "provenance_status": "derived_from_provider_data",
            "issue_types": [],
            "requested_market_date": "2026-04-09",
            "as_of_market_date": "2026-04-09",
            "latest_available_market_date": "2026-04-09",
            "source_tables": ["daily_advanced_value_factor_results"],
            "source_provider": "internal_factor_pipeline"
          }
        },
        "composite": {
          "composite_score": 0.73,
          "composite_classification": "eligible"
        }
      },
      "fundamentals": {
        "latest_snapshot": null,
        "history_summary": {
          "entries": 0,
          "latest_receipt_date": null,
          "latest_period_end_date": null,
          "available_business_years": [],
          "recent_periods": []
        },
        "freshness_provenance": {
          "freshness_status": "missing",
          "provenance_status": "provider_sourced",
          "issue_types": ["missing"],
          "requested_market_date": "2026-04-09",
          "as_of_market_date": null,
          "latest_available_market_date": null,
          "source_tables": ["dart_fundamentals_snapshots"],
          "source_provider": "dart"
        }
      },
      "recent_disclosures": {
        "count": 0,
        "items": [],
        "freshness_provenance": {
          "freshness_status": "missing",
          "provenance_status": "provider_sourced",
          "issue_types": ["missing"],
          "requested_market_date": "2026-04-09",
          "as_of_market_date": null,
          "latest_available_market_date": null,
          "source_tables": ["dart_disclosure_events"],
          "source_provider": "dart"
        }
      },
      "availability": {
        "company": true,
        "quote_context": true,
        "price_context": true,
        "technical_snapshot": true,
        "action_summary": true,
        "signal_features": true,
        "market_relative_context": true,
        "persisted_factor_rows": true,
        "fundamentals": false,
        "recent_disclosures": false
      },
      "context_completeness": {
        "factor_rows": {
          "quality": {
            "row_status": "present",
            "null_submetric_fields": ["accruals_ratio"]
          }
        },
        "signal_features": {
          "row_status": "present",
          "row_kind": "direct_plus_fallback",
          "direct_signal_source_count": 2,
          "fallback_groups_present": [
            "price_action",
            "liquidity",
            "returns",
            "volatility",
            "factor_scores",
            "quote_snapshot"
          ]
        },
        "sector_peer_mapping": {"status": "unavailable"}
      },
      "context_provenance": {
        "quote_context": {
          "freshness_status": "fresh",
          "provenance_status": "provider_sourced",
          "issue_types": []
        },
        "price_context": {
          "freshness_status": "fresh",
          "provenance_status": "derived_from_provider_data",
          "issue_types": []
        },
        "signal_features": {
          "freshness_status": "fresh",
          "provenance_status": "provider_plus_fallback",
          "issue_types": ["derived_or_fallback"],
          "requested_symbol_coverage": {
            "present_rows": 1,
            "missing_rows": 0,
            "coverage_status": "complete"
          }
        },
        "fundamentals": {
          "freshness_status": "missing",
          "provenance_status": "provider_sourced",
          "issue_types": ["missing"]
        },
        "recent_disclosures": {
          "freshness_status": "missing",
          "provenance_status": "provider_sourced",
          "issue_types": ["missing"]
        },
        "factor_rows": {
          "advanced_value": {
            "freshness_status": "fresh",
            "provenance_status": "derived_from_provider_data",
            "issue_types": []
          }
        }
      }
    }
  ]
}
```

### Semantics
This is not a raw DB dump.
It is a composed read payload intended for:
- web symbol-detail surfaces,
- agent reasoning,
- summary card generation,
- low-roundtrip consumers.

`quote_context` is now always a structured object. When `current_quote_cache` has no requested-date row, the payload returns `status="unavailable"`, `reason`, missing freshness metadata, and an unavailable `spread` sub-object instead of a raw `null`.
`price_context` is read from persisted `daily_price_contexts`, which the EOD pipeline derives from stored `daily_price_bars` for the same market date. It now includes derived liquidity (`turnover_source`, provider vs estimated turnover) plus `price_bar_quality` flags for zero opens, zero turnover, and unreliable gap semantics. `price_context_history` carries up to the latest seven persisted rows ending at the requested `market_date`.
`signal_features` may be a direct KIS row, a direct-plus-fallback row, or a fallback-only row assembled from owned data. Fallback values are intentionally marked so downstream agents do not treat them as live external ranking truth.
`intraday_execution_context` and the provider-backed optional PyKRX blocks (`market_relative_context`, `sector_peer_context`, `market_breadth_context`, `investor_flow_context`, `short_selling_context`, and `valuation_context`) also stay object-shaped when requested-date data is missing. Missingness is represented with explicit `status`, `reason`, nested unavailable sub-blocks where relevant, and `freshness_provenance`.
Major blocks now also expose `freshness_provenance`, and the top-level `context_provenance` mirror remains available alongside the current block payload. Consumers should interpret `stale` and `available_but_old` as "stored evidence exists but predates the requested market date", not as a missing-data case.
`persisted_factor_rows` keeps compact snapshots of stored factor families, `market_relative_context` derives same-market breadth and return/volume percentiles from stored `daily_price_bars`, and `context_completeness` separates present rows, genuinely missing rows, null submetrics, sparse signal-feature coverage, and the currently unavailable sector/industry peer-mapping lane.

### Important availability semantics
Expected decider-facing context blocks are status-first objects, not raw `null`, even when requested-date data is missing.
For example, a company may have:
- technical snapshot present,
- action summary present,
- `quote_context.status = "unavailable"`,
- `intraday_execution_context.status = "unavailable"`,
- `market_relative_context.status = "unavailable"`,
- `signal_features` absent,
- `fundamentals.latest_snapshot` absent,
- `recent_disclosures.items` empty.

Clients must not assume every nested fact field is populated just because the wrapper object exists. Use each block's `status`, `reason`, `missing_fields`, and `freshness_provenance` before interpreting absent inner values.

---

## 4.2 `POST /agent-context/feedback`
Stores JSON feedback from a downstream agent after it consumes Core context.

### Example request
```http
POST /agent-context/feedback
Authorization: Bearer <token>
Content-Type: application/json
```

```json
{
  "market_date": "2026-04-14",
  "symbol": "005930",
  "agent_name": "openclaw",
  "agent_version": "daily-v1",
  "run_id": "openclaw-20260414-001",
  "summary": {
    "headline": "watch breakout continuation",
    "thesis": "stored technical and signal context improved after EOD"
  },
  "feedback": {
    "confidence": 0.72,
    "risk_notes": ["earnings volatility", "market gap risk"]
  },
  "context": {
    "source_route": "/agent-context/symbol-day",
    "market_date": "2026-04-14",
    "symbol_day_context": {
      "context_provenance": {
        "quote_context": {
          "freshness_status": "stale",
          "provenance_status": "provider_sourced",
          "issue_types": ["stale"],
          "source_tables": ["current_quote_cache", "quote_snapshots"]
        }
      }
    }
  }
}
```

### Example response
```json
{
  "accepted": true,
  "item": {
    "id": 11,
    "market_date": "2026-04-14",
    "symbol": "005930",
    "agent": {
      "name": "openclaw",
      "version": "daily-v1",
      "run_id": "openclaw-20260414-001"
    },
    "summary": {
      "headline": "watch breakout continuation",
      "thesis": "stored technical and signal context improved after EOD"
    },
    "feedback": {
      "confidence": 0.72,
      "risk_notes": ["earnings volatility", "market gap risk"]
    },
    "context": {
      "source_route": "/agent-context/symbol-day",
      "market_date": "2026-04-14"
    },
    "received_at": "2026-04-14T15:00:00+00:00"
  },
  "summary": {
    "summary_keys": ["headline", "thesis"],
    "feedback_keys": ["confidence", "risk_notes"]
  }
}
```

### Notes
- `summary` and `feedback` are JSON objects; at least one must be non-empty.
- Stored feedback is append-only and separate from deterministic action summaries.
- When `context` includes symbol-day `context_provenance`, missing-data normalization can enrich stored categories with provenance-aware issue typing such as `missing`, `stale`, `available_but_old`, and `derived_or_fallback`.
- This route does not trigger orders, positions, or live trading.

---

## 4.3 `GET /agent-context/feedback`
Reads stored downstream-agent feedback for a market date.

### Example request
```http
GET /agent-context/feedback?market_date=2026-04-14&symbols=005930&agent_name=openclaw&limit=50
Authorization: Bearer <token>
```

### Notes
- `symbols` accepts repeated query parameters or comma-separated values.
- Results are ordered newest first by receipt time.
- This route is intended for operator inspection and later post-EOD orchestration jobs.

---

## 4.4 `GET /agent-context/feedback/decider-coverage`
Builds a daily operator report from persisted decider request payloads so dashboards and ops checks can inspect what the deciders actually received without reading raw prompt payloads.

### Example request
```http
GET /agent-context/feedback/decider-coverage?market_date=2026-05-08&agent_names=signal-decider&agent_names=short-term-decider
Authorization: Bearer <token>
```

### Example response
```json
{
  "request": {
    "market_date": "2026-05-08",
    "run_id": null,
    "symbols": null,
    "agent_names": ["short-term-decider", "signal-decider"]
  },
  "summary": {
    "symbols_evaluated": 111,
    "core_blocks": {
      "counts": {
        "available": 82,
        "missing": 19,
        "stale": 7,
        "derived": 3,
        "provider_error": 0
      }
    },
    "optional_blocks": {
      "counts": {
        "available": 403,
        "empty": 28,
        "unavailable": 51,
        "provider_error": 2
      }
    },
    "contract_regressions": {
      "status": "alert",
      "raw_null_count": 4,
      "unexpected_missing_core_block_count": 19
    }
  },
  "per_agent": {
    "signal-decider": {
      "feedback_rows": 111
    },
    "short-term-decider": {
      "feedback_rows": 111
    }
  }
}
```

### Notes
- `run_id` is optional and limits the report to rows that still carry that persisted feedback run id.
- `symbols` accepts repeated query parameters or comma-separated values for focused inspection.
- Core block counts are rolled up to `available`, `missing`, `stale`, `derived`, and `provider_error`; optional block counts are rolled up to `available`, `empty`, `unavailable`, and `provider_error`.
- The report also surfaces gap-effect buckets (`decision_affected`, `confidence_reducer`, `neutral_auxiliary`) plus raw-null contract regressions for expected object-style decider context paths.

---

# 5. Execution Handoff / Runtime Contract

Core's execution handoff boundary is versioned in code at
[`apps/api/app/orders/execution_handoff_contract.py`](../apps/api/app/orders/execution_handoff_contract.py).
That module is the source of truth for serialized dispatch payloads, dashboard runtime read models, status literals, today-activity rows, and the current portfolio-risk equivalent.

## Ownership
Core owns strategy/read-model work:
- persisted signal-decider and short-term-decider planning rows,
- core-to-exec dispatch payload construction,
- cached dashboard read models under `/execution/runtime/*`,
- the current portfolio summary equivalent exposed by `/portfolio/summary`.

`kai-trading-exec` owns execution truth:
- accepted execution plans,
- order, execution, and position readback,
- runtime liveness,
- pause and kill-switch state.

Web is a display-only consumer:
- read Core's `/execution/runtime/*`, `/trading/orders/exec/*`, and `/portfolio/summary` payloads,
- render explicit Core statuses instead of inferring execution state from labels,
- do not submit orders or create new execution commands from these read models.

## Contract surfaces
- Dispatch payloads use `signal_decider_exec_plan_v2` in `metadata.schema_version` and per-item `risk_context.schema_version`.
- Exec-agent handoff context uses `exec_agent_decider_handoff_v1`.
- Dashboard read models use `execution_runtime_dashboard_read_model_v1` in `freshness.schema_version`.
- Today buy/sell facts use `execution_runtime_today_activity_v1`.
- Runtime status values are `ready`, `degraded`, `offline`, `paused`, and `kill_switched`.
- Daily-board row statuses are `planned`, `accepted`, `scheduled`, `submitted`, `partial`, `filled`, `cancelled`, and `failed`.
- Monitored-symbol states are `watching_target_stop`, `watching_fill`, `reconciling`, and `idle`.
- Freshness states are `fresh`, `stale`, `partial`, `refreshing`, and `unavailable`.

## Current portfolio-risk equivalent
Core does not expose a separate portfolio-risk snapshot endpoint today. The currently owned equivalent is:
- `/portfolio/summary` with `source = "kis_portfolio_summary_v1"`, `sync.status`, account summaries, holdings, allocation, recent activity, and notices,
- execution-runtime row fields such as `held_position`, `position_quantity`, `position_average_price`, `open_quantity`, `today_activity`, and `remote_state`.

Consumers should treat this as a read model, not as executable risk authority. Exec remains the execution-truth owner for live position/order state.

# 6. Consumer Notes for UX / Web / Agent Layers

## 6.1 Freshness is not globally uniform
Do not assume:
- price latest date,
- technical latest date,
- action latest date,
- DART latest date
all match.

Use `/eod/freshness` and section-specific timestamps/dates where possible.

## 6.2 Partial coverage is normal
This system legitimately produces partial coverage in cases such as:
- sparse DART history,
- DART `[013]` no-data filings, which are stored as terminal `no_data` fundamentals capture state instead of retryable failures,
- insufficient lookback for technical fields,
- unavailable quote states.

## 6.3 Unavailable does not mean zero
If a quote or subsection is unavailable:
- preserve that state explicitly,
- do not fabricate a numeric value,
- do not silently degrade to misleading UI.

## 6.4 Symbol detail is a composite screen
Symbol-detail style consumers should think in layered modules:
- quote,
- market status,
- price history,
- technical snapshot,
- action summary,
- signal features,
- fundamentals summary,
- disclosures.

These modules may differ in freshness and completeness.

---

# 7. Recommended Core APIs for UX/GUI Planning

If a UX or frontend team needs to plan against the current system, the most important core APIs are:
- `/market/status`
- `/quotes/{symbol}`
- `/quotes/tracked`
- `/portfolio/summary`
- `/execution/runtime/status`
- `/execution/runtime/monitored-symbols`
- `/execution/runtime/daily-board`
- `/execution/runtime/symbols/{symbol}`
- `/scanner/tracked-symbols`
- `/symbols/search`
- `/symbols/{symbol}/price-bars`
- `/eod/freshness`
- `/eod/technical-snapshots`
- `/eod/action-summaries`
- `/eod/signal-features`
- `/agent-context/symbol-day`
- `/disclosures/dart/fundamentals/history`

For `/execution/runtime/status`, Core preserves exec-owned summary dictionaries such as `symbols`, `daily_pnl`, `weekly_pnl`, `monthly_pnl`, and `actionable_alerts` so the dashboard can display sell-fill coverage and FIFO P/L without re-deriving execution truth in Core.

The default `/execution/runtime/status` read is local/cache-only. Use `refresh=true` only for an explicit operator refresh that should contact exec runtime.

These cover most currently meaningful user-facing flows.

---

# 8. Summary

`kai-trading-core` currently provides a layered API system covering:
- market state,
- single-symbol and tracked-universe quotes,
- symbol search,
- historical price bars,
- EOD status and freshness,
- technical snapshots,
- action summaries,
- DART fundamentals/disclosure history,
- aggregated symbol/day context.

The key design reality for consumers is that:
- freshness differs by data layer,
- nullability is sometimes expected and correct,
- DART coverage is uneven,
- aggregate consumers should treat sections as independently optional, including portfolio account subsections that may degrade independently under upstream KIS failures.
