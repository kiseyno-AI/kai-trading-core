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

## 1.2 `GET /quotes/{symbol}`
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

### UX guidance
If `available` is false:
- do not render fake price values,
- show an explicit unavailable state.

---

## 1.3 `GET /quotes/tracked`
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

## 1.4 `GET /scanner/tracked-symbols`
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

## 1.5 `GET /symbols/search`
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
    "price_trade_date": "2026-04-09",
    "eod_status": "completed_with_errors",
    "symbols_total": 3584,
    "symbols_succeeded": 3581,
    "symbols_failed": 3,
    "technical_market_date": "2026-04-09",
    "action_market_date": "2026-04-09"
  }
}
```

### Today's stage status meanings
- `running`
- `ready`
- `missing`

`latest.eod_status` keeps the legacy EOD run statuses: `running`, `completed`, `completed_with_errors`, and `missing`.

### UX use
This route is the preferred API for expressing:
- whether today’s price, technical-analysis, action-summary, and fundamentals stages are ready or running,
- whether it was partial,
- whether derived layers lag behind price data,
- latest available dates for key data groups.

The `today` block is always scoped to the current Seoul trade date. Price, technical-analysis, and action-summary totals use the active symbol universe; fundamentals totals use the number of symbols refreshed by today's DART daily fundamentals run. When no run or rows exist for a stage, that stage remains `missing` even if `latest.*` points to a prior available date. Use `latest.symbols_total`, `latest.symbols_succeeded`, `latest.symbols_failed`, and `latest.eod_status` for the run metadata tied to `latest.price_trade_date`.

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
Returns stored daily KIS ranking signal features.

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
    "requested_symbols": ["005930"],
    "matched_symbols": ["005930"],
    "missing_symbols": []
  },
  "items": [
    {
      "symbol": "005930",
      "source": "kis_ranking",
      "signal_count": 2,
      "volume_power_rank": 3,
      "volume_power": 140.5,
      "overtime_volume_rank": 7,
      "overtime_volume": 120000,
      "overtime_fluctuation_rank": null,
      "overtime_fluctuation_rate": null
    }
  ]
}
```

### Notes
- Stored rows are keyed by symbol and market date.
- The current persisted sources are `volume-power`, `overtime-volume`, and `overtime-fluctuation`.
- `POST /eod/signal-features/collect` is the operator path that calls KIS and upserts these rows.

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
    "missing_symbols": [],
    "technical_snapshot_count": 1,
    "action_summary_count": 1,
    "signal_feature_count": 1,
    "fundamentals_count": 0,
    "recent_disclosure_symbol_count": 0
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
        "overtime_volume_rank": 7
      },
      "fundamentals": {
        "latest_snapshot": null,
        "history_summary": {
          "entries": 0,
          "latest_receipt_date": null,
          "latest_period_end_date": null,
          "available_business_years": [],
          "recent_periods": []
        }
      },
      "recent_disclosures": {
        "count": 0,
        "items": []
      },
      "availability": {
        "company": true,
        "technical_snapshot": true,
        "action_summary": true,
        "signal_features": true,
        "fundamentals": false,
        "recent_disclosures": false
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

### Important nullability
Each subsection can be absent independently.
For example, a company may have:
- technical snapshot present,
- action summary present,
- `signal_features` absent,
- `fundamentals.latest_snapshot` absent,
- `recent_disclosures.items` empty.

Clients must not assume all sub-sections are always populated.

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
    "market_date": "2026-04-14"
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

# 5. Consumer Notes for UX / Web / Agent Layers

## 5.1 Freshness is not globally uniform
Do not assume:
- price latest date,
- technical latest date,
- action latest date,
- DART latest date
all match.

Use `/eod/freshness` and section-specific timestamps/dates where possible.

## 5.2 Partial coverage is normal
This system legitimately produces partial coverage in cases such as:
- sparse DART history,
- DART `[013]` no-data filings, which are stored as terminal `no_data` fundamentals capture state instead of retryable failures,
- insufficient lookback for technical fields,
- unavailable quote states.

## 5.3 Unavailable does not mean zero
If a quote or subsection is unavailable:
- preserve that state explicitly,
- do not fabricate a numeric value,
- do not silently degrade to misleading UI.

## 5.4 Symbol detail is a composite screen
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

# 6. Recommended Core APIs for UX/GUI Planning

If a UX or frontend team needs to plan against the current system, the most important core APIs are:
- `/market/status`
- `/quotes/{symbol}`
- `/quotes/tracked`
- `/scanner/tracked-symbols`
- `/symbols/search`
- `/symbols/{symbol}/price-bars`
- `/eod/freshness`
- `/eod/technical-snapshots`
- `/eod/action-summaries`
- `/eod/signal-features`
- `/agent-context/symbol-day`
- `/disclosures/dart/fundamentals/history`

These cover most currently meaningful user-facing flows.

---

# 7. Summary

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
- aggregate consumers should treat sections as independently optional.
