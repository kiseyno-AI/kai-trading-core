# POST /agent-context/feedback/retry-cycles

## Summary
Runs bounded retry passes over incomplete symbol-level agent feedback.

The retry universe starts with the persisted active-pre-filter intersection for the requested `market_date`, derived from symbols whose `daily_market_risk_filter_results.passed_filter` and `daily_piotroski_filter_results.passed_filter` rows are both `true`. Core then applies the broad feedback cap before it builds role-specific dispatch scopes.

Callers can optionally provide `symbols` to restrict the real daily retry cycle to one explicit symbol or a small explicit symbol list. Requested-symbol dispatch bypasses automatic broad and role caps, records `requested_symbol_scope_cap_bypass` diagnostics, still runs the same summarizer and decider sequence, persists through the same durable tables, and keeps the run inspectable through the normal progress/latest/read surfaces. Requested symbols outside the active-pre-filter intersection are reported as `skipped_symbols`.

Default automatic caps are broad top 10%, minimum 150, maximum 200 symbols, then role caps of 200 EOD, 150 technical, 120 fundamental, 80 short-term-decider, and 80 signal-decider symbols. Forced-inclusion categories, when backed by local data sources, are reported under `funnel_diagnostics.role_diagnostics[role].forced_inclusion_reasons_by_symbol`.

The response includes cycle and batch details plus a fresh `progress` snapshot using the same response shape as `GET /agent-context/feedback/progress`. Consumers can read `progress.summary.first_feedback_at`, `elapsed_seconds`, rate fields, and ETA fields immediately after the retry request completes.
Retry cycles now also enforce the same-day feedback prerequisites by default. If PyKRX enrichment, fundamentals, filtering, or factor/scoring stages are still pending or blocked, the route returns a gated `waiting` or `blocked` response with a `prerequisites` block and does not create a new durable run. Operators can bypass that safety check only with `force=true`.

## Example request
```http
POST /agent-context/feedback/retry-cycles
Content-Type: application/json

{
  "market_date": "2026-04-16",
  "symbols": null,
  "roles": [
    "eod-summarizer",
    "technical-summarizer",
    "fundamental-summarizer",
    "short-term-decider",
    "signal-decider"
  ],
  "max_cycles": 3,
  "outer_batch_size": 20,
  "reuse_existing": true,
  "force": false
}
```

## Example response
```json
{
  "market_date": "2026-04-16",
  "requested_symbols": null,
  "eligible_symbols": ["005930", "000660"],
  "skipped_symbols": [],
  "roles": [
    "eod-summarizer",
    "technical-summarizer",
    "fundamental-summarizer",
    "short-term-decider",
    "signal-decider"
  ],
  "max_cycles": 3,
  "outer_batch_size": 20,
  "reuse_existing": true,
  "cycles": [
    {
      "cycle": 1,
      "status": "partial",
      "start_pending": 84,
      "end_pending": 12,
      "batches": []
    }
  ],
  "final_pending": 12,
  "completed": false,
  "progress": {
    "request": {
      "market_date": "2026-04-16",
      "symbols": null,
      "expected_agents": [
        "eod-summarizer",
        "technical-summarizer",
        "fundamental-summarizer",
        "short-term-decider",
        "signal-decider"
      ]
    },
    "summary": {
      "run_status": "running",
      "is_live": true,
      "first_feedback_at": "2026-04-16T01:00:00+00:00",
      "latest_feedback_at": "2026-04-16T01:05:00+00:00",
      "elapsed_seconds": 300,
      "total_symbols": 200,
      "broad_symbol_count": 200,
      "role_symbol_counts": {
        "eod-summarizer": 200,
        "technical-summarizer": 150,
        "fundamental-summarizer": 120,
        "short-term-decider": 80,
        "signal-decider": 80
      },
      "processed_symbols": 72,
      "completed_symbols": 70,
      "in_progress_symbols": 2,
      "pending_symbols": 12,
      "progress_percent": 85.71,
      "completed_feedback_count": 210,
      "expected_feedback_count": 630,
      "completed_feedback_per_minute": 42.0,
      "completed_symbols_per_minute": 14.0,
      "estimated_remaining_seconds": 17,
      "estimated_completion_at": "2026-04-16T01:05:17+00:00",
      "eta_confidence": "high",
      "funnel_diagnostics": {
        "schema_version": "agent_feedback_funnel_v1",
        "broad_symbol_count": 200,
        "requested_scope_bypass": false,
        "role_symbol_counts": {
          "short-term-decider": 80,
          "signal-decider": 80
        }
      }
    }
  }
}
```

## Gated response
```json
{
  "market_date": "2026-04-16",
  "requested_symbols": ["005930"],
  "roles": ["eod-summarizer", "technical-summarizer", "fundamental-summarizer"],
  "status": "blocked",
  "gated": true,
  "cycles": [],
  "final_pending": 1,
  "completed": false,
  "prerequisites": {
    "status": "blocked",
    "blocked_stage_ids": ["pykrx_enrichment"],
    "missing_prerequisites": [
      {
        "stage": "pykrx_enrichment",
        "gate_status": "blocked",
        "details": {
          "decider_blocking_categories": [
            {
              "category": "short_selling",
              "expected_rows": 3579,
              "present_rows": 1920,
              "missing_rows": 1659
            }
          ]
        }
      }
    ]
  }
}
```
