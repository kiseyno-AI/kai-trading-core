# POST /agent-context/feedback/retry-cycles

## Summary
Runs bounded retry passes over incomplete symbol-level agent feedback.

The retry universe starts with the persisted active-pre-filter intersection for the requested `market_date`. By default `feedback_scope_mode` is `decider_scope_fast_path`: Core computes the decider candidate scope first, uses that same symbol set for `eod-summarizer`, `technical-summarizer`, and `fundamental-summarizer`, and only then runs `short-term-decider` / `signal-decider` after required summarizer coverage exists for that scope.

Callers can optionally provide `symbols` to restrict the real daily retry cycle to one explicit symbol or a small explicit symbol list. Requested-symbol dispatch bypasses automatic broad and role caps, records `requested_symbol_scope_cap_bypass` diagnostics, still runs the same summarizer and decider sequence, persists through the same durable tables, and keeps the run inspectable through the normal progress/latest/read surfaces. Requested symbols outside the active-pre-filter intersection are reported as `skipped_symbols`.

Default automatic caps are broad top 10%, minimum 150, maximum 200 symbols, then role caps of 200 EOD, 150 technical, 120 fundamental, 80 short-term-decider, and 80 signal-decider symbols. In `decider_scope_fast_path`, progress totals use the decider scope and `broad_symbol_count` preserves the wider background enrichment universe. Operators can request `feedback_scope_mode="broad_feedback_universe"` to preserve independent wider summarizer scopes for offline enrichment. Forced-inclusion categories, when backed by local data sources, are reported under `funnel_diagnostics.role_diagnostics[role].forced_inclusion_reasons_by_symbol`.

The response includes cycle and batch details plus a fresh `progress` snapshot using the same response shape as `GET /agent-context/feedback/progress`. Consumers can read `progress.summary.first_feedback_at`, `elapsed_seconds`, rate fields, and ETA fields immediately after the retry request completes.
Retry cycles now also enforce the same-day feedback prerequisites by default. If PyKRX enrichment, fundamentals, filtering, or factor/scoring stages are still pending or blocked, the route returns a gated `waiting` or `blocked` response with a `prerequisites` block and does not create a new durable run. Operators can bypass that safety check only with `force=true`.

`fundamental-summarizer` can reuse the latest prior feedback across trading dates when the stored fundamental input fingerprint matches the current DART/disclosure/valuation input fingerprint. `force_recompute=true` explicitly bypasses reuse for debugging, model upgrades, or prompt/schema changes while keeping `force=true` available for prerequisite override/full refresh flows.

`knowledge_cutoff_at` is an optional timezone-aware instant used as an
exclusive upper bound for both stored-news publication time and collection
time. The resolved instant stays fixed for every cycle and role in the run.
When omitted, completed historical KRX dates use 15:30 KST and historical
US-only scopes use the XNYS calendar's actual close, including early closes.
When the field is omitted, current or future dates use request time. Mixed
KRX/US scopes require an explicit cutoff. Timezone-naive or explicitly future
values fail with HTTP 400. Core normalizes the effective instant to UTC in the
durable run request; the endpoint's top-level response does not duplicate that
field.

The normalized effective cutoff is part of feedback reuse, prior-cycle resume,
and active-run join identity. Equivalent timezone offsets match; a missing,
earlier, or later cutoff requires fresh work instead of reuse or resume.
Omitted current/future cutoffs are
resolved at request time, so different resolved instants do not exact-join.

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
  "knowledge_cutoff_at": "2026-04-16T15:30:00+09:00",
  "feedback_scope_mode": "decider_scope_fast_path",
  "max_cycles": 3,
  "outer_batch_size": 20,
  "reuse_existing": true,
  "force_recompute": false,
  "force": false
}
```

## Example response
```json
{
  "market_date": "2026-04-16",
  "requested_symbols": null,
  "eligible_symbols": null,
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
  "force_recompute": false,
  "feedback_scope_mode": "decider_scope_fast_path",
  "run_id": "feedback-cycle-2026-04-16-ab12cd34ef",
  "status": "partial",
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
      ],
      "run_id": "feedback-cycle-2026-04-16-ab12cd34ef",
      "feedback_scope_mode": "decider_scope_fast_path"
    },
    "result": {
      "market_date": "2026-04-16",
      "roles": {
        "signal-decider": {
          "requested_count": 80,
          "completed_count": 70,
          "error_count": 2
        }
      }
    },
    "summary": {
      "run_status": "running",
      "is_live": true,
      "first_feedback_at": "2026-04-16T01:00:00+00:00",
      "latest_feedback_at": "2026-04-16T01:05:00+00:00",
      "elapsed_seconds": 300,
      "feedback_scope_mode": "decider_scope_fast_path",
      "total_symbols": 80,
      "broad_symbol_count": 200,
      "role_symbol_counts": {
        "eod-summarizer": 80,
        "technical-summarizer": 80,
        "fundamental-summarizer": 80,
        "short-term-decider": 80,
        "signal-decider": 80
      },
      "processed_symbols": 72,
      "completed_symbols": 70,
      "in_progress_symbols": 2,
      "pending_symbols": 8,
      "progress_percent": 52.5,
      "completed_feedback_count": 210,
      "computed_feedback_count": 196,
      "reused_feedback_count": 14,
      "stale_feedback_count": 3,
      "expected_feedback_count": 400,
      "completed_feedback_per_minute": 42.0,
      "completed_symbols_per_minute": 14.0,
      "estimated_remaining_seconds": 17,
      "estimated_completion_at": "2026-04-16T01:05:17+00:00",
      "eta_confidence": "high",
      "funnel_diagnostics": {
        "schema_version": "agent_feedback_funnel_v1",
        "feedback_scope_mode": "decider_scope_fast_path",
        "broad_symbol_count": 200,
        "decider_scope": {
          "mode": "decider_scope_fast_path",
          "applied": true,
          "symbol_count": 80
        },
        "requested_scope_bypass": false,
        "role_symbol_counts": {
          "short-term-decider": 80,
          "signal-decider": 80
        }
      }
    },
    "prerequisites": {
      "status": "ready",
      "ready": true,
      "blocked_stage_ids": [],
      "missing_prerequisites": []
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
