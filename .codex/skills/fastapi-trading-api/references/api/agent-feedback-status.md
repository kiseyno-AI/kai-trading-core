# GET /agent-context/feedback/status

## Summary
Reports latest-run workflow status for the default five-role symbol feedback workflow.

When a durable retry-cycle run exists for the date, the response includes `latest_run_id` and `feedback_scope_mode` so operators can distinguish `decider_scope_fast_path` morning-readiness runs from `broad_feedback_universe` enrichment runs.

When upstream PyKRX enrichment, fundamentals, filtering, or factor/scoring stages are not ready yet, symbol items surface `waiting` or `blocked` instead of a generic `pending`, and the response includes a top-level `prerequisites` block describing the missing same-day stages. PyKRX enrichment prerequisite details include expected and present row counts for decider-required categories such as investor flow, short selling, sector classification, market breadth, and valuation snapshots.

Persisted incomplete terminal work is reported as `partial` or `failed`, not `in_progress`. Stored failure rows include `failure_groups` so clients can distinguish dependency blocks (`dependency/missing-upstream`) from OpenClaw transport timeouts (`transport/timeout`) and response-shape failures (`response/invalid-json`, `response/assistant-non-json`, `response/contract`).

For `fundamental-summarizer`, status treats fingerprint-matched prior-date feedback as completed but exposes it separately in `reused_agents` and `reused_feedback_count`. Changed or missing fingerprints appear under `stale_agents` / `stale_feedback_count` so operators can see why new OpenClaw work is required.

## Example request
```http
GET /agent-context/feedback/status?market_date=2026-04-15&symbols=005930&symbols=000660
```

## Example response
```json
{
  "request": {
    "market_date": "2026-04-15",
    "symbols": ["005930"],
    "latest_run_id": "feedback-cycle-2026-04-15-ab12cd34ef",
    "feedback_scope_mode": "decider_scope_fast_path",
    "expected_agents": [
      "eod-summarizer",
      "technical-summarizer",
      "fundamental-summarizer",
      "short-term-decider",
      "signal-decider"
    ]
  },
  "summary": {
    "status": "partial",
    "latest_run_id": "feedback-cycle-2026-04-15-ab12cd34ef",
    "feedback_scope_mode": "decider_scope_fast_path",
    "requested_symbol_count": 1,
    "completed_symbol_count": 0,
    "in_progress_symbol_count": 0,
    "partial_symbol_count": 1,
    "failed_symbol_count": 0,
    "pending_symbol_count": 0,
    "completed_feedback_count": 1,
    "computed_feedback_count": 1,
    "reused_feedback_count": 1,
    "stale_feedback_count": 0,
    "failed_feedback_count": 1,
    "expected_feedback_count": 5,
    "failure_groups": {
      "dependency/missing-upstream": {
        "failure_status": "dependency/missing-upstream",
        "failure_category": "dependency",
        "count": 1,
        "retryable_count": 1,
        "agents": {
          "signal-decider": 1
        },
        "symbols_sample": ["005930"],
        "failure_kinds": ["dependency_missing_upstream"]
      }
    }
  },
  "prerequisites": {
    "status": "ready",
    "waiting_stage_ids": []
  },
  "items": [
    {
      "symbol": "005930",
      "status": "partial",
      "completed_agents": ["eod-summarizer", "fundamental-summarizer"],
      "computed_agents": ["eod-summarizer"],
      "reused_agents": ["fundamental-summarizer"],
      "stale_agents": [],
      "failed_agents": ["signal-decider"],
      "completed_count": 2,
      "computed_count": 1,
      "reused_count": 1,
      "stale_count": 0,
      "failed_count": 1,
      "expected_count": 5,
      "pending_agents": [
        "technical-summarizer",
        "short-term-decider"
      ],
      "reuse": {
        "fundamental-summarizer": {
          "reason": "reuse_unchanged_fundamental_inputs",
          "latest_feedback_id": 42,
          "latest_feedback_market_date": "2026-04-14",
          "input_fingerprint": "sha256:..."
        }
      },
      "failure_groups": {
        "dependency/missing-upstream": {
          "failure_status": "dependency/missing-upstream",
          "failure_category": "dependency",
          "count": 1,
          "retryable_count": 1,
          "agents": {
            "signal-decider": 1
          },
          "symbols_sample": ["005930"],
          "failure_kinds": ["dependency_missing_upstream"]
        }
      },
      "prerequisites_ready": true,
      "missing_prerequisites": []
    }
  ]
}
```
