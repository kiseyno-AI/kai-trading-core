# POST /agent-context/feedback/followup

## Summary

Runs the same bounded retry engine and returns the same response contract as
`POST /agent-context/feedback/retry-cycles`. It is the follow-up recovery
surface for automatic or operator-initiated retries. Its
`outer_batch_size` maximum is 500 rather than 200.

`knowledge_cutoff_at` is an optional timezone-aware instant used as an
exclusive upper bound for both stored-news publication time and collection
time. The resolved instant stays fixed for every cycle and role in the
follow-up. When omitted, completed historical KRX dates use 15:30 KST and
historical US-only scopes use the XNYS calendar's actual close, including early
closes. When the field is omitted, current or future dates use request time.
Mixed KRX/US scopes require an explicit cutoff. Timezone-naive or explicitly
future values fail with HTTP 400. Core normalizes the effective instant to UTC
in the durable run request; the endpoint's top-level response does not
duplicate that field.

## Example request

```http
POST /agent-context/feedback/followup
Content-Type: application/json

{
  "market_date": "2026-04-16",
  "symbols": ["005930"],
  "roles": ["technical-summarizer", "signal-decider"],
  "knowledge_cutoff_at": "2026-04-16T15:30:00+09:00",
  "feedback_scope_mode": "decider_scope_fast_path",
  "max_cycles": 2,
  "outer_batch_size": 20,
  "reuse_existing": true,
  "force": false,
  "force_recompute": false
}
```

## Example response

```json
{
  "market_date": "2026-04-16",
  "roles": ["technical-summarizer", "signal-decider"],
  "max_cycles": 2,
  "outer_batch_size": 20,
  "reuse_existing": true,
  "force_recompute": false,
  "feedback_scope_mode": "decider_scope_fast_path",
  "requested_symbols": ["005930"],
  "eligible_symbols": ["005930"],
  "skipped_symbols": [],
  "run_id": "feedback-cycle-2026-04-16-ab12cd34ef",
  "status": "completed",
  "cycles": [
    {
      "cycle": 1,
      "status": "completed",
      "start_pending": 1,
      "end_pending": 0,
      "batches": []
    }
  ],
  "final_pending": 0,
  "completed": true,
  "progress": {
    "request": {
      "market_date": "2026-04-16",
      "symbols": ["005930"],
      "expected_agents": ["technical-summarizer", "signal-decider"],
      "run_id": "feedback-cycle-2026-04-16-ab12cd34ef",
      "feedback_scope_mode": "decider_scope_fast_path"
    },
    "result": {
      "market_date": "2026-04-16",
      "roles": {
        "technical-summarizer": {
          "requested_count": 1,
          "completed_count": 1,
          "error_count": 0
        },
        "signal-decider": {
          "requested_count": 1,
          "completed_count": 1,
          "error_count": 0
        }
      }
    },
    "summary": {
      "run_status": "completed",
      "pending_symbols": 0,
      "progress_percent": 100.0
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

If another active retry run overlaps the requested scope, the response may
instead have `status="running"`, `completed=false`, `cycles=[]`, and
`admission_status="active_scope_joined"` or
`"overlapping_scope_active"`. An incomplete prerequisite gate can return
`status="waiting"` or `"blocked"`, `gated=true`, and a `prerequisites` object.
