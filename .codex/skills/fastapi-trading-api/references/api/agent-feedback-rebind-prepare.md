# POST /agent-context/feedback/rebind/prepare

## Summary

Prepares the authoritative, no-order retry contract used to bind the latest
completed KRX decision cohort to the weekly-learning artifact effective for
its next trading session.

Core resolves the latest completed KRX source date, its exact completed-summary
run/cutoff/symbol proof, the immediately following KRX trade date, the
target-date-effective published artifact, and the persisted symbol scopes for
`short-term-decider` and `signal-decider`. A Friday holiday is handled by using
the latest trading date within the artifact week. The endpoint does not write
feedback, refresh a plan, call Exec, or submit/cancel an order.

The response status is:

- `ready`: `retry_request` is the exact request to submit unchanged to
  `POST /agent-context/feedback/retry-cycles`.
- `noop`: the exact bound run already completed with zero failures and complete
  role counts.
- `blocked`: an authoritative input or safety guard failed; no mutation was
  attempted.

## Example request

```http
POST /agent-context/feedback/rebind/prepare
Content-Type: application/json

{
  "as_of_date": "2026-10-17",
  "market": "KRX"
}
```

## Example ready response

```json
{
  "status": "ready",
  "artifact_version": "signal_decider_weekly_learning_2026-10-16_abcd1234",
  "content_sha256": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
  "source_market_date": "2026-10-16",
  "target_trade_date": "2026-10-19",
  "policy_context_as_of_date": "2026-10-19",
  "source_summary_run_id": "feedback-cycle-2026-10-16-source",
  "knowledge_cutoff_at": "2026-10-16T06:30:00+00:00",
  "symbols": ["000660", "005930"],
  "roles": ["short-term-decider", "signal-decider"],
  "role_symbol_scopes": {
    "short-term-decider": ["005930"],
    "signal-decider": ["000660"]
  },
  "feedback_scope_mode": "decider_scope_fast_path",
  "fs_div": "CFS",
  "idempotency_key": "weekly-policy-rebind:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
  "guards": {
    "target_session_not_open": true,
    "source_is_latest_completed_trading_day": true,
    "source_is_in_artifact_week": true,
    "artifact_effective": true
  },
  "retry_request": {
    "market_date": "2026-10-16",
    "policy_context_as_of_date": "2026-10-19",
    "symbols": ["000660", "005930"],
    "roles": ["short-term-decider", "signal-decider"],
    "role_symbol_scopes": {
      "short-term-decider": ["005930"],
      "signal-decider": ["000660"]
    },
    "knowledge_cutoff_at": "2026-10-16T06:30:00+00:00",
    "feedback_scope_mode": "decider_scope_fast_path",
    "fs_div": "CFS",
    "max_cycles": 3,
    "outer_batch_size": 20,
    "reuse_existing": true,
    "force": false,
    "force_recompute": true,
    "idempotency_key": "weekly-policy-rebind:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
    "scope_authority": {
      "schema_version": "agent_feedback_scope_authority_v1",
      "kind": "completed_summary_intersection_authoritative_cohort",
      "source_summary_run_id": "feedback-cycle-2026-10-16-source",
      "source_feedback_scope_mode": "broad_feedback_universe",
      "decision_feedback_scope_mode": "decider_scope_fast_path",
      "knowledge_cutoff_at": "2026-10-16T06:30:00+00:00",
      "fs_div": "CFS",
      "completed_scope_proof": {
        "schema_version": "completed_summarizer_intersection_v1",
        "completed_symbols": ["000660", "005930"],
        "admitted_symbols": ["000660", "005930"]
      },
      "completed_symbols": ["000660", "005930"],
      "admitted_symbol_count": 2,
      "admitted_symbols_sha256": "d4da1a29e6ffdf23b6e85e1ab983ca49ed0207b347c7485978a603733a3c13a9",
      "decider_role_symbol_scopes": {
        "short-term-decider": ["005930"],
        "signal-decider": ["000660"]
      },
      "policy_binding": {
        "schema_version": "weekly_policy_rebind_v1",
        "artifact_version": "signal_decider_weekly_learning_2026-10-16_abcd1234",
        "content_sha256": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        "source_market_date": "2026-10-16",
        "target_trade_date": "2026-10-19",
        "policy_context_as_of_date": "2026-10-19",
        "idempotency_key": "weekly-policy-rebind:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
      }
    }
  }
}
```

Treat every returned authority field as opaque. Do not reconstruct, widen, or
override it. Core re-derives and exact-compares the contract on retry admission,
rejects every current KRX `OPEN` state even for a future target, and rechecks
the session immediately before the feedback/snapshot transaction commits.
