# GET /agent-context/feedback/progress

## Summary
Reports aggregate progress for the default five-role symbol feedback workflow.

The response is now a stable coverage and liveness snapshot:
- `total_symbols` uses the durable run scope when a matching dispatch or retry run exists; otherwise it falls back to the active symbol set with `daily_price_bars` for the requested market date, optionally narrowed to requested symbols
- `result.market_date` echoes the requested market date, and `result.roles[role]` reports durable persisted `requested_count`, `completed_count`, and `error_count`
- `run_id` is optional and lets operators inspect the exact durable run returned by dispatch or retry endpoints
- dispatch and retry run scopes now reflect the intersection of the active pre-filters, not Piotroski alone
- retry-cycle run scopes now separate `broad_symbol_count` from per-role `role_symbol_counts`; `role_symbol_scopes` and `funnel_diagnostics` explain role caps, requested-symbol bypasses, and forced-inclusion reasons
- when a matching retry-cycle `agent_context_feedback_runs` row exists, aggregate counts stay grounded in persisted feedback and failure rows while live lifecycle fields such as `current_role`, `completed_roles`, and `cycle_status` still come from the active run metadata
- top-level `progress_ratio` and `progress_percent` represent whole-cycle completion across all expected symbol-role feedback items
- `boundary_progress_ratio` and `boundary_progress_percent` keep the separate symbol-boundary coverage signal that used to drive the top-level percentage
- `terminal_feedback_count` shows how many expected feedback items are already terminal via either stored feedback or stored failure rows
- `run_status` indicates whether feedback appears to be actively moving, stalled, pending, or completed
- completed terminal partial runs report inactive incomplete symbols as `partial_symbols`; `in_progress_symbols` is reserved for live runtime work only
- `failure_groups` groups unresolved stored failures by status, including `dependency/missing-upstream`, `transport/timeout`, `response/invalid-json`, `response/assistant-non-json`, and `response/contract`
- when prerequisites are not ready and no live or durable run exists yet, `run_status` can be `waiting` or `blocked`, and the top-level `prerequisites` block explains which upstream stages still gate dispatch
- prerequisite stage items preserve pipeline gate diagnostics (`gate_status`, `severity`, `blocking_status`, and `gate_reason`) so a partial upstream row-count stage can be shown as non-blocking degraded coverage instead of a feedback failure
- `latest_feedback_at` and `seconds_since_last_feedback` help operators tell whether a run is still alive
- `first_feedback_at`, `elapsed_seconds`, rates, and ETA fields are calculated only from feedback rows for the same target symbol universe used by the progress counts
- `estimated_completion_at` is an ETA for active runs and the latest feedback timestamp for already completed runs

## Example request
```http
GET /agent-context/feedback/progress?market_date=2026-04-16&run_id=feedback-cycle-2026-04-16-ab12cd34ef
```

## Example response
```json
{
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
    "run_id": "feedback-cycle-2026-04-16-ab12cd34ef"
  },
  "result": {
    "market_date": "2026-04-16",
    "roles": {
      "eod-summarizer": {
        "requested_count": 200,
        "completed_count": 134,
        "error_count": 0
      },
      "technical-summarizer": {
        "requested_count": 150,
        "completed_count": 130,
        "error_count": 2
      },
      "fundamental-summarizer": {
        "requested_count": 120,
        "completed_count": 100,
        "error_count": 1
      },
      "short-term-decider": {
        "requested_count": 80,
        "completed_count": 20,
        "error_count": 4
      },
      "signal-decider": {
        "requested_count": 80,
        "completed_count": 18,
        "error_count": 0
      }
    }
  },
  "summary": {
    "run_status": "stalled",
    "is_live": false,
    "first_feedback_at": "2026-04-17T06:58:10.141902+09:00",
    "latest_feedback_at": "2026-04-17T07:16:28.988041+09:00",
    "elapsed_seconds": 6330,
    "seconds_since_last_feedback": 5231,
    "stalled_threshold_seconds": 300,
    "total_symbols": 200,
    "broad_symbol_count": 200,
    "role_symbol_counts": {
      "eod-summarizer": 200,
      "technical-summarizer": 150,
      "fundamental-summarizer": 120,
      "short-term-decider": 80,
      "signal-decider": 80
    },
    "processed_symbols": 140,
    "completed_symbols": 0,
    "in_progress_symbols": 0,
    "partial_symbols": 140,
    "pending_symbols": 60,
    "terminal_feedback_count": 402,
    "progress_ratio": 0.638095,
    "progress_percent": 63.81,
    "boundary_progress_ratio": 0.7,
    "boundary_progress_percent": 70.0,
    "failed_items": 0,
    "completed_feedback_count": 402,
    "expected_feedback_count": 630,
    "failure_groups": {
      "transport/timeout": {
        "failure_status": "transport/timeout",
        "failure_category": "transport",
        "count": 2,
        "retryable_count": 2,
        "agents": {
          "technical-summarizer": 2
        },
        "symbols_sample": ["005930", "000660"],
        "failure_kinds": ["openclaw_transport_timeout"]
      }
    },
    "completed_feedback_per_minute": 3.81,
    "completed_symbols_per_minute": 0.0,
    "estimated_remaining_seconds": 198267,
    "estimated_completion_at": "2026-04-19T15:59:15.210342+09:00",
    "eta_confidence": "medium",
    "funnel_diagnostics": {
      "schema_version": "agent_feedback_funnel_v1",
      "broad_symbol_count": 200,
      "requested_scope_bypass": false,
      "role_symbol_counts": {
        "short-term-decider": 80,
        "signal-decider": 80
      },
      "role_diagnostics": {
        "signal-decider": {
          "role_cap": {
            "rule_version": "agent_feedback_role_scope_cap_v1",
            "configured_cap": 80,
            "applied": true,
            "pre_cap_symbol_count": 200,
            "post_cap_symbol_count": 80,
            "forced_included_count": 2
          },
          "forced_included_symbols": ["005930", "000660"],
          "selection_reasons_by_symbol": {
            "005930": ["rank_within_role_cap", "forced:prior_positive_decider_action"]
          }
        }
      }
    }
  },
  "prerequisites": {
    "status": "ready",
    "waiting_stage_ids": [],
    "blocked_stage_ids": [],
    "stages": {
      "market_risk_filter": {
        "status": "partial",
        "gate_status": "ready",
        "severity": "degraded",
        "blocking_status": "non_blocking",
        "gate_reason": "missing_rows_outside_active_feedback_scope",
        "details": {
          "missing_count": 1,
          "missing_symbols_sample": ["176750"]
        }
      }
    }
  }
}
```
