# GET /agent-context/signal-decider/decisions

## Summary
Reads immutable persisted `signal-decider` decision snapshots with compact traceability, policy version, and feature-snapshot data for one market date.

## Example request
```http
GET /agent-context/signal-decider/decisions?market_date=2026-04-23&symbol=005930&limit=20
```

## Example response
```json
{
  "items": [
    {
      "snapshot_id": 84,
      "feedback_id": 42,
      "decision_run_id": "symbol-run-2026-04-23-abc123",
      "market_date": "2026-04-23",
      "symbol": "005930",
      "policy_version": "signal_decider_policy_v1",
      "decision": {
        "action": "wait",
        "preferred_action": "wait",
        "next_day_open_action": "wait",
        "entry_price_basis": "next_open",
        "target_price": null,
        "expected_return_pct": 4.0,
        "stop_loss_price": null
      },
      "summary": "Momentum is constructive, but confirmation is incomplete.",
      "traceability": {
        "schema_version": "signal_decider_trace_v2",
        "agent": {
          "agent_name": "signal-decider",
          "decision_run_id": "symbol-run-2026-04-23-abc123"
        },
        "gate_checks": {
          "required_upstream_roles": ["eod-summarizer", "technical-summarizer", "fundamental-summarizer"],
          "available_upstream_roles": ["eod-summarizer", "technical-summarizer", "fundamental-summarizer"]
        },
        "factor_contributions": {
          "feature_flags": [
            {"key": "technical.trend_state", "value": "uptrend"},
            {"key": "action_summary.signal_score_bucket", "value": "high"}
          ]
        }
      },
      "feature_snapshot": {
        "technical": {
          "trend_state": "uptrend",
          "breakout_20d": true
        },
        "action_summary": {
          "action_label": "long_breakout",
          "signal_score_bucket": "high"
        }
      }
    }
  ]
}
```


# GET /agent-context/signal-decider/diagnostics

## Summary
Reads the deeper immutable `signal-decider` trace bundle for one market date or symbol slice, including frozen request/response payloads, upstream summaries, realized outcomes, and versioned evaluation rows.

## Example request
```http
GET /agent-context/signal-decider/diagnostics?market_date=2026-04-23&symbol=005930&evaluation_version=signal_decider_eval_v1
```

## Example response
```json
{
  "items": [
    {
      "snapshot_id": 84,
      "feedback_id": 42,
      "symbol": "005930",
      "request_payload": {
        "signal_decider_policy": {
          "current_version": "signal_decider_policy_v1"
        }
      },
      "response_payload": {
        "next_day_open_action": "wait",
        "supporting_evidence": ["Breadth improved into the close."]
      },
      "outcomes": [
        {
          "horizon_days": 5,
          "outcome_status": "complete",
          "close_return_pct": 0.041
        }
      ],
      "evaluations": [
        {
          "horizon_days": 5,
          "evaluation_status": "graded",
          "grade_label": "missed_opportunity_after_wait"
        }
      ]
    }
  ]
}
```
