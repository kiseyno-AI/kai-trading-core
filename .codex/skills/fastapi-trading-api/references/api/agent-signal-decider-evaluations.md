# POST /agent-context/signal-decider/evaluations/compute

## Summary
Computes or refreshes raw realized outcomes plus versioned grading rows for immutable stored `signal-decider` decision snapshots over a date range.

## Example request
```http
POST /agent-context/signal-decider/evaluations/compute
Content-Type: application/json

{
  "market_date_from": "2026-04-20",
  "market_date_to": "2026-04-23",
  "symbols": ["005930", "000660"],
  "horizons": [1, 3, 5, 10],
  "evaluation_version": "signal_decider_eval_v1"
}
```

## Example response
```json
{
  "summary": {
    "decisions_considered": 8,
    "outcomes_upserted": 32,
    "evaluations_upserted": 32,
    "graded_count": 20,
    "pending_count": 12
  }
}
```


# GET /agent-context/signal-decider/evaluations

## Summary
Reads the persisted outcome-plus-grade bundle back per immutable stored `signal-decider` decision snapshot for one market date.

## Example request
```http
GET /agent-context/signal-decider/evaluations?market_date=2026-04-23&symbol=005930&horizons=1&horizons=5&evaluation_version=signal_decider_eval_v1
```

## Example response
```json
{
  "items": [
    {
      "snapshot_id": 84,
      "feedback_id": 42,
      "decision_run_id": "symbol-run-2026-04-23-abc123",
      "symbol": "005930",
      "policy_version": "signal_decider_policy_v1",
      "outcomes": [
        {
          "decision_snapshot_id": 84,
          "horizon_days": 5,
          "outcome_status": "complete",
          "entry_trade_date": "2026-04-24",
          "exit_trade_date": "2026-04-30",
          "close_return_pct": 0.041,
          "max_upside_return_pct": 0.062
        }
      ],
      "evaluations": [
        {
          "decision_snapshot_id": 84,
          "horizon_days": 5,
          "evaluation_version": "signal_decider_eval_v1",
          "evaluation_status": "graded",
          "grade_label": "missed_opportunity_after_wait",
          "counterfactual_action": "buy"
        }
      ]
    }
  ]
}
```
