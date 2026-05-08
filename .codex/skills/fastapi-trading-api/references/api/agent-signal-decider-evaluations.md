# POST /agent-context/signal-decider/evaluations/compute

## Summary
Computes or refreshes raw synthetic realized outcomes plus versioned grading rows for immutable stored `signal-decider` decision snapshots over a date range.

The same evaluation refresh now also runs in the default daily EOD stage catalog as `signal_decider_evaluations`, which revisits the recent decision window after new price bars arrive.
By default the compute route now gates on same-day feedback prerequisites for the requested date window. If fundamentals, filtering, or factor/scoring stages are still pending or blocked for the relevant market date, the response returns `summary.status=waiting|blocked`, `summary.gated=true`, and a `prerequisites` block instead of refreshing evaluations. Operators can bypass that check with `force_recompute=true`.
Coverage is explicit per horizon. Mature windows with complete stored bars are `computed`; immature latest-date windows are `pending_maturity`; mature windows with missing or incomplete stored bars are `insufficient_price_data`; and mature windows with price bars present but missing stored evaluation rows read back as `not_computed`.

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
    "pending_count": 12,
    "coverage": {
      "count": 32,
      "computed_count": 20,
      "pending_maturity_count": 10,
      "insufficient_price_data_count": 2,
      "not_computed_count": 0,
      "status_reason_counts": {
        "computed": 20,
        "insufficient_price_data": 2,
        "pending_maturity": 10
      }
    }
  }
}
```


# GET /agent-context/signal-decider/evaluations

## Summary
Reads the persisted synthetic outcome-plus-grade bundle back per immutable stored `signal-decider` decision snapshot for one market date. Broker-executed fills and realized execution P&L remain queryable through `GET /agent-context/signal-decider/execution-outcomes`.

Each item includes per-horizon coverage metadata so clients can distinguish replay maturity from true data gaps or missing compute rows without inferring it from `pending_horizon` alone.

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
      "coverage_summary": {
        "count": 2,
        "computed_count": 1,
        "pending_maturity_count": 1,
        "insufficient_price_data_count": 0,
        "not_computed_count": 0,
        "status_reason_counts": {
          "computed": 1,
          "pending_maturity": 1
        },
        "coverage_ratio": 0.5
      },
      "coverage": [
        {
          "horizon_days": 1,
          "status_reason": "computed",
          "decision_market_date": "2026-04-23",
          "entry_trade_date": "2026-04-24",
          "observed_bars_count": 1,
          "required_bars_count": 1,
          "earliest_computable_date": "2026-04-24"
        },
        {
          "horizon_days": 5,
          "status_reason": "pending_maturity",
          "decision_market_date": "2026-04-23",
          "entry_trade_date": "2026-04-24",
          "observed_bars_count": 2,
          "required_bars_count": 5,
          "earliest_computable_date": "2026-04-30"
        }
      ],
      "outcomes": [
        {
          "decision_snapshot_id": 84,
          "horizon_days": 5,
          "outcome_status": "pending",
          "entry_trade_date": "2026-04-24",
          "exit_trade_date": null,
          "status_reason": "pending_maturity",
          "coverage": {
            "horizon_days": 5,
            "status_reason": "pending_maturity",
            "earliest_computable_date": "2026-04-30"
          }
        }
      ],
      "evaluations": [
        {
          "decision_snapshot_id": 84,
          "horizon_days": 5,
          "evaluation_version": "signal_decider_eval_v1",
          "evaluation_status": "pending",
          "grade_label": "pending_horizon",
          "counterfactual_action": "buy",
          "status_reason": "pending_maturity",
          "coverage": {
            "horizon_days": 5,
            "status_reason": "pending_maturity",
            "earliest_computable_date": "2026-04-30"
          },
          "rationale": {
            "reason": "pending_maturity",
            "evaluation_profile": {
              "requested_version": "signal_decider_eval_v1",
              "rule_set_version": "signal_decider_eval_v1"
            }
          }
        }
      ]
    }
  ],
  "summary": {
    "count": 1,
    "evaluated_count": 0,
    "stored_evaluation_count": 1,
    "coverage": {
      "count": 2,
      "computed_count": 1,
      "pending_maturity_count": 1,
      "insufficient_price_data_count": 0,
      "not_computed_count": 0
    }
  }
}
```
