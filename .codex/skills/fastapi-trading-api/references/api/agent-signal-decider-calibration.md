# GET /agent-context/signal-decider/calibration

## Summary
Builds an operator-reviewable calibration report from immutable stored `signal-decider` snapshots plus persisted replay outcomes and graded evaluations over a bounded date range.

Only mature rows with `coverage.status_reason=computed` and `evaluation_status=graded` contribute to grouping, cluster discovery, and hint generation. Pending or incomplete rows are excluded from calibration evidence and reported separately through `summary.coverage`, `summary.coverage_gap_count`, and `exclusions.status_reason_counts`.

The route is read-only. It does not change prompts, policies, or scores in production. `review_boundary.manual_review_only=true` and every hint includes `auto_apply=false`.

## Example request
```http
GET /agent-context/signal-decider/calibration?market_date_from=2026-04-20&market_date_to=2026-04-29&symbols=005930&symbols=000660&policy_version=signal_decider_policy_v1&horizon_days=5&evaluation_version=signal_decider_eval_v1&min_sample_size=3&cluster_limit=10
```

## Example response
```json
{
  "request": {
    "market_date_from": "2026-04-20",
    "market_date_to": "2026-04-29",
    "symbols": ["005930", "000660"],
    "policy_version": "signal_decider_policy_v1",
    "horizon_days": 5,
    "evaluation_version": "signal_decider_eval_v1",
    "min_sample_size": 3,
    "cluster_limit": 10
  },
  "review_boundary": {
    "report_version": "signal_decider_calibration_report_v1",
    "manual_review_only": true,
    "auto_apply_prompt_changes": false,
    "auto_apply_policy_changes": false,
    "replay_read_model": {
      "name": "signal_decider_replay_review",
      "version": "signal_decider_replay_review_v1",
      "source_tables": [
        "signal_decider_decision_snapshots",
        "signal_decider_realized_outcomes",
        "signal_decider_evaluations"
      ],
      "separate_from_actual_execution_outcomes": true,
      "actual_execution_outcomes_endpoint": "/agent-context/signal-decider/execution-outcomes"
    }
  },
  "summary": {
    "status": "ready",
    "snapshots_considered": 12,
    "mature_computed_count": 8,
    "excluded_count": 4,
    "coverage": {
      "count": 12,
      "computed_count": 8,
      "pending_maturity_count": 2,
      "insufficient_price_data_count": 1,
      "not_computed_count": 1,
      "status_reason_counts": {
        "computed": 8,
        "insufficient_price_data": 1,
        "not_computed": 1,
        "pending_maturity": 2
      },
      "coverage_ratio": 0.666667
    },
    "coverage_gap_count": 2,
    "decision_action_counts": {
      "buy": 5,
      "wait": 3
    },
    "grade_label_counts": {
      "correct_wait": 2,
      "good_buy": 4,
      "missed_opportunity_after_wait": 2
    },
    "policy_versions": ["signal_decider_policy_v1"],
    "positive_outcome_count": 6,
    "negative_outcome_count": 2,
    "positive_outcome_rate": 0.75,
    "negative_outcome_rate": 0.25,
    "ready_for_operator_review": true
  },
  "exclusions": {
    "status_reason_counts": {
      "insufficient_price_data": 1,
      "not_computed": 1,
      "pending_maturity": 2
    },
    "items": [
      {
        "snapshot_id": 91,
        "feedback_id": 42,
        "market_date": "2026-04-29",
        "symbol": "005930",
        "policy_version": "signal_decider_policy_v1",
        "decision_action": "wait",
        "wait_reason": {
          "code": "stale_quote_orderbook",
          "label": "Stale quote/orderbook freshness"
        },
        "status_reason": "pending_maturity",
        "status_message": "The replay horizon has not matured yet.",
        "matured": false,
        "stored_outcome": false,
        "stored_evaluation": false,
        "earliest_computable_date": "2026-05-08",
        "latest_available_trade_date": "2026-05-02"
      }
    ]
  },
  "group_summaries": {
    "decision_actions": {
      "label": "Decision action buckets",
      "items": [
        {
          "bucket_type": "decision_actions",
          "bucket_key": "decision.action",
          "bucket_value": "buy",
          "bucket_label": "Decision action: buy",
          "sample_count": 5,
          "positive_count": 4,
          "negative_count": 1,
          "positive_rate": 0.8,
          "negative_rate": 0.2
        }
      ]
    },
    "wait_reason_taxonomy": {
      "label": "Wait-reason taxonomy buckets",
      "items": [
        {
          "bucket_type": "wait_reason_taxonomy",
          "bucket_key": "wait_reason.code",
          "bucket_value": "stale_quote_orderbook",
          "bucket_label": "Wait reason: Stale quote/orderbook freshness",
          "sample_count": 3,
          "positive_count": 0,
          "negative_count": 2,
          "positive_rate": 0.0,
          "negative_rate": 0.666667
        }
      ]
    }
  },
  "condition_clusters": {
    "baseline_positive_rate": 0.75,
    "baseline_negative_rate": 0.25,
    "positive": [
      {
        "cluster_key": "decision.confidence_bucket=high",
        "bucket_label": "Confidence bucket: high",
        "sample_count": 4,
        "positive_rate": 1.0,
        "lift_vs_baseline": 0.25,
        "strength_score": 1.0,
        "tokens": [
          {
            "category": "confidence_buckets",
            "key": "decision.confidence_bucket",
            "value": "high",
            "label": "Confidence bucket: high"
          }
        ]
      }
    ],
    "negative": [
      {
        "cluster_key": "wait_reason.code=stale_quote_orderbook",
        "bucket_label": "Wait reason: Stale quote/orderbook freshness",
        "sample_count": 3,
        "negative_rate": 0.666667,
        "lift_vs_baseline": 0.416667,
        "strength_score": 1.25,
        "tokens": [
          {
            "category": "wait_reason_taxonomy",
            "key": "wait_reason.code",
            "value": "stale_quote_orderbook",
            "label": "Wait reason: Stale quote/orderbook freshness"
          }
        ]
      }
    ]
  },
  "calibration_hints": {
    "prompt_guidance": [],
    "scoring_guidance": [
      {
        "kind": "scoring_guidance",
        "title": "Positive cluster: Confidence bucket: high",
        "action": "Consider reinforcing confidence when confidence bucket: high recurs.",
        "manual_review_only": true,
        "auto_apply": false,
        "evidence": {
          "horizon_days": 5,
          "sample_count": 4,
          "direction": "positive",
          "direction_rate": 1.0,
          "lift_vs_baseline": 0.25,
          "cluster_key": "decision.confidence_bucket=high"
        }
      }
    ],
    "operator_review": [
      {
        "kind": "operator_review",
        "title": "Repair mature replay coverage gaps before changing prompt or scoring guidance.",
        "action": "Resolve stored replay gaps and rerun the calibration report before acting on the negative or positive condition clusters.",
        "manual_review_only": true,
        "auto_apply": false,
        "evidence": {
          "horizon_days": 5,
          "status_reason_counts": {
            "insufficient_price_data": 1,
            "not_computed": 1
          }
        }
      }
    ]
  }
}
```
