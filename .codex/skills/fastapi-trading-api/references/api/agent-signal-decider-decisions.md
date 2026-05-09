# GET /agent-context/signal-decider/decisions

## Summary
Searches immutable persisted `signal-decider` decision snapshots over one market date or a bounded date range. The route keeps the compact snapshot payload, exposes a normalized top-level `wait_reason` for wait decisions, adds a stable `explanation` block so operators can answer "why did this become buy/wait/reject?" without reading raw prompt text, and exposes `evaluation_summary` for verdict-aware list views.

## Example request
```http
GET /agent-context/signal-decider/decisions?market_date_from=2026-04-20&market_date_to=2026-04-23&action=wait&policy_version=signal_decider_policy_v1&verdict=missed_opportunity_after_wait&has_missed_opportunity=true&horizons=5&evaluation_version=signal_decider_eval_v1&limit=20
```

## Example response
```json
{
  "request": {
    "market_date_from": "2026-04-20",
    "market_date_to": "2026-04-23",
    "action": "wait",
    "policy_version": "signal_decider_policy_v1",
    "verdict": "missed_opportunity_after_wait",
    "has_missed_opportunity": true,
    "horizons": [5],
    "evaluation_version": "signal_decider_eval_v1"
  },
  "summary": {
    "count": 1,
    "evaluated_count": 1,
    "decision_actions": {
      "wait": 1
    },
    "verdict_counts": {
      "missed_opportunity_after_wait": 1
    }
  },
  "items": [
    {
      "snapshot_id": 84,
      "decision_run_id": "symbol-run-2026-04-23-abc123",
      "symbol": "005930",
      "policy_version": "signal_decider_policy_v1",
      "decision": {
        "action": "wait",
        "wait_reason": {
          "code": "stale_quote_orderbook",
          "label": "Stale quote/orderbook freshness",
          "bucket": "data_gap",
          "blocking_by_default": true,
          "summary": "Realtime orderbook freshness is stale.",
          "source": "inferred"
        },
        "entry_price_basis": "next_open",
        "target_price": null,
        "expected_return_pct": 4.0,
        "stop_loss_price": null
      },
      "wait_reason": {
        "code": "stale_quote_orderbook",
        "label": "Stale quote/orderbook freshness",
        "bucket": "data_gap",
        "blocking_by_default": true,
        "summary": "Realtime orderbook freshness is stale.",
        "source": "inferred"
      },
      "summary": "Realtime orderbook freshness is stale, so the next-open entry is deferred.",
      "explanation": {
        "wait_reason": {
          "code": "stale_quote_orderbook",
          "label": "Stale quote/orderbook freshness",
          "bucket": "data_gap",
          "blocking_by_default": true,
          "summary": "Realtime orderbook freshness is stale.",
          "source": "inferred"
        },
        "supporting_evidence": ["Breadth improved into the close."],
        "decision_risks": ["Leadership remained narrow."],
        "feature_flags": [
          {"key": "technical.breakout_20d", "value": "true"},
          {"key": "action_summary.signal_score_bucket", "value": "high"}
        ],
        "gate_checks": {
          "missing_upstream_roles": [],
          "has_data_warnings": false,
          "has_specialist_disagreement": false
        }
      },
      "evaluation_summary": {
        "evaluation_version": "signal_decider_eval_v1",
        "has_missed_opportunity": true,
        "latest_graded": {
          "horizon_days": 5,
          "grade_label": "missed_opportunity_after_wait"
        }
      }
    }
  ]
}
```


# GET /agent-context/signal-decider/decisions/{snapshot_id}

## Summary
Reads one immutable `signal-decider` decision snapshot by `snapshot_id`, including the stable normalized `wait_reason`, frozen request/response payloads, upstream references, synthetic realized outcomes, separate actual execution outcomes, and versioned evaluations in one place. The frozen request payload now preserves both the structured factor `scores` block and the compact `market_context` block exactly as the decider saw them, and the detail payload also exposes `market_context` directly for operator review.

## Example request
```http
GET /agent-context/signal-decider/decisions/84?horizons=1&horizons=5&evaluation_version=signal_decider_eval_v1
```

## Example response
```json
{
  "item": {
    "snapshot_id": 84,
    "symbol": "005930",
    "wait_reason": {
      "code": "stale_quote_orderbook",
      "label": "Stale quote/orderbook freshness",
      "bucket": "data_gap",
      "blocking_by_default": true,
      "summary": "Realtime orderbook freshness is stale.",
      "source": "inferred"
    },
    "explanation": {
      "decision_action": "wait",
      "summary": "Realtime orderbook freshness is stale, so the next-open entry is deferred.",
      "wait_reason": {
        "code": "stale_quote_orderbook",
        "label": "Stale quote/orderbook freshness",
        "bucket": "data_gap",
        "blocking_by_default": true,
        "summary": "Realtime orderbook freshness is stale.",
        "source": "inferred"
      },
      "feature_flags": [
        {"key": "technical.breakout_20d", "value": "true"}
      ]
    },
    "market_context": {
      "status": "stale",
      "market_date": "2026-04-23",
      "as_of": "2026-04-23T15:21:00+09:00",
      "summary": {
        "market_bias": "mixed",
        "line": "KOSPI +0.11%; KOSDAQ -0.19%; USD/KRW 1444.5 (fallback).",
        "breadth_summary": "KOSPI breadth 512/94/330; KOSDAQ breadth 828/94/716"
      },
      "indices": {
        "kospi": {"level": 2642.07, "change": 2.78, "change_rate": 0.11},
        "kosdaq": {"level": 857.6, "change": -1.61, "change_rate": -0.19}
      },
      "fx": {
        "usdkrw": {"value": 1444.5, "effective_date": "2026-04-22", "fallback_applied": true}
      },
      "flows": {
        "foreign": {"value": -448834, "unit": "KIS_NET_VALUE_RAW"},
        "institutional": {"value": -206217, "unit": "KIS_NET_VALUE_RAW"}
      },
      "missing_fields": [],
      "stale_reason": "usdkrw_fallback_applied"
    },
    "request_payload": {
      "market_context": {
        "status": "stale"
      },
      "scores": {
        "unified_value": {
          "status": "available",
          "score": 0.873,
          "higher_is_better": true,
          "components_used": 5,
          "components_possible": 5,
          "coverage_notes": [],
          "as_of_market_date": "2026-04-23"
        },
        "quality": {
          "status": "partial",
          "score": 0.741,
          "higher_is_better": true,
          "components_used": 5,
          "components_possible": 6,
          "coverage_notes": ["Missing quality components: accruals_ratio."],
          "as_of_market_date": "2026-04-23"
        },
        "momentum": {
          "status": "available",
          "score": 0.914,
          "higher_is_better": true,
          "components_used": 3,
          "components_possible": 3,
          "coverage_notes": [],
          "as_of_market_date": "2026-04-23"
        }
      },
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
    "actual_execution_summary": {
      "count": 1,
      "closed_count": 1,
      "total_realized_pnl_krw": "20000.00"
    },
    "actual_execution_outcomes": [
      {
        "outcome_id": 7,
        "entry_plan_item_id": 301,
        "exit_plan_item_id": 417,
        "lifecycle_status": "closed",
        "exit_reason": "target_hit",
        "entry_filled_quantity": "5.00000000",
        "exit_filled_quantity": "5.00000000",
        "entry_average_fill_price": 50000,
        "exit_average_fill_price": 54000,
        "realized_pnl_krw": "20000.00",
        "realized_return_pct": 0.08
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
}
```


# GET /agent-context/signal-decider/diagnostics

## Summary
Reads the deeper immutable `signal-decider` trace bundle for one market date or symbol slice, including frozen request/response payloads, upstream summaries, synthetic realized outcomes, separate actual execution outcomes, and versioned evaluation rows for audit or missed-opportunity review.

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
      "symbol": "005930",
      "evaluation_summary": {
        "latest_graded": {
          "horizon_days": 5,
          "grade_label": "missed_opportunity_after_wait"
        }
      },
      "request_payload": {
        "signal_decider_policy": {
          "current_version": "signal_decider_policy_v1"
        }
      },
      "outcomes": [
        {
          "horizon_days": 5,
          "outcome_status": "complete",
          "close_return_pct": 0.041
        }
      ],
      "actual_execution_summary": {
        "count": 1,
        "closed_count": 1,
        "total_realized_pnl_krw": "20000.00"
      },
      "actual_execution_outcomes": [
        {
          "outcome_id": 7,
          "lifecycle_status": "closed",
          "exit_reason": "target_hit",
          "realized_pnl_krw": "20000.00"
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


# GET /agent-context/signal-decider/diagnostics/summary

## Summary
Aggregates decision quality over a bounded date window for dashboard or policy-review use. The response groups quality metrics by horizon and policy version, and it also surfaces factor and gate contributor buckets so operators can investigate which features or guardrails correlate with `bad_buy` or missed-opportunity verdicts.

## Example request
```http
GET /agent-context/signal-decider/diagnostics/summary?market_date_from=2026-04-20&market_date_to=2026-04-30&symbols=005930&symbols=000660&horizons=5&evaluation_version=signal_decider_eval_v1&contributor_limit=10
```

## Example response
```json
{
  "summary": {
    "decision_count": 24,
    "evaluation_count": 24,
    "graded_count": 21,
    "pending_count": 3,
    "verdict_counts": {
      "bad_buy": 4,
      "good_buy": 6,
      "missed_opportunity_after_wait": 5
    }
  },
  "aggregates": {
    "by_horizon": [
      {
        "horizon_days": 5,
        "buy_precision": 0.6,
        "avg_close_return_pct": 0.021,
        "stop_hit_rate": 0.2,
        "wait_missed_opportunity_rate": 0.5
      }
    ],
    "by_policy_version": [
      {
        "policy_version": "signal_decider_policy_v1",
        "buy_precision": 0.6,
        "reject_missed_opportunity_rate": 0.25
      }
    ]
  },
  "contributors": {
    "factors": [
      {
        "key": "technical.breakout_20d",
        "value": "true",
        "sample_count": 8,
        "negative_outcome_rate": 0.625,
        "bad_buy_rate": 0.5,
        "missed_opportunity_rate": 0.75
      }
    ],
    "gates": [
      {
        "key": "has_specialist_disagreement",
        "value": "true",
        "sample_count": 3,
        "negative_outcome_rate": 0.667
      }
    ]
  }
}
```
