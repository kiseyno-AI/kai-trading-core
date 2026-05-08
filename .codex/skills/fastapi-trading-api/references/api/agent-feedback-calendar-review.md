# GET /agent-context/feedback/calendar

## Summary
Compact month-summary compatibility route for trading-web signal review calendar reads. It prefers the persisted `signal_decider_calendar_review_items` projection, falls back to snapshot/evaluation-backed signal-decider replay rows when that projection is empty, and returns per-date decision counts plus review-visible counts without 404ing on a supported path.

Compatibility responses preserve replay coverage diagnostics instead of flattening every incomplete horizon into a generic failure. Review summaries now surface `pending_maturity`, `insufficient_price_data`, and `not_computed` where applicable.

## Example request
```http
GET /agent-context/feedback/calendar?month=2026-05&horizons=1&horizons=5&primary_horizon_days=5&evaluation_version=signal_decider_eval_v1
```

## Example response
```json
{
  "request": {
    "month": "2026-05",
    "horizons": [1, 5],
    "primary_horizon_days": 5,
    "evaluation_version": "signal_decider_eval_v1"
  },
  "summary": {
    "month": "2026-05",
    "decision_count": 90,
    "reviewed_count": 90,
    "status": "reviewed",
    "pending_review_count": 0,
    "evaluation_coverage": {
      "status": "reviewed",
      "status_reason_counts": {
        "computed": 90
      }
    },
    "source": "persisted_review_projection"
  },
  "items": [
    {
      "market_date": "2026-05-07",
      "calendar_status": "reviewed",
      "decision_count": 90,
      "decision_actions": {
        "buy": 32,
        "wait": 41,
        "reject": 17,
        "unknown": 0
      },
      "review_status_counts": {
        "reviewed": 90
      },
      "reviewed_count": 90,
      "pending_review_count": 0,
      "evaluation_coverage": {
        "status": "complete",
        "status_reason_counts": {
          "computed": 90
        }
      },
      "source": "persisted_review_projection"
    }
  ],
  "availability": {
    "status": "ready",
    "source": "persisted_review_projection",
    "fallback_used": false
  }
}
```


# GET /agent-context/feedback/review

## Summary
Selected-date compatibility route for trading-web signal review detail reads. It returns lightweight `decision` and `review` rows for one `market_date`, prefers the persisted replay projection, and falls back to the snapshot/evaluation-backed signal-decider review builder when the compact projection is empty.

## Example request
```http
GET /agent-context/feedback/review?market_date=2026-05-07&horizons=1&horizons=5&primary_horizon_days=5&evaluation_version=signal_decider_eval_v1
```

## Example response
```json
{
  "request": {
    "market_date": "2026-05-07",
    "horizons": [1, 5],
    "primary_horizon_days": 5,
    "evaluation_version": "signal_decider_eval_v1"
  },
  "summary": {
    "count": 2,
    "reviewed_count": 2,
    "pending_review_count": 0,
    "review_status_counts": {
      "reviewed": 2
    },
    "evaluation_coverage": {
      "status": "complete",
      "computed_count": 2,
      "pending_maturity_count": 0,
      "insufficient_price_data_count": 0,
      "not_computed_count": 0,
      "status_reason_counts": {
        "computed": 2
      }
    },
    "status": "reviewed",
    "source": "persisted_review_projection"
  },
  "items": [
    {
      "snapshot_id": 84,
      "symbol": "005930",
      "market_date": "2026-05-07",
      "run_id": "signal-review-run-84",
      "captured_at": "2026-05-07T07:45:00+00:00",
      "decision": {
        "role": "signal-decider",
        "symbol": "005930",
        "market_date": "2026-05-07",
        "preferred_action": "buy",
        "next_day_open_action": "buy",
        "target_price": 82000,
        "stop_loss_price": 73500,
        "expected_return_pct": 4.2,
        "thesis_summary": "Momentum and fundamentals align for a tactical next-day setup.",
        "policy_version": "signal_decider_policy_v1",
        "evaluation": {
          "quality_verdict": "good_buy",
          "quality_reason": "Target was touched and the stop-loss was never touched inside the review window.",
          "review_conclusion": "Buy held up on replay.",
          "status_reason": "computed",
          "coverage": {
            "horizon_days": 5,
            "status_reason": "computed",
            "decision_market_date": "2026-05-07",
            "entry_trade_date": "2026-05-08",
            "observed_bars_count": 5,
            "required_bars_count": 5,
            "earliest_computable_date": "2026-05-14"
          },
          "target_stop_result": "target_hit_first"
        }
      },
      "review": {
        "status": "complete",
        "status_reason": "computed",
        "quality_verdict": "good_buy",
        "quality_reason": "Target was touched and the stop-loss was never touched inside the review window.",
        "review_conclusion": "Buy held up on replay.",
        "coverage": {
          "horizon_days": 5,
          "status_reason": "computed",
          "decision_market_date": "2026-05-07",
          "entry_trade_date": "2026-05-08",
          "observed_bars_count": 5,
          "required_bars_count": 5,
          "earliest_computable_date": "2026-05-14"
        },
        "intraday_path_result": "target_hit_first",
        "outcome_horizons": [
          {
            "horizon_days": 1,
            "close_return_pct": 0.82,
            "grade_label": "good_buy",
            "status_reason": "computed"
          },
          {
            "horizon_days": 5,
            "close_return_pct": 3.14,
            "grade_label": "good_buy",
            "status_reason": "computed"
          }
        ]
      },
      "source": "persisted_review_projection"
    }
  ],
  "availability": {
    "status": "ready",
    "source": "persisted_review_projection",
    "fallback_used": false
  }
}
```
