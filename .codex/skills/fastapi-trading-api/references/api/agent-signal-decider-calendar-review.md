# GET /agent-context/signal-decider/calendar

## Summary
Month-level replay review calendar over the persisted `signal_decider_calendar_review_items` read model. The response groups decisions by market date, reports buy or wait or reject counts, highlights primary-horizon evaluation coverage with explicit status-reason counts, and keeps replay scorecards separate from actual execution outcomes.

## Example request
```http
GET /agent-context/signal-decider/calendar?month=2026-04&horizons=1&horizons=3&horizons=5&primary_horizon_days=5&evaluation_version=signal_decider_eval_v1
```

## Example response
```json
{
  "request": {
    "month": "2026-04",
    "market_date_from": "2026-04-01",
    "market_date_to": "2026-04-30",
    "horizons": [1, 3, 5],
    "primary_horizon_days": 5,
    "evaluation_version": "signal_decider_eval_v1"
  },
  "read_model": {
    "name": "signal_decider_replay_review",
    "version": "signal_decider_replay_review_v1",
    "artifact_source": "signal_decider_calendar_review_items",
    "separate_from_actual_execution_outcomes": true,
    "actual_execution_outcomes_endpoint": "/agent-context/signal-decider/execution-outcomes"
  },
  "taxonomy": {
    "version": "signal_decider_replay_review_v1"
  },
  "summary": {
    "day_count": 2,
    "decision_count": 4,
    "calendar_status": "partial",
    "stored_decision_count": 4,
    "reviewed_decision_count": 3,
    "pending_review_count": 1,
    "evaluation_coverage": {
      "status": "partial",
      "complete_count": 3,
      "pending_count": 1,
      "pending_maturity_count": 1,
      "insufficient_price_data_count": 0,
      "not_computed_count": 0,
      "status_reason_counts": {
        "computed": 3,
        "pending_maturity": 1
      }
    },
    "scorecard": {
      "target_hit_count": 2,
      "stop_hit_count": 1,
      "collision_count": 0,
      "neither_count": 0,
      "pending_count": 1
    }
  },
  "items": [
    {
      "market_date": "2026-04-20",
      "calendar_status": "reviewed",
      "decision_count": 3,
      "stored_decision_count": 3,
      "reviewed_decision_count": 3,
      "pending_review_count": 0,
      "decision_actions": {
        "buy": 1,
        "reject": 1,
        "wait": 1
      },
      "evaluation_coverage": {
        "status": "complete"
      },
      "scorecard": {
        "target_hit_count": 2,
        "stop_hit_count": 1,
        "pending_count": 0
      }
    }
  ]
}
```


# GET /agent-context/signal-decider/calendar/{market_date}

## Summary
Selected-date replay review for all `signal-decider` decisions emitted on one market date, read from the persisted calendar-review artifacts. Each item keeps the original decision fields, adds D+1 or D+3 or D+5 replay summaries, and exposes both the structured replay narrative and direct calendar-facing aliases such as `quality_verdict`, `quality_reason`, `review_conclusion`, `path_outcome`, and `intraday_path_result`. Per-horizon coverage metadata makes immature windows and price-data gaps explicit.

## Example request
```http
GET /agent-context/signal-decider/calendar/2026-04-20?horizons=1&horizons=3&horizons=5&primary_horizon_days=5&evaluation_version=signal_decider_eval_v1
```

## Example response
```json
{
  "summary": {
    "count": 3,
    "review_status_counts": {
      "complete": 3
    },
    "insight_counts": {
      "buy_should_have_taken": 1,
      "wait_missed_opportunity": 1,
      "reject_correct": 1
    },
    "evaluation_coverage": {
      "primary_horizon_days": 5,
      "status": "complete"
    }
  },
  "items": [
    {
      "snapshot_id": 84,
      "symbol": "005930",
      "decision": {
        "action": "buy",
        "target_price": 105,
        "stop_loss_price": 96
      },
      "summary": "Breakout quality stayed constructive.",
      "review": {
        "status": "complete",
        "status_reason": "computed",
        "coverage": {
          "horizon_days": 5,
          "status_reason": "computed",
          "decision_market_date": "2026-04-20",
          "entry_trade_date": "2026-04-21",
          "observed_bars_count": 5,
          "required_bars_count": 5,
          "earliest_computable_date": "2026-04-27"
        },
        "quality_verdict": "buy_should_have_taken",
        "quality_reason": "Target was touched and the stop-loss was never touched inside the review window.",
        "review_conclusion": "Buy held up on replay.",
        "path_outcome": "target_hit",
        "intraday_path_result": "target_only",
        "insight": {
          "code": "buy_should_have_taken",
          "priority": "low",
          "label": "Buy held up",
          "label_key": "signal_decider.review.buy_should_have_taken"
        },
        "narrative": {
          "version": "signal_decider_replay_review_v1",
          "insight_code": "buy_should_have_taken",
          "insight_priority": "low",
          "headline": "Buy held up on replay.",
          "headline_key": "signal_decider.review.headline.buy_should_have_taken",
          "supporting_reasons": [
            {
              "code": "target_hit_without_stop",
              "message": "Target was touched and the stop-loss was never touched inside the review window.",
              "message_key": "signal_decider.review.reason.target_hit_without_stop"
            }
          ],
          "counterfactual_summary": null,
          "summary_text": "Buy held up on replay. Target was touched and the stop-loss was never touched inside the review window."
        },
        "narrative_text": "Buy held up on replay. Target was touched and the stop-loss was never touched inside the review window.",
        "path_analysis": {
          "path_marker": "target_hit",
          "first_trigger": "target_hit",
          "trigger_sequence": "target_only"
        },
        "horizons": [
          {
            "horizon_days": 1,
            "grade_label": "good_buy",
            "status_reason": "computed",
            "path_analysis": {
              "path_marker": "target_hit"
            }
          },
          {
            "horizon_days": 5,
            "grade_label": "good_buy",
            "status_reason": "computed",
            "path_analysis": {
              "path_marker": "target_hit"
            }
          }
        ],
        "outcome_horizons": [
          {
            "horizon_days": 1,
            "grade_label": "good_buy",
            "path_analysis": {
              "path_marker": "target_hit"
            }
          },
          {
            "horizon_days": 5,
            "grade_label": "good_buy",
            "path_analysis": {
              "path_marker": "target_hit"
            }
          }
        ]
      }
    }
  ]
}
```


# GET /agent-context/signal-decider/calendar/{market_date}/snapshots/{snapshot_id}

## Summary
Replay-only drilldown for one immutable decision snapshot on the selected date, backed by the persisted calendar-review detail payload. It returns the replay insight, the primary-horizon trigger checkpoints, and the observed price path used by the synthetic review model. Actual fills and broker P&L stay on `GET /agent-context/signal-decider/execution-outcomes`.

## Example request
```http
GET /agent-context/signal-decider/calendar/2026-04-20/snapshots/84?horizons=1&horizons=5&primary_horizon_days=5&evaluation_version=signal_decider_eval_v1
```

## Example response
```json
{
  "summary": {
    "market_date": "2026-04-20",
    "symbol": "005930",
    "review_status": "complete",
    "insight_code": "buy_should_have_taken",
    "insight_priority": "low",
    "headline": "Buy held up on replay.",
    "primary_path_marker": "target_hit"
  },
  "item": {
    "snapshot_id": 84,
    "symbol": "005930",
    "review": {
      "status": "complete",
      "status_reason": "computed",
      "quality_verdict": "buy_should_have_taken",
      "quality_reason": "Target was touched and the stop-loss was never touched inside the review window.",
      "review_conclusion": "Buy held up on replay.",
      "path_outcome": "target_hit",
      "intraday_path_result": "target_only",
      "insight": {
        "code": "buy_should_have_taken",
        "priority": "low"
      },
      "narrative": {
        "version": "signal_decider_replay_review_v1",
        "insight_code": "buy_should_have_taken",
        "insight_priority": "low",
        "headline": "Buy held up on replay.",
        "headline_key": "signal_decider.review.headline.buy_should_have_taken",
        "supporting_reasons": [
          {
            "code": "target_hit_without_stop",
            "message": "Target was touched and the stop-loss was never touched inside the review window.",
            "message_key": "signal_decider.review.reason.target_hit_without_stop"
          }
        ],
        "counterfactual_summary": null,
        "summary_text": "Buy held up on replay. Target was touched and the stop-loss was never touched inside the review window."
      },
      "narrative_text": "Buy held up on replay. Target was touched and the stop-loss was never touched inside the review window.",
      "path_analysis": {
        "path_marker": "target_hit",
        "checkpoints": {
          "entry_trade_date": "2026-04-21",
          "exit_trade_date": "2026-04-25",
          "first_target_hit_trade_date": "2026-04-21",
          "first_stop_loss_hit_trade_date": null
        }
      }
    },
    "observed_price_path": [
      {
        "trade_date": "2026-04-21",
        "open_price": 100,
        "high_price": 106,
        "low_price": 99,
        "close_price": 104
      }
    ]
  }
}
```


# GET /agent-context/signal-decider/calendar/maintenance

## Summary
Inspect the independent persisted calendar-review maintenance process. This route reports recent `agent-signal-decider-calendar-refresh` runs so operators can confirm the separate calendar artifact job is healthy without looking at the main daily pipeline.

## Example request
```http
GET /agent-context/signal-decider/calendar/maintenance?evaluation_version=signal_decider_eval_v1&limit=5
```

## Example response
```json
{
  "request": {
    "evaluation_version": "signal_decider_eval_v1",
    "limit": 5
  },
  "summary": {
    "count": 1,
    "latest_status": "completed",
    "latest_completed_at": "2026-04-30T09:05:00+00:00"
  },
  "items": [
    {
      "id": 17,
      "market_date_from": "2026-03-31",
      "market_date_to": "2026-04-30",
      "as_of_market_date": "2026-04-30",
      "lookback_days": 30,
      "evaluation_version": "signal_decider_eval_v1",
      "horizons": [1, 3, 5, 10],
      "primary_horizons": [1, 3, 5, 10],
      "trigger": "internal_ops_cli",
      "status": "completed",
      "summary": {
        "decision_count": 42,
        "review_rows_upserted": 168
      }
    }
  ]
}
```
