# POST /agent-context/decider/evaluations/compute

## Summary
Computes or refreshes the generic durable 5-trading-day post-decision evaluation rows for persisted `signal-decider` and `short-term-decider` feedback. The current slice uses stored daily OHLC bars only and records one upserted row per feedback source into `decider_post_decision_evaluations`.

By default the compute route now gates on same-day feedback prerequisites for the requested date window. If fundamentals, filtering, or factor/scoring stages are still pending or blocked for the relevant market date, the response returns `summary.status=waiting|blocked`, `summary.gated=true`, and a `prerequisites` block instead of refreshing evaluations. Operators can bypass that check with `force_recompute=true`.

## Example request
```http
POST /agent-context/decider/evaluations/compute
Content-Type: application/json

{
  "market_date_from": "2026-04-20",
  "market_date_to": "2026-04-23",
  "agent_names": ["signal-decider", "short-term-decider"],
  "symbols": ["005930"],
  "window_days": 5
}
```

## Example response
```json
{
  "summary": {
    "feedback_rows_considered": 4,
    "evaluations_upserted": 4,
    "outcome_label_counts": {
      "target_hit_before_stop": 2,
      "not_applicable_wait_or_reject": 2
    }
  }
}
```


# GET /agent-context/decider/evaluations

## Summary
Reads the stored generic decider post-decision evaluation rows back with explicit source references, labels, and compact metadata that the UI can render directly.

## Example request
```http
GET /agent-context/decider/evaluations?market_date=2026-04-23&agent_names=signal-decider&symbols=005930&window_days=5
```

## Example response
```json
{
  "items": [
    {
      "evaluation_id": 7,
      "source": {
        "feedback_id": 11,
        "decision_snapshot_id": 84
      },
      "agent_name": "signal-decider",
      "symbol": "005930",
      "market_date": "2026-04-23",
      "window_days": 5,
      "action": "buy",
      "entry_trade_date": "2026-04-24",
      "entry_price": 80100,
      "target_price": 84600,
      "stop_loss_price": 79200,
      "first_target_hit_trade_date": "2026-04-25",
      "first_stop_hit_trade_date": null,
      "mfe_pct": 0.0612,
      "mae_pct": -0.0137,
      "outcome_label": "target_hit_before_stop",
      "outcome_reason": "Target was first reachable on 2026-04-25 before any stop-loss touch.",
      "data_quality_warnings": [],
      "metadata": {
        "market_data_source": "daily_price_bars",
        "entry_price_basis": "next_open"
      }
    }
  ]
}
```
