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
  "window_days": 5,
  "fee_bps": 10,
  "slippage_bps": 20,
  "tax_bps": 18,
  "enable_shadow_avoid_chase_rule": false
}
```

The three cost inputs are independently optional for compatibility. The
response reports whether every component was supplied and identifies any
missing components. The avoid-chase rule is report-only and never grants live
execution authority.

Short-term `watch_for_trigger` evaluations become terminal only after five
complete daily OHLC bars are available. Until then, both an untriggered watch
and a triggered but unresolved watch return
`outcome_label="invalid_missing_price_data"`,
`metadata.price_window_complete=false`, and `metadata.paper_ledger=null`.
Only a complete five-bar window with no valid trigger may return
`neither_hit_within_5d` with paper-ledger `exit_reason="no_entry"`.
This maturity contract is identified by
`evaluation_engine_version="decider_post_decision_eval_v3"` in the request
echo and `metadata.schema_version` on each refreshed row.

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
Reads the stored generic decider post-decision evaluation rows back with explicit source references, labels, and compact metadata that the UI can render directly. The default `include_history=false` selects the latest canonical revision for each agent/date/symbol/window cohort; `include_history=true` returns durable superseded revisions for audit.

## Example request
```http
GET /agent-context/decider/evaluations?market_date=2026-04-23&agent_names=signal-decider&symbols=005930&window_days=5&include_history=false
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
