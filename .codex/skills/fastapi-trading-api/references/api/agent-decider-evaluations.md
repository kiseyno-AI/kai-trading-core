# POST /agent-context/decider/evaluations/compute

## Summary
Computes or refreshes the generic durable 5-trading-day post-decision evaluation rows for persisted `signal-decider` and `short-term-decider` feedback. The current slice uses stored daily OHLC bars only and records one upserted row per feedback source into `decider_post_decision_evaluations`.

By default the compute route now gates on same-day feedback prerequisites for the requested date window. If fundamentals, filtering, or factor/scoring stages are still pending or blocked for the relevant market date, the response returns `summary.status=waiting|blocked`, `summary.gated=true`, and a `prerequisites` block instead of refreshing evaluations. Operators can bypass that check with `force_recompute=true`.

The current slice supports only `window_days=5`. For each agent/date/symbol
cohort, compute selects the newest feedback revision before checking snapshot
validity. An invalid newest revision is excluded and increments
`summary.latest_authority_rows_excluded_invalid`; it never revives an older valid
decision as current authority.

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
response reports `costs_explicit`, `cost_inputs_provided`, and
`missing_cost_components`. A partially specified ledger carries
`partial_cost_assumption_missing:<components>`; an all-zero effective cost
assumption carries `zero_cost_assumption_may_overstate_paper_return`.

The optional `short_term_avoid_chase_shadow_v1` rule is report-only and never
grants live execution authority. It is eligible only when enabled for an
`avoid_chase` decision and every gate passes: `wait_reason.code` is
`extended_entry_price`; market regime is `normal`; return percentile is at least
`0.75`; the setup is `breakout_20d` or one of `confirmed`, `continuation`,
`breakout`, `pullback_support`, and `reclaim`, with no `breakdown_20d`;
reward/risk is at least `2.0`; and raw-source trading value is at least KRW
`3,000,000,000`.

Short-term `watch_for_trigger` evaluations validate their levels before the
five-bar maturity gate. Missing or non-positive levels, or anything other than
`stop < trigger <= chase_limit < target`, immediately returns
`outcome_label="invalid_no_price_levels"`, even before five bars exist. With
valid levels, both an untriggered watch and a triggered but unresolved watch
remain immature until five complete daily OHLC bars are available and return
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
