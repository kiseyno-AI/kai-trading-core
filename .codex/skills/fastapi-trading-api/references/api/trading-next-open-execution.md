# GET /trading/orders/next-open-execution and POST /trading/orders/next-open-execution/run-if-due

## Summary
Inspect or execute the persisted next-open buy plan derived from the latest previous-trading-day decision snapshots. The planner now includes both classic `signal-decider` next-open buys and actionable `short-term-decider` tactical candidates, stores blocked-vs-eligible validation results durably with explicit decider identity, and the submit hook either reuses the existing trading-order persistence flow or, when `SIGNAL_DECIDER_EXEC_INTEGRATION_MODE=dispatch`, translates eligible rows into normalized exec plan items and delegates sizing/execution authority to `kai-trading-exec`.

For dispatch-mode buy candidates, core sends sizing context as a candidate contract (`quantity_policy.type='exec_agent_sized'`) so exec can make final share decisions using account/feedback/context rails, budgets, reference/entry context, confidence, and target/stop metadata. Core-local disabled mode keeps the legacy fixed-share signal-decider submit path.

Short-term validation now preserves the decider request's `pre_decider_enrichment_context` in both `validation.pre_decider_enrichment_context` and `validation.short_term.pre_decider_enrichment_context`. Operators can inspect the quote/orderbook snapshot status, raw-vs-estimated trading value provenance, investor/short-selling flow, and sector-peer context that were available before dispatch validation.

In exec-dispatch mode, watcher candidates are not blocked solely because `data_quality_warnings` are present. Core carries those warnings into the validation and exec handoff so the exec-side account-aware agent can make the final sizing/risk decision.

When short-term candidates are present but `SIGNAL_DECIDER_NEXT_OPEN_ACCOUNT_TARGETS` is empty, rows are still materialized with an explicit synthetic routing rail:
- `account_reference = KIS:00000000-00`
- `account_label = UNSPECIFIED_ROUTE`
- `allocation_mode = unspecified_route` in traceability/risk context

In that case the synthetic rail is treated as a budget/route placeholder, not a hard pre-priced share budget.

## Example request
```http
GET /trading/orders/next-open-execution?target_trade_date=2026-04-30&refresh=true
```

```http
POST /trading/orders/next-open-execution/run-if-due
Content-Type: application/json

{
  "target_trade_date": "2026-04-30",
  "dry_run": false,
  "force": false
}
```

## Example response
```json
{
  "summary": {
    "target_trade_date": "2026-04-30",
    "source_market_date": "2026-04-29",
    "items_total": 2,
    "eligible_count": 2,
    "blocked_count": 0,
    "submitted_count": 0,
    "delegated_for_exec_agent_sizing_count": 0,
    "decision_agent_counts": {
      "short-term-decider": 1,
      "signal-decider": 1
    }
  },
  "items": [
    {
      "plan_item_id": 81,
      "symbol": "005930",
      "account_reference": "KIS:12345678-01",
      "agent_name": "signal-decider",
      "decision_agent_name": "signal-decider",
      "decision_snapshot_id": 144,
      "planned_order_type": "market",
      "planned_quantity": null,
      "estimated_reference_price": 50000,
      "eligibility_status": "eligible",
      "execution_status": "planned",
      "client_order_id": "SDNO-20260430-005930",
      "traceability": {
        "source_market_date": "2026-04-29",
        "target_trade_date": "2026-04-30",
        "decision_run_id": "signal-run-144",
        "source_feedback_id": 701
      }
    },
    {
      "plan_item_id": 82,
      "symbol": "005930",
      "account_reference": "KIS:12345678-01",
      "agent_name": "short-term-decider",
      "decision_agent_name": "short-term-decider",
      "decision_snapshot_id": 145,
      "planned_order_type": "limit",
      "planned_quantity": "30.00000000",
      "estimated_reference_price": 51500,
      "eligibility_status": "eligible",
      "execution_status": "planned",
      "client_order_id": "STDE-20260430-005930",
      "validation": {
        "decision_agent_name": "short-term-decider",
        "short_term": {
          "tactical_action": "watch_for_trigger",
          "trigger_price": 50500,
          "chase_limit_price": 51500,
          "target_price": 54000,
          "stop_loss_price": 49000,
          "expected_holding_period": "1-2 trading days",
          "pre_decider_enrichment_context": {
            "schema_version": "pre_decider_enrichment_v1",
            "quote_orderbook_snapshot": {
              "status": "stale_eod",
              "snapshot_type": "eod_stale",
              "quote_freshness_timestamp": "2026-04-29T15:30:00+09:00"
            },
            "trading_value": {
              "status": "available",
              "source": "raw",
              "trading_value_krw": 9870000000
            },
            "flow_short_sector_context": {
              "status": "available"
            }
          }
        }
      },
      "traceability": {
        "source_market_date": "2026-04-29",
        "target_trade_date": "2026-04-30",
        "decision_run_id": "short-run-145",
        "source_feedback_id": 702,
        "tactical_action": "watch_for_trigger",
        "trigger_price": 50500,
        "chase_limit_price": 51500
      }
    }
  ]
}
```

Signal-decider rows keep the existing market-open submit behavior only in core-local mode. `short-term-decider` rows are only mapped when the tactical action is actionable, and in core-local mode they remain planned-only because trigger/chase semantics are not translated into live market orders locally. In exec dispatch mode, both signal and short-term entry legs are sent at/after 08:00 KST as exec-agent-sized candidates; accepted rows without an agent-approved positive quantity move to `execution_status="delegated_for_exec_agent_sizing"`, and the delegated linkage or audit state is stored under `submission.exec_dispatch` for each item, including decider-aware `plan_id`, remote item ids, last dispatch attempt, and the latest matched readback snapshot.
