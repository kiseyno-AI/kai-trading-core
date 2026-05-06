# GET /trading/orders/next-open-execution and POST /trading/orders/next-open-execution/run-if-due

## Summary
Inspect or execute the persisted next-open buy plan derived from the latest previous-trading-day decision snapshots. The planner now includes both classic `signal-decider` next-open buys and actionable `short-term-decider` tactical candidates, stores blocked-vs-eligible validation results durably with explicit decider identity, and the submit hook either reuses the existing trading-order persistence flow or, when `SIGNAL_DECIDER_EXEC_INTEGRATION_MODE=dispatch`, translates eligible rows into normalized exec plan items and dispatches them to `kai-trading-exec`.

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
      "planned_quantity": "30.00000000",
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
          "expected_holding_period": "1-2 trading days"
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

Signal-decider rows keep the existing market-open submit behavior. `short-term-decider` rows are only mapped when the tactical action is actionable, and in core-local mode they remain planned-only because trigger/chase semantics are not translated into live market orders locally. In exec dispatch mode those short-term entry legs are sent as `pending_trigger_limit` candidates, and the delegated linkage or audit state is stored under `submission.exec_dispatch` for each item, including decider-aware `plan_id`, remote item ids, last dispatch attempt, and the latest matched readback snapshot.
