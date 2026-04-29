# GET /trading/orders/next-open-execution and POST /trading/orders/next-open-execution/run-if-due

## Summary
Inspect or execute the persisted next-open buy plan derived from the latest previous-trading-day `signal_decider_decision_snapshots`. The planner maps decisions to configured KIS accounts, stores blocked-vs-eligible validation results durably, and the submit hook either reuses the existing trading-order persistence flow or, when `SIGNAL_DECIDER_EXEC_INTEGRATION_MODE=dispatch`, translates eligible rows into normalized exec plan items and dispatches them to `kai-trading-exec`.

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
    "eligible_count": 1,
    "blocked_count": 1,
    "submitted_count": 1
  },
  "items": [
    {
      "plan_item_id": 81,
      "symbol": "005930",
      "account_reference": "KIS:12345678-01",
      "decision_snapshot_id": 144,
      "planned_order_type": "market",
      "planned_quantity": "30.00000000",
      "estimated_reference_price": 50000,
      "eligibility_status": "eligible",
      "execution_status": "submitted",
      "client_order_id": "SDNO-20260430-005930",
      "order_request_id": 901,
      "broker_order_row_id": 902
    },
    {
      "plan_item_id": 82,
      "symbol": "000660",
      "account_reference": "KIS:12345678-01",
      "eligibility_status": "blocked",
      "execution_status": "planned",
      "validation": {
        "blocking_reasons": [
          {
            "code": "data_quality_warnings_present",
            "message": "Snapshot has data quality warnings."
          }
        ]
      }
    }
  ]
}
```

When exec dispatch mode is enabled, the same plan rows stay visible through this route and the delegated linkage or audit state is stored under `submission.exec_dispatch` for each item, including the remote `plan_id`, remote item ids, last dispatch attempt, and latest matched readback snapshot.
