# GET /agent-context/signal-decider/execution-outcomes

## Summary
Reads dedicated actual execution outcomes for `signal-decider` and `short-term-decider` next-open plan trades. These rows stay separate from synthetic bar-based evaluation outcomes and link the decision snapshot to next-open entry execution, linked intraday auto-sell exits, stored broker orders, exec-runtime readback fills, fill events, and realized execution metrics. Short-term outcomes feed the WAIT-conflict weekly replay; unresolved/open entries remain explicit and receive no synthetic P/L.

Rows are materialized from persisted core data only. Exec readback persistence synchronizes the affected outcome rows immediately so replay selection sees newly persisted fills. Use `refresh=true` on this read for a bounded filtered refresh, or run the internal one-shot backfill command from `apps/api`:

```bash
python3.11 -m app.internal.ops agent-signal-decider-execution-outcomes-backfill --market-date-from 2026-06-29 --market-date-to 2026-07-03
```

## Example request
```http
GET /agent-context/signal-decider/execution-outcomes?market_date_from=2026-04-20&market_date_to=2026-04-30&symbol=005930&account_reference=KIS:12345678-01&lifecycle_status=closed&exit_reason=target_hit&limit=20
```

## Example response
```json
{
  "request": {
    "snapshot_id": null,
    "market_date": null,
    "market_date_from": "2026-04-20",
    "market_date_to": "2026-04-30",
    "symbol": "005930",
    "account_reference": "KIS:12345678-01",
    "policy_version": null,
    "lifecycle_status": "closed",
    "exit_reason": "target_hit",
    "refresh": false,
    "limit": 20
  },
  "summary": {
    "count": 1,
    "closed_count": 1,
    "open_count": 0,
    "unresolved_count": 0,
    "rejected_count": 0,
    "lifecycle_status_counts": {
      "closed": 1
    },
    "exit_reason_counts": {
      "target_hit": 1
    },
    "total_closed_quantity": "5.00000000",
    "total_open_quantity": "0.00000000",
    "total_realized_pnl_krw": "20000.00"
  },
  "items": [
    {
      "outcome_id": 7,
      "decision_snapshot_id": 84,
      "entry_plan_item_id": 301,
      "exit_plan_item_id": 417,
      "entry_order_request_id": 910,
      "entry_broker_order_row_id": 911,
      "exit_order_request_id": 922,
      "exit_broker_order_row_id": 923,
      "broker_name": "kis",
      "market_date": "2026-04-29",
      "source_market_date": "2026-04-29",
      "target_trade_date": "2026-04-30",
      "account_reference": "KIS:12345678-01",
      "symbol": "005930",
      "policy_version": "signal_decider_policy_v1",
      "entry_side": "buy",
      "exit_side": "sell",
      "lifecycle_status": "closed",
      "exit_reason": "target_hit",
      "entry_submitted_at": "2026-04-30T00:01:00+00:00",
      "entry_filled_at": "2026-04-30T00:01:30+00:00",
      "exit_submitted_at": "2026-04-30T03:00:00+00:00",
      "exit_filled_at": "2026-04-30T03:05:00+00:00",
      "entry_filled_quantity": "5.00000000",
      "exit_filled_quantity": "5.00000000",
      "closed_quantity": "5.00000000",
      "open_quantity": "0.00000000",
      "entry_average_fill_price": 50000,
      "exit_average_fill_price": 54000,
      "entry_notional_krw": "250000.00",
      "exit_notional_krw": "270000.00",
      "realized_pnl_krw": "20000.00",
      "realized_return_pct": 0.08,
      "entry_client_order_id": "SDNO-20260430-005930",
      "exit_client_order_ids": ["SDIS-20260430-005930-TGT-A1"],
      "diagnostics": {
        "entry": {
          "order_status": "filled"
        },
        "exit": {
          "order_status": "filled",
          "trigger_rule": "target_hit"
        }
      }
    }
  ]
}
```
