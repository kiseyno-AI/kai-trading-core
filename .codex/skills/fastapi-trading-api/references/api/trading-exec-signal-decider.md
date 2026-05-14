# GET /trading/orders/exec/signal-decider

## Summary
Read the core-owned aggregated core↔exec view for decider-driven next-open execution. The route name is legacy, but the payload now starts from persisted `signal_decider_execution_plan_items` for both `signal-decider` and actionable `short-term-decider` rows, overlays stored exec dispatch linkage and audit state, and with `refresh=true` calls the exec northbound runtime/order/execution/position APIs before persisting the latest matched readback snapshot back onto the durable plan rows.

Short-term entries dispatched in `SIGNAL_DECIDER_EXEC_INTEGRATION_MODE=dispatch` now use the `exec_agent_sized` contract on core→exec translation, so they are routed as candidate entries and exec performs final sizing while preserving fixed-quantity semantics for existing signal-decider flow.

Each exec item also carries `risk_context.decider_handoff_context`, which preserves the decider request payload, input feature snapshot payload, pre-decider enrichment context, full response payload/summary, normalized tactical output fields, risk/data-quality warnings, and traceability ids. Accepted short-term rows remain delegated sizing candidates in core (`execution_status="delegated_for_exec_agent_sizing"`) until exec readback reports scheduled/submitted/filled/monitoring outcomes.

## Example request
```http
GET /trading/orders/exec/signal-decider?target_trade_date=2026-04-30&symbol=005930&refresh=true
```

## Example response
```json
{
  "request": {
    "target_trade_date": "2026-04-30",
    "account_reference": null,
    "symbol": "005930",
    "refresh": true
  },
  "integration": {
    "mode": "dispatch",
    "enabled": true,
    "delegate_execution": true,
    "config_status": "ready"
  },
  "runtime": {
    "status": "ok",
    "payload": {
      "mode": "mock",
      "ready": true
    }
  },
  "summary": {
    "items_total": 2,
    "eligible_count": 2,
    "blocked_count": 0,
    "decision_agent_counts": {
      "short-term-decider": 1,
      "signal-decider": 1
    },
    "dispatch_status_counts": {
      "accepted": 2
    },
    "remote_status_counts": {
      "open": 2
    },
    "remote_open_count": 2,
    "remote_closed_count": 0
  },
  "items": [
    {
      "plan_item_id": 81,
      "symbol": "005930",
      "account_reference": "KIS:12345678-01",
      "agent_name": "signal-decider",
      "decision_agent_name": "signal-decider",
      "decision_snapshot_id": 144,
      "target_price": 54000,
      "stop_loss_price": 47000,
      "dispatch": {
        "status": "accepted",
        "plan_id": "core-signal-decider-20260430-KIS_12345678_01",
        "item_links": [
          {
            "intent": "next_open_buy",
            "exec_plan_item_id": "exec-item-1"
          },
          {
            "intent": "target_sell",
            "exec_plan_item_id": "exec-item-2"
          },
          {
            "intent": "stop_sell",
            "exec_plan_item_id": "exec-item-3"
          }
        ]
      },
      "remote_state": {
        "last_synced_at": "2026-04-30T00:06:00+00:00",
        "derived_status": "open",
        "entry_filled_quantity": "5",
        "exit_filled_quantity": null,
        "open_quantity": "5",
        "closed_quantity": "0",
        "orders": [
          {
            "id": "order-1",
            "execution_plan_item_id": "exec-item-1",
            "symbol": "005930",
            "status": "submitted"
          }
        ],
        "executions": [
          {
            "broker_order_id": "order-1",
            "symbol": "005930",
            "side": "buy",
            "quantity": "5"
          }
        ],
        "positions": [
          {
            "symbol": "005930",
            "quantity": "5"
          }
        ]
      }
    },
    {
      "plan_item_id": 82,
      "symbol": "005930",
      "account_reference": "KIS:12345678-01",
      "agent_name": "short-term-decider",
      "decision_agent_name": "short-term-decider",
      "decision_snapshot_id": 145,
      "execution_status": "delegated_for_exec_agent_sizing",
      "target_price": 54000,
      "stop_loss_price": 49000,
      "trigger_price": 50500,
      "chase_limit_price": 51500,
      "decision_run_id": "short-run-145",
      "source_feedback_id": 702,
      "tactical_action": "watch_for_trigger",
      "expected_holding_period": "1-2 trading days",
      "entry_execution_mode": "pending_trigger_limit",
      "dispatch": {
        "status": "accepted",
        "plan_id": "core-short_term_decider-20260430-KIS_12345678_01",
        "item_links": [
          {
            "intent": "next_open_buy",
            "exec_plan_item_id": "exec-item-4"
          },
          {
            "intent": "target_sell",
            "exec_plan_item_id": "exec-item-5"
          },
          {
            "intent": "stop_sell",
            "exec_plan_item_id": "exec-item-6"
          }
        ]
      },
      "remote_state": {
        "last_synced_at": "2026-04-30T00:06:00+00:00",
        "derived_status": "open",
        "entry_filled_quantity": "5",
        "exit_filled_quantity": null,
        "open_quantity": "5",
        "closed_quantity": "0"
      }
    }
  ]
}
```

Dispatch is grouped by `account_reference + decision_agent_name`, so same-symbol same-date rows from different deciders do not collide. `signal-decider` entry legs keep the existing next-open market semantics, while `short-term-decider` entry legs are dispatched as `pending_trigger_limit` candidates using `trigger_price` and `chase_limit_price`.
