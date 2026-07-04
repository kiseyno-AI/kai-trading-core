# GET /trading/orders/exec/signal-decider

## Summary
Read the core-owned aggregated core↔exec view for decider-driven next-open execution. The route name is legacy, but the payload now starts from persisted `signal_decider_execution_plan_items` for both `signal-decider` and actionable `short-term-decider` rows, overlays stored exec dispatch linkage and audit state, and with `refresh=true` calls the exec northbound runtime/order/execution/position APIs before persisting the latest matched readback snapshot back onto the durable plan rows.

Signal-decider and short-term entries dispatched in `SIGNAL_DECIDER_EXEC_INTEGRATION_MODE=dispatch` use the `exec_agent_sized` contract on core→exec translation, so they are routed as candidate entries and exec performs final sizing before materializing a buy. Classic signal-decider rows keep the `next_open_buy` intent. Short-term tactical rows use `intraday_watch_buy` with the decider's trigger and chase-limit prices so exec treats them as monitored intraday candidates, not unconditional next-open orders. The overview rows expose `raw_action`, `tactical_action`, `generic_action`, `handoff_intent`, `exec_monitoring_active`, `exec_agent_approved`, and `approval_state` directly so clients do not need to infer tactical state from nested validation payloads.

Each exec item also carries `risk_context.decider_handoff_context`, which preserves execution/risk context, input feature snapshots, pre-decider enrichment context, response payload/summary, normalized tactical output fields, risk/data-quality warnings, and traceability ids. Accepted rows without an agent-approved positive quantity remain delegated sizing candidates in core (`execution_status="delegated_for_exec_agent_sizing"`) until exec readback reports scheduled/submitted/filled/monitoring outcomes.

Exec handoffs now include `risk_context.decision_input_quality_context`, `risk_context.execution_feasibility_context`, and the same envelope under `risk_context.decider_handoff_context.decider_input_context.decision_input_quality_context`. The `quantity_policy` for exec-agent-sized buys also carries sizing hints such as `liquidity_tier`, `max_safe_notional_krw`, and `sizing_hints`. Exec should treat stale/fallback quote and missing raw turnover as hard blocks or major discounts, while empty news/disclosures stay neutral secondary context unless separate adverse-event evidence exists.

Buy-oriented exec handoffs also include `risk_context.market_regime_context` and the same payload under `risk_context.decider_handoff_context.decider_input_context.market_regime_context`. Core derives it from PyKRX market breadth, sector/category peer context, and market-relative strength with deterministic `normal`, `caution`, `risk_off`, `unavailable`, and `stale` posture semantics. This is the core-owned context contract for kiseyno-AI/kai-trading-exec#140, allowing exec-side buy approval to apply market/sector guardrails without querying core.

Short-term exec handoffs also carry signal-decider alignment audit metadata in `risk_context`: `signal_decider_alignment`, `alignment_gate_action`, `alignment_gate_reason`, `alignment_signal_decider_decision_snapshot_id`, and `alignment_gate`. The default live mode is report-only, so a same-symbol `WAIT` conflict can be visible as `alignment_gate_action="report_only_block_candidate"` without being represented as an actual skipped execution.

Core blocks classic signal-decider next-open exec dispatch when source-market-date PyKRX enrichment coverage is missing for `sector_classification`, `investor_flow_symbol`, or `short_selling`. Blocked dispatch responses expose `summary.reason="pykrx_enrichment_preflight_blocked"` and the full `enrichment_preflight` payload on `POST /trading/orders/next-open-execution/run-if-due`; actionable short-term watch candidates remain visible as conditional/degraded rows rather than being erased by the global preflight.

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
      "execution_status": "submitted",
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
      "raw_action": "watch_for_trigger",
      "tactical_action": "watch_for_trigger",
      "generic_action": "buy",
      "handoff_intent": "intraday_watch_buy",
      "handoff_active": true,
      "exec_monitoring_active": true,
      "exec_agent_approved": true,
      "approval_state": "approved",
      "target_price": 54000,
      "stop_loss_price": 49000,
      "signal_decider_alignment": "wait_conflict_block",
      "alignment_gate_action": "report_only_block_candidate",
      "alignment_gate_reason": "Signal-decider is WAIT for the same symbol/date context and the short-term setup did not pass the stronger override threshold.",
      "alignment_signal_decider_decision_snapshot_id": 144,
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
            "intent": "intraday_watch_buy",
            "trigger_price": 50500,
            "limit_price": 51500,
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

Dispatch is grouped by `account_reference + decision_agent_name`, so same-symbol same-date rows from different deciders do not collide. `signal-decider` entry legs keep the existing next-open market semantics, while `short-term-decider` entry legs are dispatched as `intraday_watch_buy` / `pending_trigger_limit` candidates using `trigger_price` and `chase_limit_price`.
