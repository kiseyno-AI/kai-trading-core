# GET /trading/orders/next-open-execution/overview

## Summary
Read the compact persisted next-open plan for web cards and monitoring price levels. Optional `target_trade_date` uses the same KRX session resolution as the full inspection endpoint. No refresh, execution, broker lookup, or plan mutation occurs.

The response excludes internal traceability, validation, submission, account identifiers, and linked orders. Prices and raw action are extracted as JSON scalar fields in the database; the full evidence blobs are not loaded. Missing stored prices remain null. `planned_count` counts eligible rows with status `planned`; `awaiting_trigger_count` counts all rows with that execution status. The result has one item per persisted account/strategy plan, so a symbol can appear more than once.

Authentication uses the existing `Authorization: Bearer` requirement when `UPSTREAM_BEARER_TOKEN` is configured. Hypothetical return calculation belongs to `/execution/runtime/monitoring-session-results`, not this endpoint or its web consumer.

## Example request
```http
GET /trading/orders/next-open-execution/overview?target_trade_date=2026-11-10
Authorization: Bearer <token>
```

## Example response
```json
{
  "summary": {
    "target_trade_date": "2026-11-10",
    "source_market_date": "2026-11-09",
    "items_total": 1,
    "planned_count": 0,
    "blocked_count": 0,
    "awaiting_trigger_count": 1
  },
  "items": [{
    "symbol": "005930",
    "decision_agent_name": "signal-decider",
    "raw_action": "wait",
    "trigger_price": 99000,
    "chase_limit_price": 101000,
    "target_price": 110000,
    "stop_loss_price": 95000
  }]
}
```

Invalid date query parameters return 422; invalid resolved dates return 400. Read failures return 500 with a generic detail and no internal diagnostic payload. Consumers should mark the plan unavailable while retaining independently available monitoring data; they should not fall back to the heavy inspection endpoint.
