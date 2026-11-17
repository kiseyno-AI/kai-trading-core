# GET /agent-context/feedback/payloads

## Summary
Reads stored normalized feedback payloads for one or more symbols. Decision-support roles persist a machine-readable `decision_delta` block inside the payload so consumers can compare repeated WAIT or WATCH recommendations without diffing prose.

Set `watch_only=true` for watchlist consumers. Core filters the current row for each requested date, symbol, and decider before applying `limit`.

- `signal-decider` requires `next_day_open_action=wait`.
- `short-term-decider` requires `tactical_action=wait` or `tactical_action=watch_for_trigger`.
- Both lanes accept their corresponding `preferred_action` when the primary action is absent or null. A supplied `preferred_action` must also be watch-eligible. Core compares actions case-insensitively and trims surrounding spaces. Generic `decision_action` has no watch authority.

A symbol with one eligible lane returns one row. A symbol with two eligible lanes returns two rows, each with its own prices. The default audit read includes all stored actions and roles.

## Example request
```http
GET /agent-context/feedback/payloads?market_date=2026-04-15&symbols=005930&watch_only=true
```

## Example response
```json
{
  "request": {"market_date": "2026-04-15", "symbols": ["005930"], "agent_name": null, "limit": 50, "watch_only": true},
  "summary": {"count": 1},
  "items": [{"market_date": "2026-04-15", "symbol": "005930", "agent_name": "signal-decider", "payload": {"role": "signal-decider", "preferred_action": "wait", "next_day_open_action": "wait", "decision_delta": {"has_previous_decision": true, "transition": "wait_to_wait", "persistence_trend": "persistent"}}, "missing_data_categories": [{"category": "technical_evidence_support_resistance", "status": "partially_available"}]}]
}
```
