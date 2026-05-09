# GET /agent-context/feedback/payloads

## Summary
Reads stored normalized feedback payloads for one or more symbols. Decision-support roles persist a machine-readable `decision_delta` block inside the payload so consumers can compare repeated WAIT or WATCH recommendations without diffing prose.

## Example request
```http
GET /agent-context/feedback/payloads?market_date=2026-04-15&symbols=005930
```

## Example response
```json
{
  "items": [{"market_date": "2026-04-15", "symbol": "005930", "agent_name": "signal-decider", "payload": {"role": "signal-decider", "preferred_action": "wait", "next_day_open_action": "wait", "decision_delta": {"has_previous_decision": true, "transition": "wait_to_wait", "persistence_trend": "persistent"}}, "missing_data_categories": [{"category": "technical_evidence_support_resistance", "status": "partially_available"}]}]
}
```
