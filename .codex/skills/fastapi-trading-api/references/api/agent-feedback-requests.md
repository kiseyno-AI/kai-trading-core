# GET /agent-context/feedback/requests

## Summary
Reads stored agent request/context payloads for one or more symbols.

## Example request
```http
GET /agent-context/feedback/requests?market_date=2026-04-15&symbols=005930
```

## Example response
```json
{
  "items": [{"market_date": "2026-04-15", "symbol": "005930", "agent_name": "technical-summarizer", "payload": {"symbol": "005930", "technical_snapshot": {}}}]
}
```
