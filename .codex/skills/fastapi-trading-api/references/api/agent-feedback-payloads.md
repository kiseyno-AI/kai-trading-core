# GET /agent-context/feedback/payloads

## Summary
Reads stored normalized feedback payloads for one or more symbols.

## Example request
```http
GET /agent-context/feedback/payloads?market_date=2026-04-15&symbols=005930
```

## Example response
```json
{
  "items": [{"market_date": "2026-04-15", "symbol": "005930", "agent_name": "technical-summarizer", "payload": {"role": "technical-summarizer", "summary": "Trend remains constructive."}}]
}
```
