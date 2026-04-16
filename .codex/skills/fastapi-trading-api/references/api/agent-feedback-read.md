# GET /agent-context/feedback

## Summary
Reads stored agent feedback rows by date, symbol, and agent name.

## Example request
```http
GET /agent-context/feedback?market_date=2026-04-15&symbols=005930&agent_name=fundamental-summarizer&limit=20
```

## Example response
```json
{
  "items": [{"market_date": "2026-04-15", "symbol": "005930", "agent": {"name": "fundamental-summarizer"}, "feedback": {"role": "fundamental-summarizer", "summary": "No material DART change detected."}}]
}
```
