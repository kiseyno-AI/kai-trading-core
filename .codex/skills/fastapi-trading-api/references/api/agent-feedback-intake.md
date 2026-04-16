# POST /agent-context/feedback

## Summary
Stores externally produced agent feedback into `agent_context_feedback`.

## Example request
```http
POST /agent-context/feedback
Content-Type: application/json

{
  "market_date": "2026-04-15",
  "symbol": "005930",
  "agent_name": "fundamental-summarizer",
  "summary": {},
  "feedback": {"role": "fundamental-summarizer", "summary": "No material DART change detected."},
  "context": {}
}
```

## Example response
```json
{
  "id": 12345,
  "market_date": "2026-04-15",
  "symbol": "005930",
  "agent": {"name": "fundamental-summarizer", "version": null, "run_id": null}
}
```
