# GET /agent-context/feedback/status

## Summary
Reports latest-run workflow status for the expected three-agent set.

## Example request
```http
GET /agent-context/feedback/status?market_date=2026-04-15&symbols=005930&symbols=000660
```

## Example response
```json
{
  "market_date": "2026-04-15",
  "items": [{"symbol": "005930", "status": "completed", "completed_count": 3, "expected_count": 3, "pending_agents": []}]
}
```
