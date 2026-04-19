# Agent Feedback Session Cleanup API

## Summary
Immediate-delete plus failed-only retry cleanup for OpenClaw feedback sessions after Core has already stored normalized feedback rows.

## Routes
- `POST /agent-context/feedback/session-cleanup/run`
- `GET /agent-context/feedback/session-cleanup`

## POST /agent-context/feedback/session-cleanup/run
Retries only previously failed session deletions with `openclaw gateway call sessions.delete`. Normal successful deletions happen immediately after feedback storage and are not persisted.

### Example request
```http
POST /agent-context/feedback/session-cleanup/run
Content-Type: application/json

{
  "limit": 100
}
```

### Example response
```json
{
  "ok": true,
  "summary": {
    "selected": 12,
    "succeeded": 10,
    "failed": 2
  },
  "items": [
    {
      "id": 41,
      "market_date": "2026-04-19",
      "symbol": "005930",
      "agent_name": "eod-summarizer",
      "run_id": "symbol-run-2026-04-19-abcd1234",
      "session_key": "feedback/eod-summarizer/slot-01",
      "status": "deleted",
      "cleanup_result": {
        "ok": true,
        "deleted": true,
        "parsed": {
          "ok": true,
          "deleted": true,
          "key": "feedback/eod-summarizer/slot-01"
        }
      }
    }
  ]
}
```

## GET /agent-context/feedback/session-cleanup
Lists only retained failed session-deletion rows and their latest retry state.

### Example request
```http
GET /agent-context/feedback/session-cleanup?market_date=2026-04-19&agent_name=technical-summarizer&limit=50
```

### Example response
```json
{
  "items": [
    {
      "id": 52,
      "market_date": "2026-04-19",
      "symbol": "000660",
      "agent_name": "technical-summarizer",
      "run_id": "symbol-run-2026-04-19-abcd1234",
      "session_key": "feedback/technical-summarizer/slot-03",
      "cleanup_attempts": 2,
      "last_attempt_at": "2026-04-19T04:35:00.000000+00:00",
      "next_attempt_at": "2026-04-19T04:37:00.000000+00:00",
      "last_error": "session delete failed"
    }
  ]
}
```
