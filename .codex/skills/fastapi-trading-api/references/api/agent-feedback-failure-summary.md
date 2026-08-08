# Agent Feedback Failure Summary

## Summary
`GET /agent-context/feedback/failure-summary` returns compact failure groups from `agent_context_feedback_failures` for an exact market date or bounded date range. Groups are keyed by `market_date`, `run_id`, `agent_name`, and `failure_kind`, with counts, retryable counts, a small symbol sample, timestamps, and normalized failure taxonomy fields.

## Example Request
```http
GET /agent-context/feedback/failure-summary?market_date_from=2026-05-05&market_date_to=2026-05-06&agent_name=openclaw-gateway&failure_kind=openclaw_auth_rejected&limit=25
```

## Example Response
```json
{
  "request": {
    "market_date": null,
    "market_date_from": "2026-05-05",
    "market_date_to": "2026-05-06",
    "run_id": null,
    "agent_name": "openclaw-gateway",
    "failure_kind": "openclaw_auth_rejected",
    "limit": 25
  },
  "summary": {
    "group_count": 1,
    "failure_count": 1,
    "retryable_count": 0,
    "by_failure_kind": {
      "openclaw_auth_rejected": 1
    }
  },
  "groups": [
    {
      "market_date": "2026-05-05",
      "run_id": "symbol-run-2026-05-05-auth",
      "agent_name": "openclaw-gateway",
      "failure_kind": "openclaw_auth_rejected",
      "failure_status": "auth/rejected",
      "failure_category": "auth",
      "failure_count": 1,
      "retryable_count": 0,
      "symbols_sample": ["__RUN__"],
      "first_received_at": "2026-05-05T06:00:00+00:00",
      "latest_received_at": "2026-05-05T06:00:00+00:00"
    }
  ]
}
```
