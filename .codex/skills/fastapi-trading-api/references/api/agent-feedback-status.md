# GET /agent-context/feedback/status

## Summary
Reports latest-run workflow status for the default five-role symbol feedback workflow.

When upstream fundamentals, filtering, or factor/scoring stages are not ready yet, symbol items now surface `waiting` or `blocked` instead of a generic `pending`, and the response includes a top-level `prerequisites` block describing the missing same-day stages.

## Example request
```http
GET /agent-context/feedback/status?market_date=2026-04-15&symbols=005930&symbols=000660
```

## Example response
```json
{
  "market_date": "2026-04-15",
  "status": "waiting",
  "prerequisites": {
    "status": "waiting",
    "waiting_stage_ids": ["quality_factors", "composite_factors"]
  },
  "items": [
    {
      "symbol": "005930",
      "status": "waiting",
      "completed_count": 0,
      "expected_count": 5,
      "pending_agents": [
        "eod-summarizer",
        "technical-summarizer",
        "fundamental-summarizer",
        "short-term-decider",
        "signal-decider"
      ],
      "prerequisites_ready": false,
      "missing_prerequisites": [
        {
          "stage": "quality_factors",
          "gate_status": "waiting"
        }
      ]
    }
  ]
}
```
