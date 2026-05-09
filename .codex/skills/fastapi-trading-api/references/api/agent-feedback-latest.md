# GET /agent-context/feedback/latest

## Summary
Returns the latest reusable feedback bundle per symbol and role for decision-agent consumption.

Each role item includes `latest_feedback.run_id` when stored feedback exists, so operators can trace a reusable symbol summary back to the durable run that wrote it. Decision-support roles also carry their persisted `decision_delta` inside `latest_feedback.feedback_payload` so the next decider run can hydrate prior WAIT or WATCH state directly from this bundle.

## Example request
```http
GET /agent-context/feedback/latest?market_date=2026-04-15&symbols=005930&symbols=000660
```

## Example response
```json
{
  "request": {
    "market_date": "2026-04-15",
    "symbols": ["005930", "000660"],
    "roles": ["eod-summarizer", "technical-summarizer", "fundamental-summarizer"]
  },
  "items": [
    {
      "symbol": "005930",
      "roles": [
        {
          "role": "fundamental-summarizer",
          "should_dispatch": false,
          "reason": "reuse_latest_feedback",
          "latest_feedback": {
            "run_id": "feedback-cycle-2026-04-15-ab12cd34ef",
            "feedback_payload": {
              "role": "fundamental-summarizer",
              "summary": "Recent DART filings remain unchanged."
            }
          }
        }
      ]
    }
  ]
}
```
