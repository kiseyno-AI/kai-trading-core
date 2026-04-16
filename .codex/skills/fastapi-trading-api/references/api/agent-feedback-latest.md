# GET /agent-context/feedback/latest

## Summary
Returns the latest reusable feedback bundle per symbol and role for decision-agent consumption.

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
