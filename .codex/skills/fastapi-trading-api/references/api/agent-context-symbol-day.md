# GET /agent-context/symbol-day

## Summary
Returns compact symbol/day analysis context for downstream agents.

## Example request
```http
GET /agent-context/symbol-day?market_date=2026-04-15&symbols=005930&fs_div=CFS&disclosures_limit=5
```

## Example response
```json
{
  "market_date": "2026-04-15",
  "items": [
    {
      "symbol": "005930",
      "company": {"name": "삼성전자", "market": "KOSPI"},
      "technical_snapshot": {"market_date": "2026-04-15", "trend_state": "uptrend"},
      "action_summary": {"market_date": "2026-04-15", "action_label": "hold"},
      "fundamentals": {"latest_business_year": 2025},
      "recent_disclosures": {"count": 5, "items": []}
    }
  ]
}
```
