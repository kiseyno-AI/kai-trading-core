# GET /agent-context/feedback/data-gap-summary

## Summary
Aggregates normalized agent missing-data feedback by date or date range so operators can prioritize backlog items and context wiring fixes.

## Example request
```http
GET /agent-context/feedback/data-gap-summary?market_date_from=2026-05-01&market_date_to=2026-05-06&agent_name=technical-summarizer&categories=liquidity_turnover_slippage
```

## Example response
```json
{
  "summary": {
    "feedback_rows_scanned": 24,
    "feedback_rows_matched": 7,
    "matched_note_count": 9,
    "category_count": 1
  },
  "categories": [
    {
      "category": "liquidity_turnover_slippage",
      "status": "partially_available",
      "internal_tables": ["daily_price_bars", "daily_price_contexts", "quote_snapshots", "current_quote_cache"],
      "external_providers": ["KIS quote/orderbook"],
      "counts": {
        "feedback_rows": 7,
        "note_mentions": 9,
        "symbols": 6,
        "roles": 2
      },
      "affected_symbols": ["000660", "005930"],
      "example_notes": ["거래대금과 슬리피지 근거 필요"]
    }
  ]
}
```
