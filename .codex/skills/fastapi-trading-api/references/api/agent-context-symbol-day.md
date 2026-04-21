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
  "request": {
    "market_date": "2026-04-15",
    "symbols": ["005930"],
    "fs_div": "CFS",
    "disclosures_limit": 5
  },
  "summary": {
    "requested_symbol_count": 1,
    "matched_symbol_count": 1,
    "price_context_count": 1
  },
  "items": [
    {
      "symbol": "005930",
      "market_date": "2026-04-15",
      "company": {"name_ko": "삼성전자", "market": "KOSPI"},
      "price_context": {
        "trade_date": "2026-04-15",
        "open_price": 70800,
        "high_price": 72500,
        "low_price": 70300,
        "close_price": 72100,
        "candle": {"direction": "bullish", "body_size": "medium", "wick_bias": "upper"},
        "session_range": {
          "points": 2200,
          "percent_of_open": 3.1073,
          "close_position_in_range": 0.8182,
          "close_position_label": "near_high"
        },
        "gap": {
          "previous_trade_date": "2026-04-14",
          "previous_close_price": 70400,
          "gap_points": 400,
          "gap_percent": 0.5682,
          "direction": "up",
          "filled_intraday": true
        }
      },
      "technical_snapshot": {"market_date": "2026-04-15", "trend_state": "uptrend"},
      "action_summary": {"market_date": "2026-04-15", "action_label": "hold"},
      "fundamentals": {"latest_business_year": 2025},
      "recent_disclosures": {"count": 5, "items": []}
    }
  ]
}
```
