# GET /agent-context/symbol-day

## Summary
Returns compact symbol/day analysis context for downstream agents. The `price_context` block is read from persisted `daily_price_contexts`, not derived on request, and `price_context_history` carries up to the latest seven stored rows ending at the requested `market_date`. The same response now includes a structured `scores` block for unified value, quality, and momentum when those stored factor rows exist for the requested date.

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
      "price_context_history": [
        {
          "market_date": "2026-04-14",
          "source": "pykrx",
          "captured_at": "2026-04-14T15:30:00+09:00",
          "price_context": {
            "trade_date": "2026-04-14",
            "close_price": 70400,
            "candle": {"direction": "bullish"}
          }
        },
        {
          "market_date": "2026-04-15",
          "source": "pykrx",
          "captured_at": "2026-04-15T15:30:00+09:00",
          "price_context": {
            "trade_date": "2026-04-15",
            "close_price": 72100,
            "candle": {"direction": "bullish"}
          }
        }
      ],
      "technical_snapshot": {"market_date": "2026-04-15", "trend_state": "uptrend"},
      "action_summary": {"market_date": "2026-04-15", "action_label": "hold"},
      "scores": {
        "unified_value": {
          "status": "available",
          "score": 0.873,
          "score_scale": "0_to_1_percentile_mean",
          "rank": 14,
          "percentile": 0.958,
          "higher_is_better": true,
          "meaning": "Composite value attractiveness score across stored cross-sectional value percentiles.",
          "method": "Equal-weight mean of PER, PBR, dividend yield, FCF yield, and EV/EBITDA percentiles; requires all five components, with lower PER/PBR/EV-EBITDA and higher dividend yield/FCF yield ranking better.",
          "components_used": 5,
          "components_possible": 5,
          "coverage_notes": [],
          "as_of_market_date": "2026-04-15"
        },
        "quality": {
          "status": "partial",
          "score": 0.741,
          "score_scale": "0_to_1_percentile_mean",
          "rank": 29,
          "percentile": 0.884,
          "higher_is_better": true,
          "components_used": 5,
          "components_possible": 6,
          "coverage_notes": [
            "The quality score was computed from the available components only.",
            "Missing quality components: accruals_ratio."
          ],
          "as_of_market_date": "2026-04-15"
        },
        "momentum": {
          "status": "available",
          "score": 0.914,
          "score_scale": "0_to_1_percentile",
          "rank": 21,
          "percentile": 0.914,
          "higher_is_better": true,
          "components_used": 3,
          "components_possible": 3,
          "coverage_notes": [],
          "as_of_market_date": "2026-04-15"
        }
      },
      "fundamentals": {"latest_business_year": 2025},
      "recent_disclosures": {"count": 5, "items": []}
    }
  ]
}
```
