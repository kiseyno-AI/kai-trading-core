# GET /agent-context/feedback/technical-summaries

## Summary
Read persisted `technical-summarizer` feedback results from `agent_context_feedback` for a required market date.

This route is a bounded role-specific view over the shared feedback read path. When `symbol` is omitted, it returns up to `limit` stored `technical-summarizer` rows for that date ordered by most recently received first. When `symbol` is provided, it returns only that symbol's stored technical feedback rows up to `limit`.

## Query parameters
- `market_date` (required): ISO date for the persisted `technical-summarizer` rows to inspect.
- `symbol` (optional): stock code to narrow the response to one symbol.
- `limit` (optional): maximum number of stored rows to return. Defaults to `50`.

## Example requests
```http
GET /agent-context/feedback/technical-summaries?market_date=2026-04-21
GET /agent-context/feedback/technical-summaries?market_date=2026-04-21&symbol=005930&limit=10
```

## Example response
```json
{
  "request": {
    "market_date": "2026-04-21",
    "symbols": ["005930"],
    "agent_name": "technical-summarizer",
    "limit": 10
  },
  "summary": {
    "count": 1
  },
  "items": [
    {
      "id": 42,
      "market_date": "2026-04-21",
      "symbol": "005930",
      "agent": {
        "name": "technical-summarizer",
        "version": "daily-v1",
        "run_id": "feedback-followup-2026-04-21-a1b2c3d4"
      },
      "summary": {
        "requested_role": "technical-summarizer"
      },
      "feedback": {
        "role": "technical-summarizer",
        "summary": "Short-term trend remains constructive, but momentum is cooling into prior resistance.",
        "confidence": 0.74
      },
      "normalized_feedback": {
        "role": "technical-summarizer",
        "summary": "Short-term trend remains constructive, but momentum is cooling into prior resistance.",
        "confidence": 0.74,
        "risk_flags": ["overhead_resistance"],
        "details": {
          "trend_context": {
            "ma_alignment": "bullish",
            "breakout_state": "extended"
          }
        },
        "improvement_notes": [
          "Include multi-timeframe RSI divergence context."
        ]
      },
      "details": {
        "trend_context": {
          "ma_alignment": "bullish",
          "breakout_state": "extended"
        },
        "support_resistance": {
          "nearest_resistance": 85300
        },
        "price_action": {
          "return_5d": 4.8
        },
        "data_gaps": {
          "items": []
        }
      },
      "breadth_context": null,
      "leaders_laggards": null,
      "price_action": {
        "return_5d": 4.8
      },
      "trend_context": {
        "ma_alignment": "bullish",
        "breakout_state": "extended"
      },
      "support_resistance": {
        "nearest_resistance": 85300
      },
      "anomalies_and_gaps": null,
      "data_gaps": {
        "items": []
      },
      "fundamental_backdrop": null,
      "conservative_take": null,
      "improvement_notes": [
        "Include multi-timeframe RSI divergence context."
      ],
      "context": {
        "source_route": "/agent-context/symbol-day"
      },
      "received_at": "2026-04-21T06:10:00+00:00"
    }
  ]
}
```
