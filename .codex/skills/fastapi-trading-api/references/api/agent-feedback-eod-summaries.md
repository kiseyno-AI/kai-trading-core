# GET /agent-context/feedback/eod-summaries

## Summary
Read persisted `eod-summarizer` feedback results from `agent_context_feedback` for a required market date.

When `symbol` is omitted, the endpoint returns every stored `eod-summarizer` result for that date ordered by symbol. When `symbol` is provided, the endpoint returns only that symbol's stored result, or `null` if no persisted row exists.

## Query parameters
- `market_date` (required): ISO date for the persisted `eod-summarizer` rows to inspect.
- `symbol` (optional): stock code to narrow the response to a single symbol.

## Example requests
```http
GET /agent-context/feedback/eod-summaries?market_date=2026-04-21
GET /agent-context/feedback/eod-summaries?market_date=2026-04-21&symbol=005930
```

## Example response (all rows)
```json
{
  "market_date": "2026-04-21",
  "agent_name": "eod-summarizer",
  "summary": {
    "entries": 1,
    "requested_symbol": null,
    "matched_symbols": ["000660"],
    "missing_symbols": [],
    "risk_flagged_count": 0
  },
  "items": [
    {
      "symbol": "000660",
      "market_date": "2026-04-21",
      "agent_name": "eod-summarizer",
      "agent_version": "daily-v1",
      "run_id": "feedback-followup-2026-04-21-a1b2c3d4",
      "role": "eod-summarizer",
      "summary": "Participation remained broad enough to keep the close constructive, but leadership narrowed late in the session.",
      "confidence": 0.78,
      "risk_flags": [],
      "details": {
        "breadth_context": {
          "advancers": 1214,
          "decliners": 889
        }
      },
      "breadth_context": {
        "advancers": 1214,
        "decliners": 889
      },
      "leaders_laggards": {
        "leaders": ["000660"],
        "laggards": ["005930"]
      },
      "price_action": {
        "close_vs_open_pct": 1.2
      },
      "trend_context": null,
      "support_resistance": null,
      "anomalies_and_gaps": {
        "items": []
      },
      "data_gaps": {
        "items": []
      },
      "fundamental_backdrop": null,
      "conservative_take": {
        "headline": "Hold gains unless breadth deteriorates further."
      },
      "improvement_notes": [
        "Include next-session sector breadth by industry group."
      ],
      "missing_data_categories": [
        {
          "category": "market_sector_peer_relative",
          "status": "partially_available"
        }
      ],
      "received_at": "2026-04-21T06:10:00+00:00"
    }
  ]
}
```

## Example response (single symbol)
```json
{
  "market_date": "2026-04-21",
  "agent_name": "eod-summarizer",
  "summary": {
    "entries": 1,
    "requested_symbol": "005930",
    "matched_symbols": ["005930"],
    "missing_symbols": [],
    "risk_flagged_count": 1
  },
  "item": {
    "symbol": "005930",
    "market_date": "2026-04-21",
    "agent_name": "eod-summarizer",
    "agent_version": "daily-v1",
    "run_id": "feedback-followup-2026-04-21-a1b2c3d4",
    "role": "eod-summarizer",
    "summary": "Breadth improved into the close, but the move still depended on a narrow leadership group.",
    "confidence": 0.81,
    "risk_flags": ["narrow_leadership"],
    "details": {
      "breadth_context": {
        "advancers": 1214,
        "decliners": 889
      }
    },
    "breadth_context": {
      "advancers": 1214,
      "decliners": 889
    },
    "leaders_laggards": {
      "leaders": ["005930"],
      "laggards": ["035420"]
    },
    "price_action": {
      "close_vs_open_pct": 1.2
    },
    "trend_context": null,
    "support_resistance": null,
    "anomalies_and_gaps": {
      "items": ["late-session breadth rollover"]
    },
    "data_gaps": {
      "items": []
    },
    "fundamental_backdrop": null,
    "conservative_take": {
      "headline": "Prefer confirmation from broader participation tomorrow."
    },
    "improvement_notes": [
      "Include next-session sector breadth by industry group."
    ],
    "missing_data_categories": [
      {
        "category": "market_sector_peer_relative",
        "status": "partially_available"
      }
    ],
    "received_at": "2026-04-21T06:10:00+00:00"
  }
}
```
