# GET /agent-context/feedback/data-gap-summary

## Summary
Aggregates normalized agent missing-data feedback by date or date range so operators can prioritize backlog items and context wiring fixes. The response now also includes a `quality` section with duplicate-note metrics, context issue-type note counts, resolved-vs-external category grouping, symbol and agent breakdowns, context-block coverage, and a first-vs-last-date comparison when the request spans a range. The payload is intended to be safe for dashboards and ops logs and does not echo secrets from stored context payloads.

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
    "raw_note_mentions": 14,
    "duplicate_note_mentions": 3,
    "duplicate_note_rate": 0.2143,
    "matched_note_count": 9,
    "category_count": 1,
    "context_issue_type_counts": {
      "stale": 4,
      "derived_or_fallback": 3,
      "available_but_old": 1
    }
  },
  "quality": {
    "note_metrics": {
      "raw_note_mentions": 14,
      "duplicate_note_mentions": 3,
      "duplicate_note_rate": 0.2143
    },
    "resolution_metrics": {
      "resolved_by_existing_data": {
        "category_count": 0
      },
      "requires_external_data": {
        "category_count": 1
      }
    },
    "range_comparison": {
      "before_market_date": "2026-05-01",
      "after_market_date": "2026-05-06",
      "matched_note_delta": -2,
      "direction": "improving"
    }
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
      "context_issue_type_counts": {
        "stale": 4,
        "derived_or_fallback": 3
      },
      "context_issue_type_note_mentions": {
        "stale": 6,
        "derived_or_fallback": 4
      },
      "affected_symbols": ["000660", "005930"],
      "example_notes": ["거래대금과 슬리피지 근거 필요"],
      "trend": {
        "before_market_date": "2026-05-01",
        "after_market_date": "2026-05-06",
        "note_mentions_delta": -1,
        "direction": "improving"
      }
    }
  ]
}
```
