# POST /eod/pykrx/daily/run-if-due

## Summary
Runs the daily pykrx EOD pipeline only when due, including persisted price-context refresh after bar writes. If `market_date` is omitted, the API selects the latest KRX trading session; an explicitly supplied weekend or holiday is rejected with HTTP 400. The scheduled daily sequence includes `pykrx_enrichment` immediately after price collection, so source-market-date sector classification, investor-flow-symbol, and short-selling coverage is populated before downstream exec/decider use. When `DART_API_KEY` is configured, the same daily-auto path also chains the DART daily fundamentals refresh after successful completion.

## Example request
```http
POST /eod/pykrx/daily/run-if-due?market_date=2026-04-15
```

## Example response
```json
{
  "triggered": true,
  "status": "completed",
  "latest_available_trade_date": "2026-04-15",
  "selected_stages": [
    "price_collection",
    "pykrx_enrichment",
    "technical_snapshots",
    "action_summaries",
    "trending_ranks",
    "signal_features",
    "market_risk_filter"
  ],
  "price_contexts": {
    "market_date": "2026-04-15",
    "contexts_persisted": 912,
    "contexts_failed": 0
  },
  "pykrx_enrichment": {
    "market_date": "2026-04-15",
    "status": "completed",
    "categories_requested": [
      "sector_classification",
      "market_breadth",
      "investor_flow_market",
      "investor_flow_symbol",
      "short_selling",
      "valuation_snapshot",
      "foreign_ownership"
    ],
    "downstream_preflight": {
      "status": "ready",
      "allowed": true,
      "required_categories": [
        "sector_classification",
        "investor_flow_symbol",
        "short_selling"
      ],
      "category_row_counts": {
        "sector_classification": 912,
        "investor_flow_symbol": 912,
        "short_selling": 912
      },
      "failed_categories": []
    }
  },
  "fundamentals_followup": {
    "status": "completed",
    "market_date": "2026-04-15",
    "trigger": "eod_pykrx_followup"
  }
}
```
