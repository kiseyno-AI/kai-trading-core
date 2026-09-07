# POST /eod/pykrx/backfill

## Summary
Runs a point-in-time-safe pykrx backfill for exactly one historical market date. `start_date` and `end_date` must match until date-by-date membership reconstruction is implemented. When both dates are omitted, the API selects the latest KRX trading session; an explicitly supplied weekend or holiday is rejected with HTTP 400. Historical requests do not fall back to the current active/KIS-master universe and accept only `price_collection`, `technical_snapshots`, and `action_summaries`.

## Example request
```http
POST /eod/pykrx/backfill?start_date=20260408&end_date=20260408&markets=KOSPI,KOSDAQ&stages=price_collection,technical_snapshots,action_summaries
```

## Example response
```json
{
  "status": "completed",
  "start_date": "20150101",
  "requested_end_date": "20260408",
  "latest_available_trade_date": "2026-04-08",
  "markets": ["KOSPI", "KOSDAQ"],
  "selected_stages": ["price_collection", "technical_snapshots", "action_summaries"],
  "price_contexts": {
    "market_date": "2026-04-08",
    "contexts_persisted": 912,
    "contexts_failed": 0
  }
}
```

For a longer interval, submit one request per trading date and retry failed dates independently. Unsupported stages, a multi-day range, or an unavailable requested market fail before bar collection so current-only inputs cannot be mislabeled as historical evidence.
