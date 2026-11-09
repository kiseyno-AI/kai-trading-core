# GET /portfolio/equity-history

Read Core's daily aggregate valuations in chronological order, grouped by currency. Core records the first complete, fresh valuation after the scheduled KRX close per trading day. The date uses Asia/Seoul. A missing valuation produces no point. This read does not refresh the broker or write data.

The first valuation in each currency is its baseline. A nonpositive baseline yields `returnPct: null`. Deposits and withdrawals affect the change. Use the configured Core bearer token server-side and keep the actual amounts behind the consuming application's admin gate. Responses use `Cache-Control: no-store`.

```http
GET /portfolio/equity-history
```

```json
{
  "equity": [{
    "currency": "KRW",
    "points": [
      {"date": "2026-11-10", "equity": 1000000, "returnPct": 0},
      {"date": "2026-11-11", "equity": 1025000, "returnPct": 2.5}
    ]
  }]
}
```
