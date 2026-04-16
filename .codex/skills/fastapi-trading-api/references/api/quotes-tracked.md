# GET /quotes/tracked

## Summary
Reads tracked-symbol quotes with explicit fallback provenance.

## Example request
```http
GET /quotes/tracked
```

## Example response
```json
{
  "market_state": "OPEN",
  "as_of": "2026-04-16T09:31:04+09:00",
  "items": [
    {
      "symbol": "005930",
      "company_name": "삼성전자",
      "price": 84500,
      "change": 700,
      "change_pct": 0.84,
      "volume": 12345678,
      "turnover": 987654321000,
      "as_of": "093104",
      "market_state": "OPEN",
      "source": "kis",
      "fallback": false,
      "is_live": true,
      "available": true,
      "cached_at": null,
      "trade_date": null,
      "fallback_reason": null,
      "unavailable_reason": null
    }
  ]
}
```
