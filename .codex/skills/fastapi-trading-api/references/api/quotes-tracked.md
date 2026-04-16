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
  "items": [
    {
      "symbol": "005930",
      "name": "삼성전자",
      "price": 84500,
      "change_rate": 0.82,
      "available": true,
      "source": "kis_live",
      "fallback": false,
      "cached_at": "2026-04-16T09:31:04+09:00",
      "trade_date": "2026-04-16"
    }
  ]
}
```
