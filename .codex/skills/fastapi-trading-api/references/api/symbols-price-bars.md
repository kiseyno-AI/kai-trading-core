# GET /symbols/{symbol}/price-bars

## Summary
Reads stored daily OHLCV history for a symbol.

## Example request
```http
GET /symbols/005930/price-bars?start_date=2021-04-09&end_date=2026-04-09
```

## Example response
```json
{
  "symbol": "005930",
  "items": [{"trade_date": "2026-04-09", "open_price": 84000, "high_price": 85200, "low_price": 83600, "close_price": 84500, "volume": 12345678}]
}
```
