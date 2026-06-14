# GET /market-data/crypto/candles

## Summary
Returns Upbit public day or minute candles for one whitelisted crypto market. This is a read-only paper/research endpoint and never implies live crypto order authority in core.

## Example request
```http
GET /market-data/crypto/candles?market=KRW-BTC&interval=minute&unit=15&count=30
```

## Example response
```json
{
  "provider": "upbit",
  "asset_class": "crypto",
  "venue": "UPBIT",
  "usage_scope": "paper_research_only",
  "trading_enabled_in_core": false,
  "market": "KRW-BTC",
  "symbol": "KRW-BTC",
  "base_currency": "BTC",
  "quote_currency": "KRW",
  "instrument_type": "spot_crypto_pair",
  "interval": "minute",
  "unit": 15,
  "requested_count": 30,
  "to": null,
  "count": 1,
  "source": {
    "provider": "upbit",
    "venue": "UPBIT",
    "api_family": "quotation",
    "endpoint": "/v1/candles/minutes/15",
    "auth_required": false,
    "read_only": true,
    "order_submission_enabled": false
  },
  "items": [
    {
      "market": "KRW-BTC",
      "symbol": "KRW-BTC",
      "asset_class": "crypto",
      "instrument_type": "spot_crypto_pair",
      "provider": "upbit",
      "venue": "UPBIT",
      "quote_currency": "KRW",
      "base_currency": "BTC",
      "interval": "minute",
      "unit": 15,
      "candle_date_time_utc": "2026-06-13T22:45:00",
      "candle_date_time_kst": "2026-06-14T07:45:00",
      "open": 145800000,
      "high": 145900000,
      "low": 145700000,
      "close": 145831000,
      "acc_trade_price": 4022470467.03,
      "acc_trade_volume": 27.58904602,
      "captured_at": "2026-06-13T22:59:59+00:00"
    }
  ]
}
```
