# GET /market-data/crypto/tickers

## Summary
Returns current Upbit public ticker snapshots for whitelisted crypto markets. If `markets` is omitted, the configured whitelist is used. Unsupported markets are rejected instead of inferred from KRX/KIS symbols.

## Example request
```http
GET /market-data/crypto/tickers?markets=KRW-BTC,KRW-ETH
```

## Example response
```json
{
  "provider": "upbit",
  "asset_class": "crypto",
  "venue": "UPBIT",
  "usage_scope": "paper_research_only",
  "trading_enabled_in_core": false,
  "requested_markets": ["KRW-BTC", "KRW-ETH"],
  "count": 1,
  "source": {
    "provider": "upbit",
    "venue": "UPBIT",
    "api_family": "quotation",
    "endpoint": "/v1/ticker",
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
      "currency": "KRW",
      "price": 145831000,
      "open": 145000000,
      "high": 146000000,
      "low": 144000000,
      "previous_close": 145000000,
      "change": "RISE",
      "signed_change_rate": 0.0057,
      "acc_trade_price_24h": 344100708901.19,
      "acc_trade_volume_24h": 2389.39562434,
      "last_trade_at": "2026-06-13T23:06:03+00:00",
      "captured_at": "2026-06-13T23:06:04+00:00",
      "trading_enabled_in_core": false,
      "usage_scope": "paper_research_only"
    }
  ]
}
```
