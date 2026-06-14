# GET /market-data/crypto/markets

## Summary
Lists the explicit Upbit crypto market whitelist available to core as read-only paper/research inputs. The endpoint calls Upbit public quotation metadata and marks every instrument with crypto provider provenance and `trading_enabled_in_core=false`.

## Example request
```http
GET /market-data/crypto/markets
```

## Example response
```json
{
  "provider": "upbit",
  "asset_class": "crypto",
  "venue": "UPBIT",
  "quote_currency": "KRW",
  "usage_scope": "paper_research_only",
  "trading_enabled_in_core": false,
  "whitelist": ["KRW-BTC", "KRW-ETH"],
  "count": 2,
  "source": {
    "provider": "upbit",
    "venue": "UPBIT",
    "api_family": "quotation",
    "endpoint": "/v1/market/all",
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
      "korean_name": "비트코인",
      "english_name": "Bitcoin",
      "market_warning": "NONE",
      "trading_enabled_in_core": false,
      "usage_scope": "paper_research_only"
    }
  ]
}
```
