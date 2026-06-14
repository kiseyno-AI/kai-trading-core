# GET /market-data/crypto/strategy-context

## Summary
Returns deterministic paper/research strategy context for whitelisted Upbit KRW crypto markets. The endpoint uses public Upbit ticker and daily-candle data only, labels the result as decision support, and returns explicit `partial` or `unavailable` states when source data is incomplete.

## Example request
```http
GET /market-data/crypto/strategy-context?markets=KRW-BTC,KRW-ETH&count=30
```

## Example response
```json
{
  "provider": "upbit",
  "asset_class": "crypto",
  "venue": "UPBIT",
  "quote_currency": "KRW",
  "usage_scope": "paper_research_only",
  "strategy_context_version": "crypto_strategy_context_v1",
  "status": "available",
  "trading_enabled_in_core": false,
  "decision_support_only": true,
  "private_api_used": false,
  "account_data_used": false,
  "requested_markets": ["KRW-BTC", "KRW-ETH"],
  "requested_count": 30,
  "count": 2,
  "source": {
    "provider": "upbit",
    "venue": "UPBIT",
    "api_family": "quotation_derived_strategy_context",
    "endpoints": ["/v1/ticker", "/v1/candles/days"],
    "auth_required": false,
    "read_only": true,
    "order_submission_enabled": false,
    "private_api_used": false,
    "account_data_used": false,
    "rule_version": "crypto_strategy_context_v1"
  },
  "items": [
    {
      "symbol": "KRW-BTC",
      "market": "KRW-BTC",
      "asset_class": "crypto",
      "instrument_type": "spot_crypto_pair",
      "provider": "upbit",
      "venue": "UPBIT",
      "quote_currency": "KRW",
      "base_currency": "BTC",
      "status": "available",
      "trend_state": "uptrend",
      "momentum_score": 32.5,
      "volatility_tier": "medium",
      "liquidity_tier": "high",
      "risk_tier": "medium",
      "observed_window": {
        "interval": "day",
        "requested_count": 30,
        "raw_count": 30,
        "observed_count": 30,
        "incomplete_candle_count": 0,
        "start_time_kst": "2026-05-16T09:00:00",
        "end_time_kst": "2026-06-14T09:00:00"
      },
      "session_flags": {
        "market_session_model": "continuous_24_7",
        "continuous_session": true,
        "weekend_session": true,
        "observed_weekend_candle_count": 8,
        "krx_market_close_applies": false,
        "dart_fundamentals_applicable": false
      },
      "reasons": [
        "trend_state=uptrend; momentum_score=32.5",
        "liquidity_tier=high; spread proxy uses daily high-low range, not orderbook",
        "risk_tier=medium"
      ],
      "invalidation_level": 142000000,
      "target_level": 151000000,
      "source_payload": {
        "ticker": {
          "market": "KRW-BTC",
          "price": 145831000,
          "acc_trade_price_24h": 344100708901.19
        },
        "candles": [
          {
            "market": "KRW-BTC",
            "candle_date_time_kst": "2026-06-14T09:00:00",
            "close": 145831000,
            "acc_trade_price": 4022470467.03
          }
        ]
      }
    }
  ]
}
```
