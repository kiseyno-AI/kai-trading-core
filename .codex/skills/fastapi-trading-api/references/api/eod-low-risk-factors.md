# POST /eod/low-risk-factors/compute

## Summary
Compute and persist the daily low-risk factor slice from stored `daily_price_bars`.

The compute path is deterministic and DB-backed. It reads only stored daily bars for the requested `market_date`, derives raw low-risk metrics, applies cross-sectional ranking and liquidity-floor rules, and upserts one row per matched symbol into `daily_low_risk_factor_results`.

v1 methodology is explicit:
- `volatility_60d` and `volatility_120d` use simple close-to-close daily returns from stored `close_price`
- realized volatility is annualized with `sqrt(252)`
- `max_drawdown_120d` uses the trailing 120 stored closes
- `avg_trading_value_20d` uses the trailing 20 stored trade-date bars

## Query parameters
- `market_date` (required): ISO date for the stored bar snapshot to score.
- `symbols` (optional): comma-separated symbol subset. When omitted, the service uses the full stored symbol universe with a bar on `market_date`.
- `source` (optional): persisted feature-version string. Defaults to `low_risk_v1`.

## Example request
```http
POST /eod/low-risk-factors/compute?market_date=2026-04-23&symbols=005930,000660
```

## Example response
```json
{
  "market_date": "2026-04-23",
  "source": "low_risk_v1",
  "feature_version": "low_risk_v1",
  "requested_symbols": ["005930", "000660"],
  "matched_symbols": ["005930", "000660"],
  "missing_symbols": [],
  "summary": {
    "matched_symbol_count": 2,
    "eligible_count": 1,
    "insufficient_history_count": 0,
    "ranking_universe_counts": {
      "volatility_60d": 2,
      "volatility_120d": 2,
      "max_drawdown_120d": 2,
      "liquidity": 2,
      "combined_score": 2
    }
  },
  "results_persisted": 2,
  "results_failed": 0,
  "items": [
    {
      "symbol": "005930",
      "market_date": "2026-04-23",
      "volatility_60d": 0.214381,
      "volatility_120d": 0.251047,
      "max_drawdown_120d": -0.128422,
      "avg_trading_value_20d": 31234567890.0,
      "volatility_60d_percentile": 0.82,
      "volatility_120d_percentile": 0.79,
      "max_drawdown_120d_percentile": 0.88,
      "liquidity_percentile": 0.93,
      "passes_liquidity_floor": true,
      "low_risk_score": 0.8465,
      "low_risk_classification": "eligible",
      "is_low_risk_eligible": true,
      "feature_version": "low_risk_v1"
    }
  ]
}
```

# GET /eod/low-risk-factors

## Summary
Read persisted daily low-risk factor rows for a required `market_date`.

When `symbol` is omitted, the endpoint returns ordered `items` for the requested date. When `symbol` is provided, it returns the single persisted `item` for that symbol and date, or `null` if no row is stored.

Rows include raw metrics, ranks and percentiles, the combined score, the final classification, and the per-row methodology and rule metadata under `factor_payload`.

## Query parameters
- `market_date` (required): ISO date for the persisted factor snapshot.
- `symbol` (optional): six-digit stock code to narrow the read to one symbol.
- `limit` (optional): maximum number of rows to return when reading the full date. Allowed range is `1..20000`.

## Example requests
```http
GET /eod/low-risk-factors?market_date=2026-04-23&limit=100
GET /eod/low-risk-factors?market_date=2026-04-23&symbol=005930
```

## Example response (single symbol)
```json
{
  "market_date": "2026-04-23",
  "feature_version": "low_risk_v1",
  "summary": {
    "entries": 1,
    "requested_symbol": "005930",
    "matched_symbols": ["005930"],
    "missing_symbols": [],
    "eligible_count": 1,
    "liquidity_floor_pass_count": 1,
    "insufficient_history_count": 0
  },
  "item": {
    "symbol": "005930",
    "market_date": "2026-04-23",
    "source": "low_risk_v1",
    "feature_version": "low_risk_v1",
    "volatility_60d": 0.214381,
    "volatility_120d": 0.251047,
    "max_drawdown_120d": -0.128422,
    "avg_trading_value_20d": 31234567890.0,
    "volatility_60d_rank": 87,
    "volatility_120d_rank": 93,
    "max_drawdown_120d_rank": 54,
    "liquidity_rank": 18,
    "volatility_60d_percentile": 0.82,
    "volatility_120d_percentile": 0.79,
    "max_drawdown_120d_percentile": 0.88,
    "liquidity_percentile": 0.93,
    "passes_liquidity_floor": true,
    "low_risk_score": 0.8465,
    "low_risk_classification": "eligible",
    "is_low_risk_eligible": true,
    "history_days_available": 121,
    "factor_payload": {
      "methodology": {
        "return_type": "simple",
        "annualization_days": 252
      },
      "weights": {
        "volatility_60d_percentile": 0.3,
        "volatility_120d_percentile": 0.3,
        "max_drawdown_120d_percentile": 0.25,
        "liquidity_percentile": 0.15
      }
    }
  }
}
```
