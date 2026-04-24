# POST /eod/beta-factors/compute

## Summary
Compute and persist the daily benchmark-relative beta factor slice from stored `daily_price_bars`.

The compute path is deterministic and DB-backed. It reads only stored daily bars for the requested `market_date`, derives simple close-to-close returns, builds a same-market equal-weight benchmark return series, computes trailing beta windows, applies cross-sectional lower-is-better ranking, and upserts one row per matched symbol into `daily_beta_factor_results`.

v1 methodology is explicit:
- KOSPI equities use `KOSPI_EQW`, KOSDAQ equities use `KOSDAQ_EQW`, and all other markets fall back to `KRX_EQW`
- benchmark returns are equal-weight simple daily returns built from stored `daily_price_bars` in the selected benchmark bucket
- `beta_60d` and `beta_120d` use trailing 60-return and 120-return windows
- beta is estimated as `cov(symbol_returns, benchmark_returns) / var(benchmark_returns)`
- lower beta ranks better, and `low_beta_score = 0.50 * beta_60d_percentile + 0.50 * beta_120d_percentile`

## Query parameters
- `market_date` (required): ISO date for the stored bar snapshot to score.
- `symbols` (optional): comma-separated symbol subset. When omitted, the service uses the full stored symbol universe with a bar on `market_date`.
- `source` (optional): persisted feature-version string. Defaults to `beta_v1`.

## Example request
```http
POST /eod/beta-factors/compute?market_date=2026-04-23&symbols=005930,000660
```

## Example response
```json
{
  "market_date": "2026-04-23",
  "source": "beta_v1",
  "feature_version": "beta_v1",
  "requested_symbols": ["005930", "000660"],
  "matched_symbols": ["005930", "000660"],
  "missing_symbols": [],
  "summary": {
    "matched_symbol_count": 2,
    "eligible_count": 1,
    "insufficient_history_count": 0,
    "ranking_universe_counts": {
      "beta_60d": 2,
      "beta_120d": 2,
      "low_beta_score": 2
    }
  },
  "results_persisted": 2,
  "results_failed": 0,
  "items": [
    {
      "symbol": "005930",
      "market_date": "2026-04-23",
      "benchmark_id": "KOSPI_EQW",
      "benchmark_market": "KOSPI",
      "beta_60d": 0.8421,
      "beta_120d": 0.9084,
      "beta_60d_percentile": 0.81,
      "beta_120d_percentile": 0.77,
      "low_beta_score": 0.79,
      "low_beta_classification": "eligible",
      "is_low_beta_eligible": true,
      "feature_version": "beta_v1"
    }
  ]
}
```

# GET /eod/beta-factors

## Summary
Read persisted daily beta-factor rows for a required `market_date`.

When `symbol` is omitted, the endpoint returns ordered `items` for the requested date. When `symbol` is provided, it returns the single persisted `item` for that symbol and date, or `null` if no row is stored.

Rows include the benchmark identifier, raw beta values, per-window ranks and percentiles, the combined low-beta score, the final classification, and the per-row methodology and audit metadata under `factor_payload`.

## Query parameters
- `market_date` (required): ISO date for the persisted factor snapshot.
- `symbol` (optional): stock code to narrow the read to one symbol.
- `limit` (optional): maximum number of rows to return when reading the full date. Allowed range is `1..20000`.

## Example requests
```http
GET /eod/beta-factors?market_date=2026-04-23&limit=100
GET /eod/beta-factors?market_date=2026-04-23&symbol=005930
```

## Example response (single symbol)
```json
{
  "market_date": "2026-04-23",
  "feature_version": "beta_v1",
  "summary": {
    "entries": 1,
    "requested_symbol": "005930",
    "matched_symbols": ["005930"],
    "missing_symbols": [],
    "eligible_count": 1,
    "watchlist_count": 0,
    "insufficient_history_count": 0
  },
  "item": {
    "symbol": "005930",
    "market_date": "2026-04-23",
    "source": "beta_v1",
    "feature_version": "beta_v1",
    "benchmark_id": "KOSPI_EQW",
    "benchmark_market": "KOSPI",
    "beta_60d": 0.8421,
    "beta_120d": 0.9084,
    "beta_60d_rank": 312,
    "beta_120d_rank": 401,
    "beta_60d_percentile": 0.81,
    "beta_120d_percentile": 0.77,
    "low_beta_score": 0.79,
    "low_beta_classification": "eligible",
    "is_low_beta_eligible": true,
    "history_days_available": 121,
    "factor_payload": {
      "benchmark_policy": {
        "selection_rule": "KOSPI equities use KOSPI_EQW, KOSDAQ equities use KOSDAQ_EQW, others use KRX_EQW"
      },
      "methodology": {
        "return_type": "simple",
        "beta_formula": "cov(symbol_returns, benchmark_returns) / var(benchmark_returns)"
      },
      "weights": {
        "beta_60d_percentile": 0.5,
        "beta_120d_percentile": 0.5
      }
    }
  }
}
```
