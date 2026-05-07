# Daily signal features

## Scope

Daily signal features persist selected KIS ranking outputs as symbol/day context for downstream agent assembly.
When direct ranking rows are sparse, the same layer now expands coverage across the active daily-bar universe and the current agent-feedback candidate universe from already-owned data.

Current direct sources:
- `volume-power`
- `overtime-volume`
- `overtime-fluctuation`

Owned-data fallback inputs:
- stored OHLCV / derived price context
- persisted factor rows
- `current_quote_cache` / stored quote data when available

This path is storage and ops oriented. It does not provide realtime list rendering, final ranking decisions, or order execution.

## Storage

Table: `daily_signal_features`

Key:
- `symbol_id`
- `market_date`

Each row stores compact typed fields for common filters plus `feature_payload` for source-specific details:
- `signal_count`
- `volume_power_rank`
- `volume_power`
- `overtime_volume_rank`
- `overtime_volume`
- `overtime_fluctuation_rank`
- `overtime_fluctuation_rate`
- `feature_payload`

`feature_payload` now also carries:
- `row_provenance`
  - `row_kind`: `direct_only`, `direct_plus_fallback`, or `fallback_only`
  - direct-source counts and fallback group names
  - active/candidate universe membership flags
  - a fallback note telling downstream readers to treat fallback values as owned-data context, not live ranking truth
- `freshness`
  - direct-source collection timestamp
  - price-bar freshness
  - quote freshness
  - latest factor capture timestamp
- `fallback_features`
  - `price_action`
  - `liquidity`
  - `returns`
  - `volatility`
  - `factor_scores`
  - `quote_snapshot`

Rows are upserted, so rerunning a date refreshes the same symbol/date records.

## Ops

From `apps/api`:

```bash
python3.11 -m app.internal.ops kis-daily-signal-features-collect --market-date 2026-04-14
```

HTTP equivalents:

```bash
curl -X POST "http://127.0.0.1:8000/eod/signal-features/collect?market_date=2026-04-14"
curl "http://127.0.0.1:8000/eod/signal-features?market_date=2026-04-14&symbols=005930"
```

`UPSTREAM_BEARER_TOKEN` applies to HTTP requests when configured. Internal ops call services directly and avoid bearer-token self-calls.

The collect path returns:
- persisted/failed row counts
- per-source errors when one KIS ranking endpoint fails but owned-data fallback still allows persistence
- market-date coverage for the active daily-bar universe and the agent-feedback candidate universe
- a capped `items` preview with `items_total` and `items_returned`

The read path returns:
- exact symbol/date rows
- coverage summary for the whole market date
- requested-scope coverage when `symbols` is supplied

## Agent Context

`GET /agent-context/symbol-day` includes same-day `signal_features` beside company metadata, technical snapshots, action summaries, fundamentals, and recent disclosures.
Downstream readers should use `signal_features.row_kind`, `signal_features.provenance`, `signal_features.freshness`, and `context_completeness.signal_features` to distinguish direct KIS ranking evidence from owned-data fallback context.
