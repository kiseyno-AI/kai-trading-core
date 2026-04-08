# EOD Technical Snapshots MVP

## Goal

Add a stable post-EOD chart-signal layer that stores deterministic daily technical context in the database so downstream sub-agents can consume the stored snapshot the next morning.

## Scope

The MVP is intentionally narrow and chart-driven:

- source data: stored `daily_price_bars`
- trigger: pykrx daily EOD completion path, plus a manual rerun path
- storage: one snapshot row per `symbol + market_date`
- consumers: internal services and inspect API only

Excluded for now:

- discretionary commentary generation
- fundamentals or news reasoning
- execution or trade recommendation language
- large TA-indicator expansion

## Schema

Table: `daily_technical_snapshots`

- key: unique `symbol_id + market_date`
- linkage: optional `eod_run_id` back to the producing EOD collection run
- scalar fields:
  - `trend_state`
  - `ma_alignment`
  - `return_5d`
  - `return_20d`
  - `volume_ratio_20d`
  - `breakout_20d`
  - `breakdown_20d`
  - `atr_14`
  - `atr_pct_14`
- payload: `signal_payload` JSON for the full downstream contract

## Signal contract

Each snapshot is computed only from persisted daily OHLCV history up to `market_date`.

Current signal set:

- trend / MA state:
  - 10d, 20d, 50d moving averages
  - `ma_alignment`: `bullish`, `bearish`, `mixed`, `insufficient_history`
  - `trend_state`: `uptrend`, `downtrend`, `range`, `insufficient_history`
- returns:
  - 5 trading day return %
  - 20 trading day return %
- participation:
  - 20 day average volume
  - current/20d volume ratio
  - `unusual_volume` boolean inside payload when ratio >= 1.5
- range context:
  - close above prior 20d high => `breakout_20d`
  - close below prior 20d low => `breakdown_20d`
- volatility:
  - 14 period ATR
  - ATR as % of close
  - one-ATR-below-close stop context

## Pipeline

1. Daily EOD pykrx run persists `daily_price_bars`.
2. The same run immediately calls the technical snapshot service.
3. The service loads up to 60 stored bars per symbol, computes the deterministic signal set, and upserts `daily_technical_snapshots`.
4. Downstream agents can inspect the stored rows through:
   - `GET /eod/technical-snapshots`
   - direct internal service call
   - CLI rerun `python3.11 -m app.eod.technical_snapshots --market-date YYYY-MM-DD`

## Operational use today

After today’s EOD is available, the preferred path is:

1. Run the normal pykrx daily EOD job.
2. Let the built-in post-EOD hook write technical snapshots automatically.
3. If a rerun is needed, call the dedicated technical snapshot CLI or API route.

## Why this shape

- Uses the existing durable EOD source of truth instead of recomputing against live APIs.
- Keeps the contract stable for future agent consumers.
- Supports immediate adoption for tomorrow-morning commentary without introducing orchestration complexity.
