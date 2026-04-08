
# EOD Action Summaries MVP

## Goal

Add a deterministic rules/scoring layer on top of stored `daily_technical_snapshots` so downstream sub-agents can consume a structured post-EOD action object without any LLM generation.

## Scope

- input: stored `daily_technical_snapshots`
- trigger: pykrx daily EOD completion path, plus manual rerun paths
- storage: one summary row per `symbol + market_date`
- consumers: internal services, inspect API, CLI, and downstream agent jobs

Excluded for now:

- narrative commentary text
- discretionary analyst prose
- fundamentals/news inputs
- execution or order routing

## Schema

Table: `daily_action_summaries`

- key: unique `symbol_id + market_date`
- provenance: optional links to `daily_technical_snapshots` and `eod_collection_runs`
- scalar fields:
  - `action_label`
  - `bias_direction`
  - `signal_score`
  - `confidence_tier`
  - `risk_tier`
  - `trigger_price`
  - `invalidation_price`
  - `support_price`
  - `resistance_price`
- structured fields:
  - `reasons` JSON list of deterministic reason codes
  - `summary_payload` JSON contract for sub-agents

## Rules and labels

The MVP score is a bounded deterministic sum of:

- MA alignment bias
- trend-state bias
- 5d momentum
- 20d momentum
- 20d breakout/breakdown context
- volume confirmation or weak participation

Current labels:

- `long_breakout`
- `long_bias`
- `watch_long`
- `short_breakdown`
- `risk_off`
- `watch_short`
- `neutral`
- `insufficient_data`

Risk tier is derived from ATR percent, while confidence tier is derived from score magnitude plus data completeness.

## Pipeline

1. Daily EOD pykrx run persists `daily_price_bars`.
2. The same run persists `daily_technical_snapshots`.
3. The same run then loads stored technical snapshots and persists `daily_action_summaries`.
4. Downstream consumers can inspect the rows through:
   - `GET /eod/action-summaries`
   - direct internal service call
   - CLI rerun `python3.11 -m app.eod.action_summaries --market-date YYYY-MM-DD`

## Why this shape

- Keeps the action layer explicitly downstream of durable stored technical state.
- Makes the summary contract inspectable and backtest-friendly.
- Gives sub-agents compact machine fields instead of narrative text.
