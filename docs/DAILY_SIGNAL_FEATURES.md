# Daily signal features

## Scope

Daily signal features persist selected KIS ranking outputs as symbol/day context for downstream agent assembly.

Current sources:
- `volume-power`
- `overtime-volume`
- `overtime-fluctuation`

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

## Agent Context

`GET /agent-context/symbol-day` includes same-day `signal_features` beside company metadata, technical snapshots, action summaries, fundamentals, and recent disclosures.
