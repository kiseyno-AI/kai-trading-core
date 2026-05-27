# EOD freshness and pipeline status semantics contract

This document is the source-of-truth contract for the EOD read-layer semantics behind:

- `GET /eod/freshness`
- `GET /eod/pipeline/status`

It freezes the behavior currently shipped on `main` so future web work, core refactors, and regression tests can judge changes against one explicit contract instead of re-inferring intent from multiple routes and tests.

## Terms

- `today`
  Current Seoul calendar date from `market_state.server_time[:10]`.
- `latest`
  Latest stored availability for that data layer. It is not forced to match `today`.
- `running`
  The selected current run for that scope is still in progress, so live progress should be surfaced even if some persisted rows already exist.
- `break-day`
  Any non-trading Seoul calendar day other than a normal weekend, such as a holiday or exchange closure. Current behavior treats it the same way as a weekend for default date selection.

## Shared rules

- Default date selection is calendar-date based, not prior-trading-date based.
  Both routes derive their default day from the current Seoul server date. They do not backshift to the previous trading day on weekends, holidays, or other break-days.
- Stored availability and in-flight progress are separate concerns.
  `/eod/freshness` answers what persisted data is available. `/eod/pipeline/status` answers which pipeline stage is currently active and how much work is in progress.
- Historical backfill rows must not outrank the daily pipeline row when both exist for the same trade date.
  The preferred EOD run is the first non-`historical_backfill` row for that trade date. Historical rows are only used when no daily row exists.

## Shared market-session metadata

- Both routes now include a top-level `market_session` block.
- `market_session.requested_date` is the freshness or pipeline date being described. `market_session.current_calendar_date` is the current Seoul calendar date from the market clock.
- `market_session.latest_valid_trading_date` is the latest exchange trading date on or before `requested_date`.
- `market_session.status` distinguishes requested-date market semantics with `open`, `closed`, `weekend`, and `holiday`.
- `market_session.is_trading_day` is true only when `requested_date` is an exchange trading session.
- `market_session.intraday_data_expected` and `market_session.quote_data_expected` are false on weekends, holidays, and same-day closed sessions where live intraday or quote rows are not expected.
- `market_session.recommendation_basis` is `live_intraday` only for the current open trading session; otherwise it is `latest_completed_eod`.
- On weekends, holidays, and other non-trading days, consumers must use `latest_valid_trading_date` and `recommendation_basis=latest_completed_eod` when describing freshness to operators instead of implying a missing current-cycle intraday feed.

## `GET /eod/freshness`

### `today`

- `today.trade_date` is always the current Seoul calendar date.
- `today.price`, `today.technical_analysis`, and `today.action_summary` are same-day availability views over the current active symbol universe.
- Those three stages use `ready` when same-day persisted rows exist and `missing` when same-day persisted rows do not exist.
- For those three stages, `symbols_failed` stays `0` until the stage has at least one same-day success row. Once same-day rows exist, `symbols_failed` means active symbols still missing that same-day persisted layer.
- `today` never falls back to previous-day availability. If today is a weekend, holiday, or other break-day, `today.*` can stay `missing` while `latest.*` still points to the previous stored trade date.

### `today.fundamentals`

- `today.fundamentals` is an activity view, not a latest-coverage view.
- Its `basis` is always `same_day_snapshot_updates`.
- `symbols_succeeded` counts distinct stock codes whose DART fundamentals snapshots were updated during the current Seoul day.
- `symbols_failed` counts distinct financial-report stock codes whose capture attempt failed during the current Seoul day.
- `current_run` shows the latest persisted refresh run for the current `market_date` when one exists, even if same-day snapshot updates are still zero.
- A running current-day fundamentals refresh run overrides the stage status to `running`.
  This is true even if same-day successes already exist.
- A completed same-day no-op refresh run does not make `today.fundamentals.status` become `ready` by itself. Without same-day snapshot updates, the stage stays `missing` and the no-op run remains visible only through `today.fundamentals.current_run`.

### `today.pykrx_enrichment`

- `today.pykrx_enrichment` is a same-day coverage view over persisted `daily_pykrx_enrichment_rows`.
- It reports `ready` only when all normalized PyKRX enrichment categories are complete for the current Seoul date.
- It reports `partial` when at least one category has some persisted coverage but another category is missing or incomplete, and it includes per-category `expected_rows`, `present_rows`, and `coverage_status`.
- It reports `missing` when no category coverage is available for the current Seoul date.

### `latest`

- `latest.trade_date` and `latest.price_trade_date` are anchored to the latest stored daily price-bar trade date.
- `latest.summary` is scoped to that latest stored price trade date only.
- `latest.summary.basis=latest_eod_run_symbol_universe` means counts come from the selected `eod_collection_runs` row for that trade date.
- `latest.summary.basis=stored_price_bars_for_latest_trade_date` is the fallback when bars exist for the latest trade date but no matching EOD run row exists. In that fallback, `failed=0` and `total=succeeded`.
- `latest.updated_at` comes from `run_metadata.last_progress_at` when available, then run timestamps. The stored-bar fallback uses the max `daily_price_bars.captured_at` for that trade date.
- `latest.eod_status` is derived from the selected latest run plus stored bar availability for the latest trade date.
- `latest.eod_status` uses all stored bars for `latest.price_trade_date`, not only symbols still active in the current live universe.
- `latest.trade_date`, `latest.updated_at`, `latest.succeeded`, `latest.failed`, and `latest.total` mirror `latest.summary` for compact web consumption.

### `latest.pykrx_enrichment`

- `latest.pykrx_enrichment` uses the same coverage shape as `today.pykrx_enrichment`, but it is anchored to `latest.price_trade_date`.
- This lets operators see whether the latest usable price date also has investor-flow, short-selling, sector, market-breadth, valuation, and ownership enrichment coverage.

### `latest.fundamentals`

- `latest.fundamentals` is the latest-available fundamentals coverage view.
- It is sourced from the latest completed or partial DART fundamentals refresh run across market dates.
- A running same-day refresh does not replace this baseline. While today is still running, `latest.fundamentals` continues to point at the latest completed or partial refresh result.
- Weekends, pre-close periods, break-days, and same-day no-op refreshes must not collapse fundamentals availability back to zero.
- `symbols_total` uses the refresh-run universe, preferring `run_metadata.symbols_selected` when present.
- `symbols_checked` is how many symbols that run processed.
- `symbols_succeeded` is `symbols_fresh + symbols_refreshed`.
- `symbols_refreshed` is preserved separately as the count of symbols whose fundamentals were newly updated in that run.

## `GET /eod/pipeline/status`

### Scope

- This route is operator-facing and live-progress-oriented.
- The default `market_date` is the current Seoul calendar date from `market_state.server_time[:10]`.
- On weekends, holidays, and other break-days, callers must pass an explicit `market_date` if they want to inspect the previous trading day's pipeline run instead of the current calendar date.
- `fs_div` defaults to the configured DART fundamentals default and is normalized to uppercase.

### pykrx stages

- The pykrx stage order is fixed:
  `price_collection`, `technical_snapshots`, `action_summaries`, `trending_ranks`.
- Stage counts come from the selected pykrx `eod_collection_runs` row plus downstream rows linked by `eod_run_id`, with live stage progress from `run_metadata.stage_progress`.
- The inferred current pykrx stage is resolved in this order:
  `run_metadata.stage`, then the latest populated `stage_progress` entry, then persisted downstream counts, then `price_collection` when a run exists but no better stage signal exists.
- When the selected pykrx run is still `running`, the inferred current stage is authoritative for live status display.
- A later pykrx stage stays `pending` until that later stage has actually started.
  Persisted completion of earlier stages does not advance later stages by implication alone.
- If a stage-progress entry still says `running` after the owning run has already ended, that stale running progress must be downgraded to the final run outcome instead of continuing to display as live work.

### fundamentals stage

- `fundamentals_daily_refresh` is reported alongside the pykrx stages, but it is sourced independently from the latest DART fundamentals refresh run for the requested `market_date` and `fs_div`.
- That stage can be `running`, `completed`, `partial`, `failed`, or `pending` independently of the pykrx stages.
- Its counts are refresh-run metrics, not same-day snapshot-activity counts:
  `symbols_succeeded = symbols_fresh + symbols_refreshed`, and totals prefer `run_metadata.symbols_selected`.
- Its activity text should prefer the current metadata-driven refresh stage while running, but once the refresh run is failed, the run `error_message` overrides the running-style activity text.

### factor stages

- The pipeline stage order continues after fundamentals with:
  `momentum_factors`, `value_factors`, `low_risk_factors`, `beta_factors`, `advanced_value_factors`, `unified_value_factors`, `quality_factors`, and `composite_factors`.
- Those stages are default-selected members of the daily process. A default daily run is not complete for a market date until their stored rows exist.
- Factor-stage counts are derived from the persisted result tables for the requested `market_date`, not from dedicated run rows.
- Factor-stage `total` is anchored to the same-market-date price-collection universe when available, so missing factor rows remain visible as incomplete daily scoring coverage instead of collapsing into a false completed state.
- Row-count-derived partial stages expose coverage diagnostics in `details`, including `missing_count`, `missing_symbols_sample`, `coverage_ratio`, `gate_status`, `severity`, and `blocking_status`.
- A partial row-count stage can be `severity=degraded` and `blocking_status=non_blocking` when it is sufficiently complete for downstream feedback. For `market_risk_filter`, an active feedback run with `run_status=running`, `failed_items=0`, and no missing rows inside that run's requested symbol scope is explicitly reported as `gate_status=ready`, `gate_reason=missing_rows_outside_active_feedback_scope`; the missing full-universe rows remain visible through the diagnostics.
- Factor stages can currently surface `pending`, `partial`, or `completed`.
  They do not expose a live `running` state because the repo does not persist standalone run-progress rows for those factor families.
- If a factor stage depends on fundamentals or earlier factor slices and those prerequisites are not yet completed, the factor stage stays `pending` and its activity text reports the blocking stages.
- `advanced_value_factors`, `unified_value_factors`, and `quality_factors` depend on both same-day price collection and same-day DART fundamentals refresh.
- `composite_factors` depends on the component slices `momentum_factors`, `value_factors`, `low_risk_factors`, `beta_factors`, and `advanced_value_factors`.

### agent feedback stage

- When a same-day `agent_context_feedback_runs` row or in-memory feedback run is active, `agent_feedback_summaries` reflects that run even when no first-class daily pipeline stage row exists.
- A running feedback run keeps `agent_feedback_summaries.status=running` and therefore makes the overall pipeline `status=running`; non-blocking upstream row gaps stay visible on their own stages instead of being conflated with feedback failure.
- Feedback run diagnostics include `run_id`, `run_status`, `current_role`, `failed_items`, `completed_feedback_count`, `expected_feedback_count`, `feedback_failure_status`, and `feedback_health` so consumers can separate active progress from failed or stalled feedback.

### overall status

- Overall pipeline `status` is `running` if any stage is `running`.
- Overall pipeline `status` is `pending` only when every stage is `pending`.
- Overall pipeline `status` is `completed` only when every stage is `completed`.
- On a same-day run where pykrx and fundamentals are complete but one or more default-selected factor stages still have no stored rows, overall status must stay `partial`.
- Overall pipeline `status` is `failed` only when at least one stage is `failed` and no stage is `completed` or `partial`.
- All other mixed states resolve to `partial`.

## Non-trading-day behavior

- Weekend behavior:
  `today.trade_date` and default pipeline `market_date` stay on the weekend calendar date; prior availability remains visible only through `latest.*` or an explicit query date. `market_session.status=weekend`, `latest_valid_trading_date` points at the prior session, and intraday or quote data is marked not expected for that requested date.
- Market-closed same-day behavior:
  After the market closes, `today` still refers to that same Seoul calendar date. If same-day EOD persistence is not finished yet, `today` can remain `missing` while `latest` still points to the previous stored trade date. `market_session.status=closed` and `recommendation_basis=latest_completed_eod` make that same-day closed state explicit.
- Break-day behavior:
  Exchange holidays and other non-trading break-days still keep the requested calendar date as the default route date, but `market_session.status=holiday` now distinguishes them from a weekend. The routes do not auto-resolve a previous trading date into `today` or the default pipeline `market_date`; consumers should read `latest_valid_trading_date` when they need the last completed trading session.

## Test anchors

The contract is intentionally mirrored in focused tests and helper coverage:

- `apps/api/tests/test_eod_freshness.py`
- `apps/api/tests/test_eod_pipeline_status.py`
- `apps/api/tests/test_eod_run_stage_support.py`

Future behavior changes should update this contract and the matching tests in the same PR.
