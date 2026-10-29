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

- Default date selection is route-specific.
  `/eod/freshness` derives its default day from the current Seoul server date. `/eod/pipeline/status` uses the market date of the latest persisted official daily run, falling back to the current Seoul date only when no official run exists. Neither route guesses a previous trading day from the calendar.
- Stored availability and in-flight progress are separate concerns.
  `/eod/freshness` answers what persisted data is available. `/eod/pipeline/status` answers which pipeline stage is currently active and how much work is in progress.
- Historical backfill rows must not outrank the representative daily pipeline row when both exist for the same trade date.
  The preferred EOD run starts with the first non-`historical_backfill` row for that trade date. For `/eod/freshness`, if that newest candidate is a narrow retry whose total is smaller than the same-date stored price-bar population, Core selects a more complete same-date run when it has a larger run total or success count. Historical rows remain fallback-only when no daily row exists.

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
- A newer narrow retry delta does not replace a same-date full-universe summary. For example, a 1-symbol retry remains subordinate to a representative 2,679-symbol run when stored same-date bars prove the broader population.
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
- When `market_date` is omitted, the route selects the actual latest persisted official daily run by `started_at` and durable id, then uses that run's market date for all fallback and coverage reads.
- When no official daily run exists, the route falls back to the current Seoul calendar date from `market_state.server_time[:10]`. An explicit `market_date` always wins.
- For an explicit current KRX trading date before its official/PyKRX run exists, a mixed view containing only `completed`, `skipped`, and `pending` stages remains `pending` when at least one stage is pending. It is not a failed or partial run because the scheduled run has not started yet. Historical dates keep the normal mixed-state `partial` semantics.
- `fs_div` defaults to the configured DART fundamentals default and is normalized to uppercase.
- Default reads are polling-safe. Expensive PyKRX enrichment coverage scans, row-count missing-symbol diagnostics, and feedback-run context are omitted unless `include_diagnostics=true` and/or `include_feedback_context=true` is passed.

### pykrx stages

- `stage_order` is the canonical default daily process order shown in the Daily Process monitor. Consumers must use the returned order and `total_stages` instead of hard-coding a stage count.
- `stages` is keyed by those same stage ids. Every default stage is present even when Core only has pending or fallback-inferred state for that stage.
- Top-level summary fields mirror the stage map for operator UIs:
  `total_stages`, `completed_stages`, `skipped_stages`, `display_completed_stages`, `partial_stages`, `failed_stages`, `blocked_stages`, `running_stages`, `pending_stages`, `stage_status_counts`, `current_stage`, and `current_stage_status`.
- Each stage includes normalized monitor metadata in addition to legacy fields:
  `stage_id`, `task`, `label`, `group`, `order`, `depends_on`, `inspect_routes`, `output_tables`, `default_selected`, and `details.progress_source`.
- Stage `details` also carries `rows_persisted`, `expected_rows`, and a readable `reason` where Core can infer one from diagnostics, activity, or errors.
- Persisted `details.result_summary` is bounded on both write and read. Bulk evidence collections are replaced by `details.result_summary_omissions` metadata, while aggregate counts and operational status fields remain available. This also compacts historical oversized rows without rewriting them.
- When the latest official daily run is only a resumed subset, omitted stages may be merged only from same-date history linked to the same exact EOD collection lineage. Conflicting or ambiguous `eod_collection_run_id`, `reused_eod_collection_run_id`, or stage source-run IDs fail closed instead of borrowing another run's completion. Automatic recovery transports `{trade_date, eod_run_id}` together and revalidates that pair under the collection lock; a missing or newer superseding automatic row yields `recovery_target_invalid_or_superseded` without a generic-latest fallback.
- Historical recovery is stage-specific. `signal_decider_evaluations` is recoverable alongside `agent_feedback_summaries`, `signal_decider`, and `next_open_execution_plan`; a newer run that omits a proven stage, including an unrelated safe-skip run for another stage, does not erase that older stage completion. Replacing a timestamped competing state requires strictly newer history. Undated or generic `pending` placeholders and diagnostic `partial`, `blocked`, or `waiting` history may overlay under the narrower fallback rules, but only stage-specific authoritative semantic evidence can establish `completed`; non-authoritative decision-stage history remains `partial`.
- Exact outer counts are not sufficient for `signal_decider` or `signal_decider_evaluations` when `details.result_summary` exists. Explicit `safe_skip=true`, `skipped=true`, `triggered=false`, non-terminal status, malformed controls, non-empty warnings, reported failures, pending work, or partial denominators reject authority. A stored `completed` row with a non-authoritative result is rendered as `partial`. Truly old exact-positive rows without a semantic payload retain their count-only compatibility; legacy zero-count rows require the same stage-specific proof as current semantic payloads.
- `signal_decider` proof requires terminal completed result/summary/cycle markers, `final_pending=0`, no remaining/current/live role, exact non-empty and matching `role_results` / `role_progress` role sets, and exact role-level requested/completed totals with zero errors or failures. The independent requested totals must sum to the summary expected count, so a self-consistent truncated summary cannot establish completion.
- `signal_decider_evaluations` proof requires positive unique request horizons and a positive `decisions_considered`; the independent expected denominator is `decisions_considered * len(horizons)`. `outcomes_upserted`, `evaluations_upserted`, and `coverage.count` must equal that denominator, and strict non-negative `computed_count`, `pending_maturity_count`, `insufficient_price_data_count`, and `not_computed_count` values must be present both at the summary root and under `coverage`, agree exactly, and sum to the denominator. Optional graded/pending controls must also agree. Pre-canonical wrapper rows are retained only when their outer completion controls, nested `evaluation_refresh` proof, optional calendar proof, and stored computed counts all agree exactly.
- Pre-open/global stages such as `master_refresh` and `pykrx_universe_refresh` may report `skipped` with reason metadata once same-date downstream output exists and those global stages have no active official stage row.
- The stored-market-data stage order is fixed:
  `price_collection`, `pykrx_enrichment`, `technical_snapshots`, `action_summaries`, `trending_ranks`, followed by the stored risk/filter/factor sequence. `signal_features` is produced after the factor sequence so feedback cannot consume a pre-factor snapshot from the same run.
- Stage counts come from the selected pykrx `eod_collection_runs` row plus downstream rows linked by `eod_run_id`, with live stage progress from `run_metadata.stage_progress`.
- The inferred current pykrx stage is resolved in this order:
  `run_metadata.stage`, then the latest populated `stage_progress` entry, then persisted downstream counts, then `price_collection` when a run exists but no better stage signal exists.
- When the selected pykrx run is still `running`, the inferred current stage is authoritative for live status display.
- A later pykrx stage stays `pending` until that later stage has actually started.
  Persisted completion of earlier stages does not advance later stages by implication alone.
- If a stage-progress entry still says `running` after the owning run has already ended, that stale running progress must be downgraded to the final run outcome instead of continuing to display as live work.
- If an official child stage still says `running` after its parent run has reached a terminal status, the child is rendered as `interrupted`, keeps `source_status=running`, and exposes `details.orphaned_running_stage`, `details.orphan_reason`, and `details.parent_run_status`. It is excluded from `running_stages` and current live activity.
- A stored `completed` agent-feedback stage is downgraded to `partial` when any role-level progress remains pending, running, partial, or has processed fewer symbols than its declared total.

### fundamentals stage

- `fundamentals_daily_refresh` is reported alongside the pykrx stages, but it is sourced independently from the latest DART fundamentals refresh run for the requested `market_date` and `fs_div`.
- That stage can be `running`, `completed`, `partial`, `failed`, or `pending` independently of the pykrx stages.
- Its counts are refresh-run metrics, not same-day snapshot-activity counts:
  `symbols_succeeded = symbols_fresh + symbols_refreshed`, and totals prefer `run_metadata.symbols_selected`.
- Its activity text should prefer the current metadata-driven refresh stage while running, but once the refresh run is failed, the run `error_message` overrides the running-style activity text.
- Per-corporation DART sync failures are retained in the refresh result and make the stage `partial` (or `failed` when every selected corporation fails); they are never normalized into a completed run. Missing-fundamentals probes are deliberately bounded and rotate by market date, so deferred symbols are not evidence of successful coverage.

### factor stages

- The pipeline stage order continues after fundamentals with:
  `momentum_factors`, `value_factors`, `low_risk_factors`, `beta_factors`, `advanced_value_factors`, `unified_value_factors`, `quality_factors`, and `composite_factors`.
- Those stages are default-selected members of the daily process. A default daily run is not complete for a market date until their stored rows exist.
- Factor-stage counts are derived from the persisted result tables for the requested `market_date`, not from dedicated run rows.
- Factor-stage `total` is anchored to the same-market-date price-collection universe when available, so missing factor rows remain visible as incomplete daily scoring coverage instead of collapsing into a false completed state.
- With diagnostics enabled, row-count-derived partial stages expose coverage diagnostics in `details`, including `missing_count`, `missing_symbols_sample`, `coverage_ratio`, `gate_status`, `severity`, and `blocking_status`.
- A partial row-count stage can be `severity=degraded` and `blocking_status=non_blocking` when it is sufficiently complete for downstream feedback. For `market_risk_filter`, an active feedback run with `run_status=running`, `failed_items=0`, and no missing rows inside that run's requested symbol scope is explicitly reported as `gate_status=ready`, `gate_reason=missing_rows_outside_active_feedback_scope`; the missing full-universe rows remain visible through the diagnostics.
- Factor stages can currently surface `pending`, `partial`, or `completed`.
  They do not expose a live `running` state because the repo does not persist standalone run-progress rows for those factor families.
- If a factor stage depends on fundamentals or earlier factor slices and those prerequisites are not yet completed, the factor stage stays `pending` and its activity text reports the blocking stages.
- `value_factors` depends on same-day price collection and PyKRX enrichment. `advanced_value_factors` and `unified_value_factors` additionally depend on the same-day DART fundamentals refresh; `quality_factors` depends on price collection and fundamentals. `piotroski_filter` depends on both fundamentals and PyKRX enrichment.
- `composite_factors` depends on the component slices `momentum_factors`, `value_factors`, `low_risk_factors`, `beta_factors`, and `advanced_value_factors`.

### agent feedback stage

- When a same-day `agent_context_feedback_runs` row or in-memory feedback run is active, `agent_feedback_summaries` reflects that run even when no first-class daily pipeline stage row exists. Runtime and durable fallback selection accepts only the exact summarizer cohort: `eod-summarizer`, `technical-summarizer`, and `fundamental-summarizer`.
- A running feedback run keeps `agent_feedback_summaries.status=running` and therefore makes the overall pipeline `status=running`; non-blocking upstream row gaps stay visible on their own stages instead of being conflated with feedback failure.
- Feedback run diagnostics include `run_id`, `run_status`, `current_role`, `failed_items`, `completed_feedback_count`, `expected_feedback_count`, `feedback_failure_status`, and `feedback_health` so consumers can separate active progress from failed or stalled feedback.
- A stale durable parent snapshot is reconciled against persisted child feedback and failure rows before status is rendered. The stage `updated_at` follows the latest successful or failed child activity, and any role-level pending, running, partial, failed, or under-counted work prevents a false `completed` result.
- Durable fallback prefers the latest canonical daily retry parent before legacy or child rows. A newer narrow child cannot replace that full summarizer cohort; child evidence is diagnostic/progress evidence only when it belongs to the selected parent lineage.

### persisted downstream outputs

- `signal_features` can be inferred as `completed` from same-date `daily_signal_features` rows.
- `external_news_collect` can be inferred as `completed` from distinct immutable articles whose `external_news_item_symbols.associated_at` falls within the same Seoul market date.
- `external_news_enrich` is based on append-only current-version classification sidecars for that associated article cohort: fully contract-classified rows are `completed`, partially contract-classified rows are `partial`, and rows that only have older-version or missing sidecars keep the enrich stage `pending` with reason metadata. The daily stage also classifies old articles newly attributed to an in-scope symbol during the half-open Seoul market-day association window, without treating another symbol's association as eligibility. Historical dates use `[00:00 KST, next 00:00 KST)`; for the current Seoul date, the exclusive upper bound is frozen once at the enrichment invocation time. Status details include contract-classified and legacy-classified counts.
- `macro_context_refresh` evaluates cooldown per enabled series. Inside the global interval it refreshes only missing or individually due series and exposes them under `retry_scope`; `reason=macro_context_refresh_not_due` is satisfied only when every enabled series is still recent.
- `signal_decider` can be inferred as `completed` from same-date `agent_context_feedback` rows for `agent_name=signal-decider` and/or same-date `signal_decider_decision_snapshots`.
- These persisted-output inferences are storage availability signals. They do not silently relax prerequisite gates.
- A late decision snapshot whose canonical observation time is at or after its target KRX open is not valid persisted authority. It is a tombstone for next-open, replay, evaluation, calibration, eligibility, and policy-candidate consumers; readers must not fall back to an older snapshot for the same source date.
- Once the next-open action window has passed, recovered feedback, decider, evaluation, and next-open plan stages use an auditable terminal `skipped` outcome with `reason=next_open_window_expired` by default. `allow_expired_analysis_replay=true` is the narrow opt-in exception for historical analysis: it permits the feedback, decider, and evaluation stages to run and persist their historical outputs. When persisted structural prerequisites are missing, that exception also requires `bypass_prerequisite_gate=true`; neither flag alone bypasses the normal structural gate. These historical outputs grant no BUY or execution authority: `next_open_execution_plan` remains fenced, returns `next_open_window_expired`, and cannot create or refresh plan items.

### gate summary

- Stages that expose downstream readiness include stage-level `gate_status`, `severity`, and `blocking_status` where applicable.
- The top-level `gate_summary` aggregates those gate statuses separately from physical stage execution/storage status.
- `pykrx_enrichment.status` can be `completed` while `pykrx_enrichment.gate_status` and `gate_summary.status` are `blocked` when required decider categories are below the minimum coverage ratio. Blocking category details remain visible under `details.decider_blocking_categories`.
- When diagnostic coverage is newer than a persisted official stage snapshot, its requested counts, category map, missing/not-applicable/warning categories, and `decider_*` gate fields are authoritative. A stale official `waiting` field must not make a completed ready gate appear waiting.
- The exact required categories are `sector_classification`, `investor_flow_symbol`, `short_selling`, and `valuation_snapshot`. The first three use a minimum coverage ratio of `0.99`; valuation uses `0.95` over the fresh PyKRX KOSPI/KOSDAQ stock universe and counts only direct PyKRX rows with positive market cap and at least one usable valuation metric. Component-level incompleteness remains visible through completeness fields and warning categories even when the category coverage gate is ready. Optional-category gaps are tolerated warnings rather than blockers or full-universe retry debt.

### overall status

- Overall pipeline `status` is `running` if any stage is `running`.
- Overall pipeline `status` is `pending` only when every stage is `pending`.
- Overall pipeline `status` is `completed` when every stage has a terminal satisfied outcome. `completed` and an explicit `safe_skip=true`/expired terminal `skipped` outcome are satisfied; generic `skipped`, `waiting`, `blocked`, `partial`, `failed`, `interrupted`, or malformed outcomes are not silently promoted.
- An interrupted or orphaned parent may converge to a terminal state only when every stage in its persisted resolved-stage set is terminally satisfied. Parent status alone, a non-price checkpoint, or unrelated same-date output is not sufficient convergence evidence.
- Before an orphaned parent resumes downstream work, Core compares admitted exact-date membership `M` with persisted exact-date price symbols `P`. `M - P` remains retry debt, `P - M` is recorded as unexpected and excluded, and downstream work is restricted to the verified admitted scope in `M ∩ P`. A durable inherited downstream scope is never narrowed to the latest price-retry request; it must be proven against both exact-date membership and exact-date bars before execution and debt clearance.
- On a same-day run where pykrx and fundamentals are complete but one or more default-selected factor stages still have no stored rows, overall status must stay `partial`.
- Overall pipeline `status` is `failed` only when at least one stage is `failed` and no stage is `completed` or `partial`.
- All other mixed states resolve to `partial`.

## Non-trading-day behavior

- Weekend behavior:
  `today.trade_date` stays on the weekend calendar date. Default pipeline status instead uses the market date of the latest persisted official run when one exists. `market_session` describes whichever date that route selected; an explicit pipeline date remains authoritative.
- Market-closed same-day behavior:
  After the market closes, `today` still refers to that same Seoul calendar date. If same-day EOD persistence is not finished yet, `today` can remain `missing` while `latest` still points to the previous stored trade date. `market_session.status=closed` and `recommendation_basis=latest_completed_eod` make that same-day closed state explicit.
- Break-day behavior:
  Exchange holidays and other non-trading break-days keep the current calendar date in `/eod/freshness.today`. Pipeline status uses its latest persisted official run date when available and otherwise falls back to the current calendar date. Neither route infers a prior trading day solely from the calendar.

## Test anchors

The contract is intentionally mirrored in focused tests and helper coverage:

- `apps/api/tests/test_eod_freshness.py`
- `apps/api/tests/test_eod_pipeline_status.py`
- `apps/api/tests/test_eod_run_stage_support.py`

Future behavior changes should update this contract and the matching tests in the same PR.
