# Weekday data-gap fill job

Status: specification and first implementation (`app/eod/data_gap_fill.py`).
Default: **disabled**; when enabled it is **report-only** until apply is enabled
separately.

## Why

The daily pipeline runs after the close (about 20:10 KST). Between the open and
mid-afternoon on a weekday Core has nothing scheduled. Gaps in yesterday's and
earlier data currently survive until someone notices them:

| Observed 2026-11 | Cause |
|---|---|
| no `daily_signal_features` rows for 11-02/03, 82 of 2,769 for 11-04 | collection did not run / ran for a narrow cohort |
| `participation_contexts` missing or built from 82 rows | derived from the gap above |
| 73,793 bars with partial volume and zero/low turnover (07-01..11-05) | bars stored before the provider finalized them |
| ~75-92 provider symbols per date with no stored bar | collection gap |
| ~2,200 bars per date whose OHLC differs from the official table | per-symbol feed was used as the price source |

This job closes the gaps that are safe to close automatically and reports the
rest.

## Scope and non-goals

In scope: detect gaps in the last `N` finalized sessions; repair the gap kinds in
**Tier A** idempotently, under the existing writer fences; leave an auditable
trail.

Out of scope, by design:

* No trading, order, decider, agent or Exec behaviour.
* **No OHLC rewrite.** Changing a stored price invalidates every derived row built
  on it, which the pending-price-bar-batch machinery tracks and gates Core
  startup on. That stays an audited, operator-run repair. This job only reports it.
* No creation of data that cannot be reproduced from stored or official sources
  (KIS rankings, quote snapshots, market-risk snapshots for past dates).

## Gap kinds

| kind | detector (read-only) | tier | fixer |
|---|---|---|---|
| `signal_features_missing` | symbols with a bar on a session but no signal-feature row | A | `stored_signal_feature_backfill.apply_backfill` |
| `participation_context_missing` | session has signal-feature rows but no participation-context row, or its recorded `captured_symbol_count` differs from the row count | A | rebuild from the stored signal-feature rows (after the fixer above) |
| `turnover_incomplete` | active KOSPI/KOSDAQ bars with `volume > 0` and `turnover = 0` | A | `turnover_repair` with `allow_partial_verified` (provider OHLC must equal stored OHLC; volume and turnover may only increase) |
| `provisional_bars` | official market frame differs from the stored bar in OHLC (latest `provider_check_sessions` only) | B | report only |
| `missing_bars` | official frame lists a symbol with no stored bar (same sessions) | B | report only |

Tier B findings are written to the log and the result so an operator can run the
audited repair tools. They are never applied here.

## When it runs

All of the following must hold, otherwise the tick is a no-op:

1. `DATA_GAP_FILL_ENABLED=true`.
2. Weekday in KST and a KRX trading day, local time in
   `[DATA_GAP_FILL_WINDOW_START, DATA_GAP_FILL_WINDOW_END)` (default 09:30-15:00).
3. At least `DATA_GAP_FILL_MIN_INTERVAL_SECONDS` since the previous run
   (default 1800).
4. No daily pipeline, EOD, DART sync or fundamentals writer is `running`. Every
   fixer enforces this itself through its writer fence; the job additionally
   treats a `blocked` fence as "try later", never as an error.

## What it may touch

* Only sessions **strictly before today's KRX date** and passing
  `validate_finalized_krx_write_date`.
* Only the last `DATA_GAP_FILL_LOOKBACK_TRADING_DAYS` sessions (default 10).
* Fixers only **add** missing rows or **raise** volume/turnover to
  provider-verified values. They never delete and never lower a value.

## Execution model

* One tick processes gaps oldest session first, kind order
  `signal_features_missing` -> `participation_context_missing` ->
  `turnover_incomplete`.
* Wall-clock budget `DATA_GAP_FILL_BUDGET_SECONDS` (default 900). No new fixer
  starts after the budget is spent.
* A fixer that fails or is blocked puts its `(kind, session)` in a cooldown
  (`DATA_GAP_FILL_FAILURE_COOLDOWN_SECONDS`, default 6 h). One failure never stops
  the other gaps.
* `DATA_GAP_FILL_APPLY=false` (default): detect and report only. No fixer is
  called.
* The module also has a CLI: `python -m app.eod.data_gap_fill` (dry-run) and
  `--apply`. The CLI ignores the schedule window and the interval but keeps every
  other rule.

## Verification contract (each item has a test)

1. Disabled -> no detector and no fixer is called.
2. Outside the window, on a weekend or holiday -> no-op.
3. Interval not elapsed -> no-op.
4. Apply off -> detectors run, zero fixers run, result lists the gaps.
5. Apply on -> only Tier A fixers run; Tier B kinds never reach a fixer.
6. Today's session and unfinalized sessions are never repaired.
7. Only the configured lookback sessions are examined.
8. Fixers run in dependency order; participation runs after signal features.
9. Budget exhausted -> remaining gaps are reported as deferred, not run.
10. A failing fixer is isolated; the others still run; the failure is cooled down
    and not retried inside the cooldown.
11. A `blocked` fence is recorded as deferred, not failed.
12. A second run after success finds no gaps (idempotent).
13. Detector failure degrades to "unknown" for that kind and does not stop the
    others.
14. Detector SQL is correct against a real database (far-future dates, rolled back).

## Rollout

1. Merge with `DATA_GAP_FILL_ENABLED=false`.
2. Enable with `DATA_GAP_FILL_APPLY=false` for a few weekdays; read the reports.
3. Enable apply. Tier B kinds stay report-only.
4. A later change may persist run records in a table; this version keeps the last
   result in memory (runner snapshot) and in the log.

## Dependencies

Requires `app.eod.stored_signal_feature_backfill` and the `--partial-verified`
mode of `app.eod.turnover_repair` (both separate PRs).
