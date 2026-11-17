# WAIT paper outcomes

The WAIT paper ledger records virtual entries and target/stop exits independently for `signal-decider` and `short-term-decider`. It does not create broker orders or modify actual fills. Session reads include carried positions that entered earlier and were still unresolved at the selected session start or exited during that session; prior closed positions and stale unentered WAIT rows are excluded. The original cohort date stays on each row. The existing stored-only `GET /execution/runtime/monitoring-session-results` exposes `paper_trades` and `paper_summary` without refreshing or recovering history on a read.

Quantities and round-trip cost assumptions are frozen with each decision. Net P&L subtracts half the round-trip basis-point rate on entry turnover and half on exit turnover. Open net marks estimate the second leg at the current stored mark; they are unrealized and reported separately. A trade touch models an entry without proving liquidity. A top-ask entry is a simulation capped by recorded displayed size. Neither is a confirmed fill.

For new live observations without a usable frozen or stored planned quantity, `quantity_source: paper_budget_risk_reference_v1` identifies a paper-only reference quantity derived from the exact stored plan allocation and existing risk fraction, guarded entry price and stop distance. Its saved `quantity_basis` describes the assumption; it is not a broker fill, historical actual quantity or permission to bypass Core execution gates. Historical recovery does not retroactively apply this reference rule: missing historical quantity stays unknown.

Statuses are `pending`, `open`, `target_exited`, `stop_exited` and `unavailable`. Missing inputs and path gaps stay explicit. An unknown path must not establish a target/stop exit, win, loss or zero return. Read totals alongside status counts, known-realized and known-mark counts, incomplete counts and each row's coverage reasons. Confirmed open counts, quantities and marks exclude unresolved paths but may include a date-scoped `stored_daily_bounds_no_exit` certificate; `unresolved_open_trade_count` reports those separately. Role totals remain independent even for the same symbol. A sticky `paper_recording_error` reports a sanitized error class and degrades coverage until the process restarts; later successful writes do not erase the failure.

Session results use the exchange-calendar session cutoff and explicitly include retrospective no-exit certificates whose economic cutoff is no later than that session cutoff. A certificate retains `retrospective`, `economic_cutoff`, `published_at`, daily-bar provenance and `coverage_through`: it proves only that the stored daily bounds stayed strictly between exit levels through that date, not continuous quote coverage. Its open quantity and mark may be known even though `active_tracking` stays false; a read never reactivates historical monitoring. At the next session open without another proof, exposure and marks become unresolved again. Daily Review uses the default strict history at its requested observation timestamp and excludes any certificate not yet published, so later evidence cannot rewrite what the reviewer knew then. Daily Review consumes virtual outcomes as `paper_execution`, actual broker outcomes as `actual_execution`, and daily bars as dated contextual marks. Its summaries cover all ledger rows; up to six examples per role prioritize closed and open trades and omit raw event timelines. Its legacy weekly/five-day bar model remains a separate counterfactual.

The compatibility `hypothetical_return` field retains the old trigger-to-mark calculation under `method: legacy_trigger_to_mark_gross` and `deprecated: true`. It assumes entry without a touch, ignores exits and costs, and cannot stand in for the paper ledger.

## Stored-only recovery

From `apps/api` with the normal service environment loaded, preview with
`python -B -m app.agent_context.wait_paper_recovery 2026-11-11 2026-11-12`.
Add `--apply` only after reviewing the JSON report. Explicit
`--observations-root`, `--monitoring-root` and `--ledger-root` arguments select
the existing observation, last-quote and paper-ledger directories. The preview
does not write ledger files; `-B` also prevents Python bytecode-cache writes.
Recovery reads bounded stored database evidence in a read-only transaction and
never calls providers, deciders or broker execution. Existing lots and original
touch files are preserved. Sparse price points cannot establish the first
target/stop exit; unavailable quantities, costs and path gaps remain explicit
instead of becoming fabricated profit or loss.
