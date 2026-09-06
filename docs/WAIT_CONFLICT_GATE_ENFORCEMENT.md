# WAIT-conflict gate enforcement and replay

## Safety default and modes

`SIGNAL_DECIDER_WAIT_CONFLICT_GATE_MODE` remains `report_only`. A mode change requires an explicit configuration update and API restart.

- `report_only`: records the conflict and preserves requested quantity.
- `hard_block`: makes a non-override conflict ineligible and sets final quantity to zero.
- `downsize_probe`: deterministically caps final quantity at `SIGNAL_DECIDER_WAIT_CONFLICT_PROBE_QUANTITY` without increasing a smaller request.
- `disabled`: retains an explicit disabled audit state.

`allow_by_override` remains valid only when every documented continuation, reward/risk, quality/return, volume, and raw-liquidity check passes. Missing or stale same-date signal-decider context is explicitly classified and follows the configured conflict action; it is never silently approved. Core persists `enforcement_decision`, `requested_quantity`, and `final_quantity` in the plan validation, traceability, Exec handoff, and item risk context. A downsized handoff uses a binding fixed quantity so Exec cannot restore full size.

## Weekly operator replay

From `apps/api`, run:

```bash
python3.11 -m app.internal.ops agent-short-term-wait-conflict-replay \
  --completed-weeks 4 \
  --gate-mode downsize_probe \
  --probe-quantity 3 \
  --output-format markdown
```

Use `--as-of-date-kst YYYY-MM-DD` for a reproducible rolling window. An explicit `--market-date-from` and `--market-date-to` pair is also supported. The rolling form always covers at least four completed Monday-Friday KST weeks.

The report uses only persisted short-term entry fills and their actual execution outcomes. It reports resolved skipped losses, resolved skipped winners, baseline and counterfactual realized P/L, gross P/L delta, cost-adjusted estimated net P/L delta, missing alignment evidence, and unresolved/open filled candidates separately. `--probe-quantity` accepts an integer from 1 through 100 and defaults to 1. For `downsize_probe`, that one replay-level quantity is applied uniformly to every historical trade, overriding persisted probe settings and supplying the modeled quantity for legacy rows. Skipped-loss and skipped-winner P/L is weighted by the removed quantity share; requests already at or below the requested replay probe add no removed-exposure evidence. Unresolved candidates receive no synthetic P/L.

## Promotion threshold

The report exposes these checks under `promotion_readiness`:

- at least 4 completed Monday-Friday windows that each contain resolved replay evidence;
- at least 20 resolved WAIT-conflict outcomes;
- positive cost-adjusted estimated net P/L delta, including any supplied fee and slippage assumptions;
- avoided losses greater than skipped-winner P/L;
- unresolved WAIT-conflict ratio no greater than 25%;
- manual approval for any live configuration change.

Passing the numeric checks does not activate enforcement automatically. Keep `report_only` unless all checks pass and the operational owner explicitly approves either `hard_block` or `downsize_probe`.

## Issue #547 regression week

The reproducible fixture is `apps/api/tests/fixtures/wait_conflict_week_2026-08-31.json`. For the completed 2026-08-31 through 2026-09-04 KST week, the bounded live Exec artifact reported:

| Result | Value |
| --- | ---: |
| Filled buys | 19 |
| WAIT-conflict buys | 16 |
| Valid overrides | 2 |
| Legacy missing-alignment buys | 1 |
| Resolved skipped losses | 7 / 34,163 KRW avoided |
| Resolved skipped winners | 2 / 25,400 KRW skipped |
| Baseline realized P/L | -41,851.12 KRW |
| Hard-block counterfactual | -33,088.12 KRW |
| Net realized P/L delta | +8,763 KRW |

Reported unresolved/open examples were `003550`, `035810`, `027710`, and later `004690` buys. They remain outside realized P/L calculations.

## Replay limitations

- Core synchronizes the affected outcome rows after persisting Exec readback. Readback that has not yet been collected remains missing evidence.
- Exec readback payloads are compact and may cap execution rows. Core reconciles against attributed order totals when present, but this is not an account-wide FIFO ledger.
- The issue-week fixture preserves the bounded external Exec result, including its FIFO attribution. Core's normal rolling report uses plan-attributed fills and aggregate average fill prices.
- Source-date attribution uses the persisted Core plan alignment gate. Legacy rows without that evidence are identified and treated conservatively.

## Activation and rollback

Before activation, save the rolling JSON report, confirm every promotion check, and record the approving operator and chosen mode. Set the mode and optional probe quantity in `apps/api/.env`, restart the API, force a next-open plan refresh, and inspect the new plan rows before dispatch.

To roll back, restore `SIGNAL_DECIDER_WAIT_CONFLICT_GATE_MODE=report_only`, restart the API, and force-refresh the next-open plan so undispatched rows are rebuilt with the report-only decision. This configuration rollback affects future Core plans only: it does not recall or resize plans already accepted by Exec. Handle already accepted plans through the normal Exec operational controls and verify their readback before resuming dispatch.
