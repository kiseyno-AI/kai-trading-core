# WAIT-conflict gate enforcement and replay

## Safety default and modes

`SIGNAL_DECIDER_WAIT_CONFLICT_GATE_MODE` remains `report_only`. Live Exec dispatch intentionally keeps signal-decider and short-term-decider authority independent. A mode change requires an explicit configuration update and API restart.

- `report_only`: records WAIT/reject/missing/stale signal context without changing short-term eligibility or quantity and preserves trigger-gated monitoring. It does not grant BUY authority; Exec must still pass trigger-time quote, chase-limit, quality, aggregate account/symbol exposure, market-authority, portfolio, pause, and kill-switch gates before submitting an order.
- `hard_block`: retained as observe/offline counterfactual audit evidence, but never changes either lane's plan eligibility or quantity; settings validation also rejects it when integration mode is `dispatch`.
- `downsize_probe`: retained as observe/offline counterfactual audit evidence, but never caps either lane's plan or Exec quantity; settings validation also rejects it when integration mode is `dispatch`.
- `disabled`: retains an explicit disabled audit state.

`allow_by_override`, enforcement decisions, and modeled probe quantities remain Core-only replay/audit evidence. They cannot turn cross-decider disagreement into a plan-time or dispatch-time veto or quantity cap, and they are stripped from Exec LLM risk context. In dispatch mode only `disabled` and `report_only` are accepted; common deterministic exposure and safety gates remain authoritative for both lanes.

Pre-existing rows keep their historical blocked/downsize fields unchanged for audit. Compatibility dispatch ignores those fields only when every persisted blocker is an allowlisted opposite-lane alignment reason and all normal lane-specific geometry and safety checks still pass; any unrelated blocker remains fail-closed.

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

Passing the numeric checks does not make cross-agent enforcement dispatch-safe. Keep live dispatch at `report_only` or `disabled`; any `hard_block` or `downsize_probe` experiment remains a Core-only observe/offline counterfactual and cannot alter either lane's executable plan.

## Monitoring is not BUY authority

For `watch_for_trigger` / `intraday_watch_buy` rows, Core delegates entry-quality concerns to Exec as monitoring warnings instead of removing the symbol from monitoring. A weak setup, WAIT/reject disagreement, price already above the chase limit, or unavailable/risk-off live market BUY authority may prevent an order, but it must not revoke the monitor itself. Structural contract failures such as missing trigger, chase limit, target, stop, account route, or invalid target/stop ordering still block handoff because Exec cannot monitor those rows safely.

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
