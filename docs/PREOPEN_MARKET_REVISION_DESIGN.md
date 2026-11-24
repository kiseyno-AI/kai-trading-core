# Independent KRX US-rebound rule strategy: development specification

Date: 2026-11-24. Status: proposed development plan, not implemented or deployed. This supersedes the earlier optional decider-reassessment and mandatory-preopen-selection designs. The agreed direction is an independent rule strategy: no summarizer/decider call, no artificial score bonus and no existing six-point gate on the new strategy. Existing two-decider processing continues unchanged. [Research, targets and relative-shock definitions](research/us-krx-overnight-rebound-2026-11-24.md).

Source baselines inspected: Core `a7fa8718c35d4630e50ec8610e4525d0510ff445`; Exec `b3e0254d619e4366a7200d0f76117ab2f646658b`. File/module names below that do not exist yet are proposed implementation locations.

## Product behavior and initial scope

- Keep original daily, feedback, score thresholds, plans, 08:00 dispatch, monitoring and quantity refresh independent of this job. Its absence/failure/late result never holds or revokes baseline authority.
- Add one explicit strategy owner, stable `strategy_id=krx_us_rebound`, with separate `rule_version`. Do not impersonate either decider or create fake confidence/snapshots to pass existing contracts.
- Start research/paper validation with a small configured universe of liquid, unleveraged KRX index/sector ETFs. Verify listing, KIS orderability, ETF tick sizes, adjustment conventions and instrument-specific fees/taxes. Do not assume current common-stock universe filters/economics already support them. Specific tradable instruments and parameters require validation, not ad hoc live selection.
- First strategy version evaluates intraday entry and timed same-session exit. No averaging down, same-day reentry after an exit, leverage or automatic overnight expansion. Incomplete exits remain real holdings subject to protection/reconciliation; a timer cannot guarantee a fill.
- Baseline entries have priority. Initially skip a new rule entry when the same symbol already has original executable/potential authority, orders, holdings or ambiguous exposure; do not change the original two lanes' existing independence. Use only residual budget/capacity after baseline entitlement.

## Source-backed development gaps

| Existing capability or constraint | Development implication |
| --- | --- |
| Core has overseas ETF quote/daily wrappers, but no wrapper for KIS direct overseas-index daily OHLC. [client.py:862,890](../apps/api/app/brokers/kis/client.py#L862) | Add the documented direct-index wrapper, master identity validation and actual preopen receipt check. |
| Core quantity calculation requires a recognized decider and confidence at least six. [core_quantity_sizing.py:98](../apps/api/app/orders/core_quantity_sizing.py#L98) | Add a genuine rule-risk sizing input/policy; reuse common caps/reservations without inventing an agent confidence. |
| Current Core plan storage requires a real decider snapshot FK. [db_models.py:2423](../apps/api/app/db_models.py#L2423) | A rule evaluation needs honest typed provenance and storage binding rather than a fabricated snapshot ID. |
| Exec strict v4 accepts only two decider identities and snapshot-bound entries/protection. Exec `api/schemas/execution.py:297,376,597,653`; `services/core_handoff_v4.py:2633`. | Add a versioned capability-checked rule-origin variant while leaving the existing agent variant unchanged. |
| Exec `core_sized` entry/protection bypasses trigger-time agents. Exec `services/core_quantity.py:28`, `trigger_time_exec_agent.py:245`. | Reuse deterministic execution after truthful rule intake; no second broker/order engine. |
| Plan/quantity expiry stops new entries but does not sell holdings. Existing max-holding exit is date-based, not a near-close timer. Exec `core_quantity.py:309`, `monitoring.py:12121,14039`. | Add explicit immutable session-aware timed-exit support; do not equate expiry or `trading_sessions=1` with end-session liquidation. |

Exec paths are relative to `kai-trading-exec/apps/api/app/`. These are source observations, not tests of deployed account permissions or live behavior.

## Modules and data to build

Use a cohesive proposed Core feature package `apps/api/app/overnight_rebound/`, with small collector/features/rules/repository/runner modules. Its public orchestration accepts target session, immutable input and rule configuration and returns a durable typed evaluation. Keep numeric feature/rule functions pure and testable. Reuse KIS HTTP, calendar, price/economics validation and common sizing controls; add no new SDK or heavy model dependency.

1. **Collector and repository:** validate KIS direct-index master symbols and request the latest completed US-session OHLC plus necessary history. Persist dated bars, source timestamps, first receipt, adjustment identity, raw hash and immutable daily input snapshot. Incremental daily reads are shared by the whole configured KRX universe. Missing history is prepared separately offline, never repaired by replaying the original daily pipeline.
2. **Feature calculator:** daily return, percentage-point acceleration, lag-only normalized return shock/percentile, prior US and KRX selloff, technology/semiconductor relative moves. Preserve null/zero distinctions, exclude the event session from reference volatility, and never interpret z as a probability. A US holiday is no-new-session, not a zero-return event.
3. **Versioned rule evaluator:** prespecified regime and entry/exit rules emit `NO_SIGNAL`, `WATCH` or `ENTRY_READY`, with evidence, validity, universe, price/stop/target/chase geometry and explicit rejection reasons. No LLM or decider score is required. Fit/select parameters on earlier data and validate later data before enabling them; no hardcoded +2/+4 shortcut.
4. **Plan adapter and risk sizing:** turn a validated setup into an immutable linked entry/protection/timed-exit family. Reference the real rule evaluation, input and configuration. Compute bounded whole-share quantity from explicit loss/notional/residual-budget policy rather than a fixed one-million-won amount or forged confidence.
5. **Independent runner and read model:** own job IDs, leases, retry debt, errors and status. Add a proposed authenticated read-only `GET /strategies/us-rebound/status?target_trade_date=...` with session/input/rule/run state, result counts and publication/readback evidence. Exclude account amounts/quantities from public consumers. GET does not collect, train or execute. Update actual API references/contracts when implemented; no frontend rewrite is required for the first slice.

Persist strategy runs, frozen snapshots, rule evaluations and publication/outbox state separately from ordinary feedback/cohort/failure records. Existing original upserts and max-latest-snapshot selectors must never consume this strategy. Use explicit origin-scoped keys and real IDs in uniqueness, fingerprints, recovery and readback. A disabled/failed rule job must not change original status or counts.

## Entry logic and quantity policy

The activation condition is a validated relative-shock regime plus a tradable post-open setup, not merely a positive US day. Use a predetermined finite universe and entry window. A preopen positive signal prepares a bounded watch; at OPEN the current gap, price, spread, liquidity and net edge must still satisfy typed numeric rules. An exhausted opening gap or expired setup can produce no trade even with a positive market forecast.

Rule configuration must specify event thresholds/window lengths, any historical forecast/support requirement, entry/chase cap, stop/target, per-trade/strategy loss and notional budgets, entry expiry, reentry prohibition and close-relative timed exit. Unvalidated or unsupported configuration cannot create an executable evaluation. The US input stays immutable/session-scoped; live Korean evidence has its own shorter freshness requirement.

Use a separate rule-risk sizing branch that reuses common account/broker caps. Conceptually bound quantity by the minimum of residual cash / conservative entry price, rule loss budget / conservative per-share stop risk, residual symbol/notional budget and proven liquidity/buying-power ceilings; round down to valid whole shares. Model fees/slippage and stop execution uncertainty conservatively. Missing required economics/capacity yields no executable quantity. No calibrated probability or confidence is fabricated to satisfy old sizing fields.

Baseline quantities and entitlements are calculated first. Pending/shadow rule quantities cannot enter original virtual-reservation sums or shared weighted allocation. At actual admission, account truth, reservations and baseline priority are checked again. Successful rule fills consume real shared cash; analysis failure does not.

## Core-to-Exec contract work

The existing v4 path cannot truthfully ingest this strategy unchanged. Introduce a versioned provenance union, conceptually:

- `origin=agent_decision`: existing agent identity and real decision snapshot; semantics unchanged.
- `origin=rule_strategy`: stable strategy ID, rule version, strategy run/evaluation IDs and immutable input hash; no required decider identity/confidence/snapshot.

Bind entry, target, stop and timed exit to the same origin, real evaluation and immutable material. Carry that proof through ACK validation, activation, quantity revisions, cumulative allowances, idempotency, readback and restart recovery. Update strict DTOs and capability negotiation in both repos, plus their per-endpoint references and consumer contracts. Unsupported rule capability skips only the new strategy; never weaken the agent variant or reject existing plans because it is absent.

Add explicit strategy-owner attribution in Exec order/holding/monitoring paths. Today ownership derives from the decision agent (`services/broker_orders.py:395`). A new rule version/hash must not reset ownership, spent allowance or daily reentry limits. Use a stable session/symbol/setup claim across retries and parameter changes. Protect baseline plans from newest-plan selection/duplicate cleanup; the new origin must not supersede original watches.

At rule activation and the immediate serialized broker boundary, validate exact evaluation, strategy owner, setup/session/entry validity, no conflicting original exposure, residual capacity and orderability. Shared account/control/order serialization must prevent concurrent duplicate entry. If baseline wins a conflict, deny only the rule entry. Keep existing kill switches, market gates, quantity/cash/symbol controls and protection. A price touch adds no provider or agent round trip.

## Explicit timed exits

Extend the contract with a canonical session-aware `exit_at`/`close_by` policy and durable family-linked sell scheduling. The initial intraday research convention can be close-minus-buffer, with the exact buffer chosen and tested before activation. Do not promise the close price or guaranteed liquidation by that timestamp.

Timed exit must sell only remaining strategy-owned filled inventory after accounting for target/stop fills and working/unknown sell orders. Serialize competing exits, preserve idempotency and recover pending timers/submissions after restart. Do not close another strategy's holdings or duplicate a sell. Unfilled/unknown exit outcome remains visible and protected/reconciled; plan expiry does not declare the position flat.

## Schedule and failure isolation

For ordinary 09:00 KRX sessions, proposed KST schedule: preflight 06:45; KIS attempts 07:00/07:05/07:10 only for failed inputs; freeze input 07:12; calculate/evaluate from 07:15; prepare/watch/publish before the 08:40 first-activation deadline; deterministic entry checks at OPEN. There is no 07:15–08:30 agent queue or feedback deadline in this strategy. Original 08:00 dispatch continues unchanged.

Use verified target/US trading calendars and aware timestamps. Current hardcoded-hour inconsistencies require validation; skip the optional strategy on special-hours sessions until support is proven, without altering baseline behavior. Keep request/resource budgets explicit: three required indices mean at most nine ordinary requests, with token renewal/pagination/fallback independently bounded. Higher-priority original data/order work wins resource contention.

No required daily stage, original critical-step/error/cooldown key, session-wide arming, baseline hold, baseline replacement or original tombstone is added. The job owns durable retries/status and resumes only its exact debt; no re-running summaries/deciders or original suffix recovery. `NO_SIGNAL`, optional unavailability and expiry are normal independent outcomes, not original-pipeline failure.

Lost register/activate/order responses require same-ID reconciliation. Exec may already have committed an activation; no second plan escapes uncertainty, and Core unknown is not proof of remote inactivity. First positive activation checks current server time after lock waits strictly before deadline; exact historical replay does not create new authority. Existing selected-family quantity/protection management and safe denial remain possible afterward.

Analysis/orchestration isolation is enforceable. Once real rule orders are submitted, cash/exposure and unknown submission safety are shared with the same broker account. Existing `account_submission_uncertain` checks must remain effective; no claim of post-submission financial isolation or physically zero hardware contention is made.

## Development slices and verification gates

1. **Contract/storage isolation:** rule provenance and owner, real evaluation storage, capability negotiation, separate rule-risk sizing and typed timed exit. Deploy nothing executable; keep feature disabled and prove unchanged old agent-contract behavior. This is the prerequisite to avoid fake roles/scores.
2. **KIS collection/features:** prove master coverage and actual before-open latest-session receipt; persist history and pure lag-only calculations. Add independent status GET and source/error diagnostics. No agent dependency.
3. **Rules and historical evaluation:** define the minimal ETF universe, instrument economics, entry/gap/stop/target/time-exit/risk parameters. Compare existing behavior, +2/+4 bonus experiments and rule strategy under identical point-in-time executable prices/costs. Chronological tuning/holdout, failed rebounds, gap-priced returns and drawdown matter; no winner-only replay.
4. **Observation then paper integration:** independent scheduled job, immutable rule plans, truthful Core-to-Exec path, deterministic entry/protection/timed exit and exact recovery. Existing paper mode is not assumed to mean zero broker/external effects without verification. No live authority until prospective results and fault tests pass.
5. **Reviewed optional activation:** reviewed/pushed compatible commits in both services, explicit disabled/observe/paper/live controls and future-session enablement. Unsupported capability, late/failed job or unavailable residual resources skips the rule strategy; baseline always keeps its existing route.

Required tests include: old agent payload/proof/sizing regressions; disabled/absent/failed rule job leaves original IDs/cohorts/status/prices/quantities/monitoring/dispatch unchanged; strict origin isolation and no fake FK; KIS stale/zero/future/holiday/DST bars; pure feature arithmetic and zero-volatility cases; positive regime but exhausted gap; ETF ticks/costs; quantity without decider/score; duplicate runs/version changes/reentry; baseline/rule same-symbol races; lost ACK and unknown orders; pending allowances not reserving baseline capital; target/stop/timer competition; partial exit and restart after entry expiry. Verify actual cash/holdings safety rather than pretend real exposure is isolated.

Source/test anchors: Core [handoff DTO](../apps/api/app/orders/execution_handoff_contract.py), [sizing](../apps/api/app/orders/core_quantity_sizing.py), [handoff tests](../apps/api/tests/test_exec_integration_service.py), [quantity refresh tests](../apps/api/tests/test_core_quantity_refresh.py), [runner tests](../apps/api/tests/test_eod_runner.py). Exec: `tests/test_core_handoff_v4.py`, `tests/test_core_quantity.py`, `tests/test_core_quantity_residual.py`, `tests/test_monitoring_decision_integrity.py`, `tests/test_monitoring_family_isolation.py` and current max-holding/protection tests.

Independent contract review found no concrete blockers in removal of decider/six-point dependencies, typed rule provenance/stable ownership, rule-risk sizing, explicit timed exits and baseline-priority residual budgets. This is a document/source review, not evidence of implemented compatibility or passing runtime tests.

No KIS/authenticated broker/model calls, runtime implementation, new strategy backtest, fault injection or deployment were performed for this document. Specific instruments, thresholds, timed-exit buffer and acceptable risk remain validation work, not invented production defaults.
