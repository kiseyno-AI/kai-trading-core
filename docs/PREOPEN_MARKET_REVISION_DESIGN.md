# Optional US-market supplement: isolation and positive-use policy

Date: 2026-11-24. Status: proposed, not implemented or deployed. Latest user requirement: supplement failure must not become a dependency, delay, error, pause or revocation in the existing daily/monitoring/execution workflow. This replaces the earlier mandatory preopen-finalization design. Relative-shock mathematics and research remain in the [research plan](research/us-krx-overnight-rebound-2026-11-24.md).

Source baselines: Core `a7fa8718c35d4630e50ec8610e4525d0510ff445`; Exec `b3e0254d619e4366a7200d0f76117ab2f646658b`. Existing source facts and proposed behavior are distinguished below.

## Baseline independence

The original daily pipeline, summary/decider publications, completion proof, retries, 08:00 delegated dispatch, quantity refresh and monitoring continue on existing rules. They never wait for supplement completion. Remove the previous session-wide arming, deny-by-default baseline policy, pending baseline holds, baseline replacement and mandatory 08:40 selection.

Use a separate bounded task and durable job/candidate/failure/retry/status records. Do not add a required daily stage, feed supplement errors into baseline completion, or invoke `_run_buy_authority_critical_step`, ordinary runner error/cooldown keys or global buy-pause logic. Those paths can degrade baseline status or revoke authority: [runner.py:591,634](../apps/api/app/eod/runner.py#L591). Reuse the background-task shape while catching errors inside the supplement owner: [runner.py:509](../apps/api/app/eod/runner.py#L509).

Original work has priority over optional agents, KIS calls and quote subscriptions. Dispatch optional work only with proven bounded headroom; stop new optional admissions when original work needs that capacity. If headroom cannot be established, skip the supplement. Shared hardware/gateway contention must be measured; physically zero resource competition is not claimed.

## Optional schedule

For an ordinary 09:00 KRX opening, proposed KST budgets are:

| Time | Supplement | Baseline |
| --- | --- | --- |
| Previous evening / 06:45 | Read immutable original publications/cohorts; check target session, source, models, capability and spare capacity. | Original daily runs normally; no control-message ACK or wait. |
| 07:00 / 07:05 / 07:10 | Collect validated KIS US indices; retry only failed/stale inputs, at most three attempts per required index. | Original broker/data requests retain priority. |
| 07:12 | Freeze completed-US-session rows and cutoff, or record supplement unavailable. | Original publications and plans unchanged. |
| By 07:15 | Calculate relative shocks and validated forecasts; persist exact optional review scope. | No changed original cohort or score. |
| 07:15–08:30 | Both real decider roles independently review optional candidates with reused source-valid summaries. | Original 08:00 dispatch and quantity refresh proceed normally. |
| 08:30–08:40 | Accept only timely complete feedback; publish successful supplemental observations/plans after priority, identity, risk, capacity and duplicate checks. | No original family is revoked, repriced, resized or superseded. |
| After 08:40 / OPEN | No first positive activation for this campaign; exact reconciliation continues. Accepted entries use existing deterministic quote/order checks. | Original execution, protection and quantity validity remain effective. |

These are proposed deadlines, not measured promises. Feedback acceptance is strictly before 08:30; first supplemental activation is strictly before 08:40, using server wall time after lock waits. Derive target and US sessions from trading calendars, DST and early closes. Current Core/Exec have inconsistent hardcoded hours: until verified session bounds exist, skip optional execution on special-hours sessions rather than block or change the original process. [Core clock](../apps/api/app/market/clock.py#L49), [snapshot validity](../apps/api/app/agent_context/signal_decider_snapshot_validity.py#L12).

Required/optional instruments and history belong to the validated model manifest. Three required indices imply at most nine ordinary provider requests, excluding separately tracked token renewal; pagination/fallback must fit a declared budget. Prepare history offline. Validate identity, expected completed US session, positive finite prices and receipt before cutoff. Old/zero/missing data is not fresh or a synthetic zero return. Freeze input hash/model/calendar/cutoff across retries. A US holiday with no new session is not a zero-return event.

## Where positive evidence is used

1. **Proposed structured `overnight_market_context`:** supply US session/date, input cutoff, daily returns, lag-only shock z-scores/percentiles, percentage-point return acceleration, prior US/Korean selloff, sector/broad-market confirmation, source quality and validated forecasts/support. Uncalibrated probabilities remain null. A positive label is not a confidence bonus or order permission.
2. **Optional fresh decider requests:** a validated material event triggers review of the proven eligible cohort, including prior REJECT/AVOID_CHASE or observation-only WAIT candidates. Keep hard symbol-safety exclusions. Use structured market dependencies where available; do not infer rejection types from prose. Preserve the real short-term/signal role names, horizons and independent prices.
3. **Context precedence within that optional request:** dated prior-session risk-off remains historical evidence; the new target-session assessment separates observed facts and forecasts. It must not be overridden by an unqualified stale instruction. Original requests, regime rows, feedback, scores and cutoffs are never mutated.
4. **Fresh role verdict:** judge the stock's opportunity after the anticipated opening gap, using its own technical/fundamental/sector evidence and complete trigger/chase/target/stop geometry. The new verdict may remain REJECT or become WAIT/BUY. Existing confidence thresholds, including the six-point execution criterion, remain unchanged.
5. **Additive observation or entry:** a complete WAIT can add a supplementary Core observation under existing observation rules. An execution-qualified WAIT/BUY may prepare a new entry only if that account/session/symbol/lane has no executable or potentially executable original family, no conflicting same-lane exposure, and enough residual capacity. REJECT/AVOID_CHASE creates no actionable monitoring entry and never vetoes the original decision.
6. **Existing Core sizing:** size only the new candidate from demonstrably residual cash/risk/symbol capacity after original entitlements. Its confidence, conservative net edge/stop risk and liquidity feed the existing calculator; no direct US-market quantity multiplier or original quantity rebalance is introduced.
7. **Existing deterministic execution:** an accepted `core_sized` entry uses its own prices/typed conditions and fresh Korean market/account/symbol/broker checks. No agent or US-data request is added at a price touch. Overnight optimism never overrides adverse live evidence or expiry.

| Original state for account/symbol/lane | Positive supplement treatment |
| --- | --- |
| Executable or potentially executable original family | Preserve it; publish no competing supplemental entry. Optional interpretation is informational. |
| Observation-only WAIT | Preserve the original observation and prices; a separately validated fresh verdict can add executable authority only after proving no original entry/exposure conflict. |
| REJECT/AVOID_CHASE and no executable family | Optional fresh review, not automatic acceptance; complete qualified verdict can add an observation/entry. |
| Same-lane holdings/orders/uncertain acceptance | Skip the supplement; preserve protection/reconciliation. A second plan ID is not a new allowance. |
| Another lane has an eligible entry | Keep lane independence with shared account/symbol limits. One role's rejection is not the other's veto. |

Replacing an already-executable original plan's prices or quantities is outside this initial version: a replacement can interrupt the original after revocation. No such replacement is authorized here.

## Persistence and execution isolation

Ordinary feedback/failure records are unique on `(market_date,symbol,agent_name)`: [db_models.py:1727,2708](../apps/api/app/db_models.py#L1727). Their upserts must not store supplementary feedback/errors. Ordinary snapshot/new-run selection can supersede source authority: [next_open_execution.py:6625,6973](../apps/api/app/orders/next_open_execution.py#L6625). Existing plan uniqueness/refresh maps also lack namespace and can retire unexpected rows: [db_models.py:2313](../apps/api/app/db_models.py#L2313), [planner:4831,4960,5393](../apps/api/app/orders/next_open_execution.py#L4831).

Proposed compatible seam: separate supplement job/candidate/failure records; on explicit successful publication only, create a real immutable snapshot and plan with `decision_namespace=us_rebound_supplement`. Existing records default to `baseline`. Snapshot feedback linkage is nullable, while the plan snapshot FK requires a real new snapshot: [db_models.py:1839,2426](../apps/api/app/db_models.py#L1839). Never fake an ID or attach the new verdict to an old rejected snapshot.

Namespace belongs in uniqueness, original-query filters, lookup maps, source/reuse/generation hashes, dispatch grouping and exact recovery/readback. Original paths explicitly read baseline; the publisher reads exact supplement job/input/candidate identities and its own lineage proof. Ordinary completion/cohort resolvers are not overwritten. Additive status views can explain the new evidence without granting authority.

Route callbacks/errors using the persisted server-owned job identity and purpose, not an agent's self-reported label. Missing or malformed supplementary routing must not fall through to the ordinary feedback/failure upsert. Verify the same isolation for source-reuse admission, late responses and API readback.

Both prompt forms must carry the structured evidence for supplemental requests. Any added field remains optional for baseline contracts; a supplement-only missing capability/field cannot fail original dispatch. Report supplemental status/counts separately, deduplicate shared symbol subscriptions, and never mistake multiple historical versions for multiple independent same-lane entries.

Exec chooses newest monitorable plans per symbol/lane and can supersede older watches (`monitoring.py:7344,4924`). Add explicit original-family priority before supplementary selection/materialization/duplicate cleanup; a new plan ID alone is insufficient. Retain actual role names so same-lane protection applies. Optional quote observations cannot evict original subscriptions or consume mandatory provider capacity; insufficient spare capacity skips the supplement.

Pending/shadow/proposed supplementary quantities must not become original virtual reservations or change original cash shares. Core currently counts outside-cohort allowances and uses shared weighted allocation: [core_quantity_refresh.py:187](../apps/api/app/orders/core_quantity_refresh.py#L187), [sizing:142](../apps/api/app/orders/core_quantity_sizing.py#L142). Calculate original entitlements first, exclude unpublished/pending supplements from original allocation, and size only from residual capacity. If fresh capacity cannot be proved promptly, skip the optional entry.

Register complete supplementary families through existing immutable v4 intake/activation-pending state. Persist requests before network delivery and validate exact material ACKs. New typed origin/capability applies to supplementary entries only; missing capability skips them, never legacy original plans. Holds/deadlines/recovery target only supplement records. There is no session-wide original-arm requirement, missing-record denial, original tombstone or revocation.

At supplementary activation and the immediate serialized broker boundary, recheck original same-slot authority, same-lane holdings/orders/uncertain acceptance, exact new identity/validity and residual capacity after original entitlement. Use existing account/plan/order serialization plus an entry claim across both origins for the same account/session/symbol/lane. Conflicts deny only the supplement; never revoke, shrink or delay the original to fit it. An already admitted submission is possible exposure, not retractable proof of a clean slot.

Exact post-cutoff replay can return an original receipt without creating authority. A lost activation ACK may mean Exec is already active: reconcile the same request/IDs, report unknown, create no second allowance and do not roll back the original. Current readback, not a historical receipt, determines whether the supplementary entry is active or superseded.

## Failure contract and the shared-account limit

| Failure/result | Required behavior |
| --- | --- |
| Collection/calculation/no-new-session/insufficient-support error or absence | Record only supplement status; original daily, dispatch, monitoring and quantities continue. |
| Busy queue, AI timeout/invalid/late/partial response | Skip that optional item; do not cancel original work, rerun summaries or alter original failure/retry counters. |
| Supplement REJECT/no eligible candidates | No-op; no original veto, downgrade, tombstone or revocation. |
| Local publication/capability/lineage failure | No partial authority. Original selectors cannot consume these records. |
| Original wins same slot or spare cash/feed capacity is unavailable | Deny/drop only the supplement. |
| Restart, duplicate scheduling or stale lease owner | Resume exact supplementary debt with frozen IDs/hash/cutoff; no original suffix recovery or wait. |
| Lost registration/activation ACK | Reconcile exact IDs; remote commit may exist. No second plan and no original rollback. |
| Actual/uncertain supplemental broker exposure | Apply existing account truth/safety; this cannot be isolated from shared cash/exposure checks. |

The enforceable guarantee is analysis/orchestration isolation before published/submitted exposure, not a separate financial account. Once an optional order is submitted, pending/unknown orders and fills consume shared cash and can trigger existing account-wide safety. Exec derives `account_submission_uncertain` from such orders (`core_quantity.py:247-278`). Ignoring this would weaken safety. Strict zero influence after broker submission would require a separate execution account or keeping the supplement observational; neither is silently introduced. Confirmed broker rejection and unknown submission are different outcomes.

## Required verification and delivery

- Run identical frozen baseline fixtures with supplement disabled, absent, failed, timed out, late and restarted. Compare original feedback/snapshot/plan IDs, cohorts, completion, retries, scores/prices, quantities, dispatch timing and monitoring symbols; they must match.
- Inject every optional prepublication failure and exhaust resource/request budgets. Assert original priority, no original critical-step/error/cooldown invocation and no original pause/wait.
- Persist newer high-score shadow/failed supplements and run ordinary selection, refresh, cleanup, quantity and restart paths. Assert no original supersession/retirement or pending virtual reservation.
- Test positive evidence alone, a new REJECT, observation-only WAIT, qualified WAIT/BUY, hard symbol risk and invalid prices. Only a complete admissible final verdict can add authority; thresholds remain unchanged.
- Race original/supplemental same-slot entry, two lanes on one symbol, pending baseline ACK, existing holdings and changed cash. Assert original priority before admission, no extra same-lane entry, preserved exits and common cash/symbol caps.
- Fault-inject ACK loss, duplicate delivery, deadline lock waits and crash recovery. Also test the explicit shared-account unknown-order limit rather than claim original execution ignores real exposure.
- Verify US/KS calendars, completed sessions, immutable cutoffs and lag-only calculations. Skip optional special-hours execution until verified; do not change original calendar behavior as a side effect.
- Benchmark capacity/latency and collect prospective isolated outcomes before executable rollout. More candidates or accurate market direction is not proof of net profit.

Deploy backward-compatible namespace/query isolation with the feature disabled first. Then collect/calculate observationally, run isolated decider candidates, and enable only reviewed additive publication after source/economic/operational checks. No stage arms or replaces the original workflow. Reuse immutable intake, deterministic Core sizing, execution and account safety rather than building another trading engine.

Regression anchors: Core [runner](../apps/api/tests/test_eod_runner.py), [planner](../apps/api/tests/test_next_open_execution_service.py), [handoff/restart](../apps/api/tests/test_exec_integration_service.py), [quantity refresh](../apps/api/tests/test_core_quantity_refresh.py). Exec: `tests/test_core_handoff_v4.py`, `tests/test_core_quantity.py`, `tests/test_core_quantity_residual.py`, `tests/test_monitoring_decision_integrity.py`, `tests/test_monitoring_family_isolation.py`, `tests/test_runtime_shutdown_fence.py`.

This revision supersedes the mandatory-selection design. Independent Core and Exec document reviews found no blockers in optional-task/error isolation, namespaced publication, baseline priority, residual sizing, exact ACK recovery and the explicit shared-account exposure limit. Implementation and regression/race tests remain required. No provider/model/order calls, runtime implementation, fault-injection tests or deployment were performed for this document.
