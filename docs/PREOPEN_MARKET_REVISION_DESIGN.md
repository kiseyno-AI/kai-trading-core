# Preopen market revision: collection, finalization and intraday admission

Date: 2026-11-24. Status: proposed design; not implemented, benchmarked or deployed. This operational specification supplements the [research and relative-shock model plan](research/us-krx-overnight-rebound-2026-11-24.md). Current source baselines: Core `a7fa8718c35d4630e50ec8610e4525d0510ff445`, Exec `b3e0254d619e4366a7200d0f76117ab2f646658b`.

## Intended result

Use the last completed US session to revise affected next-KRX-session decisions before entries become executable. Freeze what the models knew, preserve the previous EOD analysis, and keep short-term/signal decisions independent. Intraday execution consumes a selected, acknowledged immutable plan; it performs no new decider call at a price touch.

The guarantee to test is narrower than forecasting success: an unselected, late, shadow or duplicate revision must not create new BUY authority. A timely successful Exec activation commit is authoritative even if its ACK is lost; Core must report that uncertainty rather than infer nonexecution or issue another allowance. Unavailable optional market context must not fail a completed EOD process or erase an independently valid baseline decision. Hard execution safety and protective exits remain effective.

## Current seams and gaps

| Current behavior | Source | Required design change |
| --- | --- | --- |
| PREOPEN starts at midnight; delegated baseline dispatch can begin at 08:00. Plan refresh and write-ahead reconciliation can activate without waiting for an 08:40 revision. | [runner.py:31,56,869,891](../apps/api/app/eod/runner.py#L31), [exec_integration.py:5003,5181,5927](../apps/api/app/orders/exec_integration.py#L5003) | Persist a selection disposition before the existing dispatch window. Enforce it in shared authority and recovery paths, not just in one scheduler branch. |
| Core already validates source generations before/after activation, but Exec checks the generation stored in that plan rather than a finalized session selection. | [next_open_execution.py:3329](../apps/api/app/orders/next_open_execution.py#L3329), [exec_integration.py:3723](../apps/api/app/orders/exec_integration.py#L3723); Exec `core_handoff_v4.py:523` | Add durable session-selection admission in Exec so stale activation cannot exploit the cross-service interval. |
| Price replacement already stages revocation, preserves old authority until its exact ACK, then rebinds. Quantity refresh cannot change prices. | [next_open_execution.py:5094,10564,10646](../apps/api/app/orders/next_open_execution.py#L5094), [quantity contract](CORE_QUANTITY_SIZING.md) | Reuse revoke-before-rebind and existing immutable v4 intake. Do not mutate old prices through a quantity update. |
| Snapshot cutoff hardcodes 09:00; Core clock and Exec executable-quantity logic also hardcode regular hours, while sizing uses exchange-calendar bounds. | [snapshot validity:12,32](../apps/api/app/agent_context/signal_decider_snapshot_validity.py#L12), [clock.py:49](../apps/api/app/market/clock.py#L49); Exec `core_quantity.py:309` | Establish one persisted session interval and calendar version before claiming delayed-open/holiday support. |
| Durable dashboard market snapshots are read without provider refresh; their mutable latest cache is not an immutable decision input. | [snapshot.py:357,385,603](../apps/api/app/market/snapshot.py#L357) | Persist a separate frozen US-input snapshot. Reading status must not collect or train. |
| `core_sized` entries and protection already bypass trigger-time agents. Shutdown preserves durable plans. | Exec `trigger_time_exec_agent.py:245`, `main.py:357` | Keep the fast execution path. Restart does not finalize, revoke or authorize a revision. |

The Exec paths above are relative to `kai-trading-exec/apps/api/app/services/`, except `main.py`. These are source-backed observations, not a live account/configuration audit.

## Session identity and timing

`D` is the source KRX EOD session. `T` is the next open KRX session, not `D + 1 calendar day`. `O` is that session's canonical open. All persisted timestamps are aware UTC, displayed in `Asia/Seoul`. Select the expected completed US sessions with an American exchange calendar in `America/New_York`, including holidays and early closes.

For an ordinary 09:00 KRX opening, use this initial operating budget:

| KST time | Relative to O | Work and durable output |
| --- | --- | --- |
| Previous evening | Source D | Existing daily pipeline publishes summaries and both lane decisions. Store their immutable proof and prepared cohorts. Persist Core target-session selections and send the Exec session-policy arming request; verify its durable exact ACK before calling arming complete. |
| 06:45 | O-135m | Verify T/open/close/calendar version, exact Exec arming ACK/coverage, source publications, required activation/Core-sizing capability, history warmup and queue capacity. Missing history is not backfilled across the whole market in this window. |
| 07:00 | O-120m | First shared KIS collection for the validated required US indices. Fetch incremental dated bars, not one foreign request per Korean stock. |
| 07:05 and 07:10 | O-115m/O-110m | At most two retries, only for failed/stale required inputs. Reuse successful rows and bounded HTTP timeouts. No tight polling. |
| 07:12 | O-108m | Hard input cutoff. Validate and freeze one immutable input manifest, or record an explicit unavailable disposition. Never move this cutoff on retry. |
| By 07:15 | O-105m | Calculate relative shocks and validated market forecasts; determine exact affected `(symbol, lane)` scope and its hash. Persist before dispatching agents. |
| 07:15–08:30 | O-105m to O-30m | Bounded independent decider revisions; reuse source-valid summaries/factors/TimesFM. Persist candidate responses in isolated storage. |
| 08:30 | O-30m | Hard feedback acceptance deadline. Atomically close pending candidates to revised, retained-baseline or no-entry dispositions. No symbol waits for another lane's missing response. |
| 08:30–08:40 | O-30m to O-20m | Prepare validated families, reconcile old authority, calculate quantities through the shared account allocator, register, activate and verify exact Exec acknowledgements. |
| 08:40 | O-20m | Close first activation and new positive selection for this campaign. Core-unconfirmed delivery may already be Exec-active; reconcile the exact IDs, with no new allowance or baseline fallback. Denials/revocations and existing protective management remain possible. |
| 08:50 | O-10m | Readiness audit of selected identities, activation state, quantities, session bounds and quote subscriptions. No all-stage rerun and no new agent work. |
| From O to the original entry expiry | OPEN | Existing deterministic quote/market/risk/order checks. A price touch uses the selected plan and current evidence. |

These are proposed deadlines, not measured completion promises. Input and feedback acceptance are strictly before their respective deadlines; exactly-at-deadline arrivals are late. First activation checks server-authoritative wall time after all serialization locks are acquired and immediately before its authority-changing write; a pre-lock request timestamp is insufficient. Preserve the original server authorization/commit receipt for replay. For example: the US November 24 regular close occurs on November 25 at 06:00 KST in winter; collect on November 25 around 07:00 for the November 25 Korean session. The normal US close is 16:00 New York time; DST and early closes must come from the exchange calendar, not a fixed UTC offset. [NYSE calendar and hours](https://www.nyse.com/trade/hours-calendars).

If there is no KRX session, do not run this job. If no new US session has completed since source D, record `no_new_us_session`; do not call a holiday unchanged close a zero-return observation. During a multi-day Korean closure, record all newly completed US sessions and their cumulative move. If the US close or verified source availability cannot fit before the input cutoff, mark the new forecast unavailable; do not quietly slide later deadlines.

## Collection and calculation rules

Use the documented KIS overseas-index daily endpoint only after master-symbol and actual preopen receipt validation. The initial required/optional instruments and minimum histories belong to the validated feature manifest; optional semiconductor coverage cannot silently become a mandatory failure or an improvised model substitution. Cash-index and ETF inputs retain different identities.

Daily refresh has at most three attempts per required index: at 07:00, 07:05 and 07:10 in the normal schedule. With three required indices this is at most nine provider requests, excluding an independently tracked token renewal; initial multi-year history preparation is a separate bounded offline task. If the actual provider requires pagination, include it in the declared request budget instead of hiding extra requests. Repeated scheduler ticks and concurrent explicit requests must share this budget and durable single-flight job identity. A fallback is permitted only if already configured, independently validated and included in the same bounded cutoff/budget.

Validate selected dated rows rather than the provider's mutable headline quote:

- Correct instrument, currency and data convention; latest row equals the expected completed US regular session, with the correct prior session. Exclude unfinished current-session bars.
- Strictly positive finite prices; coherent OHLC where used. Provider zero/sentinel/null does not mean a real -100% move or a confirmed zero change.
- Retain provider session/observation time, `first_seen_at`, `fetched_at`, raw-response hash and availability basis. A fresh fetch does not make an old session fresh.
- Freeze the source-day evidence, new cutoff, selected US rows, model/feature version and calendar version. Later provider corrections are new observations, not edits to the input used by an accepted decision.

Compute daily return, percentage-point return acceleration, lag-only normalized shock, historical percentile, prior US/Korean selloff and sector/broad-market confirmation as specified in the research plan. Never use the event return in its reference-volatility window, mix units, divide by a negative/zero prior return, or turn a z-score into a probability. Zero volatility/insufficient support yields an unavailable feature or the explicitly validated reduced-feature model.

The immutable US forecast is valid for **T**, not subject to the same seconds-level expiry as live Korean quotes. Current price/spread/orderability and dynamic market evidence retain their existing shorter freshness requirements. Revalidating an old US snapshot's fetch timestamp during OPEN must not create a second decision generation.

## Durable state and ownership

Use the existing Core runner and shared orchestration/repository seams; add no second daemon or browser-dependent job. The scheduled and explicit targeted-recovery entry points invoke the same module. Persist these identities before work:

- Session job: `target_session`, source D/publication proof, expected US sessions, canonical O/close, policy/feature/model/calendar versions and deadlines.
- Input snapshot: immutable input hash and frozen cutoff, independent of the dashboard latest cache.
- Candidate revision: `(T, symbol, lane, job/input hash)`, original source evidence and later decision cutoff, role-specific status/result hash. Account secrets and monetary information are not public status fields.
- Selected authority: `(T, symbol, lane)` points to the exact accepted revision or retained baseline, with a monotonically ordered selection generation and disposition.
- Exec admission: `(T, account, route, symbol, lane)` binds that selection generation to exact plan/material/entry identities and execution validity. Account cash/exposure remains shared; two lanes do not acquire isolated wallets.

Selection generation is separate from the original immutable plan/source activation generation. A `baseline_retained` selection can explicitly admit the unchanged original material; never rewrite its `_core_activation` binding or material fingerprint just to match the new selection generation.

Use database uniqueness/CAS and leases with owner/fencing tokens. An expired worker cannot publish after a replacement owns the job. Retries reuse request IDs, cutoff, scope and input hash; changing `now` or `fetched_at` alone is not a new job. A worker may run twice, but publication and broker admission must be idempotent. Do not claim exactly-once network delivery.

Candidate storage, including late and shadow responses, must be excluded from every live selector. If selected candidates are published into existing decision-snapshot tables, selection plus official immutable publication occurs in one Core transaction; original EOD records remain unchanged. Current max-timestamp selection is insufficient. A late response cannot become authoritative merely because it is newer.

Use a small explicit per-family disposition set:

| Disposition | Meaning and allowed effect |
| --- | --- |
| `pending` | Review not finalized. New Core-owned BUY activation is held for this family under the enabled policy. Existing protection/readback continues. |
| `revised_selected` | A complete, timely new lane decision passed response/source checks. Only its exact eligible plan may be prepared for execution. REJECT/nonactionable results instead finalize `no_entry`. |
| `baseline_retained` | No material new event, no new US session, optional data unavailable, or a revision failed; original decision remains valid under its unchanged contract and all current mandatory safety checks. It is labeled retained, not newly assessed. |
| `no_entry` | No usable eligible original or revised decision, fresh mandatory risk failure, or an explicit new nonactionable verdict. Preserve a tombstone so empty results cannot revive old BUY authority. |

Publication state (`prepared`, `activation_pending`, `active`, `unconfirmed`, `closed`) is separate from model result. WAIT observation is also separate from executable BUY permission. No eligible candidates is a successful finalized selection with zero entries, not a pipeline failure.

At 08:30, CAS-resolve each still-pending candidate. Valid baseline retention is permitted only if that exact original decision is still independently valid; newly discovered hard risk, a superseding valid REJECT or retired authority cannot fall back to it. If no eligible baseline exists, the outcome is `no_entry`, not a manufactured WAIT. Do not lower scores or widen price/quantity limits to meet a deadline.

## Fix the 08:00 race in both services

This is required new behavior, not a description of today's v4 capability:

1. Enable the policy only for a future target session after compatible Core and Exec code/contracts are deployed and capability checks pass. Persist the same enabled account/route/session scope on Exec in advance: missing session records under that enabled scope deny new delegated BUY, including legacy plans. Do not enable it midway through an already-executing day. Shadow validation arms no live fence and cannot affect existing orders.
2. Extend the v4 activation control contract with explicit arming and finalization operations. Before 08:00, Core sends an idempotent arming manifest with T, account/route/lane enforcement scope, policy/calendar versions, canonical open/close/deadlines and source/cohort identity. Exec persists a deny-by-default session record and returns an exact durable ACK. Scope includes unclassified/missing families; it cannot wait for the affected subset discovered at 07:15. Arming is complete only when Core verifies ACK/readback coverage. Missing/mismatched arming evidence holds new entry for that scope, never affects protection, and does not invalidate EOD completion.
3. Before the first existing 08:00 dispatch, arm Core's durable selections as well. The shared authority inspector must check selection generation and disposition for registration, activation, quantity renewal, direct dispatch APIs and write-ahead/restart recovery. On Exec, require selection-bound Core activation and `core_sized` capability for this flow; `_core_activation` is optional today. An old client stripping the binding, or a missing family record, must not escape the persisted session policy. Legacy unbound entries require an explicit safe migration rather than an implicit bypass.
4. The typed finalization manifest carries expected prior selection generation, new generation, disposition, canonical validity, and exact selected decision/plan/entry/material bindings. Finalization can record `no_entry` without inventing an empty BUY plan. Keep the immutable source activation identity alongside the new selection identity. These contract operations are new and need producer/consumer documentation and compatibility tests; they are not available in today's v4 implementation.
5. Precreate pending family records. Selection finalization, no-entry tombstones, quantity renewal and final broker admission must acquire the **same** per-family selection serialization, in a documented canonical order with existing account/control/plan/order locks. CAS the selection and activate only its exact eligible binding. Record a higher-generation `no_entry` even when no replacement plan exists. A stale activation must fail despite matching the old plan's stored local generation. Exact replay never creates fresh allowance.
6. Perform the new generation check at Exec's immediate serialized broker boundary, not only an earlier monitoring check. Keep serialization through the admission decision and durable submission-pending reservation; no release/check/submit gap or intervening provider/LLM I/O is permitted. If old submission wins this ordering first, replacement must observe possible exposure and preserve protection/block replacement; it cannot retract an admitted call. Recheck wall time after lock waits. Existing immediate-boundary anchors are Exec `broker_orders.py:4217,4259,4292,4531`.
7. At dispatch claim and that final boundary, require the current finalized binding and valid session/entry window. Protective exits must not depend on a successful new BUY selection. The local finalized snapshot/locks provide this evidence; do not add an HTTP round trip at a price touch.

Keep finalization scoped per family so one failed symbol or lane cannot hold unrelated verified entries. Revision plans may use one complete linked family per atomic intake unit (entry plus its protective legs), with bounded local concurrency; current v4 intake is atomic per plan. Validate complete families before building each intake. Do not promise per-item recovery inside an atomic rejected batch, or omit an invalid member of a linked family.

Baseline retention also needs an explicit finalized generation; the old plan is not admitted solely because the new result list is empty. Quantities can renew only for the current selected family and never independently upgrade an old generation. This closes the existing 08:00 quantity-renewal route around the review.

## Publish through the existing replacement protocol

For a selected decision that changes prices/conditions, reuse the current lifecycle:

1. Persist the old family's exact identity and a write-ahead revocation request; freeze new entry authority for that family.
2. Revoke the old linked family in Exec and persist its exact acknowledgement. Until confirmed, do not overwrite prices, clear old dispatch proof or create an executable replacement.
3. If provably unsubmitted, prepare a new immutable plan from the selected decision. Recalculate quantities through the existing shared allocator using current account/reservation/exposure evidence. Two independent lanes still share cash and total-symbol caps.
4. Persist the exact registration request before network delivery. Register with activation pending, validate full material/identity ACK, then send the selection-bound activation. Verify the activation ACK and durable current readback before reporting active.
5. If already submitted, open, partially filled, filled or unknown exposure exists, follow the existing `manual_review_required`/reconciliation path. Do not treat a new plan ID as a fresh BUY allowance, reset filled quantities, or move protective prices automatically. Keep protective exits and broker-truth reconciliation alive; isolate the affected family.

Quantity revision is not price revision. An `updated` quantity ACK is not proof a broker order was cancelled; outstanding orders and uncertain submissions continue to count against capacity. Existing cumulative allowance, cash, symbol exposure, orderability, stop and circuit-breaker checks remain effective.

On an uncertain registration/activation response, reconcile the same durable request and exact IDs. Never generate a new idempotency key or fallback baseline to escape a timeout. Exec may already have committed the timely activation; Core reports `unconfirmed` until it has proof. At 08:40, stop **new positive selections and first activations for this campaign**, not safe denial/revocation and not ordinary quantity renewal of an already-selected family under its existing cumulative-allowance rules. After the cutoff, an exact replay may return the original timely receipt/time without any state change. If that receipt was subsequently superseded, it is historical evidence only; current readback determines active state. A request arriving before the cutoff but obtaining locks afterward cannot make its first activation. Existing entry/session/quantity validity and final admission checks remain effective.

## Intraday use

The forecast changes candidate selection and lane plans before OPEN. During the day:

1. Select only the finalized `(T, symbol, lane)` authority. At least one valid WAIT requests observation for that lane; REJECT/AVOID_CHASE never travels as an actionable monitoring entry. Both eligible lanes can independently trade the same symbol under their own plans.
2. On quote events, check current price versus that lane's trigger/chase ceiling and existing typed conditions; check quote freshness, live market/account/symbol risk, current quantity allowance, reservations and existing holdings.
3. Use the existing `core_sized` deterministic path, including protective orders. New numeric conditions need typed producer/consumer support; free-text `trigger_conditions` remains explanatory. A price touch never waits for another agent or for US data collection.
4. Current Korean conditions may pause/resume entries under existing execution policy. A positive US forecast cannot override a halt, stale mandatory input, adverse live gate or expired entry window. A market resume cannot reopen a superseded generation or replenish spent BUY allowance.
5. Do not reprice plans or rerun deciders whenever an index tick arrives. Intraday state updates are observations/vetoes of the accepted plan. A later model campaign would require a separately approved window and new version; it is outside this design.

The US snapshot's session validity and Korean live-market freshness are distinct. Do not label a five-minute-old index observation as a seconds-fresh condition. Reuse current provider pacing/cache cadence; any setup requiring tighter data needs its own proven source budget rather than unbounded polling. At entry expiry/session close, no new BUY authority survives merely because the process restarts. Protective management retains its own contract.

## Failure and recovery matrix

| Event | Required result |
| --- | --- |
| US holiday/no new completed session | `no_new_us_session`; no invented zero. Retain an independently valid baseline under explicit finalization. |
| KIS timeout, older session, malformed/zero prices | Retry only failed inputs within the three-attempt budget; after 07:12 mark unavailable. Retain a valid baseline or select no-entry; EOD remains complete. |
| Optional index missing | Use only the previously validated reduced-feature model, otherwise unavailable. Do not silently change trained inputs. |
| Source summaries/cohort proof absent | No broad EOD repair. Report exact missing prerequisites; no new verdict or allowance for that scope. |
| One symbol/lane fails or times out | Other timely verified families finalize normally. Resolve only that family's baseline/no-entry fallback at 08:30. |
| REJECT arrives successfully | Finalize no-entry and retire any eligible old unfilled authority. An empty new plan must not expose the old plan. |
| Feedback arrives at/after 08:30 | Store late/candidate-only; no active publication. This deadline is strictly exclusive. |
| Worker crashes after persistence | Resume exact missing phases/role items using saved cutoff/scope/IDs. Lease fencing rejects stale-worker writes. |
| Register/activate ACK lost | Exec may be committed-active; Core does not infer nonexecution. Read back and replay exact request only, without a second plan/order or baseline fallback. Post-cutoff replay is receipt-only; inspect current state for supersession. |
| Activation from the old generation races selection | Exec CAS/final broker admission rejects the stale binding. Core post-ACK inspection alone is insufficient. |
| Old family has possible exposure or fills during replacement | Preserve protection, prohibit replacement entry, reconcile broker truth. No blind cancellation assumption or allowance reset. |
| Two lanes buy the same symbol | Separate family IDs/decisions; shared allocator and final reservations enforce aggregate cash/symbol limits. One lane's reject does not veto the other. |
| Restart after 08:40 or after OPEN | Reconcile selected durable authority only; no late positive selection/first activation, revived EOD fallback or shifted cutoff. Risk-reducing denial and protection remain permitted. |
| Special KRX hours/calendar disagreement | Do not fabricate 09:00. Pause new authority for the affected session until the canonical interval is verified; preserve existing protection. |

Keep EOD, forecast, revision and execution-publication statuses separate. A provider error is visible as an error; an optional no-data result is not a failed whole-day pipeline. Empty eligible scope is finalized successfully with zero entries. HTTP success/counts alone do not prove completion.

## Required verification before enabling live integration

- Fake-clock both ordinary and delayed KRX sessions, US DST changes/early closes/holidays, KRX multi-day closures and the exact 07:12/08:30/08:40/open boundaries. Exercise both services' validity checks using the same canonical interval.
- Prove immutable input/cutoff/scope reuse, lag-only features and no changed summary/TimesFM fingerprints. Status GETs and UI refreshes must make zero provider/agent calls.
- Start two schedulers plus explicit recovery simultaneously; assert one logical job/request budget, per-role unique publication, no stale-worker writes and no extra BUY allowance.
- Persist a high-score shadow/late result then run ordinary planner, monitoring, quantity refresh and restart reconciliation; none may consume it. Assert generation tombstones prevent fallback after new REJECT.
- Fault-inject before/after Core persistence, registration, revocation ACK, selection CAS, activation commit and ACK delivery. Prove both no-duplicate exposure and no permanently pending state without visible exact debt.
- Race old activation, quantity renewal and order claim with replacement; include unknown broker response, an old order filling during revocation, a quantity reduction still awaiting broker cancellation and same-symbol dual-lane entries. Protective exits must remain available.
- Verify capability mismatch/missing activation binding and policy rollout before/after the first 08:00 dispatch. Do not enable the policy midway through an active session.
- Drop the Exec arming request/ACK, mismatch its scope/version, or omit a family: assert persisted enabled-scope default denial before any 08:00 old-format activation/renewal. Drop an activation ACK after timely Exec commit: assert Core uncertainty, correct remote activity, receipt-only post-cutoff replay and no fallback/new allowance. Test a request waiting across the deadline and a later risk-reducing tombstone.
- Measure largest supported cohort: shared data request count, collection latency, decider queue/runtime p50/p95, percentage finalized before deadlines, publication ACK latency and price-touch overhead. The 75-minute decider budget must be demonstrated; if it does not fit, redesign earlier collection/conditional plans rather than silently dropping candidates or extending into OPEN.
- Complete point-in-time historical evaluation plus prospective isolated results per the research plan. More candidates or a correct rebound forecast is not sufficient evidence of net trading benefit.

Regression anchors: Core [runner](../apps/api/tests/test_eod_runner.py), [snapshot validity](../apps/api/tests/test_signal_decider_snapshot_validity.py), [market session](../apps/api/tests/test_market_session.py), [planner](../apps/api/tests/test_next_open_execution_service.py), [v4 handoff/restart](../apps/api/tests/test_exec_integration_service.py), [quantity refresh](../apps/api/tests/test_core_quantity_refresh.py). Verified Exec anchors: `tests/test_core_handoff_v4.py`, `tests/test_core_handoff_v4_material_integrity.py`, `tests/test_core_quantity.py`, `tests/test_core_quantity_residual.py`, `tests/test_core_quantity_reductions.py`, `tests/test_monitoring_decision_integrity.py`, `tests/test_monitoring_family_isolation.py`, `tests/test_runtime_shutdown_fence.py`, `tests/test_app_lifespan_shutdown.py`.

## Delivery order and evidence

1. First validate KIS symbol coverage/preopen availability and one canonical Core/Exec session-time contract. Implement pure relative-shock inference and durable isolated snapshots/jobs. No execution effect.
2. Run targeted preopen decisions observationally, measure deadlines and exact scope, and collect prospective outcomes. Observation mode must not arm live fences or publish ordinary live snapshots.
3. Implement and test session-selection admission in both services, mandatory activation capability, existing revoke-before-rebind reuse and exact recovery/readback. Update Core and Exec API references/contracts for the typed activation extension. Deploy reviewed compatible remote commits before enabling the policy for a future target session.
4. Enable only after economic and operational gates pass. Record target session, source/input/model/contract/calendar/commit versions, selected/retained/no-entry counts, exact publication ACK coverage and remaining debt. Read-only operator status should show these phases and timestamps rather than claiming completion when a model merely finished.

Independent Core scheduling and Exec contract reviews accepted this design after correcting the pre-08:00 arming gap, lost-ACK semantics, immutable versus selection generations, shared final-admission serialization, deadline lock waits and ordinary quantity-renewal scope. The final targeted reviews reported no further blockers in those corrections. That is document review, not proof that the proposed protocol exists or passes integration tests.

No provider request, model run, trading change, deployment or fault-injection test was performed for this document. Availability, latency, contract compatibility and new admission behavior remain implementation/validation work.
