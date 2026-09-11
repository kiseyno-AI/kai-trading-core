# Signal Decider Agent Contract

Contract version: `agent_role_contract_v1`

Canonical runtime source: `apps/api/app/agent_context/response_contract.py`. The dispatch path records the canonical contract hash together with the exact prompt hash, prompt length, and model so a stored decision can be traced to the contract that produced it.

## Purpose

`signal-decider` combines the bounded EOD, technical, fundamental, market-regime, liquidity, data-quality, and prior-decision context supplied by Core. It provides conservative next-session decision support. It does not place orders, size positions, or bypass Core/Exec risk controls.

## Runtime model identity

Core requests `OPENCLAW_SIGNAL_DECIDER_EXPECTED_MODEL` (default
`openai/gpt-5.6-sol`) through OpenClaw's exact model override. That request is
not itself proof of the runtime model. The response `model` value such as
`openclaw/signal-decider` is only a route alias and is never recorded as the
actual backend model. After completion, Core independently reads the exact
session registry entry, binds its terminal status and timestamps to the current
dispatch attempt, and verifies the observed provider/model against the expected
model. Decision provenance keeps requested, observed actual, status, reason,
and verification basis separate from the route alias.

`actual_observed_from_response=false` is intentional: actual identity comes
from the post-dispatch session observation, never from the response route alias.
Core also hashes the canonical runtime contract and the live workspace
`AGENTS.md`; missing, unsafe, unreadable, or mismatched live contracts fail the
same actionability fence. Core does not install or repair runtime files during
dispatch. Every HTTP attempt records separate immediately-before and
immediately-after hashes. Both observations and their version/hash identity
must be verified and identical; drift during an attempt or between retries is
non-actionable and a failed-response retry is stopped.

A missing, unverified, or mismatched backend identity makes the response
non-actionable. Core preserves the raw response and audit provenance, coerces
the normalized decision to `reject`, and records a retryable failure instead of
successful feedback. Runtime auto-discovery is diagnostic only and cannot grant
model authority. Evaluation, replay, and calibration outputs carry or select a
single actual-model cohort so observations from different models are not mixed.
Legacy rows without verified identity remain available for diagnostics, but are
excluded from profitability aggregation and policy-promotion evidence.

## Allowed actions

`preferred_action` and `next_day_open_action` must both use exactly one of `buy`, `wait`, `reject`.

- `buy` requires supportable entry, target, stop, execution quality, and invalidation evidence.
- `wait` requires a concrete normalized `wait_reason` and an upgrade trigger when prior decision context exists.
- `reject` marks a broken, unsuitable, or structurally invalid setup.

## Identity and attribution

Every feedback object must include the assigned `role`, `agent_name`, exact `symbol` including leading zeroes, exact `market_date`, a `traceability` object, and `traceability.symbol`. `feedback.symbol`, `traceability.symbol`, and the enclosing `item.symbol` must agree.

Any role/symbol/market_date mismatch, missing response symbol, traceability.symbol mismatch, or missing role, agent_name, market_date, traceability object, or traceability.symbol is hard-failed and remains non-actionable. Core preserves reported identity values in traceability, coerces the normalized action to `reject`, records a retryable failure, and never persists the invalid response as successful feedback. Core never infers required response identity from request context, including for a one-symbol batch.

## Schema integrity

Every declared contract key is required, even when its value is `null` or an empty list. Prices, returns, confidence, and score fields must be numbers or `null`; list fields must be arrays of strings; `decision_delta`, `traceability`, and `details` must be objects. Both action aliases must be present and equal. A missing key, invalid type, contradictory action alias, or malformed wait reason hard-fails the response before it can be persisted, counted as completed, or supplied to another agent.

For `buy`, target, expected return, stop, confidence and conviction scores must be positive, target must exceed stop, holding period must be non-empty, and supporting evidence plus invalidation conditions must be non-empty. For `wait`, numeric fields may be `null`, but `wait_reason` must contain a supported `code` and a concrete non-empty `summary`. For `reject`, numeric fields may be `null` and `wait_reason` must be `null`.

## Buy economics

An actionable `buy` must prove a positive cost-adjusted edge; the agent-reported `expected_return_pct` is not execution authority. Core derives target upside from the entry reference, subtracts the admitted cost amount from reward, adds it to loss, and gates on the lower of reported and derived return plus the adjusted reward/risk ratio. Fields named `slippage_adjusted` cover quote-spread evidence only and must not be described as full net return.

Core accepts an explicit finite, positive `expected_slippage_bps` only with the exact, deterministically produced provenance token `quote_spread_bps`. The observation must be no older than `KIS_REALTIME_PRICE_STALE_AFTER_SECONDS` (default `30`), timezone-aware, same-market-date, and not future-dated. Quote provenance requires exactly `{current_quote_cache, quote_snapshots}`; intraday provenance requires exactly `{current_kis_intraday_snapshot_cache, kis_intraday_snapshots}`. Extra or duplicate table names are invalid. Current price, bid, ask, spread points, and spread bps must all be attributed to the same selected context; Core does not combine quote and intraday components. Prices and spread must be positive, ask must exceed bid, and both spread calculations must agree. A missing value, fake zero spread, crossed/incoherent book, unknown/non-canonical or mixed provenance, stale or EOD quote, date mismatch, negative/non-finite value, or unsupported numeric range makes buy economics unproven. A liquidity-tier label alone is never cost evidence.

Core-local execution additionally applies the server setting `SIGNAL_DECIDER_NEXT_OPEN_NON_SPREAD_ROUND_TRIP_COST_BPS` (default `50`) as a conservative fees/taxes/non-spread round-trip allowance; agents and candidate payloads cannot override it. The allowance is additive and never substitutes for executable spread evidence. The current Core-local signal-decider next-open path has no execution-time quote recheck, so it remains fail-closed rather than authorizing a next-open buy from a source-day quote. Exec dispatch remains the supported live path and performs the final trigger-time full-cost check.

Only Exec dispatch may defer the precise cost when the input is the canonical completed-EOD shape (`expected_slippage_bps=null`, basis `unavailable`, and the versioned closed-market phase contract). The prior close may remain the recommendation/reference price, but even a numeric EOD spread is normalized away as non-executable. Core records `pending_live_cost_confirmation` and uses gross target/reference geometry only as a pre-screen. Unknown, malformed, and non-finite cases remain blocked. Exec must obtain trigger-time cost evidence and apply its final full-cost check before any buy.

Delegation metadata is authoritative only at the buy item's top-level `risk_context`: legacy `delegated_gate_codes` / `delegated_gate_confirmation_required` plus structured `delegated_gates`. The codes and structured entries must have the same exact order and cardinality. Every structured entry has exactly nine keys: `code`, `observed_value`, `field_path`, `comparator`, `threshold`, `policy_version`, `observed_at`, `recheck_source`, and `expires_at`. Observations and expiry are timezone-aware, not future-dated/expired at evaluation, and bound to an allowlisted code-specific path/comparator/threshold/policy/recheck source. Missing or malformed metadata fails before Core sends an Exec POST. It is not duplicated under handoff traceability or protective sell items, and compact payloads preserve every key, including an explicitly present `observed_value=null` for pending live cost. Only trigger-time liquidity (`>= 3,000,000,000 KRW`) and reward/risk (`>= 1.80`) may move from a short-term watch block to Exec; quality score, ATR/chase, trend, and relative-strength blocks remain Core hard blocks.

Non-terminal pre-open rows are re-evaluated on every scheduler tick against current KRX-scoped snapshots, corrected daily bars, enrichment, policy state, account routing, and finite validated confidence/expected-return thresholds. The deterministic `next_open_preopen_policy_v1` fingerprint remains audit evidence for the integration mode, cost-evidence version, non-spread allowance, confidence/expected-return/reward-risk thresholds, and exact account label/reference/budget; it is not authority to reuse a non-terminal eligibility decision. Missing, US, unknown, or conflicting market claims fail closed. Terminal dispatched/submitted rows are retained as immutable history.

## Core-to-Exec authority contract

Delegated plans use only `POST /v4/execution-plans` with the exact v4 schema and `delegated_final_full_cost_gate_recheck_v1` capability. The legacy endpoint is never a fallback. A plan is atomic: every buy has exactly one linked target sell and stop sell, and Core records success only after an exact accepted contract acknowledgement maps every submitted `core_item_key` one-to-one to durable Exec plan and item IDs. Missing, partial, ambiguous, or mismatched acknowledgements do not grant authority.

Decision evidence remains immutable across revisions. A newer WAIT/reject, changed BUY, removed candidate or account route, changed policy fingerprint, or explicit dispatch-to-observe/disabled control transition first stages a stable revocation observation. Exec fences the old buy lease and returns an exact per-item revocation acknowledgement; only then does Core mark the old authority revoked or dispatch a replacement. A possible broker exposure moves the entry to manual review while preserving protective target/stop exits. Recovery and duplicate handling must prove the persisted v4 contract rather than infer acceptance from a legacy status.

Snapshot selection is cohort-aware: the newest feedback run owns its requested symbol/role scope, and a requested symbol missing or failed in that run cannot fall back to an older BUY. Narrower unrelated runs do not erase other scopes. Signal and short-term snapshots are ranked before actionability classification, so a newer WAIT is always observed.

Short-term quality authority is defined by the separate [Short-Term Decider Agent Contract](./SHORT_TERM_DECIDER_AGENT_CONTRACT.md). Signal-decider cross-checks may constrain a short-term candidate, but they do not replace that contract's server-built objective evidence or restore a failed setup.

## Data-quality and decision rules

- Operator-facing prose is Korean; machine-readable enum values remain the exact English tokens above.
- Core execution inputs such as quote freshness, liquidity, slippage, and hard blocks take precedence over auxiliary news or detailed financial gaps.
- Missing auxiliary context reduces confidence by default and must not become a standalone reason to reject an otherwise supported setup.
- Unsupported prices remain `null`; the model must not invent precision.
- A batch response contains exactly one item per assigned symbol. Prompt-budget compression must preserve every assigned symbol and the authoritative item count or fail before dispatch.

## Calibration candidates

Calibration emits the canonical predicate root
`{"schema_version":"signal_decider_rule_predicate_v1","all":[...]}` (or the
exclusive `any` branch). Negative candidates use `buy_veto` or
`tighten_buy_gate`; missed-opportunity observations remain `prompt_warning`.
Generated candidates are manual-only, inactive, and deterministically
ineligible by default. A candidate can affect the performance gate only when
its canonical type and predicate validate and all four persisted approvals are
present: `activation_status=active`, `active_policy=true`,
`deterministic_gate_eligible=true`, and `manual_review_only=false`. Legacy
`veto`/`downsize` aliases may be normalized for display but never gain authority
through normalization.

Predicate evaluation reconstructs only server-owned, persisted request
features: decision geometry, action-summary and close-location buckets,
liquidity state, and unified-value, momentum, quality, or signal-feature
buckets. Model-authored response prose and response-only features cannot match
a calibration rule. Malformed roots, mixed `all`/`any` branches, empty clauses,
unknown candidate types, or partial approval state fail closed.
