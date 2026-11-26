# Independent US-rebound execution preparation

The `overnight_rebound` execution modules prepare an independent rule origin.
They do not write either decider's snapshots/plans, run agents or daily stages,
collect data, or call a broker. The existing immutable evaluation remains a
`WATCH` with `order_authority: false`; it is not calibrated probability or an
additional decider. Runner activation is a separate integration step.

## Required policy and evidence

`RuleExecutionPolicy` is frozen and has no executable parameter defaults. Every
gap, stop/target, cost/slippage, volatility multiplier, risk/notional/liquidity
limit, evidence TTL, entry expiry and close buffer is explicit. `paper_only`
permits paper preparation only. Live preparation requires `validated_live`,
explicit validation evidence identity/hash, and real inputs; runtime handoff
also needs a separate exact `RuleExecutionAuthority`. Test literals are worked
examples, not production policy values.

`build_rule_execution_setup` accepts an intact evaluation hash, verified prior
KRX close/volatility/liquidity evidence, and an authoritative session. Missing,
stale, future, misbound, special-hours or expired evidence cannot create a
setup. Only unleveraged ETFs 069500 (KODEX 200) and 091160 (KODEX Semiconductors)
are admitted. Prices obey 1 KRW ticks below 2,000 and 5 KRW ticks otherwise,
with whole-share lots. Chase rounds down; exhausted geometry or nonpositive
cost/slippage-adjusted edge is unavailable. The same aware close exit, equal to
authoritative close minus the configured buffer, binds the complete family.

`RuleAccountEvidence` is a private group of scoped reads, not an atomic broker
snapshot. It requires account/provider/mode/symbol, the exact chase price,
the evidence read's execution destination and configured route fingerprint,
`read_group_id`, request/receipt timestamps, fresh component timestamps, full
exposure/reservation coverage, and the original two lanes' complete entitlement.
Do not derive completeness from truncated portfolio lists.

Cash capacity is `min(ord_psbl_cash, nrcvb_buy_amt)`, bounded by `nrcvb_buy_qty`
from the exact no-margin KIS order-possible response. These are conditional
broker ceilings, not settled cash or proof that all pending reservations were
deducted. Never substitute `max_*`, total assets, `thdt_buy_amt`, or a generic
parser fallback. Deduct a proven conservative
`unproven_broker_pending_reserve_krw` where reservation coverage is unclear;
unknown coverage/limits produce zero quantity. Parent adapters must validate
the raw response, scope, request price and timestamps before asserting proof.

The grant is the minimum of residual cash after baseline entitlement and other
rule commitments, buying power, broker cash-only quantity, account/trade/symbol/
strategy/liquidity ceilings, and per-share loss. Loss uses the larger of stop
distance and volatility distance, plus round-trip cost/slippage. Aggregate risk
cannot exceed 1.5% of equity or a stricter proven account limit. Potential entry,
open order, holding or ambiguous exposure from either original lane skips only
this rule on that symbol. Shadow preparation never reserves baseline capital.

## Wire identity and authority

The separate strict rule DTO retains existing v4 schema/contract/capability and
the existing dispatch fingerprint algorithm. It adds `origin_type: rule`, null
`strategy_run_id`, and typed `rule_origin.v1`. Metadata agent names and every
item's agent names/snapshot are null. One plan contains one Core-sized BUY and
exactly one linked target/stop. ETFs use the KIS/KRX/KRW `stock` envelope and
`risk_context.instrument_subtype: etf`; this does not claim ordinary common
stock identity. Every item repeats exact origin, session and close deadline.

The origin's config hash binds evaluator configuration and execution policy;
its input hash binds the immutable evaluation, feature ID, and market/session
evidence. `setup_id` hashes account + target session + symbol + fixed setup slot,
excluding version/run/evaluation, so an update cannot create a second entry.

The rule-only price digest is SHA-256 over compact sorted JSON of
`entry_limit_price`, `entry_trigger_price`, `target_price`, `stop_price` as
canonical fixed decimal strings (nullable only for entry prices), and
`close_exit_at` converted to UTC `datetime.isoformat()` with `+00:00`. The
independent cross-repository fixture 10000/10000/10500/9800 and
`2026-11-27T15:20:00+09:00` yields
`295d5aa22d7dcc2102e8224094d8e273e953e04e43b73180f2f289b08f21a94f`.

Preopen Core-confirmed activation enrolls a validated watch entitlement. Its
authority-generation fingerprint must not pretend that a preopen market BUY
gate is open. Actual BUYs still require Exec's existing fresh OPEN market,
account, quote, pause/kill, quantity and submission gates.

## Private outbox and integration

```python
prepared = build_rule_execution_setup(
    policy=explicit_policy, evaluation=sealed_evaluation, market=verified_market,
    session=authoritative_session, account_reference=private_account,
    run_id=actual_run_id, now=aware_now,
)
if prepared.setup is not None:
    outbox = RuleExecutionOutbox(private_state_dir, destination_base_url=explicit_exec_origin)
    status = outbox.prepare(
        setup=prepared.setup, policy=explicit_policy, account=complete_private_evidence,
        authority_generation_fingerprint=watch_authority_fingerprint, clock=clock,
    )
    status = await outbox.advance(
        setup=prepared.setup, authority=explicit_authority_or_none,
        client=injected_authenticated_httpx_client, clock=clock,
    )
```

`prepare` locks one account/session ledger across both symbols, adds every local
prepared/pending/unknown/filled grant at its full cost/risk to the shared rule
budget, then sizes and persists immutable bytes. This deliberately retains the
same-session cumulative spend even after fills/exits. Outside commitments must
be complete in the supplied evidence. Local claims already represented there
can be counted twice conservatively; never erase an uncertain commitment to
gain capacity. No local reservation is written into the original sizing pool.

The persistent `rule-execution/` namespace uses private directories, mode-600
checkpoints, hashes, stable sidecar locks, atomic replacement, and leaf/ancestor
directory fsync. Retain it through deploy/restart. Do not delete a spent claim,
change its destination, or clear unknown debt to retry another evaluation.

The injected client must match the immutable destination. Before any POST,
`GET /v4/execution-plans/capabilities` must prove
`rule_origin_krx_us_rebound_v1`, exact mode/provider/account matching through the
private `X-Core-Account-Reference` header, and a stable configured route
fingerprint matching the persisted account-evidence binding. The first RPC
cannot rebind a grant to another destination or route. A paper plan cannot
reach a live runtime. Redirects and legacy
fallback are disabled. Registration does not activate a disabled rule.

Pending debt is durable before create/activation RPC. Lost responses reconcile
the exact plan ID, full origin, fingerprint, prices/deadline, durable item IDs,
route, the complete original quantity grant (including expiry, calculation time
and sizing/cost inputs), and persisted entry activation state. An echoed digest
cannot substitute for checking the returned material. Only reviewed server
normalization is accepted. Unknown
responses remain debt. Exact retries use identical bytes/IDs. Extra generated
close-timer state does not change the original three intake bindings. Existing
fills, open quantities, entry closure or submission uncertainty block new
activation; this module never changes positions or revisions.

The initial grant retains the earliest component evidence expiry. Once that TTL
expires it can reconcile, but cannot create or activate new authority by
pretending the original reads are fresh. There is no implicit grant refresh.

New setup/authority stops strictly before 08:40 KST, with clock checks after
lock/network work and before each POST. Reconciliation remains available with
missing/expired authority. Public status contains only status/reason/opaque
claim ID, never account, price, quantity or raw responses. Production deployment,
paper broker setup and live enablement are separate authorized operations.

## Read-only operational evidence

`RuleEvidenceAdapter` accepts explicitly configured KIS and authenticated Exec
read clients. `resolve_rule_session(day)` returns the existing authoritative
XKRX session, prior session and versioned calendar source; closed/special-hours
sessions are unavailable. `await adapter.market(symbol=..., session=...,
mode=..., clock=..., max_pages=...)` reads the whitelist ETF directly through
the existing KIS daily chart wrapper. It requires exactly 15 contiguous
completed KRX bars ending on the prior session, positive consistent OHLC,
volume/turnover, and usable ATR from the existing pure technical calculator.
The receipt time is distinct from the bar's trading date. No stock-universe or
stored-bar mutation occurs.

`await adapter.account(setup=..., policy=..., clock=..., max_pages=...)` returns
optional `RuleAccountEvidence`. Each batch has a soft deadline of at most 90
seconds and an explicit limit of 1–10 pages per inquiry. A read-only proxy reuses
the reviewed KIS wrapper/parser methods and passes their existing deadline guard
through every HTTP page, including checks after token/rate-limit waits. It does
not modify shared clients. Each new request and receipt checks the target date,
08:40 cutoff, monotonic budget and clock direction. Full component TTL checks
also apply before any usable result.

The adapter derives proof from raw fields rather than accepting flags from
configuration. It validates one exact configured account route using the
existing account-target parser, without treating its per-item `@budget` as an
account entitlement cap. Complete raw balance pages and the existing full
positions parser are both required. Equity comes only from the validated
balance `tot_evlu_amt`; total-assets cash fallback and today's purchase totals
cannot establish spendable cash. Cash-only order capacity uses strict raw
`ord_psbl_cash`, `nrcvb_buy_amt`, `nrcvb_buy_qty` and a positive
`psbl_qty_calc_unpr` from an inquiry bound to the exact limit price with
CMA/overseas disabled. The provider's quantity calculation basis may differ;
the cash ceiling is independently recomputed at the immutable worst price
plus costs, then capped by the provider's no-misu quantity.

Both complete BUY cancelable and account-wide daily-order inquiries must
reconcile. The daily request explicitly preserves `EXCG_ID_DVSN_CD=ALL` on
every page, as documented by the [official KIS daily-order wrapper](https://raw.githubusercontent.com/koreainvestment/open-trading-api/main/examples_llm/domestic_stock/inquire_daily_ccld/inquire_daily_ccld.py).
Order aggregate identities include account, session, exchange-ID, available
broker-branch fields and order number. Identical duplicates deduplicate;
contradictory/ambiguous identities, missing positive-order identity, unresolved
market-price orders, cancellation/remaining contradictions, or any truncated
page fail closed. The official mixed-case `excg_id_dvsn_Cd` spelling is
case-normalized; no numeric exchange-division to exchange-ID mapping is
invented. All outstanding BUY notional is conservatively reserved where KIS
cash-field reservation composition is unproven.

Exec's fresh capability handshake binds exact mode/provider/account/opaque
route before and after the group. The adapter also reads complete paginated
v4 plan state and fresh runtime quantity limits/uncertainty. Changed bindings,
unknown submissions, stale components or policy limits exceeding the existing
account ceilings are unavailable. Native `KISClient` still reads shared global
credentials, so native paper clients are explicitly unavailable; setting
`use_mock=True` alone cannot provision a dedicated paper account. No real-client
fallback or credential copying occurs.

For original-lane entitlement, the default reader queries all current
account/session rows and existing stored publication/current-snapshot evidence.
It only proves zero when a nonempty completed two-lane cohort has validated
linked signal/plan completion proofs, exact source/target dates, a zero-item
nonblocking publication, matching current snapshot/run identities, no blocked
scope, no actionable effective decision, no current Core rows and no unresolved
remote original BUY authority. Trigger-valid WAIT remains actionable.

Remote item states must cover every acknowledged intake item with exact unique
IDs and explicit sides; every BUY needs complete quantity proof. Only proven
SELLs may be omitted from the BUY-authority check. A mixed known/unknown BUY
book retains the full cap.

Local book/publication reads must remain identical at the close of the group. The
read path invokes no planner refresh, agent, daily stage, dispatch or preflight.

Empty rows, a wholly empty admitted cohort, legacy/unknown authority or a
nonempty original book do not prove zero. The current adapter conservatively
reserves the freshly bound Exec whole-account maximum as the original
entitlement upper bound, reports
`original_entitlement_full_shared_cap_bound`, and can return honest
`no_capacity`. This path does not claim a funded strategy. More selective
residual allocation for a nonempty original book requires a separately reviewed
complete entitlement projection; no budget is guessed here. Held positions and
open BUY risk are conservatively bounded at full notional without assuming
unproven stops.

`EvidenceResult.value` and `private_proof` are private. The latter retains the
read-group hash material and actual component request/receipt times/content
hashes for private persistence; it is not an atomic broker snapshot.
`public_status()` exposes only status, reason codes and an optional safe error
type. Normal zero capacity, missing/partial provider evidence and malformed or
internal failures remain distinct, without account numbers or amounts in
diagnostics. The adapter has only been verified with mocked provider/Exec reads;
actual operation remains a separate authorized step.
