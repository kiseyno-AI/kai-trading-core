# Core-owned quantity planning

Core owns portfolio allocation and whole-share quantity decisions. Exec owns
order submission, fills, reservations, reconciliation and protective exits.
Neither quantity planning nor its two-hour refresh calls a decider or changes
the accepted trigger, chase ceiling, target or stop price.

## Quantity meaning

`target_quantity` is a cumulative BUY allowance for one immutable entry family,
including all historical BUY fills in that family. It is not an additional order
quantity or a target that authorizes selling existing inventory. Every refresh
preserves filled shares. A protective exit does not replenish the BUY allowance.
Signal-decider and short-term-decider remain independent candidates, sharing
account cash and aggregate symbol-exposure limits.

The shared deterministic calculator allocates NET available cash by confidence
and conservative net expected reward relative to stop/volatility risk. It caps
whole shares by loss budget, account symbol concentration and available source
liquidity ceilings. There is no fixed one-million-won allocation or minimum.
The default maximum loss budget is 1.5% of account equity and the symbol cap is
30%, matching the previous execution sizing limits. Core also reads Exec's
active account reservation state and configured absolute cash/exposure caps
before sizing, including when no current-session plan has been accepted.
Non-spread round-trip cost
uses `SIGNAL_DECIDER_NEXT_OPEN_NON_SPREAD_ROUND_TRIP_COST_BPS` (default 50 bps).
Missing optional volatility/liquidity information is audited; an explicit zero
liquidity ceiling permits no new BUY. Missing account evidence or mandatory
price geometry yields an observation-only quantity instead of inventing cash.

## Shared lifecycle

Both scheduled PyKRX and explicit/recovery daily paths converge on the existing
next-open planner and dispatch service. Quantity preparation happens at this
shared handoff, not in a second independently implemented daily pipeline.

Initial quantities are prepared for dispatch. Same-session preopen and opening
refreshes finalize current account evidence; during the regular session the
background runner checks the persisted last successful refresh and renews
quantities every 7,200 seconds. Closed, holiday, disabled and dry-run execution
states do not submit quantity revisions. Existing recovery/buy-authority gates
remain effective.

Dynamic sizing state lives in
`SignalDeciderExecutionPlanItem.submission_payload.core_quantity_sizing`.
It is deliberately separate from decision/price authority fingerprints. The
original `initial_policy` remains immutable for intake replay, while `policy`
tracks the last acknowledged revision. Pending requests are written before
network delivery and replayed exactly after an uncertain response or restart.
Diagnostic payload slimming preserves this evidence.

## Core to Exec interface

New BUY entries carry a `quantity_policy` with `type=core_sized`, integer
`target_quantity`, integer `revision`, aware `calculated_at`/`valid_until`, and
`sizing_inputs.price_contract_digest` plus bounded calculation evidence.
Legacy accepted `exec_agent_sized` plans keep their original contract during
rollout. Deploy the compatible Exec version before enabling Core-owned plans.

`POST /v4/execution-plans/quantities` accepts only quantity evidence, exact plan,
entry and snapshot identities, and expected/new revisions. It accepts no price
fields. Compare-and-swap, idempotent replay, per-item results and unchanged
price binding prevent stale or duplicate updates from becoming new orders.
The immutable `sizing_inputs.sizing_price` is the worst entry price used for
quantity calculations. It is also an execution ceiling when an immediate BUY
has no separate chase price; quantity revisions cannot raise this ceiling.
Exec subtracts lifetime fills and outstanding BUY reservations again at its
final order boundary. Unknown broker submission outcomes prohibit increases.
Existing pause, kill switch, market/hourly risk-off, cash, exposure, cost and
broker limits remain execution checks.

Inspect the existing Core plan response's `submission.core_quantity_sizing` for
calculation inputs, current policy, pending delivery, last ACK and errors. Exec's
existing v4 plan readback adds `item_states[].core_quantity_state` for target,
revision, filled, open and remaining quantities. The live readback is separate
from a local quantity proposal or an accepted HTTP request. For Core-sized rows,
`planned_quantity` is labeled `quantity_basis=cumulative_buy_allowance`, and
`core_allocation_budget_krw`/`estimated_notional_krw` use that policy's entry
ceiling. The stored route configuration's legacy `allocation_budget_krw` is not
the quantity authority.
