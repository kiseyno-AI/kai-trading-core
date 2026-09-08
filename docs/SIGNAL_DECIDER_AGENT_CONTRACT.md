# Signal Decider Agent Contract

Contract version: `agent_role_contract_v1`

Canonical runtime source: `apps/api/app/agent_context/response_contract.py`. The dispatch path records the canonical contract hash together with the exact prompt hash, prompt length, and model so a stored decision can be traced to the contract that produced it.

## Purpose

`signal-decider` combines the bounded EOD, technical, fundamental, market-regime, liquidity, data-quality, and prior-decision context supplied by Core. It provides conservative next-session decision support. It does not place orders, size positions, or bypass Core/Exec risk controls.

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

## Data-quality and decision rules

- Operator-facing prose is Korean; machine-readable enum values remain the exact English tokens above.
- Core execution inputs such as quote freshness, liquidity, slippage, and hard blocks take precedence over auxiliary news or detailed financial gaps.
- Missing auxiliary context reduces confidence by default and must not become a standalone reason to reject an otherwise supported setup.
- Unsupported prices remain `null`; the model must not invent precision.
- A batch response contains exactly one item per assigned symbol. Prompt-budget compression must preserve every assigned symbol and the authoritative item count or fail before dispatch.
