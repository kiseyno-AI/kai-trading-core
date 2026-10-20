# Short-Term Decider Agent Contract

Contract version: `agent_role_contract_v1`

Canonical runtime source: `apps/api/app/agent_context/response_contract.py`. This document explains the contract; the runtime prompt suffix and its SHA-256 provenance are generated from that canonical source.

`short-term-decider` is a tactical, decision-support-only agent for short-term momentum candidates. It is separate from the conservative swing-oriented `signal-decider`.

## Purpose

Evaluate whether a symbol has a supportable 1-3 trading day tactical setup using only supplied payload data:

- specialist summaries: EOD, technical, fundamental
- symbol-day context and score context
- market context: index state, FX, foreign/institutional flow, breadth, stale/missing flags
- price action, volume, liquidity, trend, support/resistance, disclosures, and data-quality warnings

The agent must not produce execution instructions, position sizing, leverage guidance, or brokerage/order logic.

## Runtime model identity

Core requests `OPENCLAW_SHORT_TERM_DECIDER_EXPECTED_MODEL` (default
`openai/gpt-6.1-sol`) through OpenClaw's exact model override, but does not
treat the outbound request as observed fact. The response `model` value such as
`openclaw/short-term-decider` is a route alias. Core verifies the terminal,
timestamp-bound session registry entry after dispatch and records its observed
provider/model separately from expected/requested identity and the route alias.
`actual_observed_from_response=false` is explicit because the response alias is
not the source; `post_dispatch_session_observation` is.

Reasoning effort remains an OpenClaw agent-runtime setting rather than a Core
request contract. OpenClaw's `/v1/responses` compatibility endpoint currently
accepts but ignores `reasoning`, so Core does not claim to enforce it. Configure
the agent's `thinkingDefault` in OpenClaw; `medium` is the recommended starting
default for this decider.

The runtime contract provenance similarly separates the canonical template
hash from the live workspace `AGENTS.md` hash. Missing, symlinked/non-regular,
unreadable, or mismatched live contracts fail closed, and dispatch never
auto-installs a replacement. Core captures and persists live proof immediately
before and after every HTTP attempt. Actionability requires identical verified
version/hash identity across both observations; drift during an attempt or
between retries stops failed-response retries and remains non-actionable.

Missing, unverified, or mismatched backend identity is fail-closed: Core keeps
the raw response for audit, changes the normalized tactical decision to
`reject`, and does not persist it as successful actionable feedback. Automatic
model discovery remains diagnostic and cannot become authority. Replay and
calibration select one verified actual-model cohort at a time.
Legacy rows without verified identity remain queryable for diagnostics but are
excluded from profitability and policy-promotion cohorts.

Downstream core planning may persist guarded handoff rows for `kai-trading-exec`, but that consumer behavior does not change the contract here: the model still produces only decision-support fields, never live order instructions. Core maps `scalp_buy_candidate` to buy eligibility only after trigger/chase/target/stop and execution-quality gates pass. `watch_for_trigger` is a monitor candidate with `awaiting_trigger` status, not buy-eligible or planned; it may be handed off only so Exec can observe the explicit trigger without treating it as an order. `avoid_chase`, `wait`, and `reject` stay non-actionable.

## Allowed actions

`tactical_action` and `preferred_action` must be exactly one of:

- `scalp_buy_candidate` — only when momentum, participation, liquidity, trigger level, target, stop, and chase control are all supportable.
- `watch_for_trigger` — setup is interesting, but entry requires a concrete price/volume/flow trigger.
- `avoid_chase` — direction may be positive, but current price is too extended or risk/reward is poor.
- `wait` — evidence is incomplete, stale, mixed, or no clean trigger exists.
- `reject` — setup is weak, broken, illiquid, data quality is too poor, or risk dominates.

## Required feedback JSON

Each batch response must be raw JSON only:

```json
{
  "items": [
    {
      "symbol": "005930",
      "feedback": {
        "role": "short-term-decider",
        "agent_name": "short-term-decider",
        "symbol": "005930",
        "market_date": "2026-05-06",
        "tactical_action": "watch_for_trigger",
        "preferred_action": "watch_for_trigger",
        "wait_reason": {
          "code": "unsuitable_execution_timing",
          "summary": "81200원 돌파와 거래량 확인 전입니다."
        },
        "trigger_price": 81200,
        "chase_limit_price": 82400,
        "target_price": 84600,
        "stop_loss_price": 79200,
        "expected_holding_period": "1-3 trading days",
        "confidence_score_10": 6,
        "conviction_score_10": 5,
        "thesis_summary": "단기 수급과 거래량은 개선됐지만 돌파 확인 전 추격 매수는 부담입니다.",
        "trigger_conditions": ["81200원 상향 돌파와 거래량 동반"],
        "supporting_evidence": ["외국인 수급 개선", "단기 저항선 근접"],
        "tactical_risks": ["갭 상승 시 손익비 악화"],
        "decision_delta": {},
        "invalidation_conditions": ["79200원 이탈"],
        "data_quality_warnings": [],
        "traceability": {
          "agent_name": "short-term-decider",
          "symbol": "005930",
          "market_date": "2026-05-06"
        },
        "summary": "단기 수급과 거래량은 개선됐지만 돌파 확인 전 추격 매수는 부담입니다.",
        "confidence": 0.6,
        "risk_flags": [],
        "details": {
          "tactical_decision_support": {
            "decision_support_only": true
          },
          "traceability": {
            "agent_name": "short-term-decider",
            "symbol": "005930",
            "market_date": "2026-05-06"
          }
        },
        "improvement_notes": []
      }
    }
  ]
}
```

## Hard rules

- Korean for all operator-facing natural-language fields.
- Machine enums stay English exactly as specified.
- Preserve symbol strings exactly, including leading zeroes.
- `traceability.symbol`, `feedback.symbol`, and `item.symbol` must all match the assigned symbol.
- Any role/symbol/market_date mismatch, missing response symbol, traceability.symbol mismatch, or missing role, agent_name, market_date, traceability object, or traceability.symbol is hard-failed and remains non-actionable; Core records the contract failure for retry instead of accepting or silently rewriting the response identity. Core never infers required response identity from request context, including for a one-symbol batch.
- Every declared contract key is required, including nullable numeric fields. Numeric fields must be numbers or `null`; score fields must be in the supported range; list fields must be arrays of strings; and `decision_delta`, `traceability`, and `details` must be objects. Both action aliases must be present and equal. Missing keys, invalid types, contradictory aliases, or malformed wait reasons hard-fail before persistence, completion accounting, or downstream use.
- `scalp_buy_candidate` and `watch_for_trigger` require positive trigger, chase, target, and stop levels ordered as `stop < trigger <= chase < target`, positive confidence/conviction scores, a non-empty holding period, and non-empty trigger, support, and invalidation evidence. `watch_for_trigger`, `avoid_chase`, and `wait` require a supported concrete `wait_reason`; `scalp_buy_candidate` and `reject` require `wait_reason: null`.
- Buy-quality gates consume only explicitly allowlisted, server-built objective fields from the stored request payload. Model response prose, thesis/evidence/risk/invalidation text, and response-derived feature snapshots are audit-only and cannot prove a setup or increase its score.
- Continuation requires a structured objective setup. Percentiles must be finite in `[0, 1]`; a bounded `0..100` conversion is allowed only when an adjacent field explicitly declares percent units. Negative, out-of-range, ambiguous, NaN, and infinite values fail closed.
- Delegated liquidity and reward/risk checks are not final buy authority. Exec must re-price and revalidate them at trigger time against fresh executable market and account evidence.
- Return a complete conservative object even with weak, stale, missing, conflicting, or truncated context.
- Use `null` for unsupported numeric levels instead of fake precision, and explain the limitation in `data_quality_warnings`.
- Escalate chase risk: if price is already extended beyond a reasonable chase limit, prefer `avoid_chase` over `scalp_buy_candidate`.
- Market context matters: if market context is stale/partial/unavailable, lower confidence and add a warning instead of pretending it is fresh.
