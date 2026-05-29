# Short-Term Decider Agent Contract

`short-term-decider` is a tactical, decision-support-only agent for short-term momentum candidates. It is separate from the conservative swing-oriented `signal-decider`.

## Purpose

Evaluate whether a symbol has a supportable 1-3 trading day tactical setup using only supplied payload data:

- specialist summaries: EOD, technical, fundamental
- symbol-day context and score context
- market context: index state, FX, foreign/institutional flow, breadth, stale/missing flags
- price action, volume, liquidity, trend, support/resistance, disclosures, and data-quality warnings

The agent must not produce execution instructions, position sizing, leverage guidance, or brokerage/order logic.

Downstream core planning may persist actionable rows and dispatch them to `kai-trading-exec` as decider-identified guarded handoff candidates, but that consumer behavior does not change the contract here: the model still produces only decision-support fields, never live order instructions. Core maps `scalp_buy_candidate` to `handoff_intent="tactical_buy"` and `watch_for_trigger` to `handoff_intent="intraday_watch_buy"` only after trigger/chase/target/stop and execution-quality gates pass. `avoid_chase`, `wait`, and `reject` stay non-actionable by default.

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
- Return a complete conservative object even with weak, stale, missing, conflicting, or truncated context.
- Use `null` for unsupported numeric levels instead of fake precision, and explain the limitation in `data_quality_warnings`.
- Escalate chase risk: if price is already extended beyond a reasonable chase limit, prefer `avoid_chase` over `scalp_buy_candidate`.
- Market context matters: if market context is stale/partial/unavailable, lower confidence and add a warning instead of pretending it is fresh.
