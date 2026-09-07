# GET /agent-context/signal-decider/decision-policies

## Summary
Lists the durable signal-decider policy-version registry, including active, candidate, draft, and retired versions.

## Example request
```http
GET /agent-context/signal-decider/decision-policies?status=candidate&limit=20
```

## Example response
```json
{
  "summary": {
    "count": 1,
    "active_policy_version": "signal_decider_policy_v1",
    "manual_activation_required": true
  },
  "items": [
    {
      "policy_version": "signal_decider_policy_v1-h5-tighten_buy_gate-12ab34cd56ef",
      "status": "candidate",
      "parent_policy_version": "signal_decider_policy_v1",
      "change_summary": "Tighten buy decisions when technical.breakout_20d=true: 5/6 buys graded bad over 5d (83%)."
    }
  ]
}
```


# GET /agent-context/signal-decider/decision-policies/{policy_version}

## Summary
Reads one durable signal-decider policy-version record, including the stored rule bundle, metadata, and any linked generated candidate evidence.

## Example request
```http
GET /agent-context/signal-decider/decision-policies/signal_decider_policy_v1-h5-tighten_buy_gate-12ab34cd56ef
```

## Example response
```json
{
  "item": {
    "policy_version": "signal_decider_policy_v1-h5-tighten_buy_gate-12ab34cd56ef",
    "status": "candidate",
    "parent_policy_version": "signal_decider_policy_v1",
    "activation_mode": "manual_only",
    "candidate_activation_required": true,
    "rule_bundle": {
      "schema_version": "signal_decider_policy_bundle_v1",
      "candidate_kind": "tighten_buy_gate",
      "base_policy_version": "signal_decider_policy_v1",
      "replay_contract": {
        "market_date_from": "2026-04-20",
        "market_date_to": "2026-04-30",
        "agent_name": "signal-decider",
        "symbols": ["005930"],
        "policy_versions": ["signal_decider_policy_v1"],
        "horizon_days": 5,
        "cost_assumptions": {
          "fee_bps": 10,
          "slippage_bps": 5,
          "notional_krw": 1000000
        },
        "rule_config": {
          "schema_version": "decider_replay_rule_config_v1",
          "candidate_policy_version": "signal_decider_policy_v1-h5-tighten_buy_gate-12ab34cd56ef",
          "rules": [
            {
              "name": "candidate_policy_rule",
              "predicates": [
                {
                  "type": "field",
                  "field": "technical.breakout_20d",
                  "op": "==",
                  "value": true
                }
              ]
            }
          ]
        },
        "top_examples_limit": 10
      }
    },
    "candidate": {
      "candidate_version": "signal_decider_policy_v1-h5-tighten_buy_gate-12ab34cd56ef",
      "support_ratio": 0.833333
    }
  }
}
```


# POST /agent-context/signal-decider/decision-policies/{policy_version}/activate

## Summary
Performs the explicit human approval step that activates a candidate decision-policy version and retires the prior active version. Activation requires the complete, unmodified JSON replay artifact for the stored candidate replay contract. Core recomputes that replay server-side and rejects a different base policy, rule, symbol cohort, date window, horizon, cost assumption, or result.

Only `tighten_buy_gate` candidates are currently activation-eligible. `relax_wait_gate` and `relax_reject_gate` remain reviewable proposals but fail closed until a replay implementation can model adding counterfactual trades.
Legacy candidates whose rule bundle lacks a replay contract, base policy, or base-policy-only `policy_versions` cohort also fail closed and must be regenerated before review.

## Example request workflow
```bash
POLICY_VERSION=signal_decider_policy_v1-h5-tighten_buy_gate-12ab34cd56ef

# Copy every replay input from item.rule_bundle.replay_contract.
curl -s "http://127.0.0.1:8000/agent-context/signal-decider/decision-policies/${POLICY_VERSION}" \
  | jq '.item.rule_bundle.replay_contract.rule_config' > /tmp/candidate-rule.json

python3.11 -m app.internal.ops agent-decider-replay \
  --market-date-from 2026-04-20 \
  --market-date-to 2026-04-30 \
  --agent-name signal-decider \
  --symbols 005930 \
  --policy-versions signal_decider_policy_v1 \
  --horizon-days 5 \
  --rule-config-path /tmp/candidate-rule.json \
  --fee-bps 10 \
  --slippage-bps 5 \
  --notional-krw 1000000 \
  --top-examples-limit 10 \
  --output-format json > /tmp/candidate-replay.json

# replay_artifact must be the complete JSON object emitted above, not a summary.
jq --arg policy_version "${POLICY_VERSION}" \
  '{
    activated_by: "human.reviewer",
    activation_note: "Reviewed diagnostics and approved the candidate.",
    replay_evidence: {
      schema_version: "signal_decider_policy_activation_evidence_v1",
      candidate_policy_version: $policy_version,
      replay_artifact: .
    }
  }' /tmp/candidate-replay.json > /tmp/policy-activation.json

curl -X POST \
  "http://127.0.0.1:8000/agent-context/signal-decider/decision-policies/${POLICY_VERSION}/activate" \
  -H 'Content-Type: application/json' \
  --data-binary @/tmp/policy-activation.json
```

## Example response
```json
{
  "summary": {
    "activated": true,
    "already_active": false,
    "previous_active_policy_version": "signal_decider_policy_v1",
    "manual_activation_required": true
  },
  "item": {
    "policy_version": "signal_decider_policy_v1-h5-tighten_buy_gate-12ab34cd56ef",
    "status": "active",
    "activated_by": "human.reviewer"
  }
}
```
