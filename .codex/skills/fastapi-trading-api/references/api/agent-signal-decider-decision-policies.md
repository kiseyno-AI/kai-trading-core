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
      "policy_version": "signal_decider_policy_v1-h5-relax_wait_gate-12ab34cd56ef",
      "status": "candidate",
      "parent_policy_version": "signal_decider_policy_v1",
      "change_summary": "Relax wait decisions when technical.breakout_20d=true: 5/6 wait calls missed upside over 5d (83%)."
    }
  ]
}
```


# GET /agent-context/signal-decider/decision-policies/{policy_version}

## Summary
Reads one durable signal-decider policy-version record, including the stored rule bundle, metadata, and any linked generated candidate evidence.

## Example request
```http
GET /agent-context/signal-decider/decision-policies/signal_decider_policy_v1-h5-relax_wait_gate-12ab34cd56ef
```

## Example response
```json
{
  "item": {
    "policy_version": "signal_decider_policy_v1-h5-relax_wait_gate-12ab34cd56ef",
    "status": "candidate",
    "parent_policy_version": "signal_decider_policy_v1",
    "activation_mode": "manual_only",
    "candidate_activation_required": true,
    "rule_bundle": {
      "schema_version": "signal_decider_policy_bundle_v1",
      "candidate_kind": "relax_wait_gate"
    },
    "candidate": {
      "candidate_version": "signal_decider_policy_v1-h5-relax_wait_gate-12ab34cd56ef",
      "support_ratio": 0.833333
    }
  }
}
```


# POST /agent-context/signal-decider/decision-policies/{policy_version}/activate

## Summary
Performs the explicit human approval step that activates a candidate or draft decision-policy version and retires the prior active version.

## Example request
```http
POST /agent-context/signal-decider/decision-policies/signal_decider_policy_v1-h5-relax_wait_gate-12ab34cd56ef/activate
Content-Type: application/json

{
  "activated_by": "human.reviewer",
  "activation_note": "Reviewed diagnostics and approved the candidate."
}
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
    "policy_version": "signal_decider_policy_v1-h5-relax_wait_gate-12ab34cd56ef",
    "status": "active",
    "activated_by": "human.reviewer"
  }
}
```
