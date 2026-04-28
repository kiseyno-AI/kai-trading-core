# POST /agent-context/signal-decider/policy-candidates/generate

Alias: `POST /agent-context/signal-decider/decision-policies/candidates/generate`

## Summary
Refreshes the requested evaluation slice and generates manual-only candidate policy improvements from accumulated immutable `signal-decider` snapshot evidence.

## Example request
```http
POST /agent-context/signal-decider/policy-candidates/generate
Content-Type: application/json

{
  "market_date_from": "2026-04-20",
  "market_date_to": "2026-04-30",
  "horizons": [5],
  "primary_horizon_days": 5,
  "evaluation_version": "signal_decider_eval_v1",
  "min_sample_size": 3
}
```

## Example response
```json
{
  "summary": {
    "evaluations_considered": 24,
    "candidates_generated": 2,
    "policy_versions_registered": 2,
    "manual_activation_required": true
  },
  "items": [
    {
      "candidate_version": "signal_decider_policy_v1-h5-relax_wait_gate-12ab34cd56ef",
      "policy_version": "signal_decider_policy_v1-h5-relax_wait_gate-12ab34cd56ef",
      "base_policy_version": "signal_decider_policy_v1",
      "candidate_kind": "relax_wait_gate",
      "activation_mode": "manual_only",
      "policy_status": "candidate",
      "summary": "Relax wait decisions when technical.breakout_20d=true: 5/6 wait calls missed upside over 5d (83%)."
    }
  ]
}
```


# GET /agent-context/signal-decider/policy-candidates

Alias: `GET /agent-context/signal-decider/decision-policies/candidates`

## Summary
Reads the generated candidate policy-improvement rows. Candidate generation is automatic, but activation remains manual-only.

## Example request
```http
GET /agent-context/signal-decider/policy-candidates?status=generated&evaluation_version=signal_decider_eval_v1&limit=20
```

## Example response
```json
{
  "items": [
    {
      "candidate_version": "signal_decider_policy_v1-h5-tighten_buy_gate-98fe76dc54ba",
      "policy_version": "signal_decider_policy_v1-h5-tighten_buy_gate-98fe76dc54ba",
      "status": "generated",
      "activation_mode": "manual_only",
      "policy_status": "candidate",
      "feature_key": "meta.has_data_warnings",
      "feature_value": "true",
      "support_ratio": 0.75
    }
  ]
}
```
