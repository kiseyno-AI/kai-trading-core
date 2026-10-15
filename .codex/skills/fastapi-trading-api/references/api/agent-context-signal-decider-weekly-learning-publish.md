# POST /agent-context/signal-decider/weekly-learning/publish

## Summary

Build and persist one immutable signal-decider weekly learning artifact. The
requested week is the evidence/audit window. Calibration is built from a
90-calendar-day verified cohort ending before a conservative maturity buffer
(`max(14, 3 * horizon_days)`), so pending same-week forward outcomes cannot be
published as learning.

Publication is fail closed: the calibration report must be ready, meet the
verified sample threshold, have no coverage gaps, carry a verified model
identity cohort, and retain its manual-review/no-auto-activation boundary.
Eligible actual execution outcomes for the evidence week are included only as
gross, advisory corroboration. Publishing never submits an order and never
activates a policy candidate.

`evidence_manifest.idempotency_key` is an opaque stable key. Core scopes it to
the week, horizon, evaluation version, minimum sample, lookback, maturity, and
validity settings, plus the canonical source evidence SHA-256 when supplied.
Retry-only fields such as run IDs and completion timestamps do not create a
second artifact, while corrected source evidence with a different SHA creates
a new immutable artifact. The complete first accepted manifest remains in the
artifact for audit.

## Example request

```http
POST /agent-context/signal-decider/weekly-learning/publish
Content-Type: application/json

{
  "week_start": "2026-10-05",
  "week_end": "2026-10-09",
  "horizon_days": 5,
  "evaluation_version": "signal_decider_eval_v1",
  "min_sample_size": 30,
  "evidence_manifest": {
    "source": {
      "name": "weekly-trading-analysis",
      "sha256": "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
    },
    "idempotency_key": "weekly:2026-10-05:2026-10-09:h5:v1:n30",
    "run_id": "weekly-run-2026-10-10"
  }
}
```

## Response contract

The response has `status` (`published` or `ineligible`),
`idempotent_replay`, and `artifact`. The artifact exposes its version, source
fingerprint, content hash, evidence week, maturity-safe training window,
`valid_through`, eligibility reasons, full source report, evidence manifest,
and the advisory `learning_context.performance_criteria` injected into decider
requests.
