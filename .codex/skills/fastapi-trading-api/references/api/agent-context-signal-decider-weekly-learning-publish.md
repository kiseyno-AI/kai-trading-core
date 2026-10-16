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
The versioned Exec evidence manifest must also carry a server-validated
`learning_quality` summary. A week with closed trades, failures, or terminal
rejections is ineligible when its corresponding Exec feedback channel is not
consumer-eligible. Core independently derives performance eligibility from
cost-basis, transaction-cost, settlement-currency, fill-attribution, and
closed-trade-lineage coverage. It derives failure eligibility from normalized
failure counts and complete failure lineage; compact status/eligibility claims
cannot override incomplete evidence. A zero-execution/zero-failure week can
publish only as the explicit `calibration_only` mode. That mode omits actual
execution learning and is labeled in the artifact, learning context, and
injected performance criteria. Publishing never submits an order and never
activates a policy candidate.

`evidence_manifest.idempotency_key` is an opaque stable key. Core scopes it to
the week, horizon, evaluation version, minimum sample, lookback, maturity, and
validity settings, plus the canonical source evidence SHA-256 when supplied.
Retry-only fields such as run IDs and completion timestamps do not create a
second artifact, while corrected source evidence with a different SHA creates
a new immutable artifact. The complete first accepted manifest remains in the
artifact for audit. Corrections are ordered by acceptance time within the exact
evidence week and source identity. A valid ineligible correction revokes the
older artifact from latest/context reads without deleting or rewriting either
audit row; a later eligible correction restores guidance only from its normal
effective date.

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
    "schema_version": "weekly-learning-evidence-manifest/v1",
    "evidence_schema_version": "weekly-trade-review-v2",
    "week_start": "2026-10-05",
    "week_end": "2026-10-09",
    "source": {
      "sha256": "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
      "runtime_mode": "live",
      "exec_git_commit": "0123456789abcdef0123456789abcdef01234567"
    },
    "validation": {"status": "valid"},
    "learning_quality": {
      "schema_version": "weekly-learning-quality/v1",
      "mode": "calibration_only",
      "publication_eligible": true,
      "ineligible_reasons": [],
      "performance": {
        "status": "no_data",
        "consumer_eligible": false,
        "closed_trade_count": 0,
        "cost_basis_coverage": "complete",
        "transaction_cost_coverage": "complete",
        "settlement_currency_coverage": "incomplete",
        "fill_decision_lineage_coverage": "empty",
        "closed_trade_decision_lineage_missing_count": 0
      },
      "failure_and_rejection": {
        "status": "no_data",
        "consumer_eligible": false,
        "execution_failure_count": 0,
        "terminal_order_outcome_count": 0,
        "lineage_linked_execution_failure_count": 0,
        "lineage_missing_execution_failure_count": 0
      }
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
explicit publication mode, and the advisory
`learning_context.performance_criteria` injected into decider requests.
Malformed, missing, or internally contradictory evidence contracts return
HTTP 400. A structurally valid `ineligible_evidence` manifest is persisted for
audit with `status="ineligible"` and is never returned by the latest-published
read.
