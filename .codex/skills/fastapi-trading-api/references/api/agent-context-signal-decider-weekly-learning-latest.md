# GET /agent-context/signal-decider/weekly-learning/latest

## Summary

Read the newest published, eligible, hash-valid signal-decider weekly learning
artifact. `as_of_date` is optional and lets operators audit freshness for a
specific market date.

Core returns `available=false` for a missing, ineligible, content-hash-mismatched,
evidence-contract-invalid, or expired artifact. Published candidates must retain
the validated `weekly-learning-quality/v1` contract that authorized their
publication. A newer hash-valid, contract-valid correction for the same evidence
week and source identity supersedes the older immutable artifact. When that
correction is ineligible, the response is unavailable with
`reason="artifact_superseded_by_ineligible_correction"`, and the older published
guidance is not injected into a decider context. A later eligible correction
becomes available only after its normal effective date. Invalid or future-dated
rows do not revoke point-in-time history.

Expiry is the evidence `week_end + 14 days`; stale artifacts remain visible in
the response for audit but are not injected into a decider payload. Candidate
selection skips a newly published but already stale backfill in favor of the
newest still-valid artifact. An artifact is eligible only from the day after
both its evidence week and publication date, whether Core resolves the current
date or the caller supplies `as_of_date`. This keeps future or same-day weekly
learning out of live and historical decision contexts.

## Example request

```http
GET /agent-context/signal-decider/weekly-learning/latest?as_of_date=2026-10-13
```

## Example response

```json
{
  "available": true,
  "stale": false,
  "as_of_date": "2026-10-13",
  "valid_through": "2026-10-23",
  "artifact": {
    "artifact_version": "signal_decider_weekly_learning_2026-10-09_abc123def456",
    "source_fingerprint": "...",
    "content_sha256": "...",
    "week_start": "2026-10-05",
    "week_end": "2026-10-09",
    "valid_through": "2026-10-23",
    "effective_from": "2026-10-11",
    "training_window": {
      "training_date_from": "2026-06-22",
      "training_date_to": "2026-09-20",
      "lookback_days": 90,
      "maturity_buffer_days": 15
    }
  }
}
```

Every signal-decider and short-term-decider dispatch/revalidation reads this
surface internally and, when available, places the exact context under
`signal_decider_policy.performance_criteria`. Signal-decider retains the
existing rolling calibration fallback when no eligible artifact exists;
short-term-decider retains its prior empty policy context. No learning artifact
grants buy/sell authority or activates policy changes.
