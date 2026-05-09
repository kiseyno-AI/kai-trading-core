# GET /agent-context/feedback/decider-coverage

## Summary
Builds the operator-facing daily decider coverage report from persisted decider request payloads. The response rolls up core execution block availability (`available`, `missing`, `stale`, `derived`, `provider_error`), optional block availability (`available`, `empty`, `unavailable`, `provider_error`), top degraded categories, default gap-effect buckets (`decision_affected`, `confidence_reducer`, `neutral_auxiliary`), raw-null contract regressions for expected object-style context paths, and per-agent summaries when both deciders are present.

## Example request
```http
GET /agent-context/feedback/decider-coverage?market_date=2026-05-08&run_id=symbol-run-2026-05-08-abc123&agent_names=signal-decider&agent_names=short-term-decider
```

## Example response
```json
{
  "request": {
    "market_date": "2026-05-08",
    "run_id": "symbol-run-2026-05-08-abc123",
    "symbols": null,
    "agent_names": ["short-term-decider", "signal-decider"]
  },
  "summary": {
    "symbols_evaluated": 111,
    "core_blocks": {
      "counts": {
        "available": 82,
        "missing": 19,
        "stale": 7,
        "derived": 3,
        "provider_error": 0
      }
    },
    "optional_blocks": {
      "counts": {
        "available": 403,
        "empty": 28,
        "unavailable": 51,
        "provider_error": 2
      }
    },
    "contract_regressions": {
      "status": "alert",
      "raw_null_count": 4,
      "unexpected_missing_core_block_count": 19
    }
  },
  "per_agent": {
    "signal-decider": {
      "feedback_rows": 111
    },
    "short-term-decider": {
      "feedback_rows": 111
    }
  }
}
```
