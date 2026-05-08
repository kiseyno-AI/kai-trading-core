# POST /agent-context/feedback

## Summary
Stores externally produced agent feedback into `agent_context_feedback`. When the submitted `context` includes symbol-day freshness/provenance metadata, Core uses it to enrich normalized missing-data categories so later reporting can distinguish `missing`, `stale`, `available_but_old`, and `derived_or_fallback` complaints. Those stored categories now also expose decision-tier and severity semantics so downstream tooling can separate core execution blockers from auxiliary confidence-only gaps.

## Example request
```http
POST /agent-context/feedback
Content-Type: application/json

{
  "market_date": "2026-04-15",
  "symbol": "005930",
  "agent_name": "fundamental-summarizer",
  "summary": {},
  "feedback": {"role": "fundamental-summarizer", "summary": "No material DART change detected."},
  "context": {
    "source_route": "/agent-context/symbol-day",
    "symbol_day_context": {
      "context_provenance": {
        "fundamentals": {
          "freshness_status": "available_but_old",
          "provenance_status": "provider_sourced",
          "issue_types": ["available_but_old"],
          "source_tables": ["dart_fundamentals_snapshots"]
        }
      }
    }
  }
}
```

## Example response
```json
{
  "id": 12345,
  "market_date": "2026-04-15",
  "symbol": "005930",
  "agent": {"name": "fundamental-summarizer", "version": null, "run_id": null}
}
```

## Notes
- `summary` and `feedback` remain free-form JSON objects.
- The route does not require `context`, but provenance-aware missing-data normalization only happens when the submitted `context` includes the relevant symbol-day metadata.
- Persisted `missing_data_categories` now carry `decision_tier`, `severity`, `default_decider_effect`, and `blocking_by_default` fields in addition to the existing taxonomy status.
