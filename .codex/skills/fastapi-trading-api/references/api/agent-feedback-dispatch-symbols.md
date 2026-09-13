# POST /agent-context/feedback/dispatch/symbols

## Summary
Dispatches symbol-level summarizer feedback collection with optional freshness-aware reuse.

Core dispatches requested symbols through the strategy-aware prefilter scoring rule. `daily_market_risk_filter_results.passed_filter=false` remains a hard block and those symbols are returned in `request.skipped_symbols`. Piotroski/value failures are soft penalties for the short-term/momentum/breakout feedback path, so those symbols remain in `request.symbols` and are listed in `request.soft_penalized_symbols` with details under `request.eligibility_diagnostics`.
Explicit requested-symbol dispatch bypasses the automatic broad and role caps used by daily retry cycles. The response records `auto_scope=false`, per-role scopes equal to the requested eligible symbols, and `requested_symbol_scope_cap_bypass` under `request.funnel_diagnostics`.
Each normal dispatch also creates a durable `agent_context_feedback_runs` row, checkpoints role progress as the run advances, and finalizes that run row even when the cycle exits partially. When `wait_for_completion=false`, Core now creates the durable queued run row before returning and reuses the same `request_run_id` when the background work starts.
Normal dispatch now also gates on same-day feedback prerequisites: PyKRX enrichment coverage, DART fundamentals refresh, market-risk filter, Piotroski filter, and the factor/scoring stages must all be terminal and successful enough for agent context. The PyKRX gate includes decider-required category details with expected and present row counts. When prerequisites are still running or incomplete, the route returns `summary.status=waiting|blocked`, `summary.gated=true`, and a `prerequisites` block instead of starting dispatch. Operators can bypass that guard only with `force=true`.

Before live OpenClaw dispatch starts, Core preflights gateway auth. Missing `OPENCLAW_GATEWAY_TOKEN` or HTTP 401/403 from the gateway persists one run-level failure in `agent_context_feedback_failures` with `agent_name="openclaw-gateway"` and `symbol="__RUN__"`, then returns `summary.status="failed"` without loading symbol context or queuing batches. Operators can inspect grouped auth failures with `GET /agent-context/feedback/failure-summary`.

For `fundamental-summarizer`, `reuse_existing=true` reuses a prior trading date only when the stored fundamental input fingerprint matches the current DART/disclosure/valuation input fingerprint. `force_recompute=true` bypasses reuse explicitly without changing the prerequisite gate behavior.

`knowledge_cutoff_at` is an optional timezone-aware instant that acts as an
exclusive upper bound for both stored-news publication time and collection
time. The same effective instant is fixed across every requested role. When it
is omitted, a completed historical KRX date uses 15:30 KST and a historical
US-only request uses the XNYS calendar's actual close, including early closes.
When the field is omitted, current or future dates use request time. Mixed
KRX/US symbol requests require an explicit cutoff. Timezone-naive or explicitly
future values fail with HTTP 400. Response and durable request payloads
serialize the effective instant in UTC, so an input such as
`2026-04-15T15:30:00+09:00` is returned as
`2026-04-15T06:30:00+00:00`.

## Example request
```http
POST /agent-context/feedback/dispatch/symbols
Content-Type: application/json

{
  "market_date": "2026-04-15",
  "symbols": ["005930", "000660"],
  "roles": [
    "eod-summarizer",
    "technical-summarizer",
    "fundamental-summarizer",
    "short-term-decider",
    "signal-decider"
  ],
  "fs_div": "CFS",
  "disclosures_limit": 5,
  "knowledge_cutoff_at": "2026-04-15T15:30:00+09:00",
  "reuse_existing": true,
  "force_recompute": false,
  "wait_for_completion": false,
  "force": false
}
```

## Example response
```json
{
  "request": {
    "market_date": "2026-04-15",
    "symbols": ["005930", "000660"],
    "requested_symbols": ["005930", "000660"],
    "request_run_id": "symbol-run-2026-04-15-ab12cd34ef",
    "roles": [
      "eod-summarizer",
      "technical-summarizer",
      "fundamental-summarizer",
      "short-term-decider",
      "signal-decider"
    ],
    "fs_div": "CFS",
    "disclosures_limit": 5,
    "knowledge_cutoff_at": "2026-04-15T06:30:00+00:00",
    "reuse_existing": true,
    "scope": "requested_symbols_scored_by_strategy_aware_prefilter",
    "eligibility_rule": "strategy_aware_prefilter_scoring_v1",
    "eligibility_filters": ["market_risk_filter", "piotroski_filter"],
    "force_recompute": false,
    "wait_for_completion": false
  },
  "summary": {
    "status": "queued",
    "queued": 2
  }
}
```

When `wait_for_completion=true`, the completed dispatch response also includes `request.skipped_symbols`, `request.soft_penalized_symbols`, and `request.eligibility_diagnostics`:

```json
{
  "request": {
    "skipped_symbols": ["005930"],
    "soft_penalized_symbols": ["000660"],
    "eligibility_diagnostics": {
      "rule_version": "strategy_aware_prefilter_scoring_v1",
      "strategy_scope": "short_term_momentum_breakout",
      "hard_blocked_symbols": ["005930"],
      "soft_penalized_symbols": ["000660"]
    }
  }
}
```

Completed synchronous responses include reuse accounting in `summary`:

```json
{
  "summary": {
    "status": "completed",
    "completed": 1,
    "computed_count": 1,
    "reused_count": 1,
    "stale_count": 0,
    "failed": 0,
    "flushed_to_db": 1
  }
}
```

## Gated response
```json
{
  "request": {
    "market_date": "2026-04-15",
    "symbols": ["005930"]
  },
  "summary": {
    "status": "blocked",
    "completed": 0,
    "failed": 0,
    "skipped": 0,
    "flushed_to_db": 0,
    "gated": true
  },
  "items": [],
  "failures": [],
  "prerequisites": {
    "status": "blocked",
    "blocked_stage_ids": ["pykrx_enrichment"],
    "missing_prerequisites": [
      {
        "stage": "pykrx_enrichment",
        "gate_status": "blocked",
        "details": {
          "decider_blocking_categories": [
            {
              "category": "investor_flow_symbol",
              "expected_rows": 3579,
              "present_rows": 0,
              "missing_rows": 3579
            }
          ]
        }
      }
    ]
  }
}
```

## Auth preflight failure
```json
{
  "summary": {
    "status": "failed",
    "completed": 0,
    "computed_count": 0,
    "reused_count": 0,
    "failed": 1,
    "run_level_failure": true,
    "failure_kind": "openclaw_auth_missing"
  },
  "failures": [
    {
      "agent_name": "openclaw-gateway",
      "symbol": "__RUN__",
      "failure_kind": "openclaw_auth_missing",
      "retryable": false
    }
  ]
}
```
