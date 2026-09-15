# GET /eod/pipeline/stages and POST /eod/pipeline/run

## Summary
Lists the canonical daily stage catalog and runs an explicit subset of those stages through the thin daily pipeline orchestrator.

`GET /eod/pipeline/stages` returns the stable stage order, default operator sequence, dependency metadata, inspect routes, and output-table hints so operators can decide which stages to run.

The default operator sequence now includes `pykrx_enrichment` immediately after `price_collection`, then the factor/scoring stages after `fundamentals_daily_refresh` and `piotroski_filter`: `momentum_factors`, `value_factors`, `low_risk_factors`, `beta_factors`, `advanced_value_factors`, `unified_value_factors`, `quality_factors`, and `composite_factors`. It then runs `external_news_collect`, `external_news_enrich`, and `macro_context_refresh` before agent feedback dispatch.

`POST /eod/pipeline/run` accepts a stage subset, preserves canonical ordering, creates a durable `daily_pipeline_runs` row plus per-stage `daily_pipeline_stage_runs`, and delegates execution to the reusable stage operations. Callers should include any prerequisites they need; declared dependencies included in the same run are enforced and block downstream stages when an upstream stage does not complete. `resume_from_stage` narrows the run to that stage and later canonical stages so operators can restart at `market_risk_filter` without rerunning upstream price or technical outputs. Set `background=true` for long-running recovery work; stage names and `market_date` are validated before the route returns `202 Accepted`, and invalid requests return the same `400` response as synchronous runs. The durable pipeline status endpoints expose subsequent progress. Admission is serialized per market date and overlapping active stage sets are deduplicated across API, manual, and monitor triggers. Before deduplication, `running` rows older than `EOD_AUTO_PYKRX_STALE_RUN_MINUTES` by their last persisted activity are marked `abandoned` with resumable audit metadata so crashed workers cannot block recovery indefinitely. `force` controls stage recomputation, while the separate recovery-only `bypass_prerequisite_gate` flag bypasses prerequisite inspection without forcing completed feedback artifacts to be recomputed.

## Historical recovery boundary

An explicit prior `market_date` submitted through this route uses trigger `manual_api` and automatically enables point-in-time recovery. The following current-only stages return an HTTP 200 safe-skip result rather than reading live data or writing it into a historical date: `master_refresh`, `pykrx_universe_refresh`, `signal_features`, `market_risk_snapshots`, `market_risk_filter`, `fundamentals_daily_refresh`, `external_news_collect`, `external_news_enrich`, `macro_context_refresh`, and `intraday_snapshots`.

Once the target next-open window has passed, `agent_feedback_summaries`, `signal_decider`, and `signal_decider_evaluations` return a `next_open_window_expired` result with their stage ID. `next_open_execution_plan` returns the same expiry reason without creating or refreshing plan items. These are auditable no-ops; they must not be interpreted as successful historical decisions.

## Example catalog request
```http
GET /eod/pipeline/stages
```

## Example run request
```http
POST /eod/pipeline/run
Content-Type: application/json

{
  "stages": ["signal_decider_evaluations", "signal_decider", "technical_snapshots", "price_collection", "agent_feedback_summaries"],
  "symbols": ["005930", "000660"],
  "markets": ["KOSPI", "KOSDAQ"],
  "force": false,
  "bypass_prerequisite_gate": true,
  "fs_div": "CFS",
  "capture_limit": 100,
  "resume_from_stage": "technical_snapshots",
  "background": true
}
```

The request above omits `market_date`, so the service resolves it at request time. The completed response shape below applies only while that source date's next-open action window is still actionable.

## Example background response

```json
{
  "status": "accepted",
  "requested_stages": ["technical_snapshots", "agent_feedback_summaries"],
  "background": true
}
```

## Example response
```json
{
  "pipeline_run_id": 812,
  "pipeline_run": {
    "id": 812,
    "status": "completed",
    "current_stage": "signal_decider_evaluations"
  },
  "requested_stages": [
    "signal_decider_evaluations",
    "signal_decider",
    "technical_snapshots",
    "price_collection",
    "agent_feedback_summaries"
  ],
  "resolved_stages": [
    "price_collection",
    "technical_snapshots",
    "agent_feedback_summaries",
    "signal_decider",
    "signal_decider_evaluations"
  ],
  "status": "completed",
  "stages": {
    "price_collection": {
      "stage": "price_collection",
      "label": "pykrx daily price collection",
      "group": "market_data",
      "status": "completed",
      "result": {
        "run_id": 184,
        "bars_upserted": 2,
        "symbols_succeeded": 2,
        "symbols_failed": 0
      }
    },
    "signal_decider": {
      "stage": "signal_decider",
      "label": "signal decider dispatch",
      "group": "agent_feedback",
      "status": "completed",
      "result": {
        "completed": true,
        "final_pending": 0
      }
    },
    "signal_decider_evaluations": {
      "stage": "signal_decider_evaluations",
      "label": "signal decider evaluation refresh",
      "group": "agent_feedback",
      "status": "completed",
      "result": {
        "summary": {
          "outcomes_upserted": 8,
          "evaluations_upserted": 8
        }
      }
    }
  }
}
```

For a prior-date recovery, current-only stage results have this exact shape:

```json
{
  "triggered": false,
  "skipped": true,
  "safe_skip": true,
  "status": "skipped",
  "reason": "point_in_time_recovery_current_only_stage",
  "market_date": "2026-04-21",
  "stage": "market_risk_filter"
}
```

Expired feedback, decider, and evaluation stages include `stage`; the next-open plan result uses the same shape without `stage`:

```json
{
  "triggered": false,
  "status": "skipped",
  "reason": "next_open_window_expired",
  "market_date": "2026-04-21",
  "target_trade_date": "2026-04-22",
  "current_market_date": "2026-04-23",
  "market_state": "CLOSED",
  "stage": "signal_decider"
}
```
