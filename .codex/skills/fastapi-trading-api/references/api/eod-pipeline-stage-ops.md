# GET /eod/pipeline/stages and POST /eod/pipeline/run

## Summary
Lists the canonical daily stage catalog and runs an explicit subset of those stages through the thin daily pipeline orchestrator.

`GET /eod/pipeline/stages` returns the stable stage order, default operator sequence, dependency metadata, inspect routes, and output-table hints so operators can decide which stages to run.

The default operator sequence now includes `pykrx_enrichment` immediately after `price_collection`, then the factor/scoring stages after `fundamentals_daily_refresh` and `piotroski_filter`: `momentum_factors`, `value_factors`, `low_risk_factors`, `beta_factors`, `advanced_value_factors`, `unified_value_factors`, `quality_factors`, and `composite_factors`. It then runs `external_news_collect`, `external_news_enrich`, and `macro_context_refresh` before agent feedback dispatch.

`POST /eod/pipeline/run` accepts a stage subset, preserves canonical ordering, creates a durable `daily_pipeline_runs` row plus per-stage `daily_pipeline_stage_runs`, and delegates execution to the reusable stage operations. Callers should include any prerequisites they need; the catalog response exposes `depends_on` for inspectability. `resume_from_stage` narrows the run to that stage and later canonical stages so operators can restart at `market_risk_filter` without rerunning upstream price or technical outputs.

## Example catalog request
```http
GET /eod/pipeline/stages
```

## Example run request
```http
POST /eod/pipeline/run
Content-Type: application/json

{
  "market_date": "2026-04-21",
  "stages": ["signal_decider_evaluations", "signal_decider", "technical_snapshots", "price_collection", "agent_feedback_summaries"],
  "symbols": ["005930", "000660"],
  "markets": ["KOSPI", "KOSDAQ"],
  "force": true,
  "fs_div": "CFS",
  "capture_limit": 100,
  "resume_from_stage": "technical_snapshots"
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
  "market_date": "2026-04-21",
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
        "market_date": "2026-04-21",
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
