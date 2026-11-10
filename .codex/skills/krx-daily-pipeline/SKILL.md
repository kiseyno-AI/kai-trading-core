---
name: krx-daily-pipeline
description: Diagnose or change Core's KRX daily pipeline stage outcomes, symbol admission, data coverage, retries, and downstream block conditions. Use for PyKRX, DART, KIS, factor, agent, or next-open stages in this pipeline.
---

# KRX daily pipeline

Start in the Core repository. `apps/api/app/eod/daily_pipeline_stages.py` defines 28 stage IDs and their declared dependencies. The default stage executor selects 26. The explicit KIS market-risk snapshot stage and decider evaluations are outside that selection. The automatic PyKRX runner tracks 24 stages starting at price collection. Read the reference for the affected stage and check current source before changing a rule.

- [Market data and risk](references/market-data.md) covers stages 1–9.
- [DART, factors and signal features](references/factors.md) covers stages 10–20.
- [Optional context and agent-to-plan](references/agents.md) covers stages 21–28.

## Trace the stage before changing a gate

1. Start with one market date, pipeline run ID and stage ID. Read its persisted stage outcome and exact run lineage. The read-only views are `GET /eod/pipeline/status` and the stage's inspect route. For DB diagnosis use a read-only transaction and look at `daily_pipeline_runs`, `daily_pipeline_stage_runs` and the linked `eod_collection_runs`. Do not call a refresh or run endpoint to inspect.
2. Compare selected, published, retry-debt and terminal not-applicable scopes for that attempt. Use the exact symbol lists and publication digest rather than preview samples or percentage counters. A narrowed attempt may show 100% without covering the whole market. Cross-check `result_summary`, `source_run_ids` and `scope_finalization` / `exact_scope_outcome_proof` when present. For agent stages, check both the parent and the linked child runs.
3. Identify which path consumed it. `pykrx_pipeline_runner.py` handles the first chained daily run and verified symbol cohorts. `DailyPipelineStageService` in `daily_pipeline_stages.py` resolves explicit dependencies, exact retry cohorts and omitted prerequisites for suffix/resume. `daily_pipeline_outcomes.py` normalizes and interprets status. `agent_context/prerequisite_service.py` also checks persisted readiness before agent dispatch. `orders/next_open_execution.py` separately checks trading authority. An eligibility result for one symbol is distinct from stage completion.
4. Separate a durably published null or an applicable provider's valid no-result response from a failed request, malformed payload, missing publication proof, stale source or wrong session. Preserve missing field reasons and confidence metadata. Apply strategy-specific requirements only where the relevant field is consumed. Keep invalid market membership, current-price provenance, missing mandatory risk evidence, storage failures and source authority fail-closed.
5. Exercise the producer and a downstream caller with the same known-null symbol and a separate genuinely failed symbol. Test first-run and explicit/resume paths if they share a gate. Verify that the nullable record reaches the next applicable stage, the real failure remains attributable, and an empty but definitive candidate set ends without dispatch. Use the smallest affected scope. Exact historical repair, service restarts and live orders are separate operations.
