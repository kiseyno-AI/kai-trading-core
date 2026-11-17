# Optional context and agent-to-plan stages

Source paths are under `apps/api/app/`. News, macro and intraday snapshots are ordering-only optional context in `eod/daily_pipeline_outcomes.py`. A terminal absence or terminal optional degradation may inform an agent's confidence. It is separate from missing required market/risk authority, unresolved candidate scope or a failed dispatch.

## 21. external_news_collect

- **Input and output** — Strategy-eligible candidate scope and provider search results. Persists `external_news_items` and `external_news_item_symbols` as observed evidence.
- **Completion and blocking** — A successful search with no recent article is terminal collection for that symbol. Keep no-result, stale and failed requests distinct. Optional provider degradation can finish without usable news. A provider/search failure does not prove there was no relevant news.
- **Trace** — `external_news/service.py::collect_for_agent_feedback_candidates`, `GET /external/news/health` and `GET /external/news`.

## 22. external_news_enrich

- **Input and output** — Existing news observations. Appends versioned classifications to `external_news_item_classifications`.
- **Completion and blocking** — No applicable items is valid no work. Missing required classification on an attempted item or a classifier/write error is a genuine enrichment issue. It is optional context for the agents once the optional stage is terminal.
- **Trace** — `external_news/classifier.py`, `external_news/service.py`, and the `external_news_enrich` dispatch in `eod/daily_pipeline_stages.py`.

## 23. macro_context_refresh

- **Input and output** — Due macro series and observations, with timestamps and provider availability. Writes `macro_context_observations`.
- **Completion and blocking** — `macro_context_refresh_not_due` is a valid terminal skip. Unavailable optional series retain their own quality or availability state. A failed fetch or write remains observable as optional degradation and does not by itself gate candidate dispatch.
- **Trace** — `macro_context/service.py::refresh_context` and `GET /macro/context`.

## 24. intraday_snapshots

- **Input and output** — Strategy-eligible symbols and optional KIS program/investor context. Writes `kis_intraday_snapshots`. This is distinct from mandatory `market_risk_snapshots`.
- **Completion and blocking** — An empty successful response is recorded as empty without retry. Each per-symbol quote, orderbook, program, investor-estimate and optional news GET retries only its own transient failure, at most four attempts with 1/2/4-second backoff. Successful surfaces remain unchanged. Timeouts, transport errors, HTTP 408/429/5xx and KIS `EGW00201` qualify; authentication and other business errors remain terminal. Exhaustion retains the provider error and partial snapshot. Shared ranking reads keep their separate collection behavior. The default follow-up can make optional degradation terminal for the daily chain. Disabled, non-trading-day and definitive no-eligible-symbol cases can be safe skips.
- **Trace** — `market/kis_intraday_snapshots.py`, `eod/pykrx_pipeline_runner.py::_terminalize_optional_intraday_snapshot_result` and `GET /broker/kis/intraday-snapshots`.

## 25. agent_feedback_summaries

- **Input and output** — Verified same-date candidate universe, technical/action context, required market-risk and PyKRX evidence, applicable factor rows and optional context. Persists per-role, per-symbol summaries in `agent_context_feedback` and parent/child progress in `agent_context_feedback_runs`.
- **Entry economics** — For a formed `long_bias` setup, compare the best observed entry and upside to the stored invalidation level before applying candidate caps. A role can exclude the setup when even gross reward/risk is below its floor. Report estimated non-spread costs and raw turnover with the reason. Missing or stale levels and breakouts without a new upside level stay eligible for analysis. Preserve protected watch and order scopes and explicit exact reruns.
- **Completion and blocking** — `agent_context/prerequisite_service.py` checks persisted prerequisites independently of the runner's stage label. The enrichment check also considers market breadth and market-level flows. The candidate selector then applies strategy-specific hard blocks, soft penalties and the candidate cap. Missing optional news, disclosures or a factor value must not create an all-run block. Missing mandatory risk proof can disqualify a symbol. For `requested_scope_has_no_eligible_symbols`, inspect whether the exact requested scope has complete risk evidence with zero eligible symbols or its eligibility could not be established. The scoped retry path treats the absence as a blocked market-risk prerequisite. A definitive empty candidate set requires no dispatch.
- **Batch and progression** — Attribute failures to the affected symbols when output identity is unambiguous. Missing optional improvement notes are independent of a usable summary. An ambiguous, unexpected or duplicate symbol identity is a response-contract failure. For first-run partial progression require terminal progress with completed symbols and zero pending/in-flight work. For resume require the exact completed-summarizer intersection and immutable cutoff/run identity, not a generic latest summary.
- **Trace** — `agent_context/eligibility.py`, `agent_context/prerequisite_service.py`, `agent_context/retry_service.py`, `eod/agent_chain_gates.py` and `GET /agent-context/feedback/progress`.

## 26. signal_decider

- **Input and output** — Proven summary cohort and knowledge cutoff. Runs the independent `signal-decider` and `short-term-decider` roles. Writes feedback run/items and `signal_decider_decision_snapshots`. Optional forecast and nullable supporting evidence can be absent.
- **Completion and blocking** — A valid decision can be wait, skip or another non-actionable outcome. Preserve role-specific completed scopes. Missing summaries, conflicting cutoff or scope, invalid decision payload, unresolved/in-flight children and actual gateway failures are distinct. A valid decision from one role must not be made conditional on the other role choosing the same symbol.
- **Trace** — `eod/agent_chain_gates.py`, `eod/summary_cutoff_contract.py`, `eod/daily_pipeline_stages.py` and `GET /agent-context/feedback/latest`. For resume inspect the completed-summary cutoff proof and the terminal completion evidence by role.

## 27. next_open_execution_plan

- **Input and output** — Stored decisions, role-specific completion proof, the target trading session and current planning policy. Materializes `signal_decider_execution_plan_items`.
- **Completion and blocking** — No eligible actionable decision can be a legitimate empty plan. A valid WATCH does not grant BUY authority by itself. Keep invalid role/source proof, a stale window, missing prices or authority for a specific action, risk rules and acknowledgement state distinct from whether the plan was materialized. A late-open recovery must meet its exact completion proof; a generic expired run must not be reclassified as authorized.
- **Trace** — `orders/next_open_execution.py::refresh_plan_items`, `eod/run_stage_support.py` and `GET /trading/orders/next-open-execution`. Distinguish plan materialization, Exec handoff acknowledgement, runtime monitoring and actual order eligibility. Do not send orders or change risk controls to verify a plan.

## 28. signal_decider_evaluations

- **Input and output** — Immutable signal-decider snapshots, subsequent prices and evaluation horizons. Writes `signal_decider_realized_outcomes` and `signal_decider_evaluations`.
- **Completion and blocking** — Pending maturity and unavailable realized-price evidence are tracked separately from a failed evaluation refresh. Explicit execution checks evaluation prerequisites and the signal-decider's own completed scope. This stage is maintenance outside the default daily route and must not block next-open plan creation.
- **Trace** — `agent_context/signal_decider_evaluation_maintenance_service.py`, the explicit stage in `eod/daily_pipeline_stages.py` and `GET /agent-context/signal-decider/evaluations`.
