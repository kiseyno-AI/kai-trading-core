# Market data and risk

The source paths in this reference are under `apps/api/app/`. The numbered order is the canonical catalog order. For a stage's current API and dependency declaration, inspect `eod/daily_pipeline_stages.py::STAGE_DEFINITIONS`.

## 1. master_refresh

- **Input and output** — KIS market master when due. Updates the symbol master in `symbols`.
- **Completion and blocking** — A completed import or a verified already-current master can supply the next stage. A failed import or stale/missing authoritative master is a scope problem. Do not treat a symbol's optional display field as failed price evidence.
- **Trace** — `eod/daily_pipeline_stages.py` dispatches the stage. Inspect `GET /eod/status` for master refresh state.

## 2. pykrx_universe_refresh

- **Input and output** — Requested markets and current listing/master data. Refreshes active `symbols` for PyKRX collection. An explicit symbol limit is an operator scope and does not prove full-market coverage.
- **Completion and blocking** — Compare requested markets against the authoritative returned membership. An empty or incomplete required market is a real admission block. The historical path requires point-in-time membership rather than a current-universe shortcut.
- **Trace** — `eod/pykrx_backfill.py::refresh_recent_universe` and the membership logic in `eod/market_membership.py`. Check the collection run's recorded authority and membership hash.

## 3. price_collection

- **Input and output** — Date-bound symbol membership and PyKRX regular-session per-symbol OHLCV. Publishes `daily_price_bars` and a linked `eod_collection_runs` outcome.
- **Completion and blocking** — The exact-date persisted bar is required for the price-backed symbol scope. Missing/invalid date bars and write errors are real retry debt. Preserve exact successful symbols when partial progression is authorized. An unexpected symbol, wrong date or missing market membership proof cannot substitute for a valid bar.
- **Trace** — `eod/pykrx_pipeline_runner.py::run_market_data_pipeline` and `eod/pykrx_backfill.py`. Inspect `GET /eod/pipeline/status` and the price/downstream retry partition on the linked run.

## 4. pykrx_enrichment

- **Input and output** — PyKRX enrichment for the exact date and proven market scope. Writes `daily_pykrx_enrichment_rows` for sector classification, investor flows, short selling, valuation and other context.
- **Completion and blocking** — A provider/category result and field completeness are separate. Optional short-balance gaps can be warnings even if the remaining short-selling evidence is usable. The persisted structural preflight checks direct same-date decision-usable evidence for sector classification, per-symbol investor flow, short selling and valuation. Current readiness floors are 95% for investor flow and valuation and 99% for the other required categories. Failed, waiting or unverified required categories can stop the agent or plan path independently of the stage label.
- **Trace** — `eod/pykrx_enrichment.py`, `eod/pykrx_enrichment_gate.py` and `eod/pykrx_enrichment_coverage_policy.py`. Inspect `GET /eod/pykrx/enrichment` and category preflight evidence. Distinguish known field absence from a missing publication or provider failure.

## 5. technical_snapshots

- **Input and output** — Exact-date stored prices plus available lookback bars. Rebuilds `daily_price_contexts` and `daily_technical_snapshots` with moving averages and technical indicators.
- **Completion and blocking** — A published row with a nullable long-window indicator can be a success. An absent bar, row computation/storage failure or invalid scope proof is debt. The combined stage succeeds for the intersection of its price-context and technical-snapshot publications.
- **Trace** — `agent_context/derived_contexts.py`, `eod/technical_snapshots.py` and the `technical_snapshots` dispatch in `eod/daily_pipeline_stages.py`. Inspect `GET /eod/technical-snapshots`.

## 6. action_summaries

- **Input and output** — Published technical snapshots and price-derived context. Writes `daily_action_summaries`.
- **Completion and blocking** — Optional null indicators can remain absent in the output. A missing mandatory technical snapshot, compute/write error or invalid publication proof leaves the affected symbol unresolved.
- **Trace** — `eod/action_summaries.py`, the `action_summaries` dispatch in `eod/daily_pipeline_stages.py` and the paired caller in `eod/pykrx_pipeline_runner.py`. Inspect `GET /eod/action-summaries`.

## 7. trending_ranks

- **Input and output** — Exact-date price evidence and verified ranking universe. Writes period-specific `daily_trending_ranks`.
- **Completion and blocking** — Global ranking needs the required membership and input proof for that computation. An empty or unavailable ranking period is distinct from an individual symbol missing a long-window indicator. The daily path preserves authoritative ranks while upstream technical retry debt makes a replacement unsafe. A specifically authorized partial-price recovery must prove its exact admitted subset.
- **Trace** — `trending_ranks` dispatch in `eod/daily_pipeline_stages.py`, `eod/pykrx_pipeline_runner.py` and `eod/exact_trending_repair.py`. Inspect `GET /scanner/trending-stocks` for the date and period.

## 8. market_risk_filter

- **Input and output** — KIS risk snapshots and stored recent trading context. Publishes `daily_market_risk_filter_results` with per-symbol passed, watch and rejected evidence.
- **Completion and blocking** — A persisted rejection is a successful evaluation with a negative outcome. The consumer may exclude the symbol for proven safety reasons such as a halt. Missing mandatory snapshot proof or fetch/compute/write error is symbol debt. Keep the structural stage gate distinct from downstream eligibility.
- **Trace** — `signals/market_risk_filter.py::rebuild_for_market_date`, `signals/market_risk_evaluators.py` and `agent_context/eligibility.py`. Inspect `GET /eod/market-risk-filters`.

## 9. market_risk_snapshots

- **Input and output** — Required KIS quote/risk evidence for the exact scope. Writes `daily_market_risk_snapshots`. This is an explicit stage. The default risk filter collects its snapshots itself.
- **Completion and blocking** — Missing credentials, provider errors, unverifiable date/identity and absent required risk snapshots are not equivalent to the optional KIS intraday feeds. Retain the real missing-symbol scope.
- **Trace** — `signals/market_risk_snapshot.py::collect_for_market_date` and `GET /eod/market-risk-snapshots`.
