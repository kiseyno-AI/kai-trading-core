# DART, factors and signal features

Source paths are under `apps/api/app/`. A factor output row can carry an empty score, rank or component and still prove that the symbol was processed. Inspect the nullable fields and reasons separately from the stage status, exact publication proof and retry debt. `nullable_output_completed` is only useful with the attempt's validated atomic proof. The consumer decides whether a particular strategy can use that symbol's available evidence.

The first-run factor group in `eod/pykrx_pipeline_runner.py` runs momentum, basic value, low-risk and beta over the verified price cohort. DART-dependent stages use the applicable DART/Piotroski cohort. An aggregate partial factor result can still provide its proven successful cohort for the signal-feature and feedback path. The explicit/resume executor in `eod/daily_pipeline_stages.py` applies dependency proofs and may narrow the symbol scope. Do not treat a smaller returned scope or an old stored row as evidence that an omitted symbol succeeded in this attempt.

For a mixed explicit/resumed scope, keep proven preferred-share Piotroski exemptions only for supported signal, news and feedback stages. A symbol still needs its own price and supported factor evidence. An isolated preferred-only scope does not produce a common-share factor proof. Diagnose that scope separately from an applicable symbol whose factor row contains a null score.

## 10. fundamentals_daily_refresh

- **Input and output** — Applicable KRX issuers, DART mappings, filing events and existing snapshots. Updates `dart_fundamentals_snapshots` and `dart_fundamentals_rows`. Output also records fresh, missing, deferred and retry-debt states.
- **Completion and blocking** — For a requested scope, count only verified preferred symbols in its terminal not-applicable partition as satisfying the DART dependency without DART rows. A valid no-new-filing or no-data result can finish with no new statement. Agent prerequisite checks accept zero captures when the entire requested DART scope is proven. Inspect mapping failures, actual provider/capture errors, retryable filing backlog and missing authority separately. A completed incremental check does not prove that usable annual statements exist.
- **Trace** — `dart/fundamentals_capture_service.py::run_daily_incremental_refresh` and `GET /disclosures/dart/fundamentals/daily/inspect`. To explain a financial gap, inspect filings and saved snapshots as of the pipeline cutoff, then check the newest chosen annual report, comparative columns and CFS/OFS selection. Distinguish no annual snapshots, fields absent in the selected report, and fields the parser missed.

## 11. piotroski_filter

- **Input and output** — Annual DART evidence for current/prior comparison, same-day PyKRX PBR, price momentum and optional issuance evidence. Writes `daily_piotroski_filter_results`. Verified preferred shares are out of scope.
- **Completion and blocking** — Missing accounting components can leave individual tests unknown. Eight financial checks and the optional issuance check have separate availability. Do not convert unknown into false or into a passing strict value screen. A strategy that requires a Piotroski pass can still exclude that symbol. Keep computation/write failures and missing ranking dependency proof distinct from financial coverage.
- **Trace** — `signals/piotroski_filter.py::rebuild_for_market_date`, `eod/run_stage_support.py` and `GET /eod/piotroski-filter-results`. Missing gross margin, current ratio, leverage or turnover may reflect a missing numerator/denominator in the current or comparative column. A report date alone does not establish evidence.

## 12. momentum_factors

- **Input and output** — Daily price bars. Writes `daily_momentum_factor_results` with 12-minus-1-month momentum, trend and overheat context.
- **Completion and blocking** — The main return uses 253 closes with a 21-session skip. A short valid series leaves the long return or related score null while shorter evidence can survive. Do not add a retry for known short history. Missing expected exact-date bars, calculation/write exceptions and scope-proof failures are different.
- **Trace** — `signals/momentum_factor.py` and `GET /eod/momentum-factors`.

## 13. value_factors

- **Input and output** — Exact-date prices and PyKRX valuation snapshot. Writes `daily_value_factor_results` with PER, PBR, dividend yield and score.
- **Completion and blocking** — Nonpositive PER or missing valuation fields can leave the score null on a published row. A provider PER of zero by itself does not identify the company's earnings. Keep the upstream valuation preflight and actual fetch/compute/write failures distinct.
- **Trace** — `signals/value_factor.py` and `GET /eod/value-factors`. Inspect both the raw valuation row and per-metric status.

## 14. low_risk_factors

- **Input and output** — Daily bars with 60/120-session volatility, 120-bar drawdown and 20-bar trading value. Writes `daily_low_risk_factor_results`.
- **Completion and blocking** — The 120-return window uses 121 closes. Missing historical observations can leave a null score. A successfully computed low rank or liquidity rejection is a strategy result. Preserve true calculation/write debt and exact date/identity checks.
- **Trace** — `signals/low_risk_factor.py` and `GET /eod/low-risk-factors`.

## 15. beta_factors

- **Input and output** — Symbol returns and equal-weight benchmark returns aligned by date for 60/120 sessions. Writes `daily_beta_factor_results`.
- **Completion and blocking** — Short aligned history or a non-computable beta can leave the metric null. No substitute benchmark or fabricated beta. Preserve actual dependency, computation, storage and scope errors.
- **Trace** — `signals/beta_factor.py` and `GET /eod/beta-factors`. Compare both the symbol and its benchmark history.

## 16. advanced_value_factors

- **Input and output** — Exact-date prices and market cap with selected annual DART inputs. Writes `daily_advanced_value_factor_results`. Uses free-cash-flow yield and EV/EBITDA.
- **Completion and blocking** — The combined score requires both components. Operating cash flow, CAPEX, D&A, positive EBITDA and valid EV are metric inputs. Unknown values remain unknown, including missing D&A. Distinguish a missing raw account from a recognized account excluded by the DB prefilter before matching. DART non-applicability, normal missing input and real compute/write debt are separate.
- **Trace** — `signals/advanced_value_factor.py` and `GET /eod/advanced-value-factors`. Check the selected annual raw rows and matcher predicates before assuming the issuer omitted an item.

## 17. unified_value_factors

- **Input and output** — Same-day prices and valuation plus annual DART components. Writes `daily_unified_value_factor_results`.
- **Completion and blocking** — The score requires valid PER, PBR, dividend yield, FCF yield and EV/EBITDA components. Carry available component data when the unified score is null. Audit upstream coverage or publication failures independently.
- **Trace** — `signals/unified_value_factor.py` and `GET /eod/unified-value-factors`.

## 18. quality_factors

- **Input and output** — Same-day prices and annual DART accounting fields. Writes `daily_quality_factor_results` with profitability, cash flow, leverage and accruals components.
- **Completion and blocking** — Available components can support a partial-quality score where the formula allows. A nonpositive net-income denominator suppresses the cash-flow/net-income component. Missing gross profit, assets or other statement fields can leave components or the score null. Distinguish partial data quality from actual stage failure.
- **Trace** — `signals/quality_factor.py` and `GET /eod/quality-factors`.

## 19. composite_factors

- **Input and output** — Published momentum, basic value, low-risk, beta and advanced-value outputs. Writes `daily_composite_factor_results`.
- **Completion and blocking** — A score requires at least three of the five inputs, at least 60% of base weight, and at least one value input. Null component outputs can legitimately leave the composite score null. A dependency that failed to publish its authoritative attempt still needs separate handling. Never infer a missing input as zero or use an unverified old row to complete the dependency.
- **Trace** — `signals/composite_factor.py` and `GET /eod/composite-factors`. Use the available-factor count and valuation-leg statuses to explain a missing score.

## 20. signal_features

- **Input and output** — KIS volume power, overtime volume and overtime fluctuation rankings plus exact-date price, risk and available factor context. Publishes `daily_signal_features` and `daily_market_participation_contexts`.
- **Completion and blocking** — Same-date captured overtime evidence is preferred before the live ranking request. A well-formed empty list or valid-symbol overtime-volume zero is observed absence. Publish null ranking fields and `no_signal` quality when no same-date direct or owned fallback evidence supplies a value. A proven publication can be complete without semantic signal readiness. An observed no-signal response must have a real collection timestamp on the requested KRX date. Provider errors, malformed rows, pagination truncation, stale/unproven fallbacks and failed writes remain distinct. Bind the proof to the actual invocation, including preferred shares where their supported inputs proved them.
- **Trace** — `signals/daily_signal_features.py`, `signals/overtime_ranking_capture.py`, both EOD executors and `GET /eod/signal-features`. Compare `source_health`, `source_errors`, `incomplete_sources` and the exact successful/retry scope instead of using the count of ranking rows as the symbol publication count.
