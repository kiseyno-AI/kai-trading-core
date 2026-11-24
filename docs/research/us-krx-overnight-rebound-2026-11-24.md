# US overnight information and KRX preopen decision revisions

Date: 2026-11-24. Status: research and implementation plan, not implemented or calibrated. Source baseline: Core `a7fa8718c35d4630e50ec8610e4525d0510ff445`; Exec `b3e0254d619e4366a7200d0f76117ab2f646658b`.

The latest policy is an **optional additive supplement**, specified in [Optional US-market supplement design](../PREOPEN_MARKET_REVISION_DESIGN.md). Existing daily/monitoring/execution never waits for it; the earlier mandatory preopen-selection/08:00 baseline-fence proposal is superseded. Only independently successful new candidates can be added, without replacing executable baseline families. Shared-account real/uncertain broker exposure remains subject to existing safety.

## Expected behavior

Before each KRX session, optionally analyze information published since previous EOD decisions, including the completed US session. Estimate the Korean opening response separately from the return available after an attainable entry. Fresh optional lane decisions may add opportunities where no executable baseline family exists; original publications/plans remain independent. Price-touch processing uses a persisted decision and deterministic conditions, without another agent call.

A strong US rebound should be available to the deciders as timestamped evidence. It must not remain hidden behind an authoritative previous-session defensive label. A market forecast does not itself choose a stock, create a WAIT, or authorize a broker order.

This plan does not lower the existing six-point threshold, add a fixed confidence bonus, force a minimum number of buys, or change account risk limits. It proposes a new use of market evidence; it does not approve or deploy new execution behavior.

## Current behavior and the missing connection

| Finding | Current source | Design implication |
| --- | --- | --- |
| Historical market context deliberately excludes current-session fields and rejects a snapshot from another date. | [signal_decider_market_context.py:85,122,137](../../apps/api/app/agent_context/signal_decider_market_context.py#L85), [tests](../../apps/api/tests/test_signal_decider_market_context.py#L138) | Keep historical decisions intact. Create a new target-session revision with a later cutoff. |
| PREOPEN and OPEN already refresh the market snapshot and next-open plans. | [runner.py:787,845,869,929](../../apps/api/app/eod/runner.py#L787) | Add the missing revision step to existing scheduling; do not build another always-running polling loop. |
| Full and compact prompts treat `market_regime_context` as authoritative; defensive/risk-off defaults to WAIT/REJECT. | [orchestrator.py:2504,2539](../../apps/api/app/agent_context/orchestrator.py#L2504), [payload blocks](../../apps/api/app/agent_context/orchestrator_helpers.py#L2939) | Appending bullish text while retaining conflicting old authority is insufficient. Define target-session evidence and precedence explicitly. |
| Current live-market authority applies during OPEN and can veto/release execution, but does not create a new decider verdict. Nonactionable snapshots remain nonactionable. | [next_open_execution.py:1178,1211,2143,4987](../../apps/api/app/orders/next_open_execution.py#L1178) | A market refresh alone cannot turn yesterday's REJECT into a new opportunity. A fresh lane verdict is necessary. |
| US-listed ETF proxies already exist: SPY, QQQ, SOXX, EWY, TLT, UUP, USO and GLD. They use KIS overseas quotes, not Korea-listed ETF quotes, and are read at collection time rather than certified as completed-session closes. Core has no wrapper for KIS's separate overseas-index chart API. | [market_inputs.py:27,254,300](../../apps/api/app/side_agents/market_inputs.py#L27), [KIS overseas quote/daily adapters](../../apps/api/app/brokers/kis/client.py#L862), [official index API](https://github.com/koreainvestment/open-trading-api/blob/main/examples_llm/overseas_stock/inquire_daily_chartprice/inquire_daily_chartprice.py) | Verify KIS direct index data first. Prefer it if the required indices and latest completed US session arrive before the cutoff; keep independent providers as a contingency. Actual preopen freshness and exact index coverage remain unverified. |
| Pre-decider market-risk filters concern symbol safety, including halts and management/disclosure warnings, rather than broad index breadth. | [market_risk_evaluators.py:102](../../apps/api/app/signals/market_risk_evaluators.py#L102), [eligibility.py:433](../../apps/api/app/agent_context/eligibility.py#L433) | Preserve verified safety filters. The stale market judgment must be corrected at the decision context, not by bypassing symbol safety. |
| REJECT lacks a structured rejection category; its `wait_reason` must be null. | [response_contract.py:729,880](../../apps/api/app/agent_context/response_contract.py#L729) | Do not discover a market-only rejection by searching prose. Add structured provenance for future results and define a bounded legacy policy. |
| Durable EOD fingerprints intentionally exclude transient open-drive/quote changes. TimesFM reuses unchanged stored-price inputs. | [orchestrator_helpers.py:641,920](../../apps/api/app/agent_context/orchestrator_helpers.py#L641), [forecast service:103,215](../../apps/api/app/forecast_context/service.py#L103) | Preserve this reuse. Give a new revision its own forecast/context identity instead of invalidating all summaries and forecasts. |

Exec already refreshes Core market context and can release a prior-session market fence. These are execution-time checks, not a replacement for a Core verdict: `apps/api/app/services/live_market_context.py:68,108,728,811` and `plan_judgment_actions.py:457` in the Exec checkout. Core's shared open-drive builder already validates timestamps, session, continuation and turnover: [read_service.py:2798,3435](../../apps/api/app/agent_context/read_service.py#L2798). Its aggressive sizing remains uncalibrated/shadow-only; this proposal does not activate it.

The November 24 incident illustrates the distinction: the recovered decisions retained a November 23 21:56 KST cutoff and November 23 defensive breadth. Running that recovery on November 24 did not make its information current. Retrofitting November 24's rally into the old cutoff would corrupt the record rather than fix the forecast.

## Research basis and limits

1. Park and Yi, *Mispricing of US Shocks in the Korean Stock Market* (2011), report that Korean openings follow overnight US performance, while subsequent intraday returns in their sample are negatively related to the US move. This supports separate gap and intraday targets, not automatic buying after a large US rise. [Publisher abstract](https://onlinelibrary.wiley.com/doi/10.1111/j.2041-6156.2011.01042.x).
2. Kwon et al., *Who Overreacts to Overnight News?* (2015), also examine Korean opening overreaction and daytime reversal after large US moves. The papers differ on the investor mechanism; neither supplies a present-day trading guarantee. Both Wiley findings here are limited to their accessible abstracts, not independently reproduced full-paper estimates. [Publisher abstract](https://onlinelibrary.wiley.com/doi/10.1111/ajfs.12090).
3. Lee and Yu, BOK Working Paper 2018-2, study industry-level international comovement and trade linkages. This supports testing different sector sensitivities and regime stability, but its weekly industry evidence cannot establish next-morning individual-stock alpha. [Bank of Korea original research](https://www.bok.or.kr/portal/bbs/P0002456/view.do?depth=201670&menuNo=201670&nttId=234611&oldMenuNo=201150&programType=newsData&relate=Y).
4. Kim and Rogers (1995) document historical spillovers concentrated at Korean opening prices. Wongswan (2003) studies information transmission through volatility and volume. These motivate separate opening/risk targets; volatility transmission is not evidence of a positive expected return. [Fed IFDP 499](https://www.federalreserve.gov/pubs/ifdp/1995/499/default.htm), [Fed IFDP 759](https://www.federalreserve.gov/pubs/ifdp/2003/759/default.htm). Both datasets predate today's market structure.

Everything below is a proposed system design. No hit rate, return uplift, probability calibration or net trading edge has been measured for it yet.

## Forecast targets: keep three questions separate

For target KRX session `t`, let `C_prev` be the previous KRX-session close, `O_t` the target-session open and `C_t` its close. Produce separate KOSPI and KOSDAQ forecasts:

| Question | Outcome used in training | Why it matters |
| --- | --- | --- |
| Will Korea open higher? | `gap = O_t / C_prev - 1`; `P(gap > 0)` | Measures overnight transmission, including a rebound already realized at the open. |
| Will Korea finish above yesterday? | `day = C_t / C_prev - 1`; `P(day > 0)` | Describes the market day, without implying an executable entry at yesterday's close. |
| Is there an opportunity after entry? | `intraday = C_t / O_t - 1`, plus realized return from the specified attainable entry, after costs | Distinguishes a tradable continuation/pullback from an expensive opening gap. |

The first two are market context. The third must ultimately be tested against each lane's own entry, chase ceiling, stop, target, fill probability and horizon. An index forecast is not a stock-specific success probability. Existing lane-thesis confidence and these empirically calibrated probabilities remain distinct fields; never add them together or rescale one to clear the six-point gate.

For stock selection, estimate market/sector sensitivity using only training-period history, shrinking sparse estimates toward the relevant market. Evaluate short-term entry timing and 2–3-session outcomes separately from the signal lane's stated swing horizon. Preserve the current role contracts; changing their holding periods is outside this plan.

## Inputs and data availability

Start with a small shared snapshot rather than fetching foreign data separately for every stock:

- Prefer completed-session S&P 500, a clearly identified Nasdaq index and a semiconductor index through KIS where the official master and authenticated response prove support. The existing SPY, QQQ, SOXX and EWY basket is a separately labeled proxy alternative: QQQ is not Nasdaq Composite, and SOXX is not the SOX index. Never mix an index close with an ETF close to calculate a return or switch model inputs without matching training/feature identity.
- Previous KRX-session return, trend, volatility, breadth and sector-relative strength, already held by Core. Previous-session breadth remains a dated observation, not a current-session fact.
- Use QQQ relative to SPY and SOXX relative to QQQ as candidate residual/sector features. US benchmarks overlap; do not count the same technology rally as three independent votes. EWY also reflects Korean and currency information; test its incremental contribution rather than calling it a pure US shock.
- Where synchronized OHLC history is available, separate US open-to-close and prior-close-to-open returns. US close-to-close returns include information from the already-completed Korean daytime session. Conditioning on the previous KRX return and testing the US-session components helps avoid calling already-known Korean information a novel US effect.
- VIX change, USD/KRW or a documented proxy, and KRX night futures are optional challenger features only after usable preopen access and historical timestamps are established. A stale fixing or FX proxy must keep its own label; it cannot masquerade as live USD/KRW.

Every observation needs `provider`, `instrument`, `session_date`, `observed_at`, `available_at` or a clearly labeled availability assumption, and `fetched_at`. A daily economic date alone cannot prove that a value was available before the KRX open. Training and online inference must use the same session definitions, return adjustments and input availability policy.

Align `Asia/Seoul` KRX sessions and `America/New_York` US sessions using their calendars, including DST, US-only holidays, KRX holidays and accumulated moves across multiple US sessions during a Korean closure. Select data available strictly before the decision cutoff. Never shift a daily series by one calendar day and assume this solves alignment. Historical datasets with unprovable publication timing can be exploratory only until a conservative availability rule or archived snapshots makes the evaluation defensible.

Validate KIS direct overseas-index data first, as requested in the latest steering. It becomes the primary source only after proving required instrument coverage, the latest completed US session and before-cutoff receipt. Reuse Core's existing KIS HTTP client and context/provenance conventions. Keep the independently documented external alternatives below as contingencies if that validation fails, without creating a new account or paid dependency in this planning task. Use durable once-per-session shared collection, bounded retries, minimum refresh intervals and existing request budgets; no extra WebSocket registrations or per-stock foreign requests are needed. Read-only UI requests must not train models or trigger provider collection.

### KIS direct-index acceptance

The official KIS example provides `/uapi/overseas-price/v1/quotations/inquire-daily-chartprice`, TR `FHKST03030100`, with `FID_COND_MRKT_DIV_CODE=N` for overseas indices, `FID_PERIOD_DIV_CODE=D` for daily bars, an index code and a requested date range. It demonstrates `.DJI` and exposes dated OHLC fields. Resolve desired S&P/Nasdaq/semiconductor codes from the official index master rather than guessing that ETF support establishes index support. [Official request](https://github.com/koreainvestment/open-trading-api/blob/main/examples_llm/overseas_stock/inquire_daily_chartprice/inquire_daily_chartprice.py), [official master helper](https://github.com/koreainvestment/open-trading-api/blob/main/stocks_info/overseas_index_code.py), [response fields](https://github.com/koreainvestment/open-trading-api/blob/main/examples_llm/overseas_stock/inquire_daily_chartprice/chk_inquire_daily_chartprice.py).

Add a thin index-data wrapper to the existing client only during implementation. Validate the master identity and latest expected completed US-session row at the actual preopen collection time; a successful HTTP response, a current headline quote, or a newly fetched old bar is insufficient. Exclude any unfinished current-US-session bar. Persist historical bars for return/volatility calculation and fetch only incremental data on following days, shared by all domestic stocks. No authenticated index request or preopen availability test has been performed yet.

### External API contingencies and before-open acceptance

The desired input on Korean November 24 morning is the completed **US November 23** session. Its US date being yesterday does not imply it is stale. A Korea-listed US-index ETF is a different instrument, traded during Korean hours; do not substitute it for the actual US-listed ETF's completed session.

| Candidate | Officially documented behavior | Planned use |
| --- | --- | --- |
| [Alpaca historical bars](https://docs.alpaca.markets/us/reference/stockbars), [feed/access FAQ](https://docs.alpaca.markets/us/docs/market-data-faq) | Historical queries support explicit `feed=sip`. Without a real-time SIP subscription, the query `end` must be at least 15 minutes old. This is distinct from subscription-gated SIP latest/snapshot endpoints. | First independent alternative if KIS fails validation: completed-session US-stock/ETF proxies after the delay has elapsed. Authentication/account eligibility and actual timing still need proof; no account/subscription has been created. It does not by itself prove access to the desired cash indices. |
| [Alpha Vantage daily/quote documentation](https://www.alphavantage.co/documentation/) | Daily OHLCV has explicit session dates; compact daily history contains 100 observations. The default quote updates at EOD, but the documentation does not promise a pre-KRX delivery deadline. | Conditional fallback candidate only after measured before-open delivery; do not equate the EOD label with a guaranteed publication time. |
| [Massive Stocks Basic release timing](https://massive.com/knowledge-base/article/on-the-free-basic-plan-when-can-i-pull-the-previous-trading-days-data) | Free Basic serves a trading day's bars only after midnight ET, not immediately after the US close. | Unsuitable as the assumed free preopen source: midnight ET is approximately 13:00/14:00 KST, after the KRX opening. A different paid entitlement would need separate evaluation. |
| [Twelve Data US coverage/release timing](https://support.twelvedata.com/en/articles/9935903-us-equities-market-data) | Consolidated historical/EOD data is released after midnight ET on the next trading day. Its default live feed represents approximately 5% of US trading volume. | Do not substitute default partial-venue data for consolidated EOD, or assume the consolidated EOD release meets this cutoff. |

For Alpaca, request the four symbols together on `/v2/stocks/bars`, with explicit `feed=sip`, `timeframe=1Day`, exchange-calendar-derived bounds and a fixed adjustment policy. Check pagination and coverage of **each** required symbol, not just a successful response or the first returned symbol. Keep both comparison sessions on the same feed/provider. Do not use a default feed that silently selects IEX.

The provider's daily OHLC is a trade-condition-filtered aggregate, not a guaranteed official primary-exchange closing-auction print. Its daily timestamp labels the New York session rather than publication time. Extended-hours trades can contribute daily volume without changing daily OHLC under its documented rules. Do not use final full-day volume/VWAP in a historical preopen forecast when that includes after-hours activity not yet observed at the cutoff. Archive the actual preopen response and retain corrections separately; a historical query's date bounds do not reconstruct its earlier publication state. [Aggregation rules](https://docs.alpaca.markets/us/docs/market-data-faq).

Acceptance is provider-independent: derive the last expected completed US regular session, require that session and the matching prior session for every core input, verify instrument/currency/feed/adjustment and strictly positive finite usable prices, and record receipt before the Korean decision cutoff. Zero/sentinel or invalid prices make the input unavailable, never a return denominator or a synthetic -100% event. Treat regular-session and after-hours features separately. A successful HTTP response with an older US session is `stale`, not ready. On missing timely data, try only the bounded independently verified fallback; if none qualifies, leave the new forecast unavailable, never reuse an older session as a fresh rebound signal or force zero returns.

An illustrative winter schedule is: US regular close at 06:00 KST, primary collection around 07:00, bounded retry before the frozen decision cutoff, and verified revised verdict publication before 08:40 for an ordinary 09:00 KRX opening. Derive actual times from both exchange calendars/DST and measured dispatch latency. A 15-minute market-data delay need not prevent this use; an EOD provider that waits until US midnight does.

Verified source constraints:

| Source | What the official source establishes | Planning consequence |
| --- | --- | --- |
| [KIS official examples](https://github.com/koreainvestment/open-trading-api/tree/main/examples_llm/overseas_stock) | Quote/daily-price request examples exist. Examples are not a guarantee of account entitlement, historic depth, publication time or redistribution rights. | Reuse the existing adapter and validate session fields/coverage; do not probe subscription limits or use the portal browser. |
| [FRED NASDAQCOM](https://fred.stlouisfed.org/series/NASDAQCOM) | A daily observation has an update timestamp separate from its observation date. A checked example showed an Updated timestamp after the following Korean open; this establishes neither first availability nor a universal schedule. | An economic-date shift is insufficient for a preopen backtest. Archive actual availability or use a conservative verified rule. |
| [FRED DEXKOUS](https://fred.stlouisfed.org/series/DEXKOUS), [Fed H.10](https://www.federalreserve.gov/Releases/H10/) | DEXKOUS is a New York noon buying rate; H.10 releases prior-week daily observations weekly. | It cannot stand in for a fresh overnight USD/KRW quote. |
| [Cboe VIX historical data](https://www.cboe.com/tradable_products/vix/vix_historical_data) | Daily history exists; this page is not a guaranteed pre-KRX delivery SLA. | Optional until collection timing is verified; VIX index and futures retain different identities. |
| [KRX night-session guide](https://open.krx.co.kr/contents/OPN/01/01041401/Guide_to_Night_Session_in_KRX_Derivatives_Market.pdf) | The guide documents the 2025 night-session transition, 18:00–06:00 night trading, 08:45 regular futures opening, and next-business-day assignment of night transactions. | Use exchange-session identity, not just the wall-clock date; verify current calendars and data access when implementing. |

## Relative-shock algorithm specification

Use price **changes**, not an index's raw point level, to detect the new event. Let `t` be the latest completed US session. Express return fractions internally, and convert to percent or percentage points only in explicitly named presentation fields:

| Feature | Definition | Interpretation |
| --- | --- | --- |
| `return_1d` | `r_t = C_t / C_(t-1) - 1` | Direction and size relative to the previous completed US close. |
| `return_jump_pp` | `100 * (r_t - r_(t-1))` | Change in the daily return, in percentage points. Never divide by yesterday's return, which may be zero or negative. |
| `return_shock_z` | `(r_t - mean(r_(t-N), ..., r_(t-1))) / sd(r_(t-N), ..., r_(t-1))` | How unusual this move is compared with prior daily volatility; exclude the event session from the reference window. |
| `return_percentile` | Empirical rank of `r_t` against earlier returns in a prespecified trailing window | A distribution-based cross-check without assuming Gaussian returns. |
| `prior_selloff` | Prior 3/5-session cumulative return and trailing drawdown, ending at `t-1` | Distinguishes recovery after a selloff from acceleration in an existing rise. Include dated prior-KRX selloff separately. |
| `leadership` | Broad-market confirmation and technology/semiconductor relative returns | Distinguishes a broad shock from a narrow sector event without counting overlapping indices as independent votes. |

Use a prespecified candidate volatility window such as 60 prior sessions and compare it with alternatives only inside training folds. A longer historical percentile window can check tail sensitivity if enough data exists. Zero/non-finite volatility or insufficient history makes the standardized feature unavailable, not infinity or a synthetic zero. Returns, acceleration, z-score and percentile describe overlapping information and do not earn four separate confidence bonuses.

Examples are arithmetic illustrations, not fitted triggers:

- A +2% move with prior mean near zero and daily volatility 0.5% is about +4 standard deviations; with 2% daily volatility it is about +1. The same daily rise carries different surprise.
- A move from yesterday's -2% return to today's +3% is a +5 percentage-point improvement, while today's price gain is still +3%.
- A move from -6% to -1% also improves by +5 percentage points but remains a falling market. Positive acceleration alone must never label this a positive rebound.

Describe the event before predicting an outcome: `positive_shock_after_selloff`, `positive_shock_in_uptrend`, `ordinary_positive`, `negative_but_improving`, or `negative_shock`. A positive rebound needs positive realized US return as well as evidence of an unusual rise and prior weakness; a normal uptrend need not be misclassified as no opportunity merely because acceleration is small. These are auditable contextual categories, not buy/reject gates. Preserve the continuous features alongside any category, and fit category thresholds in training rather than declaring a fixed +2% or z=2 rule universally correct.

Estimate KRX gap/day/post-entry outcomes **conditional on** this feature vector and the previous Korean market/sector state. Use a shrinkage event-frequency baseline first, with a small regularized model as an offline challenger. Validate whether shock and acceleration actually improve on raw US return alone. Do not force coefficients positive, attach an arbitrary bullish confidence bonus, interpret a z-score as a probability, or train on only the winning rebound examples. Retain adverse events and ordinary sessions so the system can recognize failed rebounds and downside shocks.

The initial production use, after validation, is a structured reason for fresh preopen review of affected lane candidates. It does not automatically reverse REJECT into WAIT. Each decider receives the same dated market features and its own horizon/stock evidence; the short-term lane emphasizes attainable post-gap entry and liquidity, while the signal lane evaluates its longer thesis independently. A broad positive forecast with no remaining post-entry edge remains insufficient for a new trade.

## Smallest forecasting method worth comparing

1. Establish baselines: unconditional historical frequencies, the previous-KRX-only model, and a simple US-return-sign rule. Preserve the existing end-to-end strategy as the execution baseline.
2. Build an interpretable conditional-event table: US move standardized by trailing volatility, previous KRX drawdown and broad/technology leadership. Report event counts and intervals; shrink thin cells toward the broader training-sample rate. Use only a few prespecified bins, selected in training, rather than many hand-tuned rebound rules.
3. Compare a small regularized regression/classifier offline using the existing analytical stack, if supported without adding production dependencies. Estimate weights from training data, including interactions for prior selloff and volatility; do not assign arbitrary US/technical/fundamental percentages. Prefer the simpler model unless out-of-sample evidence justifies the challenger.
4. Store a versioned model with its training end, feature specification, availability policy and validation result. Emit `not_calibrated`/`insufficient_support` and null probabilities when evidence cannot support a usable estimate. Do not print an apparently precise 80–90% probability because the current event looks obvious.

Model fitting is an offline research job, never a price-touch operation. Daily inference is a small deterministic calculation over the persisted shared snapshot. Optional features missing by design must not create synthetic zero returns or fail the entire EOD process.

## Decision flow and ownership

### Previous evening: preserve the original EOD analysis

Run the current daily collection, summaries and independent deciders. Retain original `(source_market_date, symbol, agent_name)` identities and frozen source evidence. Record explicit market-dependent versus symbol-specific blockers for new decisions, including the evidence paths and timestamps. Add this response-contract metadata compatibly; an absent legacy field is `unknown`, not a successful classification.

The market forecast must be supplied as structured evidence to both full and compact prompt paths. Short-term reasoning should emphasize attainable entry, liquidity, sector transmission, remaining upside and reversal risk. Signal reasoning should emphasize whether the overnight move changes its catalyst/valuation/thesis over the stated horizon. Neither lane borrows the other's score, price levels or permission.

### Target-session preopen: optional additive decision with a new cutoff

Use an independent bounded task scheduled by the existing runner, never a required daily stage or buy-authority-critical step. Give it a persisted job identity per target session, model/input hash and lane. Freeze the cutoff/hash; retries reuse them, and changing only `now`/`fetched_at` creates no new decision. Schedule after actual US-session completion/availability, around open-minus-120 minutes with optional publication by open-minus-20 minutes. Validate queue/response capacity first. Original 08:00 dispatch does not wait; failed or late optional work is skipped.

1. Collect or reuse the shared overnight snapshot; compute and persist the forecasts.
2. Resolve the original prepared decider cohorts and their immutable lineage. Do not restrict reconsideration to yesterday's WAITs: that cannot recover a market-driven REJECT. Do not introduce a new unexplained top-N cutoff.
3. For new-format results, select lane decisions whose recorded market dependency materially changed, including qualifying rejected/chase-avoiding/observation-only candidates. Exclude executable or potentially executable original families from supplementary entry publication; preserve hard symbol-safety exclusions. For legacy results without classification, use the proven original eligible cohort within the optional budget, or skip unavailable scope. Do not infer safety/rejection type from prose.
4. Reuse source-valid summaries, stored factors, fundamentals and unchanged TimesFM evidence. Create a **new** preopen decision request: original EOD source date/cutoff plus target KRX date, new decision cutoff and the new overnight snapshot. Revalidate freshness requirements per evidence type. Refresh only a specifically invalid/missing input, not the entire daily suffix.
5. Supply the old breadth under a dated prior-session field, and the overnight estimate under a target-session **forecast** field. Compute a source-attributed target-session assessment once; do not leave old `risk_off` as an unqualified higher-priority instruction beside bullish new evidence. A forecast must never be labeled observed breadth, observed open-drive continuation or a passed execution safety gate. Fresh verified hard safety evidence remains authoritative.
6. Dispatch only this new bounded set to the relevant decider roles, ahead of the entry window. Preserve original decisions and append new revisions; do not mutate historical context or reuse an old verdict merely because its EOD fingerprint matches. Revision identity includes original source proof, target session, role, forecast/input hash, response-contract version and later cutoff.
7. Validate and persist the optional response in isolated storage. Only a timely successful eligible verdict can publish a real namespaced snapshot/plan, with no executable original same-slot family, no conflicting exposure and demonstrated residual cash/risk/feed capacity after original entitlements. Use the existing Core sizing/handoff owners without changing baseline quantities. Forecast odds never synthesize approval.

An optional failure never delays original work or another verified optional lane. Persist exact supplementary debt and resume only that debt. Do not retire, reprice, resize or supersede an original family to make room. If original authority/exposure wins a race, deny only the supplement. Baseline replacement is outside this version because failure after revocation could interrupt the original process.

Keep supplementary feedback, failure/retry debt and run/cohort proof separate from ordinary upserts/resolvers. Current latest-snapshot selection can replace authority: [next_open_execution.py:6625,6973](../../apps/api/app/orders/next_open_execution.py#L6625). Successful supplementary publication also needs a real explicit namespace in snapshot/plan uniqueness, queries, row maps, fingerprints, sizing, dispatch, monitoring and recovery. Original selectors retain baseline-only scope; supplementary paths load exact IDs. Shadow/late/failed records never reach authority, and a supplementary REJECT never creates a baseline tombstone or veto.

The new optional forecast/revision job needs independent completion evidence. It must neither mark a completed EOD stage failed nor manufacture success when provider/model/dispatch work failed. Distinguish `no_new_information`, `not_applicable`, `unavailable`, `partial`, `failed`, and verified completion using the repository's outcome conventions. A missed deadline cannot authorize a late next-open buy or silently extend an expired trading window. Keep an existing valid decision only under its own still-valid execution contract; a failed revision does not create a new allowance.

### Market open and price touch: deterministic, bounded work

Reuse current-session price/liquidity/risk checks and the existing live-market snapshot/open-drive logic. A candidate may require numeric, predeclared conditions such as a maximum opening gap, a minimum remaining net edge, price within its chase ceiling, and freshness. Some setups can enter immediately when their verified conditions hold; opening-range continuation conditions can mature later. Do not impose one arbitrary waiting period on every setup.

Where a future candidate requires a new condition not supported by Exec, first add a small typed condition and producer/consumer contract tests; free-text `trigger_conditions` remains explanation, not executable code or another LLM gate. No new slow agent/provider round trip belongs inside the serialized broker boundary. Use versioned cached evidence within existing freshness limits.

Any positive overnight estimate can be invalidated by current adverse evidence. Current halts, account limits, symbol risks, stale mandatory execution inputs and existing circuit breakers still apply. Conversely, a previous-session defensive assessment alone is not current-session market evidence.

Preserve independent `(session, symbol, agent)` lanes. One lane's WAIT is sufficient to request that lane's observation; REJECT/AVOID_CHASE must not be forwarded as actionable monitoring entries. Both eligible lanes may independently hold the same symbol under their own prices and sizing; one lane's rejection cannot veto the other.

### Later optimization, only if measured preopen latency requires it

An EOD decider could prepare an explicitly conditional rebound branch with its own complete prices and typed numeric conditions. The next morning would select only a branch already approved by that lane, after current checks. This could avoid most preopen LLM calls, but it needs a new conditional-plan contract and separate replay validation. Do not implement it in the first slice or treat an ordinary REJECT as an implicit conditional buy.

## Verification and acceptance

### Data and forecast checks

- Match US and KRX session dates across DST changes, US-only holidays, Korean multi-day closures and special opening hours. Reject future observations and observations delivered after the decision cutoff.
- Preserve proxy identities and price adjustments. Verify US-session completion rather than trusting a successful HTTP response. Missing data produces nulls and a reason; confirmed zero is distinct.
- Reconstruct at least several years of trustworthy synchronized market history where accessible. Train on earlier periods, validate/tune on subsequent periods, then score untouched later periods. Refit only with labels already observable at each forecast cutoff.
- Use point-in-time universe/sector membership, including delisted and then-eligible stocks where reconstructible. The existing signal-lab loader's current-active-stock selection must not silently become a survivor-free historical claim. Bound or disclose unreconstructible coverage.
- Use chronological walk-forward evaluation. Fit scalers, bins, sector mappings and weights inside each training fold. Calibrate probabilities only on a temporally later inner calibration set or past out-of-fold predictions, never on the base model's fitted predictions or the untouched test block. Purge/embargo overlapping multi-session labels. For panel results, split and bootstrap by session/block so hundreds of stocks on one rebound day are not hundreds of independent events.
- Report gap/day/intraday accuracy separately, Brier/log loss and calibration versus base rates, return forecast error/interval coverage, effective sample counts, and breakdowns for prior selloffs, large US rebounds and adverse/quiet regimes. Sparse or unprecedented shocks need explicit support limits.

### End-to-end economic checks

- Compare the existing pipeline, a simple US-sign augmentation and the proposed revision flow on the same point-in-time cohorts and attainable execution times. Keep the six-point gate and account risk budget constant to isolate this change.
- Include realistic entry gap, trigger fill/non-fill, spread, fees/taxes, slippage and rejected/unavailable opportunities. Do not buy at yesterday's close using overnight information. OHLC bars alone cannot prove a touch sequence; collect intraday evidence or bound both possible stop/target orders rather than claiming exact fills.
- Report net expectancy, drawdown/tail loss, turnover, filled capital exposure, missed-opportunity rate and late/failed revision rate per lane. A higher market-direction hit rate or more trades is insufficient.
- Predeclare comparison metrics and promotion rules before scoring the holdout. Require probability skill against the matching base-rate baseline and credible incremental net benefit under cost sensitivity, with uncertainty and tail-risk limits. A profitable in-sample November 24 replay alone cannot pass. If support or confidence is inadequate, remain observational rather than reporting a validated edge.
- Prefer original timestamped feedback for historical comparisons. Rerunning today's LLM on old market data is not a time machine: model knowledge and prompt changes can introduce hindsight. Separate deterministic historical replay from newly simulated verdicts, then require prospective shadow observations for the new decider flow.

### Contract and operational checks

- Original EOD rows, source cutoff and successful summaries remain unchanged; unchanged TimesFM inputs reuse evidence. Only target-session revision identities change.
- Full and compact prompts receive equivalent overnight evidence and explicit observed/forecast/prior-session precedence. A stale old defensive label cannot silently override the new assessment; a forecast cannot impersonate a live pass.
- Legacy REJECT selection never uses prose matching. New structured reason metadata is optional for historical readers and explicit for new revisions. Validate both decider roles and mixed WAIT/REJECT outcomes.
- Scheduled execution and explicit targeted recovery use the same revision module and exact scope. Simulate a provider error, no new US session, one role timeout, partial publication, duplicate scheduling, restart between persistence/handoff, and a deadline missed during recovery. Verify no broad rerun or duplicate exposure.
- Measure collection request count, queue wait, decider p50/p95, completion-before-open rate and price-touch latency against the existing baseline. Insufficient dispatch capacity must be visible before rollout. No per-stock foreign fetch loop and no new price-touch agent call.
- Differentially verify that absent/failed/late supplements leave original IDs, status, retries, prices, quantities, dispatch timing and monitoring unchanged. Race successful additive publication against original same-slot authority/exposure; deny only the supplement. Never release an account/market fence or ignore unknown real broker exposure.
- Persist a later/high-scoring observational BUY and run the next ordinary scheduled plan/quantity/monitoring refresh: it must not replace live authority, request a subscription or reach Exec. Verify explicit activation selects only the intended target-session revision and role. A retry changing only acquisition metadata must not create another decision/order allowance.

Relevant existing regression anchors: [market-context tests](../../apps/api/tests/test_signal_decider_market_context.py), [next-open execution tests](../../apps/api/tests/test_next_open_execution_service.py), [TimesFM reuse tests](../../apps/api/tests/test_timesfm_feedback_reuse.py), [forecast tests](../../apps/api/tests/test_timesfm_forecast_service.py), [side-agent input tests](../../apps/api/tests/test_side_agent_inputs.py). Exec anchors are `tests/test_live_market_context.py` and `tests/test_plan_judgment_actions.py`.

## Delivery slices

1. **Evidence and offline baseline:** validate KIS direct-index master coverage, timing and historical rows first; use independently verified external alternatives only if it cannot meet the requirement. Persist shared timestamped snapshots; implement the lag-only relative-shock features and chronological benchmark/calibration report. No trading behavior changes. Reuse the KIS client, existing HTTP/context patterns, `side_agents/market_inputs.py`, `us_equities/market_data.py`, `market/`, and `analytics/signal_lab.py` where appropriate; isolate optional-source failures from the daily pipeline and avoid a new provider SDK.
2. **Preopen revisions in observation mode:** add the shared revision module, durable job/proof identity, backward-compatible reason metadata, both prompt forms, exact role-level retries and target-session readback. Wire the existing scheduled runner and explicit recovery entry points to that same module. Persist new verdicts in isolated observational storage, collect subsequent outcomes and measure latency; prove they cannot be consumed by live selectors, overwrite authority or reach Exec.
3. **Reviewed additive integration:** after acceptance gates pass, publish only isolated eligible supplementary families through namespaced plan generation, residual-capacity Core sizing and existing Exec intake. Preserve original selection/quantities/monitoring with explicit namespace filters and priority. Missing capability skips optional entries. Align contracts/docs and verify reviewed remote revisions before deployment; no broad EOD replay, baseline replacement or late-buy catch-up.

Rollout evidence must identify source/model/contract/commit versions and show which session consumed which revision. A compatible deterministic branch selector is a later optimization, not a prerequisite for proving whether this forecast adds value.

## Plan review and verification

Independent source-mapping and research reviews were completed on 2026-11-24. Accepted corrections: explicit isolation of shadow verdicts from latest-snapshot selection; cutoff/material-hash freezing across retries; separate probability calibration data; prospective evidence for new LLM verdicts; US-session return decomposition; and verification of regression-anchor filenames. Graphify queries were budget-limited, so the relevant findings were confirmed against current source. This document records proposed tests, not passing implementation tests.

Latest steering selects **KIS direct indices first if validation passes**, with external providers retained as contingencies. This supersedes the earlier external-first sourcing proposal. Official examples establish an index endpoint, not the requested symbols' availability or before-open freshness. The algorithm now specifies lag-only normalized return shocks, return acceleration in percentage points, prior-selloff conditioning and separate sector context; none has yet been calibrated or deployed.

## Remaining decisions and risks

- Historical preopen availability and accessible licensed depth are not yet verified for every feature; optional futures/FX/volatility legs must not delay the minimal ETF baseline.
- A US rebound may already be captured by the Korean opening gap, may be sector-specific, or may reverse. Directional predictability is not proof of executable net profit.
- A large cross-market regime change may affect most of the original cohort. Structured reasons reduce avoidable work in future runs, but they cannot retroactively classify legacy rejections. Measure whether the bounded morning revision fits the available window.
- New preopen evidence has a new cutoff. Historical recovery, evaluation and live authorization must continue to distinguish it from the original EOD decision.

This change set contains this plan only. No models, broker requests, source-data replay, score changes, order-path changes or deployment were performed for the proposal.
