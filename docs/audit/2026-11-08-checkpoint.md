# Core audit checkpoint — 2026-11-08

**Status: incomplete; source changes are not ready for release.**

This checkpoint records the interrupted audit and accompanies the README/architecture documentation. The source/test patches described below were applied or prepared in the local audit checkout before its execution environment became unavailable. **They are not included in this remote documentation commit.** At interruption, some newly added regressions were still failing, and the final full-suite run had not been completed. This is not a certification of the repository or a claim of line-by-line completion.

**Workspace reconnection update:** On reconnection, the earlier uncommitted audit patches and scratch test artifacts could not be located in the available workspace; the earlier Exec audit checkout was also absent. The findings and test results below describe the last observed pre-interruption work. They do not establish that those source patches are currently available. Code changes must be recovered or recreated and validated before publication.

## Baseline, scope, and branch

- Repository: `kiseyno-AI/kai-trading-core`.
- Audited base: [`2322295cdda099367e812b09d4187858434e49fc`](https://github.com/kiseyno-AI/kai-trading-core/commit/2322295cdda099367e812b09d4187858434e49fc).
- Task branch: `codex/core-deep-audit-20261108`.
- The baseline was the latest observed `main`, including the 2026-11-08 merged changes and the preceding English architecture documentation merge.
- The user authorized direct promotion of the root `README.md` to `main`. The detailed architecture and audit documents remain on the separate task branch. No application/source-code or test change from this audit is included in the README promotion. No deployment or live trading action was performed.

The tracked-code inventory selected `.py`, `.sh`, `.js`, `.ts`, `.tsx`, `.sql`, `.yml`, `.yaml`, and `.toml` files and counted their lines. Core contained **629 selected files / 555,047 lines**, including **287,778 non-test source/configuration lines**. The test remainder is **267,269 = 555,047 − 287,778**. Generated graph nodes and passing tests do not count as manually inspected source lines.

Ponytail Audit guided the defect review; Ponytail guided the narrow fixes and regression evidence; Ponytail Review guided the review requirements. Graphify supplied a code-relationship map. Manual review remained partial, with the principal exact ranges recorded below.

## Baseline validation

On the untouched baseline, the isolated PostgreSQL environment completed migrations, schema parity, application import, and the full suite:

```text
5,217 passed, 17 warnings in 191.80s
```

The test result is evidence about that run, not proof that all logic is correct. The defects below were found afterward with additional behavioral regressions. Raw local logs could not be uploaded after the workspace disconnected; the counts here transcribe the completed audit command outputs.

## Confirmed findings with locally applied fixes and focused passing evidence

None of these application changes is present in this documentation-only remote checkpoint.

| ID | Reproduced problem | Local change | Evidence and status |
| --- | --- | --- | --- |
| CORE-1 | Upstream audit records exposed credentials in the URL path, encoded path variants, or exception text even when another URL field was redacted. | Collect configured/query/header secret values; redact their raw and encoded forms from the recorded URL/path and error messages. | Before-fix regression failures; the combined auth/audit selection passed **25 tests** after changes. This does not establish coverage of every possible encoding. |
| CORE-2 | A non-ASCII bearer credential caused `compare_digest(str, str)` to raise instead of returning unauthorized. | Compare encoded bytes while preserving the existing bearer policy. | A request containing `Bearer invalid-\\xff` previously raised; afterward it returned 401 and did not call the protected route. Included in the 25-test selection above. |
| CONTRACT-1 | Decimal normalization rounded dispatch/buy-authority material using ambient precision; distinct large values could share a fingerprint. | Normalize finite decimal tuples exactly without context rounding, identically in Core and Exec. | Six new cases failed before the fix in each repository. Core canonicalization plus existing material-vector tests passed **11**; Exec's corresponding selection passed **12**. |
| RESEARCH-1 | Historical top-k selection discarded missing future outcomes before ranking, allowing lower-ranked substitutes into the evaluation. | Rank first; preserve an unpriced-period outcome instead of substituting. | Included in the **94-test** research/improvement selection. |
| RESEARCH-2 | Drawdown omitted starting capital. | Include the initial equity point when computing peak-to-trough loss. | A negative first period is now represented; included in the 94-test selection. |
| RESEARCH-3 | Multiple same-week runs could satisfy multi-week improvement persistence. | Retain dated KST week identity, replace same-week reruns, and require consecutive distinct weeks; undated legacy entries cannot authorize changes. | Included in the 94-test selection. |
| RESEARCH-4 | Undated changelog entries could avoid a date-based cooldown. | Require a valid dated changelog entry. | Included in the 94-test selection. |
| RESEARCH-5 | A null statistical value caused issue selection to fail; issue identity also varied with the selected horizon. | Handle null `t_hac` and keep the issue title stable across horizons. | Included in the 94-test selection. |
| DATA-2 | Due DART `[014]` IDs selected outside the newer-filings window were filtered out by capture's repeated lower bound. | Keep the selected exact IDs and upper market-date bound without reapplying the newer-filings lower bound. | Regression asserted actual capture/publication, changing **0 → 1**. Included in **160 passing tests** with gap/backfill/DART suites. |
| DATA-3 | One unrelated feature row could cancel one missing bar-symbol feature in a count-only gap detector. | Count feature coverage only within the exact stored bar-symbol membership. | SQL fixture: bar for A, feature for B. The former no-gap result was reproduced. Included in the 160-test selection. |
| DATA-4 | The gap service retained stale derived provider-comparison counts after stored bars changed. | Remove comparison-result caching while retaining provider-frame caching. | Same-service missing count changed from incorrect **1 → 1** to **1 → 0** after repair. Included in the 160-test selection. |
| DATA-5 | An empty DART/Piotroski cohort skipped all factor follow-up, including the independent price/PyKRX factors. | Pass an explicit empty DART cohort into normal and point-in-time follow-up while preserving the proven price cohort. | Full-path regression plus cohort tests passed **9**. Broader mocked validation remains pending; decision admission remains blocked. |

### Source and regression locations

- CORE-1: [`audit_logging.py`](https://github.com/kiseyno-AI/kai-trading-core/blob/2322295cdda099367e812b09d4187858434e49fc/apps/api/app/core/audit_logging.py); local regression in `apps/api/tests/test_audit_logging.py`, including `test_upstream_failure_logging_redacts_encoded_path_and_query_secrets`.
- CORE-2: [`main.py`](https://github.com/kiseyno-AI/kai-trading-core/blob/2322295cdda099367e812b09d4187858434e49fc/apps/api/app/main.py); local `InternalAPIClientTests.test_non_ascii_bearer_credentials_return_unauthorized` in `test_internal_ops.py`.
- CONTRACT-1: [`execution_handoff_contract.py`](https://github.com/kiseyno-AI/kai-trading-core/blob/2322295cdda099367e812b09d4187858434e49fc/apps/api/app/orders/execution_handoff_contract.py), [`buy_authority_contract.py`](https://github.com/kiseyno-AI/kai-trading-core/blob/2322295cdda099367e812b09d4187858434e49fc/apps/api/app/orders/buy_authority_contract.py), [`numbers.py`](https://github.com/kiseyno-AI/kai-trading-core/blob/2322295cdda099367e812b09d4187858434e49fc/apps/api/app/core/numbers.py); new local `test_contract_numeric_canonicalization.py` in each repository.
- Research fixes: [`signal_evaluation.py`](https://github.com/kiseyno-AI/kai-trading-core/blob/2322295cdda099367e812b09d4187858434e49fc/apps/api/app/analytics/signal_evaluation.py), [`signal_lab.py`](https://github.com/kiseyno-AI/kai-trading-core/blob/2322295cdda099367e812b09d4187858434e49fc/apps/api/app/analytics/signal_lab.py), [`improvement_findings.py`](https://github.com/kiseyno-AI/kai-trading-core/blob/2322295cdda099367e812b09d4187858434e49fc/apps/api/app/analytics/improvement_findings.py), [`agent_improvement_run.py`](https://github.com/kiseyno-AI/kai-trading-core/blob/2322295cdda099367e812b09d4187858434e49fc/scripts/agent_improvement_run.py), [`agent_rules_guard.py`](https://github.com/kiseyno-AI/kai-trading-core/blob/2322295cdda099367e812b09d4187858434e49fc/scripts/agent_rules_guard.py).
- DATA-2: [capture service](https://github.com/kiseyno-AI/kai-trading-core/blob/2322295cdda099367e812b09d4187858434e49fc/apps/api/app/dart/fundamentals_capture_service.py), `FileNotFoundRecheckDailyPathTests.test_daily_refresh_captures_a_due_014_event_outside_the_window`.
- DATA-3/DATA-4: [gap detectors/cache](https://github.com/kiseyno-AI/kai-trading-core/blob/2322295cdda099367e812b09d4187858434e49fc/apps/api/app/eod/data_gap_fill.py), `DetectorSqlTests.test_unrelated_feature_rows_do_not_hide_missing_bar_symbols`, `test_provider_comparison_reads_repaired_stored_bars_on_each_tick`.
- DATA-5: [pipeline caller/helper](https://github.com/kiseyno-AI/kai-trading-core/blob/2322295cdda099367e812b09d4187858434e49fc/apps/api/app/eod/pykrx_pipeline_runner.py), `PykrxBackfillServiceTests.test_empty_piotroski_cohort_keeps_price_factors_and_blocks_signal_features`.

The links identify the baseline implementation. Locally added tests and fixed implementations are not available at those pinned baseline links.

### Cross-repository compatibility note

The exact-decimal fix preserved the existing ordinary wire vector:

```text
2cdf1f12fee6324b85e62f42b5a18b0ab31e1aa87a0f6017cbf68ac2b7ebcbc3
```

That vector corresponds to material containing quantity `100`, price `12345.67`, ratio `Decimal("0.001200")`, and empty metadata. The new regression also changes ambient Decimal precision to 3 and its exponent bounds to ±3; the result remains the same.

Previously rounded payloads with extreme precision can produce a different correct fingerprint and require a fresh handoff after both sides are updated. This is not permission to rewrite accepted historical identities. Paired final validation remains required before the contract patch is published.

## Confirmed regressions still pending source fixes

The following five regressions were added locally and failed for the intended behavior. The subsequent source patch failed before applying any target because the executor disconnected.

| ID | Confirmed problem | Pending correction and evidence |
| --- | --- | --- |
| RESEARCH-6 | Historical bars disappear when their symbol is inactive today, creating survivorship bias. | Remove the current `s.is_active` condition from the historical loader. Regression: `LoaderSmokeTests.test_historical_bars_survive_a_later_symbol_deactivation`. |
| AGENT-1 | Weekly-rebind fallback refers to an unimported `DAILY_RETRY_CYCLE_MODE`. | Import it from `run_modes`. Regression: `test_prepare_supports_a_latest_run_adapter_without_list_runs`. |
| AGENT-2 | The coordinator's progress callback accepts a post-target date with an injected retry adapter. | Make the callback reject dates after the target. Regression: `test_coordinate_blocks_if_a_paused_retry_resumes_after_the_target_date`. The production retry service has an independent stricter guard, so a production bypass has not been demonstrated. |
| GUARD-1 | Four calibration bullets pass a guard although the hard policy allows three. | Make the guard enforce the policy limit; strict character-boundary review remains pending. Regression: `GuardTests.test_the_hard_policy_limit_is_three_bullets`. |
| GUARD-2 | Moving local `origin/main` after a forbidden commit can change the guard's comparison base. | Pin the fetched base SHA before candidate execution and use it for all guarded reads/diffs; disable Git replacement-object interpretation. Regression: `RealRepositoryTests.test_moving_origin_main_cannot_hide_a_forbidden_contract_edit`. |

AGENT-1 affects the fallback adapter without `list_runs`, not the ordinary repository service. AGENT-2 is an inconsistent coordinator/adapter boundary: the production retry service composes `_assert_weekly_policy_target_session_not_open` before the caller callback and independently rejects dates later than the target. The regression establishes the callback gap, not a bypass of that production guard.

GUARD-2 was reproduced only in a disposable local repository with push and PR actions mocked. It did not change this repository's remote refs.

**Source:** [historical signal lab](https://github.com/kiseyno-AI/kai-trading-core/blob/2322295cdda099367e812b09d4187858434e49fc/apps/api/app/analytics/signal_lab.py), [weekly rebind](https://github.com/kiseyno-AI/kai-trading-core/blob/2322295cdda099367e812b09d4187858434e49fc/apps/api/app/agent_context/weekly_policy_rebind_service.py), [improvement runner](https://github.com/kiseyno-AI/kai-trading-core/blob/2322295cdda099367e812b09d4187858434e49fc/scripts/agent_improvement_run.py), [rules guard](https://github.com/kiseyno-AI/kai-trading-core/blob/2322295cdda099367e812b09d4187858434e49fc/scripts/agent_rules_guard.py).

## Reproduced repair race; applied local fix still unvalidated

**DATA-1 — a new downstream date can escape a final-bar repair's rebuild obligation.**

The repair discovers dependent dates before locking and uses shared global admission. A different-date writer can therefore introduce a date outside that discovered scope. The transactional writer trusts the supplied rebuild-date set.

Two real PostgreSQL regressions failed before the local fix:

1. `KISFinalBarRepairServiceTests.test_apply_blocks_a_writer_for_a_new_downstream_date`.
2. `KISFinalBarRepositoryPostgresTests.test_new_downstream_date_invalidates_the_discovered_rebuild_scope`.

The local patch requests exclusive global writer admission and rechecks the downstream scope inside the transaction before audit or price writes. **Its post-fix run has not completed.** Preserve the existing expected-old-value, immutable audit, and explicit closeout gates during follow-up review.

**Source:** [repair writer](https://github.com/kiseyno-AI/kai-trading-core/blob/2322295cdda099367e812b09d4187858434e49fc/apps/api/app/eod/kis_final_bar_repair.py), [writer locks](https://github.com/kiseyno-AI/kai-trading-core/blob/2322295cdda099367e812b09d4187858434e49fc/apps/api/app/repositories/eod_run_repository.py), [official restatement caller](https://github.com/kiseyno-AI/kai-trading-core/blob/2322295cdda099367e812b09d4187858434e49fc/apps/api/app/eod/official_bar_restatement.py).

## Unverified review leads

These are hypotheses or static observations requiring reproductions, not additional confirmed fixes:

- Core's retained direct-order decimal parsers do not visibly reject every non-finite value at the internal service/store boundary. HTTP validation and internal callers must be examined separately.
- A realtime fill arriving before the direct-order ACK may be overwritten by an unconditional ACK snapshot with zero filled quantity.
- A critical OPEN readiness exception reaches the outer runner handler before later Core protective/reconciliation steps.
- An earlier CI authority-replacement failure remains unexplained; the current baseline full suite passed. A clock-fixture lead is not a demonstrated production cause.
- Side-agent postmortem SQL may use integer division for market returns, select decisions without the research cutoff, count per-symbol rows across missing sessions, or include a next-day boundary in historical review.

**Source:** [order service](https://github.com/kiseyno-AI/kai-trading-core/blob/2322295cdda099367e812b09d4187858434e49fc/apps/api/app/orders/service.py), [order store](https://github.com/kiseyno-AI/kai-trading-core/blob/2322295cdda099367e812b09d4187858434e49fc/apps/api/app/orders/store.py), [runner](https://github.com/kiseyno-AI/kai-trading-core/blob/2322295cdda099367e812b09d4187858434e49fc/apps/api/app/eod/runner.py), [Exec integration](https://github.com/kiseyno-AI/kai-trading-core/blob/2322295cdda099367e812b09d4187858434e49fc/apps/api/app/orders/exec_integration.py), [postmortem inputs](https://github.com/kiseyno-AI/kai-trading-core/blob/2322295cdda099367e812b09d4187858434e49fc/apps/api/app/side_agents/postmortem_inputs.py).

## Review coverage

This is a coverage checkpoint, not a claim that a fully read file is proven correct. Ranges refer to the baseline before edits.

### Data/EOD reviewer

Fully read:

```text
eod/data_gap_fill.py                         1–533
eod/stored_signal_feature_backfill.py        1–211
eod/finality.py                             1–57
eod/official_bar_restatement.py              1–381
eod/turnover_repair.py                       1–654
eod/auto_pykrx.py                            1–545
eod/runner.py                               1–901
eod/daily_pipeline_tracking.py              1–241
eod/stage_heartbeat.py                      1–83
eod/market_membership.py                    1–116
signals/market_risk_filter.py               1–412
signals/market_risk_evaluators.py            1–184
```

Additional partial ranges:

```text
eod/kis_final_bar_repair.py                  1–1354
eod/pykrx_pipeline_runner.py                2405–2825, 6025–6300, 6400–7095
repositories/eod_run_repository.py          214–475
eod/derived_stage_repair.py                 162–242
eod/exact_signal_feature_repair.py          586–675
eod/scope_outcomes.py                       1–160
eod/pykrx_enrichment.py                     1360–1460
market_data_store.py                       990–1415, 1455–1490, 4930–5020
dart/fundamentals_store.py                  1–90, 560–730
dart/fundamentals_capture_service.py        1–85, 1190–1335, 1510–1910,
                                          2300–2375, 3093–3335, 3650–3880,
                                          3900–4020
```

The reviewer's owned inventory was 86 Python files / 127,294 baseline lines. Files outside the listed reads must not be counted as fully manually reviewed.

### Research/agent reviewer

Fully read: the three nonempty `analytics` modules; seven `side_agents` modules (`__init__`, `__main__`, `client`, `payloads`, `store`, `runner`, `postmortem_inputs`); `weekly_policy_rebind_service.py`; and the improvement runner/guard plus their two local launcher/install scripts.

Partial `retry_service.py` reads: **5804–5995** and **6210–7056**. Supporting `db_models.py` reads: **1–110** and **658–714**. The broader agent-context implementation, three remaining side-agent modules, and external/macro/news packages did not receive exhaustive manual review.

### Parent review and automated scans

The parent traced authentication/audit logging, both numeric contracts, selected direct-order submission/store paths, and selected execution-generation paths. Large order/integration modules remain partial. The focused authentication and canonicalization regressions were reviewed; their passing selections are recorded above.

Whole-source name scans checked F821/F822/F823/F811; another scan checked F601/F602/B006/B012/B023. These scans are not line-by-line review:

- Weekly-rebind's missing run-mode import was a real reproduced failure.
- The two `snapshot_after_cycle` warnings were control-flow false positives.
- DART telemetry's `exc` lambdas are invoked synchronously inside the corresponding exception scope; no runtime failure was established.
- The missing `Mapping` import was in a postponed annotation, not an executed type check.
- The `func` parameter in enrichment intentionally shadows the SQL helper in a scope that calls that parameter.
- Loop-capture warnings require call-site analysis; the full warning set was not classified before interruption.

The existing Core code-only Graphify map had **17,846 nodes, 59,648 links, and 475 communities**. Its links comprise **53,954 extracted + 5,694 inferred = 59,648**. It was built on the preceding source snapshot, before the documentation-only merge and before these local patches. It is a relationship aid, not runtime evidence or proof of complete review.

## Execution interruption and safe continuation

An expanded data regression run reached an unmocked `fchart.stock.naver.com` path. Automatic approval review rejected that external request. No successful provider response is claimed, and that request must not be retried through another route.

A test runner was prepared to deny external DNS/socket connections before pytest collection while allowing the isolated loopback PostgreSQL database. Its guarded rerun did not start: the executor disconnected and later reported the workspace unavailable. Even a harmless working-directory read failed. No causal connection between the approval rejection and infrastructure failure is asserted.

The next required steps are:

1. Inspect the reconnected workspace and recover or recreate the earlier source fixes and regressions; the prior uncommitted audit patches were not found after reconnection.
2. Apply the five pending source patches and complete the affected factor-test mocks.
3. Run repair-race and broader tests under the external-network-denying runner.
4. Resolve the unverified order/runner/research leads with meaningful regressions before changing behavior.
5. Continue manual coverage across the remaining source, migrations, scripts, and critical tests.
6. Run the required application import, final complete test suite, schema checks if relevant, and independent diff review.
7. Publish the reviewed source/test changes to the isolated task branch. Do not merge to main.

The documentation was preserved through the GitHub connector while local execution was unavailable. That recovery step did not transfer the local source patches, raw test logs, or unfinished generated graph artifacts.
