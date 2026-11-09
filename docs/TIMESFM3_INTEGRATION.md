# TimesFM 3.0 decision evidence

## Scope and integration

This optional personal-study integration supplies a numerical forecast to both
`short-term-decider` and `signal-decider` immediately before their normal dispatch.
The standard API launcher automatically enables a provisioned native Apple
Silicon worker when `TIMESFM_ENABLED` is unset. Explicit `false` keeps it disabled;
direct Python/CLI invocation retains the disabled setting default. It does not add
a specialist vote, change an order rule, activate a policy, or replace summaries.

The first version uses KRX `daily_price_bars.close_price`: up to 512 observations,
at least 128 by default, with 1-, 5-, and 20-trading-bar forecasts. These values are
configuration choices, not validated investment horizons or a minimum guaranteed
to produce useful forecasts. The stored close adjustment basis is not verified;
corporate actions and discontinuities remain a research limitation. US histories,
intraday bars, covariates, fine-tuning, and forecast-based position sizing are not
part of this implementation.

`AgentFeedbackOrchestrator._prepare_role_dispatch_items` prepares one eligible
symbol batch after reuse, upstream-summary, and canonical-price checks. Blocked
symbols do not invoke the forecast worker. The shared payload adds
`forecast_evidence.timesfm3` after the
existing source-generation hashes are created. Prompt trimming retains the
forecast and its interpretation guidance. Both existing decider request snapshots
keep the complete evidence. No database schema migration or new HTTP route is
required. Generic `GET /agent-context/symbol-day` reads do not invoke the model.

The forecast carries model/package revisions, the decision cutoff, input dates,
observation/revision timestamps, source names, input and evidence fingerprints,
the anchor close, quantiles, calibration status, and timing scope. Existing
pre/post-dispatch and durable-snapshot revalidation also validate this evidence
using price reads and hashes; they never rerun the model. A changed source window
invalidates previously available evidence through the existing input-change fence.
Feedback reuse validates the saved forecast with the same checks before keeping
an earlier decision, including after forecasting is disabled. It redispatches
deciders whose forecast evidence or input window is no longer valid. Read-only
status calls without a requested cutoff use the feedback run's saved admission
cutoff; they do not derive an authoritative cutoff from the forecast itself.

## Automatic daily process

The automatic after-close PyKRX chain and the manual/recovery daily orchestrator
both run TimesFM inside the existing `signal_decider` stage. After the summarizer
prerequisites, decision reuse, and canonical-price checks, the eligible symbols
receive forecasts before either final decider is dispatched. Both roles share the
existing process cache. The forecast uses the same admitted
`source_knowledge_cutoff_at` as the decider; it does not create a later cutoff.

`GET /eod/pipeline/live` exposes `substeps.timesfm_forecast` on the running
`signal_decider` stage, with start/completion status, role and request-run identity,
symbol count, final evidence availability counts and reasons, cutoff, and elapsed
milliseconds for the current invocation. Counts describe the latest role/batch,
not the whole daily universe; the second role may reuse cached forecasts.
Availability is measured after the forecast's anchor is checked against the
decider's canonical price. A completed forecast operation can still report
unavailable evidence; that remains optional and does not change the parent
stage's decision success/failure counters. An explicitly disabled forecast is
reported as skipped. Generic dispatches without an active daily stage do not
create a synthetic daily run.

The complete evidence remains in each decider request snapshot at
`forecast_evidence.timesfm3`. There is no separate persisted forecast-completion
stage: after restart, the process cache is empty and eligible decider dispatches
prepare forecasts again. Existing daily scheduling, scope, recovery, and
next-open execution gates remain in force.

## Model and output contract

- Package: `timesfm[mlx]==3.0.2`.
- Model: `google/timesfm-3.0-pytorch`.
- Checkpoint revision: `43046b85ec22d584a13f8098c2ed39c889e129c2`.
- Backend: native Apple Silicon MLX, FP32, compilation enabled.
- Input: univariate stored closes, no external normalization, padding, or covariates.
- Forecast once to horizon 20, then select steps 1, 5, and 20.
- Native nine-decile indices 0, 4, and 8 give Q10, Q50, and Q90.

`median_return_pct = (price_q50 / anchor_close - 1) * 100`; the same monotonic
conversion applies to Q10 and Q90. For an illustrative anchor of 100 and Q10/50/90
of 98/101/104, the return fields are -2%/+1%/+4%. This is a calculation example,
not a model result. Q50 is a median, not an expected return. The nominal Q10–Q90
interval is not a calibrated confidence score or an 80% probability of profit.
`calibration_status` starts as `not_evaluated`. Close-to-close forecasts do not
incorporate next-open entry prices, costs, gaps, market impact, or portfolio risk.

## Install on the Core Mac

Use an ARM-native Python on Apple Silicon. The worker requires MLX; the base API
requirements remain unchanged. Run the following from `apps/api` in the reviewed
checkout. The separate worker environment lives under the existing ignored `var/`
directory. Choose the Python version already used on the Mac; Python 3.12 is shown.

```bash
python3.12 -m venv var/timesfm-venv
var/timesfm-venv/bin/python -m pip install -r requirements-timesfm.txt
var/timesfm-venv/bin/python -m app.forecast_context.mlx_runner \
  --benchmark --context-length 512 --horizon 20 --batch-size 1 --repeats 30
```

The first benchmark downloads the pinned checkpoint if it is not cached. The
official model repository lists an approximately 1.32 GB weight file; download
size is not the same as peak runtime memory. Run provisioning as the same OS user
and with the same `HF_HOME` as the Core worker. Normal decider execution sets
`HF_HUB_OFFLINE=1` so a missing checkpoint cannot start a network download on the
decision path. The model source code is Apache-2.0; the selected 3.0 weights have
a separate non-commercial/non-production license. This integration is scoped to
the owner's stated personal study purpose; see the official license below.

When started through `start-api.sh` (including the existing Mac launchd service),
Darwin/arm64 plus an executable `apps/api/var/timesfm-venv/bin/python` enables the
feature and supplies that absolute interpreter path if the corresponding settings
are unset. Explicit process-environment values take precedence over `.env`, and
explicit settings are preserved. Other platforms and installations without that
worker retain the disabled default. Startup does not install or download a model.

For direct Python/CLI runs, a worker installed elsewhere, or explicit overrides,
use these settings in the Core environment, using the real absolute path:

```dotenv
TIMESFM_ENABLED=true
TIMESFM_PYTHON_EXECUTABLE=/absolute/path/to/kai-trading-core/apps/api/var/timesfm-venv/bin/python
TIMESFM_CONTEXT_LENGTH=512
TIMESFM_MIN_CONTEXT_LENGTH=128
TIMESFM_BATCH_SIZE=32
TIMESFM_TIMEOUT_SECONDS=120
```

Restart the API through its existing supervisor/launcher after the reviewed change
is deployed. No `alembic` migration is introduced by this feature. Keep all existing
execution approvals and configuration independent of this research setup.

## Read-only preview with stored data

Before enabling the API flag, use the API interpreter to preview a symbol. This
command loads `apps/api/.env`, enables forecasting only in its own process, reads
stored prices, prints evidence, and does not dispatch an LLM or write a decision.
Use a completed date and the actual timezone-aware cutoff for the intended
decision. The example cutoff is illustrative; it must be after all consumed rows
were observed and must not be in the future.

```bash
.venv/bin/python -m app.forecast_context.preview \
  --symbols 005930 \
  --market-date 2026-11-09 \
  --knowledge-cutoff-at 2026-11-09T23:00:00+09:00
```

Exit status 0 means every requested forecast is available. Otherwise the JSON
contains per-symbol reasons and exit status 1. `elapsed_ms` includes the price
read, validation, worker startup/loading, inference, and evidence assembly;
it excludes the final decider and API process startup/imports.

The existing normal feedback dispatch automatically attaches the block to both
deciders when enabled. A reused prior decision is not retroactively changed merely
by switching this flag; use the normal next run, or an intentionally requested
recompute through the existing dispatch controls. Inspect the saved decider
request payload for `forecast_evidence.timesfm3`.

## Timing and memory

Google publishes these MLX results for the 330M model on **M4 Max**, context 512,
horizon 64, FP32, and `mx.compile`:

| Batch | Published p50 batch latency | Published throughput |
|---:|---:|---:|
| 1 | 11.1 ms | 90 series/s |
| 8 | 19.7 ms | 406 series/s |
| 32 | 48.1 ms | 666 series/s |

These are official upstream benchmarks, not measurements on the owner's Mac or
of Core. The README does not document the complete warmup/repetition procedure.
48.1 / 32 = 1.503125 ms is amortized work per series; the batch still waits for
its full completion. Our default horizon 20 differs from the official horizon 64.

The worker benchmark reports loading separately from first inference/compilation,
then synchronized repeated calls with p50/p95 and MLX peak memory. Synthetic input
is explicitly marked. To compare batch throughput, repeat the benchmark with
`--batch-size 8` and `--batch-size 32`. `--input request.json` uses actual validated
series supplied in the worker JSON contract instead of generated input.

The Core integration intentionally uses a bounded subprocess for each **uncached
source batch**, loading weights once within that worker. Manual chunks are at most
32 because the current upstream MLX fast path does not enforce
`per_core_batch_size`. The process exits after the batch. An in-memory, bounded
result cache reuses identical source/model/configuration across the two deciders
and retries; it is lost on API restart and is not shared between API processes.
Therefore a one-symbol uncached Core request includes process/model startup, and
must not be advertised as an 11.1 ms end-to-end operation. Use the preview to
measure that actual boundary. Model runtime RSS is not the same metric as MLX peak
allocation, and no minimum RAM recommendation was measured in this change.

## Availability and point-in-time behavior

The service rejects missing, stale, duplicate, nonpositive/nonfinite, or discontinuous
price windows. It verifies XKRX sessions locally, never invents missing bars, and
requires the last input date to equal the requested completed market date. It
reuses the existing Core finality guard (default 20:10 KST) with the supplied
cutoff. Every consumed row's `captured_at` must be strictly before that cutoff.

### KRX calendar corrections

Core's shared `KRX_EXCHANGE_CALENDAR` supplements the upstream XKRX calendar with
two confirmed 2026 closures missing from `exchange_calendars==4.13.2`:

- `2026-06-03`: local election day.
- `2026-07-17`: Constitution Day.

The [KRX market-operation notices](https://strn.krx.co.kr/corebbs5/BHPSTRN0401/list)
(posted June 1 and July 10, 2026) and
[Samsung Futures' May 14 market closure notice](https://www.ssfutures.com/ssf/customer/notice/CUS_01102.cmd?ND_PATH=%2Fbiz%2Fcustomer%2Fnotice%2FAd202_4589.shtm&categoryID=M158746897264671332)
confirm the dates. As checked on November 9, 2026, the latest upstream release is
still [4.13.2](https://github.com/gerrymanoim/exchange_calendars/releases/tag/4.13.2).
June 3 is fixed by the unreleased
[upstream PR #574](https://github.com/gerrymanoim/exchange_calendars/pull/574);
July 17 remains reported in
[upstream issue #611](https://github.com/gerrymanoim/exchange_calendars/issues/611).
Upgrading to 4.13.2 alone does not correct either date.

The local calendar adds these dates before constructing its sessions, using a
union so future upstream inclusion does not duplicate a holiday. This same object
serves TimesFM, history-repair session checks, and the existing exchange-calendar
fallback. KIS remains the authoritative first choice for normal market-calendar
lookups; strict lookups still reject fallback. No additional network call is added
to TimesFM. These exact-date corrections do not implement later-year holiday rules;
remove them only after testing an upstream release that covers both dates.
Forecast `calendar_source` identifies this correction as
`exchange_calendars:XKRX+core_2026_closures_v1`.

For the reported `2025-08-01` through `2026-11-06` window, the corrected calendar
expects 308 sessions (`310 - 2`), matching the stored row count. The recent window
from `2026-04-29` expects 128 (`130 - 2`). A missing bar on an actual session or
an extra bar on either closure is still rejected. Do not backfill prices or lower
the history requirement to address this mismatch. Pull the fix, restart any
running Core process, then rerun the read-only preview. Diagnostic scripts must
import `app.market.session.KRX_EXCHANGE_CALENDAR`; a separate
`exchange_calendars.get_calendar('XKRX')` still uses the uncorrected upstream
registry.

### Source availability

The loader reads current mutable price rows. A repaired/backfilled row observed
after the cutoff makes the forecast unavailable; it is not silently filtered out.
The input hash detects subsequent source changes, but does not reconstruct old
price vintages. Existing historical dispatches can default to a 15:30 KST cutoff,
which is earlier than final EOD admission; an unavailable forecast is the correct
result in that case. Never relax a historical cutoff just to obtain an output.

These checks address stored-price availability. They do not establish that the
pretrained model existed at a historical decision time or excluded that history
from training. Historical outputs are research reconstructions, not proof of
leakage-free historical trading performance.

Missing optional dependencies/checkpoint, a timeout, malformed outputs, or bad
input produce `status=unavailable` with a reason. The remaining decision evidence
continues. Worker failures have a 60-second retry cooldown. Invalid per-symbol
outputs use a 60-second cooldown keyed by the exact input, so a bad output does
not delay other symbols or revised inputs. Existing cached successes
are reused only for exactly matching inputs, never as a stale last-good fallback.
Unavailable evidence is neutral; it is not converted into a zero return or a
negative signal. Previously available evidence that fails integrity/source checks
is handled by the existing changed-input fence, not silently downgraded.

Rollback: set `TIMESFM_ENABLED=false` and restart the API. Existing saved evidence
remains auditable, and any previously available evidence still undergoes its
source validation when its saved request is revalidated.

## Validation scope

The repository tests exercise dependency-free imports, chunking with fake MLX
objects, full generator consumption, quantile mapping, model/package identity,
bad inputs and timeouts, point-in-time handling, cache reuse, source changes,
both deciders, prompt preservation, and unchanged generic source generations.
Base FastAPI import works without installing TimesFM. Real MLX inference and
latency require the Apple Silicon provisioning and preview steps above; Linux
unit tests do not establish numerical model correctness or forecast profitability.

## Primary sources

- [Official source and benchmark](https://github.com/google-research/timesfm)
- [Pinned PyPI package](https://pypi.org/project/timesfm/3.0.2/)
- [Pinned model files](https://huggingface.co/google/timesfm-3.0-pytorch/tree/43046b85ec22d584a13f8098c2ed39c889e129c2)
- [MLX forecaster implementation](https://github.com/google-research/timesfm/blob/master/src/timesfm3/mlx/timesfm3_forecaster.py)
- [MLX installation](https://ml-explore.github.io/mlx/build/html/install.html)
- [Model weight license](https://huggingface.co/google/timesfm-3.0-pytorch/blob/43046b85ec22d584a13f8098c2ed39c889e129c2/LICENSE)
