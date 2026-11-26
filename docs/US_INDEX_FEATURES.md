# Descriptive US-index features

Stage 8 calculates data-only features from sealed `SPX`, `COMP` and `SOX` inputs.
`app.market.us_index_features.calculate_us_index_features(current, *, history=())`
is pure: it returns JSON-safe data without providers, live calendars, writes or
automatic history discovery. The separate CLI reads explicit sealed dates; the
preopen collector does not invoke it. A proposed 07:15 evaluation remains future
work, alongside strategy validation and any signal, score or order integration.

Stage 9 adds opt-in `--include-history`: the current `history_22` seal supplies
20 lagged pairs directly, producing `us_index_features_v2` with
`history_source: current_snapshot_history` and an ID binding that full history.
It never synthesizes older freeze records. Defaults and the explicit older-seal
version-1 path below remain unchanged. See [full-history inputs](US_INDEX_HISTORY.md).

## Inputs and replay

Callers must supply verified `USIndexInputFreezeRecord` objects. The calculator
validates record/session bindings and finite positive decimal closes, but does
not reauthenticate files. The CLI uses the existing hash-verifying
[`load_frozen_us_index_inputs`](US_INDEX_INPUT_FREEZE.md) reader.

Supply at most 20 historical records, oldest to newest, with distinct earlier
target dates, the same source environment and valid earlier seal windows. Future,
duplicate, reversed or mixed-environment history is a technical `ValueError`.
The calculator traces prior US-session links backwards using frozen windows,
without a live calendar. A gap stops the usable history; conflicting overlapping
closes or close timestamps raise `ValueError` rather than mixing revisions.
Overlap checks cover all explicitly supplied adjacent records, including those
before a later gap that makes them unusable for the reference statistics.
`history_count` counts that contiguous suffix; `history_inputs` preserves every
explicitly supplied target date and manifest ID, including records beyond a gap.

Results use schema `us_index_features_v1` and an isolated decimal context with
34 significant digits and round-half-even. Numeric features are decimal strings
or `null`, not JSON floats. The content-addressed `feature_id` hashes the result
body excluding itself, binding the versioned algorithm, calculated values,
current `input_manifest_id` and explicit `history_inputs` to reproducible replay.
Metadata includes target date, environment and both current US-session dates.
Formula, window or numeric-convention changes require a new schema/algorithm version.
Different explicit history selections intentionally produce different feature IDs
for the same current seal. Any future rule evaluation must bind `feature_id` and
its exact inputs; `status: computed` alone does not establish complete history or
trading authority.

## Formulas and unavailable values

For positive closing index levels `C[t]`, the daily simple return is
`r[t] = 100 * (C[t] - C[t-1]) / C[t-1]` (%). Acceleration is
`r[t] - r[t-1]` (percentage points). Output `return_spreads_pp` subtracts the
synchronized daily returns for `COMP-SPX`, `SOX-SPX` and `SOX-COMP`; these are
arithmetic spreads, not returns on an index ratio.
Keep the same price-index variant throughout. Price-index changes exclude
ordinary dividend reinvestment; constituent corporate actions are already
handled by index methodology, so do not apply stock split adjustments again.
[S&P index mathematics](https://www.spglobal.com/spdji/en/documents/methodologies/methodology-index-math.pdf).

The reference is exactly `r[t-20], ..., r[t-1]`, excluding the current event:
`mean = sum(reference) / 20`, `s = sqrt(sum((r - mean)^2) / 19)`, and
`shock_z = (r[t] - mean) / s`. The denominator 19 is sample standard deviation.
Mean/variance arithmetic is centered on the first reference return so identical
repeating returns do not acquire artificial variance from summation rounding.
[NIST measures of scale](https://www.itl.nist.gov/div898/handbook/eda/section3/eda356.htm).
`percentile_pct = 100 * count(reference_return <= r[t]) / 20` uses a
right-continuous empirical CDF, includes ties and has five-point resolution.
[R empirical CDF documentation](https://stat.ethz.ch/R-manual/R-devel/library/stats/html/ecdf.html).

These are project conventions, not evidence of predictive power or profitability.
`shock_z` is descriptive standardized distance, not confidence, a p-value or a
KRX rebound probability. Normal-distribution probability calculations require
additional distributional assumptions; standardization alone does not establish
them. [NIST normal distribution](https://www.itl.nist.gov/div898/handbook/eda/section3/eda3661.htm).

Two closes support returns and spreads; acceleration needs three consecutive
closes. Twenty lagged returns plus the event need 22 consecutive closes in total.
Incomplete history leaves reference mean, sample standard deviation, shock and
percentile `null`, with `history_unavailable_reason` and
`shock_unavailable_reason` of `insufficient_history` or `nonconsecutive_history`.
An immediate valid predecessor still permits acceleration across an older gap.
With complete constant history, `history_unavailable_reason` is `null`, `s` is
`"0"`, shock is `null` with `zero_reference_volatility`, and percentile stays valid.

KRX closures and special-hours no-ops can leave missing US-history seals. Startup
and holiday gaps therefore need not provide normalization. Never invent zero
returns, backdate seals or silently replace a missing session with an older one.
The opt-in [full-history capture](US_INDEX_HISTORY.md) can cover those gaps from
the current 22-close range without backdating seals; missing provider dates still
remain unavailable.

## Read-only CLI

From `apps/api`, with the existing Core database configuration:

```sh
python -m app.internal.us_index_features --target-trade-date 2026-11-25 --state-dir var/us-index-preopen --source-environment real
# Optional history: repeat --history-date, oldest first, at most 20 dates.
python -m app.internal.us_index_features --target-trade-date 2026-11-25 --history-date 2026-11-24
# Opt-in replay of the current full-history seal; cannot combine with --history-date.
python -m app.internal.us_index_features --include-history --target-trade-date 2026-11-25 --state-dir var/us-index-preopen
```

Environment defaults to `real`; `mock` is supported. No current seal gives
`status: unavailable`, `reason: no_frozen_inputs`; an explicitly requested missing
history seal gives `reason: missing_history`. Both report
`missing_target_trade_date` and exit 0. Technical failures report
`status: failed`, `stage: features`, `error_type` and exit 1 without exception text.
Malformed CLI arguments exit 2; validly parsed but inconsistent history is a
technical failure even when those seals are absent. Requested dates are checked
before storage availability. Nothing is collected, scheduled, installed or persisted.

For a frozen fixture with previous closes 100 and current closes
`SPX=102, COMP=104, SOX=106`, the first command produces this output excerpt
(IDs, dates and the other two per-index objects omitted):

```json
{
  "status": "computed", "schema_version": "us_index_features_v1",
  "reference_window": 20, "history_count": 0,
  "history_unavailable_reason": "insufficient_history",
  "indices": {"SPX": {
    "return_pct": "2", "acceleration_pp": null,
    "reference_mean_pct": null, "reference_sample_std_pct": null,
    "shock_z": null, "shock_unavailable_reason": "insufficient_history",
    "percentile_pct": null
  }},
  "return_spreads_pp": {"COMP-SPX": "2", "SOX-SPX": "4", "SOX-COMP": "2"}
}
```
