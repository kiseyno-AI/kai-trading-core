# US-index input freeze

This seventh integration slice freezes data only. After `data_complete` collection,
the standalone `app.internal.us_index_preopen` CLI calls `freeze_us_index_inputs`
to bind one exact committed snapshot ID for each of `SPX`, `COMP` and `SOX`.
It adds no strategy, score, rule, order, HTTP route, migration, installation,
deployment or live KIS call. The existing baseline, daily runner and Exec are
unchanged. The separate [stage 8 feature calculator](US_INDEX_FEATURES.md) now
consumes explicit sealed inputs; decision strategy remains future work.

Opt-in `--include-history` uses `frozen-history/<real|mock>-YYYY-MM-DD/` with
`us_index_history_input_v1`, `input_kind: history_22` and all three synchronized
22-close captures. The seal and two-phase cutoff rules below are shared; existing
`frozen/` files remain unchanged and are never enriched. See [history mode](US_INDEX_HISTORY.md).

## Selection and cutoff

A new freeze requires **07:00 <= current time < 07:12 Asia/Seoul** on the KRX
target date and the existing preopen calendar/session eligibility checks. Its
`as_of` is that date's 07:00 KST, with `cutoff_at` fixed at 07:12 KST. All three
exact database rows must match the index, source environment (`real` or `mock`),
`as_of` and the same frozen completed-US-session window. The snapshot IDs already
hash that session evidence and exact prices; the freeze does not refetch prices.
Each receipt must satisfy `as_of <= collected_at <= selected_at`.

The selection is recorded under the shared state directory:

```text
<state-dir>/frozen/<real|mock>-YYYY-MM-DD/input.json
<state-dir>/frozen/<real|mock>-YYYY-MM-DD/seal.json
```

`input.json` uses schema `us_index_input_v1` and binds the target date, environment,
`as_of`, cutoff, `selected_at` and all three snapshot IDs. Its `manifest_id` is the
SHA-256 of the canonical JSON body, excluding `manifest_id`. `selected_at` is read
after the committed database rows have been loaded and must precede the cutoff.
The existing fsync and atomic-replacement writer makes the file and its directory
durable; newly created parent directory links are also synced before reading
`confirmed_at`.

Only `selected_at <= confirmed_at < cutoff_at` permits a seal. `seal.json` uses
schema `us_index_seal_v1` and binds that `manifest_id` and `confirmed_at`, with a
canonical JSON SHA-256 `seal_hash` excluding itself. Seal publication may finish
after 07:12 because it binds a selection already durable and confirmed before
the cutoff. An input file alone does not establish a frozen input.

## Recovery and replay

| Evidence | Behavior |
| --- | --- |
| No seal, before cutoff | A complete valid ID set may create the first selection. An existing `input.json` candidate resumes its original IDs and `selected_at`; supplied replacement IDs are ignored. |
| No seal, at or after cutoff | No selection or promotion is allowed, even if receipts or the candidate timestamp precede the cutoff. The candidate is retained. |
| Valid existing seal | Replay its exact IDs, regardless of current time. Later captures and supplied replacement IDs do not change it. |
| Invalid sealed evidence or missing linked snapshot | Raise a technical error; never fall back to current/latest data, recompute calendars or replace the evidence. |

`load_frozen_us_index_inputs` is the internal replay surface. It returns `None`
when no seal exists. With a seal, it validates both hashes/versions, target and
environment bindings, the stored time window and confirmation, and all linked
database snapshot hashes/metadata. Missing `input.json` or database evidence is a
technical error. Replay uses stored session evidence without calendar or provider
calls; it can run in a fresh process after the collection window.

Keep one stable persisted state directory on one host across invocations and
restarts, with a correct host clock and storage honoring file/directory `fsync`.
A retained sidecar `flock` coordinates seal writers; separate directories
do not share the selection. Retain and back up both files together with the
referenced `us_index_input_snapshots` rows. Do not delete or edit files, reset
today's collection checkpoint, or remove a candidate/seal to choose different IDs.
Restore damaged evidence from a consistent backup rather than reselecting it.

The service account must be able to open and fsync each ancestor through the
filesystem's mount root. The collector can create the state directory itself;
an existing directory after interruption does not prove its link was synced.
Every retry therefore syncs the ancestors again. A permission/fsync error is a
technical failure and leaves an unsealed candidate; it is never ignored to claim
a successful freeze. Verify these permissions before enabling the optional job.

## Running and reading

From `apps/api`, with existing Core database/KIS configuration, the existing CLI
collects and then attempts freezing when complete:

```sh
python -m app.internal.us_index_preopen --state-dir var/us-index-preopen
```

On success it adds `input_freeze` with `status: frozen`, `manifest_id` and nested
`snapshot_ids`. A normal inability to obtain a seal adds `status: unavailable`.
Top-level `snapshot_ids` remain collection output and may differ from a seal
created earlier. Only the nested IDs identify the frozen selection. Technical
freeze failures produce `status: failed`, `stage: freeze`, a safe `error_type`
and exit 1; exception text is omitted. Invocations without `data_complete` do not
attempt freezing or add `input_freeze`.

Use the internal reader for an explicit date, including after 07:12; the scheduled
CLI still observes its collection window:

```sh
python - <<'PY'
from datetime import date
from app.market.us_index_input_freeze import load_frozen_us_index_inputs

record = load_frozen_us_index_inputs(
    target_trade_date=date(2026, 11, 25),
    source_environment='real',
    state_dir='var/us-index-preopen',
)
print('unavailable' if record is None else (record.manifest_id, record.snapshot_ids))
PY
```

The manifest proves the local input selection and cutoff contract. It is not a
provider-finality attestation, a computed feature or permission to trade. See
[collection operations](US_INDEX_PREOPEN_COLLECTION.md) for attempt budgets and the
separate opt-in launchd installation.

For read-only calculation, use `python -m app.internal.us_index_features
--target-trade-date YYYY-MM-DD` from `apps/api`, optionally naming earlier seals
with repeated `--history-date` arguments. The calculator does not discover
history or fetch replacement snapshots. A seal's two closes support current
returns; acceleration and normalization require explicit contiguous prior inputs.
KRX closures or special-hours no-ops may leave missing seals; do not backdate new
ones to fill history. Pass `--include-history` to the preopen and feature commands
to seal/replay the current full-history namespace instead. See [feature inputs and limits](US_INDEX_FEATURES.md).
