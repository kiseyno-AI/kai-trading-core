# US-index full-history inputs

Stage 9 adds opt-in 22-close history to the existing US-index capture, freeze and
descriptive-feature path. `include_history=True` / `--include-history` selects it;
all defaults retain the existing two-close behavior. This work does not schedule,
install or deploy anything, or connect agents, rules, scores, orders or the baseline.

## Collection and limits

The collector still makes one sequential request per pending index (`SPX`, `COMP`,
`SOX`), widening only the start date to the oldest of 22 exact completed US-session
closes. The same KIS endpoint/TR, authentication, pacing and transport limits apply.
The [official range sample](https://github.com/koreainvestment/open-trading-api/blob/main/examples_llm/overseas_stock/inquire_daily_chartprice/inquire_daily_chartprice.py)
documents start/end `YYYYMMDD`, market `N` and period `D`. The checked samples give
no numeric row cap or guarantee that every requested close arrives in one page;
request bounds alone do not establish complete history or provider finality.

Validation matches required dates by `stck_bsop_date` and uses each row's
`ovrs_nmix_prpr`, as named in the [official field sample](https://github.com/koreainvestment/open-trading-api/blob/main/examples_llm/overseas_stock/inquire_daily_chartprice/chk_inquire_daily_chartprice.py).
It does not depend on provider row order. Any missing required date is normal
`unavailable`; malformed data is a technical failure. Never substitute an older
date, zero return, header price or sibling index. The [official functions sample](https://github.com/koreainvestment/open-trading-api/blob/main/examples_user/overseas_stock/overseas_stock_functions.py)
treats `tr_cont=M/F` as continuation; Core retains its existing rejection of those
responses. No extra page, retry or fallback loop is added.

Preopen history and two-close modes share the same date/environment checkpoint
and 07:00/07:05/07:10 KST claims in one persisted state directory: **at most nine
fetches combined**, not nine per mode. Changing modes does not reset spent slots.
The existing 90-second maximum tick and strict window before 07:12 still apply.
History recovery filters for complete histories before choosing the first capture;
two-close recovery may reuse a history capture's current pair. Retain the state
directory and spent claims across restarts. See [preopen operations](US_INDEX_PREOPEN_COLLECTION.md).

## Immutable evidence and cutoff

History captures use append-only snapshot schema `2` in the existing
`us_index_input_snapshots` table. The current `USIndexSessionPrices` pair plus
20 prior pairs cover 22 unique closes. Every pair's exact prices and frozen session
metadata enter the content hash. `USIndexInputSnapshotRecord.history_prices` is
a tuple, empty by default. Schema `1` serialization and IDs remain unchanged;
no migration or dependency is added.

Explicit history mode selects a separate namespace under the same state directory:

```text
<state-dir>/frozen-history/<real|mock>-YYYY-MM-DD/input.json
<state-dir>/frozen-history/<real|mock>-YYYY-MM-DD/seal.json
```

The input schema is `us_index_history_input_v1`, with `input_kind: history_22`;
`USIndexInputFreezeRecord.input_kind` defaults to `session` for existing inputs.
All three full histories must have synchronized frozen sessions. Mixed or partial
history is rejected. The generic `us_index_seal_v1` seal and existing two-phase
durability contract still require selection and confirmation before 07:12 KST.
An existing session seal cannot authorize a history selection after that cutoff.
Existing `frozen/` files remain byte-unchanged: never enrich, rewrite or backdate
an older seal. See [freeze and recovery](US_INDEX_INPUT_FREEZE.md).

## Explicit commands and replay

From `apps/api`, with the existing Core database/KIS configuration for collection.
Choose manual capture or preopen collection; only preopen attempts sealing:

```sh
python -m app.internal.us_index_collect --include-history
python -m app.internal.us_index_preopen --include-history --state-dir var/us-index-preopen
python -m app.internal.us_index_features --include-history --target-trade-date 2026-11-25 --state-dir var/us-index-preopen --source-environment real
```

The feature command is read-only and cannot combine `--include-history` with
`--history-date`. It loads the current history seal and calculates schema
`us_index_features_v2`, with `history_source: current_snapshot_history`. The
current capture directly supplies the 20 lagged pairs; it never manufactures
20 older freeze records. Its feature ID binds the current manifest and full
history. Replay uses stored evidence without provider or calendar calls. The
existing schema `us_index_features_v1` path with explicit older seals is unchanged.

A current US-session range can cover missing legacy seals caused by KRX holidays
or special-hours no-ops, provided all required closes are captured, selected and
durably confirmed before the current cutoff. This fills history without backdating evidence;
it does not establish publication finality, predictive value or trading authority.

## Rollback

Stop opting into history to restore default collection behavior. After writing
schema `2`, retain readers that understand it; older readers cannot interpret
those captures. Never rewrite or delete immutable captures to downgrade. Keep
both history files and referenced snapshots together in backups. Strategy
validation, 07:15 evaluation, scheduling and deployment enablement remain separate.
