# Targeted Repair Tooling

These commands repair reviewed, immutable scopes. They do not run the daily
pipeline, a decider, an order planner, or Exec. Every command defaults to a
read-only dry run. Apply mode requires the SHA-256 printed by a fresh dry run
and a durable checkpoint path.

## Exact DART audit signals

Manifest shape:

```json
{
  "schema_version": "dart_audit_signal_exact_manifest_v1",
  "market_date": "2026-10-28",
  "entry_count": 1,
  "batch_size": 25,
  "entries": [
    {
      "stock_code": "147760",
      "event_id": 108389,
      "rcept_no": "20260316001657",
      "replacement_for": {
        "stock_code": "147760",
        "event_id": 108388,
        "rcept_no": "20260317000801",
        "source_error_code": "014",
        "reason": "reviewed source document is unavailable"
      }
    }
  ]
}
```

The optional `replacement_for` object is the only replacement authority. The
runner verifies both stored event identities and never discovers or substitutes
another filing on its own. Both the target and replacement must have a persisted
receipt date on or before the manifest market date; null or future-dated
receipts are rejected. A DART `013`/`014` provider response stays failed
retry debt; no synthetic audit opinion is written. A no-signal result is
terminal only when the capture service explicitly returns
`audit_signal_evaluated=true` and durably upserts a versioned
`authoritative_absence` proof bound to the event, receipt, archive digest, and
complete bounded diagnostics. That proof is not an unqualified opinion and
remains fail-closed for Piotroski eligibility. Diagnostics alone never promote
an ambiguous gap to authoritative absence.

```bash
cd apps/api
.venv/bin/python -m app.dart.exact_audit_signal_repair \
  --manifest /reviewed/dart-audit-manifest.json --dry-run

.venv/bin/python -m app.dart.exact_audit_signal_repair \
  --manifest /reviewed/dart-audit-manifest.json \
  --checkpoint /durable/dart-audit-checkpoint.json \
  --expected-manifest-sha256 SHA_FROM_DRY_RUN \
  --concurrency 4 --apply
```

Before every batch, apply mode takes the daily/EOD and DART advisory locks,
fences the writer run tables, and refuses to run while any daily pipeline, EOD,
DART sync, fundamentals capture, or fundamentals refresh row is active.
Completed checkpoint batches are skipped only after the stored event identity,
receipt date, and authoritative audit opinion/issue content are revalidated
while the writer fence is held. Missing or changed captured rows are replayed
idempotently. A versioned authoritative-absence row is terminal for repair and
retry accounting only after its payload identity and diagnostic invariants are
revalidated; it is never reinterpreted as a clean audit opinion. Failed,
blocked, malformed-proof, or incomplete batches are retried with the same
immutable batch hash.

DART audit repair checkpoints use
`dart_audit_signal_exact_checkpoint_v2`. The v2 contract records terminal
durable audit content for live revalidation, so a v1 checkpoint is explicitly
incompatible and apply mode blocks it as `checkpoint_manifest_mismatch`.
Resume this repair only from a checkpoint already created as v2, or provide a
new checkpoint path; do not reuse or relabel a v1 checkpoint.

## Exact stored-data derived stages

Each date entry must contain the complete, sorted stored price-bar membership
for that date and its membership hash.

```json
{
  "schema_version": "derived_stage_exact_date_manifest_v1",
  "date_count": 1,
  "dates": [
    {
      "market_date": "2026-10-28",
      "symbol_count": 2,
      "membership_sha256": "SHA256_OF_SORTED_NEWLINE_JOINED_SYMBOLS",
      "symbols": ["000660", "005930"]
    }
  ]
}
```

```bash
cd apps/api
.venv/bin/python -m app.eod.derived_stage_repair \
  --manifest /reviewed/derived-date-manifest.json --dry-run

.venv/bin/python -m app.eod.derived_stage_repair \
  --manifest /reviewed/derived-date-manifest.json \
  --checkpoint /durable/derived-date-checkpoint.json \
  --expected-manifest-sha256 SHA_FROM_DRY_RUN --apply
```

The apply path holds exclusive daily-price writer admission across the complete
history read and publication window, fences the daily/EOD and DART writer run
tables, refuses active writers, and runs only:

1. stored daily bars to price contexts;
2. successful price-context members to technical snapshots;
3. successful technical members to action summaries.

Every stage receives the original manifest membership as its exact publication
authority. Successful writes, stale-row deletion, and exact-set verification
commit in one service transaction, so an upstream or per-symbol failure cannot
leave a stale same-date row falsely ready. No network market-data fetch, force
pipeline, decider, order, or Exec call is in this driver.

Completed checkpoint dates are skipped only after deterministic source-content
and output-content fingerprints, all three output memberships, and each atomic
scope-finalization proof are revalidated under the writer fence. A deleted or
changed output triggers an idempotent exact-date rebuild; input content or
membership drift blocks the rebuild.

## Exact stored-data trending ranks

The trending repair manifest is the exact frozen ranking scope from one pending
price-bar repair batch. Stored daily bars may be a superset, but every frozen
symbol must be present; protected provider-only extras remain outside ranking
publication authority. It computes only stored-data `1d`, `5d`, and `20d`
ranks, then
records the resulting output fingerprints with
`record_pending_trending_rebuild` for closeout.

```bash
cd apps/api
.venv/bin/python -m app.eod.exact_trending_repair \
  --manifest /reviewed/derived-date-manifest.json --dry-run

.venv/bin/python -m app.eod.exact_trending_repair \
  --manifest /reviewed/derived-date-manifest.json \
  --checkpoint /durable/trending-date-checkpoint.json \
  --expected-manifest-sha256 SHA_FROM_DRY_RUN --apply
```

Apply mode holds the same exclusive writer fence and refuses scope or source
dependency drift. All
three periods must finish with the manifest count/hash and produce non-empty
fingerprints before pending repair evidence is recorded. A completed date is
skipped only when live output fingerprints and pending-batch metadata still
match its checkpoint; otherwise the stored-data rebuild is safely repeated.
The driver has no provider fetch, official/force pipeline, decider, order, or
Exec call.

## Exact stored-data factor stages

The factor bulk repair uses the exact frozen ranking scope from the pending
price-bar repair state. Stored daily bars may be a superset, but every frozen
symbol must be present and every provider-only extra must match the reviewed
protected-provider proof. Membership authority must come from either strict
same-date completed EOD run metadata or a canonical, validated pending
missing-bar PyKRX proof bound to the frozen scope and current persisted input
fingerprint. The pending proof is persisted evidence only: stored-only policy
never calls the live trade-date resolver or the PyKRX market loader.

Dry run writes a per-date reviewed provider count/hash proof into the dedicated
checkpoint. Apply refuses a missing or changed proof, revalidates the manifest
membership and frozen membership under the writer fence, and runs the existing
exact factor repair with `prune_stale=true`.

```bash
cd apps/api
.venv/bin/python -m app.eod.exact_factor_bulk_repair \
  --manifest /reviewed/derived-date-manifest.json \
  --checkpoint /durable/factor-date-checkpoint.json --dry-run

.venv/bin/python -m app.eod.exact_factor_bulk_repair \
  --manifest /reviewed/derived-date-manifest.json \
  --checkpoint /durable/factor-date-checkpoint.json \
  --expected-manifest-sha256 SHA_FROM_DRY_RUN --apply
```

Each factor stage atomically publishes its successful exact-scope rows and
removes stale rows for failed members before returning an exact-scope
finalization proof. Only after all stages succeed does the existing
outside-membership stale prune run. Pending repair evidence records the exact
raw-input hash and all nine factor output fingerprints.

The bulk guard holds exclusive daily-price admission for the entire 253-row
history window, the DART audit/dilution writer lock, and a shared `symbols`
table lock. Apply rejects placeholder symbol names before compute. Checkpoints
also fingerprint price history, symbol metadata, technical inputs, valuation
enrichment, annual DART fundamentals, audit signals, and dilution signals, and
revalidate those inputs before and after publication. Preferred-share scopes
include both the preferred symbol and the resolved common-share issuer. DART
fingerprints cover the union of the exact filings selected by Piotroski and by
Advanced/Unified/Quality, their consumed rows and highlights, the current
Piotroski audit receipt, and its bounded dilution-evidence window. A newly
eligible winning filing invalidates the checkpoint; historical filings that no
factor consumer can select do not inflate the review proof.

Factor bulk checkpoints use `factor_exact_date_bulk_checkpoint_v2` and bind
each dry proof and completion result to
`factor_exact_date_bulk_implementation_v2`. Older checkpoints must be replaced
with a fresh v2 dry review. Pending-repair membership lineage is re-resolved
from the currently locked pending batches during final verification; the proof
hash, provider hash, exact membership, input hash, and batch IDs must all still
match the recorded factor evidence.

A completed checkpoint date is skipped only after a fresh stored-only dry proof
matches the reviewed proof, all live factor table memberships and fingerprints
match, and the pending repair evidence still matches the raw input and outputs.
Deletion or output drift causes an idempotent rerun; provider, frozen, or
manifest authority drift requires a new dry review. The driver has no live
provider fetch, official/force pipeline, decider, order, or Exec call.

Direct publication through `python -m app.eod.exact_factor_repair --apply` is
intentionally blocked. That lower-level service remains an implementation
dependency of the guarded bulk driver, but its executable cannot bypass the
bulk checkpoint, full-source fingerprint, or writer-admission contract.

## Exact stored-data signal features

The signal-feature repair accepts the same reviewed
`derived_stage_exact_date_manifest_v1` used by the factor bulk repair. It
rebuilds `daily_signal_features` only from stored bars, factor rows, symbol
metadata, and each row's already persisted KIS ranking and historical quote
evidence. It never calls KIS, an agent, a decider, a plan builder, an order
path, or Exec.

```bash
cd apps/api
.venv/bin/python -m app.eod.exact_signal_feature_repair \
  --manifest /reviewed/factor-date-manifest.json \
  --checkpoint /durable/signal-feature-checkpoint.json --dry-run

.venv/bin/python -m app.eod.exact_signal_feature_repair \
  --manifest /reviewed/factor-date-manifest.json \
  --checkpoint /durable/signal-feature-checkpoint.json \
  --expected-manifest-sha256 SHA_FROM_DRY_RUN --apply
```

Dry run is the default and writes the date-scoped source fingerprints plus the
reviewed stale-extra proof into a dedicated checkpoint. Apply requires that
fresh proof and the printed manifest SHA-256. For every date it revalidates
market finality, the pending frozen factor membership, stored-bar coverage,
all factor source fingerprints, and the stale-extra count/membership/content
hash while holding exclusive daily/EOD writer admission. It first persists
and freshness-verifies every manifest member. Only then does it lock
`daily_signal_features`, compare the reviewed stale proof, remove outside
rows, and verify that the remaining date membership is exactly the manifest.

A completed date is skipped only after live source, membership, and output
fingerprints still match its checkpoint. Missing or changed exact output is
rebuilt idempotently. A new outside row or any source drift requires another
dry review; it is never silently pruned under an older proof. The legacy
factor checkpoint is not a signal checkpoint and cannot be reused.

This command owns only `daily_signal_features`; it does not mark a pending
price-bar repair batch complete. After a successful apply, continue the
documented `app.eod.price_bar_repair_state --rebuild-evaluations`, verify, and
`--complete` sequence so stored market-participation, evaluation, calendar,
and closeout fingerprints are refreshed from the repaired signal scope.
