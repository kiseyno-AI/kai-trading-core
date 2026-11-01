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
`audit_signal_evaluated=true`, proving a complete safe negative evaluation.
Diagnostics alone never promote an ambiguous gap to authoritative absence.

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
idempotently. Because the
current schema does not persist a negative audit-evaluation record, an
authoritative-absence checkpoint is replayed unless the caller supplies a
separate durable negative proof. Failed, blocked, or incomplete batches are
retried with the same immutable batch hash.

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
protected-provider proof. It additionally requires strict same-date completed
EOD run metadata. Its stored-only policy never calls the live trade-date
resolver or the PyKRX market loader.

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
revalidate those inputs before and after publication.

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
