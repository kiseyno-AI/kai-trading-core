# Prepared internal PREOPEN Summary continuation

Status: **preparation only, disabled for existing entrypoints**. The scheduler,
HTTP request schemas, settings, and operational callers do not supply the new
optional argument. Enabling a caller is a separate product decision.

`DailyPipelineStageService.run` accepts an internal
`preopen_continuation_source_pipeline_run_id`, default `None`, together with the
existing canonical `summary_cutoff_proof`. This identifies persisted authority;
it is not a permission boolean. With no source ID, the existing admission rules
remain in effect, including blocking a fresh previous-session recovery in OPEN.

The narrow supported case is a native automatic PITR recovery admitted in the
target session's PREOPEN, which failed its Summary parent-contract check before
creating any Summary work or decider/plan stage. Its frozen Summary retry must
have started during that same PREOPEN and must now be fully completed. A new
post-open Summary retry, a partial Summary intersection, generic historical
replay, and failure for a different reason remain ineligible.

Under the existing date admission transaction, the claim requires:

- The actual owned daily-price EOD fence, exact automatic decider/plan suffix,
  force false, feedback reuse true, and all normal prerequisite gates.
- The persisted native PREOPEN admission, original failure, original EOD run,
  and no decider/plan stage or prior work on the source pipeline.
- An authoritative next-KRX-session calendar result matching the current OPEN.
- A completed EOD source whose directly owned or repair-origin price rows cover
  the original requested subset. The EOD universe may contain extra symbols.
- The exact original feedback parent and direct retry link, frozen requested
  membership, per-role scopes, CFS/policy/cutoff/mode, and aware causal timestamps.
- A fully completed retry started before target OPEN, no newer summarizer or
  decider cohort, and validation by the existing completed-summary resolver in
  the same transaction. Caller-provided proof cannot replace persisted proof.

The claim creates a **new** pipeline cohort with the native
`preopen_summary_continuation_v1` audit record. It preserves the failed source
and its stage history. Concurrent calls serialize on the existing date lock;
a consumed source returns the existing cohort even after that cohort fails or
completes. It cannot mint another attempt from the same source. Existing native
stage retries continue to handle ordinary transient failures inside the cohort.

This changes cohort admission only. Existing post-open BUY/scalp/next-open
market-order tombstones remain in force. Only independently validated WAIT
lanes can reach the existing live-quote, watch-contract, and risk checks. No
order path, decision-lane independence, or latest-source callback barrier is
changed.

Verification uses isolated PostgreSQL test schemas, including an actual native
completed-summary resolver, concurrent one-shot claims, immutable failed-source
history, mismatched/unknown/late evidence, ordinary explicit and scheduled
admission, and existing late-BUY/scalp execution regressions. No operational
model run, production database write, deployment, or order was performed.
