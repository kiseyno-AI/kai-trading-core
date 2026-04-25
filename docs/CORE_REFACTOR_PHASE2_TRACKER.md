# Core Refactor Phase 2 Tracker

This document tracks the current phase-2 EOD and agent-context refactor stream behind GitHub issue #188.
It is based on the live GitHub issue and PR state as of 2026-04-25, not only the original tracker issue text.

## Current state

- The structural phase-2 refactor work is largely landed on `main`.
- Landed child issues:
  - #179 via merged PR #180 on 2026-04-25
  - #181 via merged PR #184 on 2026-04-25
  - #182 via merged PR #185 on 2026-04-25
  - #183 via merged PR #186 on 2026-04-25
  - #187 via merged PR #191 on 2026-04-25
- Remaining open child issues:
  - #190 Define latest/today/running semantics contract for EOD freshness and pipeline status
  - #189 Add dedicated regression test pack for EOD freshness and pipeline status semantics
- Important pre-phase context already landed:
  - #175 via merged PR #176 on 2026-04-24
  - #177 via merged PR #178 on 2026-04-24

## Recommended execution order

| Order | Issue | State | Why this is the right position | Dependency notes |
| --- | --- | --- | --- | --- |
| 1 | #179 / PR #180 | Landed | TC-first behavior lock and first mechanical split established the safe refactor base. | Base prerequisite for the narrower phase-2 follow-ups. |
| 2 | #181 / PR #184 | Landed | Standardized repository return payloads so later helpers stop depending on session-bound ORM rows. | Builds directly on the split and test coverage from #179. |
| 3 | #182 / PR #185 | Landed | Shared run/stage mapping helpers reduced duplicated interpretation logic across freshness and pipeline status. | Safer after #181 stopped leaking mixed repository return types. |
| 4 | #183 / PR #186 | Landed | Second-pass helper consolidation cleaned up duplication left behind after the larger file splits. | Only worth doing after #179 landed and after #182 exposed the common mapper layer. |
| 5 | #187 / PR #191 | Landed | Deployment and migration hygiene followed the same incident stream and is no longer pending work. | Parallel operational follow-up, not a blocker for the remaining semantics and regression tickets. |
| 6 | #190 | Next | The remaining ambiguity is no longer structural; it is semantic. Write the contract against the behavior now shipped on `main`. | Use the current `/eod/freshness` and `/eod/pipeline/status` behavior, plus the already-landed fixes in #175 and #177, as the baseline. |
| 7 | #189 | Next after #190 | Once the contract is explicit, freeze it in a dedicated regression pack and use that pack as the gate for future behavior changes. | Test scaffolding can be prepared in parallel, but final assertions should align with #190. |

## Dependency notes

- #181 depended on #179's file and repository split because the return-type cleanup needed the new boundaries to be worth standardizing.
- #182 depended on #181 because shared freshness and pipeline helpers are safer once repository calls return plain values instead of ORM-bound rows.
- #183 depended on #179 and benefited from #182 because second-pass consolidation should happen after the first-pass split and common mapper extraction are stable.
- #187 was parallel operational follow-up. It should stay out of the critical path for #190 and #189 unless a semantics decision requires a deploy-health check note.
- #190 should document current behavior first, not reopen the behavior-preserving split work from #179 through #183.
- #189 should reuse and sharpen the existing targeted coverage instead of duplicating broad route coverage.

## Current sources of truth for the remaining work

- `docs/EOD_STATUS_SEMANTICS_CONTRACT.md`
- `docs/API_BUILD_ORDER.md`
  - `EOD freshness read-layer notes`
  - `EOD pipeline status notes`
- `apps/api/app/eod/run_stage_support.py`
- `apps/api/tests/test_eod_freshness.py`
- `apps/api/tests/test_eod_pipeline_status.py`
- `apps/api/tests/test_eod_run_stage_support.py`
- `apps/api/tests/test_eod_route.py`

## Done versus next

Done:

- behavior-preserving split and TC-first lock
- repository return-type cleanup
- shared run and stage mapper extraction
- reusable pykrx and orchestration helper consolidation
- deployment and migration hygiene follow-up

Next:

- define the latest/today/running semantics contract in #190 against the code now on `main`
- turn that contract into a dedicated regression pack in #189
- require that regression pack for future freshness or pipeline status behavior changes
