# KRX Daily Review

Daily Review examines one closed KRX session. The postmortem analyst reviews the two deciders independently, comparing their WAIT monitoring results with recent sessions and the market, business, and news evidence available at the review cutoff. The review operates outside `daily_pipeline_runs` and runs even when a daily pipeline stage failed. Nothing in the decision or execution flow waits for its result.

## Requirements and evidence

| Requirement | Why it matters |
| --- | --- |
| Join monitoring and execution by decision snapshot ID and keep the roles separate | Each decider supplied its own trigger and holding horizon. Overlapping symbols are two decisions. |
| Count trade-price touches, best-ask touches, in-range offers and confirmed fills separately | A printed trade or displayed offer gives different evidence about whether an entry could have filled. Displayed size is context and does not prove a fill. |
| Use timestamped stored evidence for the reviewed session and up to five recent KRX sessions | Market breadth, company fundamentals, filings, news and indicator context support comparison without pulling in later data. Missing or older evidence stays visible as such. |
| Keep a completed run and the two candidate contracts together | The result exposes a reproducible version of each draft with its base hash, even when that role retained its base contract. |
| Treat daily findings as hypotheses and keep activation separate | A daily sample can suggest what to examine next. A contract change still goes through normal review and deployment. |

The review supplies measured coverage and bounded examples. It keeps monitored WAIT decisions separate from the whole candidate universe, and unavailable evidence separate from a measured zero. The analyst returns observations and implications ranked by relevance to each role. Core accepts one supported calibration focus per role when at least five WAIT decisions were tracked and the focus's source is available. Only a Core-owned sentence may be added to the canonical contract's bounded `AUTO-CALIBRATION` block. The review stores full drafts privately with status `ready_for_review`, `unchanged`, `insufficient_evidence`, or `calibration_block_full`. It does not install a draft or adjust execution policy.

## Schedule and recovery

The Core side-agent launcher invokes `python -m app.side_agents run-daily-review` from 21:30 KST on exchange sessions, with catch-up before 06:00 KST. The window allows the existing evening jobs to make current context available. A date lock prevents overlap. A completed date is immutable to the scheduler. Failures retry at least 25 minutes apart up to `SIDE_AGENTS_MAX_DAILY_ATTEMPTS` (three by default). The same date can be retried automatically during its window. Older weekly postmortems remain available via the legacy manual command.

Install the reviewed `postmortem-analyst` runtime contract using `scripts/sync_openclaw_runtime_contract.py --agent postmortem-analyst --install` and confirm it with `--check`. Daily Review verifies the canonical and live analyst files match before and after dispatch. The regular side-agent launchd job invokes the dedicated command without a new launch agent. Its log is `var/side-agents/logs/daily-review.log`; the atomic records are `var/side-agents/daily-review/YYYY-MM-DD.json` or under `SIDE_AGENTS_OUTPUT_DIR` when set. The Core API and scheduler must share that directory.

## Web handoff

Use the existing server-side Core bearer credential to call `GET /agent-context/daily-review`. A date filter selects one KRX session and the default query returns recent dates newest first. Poll while status is `running` or `retry_wait`. Show the stage, attempt, evidence coverage, and the two role-specific findings and candidate statuses. Render agent text as plain text. See the [endpoint contract](../.codex/skills/fastapi-trading-api/references/api/agent-context-daily-review.md).
