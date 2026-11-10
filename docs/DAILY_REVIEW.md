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

The review supplies measured coverage and bounded examples, prioritizing examples with a recorded execution or price event. A watch registration alone provides no price coverage. The analyst returns observations and implications ranked by relevance to each role. Core accepts one supported calibration focus per role when at least five WAIT decisions were tracked and the focus's source is available. Liquidity needs a recorded offer and price confirmation needs both trade and in-band offer evidence. Only a Core-owned sentence may be added to the canonical contract's bounded `AUTO-CALIBRATION` block. The review stores full drafts privately with status `ready_for_review`, `unchanged`, `insufficient_evidence`, or `calibration_block_full`. It does not install a draft or adjust execution policy.

## Schedule and recovery

The independent `com.kai.daily-review` launchd job checks on the hour and half-hour. The first eligible KRX slot is 16:30 KST, with catch-up until 06:00 KST the next calendar day. The review uses stored evidence at its cutoff and reports gaps in later-arriving inputs. A date lock prevents overlap. A completed date is immutable to the scheduler. Failures retry at least 25 minutes apart up to `SIDE_AGENTS_MAX_DAILY_ATTEMPTS` (three by default). Older weekly postmortems remain available via the legacy manual command.

Install the reviewed `postmortem-analyst` runtime contract using `scripts/sync_openclaw_runtime_contract.py --agent postmortem-analyst --install` and confirm it with `--check`. Run `scripts/install_side_agents_launchd.sh --daily-review` to install the dedicated job. Daily Review verifies OpenClaw storage capacity and that the canonical and live analyst files match. Its log is `var/side-agents/logs/daily-review.log`; the atomic records are `var/side-agents/daily-review/YYYY-MM-DD.json` or under `SIDE_AGENTS_OUTPUT_DIR` when set. The Core API and scheduler must share that directory.

Keep the analyst's OpenClaw model and reasoning setting aligned with both deciders. The current setting is `openai/gpt-6.1-sol` with `medium` reasoning.

## Web handoff

Use the existing server-side Core bearer credential to call `GET /agent-context/daily-review`. A date filter selects one KRX session and the default query returns recent dates newest first. Poll while status is `running` or `retry_wait`. Show the stage, attempt, evidence coverage, and the two role-specific findings and candidate statuses. Render agent text as plain text. See the [endpoint contract](../.codex/skills/fastapi-trading-api/references/api/agent-context-daily-review.md).
