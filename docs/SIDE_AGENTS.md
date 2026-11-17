# Side agents (market context, news events, postmortem)

Three OpenClaw analyst agents that run **beside** the per-symbol decision pipeline. They never feed the
deciders, never touch orders, and their outputs are advisory files until a human or a guarded process uses them.

| Agent | When (KST) | Inputs (all computed in code) | Output |
| --- | --- | --- | --- |
| `market-context-analyst` | trading day, from 21:30 once that day's bars and risk snapshots exist (catch-up until 06:00) | KIS KOSPI/KOSDAQ index (level, range, advancers/decliners, limit moves, 52-week position), KIS market-level investor flows (foreign, institution, individual, sub-groups), KIS US ETF proxies (SPY, QQQ, SOXX, EWY, TLT, UUP, USO, GLD), Bank of Korea daily rates and USD/KRW, stored Korea Exim FX, stored breadth by market and sector from `daily_market_risk_snapshots` plus five-session history | regime, trend, volatility, rotation, drivers, event risk, posture hint |
| `news-event-analyst` | same gate as the market agent | up to 30 symbols (news-driven and filing-driven interleaved): Naver headlines and snippets, official DART filing titles with an importance rule, 1d/5d price move and 20d volume ratio | per-symbol materiality, event type, direction, evidence grade, reaction check |
| `postmortem-analyst` | trading day, from 16:30 | KRX WAIT observations, confirmed outcomes, stored market, business and news context | role-specific [Daily Review](DAILY_REVIEW.md) and candidate contracts |

The weekly learning runner separately uses `weekly-learning-analyst` for its Korean evidence report. Core validates and publishes the learning artifact before the narrative phase. Both review agents use the same model and reasoning configuration as the two deciders. The weekly report's [runtime contract](../runtime_contracts/openclaw/v1/weekly-learning-analyst/AGENTS.md) is installed by the same contract sync command.

The evening gate checks that at least 500 daily bars and 500 risk snapshots exist for the date; until then the run reports `waiting` without using the attempt budget. KIS requests are sent one at a time, 0.25 s apart (the KIS client has no rate limiter and Core may be calling it too). Each live leg is isolated and carries a status. A failing KIS or BOK call is reported to the agent as an error, never hidden
and never filled in. For a past market date the live legs (index, flows, US proxies) are skipped by design.

## How it runs

`python -m app.side_agents run-due` (from `apps/api`) decides what is due, dispatches through the OpenClaw
gateway (`POST /v1/responses`, model `openclaw/<agent>`), validates the reply fail-closed and writes it to
`var/side-agents/<agent>/<unit>.json`. `scripts/install_side_agents_launchd.sh` installs a per-user launchd job
that calls the evening analysts every 30 minutes. `scripts/install_side_agents_launchd.sh --daily-review` installs the independent Daily Review job with the same interval. The schedule logic uses KRX time. The legacy weekly postmortem remains available as a manual command.

- **Idempotent:** a finished unit is never re-run. Interrupted news batches resume.
- **Quota-safe:** only failed or rejected attempts count; at most `SIDE_AGENTS_MAX_DAILY_ATTEMPTS` (3) per
  agent and day, at least 25 minutes apart. While Codex quota is exhausted this costs a few cheap failed calls.
- **Fail-closed:** an output that is not valid JSON, has the wrong enum, wrong date, or a missing/extra symbol is
  stored under `rejected/` and never becomes a result. Postmortem proposals need a sufficient sample, may only
  target `SOUL.md`/`AGENTS.md` of known agents and have confidence capped at 0.70.
- **No side effects:** nothing is written to the database; the only I/O is read-only DB access for the payloads.

Other commands: `run --agent <name> [--date YYYY-MM-DD] [--dry-run]`, `run-due --dry-run`, `status`.
Settings (env): `SIDE_AGENTS_OUTPUT_DIR`, `SIDE_AGENTS_TIMEOUT_SECONDS`, `SIDE_AGENTS_MAX_DAILY_ATTEMPTS`,
`SIDE_AGENTS_NEWS_MAX_SYMBOLS`, `SIDE_AGENTS_NEWS_BATCH_SIZE`, `SIDE_AGENTS_NEWS_LOOKBACK_HOURS`.

## OpenClaw side

Canonical contracts: `runtime_contracts/openclaw/v1/<agent>/AGENTS.md`, installed with
`scripts/sync_openclaw_runtime_contract.py --agent <name> --install`. The agents are registered in
`~/.openclaw/openclaw.json` with the same restrictions as the summarizers (no tools, no skills, no sessions).
Persona/knowledge files (`SOUL.md`, `IDENTITY.md`, `USER.md`) live only in the OpenClaw workspace.

## Data coverage (checked 2026-11-07)

Covered by APIs the system already uses: index, breadth, limit moves, market flows, US overnight proxies (ETFs, not index
closes), Korean rates and FX, sector rotation, official filings, price reaction. **Not covered:** an economic or earnings
calendar (reported to the agent as `not_available`), full news text (Naver gives headline and snippet only, so news
evidence is capped), US index levels and VIX (ETF proxies stand in), FRED series (no key configured).
The deciders produced no `buy` decisions in the six weeks before this was written, so the postmortem can judge
abstentions only.

## Not done / not verified

- Outputs have never been produced by a live model (Codex quota was exhausted when built); validators and the
  runner are covered by tests with a fake gateway only.
- Nothing consumes the outputs yet. Feeding the market context to the deciders is deliberately a later,
  separately reviewed step once real outputs exist.
- The postmortem uses a compact built-in outcome calculation (51 bp round trip, next-open entry), not the signal lab, which is a separate unmerged PR.
- The market snapshot cache (`/market/snapshot`) was empty when checked; this package does not depend on it.
