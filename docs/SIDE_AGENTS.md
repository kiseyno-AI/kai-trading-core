# Side agents (market context, news events, postmortem)

Three OpenClaw analyst agents that run **beside** the per-symbol decision pipeline. They never feed the
deciders, never touch orders, and their outputs are advisory files until a human or a guarded process uses them.

| Agent | When (KST) | Input | Output |
| --- | --- | --- | --- |
| `market-context-analyst` | trading day, from 18:30 | stored market snapshot + macro context | regime, trend, volatility, rotation, drivers, event risk, posture hint |
| `news-event-analyst` | trading day, from 19:00 | recent external news for up to 30 symbols (batches of 10) | per-symbol materiality, event type, direction, evidence grade, reaction check |
| `postmortem-analyst` | Saturday, from 09:00 | latest `var/alpha-lab/report-*.json` | classification, calibration, patterns, up to 5 advisory proposals |

## How it runs

`python -m app.side_agents run-due` (from `apps/api`) decides what is due, dispatches through the OpenClaw
gateway (`POST /v1/responses`, model `openclaw/<agent>`), validates the reply fail-closed and writes it to
`var/side-agents/<agent>/<unit>.json`. `scripts/install_side_agents_launchd.sh` installs a per-user launchd job
that calls it every 30 minutes; the schedule logic lives in code, so DST and the Mac's US-Eastern clock do not matter.

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

## Not done / not verified

- Outputs have never been produced by a live model (Codex quota was exhausted when built); validators and the
  runner are covered by tests with a fake gateway only.
- Nothing consumes the outputs yet. Feeding the market context to the deciders is deliberately a later,
  separately reviewed step once real outputs exist.
- The postmortem input is the signal-lab JSON, which exists only once the signal lab is merged.
