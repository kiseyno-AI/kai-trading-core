# Agent improvement loop

Goal: stop depending on a person reading evaluation reports. Every week the system measures its own signals,
decides by fixed rules whether the evidence justifies touching the agents, lets Codex word the change, and opens a
pull request that the existing Codex review checks. Nobody has to read the report.

## The money question

The agents and Codex cost about **500,000 KRW per month**. That is the hurdle: a signal has to earn more than that
after trading costs. `app.analytics.improvement_findings` prints, for every simulated signal, the capital at which its
net monthly edge would just pay the bill (`breakeven_capital_krw`), or `null` when there is no positive edge at any
capital. Today most signals have none, so the loop also reports honestly when the agents are not paying for themselves.

## Flow (`scripts/run_agent_improvement_local.sh` via launchd, Saturday from 09:00 KST)

1. **Evaluate** (`app.analytics.signal_lab`, read-only) every signal with the same rules.
2. **Decide** (`app.analytics.improvement_findings`, deterministic): a finding exists only if a verdict persisted over
   several weekly runs (2 for an inverse claim, 4 for "no skill"), the 14-day cooldown has passed, and, for deterministic
   factors, the effect survives the volatility control. A single noisy week never changes an agent.
3. **Act** (`scripts/agent_improvement_run.py`):
   * `text_change` (decider agents only): Codex edits only the calibration block of that agent's contract, in a
     `bot/agent-rules-*` branch, and the guard must pass before anything is pushed. One change per agent per run.
   * `code_issue` (summarizers, and deterministic signals worth exposing to the deciders): opens one deduplicated issue
     for the normal Codex/PR flow. The loop never edits code.
   * `observation`: logged only.
4. **Review** is the repository's existing flow: opening a PR triggers `@codex check it`. The loop never merges.

## What the automation may touch (`scripts/agent_rules_guard.py`, run by `agent-rules-guard.yml`)

* Only the text between `<!-- BEGIN AUTO-CALIBRATION -->` and `<!-- END AUTO-CALIBRATION -->` in
  `runtime_contracts/openclaw/v1/{signal-decider,short-term-decider}/AGENTS.md`, and the append-only
  `agent_rules/CHANGELOG.md`.
* Everything else in a contract (fail-closed rules, gate semantics, output schema) must stay byte-identical.
* At most 12 lines / 1,500 characters, plain bullets only, wording that could widen authority or leak is refused.
* The guard is not an allowed path, and the guard workflow (GitHub-hosted, PR-triggered) runs the base branch's copy so a PR cannot weaken it.
* Execution, risk, order and gate code is never in scope.

## Rollback and deployment

* Revert the PR (a one-block revert). The changelog records every change with its report and finding id.
* The canonical contract's SHA-256 is compared with the live workspace copy at runtime, so after merging a calibration PR
  run `python scripts/sync_openclaw_runtime_contract.py --install --agent signal-decider` (and `short-term-decider`) from
  the main checkout. The first merge of this loop also changes both files (the empty block), so it needs the same step.
  Auto-deploy is intentionally not wired yet.

## Setup the owner must do once (cannot be done from the repository)

1. **Install the launchd job on this Mac:** `scripts/install_agent_improvement_launchd.sh` (uninstall with
   `--uninstall`). It checks hourly and runs once per ISO week on Saturday from 09:00 KST against the live main
   checkout; everything (report, findings, logs) stays under `var/alpha-lab/`. No GitHub runner is involved, so the
   public repository never reaches this machine.
2. **Applying is opt-in:** create `var/alpha-lab/loop.env` with `AGENT_RULES_AUTO_PR=true` (or run the script with
   `--apply`). The PR it opens goes through CI, the rules guard and the Codex review like any other PR.
3. **Manual run:** `scripts/run_agent_improvement_local.sh --force` (dry run unless applying is enabled).
4. **Relation to the postmortem side agent:** the postmortem analyst (side agents) writes advisory proposals under
   `var/side-agents/postmortem-analyst/`; this loop is the only path that turns evidence into a guarded contract PR.

## What is not verified yet

The scheduled run, `codex exec` and the PR creation have not been exercised: they need Codex quota (exhausted when
written). The deterministic parts (guard, findings, prompt, command sequence, workflow
safety properties) are unit tested. Run the workflow once with `apply` off to see the dry run, then once with it on.
