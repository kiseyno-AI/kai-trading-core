# Agent improvement loop

Goal: stop depending on a person reading evaluation reports. Every week the system measures its own signals,
decides by fixed rules whether the evidence justifies touching the agents, lets Codex word the change, and opens a
pull request that the existing Codex review checks. Nobody has to read the report.

## The money question

The agents and Codex cost about **500,000 KRW per month**. That is the hurdle: a signal has to earn more than that
after trading costs. `app.analytics.improvement_findings` prints, for every simulated signal, the capital at which its
net monthly edge would just pay the bill (`breakeven_capital_krw`), or `null` when there is no positive edge at any
capital. Today most signals have none, so the loop also reports honestly when the agents are not paying for themselves.

## Flow (`.github/workflows/signal-lab-weekly.yml`, Saturday 09:00 KST)

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
* The guard is not an allowed path, and the workflow runs the base branch's copy so a PR cannot weaken it.
* Execution, risk, order and gate code is never in scope.

## Rollback and deployment

* Revert the PR (a one-block revert). The changelog records every change with its report and finding id.
* The canonical contract's SHA-256 is compared with the live workspace copy at runtime, so after merging a calibration PR
  run `python scripts/sync_openclaw_runtime_contract.py --install --agent signal-decider` (and `short-term-decider`) from
  the main checkout. The first merge of this loop also changes both files (the empty block), so it needs the same step.
  Auto-deploy is intentionally not wired yet.

## Setup the owner must do once (cannot be done from the repository)

1. **Register a self-hosted runner on this Mac** (Settings > Actions > Runners > New self-hosted runner, macOS),
   label it `kai-core`, and keep it running. It needs the existing `gh` login and the `codex` CLI.
2. **The repository is public.** Before enabling any self-hosted runner set Settings > Actions > General > "Fork pull
   request workflows": require approval for all outside collaborators. The lab workflow has no `pull_request` trigger and
   only runs on main, but this setting is the backstop.
3. Optional repository variables: `KAI_CORE_HOME` (default `/Users/shawn/kai-trading-core`) and
   `AGENT_RULES_AUTO_PR=true` to let the schedule create PRs and issues (default is a dry run; a manual run with
   `apply` does it once).
4. Reports and the verdict history stay in `var/alpha-lab` on the Mac; Actions logs of a public repository are public,
   so no number is printed there and PR/issue text carries verdicts only.

## What is not verified yet

The scheduled run, the runner, `codex exec` and the PR creation cannot be exercised from here: they need the runner
and Codex quota (currently exhausted). The deterministic parts (guard, findings, prompt, command sequence, workflow
safety properties) are unit tested. Run the workflow once with `apply` off to see the dry run, then once with it on.
