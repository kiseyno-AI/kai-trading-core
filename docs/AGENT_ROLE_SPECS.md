# Agent role specifications

Canonical role contracts for the OpenClaw agents live in `runtime_contracts/openclaw/v1/<agent>/AGENTS.md`.
They are copied into the live workspaces with `scripts/sync_openclaw_runtime_contract.py`; the SHA-256 of
the canonical file is the runtime provenance recorded with each decision.

| Agent | Role | Authority |
| --- | --- | --- |
| `eod-summarizer` | Data-readiness analyst | Reports input quality; no trade view |
| `technical-summarizer` | Technical analyst | Trend/momentum/levels/invalidation; no decision |
| `fundamental-summarizer` | Fundamental and credit analyst | Quality/risk filter; no timing, no decision |
| `signal-decider` | Final reviewer, swing/longer lane | Veto-only (preserve or make safer) |
| `short-term-decider` | Final reviewer, 2–3 session lane | Veto-only (preserve or make safer) |

## What changed in this revision

- **Summarizers are now version-controlled.** Before, their workspace `AGENTS.md` was the generic assistant
  template (diary, heartbeat, pasted tool notes) and only the request prompt defined the work. Each contract
  now keeps the input-boundary and output rules and adds identity, an ordered method, Korean-market specifics,
  calibration bands and a `details.house_view` structure.
- **Deciders gained a "Professional standard" section** placed before `## Output contract`. It is add-only:
  every existing safety sentence is unchanged and the section states it never widens authority.
- Summarizer `risk_flags` use a fixed code vocabulary (`code: 한국어 설명`) so flags can be counted and
  evaluated by the signal lab instead of being free text.

## Design sources

- Role decomposition, bull/bear steelman, pre-mortem and structured-output ideas: TauricResearch/TradingAgents
  (Apache-2.0), adapted, not copied.
- Calibration bands and the "use the whole range" rule address the measured clustering of agent confidence
  (most results near 0.65–0.75) found while evaluating the stored feedback.
- Not-applicable handling of after-close microstructure follows the project's own finding that spread/depth
  were reported as blockers when the market was closed.
- Korean-market specifics (±30% price limit, VI, call auctions, consolidated vs separate statements,
  administrative-issue designations) come from exchange rules and DART conventions.
- Round-trip cost of about 0.5% is the assumption used by `docs/SIGNAL_EVALUATION_LAB.md`.

## Deploying

Nothing is installed automatically. After merge, per agent:

```bash
python scripts/sync_openclaw_runtime_contract.py --agent technical-summarizer            # check
python scripts/sync_openclaw_runtime_contract.py --agent technical-summarizer --install  # install
```

The decider contract SHA changes with this revision, so decider workspaces must be re-synced before the next
decision run or provenance will report drift. Other workspace files (`IDENTITY.md`, `SOUL.md`, `USER.md`,
`DREAMS.md`, ...) are not managed here; the contracts say they do not apply to the role.

## Not verified

- Output quality of the new specs has not been exercised against a live model (Codex quota exhausted when
  written). Tests check structure and preserved safety text only.
- Whether the request schemas accept `details.house_view` in every stage must be confirmed per stage; the
  contracts only use it "when the request schema allows free-form `details`".
- Value of the additions is unproven until the signal lab compares pre/post-change agent feedback.
