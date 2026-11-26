# US-rebound observation operations

`krx_us_rebound` is an independent, disabled-by-default runner. Observation mode
collects US-index history, seals inputs and records an exploratory `WATCH`,
`NO_SIGNAL` or `UNAVAILABLE` observation. Every evaluation retains
`order_authority: false`. A separate disabled-by-default rule handoff can prepare
and delegate explicitly configured paper watch families; it does not change either
decider or their daily pipeline. No production paper/live configuration is
enabled by this code change.

The implementation is in the [runner](../apps/api/app/overnight_rebound/runner.py),
[rules](../apps/api/app/overnight_rebound/rules.py),
[standalone CLI](../apps/api/app/internal/us_rebound.py) and
[read-only route](../apps/api/app/api/routes_us_rebound.py). It does not enter the
existing agents or daily pipeline, and its failures do not block those services.

## Activate observation

Deploy a reviewed remote commit containing this feature, then apply additive
snapshot migration `20261124_0091` through the normal Core deployment procedure
before registering the new job. Preserve the existing database rows and Core/KIS
configuration. Set these values in `apps/api/.env`:

```dotenv
US_REBOUND_MODE=observe
US_REBOUND_ENABLED_FROM=2026-11-27
US_REBOUND_STATE_DIR=var/us-index-preopen
```

`US_REBOUND_MODE` accepts `disabled`, `observe`, `paper` and `live`; live handoff
is rejected pending product approval. Observation requires an
explicit ISO start date; earlier dates do no work. The state setting is independent
of the daily pipeline. Keep the same persistent state directory across restarts
and deployments, shared with existing preopen attempt claims. Relative paths are
under `apps/api`; use an existing absolute persistent path if deployment replaces
the checkout. Keep the API and job on the same setting; CLI `--state-dir` overrides
only that invocation. The API exposes configuration loaded by its running process.

From the repository root, preview and then install the optional job:

```sh
bash scripts/install_us_rebound_launchd.sh --print
bash scripts/install_us_rebound_launchd.sh
```

Preview creates no directories and makes no launchd calls. The
[installer](../scripts/install_us_rebound_launchd.sh) resolves and pins an absolute
Python interpreter using the existing preopen precedence, preserving an explicit
inherited `PYTHON_BIN`. It enables `com.kiseyno.kai-us-rebound`, registers it, then
disables and boots out the optional `com.kiseyno.kai-us-index-preopen` job only
after successful registration. The old plist and all state remain available for
rollback. A failed registration leaves the legacy job untouched. Core, Exec and
daily services remain independent.
If disabling the legacy job fails after registration, installation reports an
error and attempts to stop/disable the new collector; verify launchd state before
retrying instead of treating that installation as successful.

The new job polls every 60 seconds, with `RunAtLoad=true` and `KeepAlive=false`.
Logs are `apps/api/var/log/us-rebound.log` and `us-rebound-error.log`. The
[wrapper](../scripts/run_us_rebound.sh) cheaply gates on KST before sourcing `.env`
or starting Python, independent of host timezone. A manual bounded tick uses
`bash scripts/run_us_rebound.sh`; Python rechecks its phase and deadline.

## Schedule and durable evidence

| KST phase | Behavior |
| --- | --- |
| 06:45–15:30 | Wrapper polling window; eligible sessions wait until collection. |
| 07:00, 07:05, 07:10 | Collect missing `SPX`, `COMP`, `SOX` histories with `include_history=True`: 22 exact US closes per index. |
| Before 07:12 | Reuse the existing shared attempt claims, combined maximum of nine fetches, 90-second tick budget and history input seal cutoff. |
| 07:15 until before 08:40 | Evaluate the current history seal; missing input records `unavailable` liveness and waits without recollection. |
| 08:20 and 08:30 | Only explicitly enabled paper operation may claim a bounded ETF/account evidence attempt. Observation never enters this path. |
| 08:40 onward | An unfinished evaluation becomes normal `UNAVAILABLE` with `reason: evaluation_expired`; saved evaluations replay unchanged. |

No new US session, a closed KRX session or special KRX hours produces a normal
`NO_SIGNAL` observation. Never reuse an older session to fill the gap. History
captures, [seals](US_INDEX_HISTORY.md#immutable-evidence-and-cutoff) and spent
claims retain their existing identities and cutoff rules.

The runner stores these files under the configured state directory:

```text
frozen-history/real-YYYY-MM-DD/{input.json,seal.json}
rebound/real-YYYY-MM-DD/evaluation.json
rebound/real-YYYY-MM-DD/run.json
rebound/real-YYYY-MM-DD/execution-operation.json
rebound/real-YYYY-MM-DD/execution-status.json
rule-execution/<private-account-session-hash>/<private-setup-hash>/{prepared.json,state.json}
```

`evaluation.json` is immutable after completion. Its `evaluation_id` hashes the
persisted body, including input and feature identities when present. Restarting
uses the same evaluation ID. `run.json` is mutable liveness (`checked_at`, status
and safe failure type). Top-level `completed` means evaluation evidence was
saved; use `evaluation.status` for the observation outcome. Technical failures
produce `failed` and CLI exit 1; missing data and calendar no-ops are normal exit 0.
Do not delete state, reset spent claims, backdate a seal or replace missing inputs
with a fabricated snapshot or score. Preserve the linked snapshot database too.

## Exploratory rule v1

`krx_us_rebound_exploratory_v1` requires full-history version-2 features and a
20-return lagged reference window. `WATCH` requires all four conditions:

- SPX and COMP returns are both strictly positive.
- COMP or SOX acceleration is strictly positive.
- At least one of SPX, COMP or SOX has both a shock z-score at least 2 and a
  reference percentile at least 95; it need not be the accelerating tech index.
- COMP or SOX outperforms SPX by a strictly positive return spread.

Zero reference volatility in any index makes the observation `UNAVAILABLE`.
Otherwise failed conditions give `NO_SIGNAL`. Thresholds are explicitly
`exploratory_unvalidated`; they are neither calibrated probabilities nor profit
claims and never produce `ENTRY_READY` or order authority.

## Inspect and calendar expectations

`GET /strategies/us-rebound/status?target_trade_date=2026-11-27` reads stored real
environment evidence only; omitted dates default to today in KST. Global bearer
authentication is unchanged. The route makes no provider or order calls and
returns no account or quantity fields. Corrupt/unreadable state returns a safe
503. See the [endpoint reference](../.codex/skills/fastapi-trading-api/references/api/us-rebound-status.md).

Nasdaq's [2026 calendar](https://www.nasdaqtrader.com/trader.aspx?id=Calendar)
(redirects to its current holiday schedule) and
[Thanksgiving alert ETA2026-69](https://www.nasdaqtrader.com/TraderNews.aspx?id=ETA2026-69)
confirm closure on November 26 and a 13:00 ET close on November 27. Consequently,
the runner records `NO_SIGNAL/no_new_us_session` on November 27 KST. The next
potentially usable input is the November 27 US early-close session for November 30
KRX, subject to complete history capture and sealing; that mapping follows the
runner's calendar checks and is not a promise of provider availability.

## Optional rule handoff

The [operation module](../apps/api/app/overnight_rebound/operation.py) connects the
[rule contract and private outbox](US_REBOUND_EXECUTION_CONTRACT.md) to the same
independent CLI. `observe` always bypasses execution, regardless of other flags.
Paper mode requires the single separate `US_REBOUND_EXECUTION_ENABLED=true`
opt-in plus every explicit field below. Missing policy/configuration is normal
execution unavailability, not a Core startup error. These are placeholders for
an authorized configuration, not instructions to enable the current deployment:

```dotenv
US_REBOUND_MODE=paper
US_REBOUND_EXECUTION_ENABLED=true
US_REBOUND_EXECUTION_ACCOUNT_REFERENCE=KIS:<account>-<product>
US_REBOUND_EXECUTION_POLICY_FILE=<absolute-private-policy-path>
US_REBOUND_EXECUTION_DESTINATION_BASE_URL=<explicit-Exec-origin>
US_REBOUND_EXECUTION_BEARER_TOKEN=<private-Exec-token>
US_REBOUND_EXECUTION_AUTHORITY_ID=<operator-approval-reference>
US_REBOUND_EXECUTION_AUTHORITY_UNTIL=<aware-operator-validity-timestamp>
```

The account must match the one valid route from the existing account-target
configuration. The destination/token are explicit and independent of original
decider execution flags. Native paper credentials are not provisioned: paper
operation reports unavailable before evidence/transport calls rather than using
real credentials. This release rejects new live handoffs with
`live_execution_not_approved`, even when a policy claims live validation and an
operator authority is supplied. Live activation needs separate explicit product
approval and a reviewed code change. No validation identity, score or
probability is fabricated.

The policy is an owned, private JSON file with no group/world permissions. Its
exact outer shape is shown below; the ellipsis is documentation, not a valid
policy or executable default:

```json
{
  "schema_version": "us_rebound_execution_policy_v1",
  "entry_expires_at_kst": "<HH:MM:SS>",
  "policy": {
    "execution_mode": "paper",
    "validation_status": "paper_only",
    "...": "every RuleExecutionPolicy parameter except entry_expires_at"
  }
}
```

All numerical policy parameters remain required. The explicitly approved KST
entry-expiry time binds to each authoritative target session and must follow
09:00, before its close exit; it is not the 08:40 enrollment cutoff. Operator
validity may span multiple sessions. Each session's watch authority binds exact
mode/account/evaluation/policy and expires at the earliest of 08:40 KST,
operator validity, and evidence TTL. It never claims preopen market BUY permission;
Exec still owns fresh OPEN-market and shared broker checks.

Only a current sealed `WATCH` can enter this path. New ETF/account evidence is
limited to two durable batch claims, 08:20–08:30 and 08:30–08:40 KST, at most two
pages per raw inquiry and a 90-second operation budget. The attempt is persisted
before reads; minute polling does not repeat a spent slot. The second slot may
refresh only a setup with no persisted grant or RPC debt. A prepared grant and
its prices/IDs are retained unchanged. No new US session, `NO_SIGNAL`, missing
configuration and disabled execution perform no execution/evidence HTTP.

The operation caches each immutable setup before grant preparation and uses the
existing outbox to register, verify exact readback and activate. Lost ACKs retain
debt; restart performs readback only for uncertain create/activation state and
does not send another POST or fetch fresh provider evidence. Confirmed active
families require no repeated provider or Exec polling from this job. Private
operation/outbox state must remain together; a missing operation record with
existing outbox debt fails closed rather than rebuilding a grant.

`execution-status.json` is a separate safe projection. The read-only API returns
its status/reasons, whitelisted symbols and actual `last_confirmed_at` timestamps;
it excludes accounts, quantities, prices, policy paths, tokens and private claim
hashes. `order_execution_enabled=true` requires today's KST target, current paper mode, the
separate true opt-in, matching persisted mode and confirmed active watch
authority. It is not fresh remote trading permission; Core market and Exec
pause/kill gates still apply. `checked_at` is job liveness; it does not
refresh `last_confirmed_at` without a new acknowledgement/readback. Disabling
new Core handoffs does not revoke an already accepted Exec family or its
protection; preserve its state and use the reviewed Exec controls when needed.

## Rollback and pending work

Set `US_REBOUND_MODE=disabled` in the job's `.env`, then stop only the optional job:

```sh
launch_domain="gui/$(id -u)"
launchctl disable "$launch_domain/com.kiseyno.kai-us-rebound"
launchctl bootout "$launch_domain/com.kiseyno.kai-us-rebound" 2>/dev/null || true
```

If restoring the preserved optional legacy job, after stopping rebound:

```sh
launchctl enable "$launch_domain/com.kiseyno.kai-us-index-preopen"
launchctl bootstrap "$launch_domain" "$HOME/Library/LaunchAgents/com.kiseyno.kai-us-index-preopen.plist"
```

Keep both plists, all checkpoints, input/seal files, evaluations and snapshot rows.
Do not stop or disable baseline Core, Exec or daily services. Retain readers that
understand history snapshot schema 2.

Live-versus-paper activation remains undecided. The rule-origin handoff and
recovery code is available behind explicit disabled defaults; actual paper
credentials, historical validation and live authorization remain separate work.
No observation setting or score substitutes for those contracts or validation.
