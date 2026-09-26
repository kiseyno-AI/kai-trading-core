# kai-trading-core

Strategy, market-data, decision-support, and read-API service for the KAI trading stack.

Core collects and normalizes market/fundamental data, builds deterministic factors and agent context, evaluates decision policies, and produces execution plans. The separate `kai-trading-exec` service owns the always-on broker execution loop and execution truth.

## Quickstart

The application root is `apps/api`; the Python import root is `app`. Python 3.11 is the primary local runtime, and CI also verifies Python 3.12. The launcher prefers `PYTHON_BIN`, then the active virtual environment, then `apps/api/.venv/bin/python`, and finally `python3.11` on `PATH`.

```bash
cd apps/api
python3.11 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements-dev.txt
cp .env.secrets.example .env
# Replace placeholders for providers you use and clear unused placeholders.
# For UPSTREAM_BEARER_TOKEN, use a real local token or leave the value empty.
# Append non-secret overrides documented in .env.example when needed.
# Fresh local PostgreSQL setup; enter kai_trading_dev at the password prompts.
createuser --login --createdb --pwprompt kai_trading_app
createdb --owner=kai_trading_app kai_trading
python -m alembic upgrade head
./start-api.sh
```

The API listens on `http://127.0.0.1:8000` by default.

```bash
set -a
source .env
set +a
CURL_AUTH_ARGS=()
if [ -n "${UPSTREAM_BEARER_TOKEN:-}" ]; then
  CURL_AUTH_ARGS=(-H "Authorization: Bearer ${UPSTREAM_BEARER_TOKEN}")
fi
curl "${CURL_AUTH_ARGS[@]}" http://127.0.0.1:8000/health
curl "${CURL_AUTH_ARGS[@]}" http://127.0.0.1:8000/eod/freshness
open http://127.0.0.1:8000/docs
```

If the local `kai_trading_app` role or database already exists, skip the corresponding creation command. The documented password matches the development-only defaults in `app/core/settings.py`; use `DATABASE_URL` instead in any shared or production environment. Browser-based `/docs` is directly usable only when bearer auth is disabled or the browser client supplies the configured header.

For the operator UI on iPhone Safari, connect the phone to the same Tailnet and
open the [KAI Trading Web UI](https://gangseunghyeon-uiui-macmini-1.tail1d831c.ts.net/trading-web).
The production Web service runs locally on port `8383` behind that HTTPS entry
point. Do not enter `127.0.0.1:8000` or `127.0.0.1:8010` on the phone: loopback
resolves to the phone itself. Do not rebind or expose Core or Exec merely to
enable mobile UI access.

`start-api.sh` requires `apps/api/.env`. When `DB_AUTO_UPGRADE=true`, it runs the deploy-hygiene preflight, applies ordinary pending Alembic migrations, verifies the database is at head, and then starts Uvicorn. To run Uvicorn directly, export the environment yourself:

```bash
set -a
source .env
set +a
python -m uvicorn app.main:app --reload --host 127.0.0.1 --port 8000
```

## Safety

- KIS uses mock trading by default (`KIS_USE_MOCK=true`). Do not set it to `false` until the production account, credentials, account references, and operational runbook have been verified.
- US-equity collection, screening, and agent planning are available, but the Core-to-Exec USD handoff is preview-only. `US_EQUITY_EXECUTION_ENABLED` cannot override that safety boundary until Exec advertises and enforces currency-aware USD sizing; non-preview requests fail before any Exec client call.
- Core-to-Exec dispatch currently supports one explicit account target per Exec base URL. Dispatch mode fails closed with `multi_account_exec_endpoint_unsupported` when multiple targets are configured. Live runtime additionally requires exactly one explicit valid KIS target and exact row-to-target binding; an empty, invalid, mismatched, or synthetic `KIS:00000000-00` route fails before any Exec POST with `live_exec_account_target_required`. The synthetic route remains available only for mock/paper dispatch and monitor-only planning, while disabled/planning-only mode may still materialize multi-account plans.
- Live next-open buys do not trust an agent-reported return by itself. Core accepts `quote_spread_bps` only from a current executable snapshot whose timezone-aware timestamp is no older than `KIS_REALTIME_PRICE_STALE_AFTER_SECONDS` (default `30`, startup-validated range `>0` to `300`) and is not future-dated. When both quote-cache and intraday bundles are coherent, Core selects the freshest canonical, same-market-date provider bundle and never mixes their fields. The provider/source-table set must exactly match the selected quote or intraday context, and current price, bid, ask, spread points, and spread bps must all come from that one context and form a positive coherent book. Completed-EOD prices remain valid references but their spreads are never execution-cost evidence. Slippage cost reduces reward and increases loss. Missing, negative, non-finite, out-of-range, stale, future-dated, date-mismatched, mixed-source, or unrecognized evidence fails closed. A canonical completed-EOD snapshot may enter `pending_live_cost_confirmation` only in Exec dispatch mode, where Core uses gross economics solely for pre-screening and Exec must re-price and pass its final full-cost check. Core-local signal-decider next-open buys remain disabled in every integration mode because that path has no execution-time quote recheck; the server-owned `SIGNAL_DECIDER_NEXT_OPEN_NON_SPREAD_ROUND_TRIP_COST_BPS` allowance cannot substitute for spread evidence.
- The KRX next-open planner accepts only snapshots whose canonical symbol context explicitly identifies `KRX`, `KOSPI`, `KOSDAQ`, or `KONEX`; missing, US, unknown, or conflicting market claims are excluded. Every non-terminal pre-open row is re-evaluated against current bars, enrichment, policy state, account targets, and startup-validated finite thresholds before dispatch. Terminal rows remain immutable audit history.
- Prefer delegated execution through `kai-trading-exec`. Core still exposes legacy/direct KIS order, account, reconciliation, and execution-handoff routes, so treat write endpoints as broker-affecting operations.
- The launcher binds to loopback by default and refuses a non-loopback `HOST` unless `UPSTREAM_BEARER_TOKEN` is configured. With a token, every request except `OPTIONS` must send `Authorization: Bearer <token>`. Local internal jobs should prefer `python -m app.internal.ops ...`, which does not loop through bearer middleware.
- `DB_AUTO_UPGRADE=true` changes the database at process start. Run the preflight explicitly before production restarts.
- CORS is permissive for local development; constrain it before exposing the API outside a trusted network.

## Runtime matrix

| Runtime | Default address | Owns | Start path |
| --- | --- | --- | --- |
| Core API | `127.0.0.1:8000` | data collection, factors, agent context, decision policy, plan generation, read APIs | `apps/api/start-api.sh` |
| Core background services | same process | market snapshots, EOD/DART work, legacy/direct-KIS reconciliation, Core-side handoff/readback reconciliation, signal-decider maintenance, US pipeline scheduling | FastAPI lifespan |
| Core operator CLI | local process | bounded jobs, backfills, inspection, deploy hygiene | `python -m app.internal.ops ...` |
| Exec API | `127.0.0.1:8010` | delegated-plan order submission, monitoring, fills, broker truth and reconciliation, kill switch | separate `kai-trading-exec` repo |
| Web UI | deployment-specific | operator and dashboard UI | separate `kai-trading-web` repo |

Core is designed to stay available continuously, but an Exec process must remain independently healthy when Core restarts.

## Architecture and data flow

The backend is a FastAPI modular monolith backed by SQLAlchemy and Alembic. Its main flow is:

```text
KIS / pykrx / DART / SEC / macro / news
                  |
                  v
       normalized persisted facts
                  |
                  v
 technical + fundamental + risk factors
                  |
                  v
 agent context -> summarizers/deciders -> evaluations/policies
                  |
                  v
        execution plan / Exec handoff
```

### Decision integrity at a glance

| Boundary | Evidence Core accepts | When evidence is incomplete |
| --- | --- | --- |
| Historical market context | Persisted point-in-time market membership, per-market counts and digests, and the exact requested symbol set | Prior turnover and breadth context are unavailable; the current symbol master is never used to rewrite history |
| Live market regime | Current-session KIS date/time, current-before/bar/current-after chronology with monotonic cumulative turnover, and complete same-date breadth | The regime is unavailable or risk-off; missing data cannot enable a buy or a larger position |
| Buy economics | One coherent executable quote context plus explicit non-spread costs, all finite and current | Block locally, or mark `pending_live_cost_confirmation` only when Exec will perform the final live re-price |
| Agent decision | Versioned, cutoff-safe inputs with deterministic quality, policy, liquidity, and expected-return gates | Agent output may rank or reduce candidates but cannot restore missing evidence or override a failed gate |
| Exec dispatch | One material-bound v4 contract ACK, one unique ACK per Core item, and durable readback proof | Keep the item pending/failed; duplicate, missing, stale-material, or mismatched acknowledgements never count as accepted |
| OPEN buy authority | A current `current_live_market_buy_authority_v1` observation covering both KOSPI and KOSDAQ, delivered for the exact KIS/KRX/KRW runtime/account scope | Persist the observation as pending and block every new v4 buy until Exec ACKs with `fence_active=false` and `projection_pending=false` |
| Market-open revalidation | Fresh OPEN-session identity plus required index/breadth evidence before a pre-open buy remains actionable | Revoke unsafe delegated BUY authority; missing required evidence fails closed, while an optional-only partial snapshot does not create or remove authority |

Delegated v4 dispatch uses Exec's `GET/POST /v4/execution-plans` and
`POST /v4/execution-plans/revocations` endpoints. Every plan includes
`metadata.dispatch_material_fingerprint`, a 64-character lowercase SHA-256 of
the recursively canonicalized complete dispatch material. Only top-level
`requested_at`, `metadata.requested_at`, `metadata.trigger`, and the fingerprint
field itself are excluded; numerically equal integer, float, and decimal values
share one representation, while booleans remain distinct. Exec recomputes the
fingerprint before persistence and echoes the exact value from create and list
`contract_ack`; Core rejects any ACK that differs from the submitted material.
This is a breaking v4 wire migration: pause delegated dispatch, upgrade Exec,
upgrade Core, remove any noncanonical `SIGNAL_DECIDER_EXEC_DISPATCH_SOURCE`
override (v4 requires exactly `kai-trading-core`), verify a mock or paper
create-and-readback ACK, and then resume.

Core commits a compact, secret-free `pending_remote_ack` write-ahead record
before every v4 create request. The record binds the exact plan/material and row
revisions to the normalized original Exec URL, runtime mode, dispatch source,
and account reference. After a restart, Core reconciles that exact destination
before refreshing or redispatching: an exact ACK restores the accepted proof,
a confirmed not-found result retries only the identical material while the
decision authority is unchanged, and an unavailable or mismatched result stays
blocked. A URL/runtime/account/source mismatch is never redirected to the new
endpoint for readback or revocation; it enters explicit manual review instead.
Bearer tokens and embedded URL credentials are never persisted as destination
identity. Rotating a bearer token for the same destination therefore does not
prevent reconciliation or revocation. A durable `pending_remote_ack` remains a
managed delegated authority even if dispatch or next-open execution is later
disabled: Core reconciles and revokes it at the original destination before any
Core-local execution path can be selected.

At OPEN, Core separately derives one route-wide buy-authority observation from
the deterministic `current_live_market_buy_authority_v1` read model. It does
not count agent prose, confidence, or risk-flag text as authority. The
observation requires fresh authoritative KOSPI and KOSDAQ evidence and binds
its canonical material fingerprint and idempotent observation ID to the exact
KIS/KRX/KRW runtime and account reference. Core stores the scoped authority
state and pending outbox row in one transaction, then posts the observation to
Exec at `POST /runtime/core-buy-authority/observations` even when the candidate
set is empty. Missing or stale market evidence becomes an explicit
`block_new_buy` observation; delivery failure or an invalid, stale, conflicting,
or mismatched ACK never synthesizes allow.

Core may dispatch a new OPEN v4 buy only after the latest exact-scope
observation is durably acknowledged and the ACK reports both
`fence_active=false` and `projection_pending=false`. Exec applies block and
recovery through a durable two-phase projection so an interrupted item update
leaves the fence closed and is retryable by the same observation ID. A healthy
observation duplicate cannot advance the recovery streak. Recovery releases
only its scoped market-authority fence. Account-drawdown latches use the
separate account-recovery observer contract. Neither recovery resumes a paused
or kill-switched runtime, bypasses the independent hourly risk-off fence, or
revives a rejected order. Every fresh candidate is evaluated again through
current runtime, account, session, quote, sizing, exposure, and broker-boundary
gates.

Accepted v4 authority also binds every Core item key to the exact Exec plan and
plan-item IDs returned by the validated ACK. Later plan readback may update
status only when all three identities match. Remote orders and executions with
stable IDs must match one ACK-bound item consistently across every supplied ID,
symbol, and side; symbol-only fallback is limited to legacy rows that supply no
stable ID. Plan-scoped position readback requires both the Core external plan ID
and Exec internal plan ID, then accepts only KIS/KRX proofs with one non-empty
opaque account scope for the group. Conflicting identity or scope evidence is
discarded and audited, so a later revocation continues to address the original
ACK-bound authority. Compact readback keeps authoritative total counts separate
from retained items. A truncated source is excluded from outcome calculation;
P/L stays unavailable unless another complete source proves it.

Exec's KIS US-equity gate also requires complete valuation and economic proof
before a new buy. Every non-zero holding needs either a finite, non-negative
broker market value or a deterministic value derived from average price,
quantity, and unrealized P/L. Every open buy needs a known positive-price
notional. A cumulative legacy fill without explicit `fill_time_precision` is
treated as `observed_upper_bound`, not exact, so it contaminates FIFO chronology
until a later exact-scope verified-flat boundary and keeps the US gate closed.
Duplicate broker fill identities collapse only when quantity, base/quote
notional, price, settlement currency, transaction cost, coverage, source, and
currency-conflict evidence agree; any economic conflict excludes the whole
group from realized P/L and keeps the US new-buy gate closed for review.

Open-drive snapshot refreshes use both process-local single-flight and a durable
database bucket claim. Transaction advisory locking makes the first cache-row
insert race-safe, and writes advance only when the refresh attempt, provider
observation for every persisted market leg, receipt time, and snapshot quality
are monotonic. A slow older failure or a regressed KOSPI/KOSDAQ observation
therefore cannot replace a newer successful snapshot, even when the other leg
advanced the top-level timestamp. The runner keeps one shielded refresh in
flight across an initial timeout. That timeout remains fail-closed, but a later
successful completion clears the retry cooldown immediately; failed or
cancelled work retains the cooldown and duplicate refreshes remain coalesced.

During OPEN, risk-off or unavailable required evidence stages every accepted or
ambiguously accepted delegated BUY family for exact v4 revocation. A critical
evidence-chain failure synchronously flushes pending revocations before any new
dispatch. Core remains blocked until an exact Exec ACK; possible exposure moves
to manual review and protective exits are preserved. A revocation ACK never
resumes buying by itself. A transient critical-evidence revocation may be
replanned only after the exact revocation ACK, unchanged row authority,
destination and material fingerprint, and a later fresh authoritative
`action=allow` observation. Core archives the old dispatch proof and creates a
new generation instead of reviving it. Explicitly revoked, tampered, rerouted,
or manual-review rows are never resurrected, and every resumed or fresh
candidate must pass all gates again.

Each live KIS index row is joined with its intraday timestamp using a
current-before, one-minute-series, current-after sequence. Cumulative turnover
must be non-negative, monotonic, and bracket the timestamped observation; index
price itself only has to remain positive and finite because price is not
monotonic between valid provider reads. The newer current response is published.
This integrity check intentionally uses two current-index calls per market for
each refresh, so open-session refresh cadence must be sized for that KIS quota.

Regime-driven live upsizing is deliberately disabled: the multiplier stays at
or below `1.0` until a separately calibrated policy is promoted. This protects
capital but does not guarantee profitability.

Key layers:

1. **Collection:** KIS and pykrx market data, DART disclosures/fundamentals, SEC facts, BOK/ECOS, Korea Exim FX, news, crypto, and US-equity sources.
2. **Persistence:** symbol master, quote/market snapshots, EOD bars, factor snapshots, disclosure/fundamental history, agent feedback, decision/evaluation history, plans, and order handoff/readback state.
3. **Deterministic computation:** technical snapshots, action summaries, momentum/value/low-risk/beta/advanced-value/unified-value/quality/composite factors, market-risk context, and screening.
4. **Decision support:** symbol-day context, role-specific feedback, signal-decider diagnostics/calibration/policies, replay, and execution-plan generation.
5. **Execution integration:** Core plans and observes; `kai-trading-exec` owns the long-running broker lifecycle. Core's direct KIS paths remain implemented for compatibility and guarded operations.

Stored facts and versioned read models should be preferred over ad hoc recomputation. Decisions must retain source market date, freshness, provenance, policy version, and execution lineage.

Architecture references:

- `docs/REPOSITORY_ARCHITECTURE.md` — repository walkthrough and diagrams
- `docs/ARCHITECTURE.md` — documentation index
- `docs/API_BUILD_ORDER.md` — detailed API inventory and implementation notes
- `ARCHITECTURE.md` — top-level summary
- `RUNBOOK.md` — production and operator procedures

## Configuration

`app/core/settings.py` is authoritative for defaults and validation. Environment variables are case-insensitive.

### Environment files

- `.env.secrets.example` contains the secret placeholders that should be copied to `.env`.
- `.env.example` documents optional, non-secret runtime overrides.
- `start-api.sh` sources `.env`; Pydantic Settings does not load that file on its own when Uvicorn is started directly.
- Never commit populated environment files, tokens, account numbers, or broker responses containing credentials.

Required or integration-specific secrets include KIS, DART, BOK/ECOS, Korea Exim, Naver, FRED, OpenClaw, and the optional upstream bearer token. Only configure the providers used in the target environment.

### Important control groups

| Group | Representative settings | Purpose |
| --- | --- | --- |
| Database | `DATABASE_URL`, `DB_AUTO_UPGRADE`, `DB_POOL_*`, `DB_*_TIMEOUT_*` | storage, pool limits, migration behavior |
| KIS | `KIS_USE_MOCK`, `KIS_APP_KEY`, `KIS_APP_SECRET`, `KIS_HTS_ID`, `KIS_REALTIME_*` | quotes, realtime feeds, account and order access |
| EOD | `EOD_*`, `EOD_PYKRX_*`, `BACKGROUND_RUNNER_ENABLED` | collection cadence and enrichment |
| Agent feedback | `OPENCLAW_*`, `AGENT_FEEDBACK_*` | fan-out, role caps, reuse, retry, timeouts |
| Signal decider | `SIGNAL_DECIDER_*` | evaluation, policy, next-open and Exec handoff gates |
| US equities | `US_EQUITY_*`, `SEC_EDGAR_*`, `NASDAQ_TRADER_*` | universe, price/fundamental collection, screening, planning |
| Core-to-Exec | `SIGNAL_DECIDER_EXEC_*` | delegated plan dispatch and readback |
| Auth/logging | `UPSTREAM_BEARER_TOKEN`, `API_*_LOGGING_*` | northbound auth and audit logs |

Production portfolio summaries additionally require valid KIS credentials and `PORTFOLIO_ACCOUNT_REFERENCES`. See `.env.example` for bounded concurrency, freshness, and safety defaults before tuning collection throughput.

### KIS OAuth token cache (Core/Exec shared contract)

Core follows the [official KIS open-trading-api examples](https://github.com/koreainvestment/open-trading-api) principle of reusing a token before it must be refreshed and keeping production and demo credentials separate. Every process that uses the same KIS credential must share one exact cache leaf: Core sets `KIS_TOKEN_CACHE_DIRECTORY`, while Exec sets `KAI_EXEC_KIS_TOKEN_CACHE_DIRECTORY`. Align Core's remaining `KIS_TOKEN_*` timing settings with Exec's corresponding `KAI_EXEC_KIS_TOKEN_*` settings. The default directory is `~/.cache/kai-trading/kis-oauth-tokens`; `~` is expanded before use, and relative, root, home, symlinked, or otherwise unsafe locations are rejected.

#### On-disk v1 contract

All JSON records use `schema_version: 1` and the same canonical identity.

| Area | Contract |
| --- | --- |
| Identity | `environment` is `real` or `demo`. `credential_fingerprint` is `hmac-sha256:` plus `HMAC-SHA256(key=app_secret, message="kis-token-cache-v1\0{environment}\0{app_key}")`; `\0` is a NUL-byte separator. Raw app keys and secrets are never written. |
| Files | The prefix is `{environment}-{first_32_fingerprint_hex}` and the suffixes are `.token.json`, `.lock`, and `.failure.json`. The owning user must own the `0700` directory and regular, singly linked `0600` files. Version-1 `token`, `revocation`, `failure`, `issuance_attempt`, and `issuance_success` records each require their exact record-type key set; missing, extra, or unknown fields are rejected. Unsafe paths/files, malformed JSON, duplicate keys, non-finite values, naive write timestamps, and identity/schema mismatches fail closed before OAuth issuance. |
| Token | A `token` record contains a canonical UUID `generation`, header-safe token68 `access_token`, `token_type: "Bearer"`, and UTC RFC 3339 `issued_at`, `expires_at`, and `refresh_after`. `issued_at` is response-received time. `expires_at` is the earlier of request-start plus `expires_in` and the strictly parsed KIS `access_token_token_expired` KST timestamp. `refresh_after` already includes the margin and must still be future when the response arrives; readers must not subtract a second margin. |
| Issuance ledger | `.failure.json` contains `failure`, `issuance_attempt`, or `issuance_success`. It shares failures, preserves ambiguous/crashed attempts, and enforces the minimum issuance interval even if the token file disappears. A positive cooldown shorter than the reader's local floor is extended in memory; zero, negative, over-one-hour, or future-start records fail closed. An attempt's `invalidates_generation` permanently prevents revival of that older token: callers wait during the lease, then exactly one may issue a replacement. |
| Revocation | `invalidate_if_matches(...)` runs under the shared lock. Every supplied comparator must match: token-only and generation-only calls are supported, while a call carrying both values requires both to identify the current record. A different token or generation is preserved. A match becomes a `revocation` tombstone with a SHA-256 token fingerprint and `not_before`, never the raw token. The maximum valid tombstone window is the maximum one-hour issue interval plus five minutes of allowed token clock skew. HTTP `401` and business error `EGW00123` invoke this CAS path; broker GETs and order POSTs are not replayed. Exec must use the same CAS rule and pass its force flag through authentication. |

#### Concurrency and crash ordering

The stable `.lock` serializes state decisions and OAuth requests across processes. Cache reads, writes, and `fsync` calls run outside the async event-loop thread while the process continues to hold that lock. `force_refresh` ignores the caller's old generation, adopts a generation just written by a peer, and never bypasses the minimum issue interval.

Exec treats the issuance lease as a hard monotonic deadline for both async and
sync lock waiters and bounds the synchronous coroutine bridge as well. A lock
or cache-operation timeout closes its descriptor and returns a retryable `503`
classified as `TRANSIENT` / `RETRY_PENDING`; it never falls through to an
uncoordinated token issue or replays a broker request.

The crash-safe order under the lock is:

1. Atomically write and `fsync` `issuance_attempt`.
2. Call `tokenP`.
3. Atomically write and file/directory-`fsync` the token.
4. Atomically write and `fsync` `issuance_success`.

Never write success before the token. While a success ledger is active, a missing or mismatched token is not reused. After the interval, exactly one caller may repair that torn state. A still-fresh token whose generation matches the ledger remains reusable.

#### Timing and clock requirement

| Setting | Default | Purpose |
| --- | ---: | --- |
| `KIS_TOKEN_REFRESH_MARGIN_SECONDS` | `300` | Proactive refresh margin, applied once by the writer |
| `KIS_TOKEN_FAILURE_COOLDOWN_SECONDS` | `60` | Shared explicit-failure cooldown |
| `KIS_TOKEN_REQUEST_TIMEOUT_SECONDS` | `15` | Whole token-request deadline |
| `KIS_TOKEN_ISSUANCE_LEASE_SECONDS` | `90` | Ambiguous/crashed-request lease |
| `KIS_TOKEN_MIN_ISSUE_INTERVAL_SECONDS` | `60` | Hard minimum interval between issuances |

The lease must be at least request timeout plus minimum issue interval; startup rejects a shorter value. Failures and timeouts cool down for at least the minimum interval. Core and Exec must use aligned values, including this relationship when Exec has a longer timeout.

Durable cross-process records necessarily use UTC wall-clock time; a monotonic process clock cannot survive restarts or be compared by another process. Hosts sharing the directory must therefore maintain stable NTP-synchronized clocks and avoid manual clock jumps. A forward jump can make a token or guard expire early; a backward jump can extend reuse/cooldown windows. Future failure/attempt/success starts fail closed, while token and revocation timestamps tolerate at most five minutes of skew and fail closed beyond it. Restore clock stability before retrying after a discontinuity.

#### Token endpoint migration

`POST /broker/kis/token` is an authenticated operator action, not a token-reading API. It is disabled with `503` unless `UPSTREAM_BEARER_TOKEN` is configured and requires exact `Authorization: Bearer <UPSTREAM_BEARER_TOKEN>` authentication. KIS routes return bounded errors without broker, token, account, or raw exception text.

The response contract intentionally changed from returning `access_token` to metadata only:

| Client behavior | Before | Now |
| --- | --- | --- |
| Read response | Consume `access_token` and possibly build a downstream Authorization header | Consume `access_token_present`, `token_type`, `expires_in`, `cached`, `refresh_deferred`, and optional `defer_reason` |
| Broker authentication | Client could handle the raw KIS token | Remains server-side; clients must not expect or reconstruct the KIS bearer token |
| Forced refresh | Treat success as a newly issued token | Inspect `refresh_deferred`/`defer_reason`; success may mean safe reuse because a peer refreshed or the minimum interval is active |

Clients using the old raw-token response must migrate before deploying this API version. See the endpoint reference under `.codex/skills/fastapi-trading-api/references/api/` for the exact response and error shapes.

## HTTP API

OpenAPI at `/docs` or `/redoc` is the authoritative endpoint-level reference. The implemented route groups are:

| Prefix | Responsibility |
| --- | --- |
| `/health` | application and database liveness |
| `/broker/kis` | KIS health, token, quotes, and broker integration |
| `/market`, `/quotes`, `/scanner`, `/symbols`, `/symbols/favorites` | market state, quote provenance, tracking, search, history |
| `/macro`, `/fx`, `/external/news` | macro, FX, and external context |
| `/eod` | collection, freshness, enrichment, factor and snapshot jobs |
| `/disclosures/dart` | disclosures, filing artifacts, fundamentals, history, backfill |
| `/agent-context` | context, feedback, decisions, evaluations, diagnostics, calibration and policies |
| `/portfolio` | aggregate account/portfolio read model |
| `/trading/orders`, `/trading/accounts` | guarded Core-side order and account operations |
| `/execution/runtime` | Exec runtime status, monitored symbols, daily board and symbol readback |
| `/market-data/crypto` | crypto market-data context |
| `/us-equities` | US universe, collection, screening, agent analysis and preview-only execution planning |

Quote reads prefer live KIS data while the market is open, then same-day persisted bars/cache. Pre-open and weekend fallback may use a recent persisted daily bar within three calendar days. Multi-day stale intraday cache is rejected and returned as explicitly unavailable.

## Operations

### Internal read and job commands

Run from `apps/api`:

```bash
python3.11 -m app.internal.ops market-status
python3.11 -m app.internal.ops quotes-tracked
python3.11 -m app.internal.ops scanner-tracked-symbols
python3.11 -m app.internal.ops eod-status
python3.11 -m app.internal.ops eod-pipeline-stages
python3.11 -m app.internal.ops eod-pykrx-daily-run-if-due --market-date 2026-04-08 --force
```

Common bounded recomputation/backfill paths:

```bash
python3.11 -m app.eod.technical_snapshots --market-date YYYY-MM-DD
python3.11 -m app.eod.action_summaries --market-date YYYY-MM-DD
python3.11 -m app.dart.backfill_current_universe --symbol-limit 10 --capture-limit 200 --fs-div CFS
python3.11 -m app.internal.ops eod-momentum-factors-compute --market-date YYYY-MM-DD
python3.11 -m app.internal.ops agent-signal-decider-evaluations-compute --market-date YYYY-MM-DD --horizons 1,3,5,10
```

Use `RUNBOOK.md` for the full command catalog, stage dependencies, overnight runners, and recovery procedures.

The automatic close pipeline records price collection and every downstream stage separately. Once price collection is terminal, a failed fundamentals, factor, news, feedback, decider, plan, or evaluation stage is resumed from the first incomplete follow-up without downloading prices again. Follow-up recovery waits at least five minutes between attempts and stops after five attempts for the trade date; the skip reason and recovery metadata remain visible in the run result.

The default daily stage order is:

```text
master_refresh -> pykrx_universe_refresh -> price_collection -> pykrx_enrichment
-> technical_snapshots -> action_summaries -> trending_ranks -> market_risk_filter
-> fundamentals_daily_refresh -> piotroski_filter -> momentum_factors -> value_factors
-> low_risk_factors -> beta_factors -> advanced_value_factors -> unified_value_factors
-> quality_factors -> composite_factors -> signal_features -> external_news_collect
-> external_news_enrich -> macro_context_refresh -> agent_feedback_summaries
-> signal_decider -> next_open_execution_plan -> signal_decider_evaluations
```

Consumers must read `/eod/pipeline/stages` rather than hard-code this list. The PyKRX decision preflight requires same-day `sector_classification`, `investor_flow_symbol`, `short_selling`, and direct `valuation_snapshot` coverage; valuation must cover at least 95% of the fresh PyKRX KOSPI/KOSDAQ stock universe (excluding active-universe ETFs outside that feed), and the other required categories at least 99%.

Pipeline completion is semantic, not row-count-only. Factor stages publish computed `ready_count`/`unready_count` and post-write `persisted_ready_count`/`persisted_unready_count`; a non-empty scope with zero decision-usable persisted rows remains partial/blocked even when row coverage exceeds 99%. A direct `next_open_execution_plan` run automatically includes `signal_decider`, and the decider fast path cannot bypass an unresolved structural prerequisite. Historical factor inputs use an index-friendly per-symbol `LATERAL ... LIMIT` lookback in SQL, while derived factor/filter rows are persisted in bounded batch upserts with single-row fallback for error isolation.

Successful annual DART fundamentals capture extracts and persists the filing's audit opinion before writing the financial snapshot or marking the disclosure event captured. Missing or unparseable audit evidence keeps that event retryable and the refresh visibly incomplete. Piotroski reads only captured-event snapshots and accepts audit evidence with the exact same `rcept_no`, so it cannot silently reuse an older clean opinion.

### Agent feedback and decisions

Core fans symbol-day context out to the configured summarizer/decider roles, persists request and response payloads, and can reuse unchanged inputs. Daily operations should run the broad universe once and then retry only incomplete symbols.

`agent_context_feedback` is the mutable latest row for each market-date, symbol, and role. Signal-decider audit history lives in append-only decision snapshots: exact semantic retries reuse the same snapshot even if the receipt timestamp changes, while a changed decision appends a new snapshot. Replay uses only the latest snapshot for each non-null feedback id; legacy/null-feedback snapshots remain independent observations.

```bash
curl -X POST http://127.0.0.1:8000/agent-context/feedback/dispatch \
  -H 'content-type: application/json' \
  -d '{"eod_run_id":158}'

curl 'http://127.0.0.1:8000/agent-context/feedback/status?market_date=2026-04-15&symbols=005930'

python3.11 -m scripts.run_feedback_retry_cycles
```

Useful reads include `feedback/latest`, `feedback/payloads`, `feedback/requests`, `feedback/status`, `feedback/progress`, signal-decider decisions, diagnostics, evaluation coverage, calibration, and active policy metadata.

For ad hoc rule analysis, replay against stored point-in-time outcomes and include realistic fees and slippage:

```bash
python3.11 -m app.internal.ops agent-decider-replay \
  --market-date-from 2026-06-15 \
  --market-date-to 2026-06-19 \
  --agent-name short-term-decider \
  --rule-config-json '{"rules":[{"name":"stale_watch_trigger_drift","predicates":[{"type":"trigger_price_drift","max_below_trigger_pct":0.03}]}]}' \
  --fee-bps 10 \
  --slippage-bps 5 \
  --output-format markdown
```

Signal-decider policy activation is stricter than this diagnostic example. Use every input from the candidate's stored `rule_bundle.replay_contract`, including its base-policy-only `policy_versions` cohort, submit the command's complete JSON artifact under `replay_evidence`, and expect Core to recompute and compare it before activation. Only `tighten_buy_gate` is currently activation-eligible; wait/reject relaxation candidates fail closed until an added-trade counterfactual replay exists.

Zero-fee and zero-slippage runs are labeled `gross_only` and are not eligible profitability evidence for promotion in both JSON and Markdown output. Historical context excludes news published or collected after the decision cutoff: an explicit timezone-aware `knowledge_cutoff_at`, or 15:30 KST for a completed historical KRX session. Historical PyKRX requests are intentionally single-day only (`start_date == end_date`) until date-by-date membership reconstruction exists. They resolve that day's membership without falling back to the current active/KIS-master universe and may run only the point-in-time-safe `price_collection`, `technical_snapshots`, and `action_summaries` stages. Any unavailable requested market or unsupported stage fails before bar collection. Run a separate request for every historical trading day; a global `limit` may intentionally stop before later markets are queried, and the response distinguishes that state from a provider failure.

#### Live market-regime evidence

`aggressive_open` and `trend_day` are enabled only from evidence that can be
traced to the current KRX session:

| Evidence | Accepted proof |
| --- | --- |
| Session identity | KIS provider business date and session timestamp |
| Prior turnover | KIS prior-session turnover; PyKRX only when its completed EOD universe exactly matches |
| Breadth | All five buckets from a complete, same-date KIS symbol master |
| Intraday index | Refreshed every five minutes from 09:00 to 14:30 KST before plan refresh or dispatch |
| Flow and FX | Optional context refreshed hourly and evaluated independently; absence alone does not revoke healthy core authority |

Missing or mismatched required session, index, or breadth proof makes live BUY
authority unavailable instead of borrowing another source's timestamp. Missing
flow or FX may mark the snapshot envelope `partial`, but cannot hide severe
breadth, enable risk-taking, or by itself revoke otherwise healthy core
authority. Regime-driven risk increases remain shadow-only; the live position
multiplier is capped at `1.0` until separately calibrated.

External-news article observations are immutable. `external_news_item_symbols` records each article-to-symbol attribution with its first-known `associated_at`, so a later search can add another symbol without rewriting the original article or leaking that attribution across an earlier knowledge cutoff. Classifier output is stored in append-only `external_news_item_classifications` rows keyed by article and classifier version. Historical reads require the article, symbol association, and selected classification to each predate the exclusive knowledge cutoff; legacy embedded labels remain untrusted until enrichment records a timestamped sidecar without changing the article payload.

### Database and deploy hygiene

```bash
python3.11 -m app.internal.deploy_hygiene preflight --format text
python3.11 -m alembic upgrade head
python3.11 -m app.internal.deploy_hygiene preflight --require-head --format text
python3.11 -m app.internal.deploy_hygiene smoke-check \
  --base-url http://127.0.0.1:8000 \
  --format text
python3.11 -m app.internal.index_audit
```

Run `alembic current` before and after a migration when explicit revision evidence is required. The migration source of truth is `apps/api/db_migrations/versions/`.

## Tests and quality gates

Install runtime and test dependencies, then run from `apps/api`:

```bash
python3.11 -m pip install -r requirements-dev.txt
export DATABASE_URL=postgresql+psycopg://kai:kai@127.0.0.1:5432/kai_trading_test
DB_AUTO_UPGRADE=false python3.11 -m alembic upgrade head
python3.11 -m pytest -q
python3.11 -c 'from app.main import app'
bash -n start-api.sh
```

The test suite accepts only Core-scoped PostgreSQL databases: `kai_trading_test`, an isolated `kai_trading_test_<suffix>` worker database, or a named isolation database matching `kai_trading_core_<scope>_test`. Pytest enforces this at collection. The `tests` package and the database module enforce the same check for `python -m unittest`, unittest discovery, and direct `tests/test_*.py` execution, so alternate runners cannot bypass the database-name guard. Generic databases whose names merely contain `test`, another repository's test database, the development/production database, and in-memory SQLite are rejected; Core's PostgreSQL engine configuration is not SQLite-compatible. CI applies the complete PostgreSQL migration chain, rejects ORM/schema drift with `alembic check`, and runs the import smoke test plus complete pytest suite on Python 3.11 and 3.12. The repository does not currently declare a lint or static typecheck tool; do not report either gate as passing until one is added and configured.

## Repository relationships

- `kai-trading-core` — market data, strategy context, decisions, plans and read APIs
- `kai-trading-exec` — independent execution runtime and broker truth
- `kai-trading-web` — dashboards and operator console

Keep shared request/response schemas and execution-plan fixtures synchronized across repositories. Read `AGENTS.md` and `RUNBOOK.md` before changing production-facing paths.
