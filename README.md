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

`start-api.sh` requires `apps/api/.env`. When `DB_AUTO_UPGRADE=true`, it runs the deploy-hygiene preflight, applies ordinary pending Alembic migrations, verifies the database is at head, and then starts Uvicorn. To run Uvicorn directly, export the environment yourself:

```bash
set -a
source .env
set +a
python -m uvicorn app.main:app --reload --host 127.0.0.1 --port 8000
```

## Safety

- KIS uses mock trading by default (`KIS_USE_MOCK=true`). Do not set it to `false` until the production account, credentials, account references, and operational runbook have been verified.
- US-equity execution and signal-decider execution are separately gated and disabled by default (`US_EQUITY_EXECUTION_ENABLED=false`, `SIGNAL_DECIDER_EXEC_INTEGRATION_MODE=disabled`).
- Core-to-Exec dispatch currently supports one explicit account target per Exec base URL. Dispatch mode fails closed with `multi_account_exec_endpoint_unsupported` when multiple targets are configured; disabled/planning-only mode may still materialize multi-account plans.
- Prefer delegated execution through `kai-trading-exec`. Core still exposes legacy/direct KIS order, account, reconciliation, and execution-handoff routes, so treat write endpoints as broker-affecting operations.
- The launcher binds to loopback by default and refuses a non-loopback `HOST` unless `UPSTREAM_BEARER_TOKEN` is configured. With a token, every request except `OPTIONS` must send `Authorization: Bearer <token>`. Local internal jobs should prefer `python -m app.internal.ops ...`, which does not loop through bearer middleware.
- `DB_AUTO_UPGRADE=true` changes the database at process start. Run the preflight explicitly before production restarts.
- CORS is permissive for local development; constrain it before exposing the API outside a trusted network.

## Runtime matrix

| Runtime | Default address | Owns | Start path |
| --- | --- | --- | --- |
| Core API | `127.0.0.1:8000` | data collection, factors, agent context, decision policy, plan generation, read APIs | `apps/api/start-api.sh` |
| Core background services | same process | market snapshots, EOD/DART work, reconciliation maintenance, signal-decider maintenance, US pipeline scheduling | FastAPI lifespan |
| Core operator CLI | local process | bounded jobs, backfills, inspection, deploy hygiene | `python -m app.internal.ops ...` |
| Exec API | `127.0.0.1:8010` | durable order submission, monitoring, fills, reconciliation, kill switch | separate `kai-trading-exec` repo |
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

Key layers:

1. **Collection:** KIS and pykrx market data, DART disclosures/fundamentals, SEC facts, BOK/ECOS, Korea Exim FX, news, crypto, and US-equity sources.
2. **Persistence:** symbol master, quote/market snapshots, EOD bars, factor snapshots, disclosure/fundamental history, agent feedback, decision/evaluation history, plans, and order handoff/readback state.
3. **Deterministic computation:** technical snapshots, action summaries, momentum/value/low-risk/composite factors, market-risk context, and screening.
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
| `/us-equities` | US universe, collection, screening, agent analysis and execution planning |

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
