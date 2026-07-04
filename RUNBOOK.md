# RUNBOOK

## Goal
Run `kai-trading-core` as a 24/7 local service.

## API service
Working directory:
- `apps/api`

Start command:
- `./start-api.sh`

What it does:
- installs Python requirements
- loads `apps/api/.env`
- when `DB_AUTO_UPGRADE=true`, runs deploy preflight before uvicorn, applies `alembic upgrade head` itself when the database is safely behind head, then verifies `--require-head` before starting the server
- starts FastAPI via uvicorn on `127.0.0.1:8000` by default
- if `UPSTREAM_BEARER_TOKEN` is set, all incoming API requests require `Authorization: Bearer <token>`
- local orchestration should prefer `python3.11 -m app.internal.ops ...` over raw localhost `curl` so bearer auth does not break internal jobs
- when `./start-api.sh` handles the migration path, it exports `DB_AUTO_UPGRADE=false` for the uvicorn process so FastAPI lifespan startup does not immediately run a duplicate migration pass
- if `API_REQUEST_LOGGING_ENABLED=true` and/or `UPSTREAM_HTTP_LOGGING_ENABLED=true`, the service writes rotating JSONL audit logs under `apps/api/var/log` by default

## Health checks
- `GET /health`
- `GET /broker/kis/health`
- `GET /quotes/realtime/monitored`
- `GET /market-data/crypto/markets`
- `GET /market-data/crypto/tickers?markets=KRW-BTC,KRW-ETH`
- `GET /market-data/crypto/candles?market=KRW-BTC&interval=day&count=30`
- `GET /market-data/crypto/candles?market=KRW-BTC&interval=minute&unit=15&count=30`
- `GET /market-data/crypto/strategy-context?markets=KRW-BTC,KRW-ETH&count=30`
- `POST /broker/kis/intraday-snapshots/collect?symbols=005930`
- `GET /broker/kis/intraday-snapshots?symbols=005930&limit=5`
- `GET /portfolio/summary`
- `GET /trading/orders/exec/runtime?refresh=true`
- `GET /trading/orders/exec/signal-decider?target_trade_date=2026-04-30&refresh=true`
- `GET /trading/orders/intraday-auto-sell?account_reference=KIS:12345678-01&symbol=005930&refresh=true`
- `POST /trading/orders/intraday-auto-sell/run-if-due`
- `GET /macro/ecos/series?stat_code=722Y001&cycle=M&start_period=202401&end_period=202403&item_code_1=0101000`
- `GET /macro/context`
- `POST /macro/context/refresh?force=true`
- `GET /disclosures/dart/company-snapshot?stock_code=005930`
- `POST /disclosures/dart/sync/events?days=7`
- `GET /disclosures/dart/sync/inspect?financial_only=true`
- `GET /disclosures/dart/fundamentals?stock_code=005930&business_year=2025&report_code=11011&fs_div=CFS`
- `GET /disclosures/dart/fundamentals/history?symbols=005930&symbols=000660&start_date=2024-01-01&end_date=2024-12-31`
- `POST /disclosures/dart/fundamentals/capture?limit=20`
- `GET /disclosures/dart/fundamentals/capture/inspect?capture_status=failed`
- `POST /disclosures/dart/fundamentals/backfill/current-universe?symbol_limit=25&capture_limit=250`
- `POST /disclosures/dart/fundamentals/daily/run-if-due?market_date=2026-04-14&symbol_limit=250&capture_limit=500`
- `GET /disclosures/dart/fundamentals/daily/inspect?market_date=2026-04-14`
- `GET /disclosures/dart/filings/document?rcept_no=20260407000001`
- `GET /disclosures/dart/filings/xbrl?rcept_no=20260407000001&report_code=11011`
- `GET /fx/koreaexim/rates?search_date=20260406&currency=USD`
- `GET /scanner/trending-stocks?period=5d&direction=both&limit=20`
- `GET /eod/status`
- `GET /agent-context/feedback/decider-coverage?market_date=2026-05-08`
- `GET /eod/pipeline/status`
- `GET /eod/pipeline/stages`
- `POST /eod/master/refresh-if-due`
- `POST /eod/pykrx/universe/refresh`
- `POST /eod/pykrx/backfill`
- `POST /eod/pykrx/daily/run-if-due`
- `POST /eod/pykrx/enrichment/collect`
- `GET /eod/pykrx/enrichment`
- `POST /eod/pipeline/run`
- `POST /eod/technical-snapshots/compute`
- `GET /eod/technical-snapshots`
- `POST /eod/action-summaries/compute`
- `GET /eod/action-summaries`
- `POST /eod/signal-features/collect`
- `GET /eod/signal-features`
- `POST /eod/market-risk-snapshots/collect`
- `GET /eod/market-risk-snapshots`
- `POST /eod/market-risk-filters/compute`
- `GET /eod/market-risk-filters`
- `POST /eod/low-risk-factors/compute`
- `GET /eod/low-risk-factors`
- `POST /eod/beta-factors/compute`
- `GET /eod/beta-factors`
- `POST /eod/advanced-value-factors/compute`
- `GET /eod/advanced-value-factors`
- `POST /eod/unified-value-factors/compute`
- `GET /eod/unified-value-factors`
- `POST /eod/momentum-factors/compute`
- `GET /eod/momentum-factors`
- `POST /eod/composite-factors/compute`
- `GET /eod/composite-factors`

## Current runtime expectations
- Core server should stay up 24/7.
- The API startup path tries to prime the persisted `/market/snapshot` cache once, and the background runner refreshes it on the configured cadence (`MARKET_SNAPSHOT_REFRESH_INTERVAL_SECONDS`, default 3600s).
- `GET /market/snapshot` serves the persisted snapshot read model instead of making live upstream calls per dashboard request.
- Core owns one KIS realtime domestic price websocket for currently monitored execution-runtime symbols when `KIS_REALTIME_PRICE_ENABLED=true`; dashboard reads use process-local memory through `/quotes/realtime/monitored`, `/quotes/realtime/monitored/stream`, and execution-runtime display price fields instead of browser-side KIS sockets or request-time KIS quote fan-out. When monitored symbols exceed websocket capacity, execution-runtime dashboard rows can display local quote-cache/current-quote/daily-close fallbacks with explicit `price_source`, `price_status`, `display_price_available`, and realtime subscription metadata.
- Pre-market master refresh uses the KIS master zip files.
- A background runner checks market state every 60 seconds while the API process is alive.
- During `PREOPEN`, it refreshes the daily master if needed.
- During `PREOPEN`, it also refreshes the persisted signal-decider next-open execution plan for the current trade date when account targets are configured.
- When `SIGNAL_DECIDER_EXEC_INTEGRATION_MODE=dispatch`, the same `PREOPEN` loop translates eligible next-open rows into normalized exec plan items and dispatches them to `kai-trading-exec` once the clock reaches 08:00 KST, using stable plan/item keys for idempotent redispatch.
- Signal-decider, short-term-decider, and exec-agent handoffs now carry `decision_input_quality_context`, a deterministic buy-quality envelope that separates data quality, market/setup edge, execution feasibility, and final judgment. Operators should treat stale/fallback quote evidence and missing raw source turnover as hard blocks or major discounts in that envelope; missing news/disclosure context is secondary and neutral by default. The envelope is explicitly free/current-data-only (`paid_api_dependency=false`): missing provider data should be encoded with provenance and conservative gates, not solved by assuming paid APIs.
- Short-term next-open/watch candidates now carry a signal-decider alignment gate audit. When same-symbol signal-decider context is `WAIT`, the default `SIGNAL_DECIDER_WAIT_CONFLICT_GATE_MODE=report_only` records `signal_decider_alignment`, `alignment_gate_action`, `alignment_gate_reason`, and the linked signal-decider `decision_snapshot_id` without creating a live hard veto. Use `hard_block` only after replay review accepts the skipped-loss versus skipped-winner tradeoff.
- During `OPEN`, if `SIGNAL_DECIDER_EXEC_INTEGRATION_MODE=disabled`, core submits or dry-runs eligible next-open execution-plan rows through the existing trading order service, using the `SIGNAL_DECIDER_NEXT_OPEN_*` safety settings and deterministic client-order ids for duplicate protection.
- During `OPEN`, if `SIGNAL_DECIDER_EXEC_INTEGRATION_MODE=disabled`, core also evaluates linked next-open positions against persisted signal-decider target/stop thresholds and submits intraday auto-sell market orders when the live KIS quote crosses a configured exit rule.
- During `OPEN`, if `SIGNAL_DECIDER_EXEC_INTEGRATION_MODE=dispatch`, core does not place those live signal-decider orders itself. Instead it refreshes the core-owned exec read model from the exec runtime APIs so operator and web views can read dispatch/runtime/order/execution state without double-submitting live actions from core.
- Execution runtime dashboard reads (`/execution/runtime/daily-board`, `/execution/runtime/monitored-symbols`, and `/execution/runtime/symbols/{symbol}`) are cache-first/local-read-model paths. `refresh=true` no longer waits for exec/KIS readback in the HTTP request; it schedules one bounded, debounced background refresh and returns the current read model with `freshness` and `refresh` metadata for stale, partial, refreshing, timeout, or unavailable states. Daily-board and symbol-detail rows include compact `today_activity` fields so Today bought/sold views can use selected-date buy/sell fills instead of inferring side from lifecycle intent.
- KIS intraday execution-context snapshots are a separate credential-aware capture lane. They do not run inside the PyKRX/DART EOD jobs, and the manual/API collect path safely returns `status=disabled` when KIS credentials are unavailable or the feature toggle is off.
- Macro context refresh is a slow-moving contextual input lane. It uses no-key World Bank indicators by default, reports optional FRED as disabled unless `FRED_API_KEY` is configured, and must be treated as background context only, not a direct buy/sell signal.
- External context provider registry is currently a foundational internal service (`app.external_context`) rather than an HTTP route. Providers declare supported context keys, TTL, minimum refresh interval, source status, source URL/upstream endpoint, and last success/error metadata. The in-process store can serve fresh cached payloads before TTL expiry, force refreshes bypass TTL while still honoring provider refresh guards, and provider errors preserve the last good payload as stale metadata.
- During `CLOSED`, after `EOD_AUTO_PYKRX_AFTER_CLOSE_MINUTES`, it schedules a thread-based pykrx daily EOD run if that trade date is available and not already completed. That scheduled PyKRX path includes `pykrx_enrichment` after price collection so sector classification, investor-flow-symbol, and short-selling rows exist before the next exec/decider cycle.
- The automatic after-close path now also refreshes recent signal-decider evaluation coverage over the rolling lookback once stored EOD bars are available, and it refreshes the persisted review calendar from the same recompute.
- API startup also runs a bounded recent signal-decider evaluation catch-up from the latest stored price-bar trade date so deploys or migrations can backfill mature horizons without waiting for the next close.
- Database persistence stores symbol master data, tracked symbols, quote cache/history, market status snapshots, the current market snapshot cache, durable Korea Exim FX snapshots for request-time USD/KRW fallback, macro context observations, EOD bars, EOD run records, persisted daily price contexts, daily PyKRX enrichment rows, daily technical/action snapshots, KIS daily signal features, KIS intraday snapshot history/current-cache rows, KIS daily market-risk snapshots, persisted daily market-risk filter results, persisted daily momentum factor results, persisted daily low-risk factor results, persisted daily beta factor results, persisted daily advanced value factor results, persisted daily unified value factor results, persisted daily composite factor results, persisted daily market participation context, and stored day-based trending ranks.

## Database bootstrap

Before first startup on a fresh machine, from `apps/api`:

1. `pip install -r requirements.txt`
2. Create the Postgres database and user referenced by `.env`
3. `alembic upgrade head`
4. `python -c "from app.main import app"`

## Deployment migration hygiene

Use these commands from `apps/api` when you want deploy-time schema checks to fail early and explain drift more clearly:

1. Preflight before upgrade or restart:
   `python3.11 -m app.internal.deploy_hygiene preflight --format text`
2. Explicit head gate for a deploy:
   `alembic upgrade head`
   `python3.11 -m app.internal.deploy_hygiene preflight --require-head --format text`
3. Restart the API.
4. Post-restart smoke:
   `python3.11 -m app.internal.deploy_hygiene smoke-check --base-url http://127.0.0.1:8000 --format text`

Notes:

- The preflight is intentionally non-destructive. It does not mutate schema state.
- The preflight now also reports whether `/market/snapshot` is runtime-ready before restart, including the resolved KIS environment and any missing required secrets.
- The preflight now also reports whether `/portfolio/summary` is runtime-ready before restart, including whether `PORTFOLIO_ACCOUNT_REFERENCES` is present and whether production is still pointed at the mock KIS environment.
- The duplicate-table warning is heuristic by design: it flags live tables that overlap with still-pending unguarded `create_table(...)` revisions, which is the common `alembic_version` lag pattern behind repeated `DuplicateTable` deploy failures.
- The smoke command now exercises `/portfolio/summary` in addition to `/health`, `/eod/freshness`, and `/market/snapshot`, and it fails when portfolio sync is still `not_configured` after restart.
- The smoke command automatically sends `Authorization: Bearer <token>` when `UPSTREAM_BEARER_TOKEN` is set in the current shell environment.

## Duplicate-table drift recovery

If the preflight flags likely drift, or `alembic upgrade head` fails with `DuplicateTable`:

1. Stop automatic restart or migration retries for that deploy.
2. Inspect the flagged live table, indexes, unique constraints, and foreign keys against the pending migration file.
3. Only if the live schema materially matches the intended migration boundary, run `alembic stamp <matching_revision>`.
4. Use `alembic stamp head` only when every still-pending revision is already reflected in the live schema.
5. Rerun `python3.11 -m app.internal.deploy_hygiene preflight --require-head --format text`.
6. Restart the API and rerun `python3.11 -m app.internal.deploy_hygiene smoke-check --base-url http://127.0.0.1:8000 --format text`.

If schema parity is not clean, write a corrective migration instead of dropping tables in place.

## pykrx EOD backfill

From `apps/api` on this machine:

1. `python3.11 -m pip install -r requirements.txt`
2. `alembic upgrade head`
3. `python3.11 -m app.eod.pykrx_backfill refresh-universe`
4. `python3.11 -m app.eod.pykrx_backfill backfill --start-date 19560101`

Useful limited-scope runs:

- One-symbol verification: `python3.11 -m app.eod.pykrx_backfill backfill --symbol 005930 --limit 1`
- API-triggered universe refresh: `curl -X POST http://127.0.0.1:8000/eod/pykrx/universe/refresh`
- API-triggered backfill: `curl -X POST "http://127.0.0.1:8000/eod/pykrx/backfill?start_date=19560101"`
- DART company snapshot smoke test: `curl "http://127.0.0.1:8000/disclosures/dart/company-snapshot?stock_code=005930"`
- DART fundamentals smoke test: `curl "http://127.0.0.1:8000/disclosures/dart/fundamentals?stock_code=005930&business_year=2025&report_code=11011&fs_div=CFS"`
- DART stored fundamentals history smoke test: `curl "http://127.0.0.1:8000/disclosures/dart/fundamentals/history?symbols=005930&symbols=000660&start_date=2024-01-01&end_date=2024-12-31"`
- DART fundamentals capture smoke test: `curl -X POST "http://127.0.0.1:8000/disclosures/dart/fundamentals/capture?limit=5"`
- DART fundamentals capture inspect smoke test: `curl "http://127.0.0.1:8000/disclosures/dart/fundamentals/capture/inspect?capture_status=failed"`
- DART current-universe backfill smoke test: `curl -X POST "http://127.0.0.1:8000/disclosures/dart/fundamentals/backfill/current-universe?symbol_limit=5&capture_limit=25"`
- DART daily incremental fundamentals smoke test: `curl -X POST "http://127.0.0.1:8000/disclosures/dart/fundamentals/daily/run-if-due?market_date=2026-04-14&symbol_limit=25&capture_limit=100"`
- DART daily incremental fundamentals inspect: `curl "http://127.0.0.1:8000/disclosures/dart/fundamentals/daily/inspect?market_date=2026-04-14"`
- DART current-universe backfill continuation CLI: `python3.11 -m app.dart.backfill_current_universe --symbol-limit 10 --symbol-offset 5 --capture-limit 200 --fs-div CFS`
- DART current-universe overnight runner: `python3.11 -m app.dart.backfill_current_universe_runner --start-offset 25 --batch-size 10 --capture-limit 200 --fs-div CFS --log-file var/log/dart-backfill-overnight.log`
- DART filing original document smoke test: `curl -OJ "http://127.0.0.1:8000/disclosures/dart/filings/document?rcept_no=20260407000001"`
- DART filing XBRL smoke test: `curl -OJ "http://127.0.0.1:8000/disclosures/dart/filings/xbrl?rcept_no=20260407000001&report_code=11011"`
- ECOS series smoke test: `curl "http://127.0.0.1:8000/macro/ecos/series?stat_code=722Y001&cycle=M&start_period=202401&end_period=202403&item_code_1=0101000"`
- API-triggered daily auto path: `curl -X POST "http://127.0.0.1:8000/eod/pykrx/daily/run-if-due?market_date=2026-04-03&force=true"`
- API-triggered PyKRX enrichment collect: `curl -X POST "http://127.0.0.1:8000/eod/pykrx/enrichment/collect?market_date=2026-04-21&markets=KOSPI,KOSDAQ&categories=sector_classification,investor_flow_symbol,short_selling"`
- API-triggered PyKRX enrichment inspect: `curl "http://127.0.0.1:8000/eod/pykrx/enrichment?market_date=2026-04-21&markets=KOSPI&symbols=005930"`
- Stage catalog inspect path: `curl "http://127.0.0.1:8000/eod/pipeline/stages"`
- API-triggered explicit stage run: `curl -X POST "http://127.0.0.1:8000/eod/pipeline/run" -H "Content-Type: application/json" -d '{"market_date":"2026-04-03","stages":["price_collection","technical_snapshots","agent_feedback_summaries"],"symbols":["005930"]}'`
- API-triggered explicit stage run with signal-decider evaluation refresh: `curl -X POST "http://127.0.0.1:8000/eod/pipeline/run" -H "Content-Type: application/json" -d '{"market_date":"2026-04-30","stages":["price_collection","technical_snapshots","agent_feedback_summaries","signal_decider","signal_decider_evaluations"],"symbols":["005930"]}'`
- Preferred internal daily auto path: `python3.11 -m app.internal.ops eod-pykrx-daily-run-if-due --market-date 2026-04-03 --force`
- Internal PyKRX enrichment collect path: `python3.11 -m app.internal.ops eod-pykrx-enrichment-collect --market-date 2026-04-21 --markets KOSPI,KOSDAQ --categories sector_classification,investor_flow_symbol,short_selling`
- Internal PyKRX enrichment inspect path: `python3.11 -m app.internal.ops eod-pykrx-enrichment-inspect --market-date 2026-04-21 --symbols 005930`
- Internal stage catalog path: `python3.11 -m app.internal.ops eod-pipeline-stages`
- Internal explicit stage run path: `python3.11 -m app.internal.ops eod-pipeline-run --market-date 2026-04-03 --stages price_collection,technical_snapshots --symbols 005930`
- Manual signal-decider evaluation refresh over the recent window: `python3.11 -m app.internal.ops agent-signal-decider-evaluations-compute --market-date 2026-04-30 --symbols 005930 --horizons 1,3,5,10`
- Manual signal-decider evaluation coverage repair over an explicit range: `python3.11 -m app.internal.ops agent-signal-decider-evaluations-repair --market-date-from 2026-04-01 --market-date-to 2026-04-30 --horizons 1,3,5,10 --primary-horizons 5`
- Manual short-term WAIT-conflict gate replay over the issue-464 review week: `python3.11 -m app.internal.ops agent-short-term-wait-conflict-replay --market-date-from 2026-06-29 --market-date-to 2026-07-03 --output-format markdown`
- Manual technical snapshot rerun after EOD: `python3.11 -m app.eod.technical_snapshots --market-date 2026-04-07`
- Manual action summary rerun after EOD: `python3.11 -m app.eod.action_summaries --market-date 2026-04-07`
- Manual low-risk factor rerun from stored bars: `python3.11 -m app.internal.ops eod-low-risk-factors-compute --market-date 2026-04-23`
- Manual beta factor rerun from stored bars: `python3.11 -m app.internal.ops eod-beta-factors-compute --market-date 2026-04-23`
- Manual stored trending rank rebuild after EOD: `python3.11 -m app.internal.ops eod-trending-ranks-compute --market-date 2026-04-07`
- API-triggered technical snapshot rerun: `curl -X POST "http://127.0.0.1:8000/eod/technical-snapshots/compute?market_date=2026-04-07"`
- API-triggered action summary rerun: `curl -X POST "http://127.0.0.1:8000/eod/action-summaries/compute?market_date=2026-04-07"`
- API-triggered low-risk factor rerun: `curl -X POST "http://127.0.0.1:8000/eod/low-risk-factors/compute?market_date=2026-04-23"`
- Persisted low-risk factor inspect: `curl "http://127.0.0.1:8000/eod/low-risk-factors?market_date=2026-04-23&symbol=005930"`
- API-triggered beta factor rerun: `curl -X POST "http://127.0.0.1:8000/eod/beta-factors/compute?market_date=2026-04-23"`
- Persisted beta factor inspect: `curl "http://127.0.0.1:8000/eod/beta-factors?market_date=2026-04-23&symbol=005930"`
- Manual advanced value factor rerun: `python3.11 -m app.internal.ops eod-advanced-value-factors-compute --market-date 2026-04-24`
- API-triggered advanced value factor rerun: `curl -X POST "http://127.0.0.1:8000/eod/advanced-value-factors/compute?market_date=2026-04-24"`
- Persisted advanced value factor inspect: `curl "http://127.0.0.1:8000/eod/advanced-value-factors?market_date=2026-04-24&symbol=005930"`
- Manual unified value factor rerun: `python3.11 -m app.internal.ops eod-unified-value-factors-compute --market-date 2026-04-24`
- API-triggered unified value factor rerun: `curl -X POST "http://127.0.0.1:8000/eod/unified-value-factors/compute?market_date=2026-04-24"`
- Persisted unified value factor inspect: `curl "http://127.0.0.1:8000/eod/unified-value-factors?market_date=2026-04-24&symbol=005930"`
- Manual momentum factor rerun: `python3.11 -m app.internal.ops eod-momentum-factors-compute --market-date 2026-04-07`
- API-triggered momentum factor rerun: `curl -X POST "http://127.0.0.1:8000/eod/momentum-factors/compute?market_date=2026-04-07"`
- Manual composite factor rerun: `python3.11 -m app.internal.ops eod-composite-factors-compute --market-date 2026-04-24`
- API-triggered composite factor rerun: `curl -X POST "http://127.0.0.1:8000/eod/composite-factors/compute?market_date=2026-04-24"`
- Persisted composite factor inspect: `curl "http://127.0.0.1:8000/eod/composite-factors?market_date=2026-04-24&symbol=005930"`
- KIS ranking signal-feature collection: `python3.11 -m app.internal.ops kis-daily-signal-features-collect --market-date 2026-04-14`
- KIS intraday execution-context snapshot collection: `python3.11 -m app.internal.ops kis-intraday-snapshots-collect --symbols 005930 --include-news true`
- KIS market-risk snapshot collection: `python3.11 -m app.internal.ops eod-market-risk-snapshots-collect --market-date 2026-04-14`
- API-triggered KIS intraday execution-context snapshot collection: `curl -X POST "http://127.0.0.1:8000/broker/kis/intraday-snapshots/collect?symbols=005930&max_news_items=5"`
- API-triggered KIS intraday execution-context snapshot inspect: `curl "http://127.0.0.1:8000/broker/kis/intraday-snapshots?symbols=005930&limit=5"`
- API-triggered KIS market-risk snapshot collection: `curl -X POST "http://127.0.0.1:8000/eod/market-risk-snapshots/collect?market_date=2026-04-14"`
- API-triggered KIS market-risk filter compute: `curl -X POST "http://127.0.0.1:8000/eod/market-risk-filters/compute?market_date=2026-04-14"`
- Recent derived-context backfill: `python3.11 -m app.internal.ops agent-derived-contexts-backfill-recent`
- API-triggered KIS ranking signal-feature collection: `curl -X POST "http://127.0.0.1:8000/eod/signal-features/collect?market_date=2026-04-14"`

Internal ops commands from `apps/api`:

- `python3.11 -m app.internal.ops market-status`
- `python3.11 -m app.internal.ops quotes-tracked`
- `python3.11 -m app.internal.ops scanner-tracked-symbols`
- `python3.11 -m app.internal.ops eod-status`
- `python3.11 -m app.internal.ops eod-pipeline-stages`
- `python3.11 -m app.internal.ops eod-pipeline-run --market-date 2026-04-08 --stages price_collection,technical_snapshots`
- `python3.11 -m app.internal.ops eod-master-refresh-if-due --force`
- `python3.11 -m app.internal.ops eod-pykrx-daily-run-if-due --market-date 2026-04-08 --force`
- `python3.11 -m app.internal.ops eod-pykrx-enrichment-collect --market-date 2026-04-21 --categories sector_classification,investor_flow_symbol,short_selling`
- `python3.11 -m app.internal.ops eod-pykrx-enrichment-inspect --market-date 2026-04-21 --symbols 005930`
- `python3.11 -m app.internal.ops eod-trending-ranks-compute --market-date 2026-04-08`
- `python3.11 -m app.internal.ops eod-momentum-factors-compute --market-date 2026-04-08`
- `python3.11 -m app.internal.ops eod-piotroski-filter-collect --market-date 2026-04-08`
- `python3.11 -m app.internal.ops eod-market-risk-snapshots-collect --market-date 2026-04-14`
- `python3.11 -m app.internal.ops eod-market-risk-filters-compute --market-date 2026-04-14`
- `python3.11 -m app.internal.ops eod-low-risk-factors-compute --market-date 2026-04-23`
- `python3.11 -m app.internal.ops eod-beta-factors-compute --market-date 2026-04-23`
- `python3.11 -m app.internal.ops eod-advanced-value-factors-compute --market-date 2026-04-24`
- `python3.11 -m app.internal.ops eod-unified-value-factors-compute --market-date 2026-04-24`
- `python3.11 -m app.internal.ops eod-composite-factors-compute --market-date 2026-04-24`
- `python3.11 -m app.internal.ops trading-intraday-auto-sell-run-if-due --account-reference KIS:12345678-01 --symbol 005930 --force`
- `python3.11 -m app.internal.ops trading-next-open-execution-run-if-due --target-trade-date 2026-04-30 --dry-run true --force`
- `python3.11 -m app.internal.ops trading-signal-decider-slim-exec-payloads --dry-run true --target-trade-date 2026-05-22`
- `python3.11 -m app.internal.ops dart-fundamentals-daily-run-if-due --market-date 2026-04-14 --symbol-limit 250 --capture-limit 500`
- `python3.11 -m app.internal.ops agent-feedback-retry-cycles --market-date 2026-04-14 --symbols 005930`
- `python3.11 -m app.internal.ops agent-feedback-data-gap-summary --market-date-from 2026-05-01 --market-date-to 2026-05-06 --persist-missing-categories true`
- `python3.11 -m app.internal.ops agent-feedback-data-gap-quality-report --market-date-from 2026-05-01 --market-date-to 2026-05-06 --agent-name technical-summarizer`
- `python3.11 -m app.internal.ops agent-signal-decider-evaluations-compute --market-date 2026-04-30 --horizons 1,3,5,10`
- `python3.11 -m app.internal.ops agent-signal-decider-evaluations-repair --market-date-from 2026-04-01 --market-date-to 2026-04-30 --horizons 1,3,5,10 --primary-horizons 5`
- `python3.11 -m app.internal.ops agent-signal-decider-execution-outcomes-backfill --market-date-from 2026-06-29 --market-date-to 2026-07-03`
- `python3.11 -m app.internal.ops agent-short-term-wait-conflict-replay --market-date-from 2026-06-29 --market-date-to 2026-07-03 --output-format markdown`
- `python3.11 -m app.internal.ops agent-signal-decider-calendar-refresh --market-date 2026-04-30 --lookback-days 30 --horizons 1,3,5,10 --primary-horizons 1,3,5,10`
- Offline decider rule replay before activation: `python3.11 -m app.internal.ops agent-decider-replay --market-date-from 2026-06-15 --market-date-to 2026-06-19 --agent-name short-term-decider --rule-config-json '{"rules":[{"name":"stale_watch_trigger_drift","predicates":[{"type":"trigger_price_drift","max_below_trigger_pct":0.03}]}]}' --fee-bps 0 --slippage-bps 0 --output-format markdown`
  - The replay command accepts `--rule-config-json` or `--rule-config-path`, optional `--symbols`, `--horizon-days`, `--fee-bps`, and `--slippage-bps`.
  - Supported first-pass predicates include `trigger_price_drift`, `confidence`, `risk_flag`, `liquidity_bucket`, `factor_score_bucket`, `source_availability`, and generic `field` path checks.
  - Candidate predicates are evaluated only against persisted snapshot-time request/response/traceability/input-feature payloads. Realized outcomes are used only for baseline vs candidate P/L comparison.
- If historical duplicate `signal_decider_decision_snapshots.feedback_id` rows were repaired during a deploy or manual schema recovery, rerun `agent-signal-decider-evaluations-repair` for the affected date range and then `agent-signal-decider-calendar-refresh` so persisted review coverage and calendar items realign to the canonical deduplicated snapshots.
- `python3.11 -m app.internal.ops kis-daily-signal-features-collect --market-date 2026-04-14`
- `python3.11 -m app.internal.ops kis-intraday-snapshots-collect --symbols 005930 --include-news true`
- `python3.11 -m app.internal.ops agent-derived-contexts-backfill-recent`
- `python3.11 -m app.internal.index_audit`

Performance audit note:

- Use `python3.11 -m app.internal.index_audit` when large-table reads suddenly slow down after storage moves, cache resets, or dataset growth.
- It reports the heaviest sequentially-read tables from `pg_stat_user_tables`, shows existing indexes, and prints simple index suggestions for common date-driven access patterns.
- Current known hot-path examples include `daily_price_bars`, `daily_technical_snapshots`, `daily_action_summaries`, and `dart_fundamentals_rows`.

Notes:

- These commands call repo services directly instead of looping back through `127.0.0.1:8000`, so they are safe when `UPSTREAM_BEARER_TOKEN` is enabled.
- If HTTP must remain for an internal localhost caller, use `app.internal.http_client.InternalAPIClient`, which automatically sends the configured bearer token.

Notes:

- The pykrx path refreshes the active listed universe first, then backfills only those active symbols.
- Automatic daily ingestion uses the same pykrx universe/backfill persistence path, but only for the resolved market date and only once per day unless forced or retrying a partial/failed run. Automatic retries are capped at 5 runs per trade date and retry only the symbols that failed in the prior run.
- The automatic pykrx daily ingestion path now also persists `daily_price_contexts`, `daily_pykrx_enrichment_rows`, `daily_technical_snapshots`, `daily_action_summaries`, and day-based `daily_trending_ranks` after the day bar write completes.
- The same automatic daily path now also collects `daily_market_risk_snapshots` from KIS `inquire-price-2`, then computes `daily_market_risk_filter_results` from those stored snapshots for the successfully stored symbol universe. Per-symbol KIS failures are reported as partial errors and do not abort the rest of the daily run.
- KIS ranking signal-feature collection is a separate ops path because it calls KIS ranking endpoints (`volume-power`, `overtime-volume`, and `overtime-fluctuation`) rather than pykrx bars.
- The KIS ranking signal-feature collection path also rebuilds `daily_market_participation_contexts` from the stored `daily_signal_features` rows for that market date.
- `python3.11 -m app.internal.ops agent-derived-contexts-backfill-recent` is the repo-owned bootstrap path for the new derived-context tables. It backfills the recent seven trading days ending yesterday from existing stored `daily_price_bars` and `daily_signal_features`, and it skips participation dates that do not yet have stored signal-feature rows.
- Duplicate bar writes are harmless because `daily_price_bars` upserts on `(symbol_id, trade_date)`, and the scheduler checks `eod_collection_runs` plus a Postgres advisory lock before starting.
- When pykrx market-wide universe endpoints are empty, the command falls back to the existing KIS master universe so delisted names are still excluded.
- Historical bars use pykrx's default adjusted OHLCV series because long-range `adjusted=False` calls returned empty data during local verification.
- `--limit` runs do not deactivate missing symbols in the DB, so they are safe for small smoke tests.
- The historical pykrx backfill writes into `symbols`, `daily_price_bars`, `eod_collection_runs`, `daily_price_contexts`, `daily_technical_snapshots`, `daily_action_summaries`, and day-based `daily_trending_ranks`.

Local overrides:

- Set `EOD_AUTO_PYKRX_ENABLED=false` in `apps/api/.env` to disable the recurring scheduler.
- Tune `EOD_AUTO_PYKRX_AFTER_CLOSE_MINUTES`, `EOD_AUTO_PYKRX_POLL_SECONDS`, `EOD_AUTO_PYKRX_MARKETS`, `EOD_AUTO_PYKRX_REFRESH_UNIVERSE`, `EOD_AUTO_PYKRX_STALE_RUN_MINUTES`, and `EOD_PYKRX_REQUEST_TIMEOUT_SECONDS` in `apps/api/.env`, then restart the API.
- Use `SIGNAL_DECIDER_NEXT_OPEN_ENABLED`, `SIGNAL_DECIDER_NEXT_OPEN_DRY_RUN`, `SIGNAL_DECIDER_NEXT_OPEN_ACCOUNT_TARGETS`, `SIGNAL_DECIDER_NEXT_OPEN_MIN_CONFIDENCE_SCORE_10`, `SIGNAL_DECIDER_NEXT_OPEN_MIN_EXPECTED_RETURN_PCT`, `SIGNAL_DECIDER_WAIT_CONFLICT_GATE_MODE`, and the `SIGNAL_DECIDER_NEXT_OPEN_REQUIRE_*` / `...BLOCK_ON_*` guardrails in `apps/api/.env`, then restart the API. Keep the WAIT-conflict gate at `report_only` until actual-outcome replay supports a hard block.
- Use `SIGNAL_DECIDER_EXEC_INTEGRATION_MODE=disabled|observe|dispatch`, `SIGNAL_DECIDER_EXEC_BASE_URL`, optional `SIGNAL_DECIDER_EXEC_BEARER_TOKEN`, `SIGNAL_DECIDER_EXEC_RUNTIME_MODE`, and `SIGNAL_DECIDER_EXEC_REQUEST_TIMEOUT_SECONDS` in `apps/api/.env` to control the core-to-exec integration lane, then restart the API.
- In `dispatch` mode, eligible `signal-decider` next-open buys and actionable `short-term-decider` watcher candidates with `confidence_score_10 >= 6.0` are handed to exec for account-aware sizing at/after 08:00 KST. Core sends budget/reference/target/stop/confidence/risk context, does not force buy quantity from budget/reference fallback sizing, and leaves accepted rows delegated unless exec returns a positive agent-approved quantity.
- Use `GET /eod/status` to inspect the scheduler state and last result.

## Audit logging

- Default log directory: `apps/api/var/log`
- Incoming API log file: `api-requests.log`
- Outbound integration log file: `upstream-http.log`
- Rotation uses `RotatingFileHandler` with `API_LOG_ROTATION_MAX_BYTES` and `API_LOG_ROTATION_BACKUP_COUNT`
- Logged fields are metadata only: request ID, method, path/target URL, redacted query params, status, timing, content type/length, client IP, user agent, and error type/message when a call fails
- Request and response bodies are omitted by default; secrets such as bearer tokens, API keys, app secrets, cookies, and token-like query params are redacted

Local validation:

1. From `apps/api`, start the API with `python -m uvicorn app.main:app --reload`
2. Call `curl -i -H "Authorization: Bearer <token>" http://127.0.0.1:8000/health` when `UPSTREAM_BEARER_TOKEN` is set
3. Verify portfolio wiring with `curl -H "Authorization: Bearer <token>" http://127.0.0.1:8000/portfolio/summary` after setting `PORTFOLIO_ACCOUNT_REFERENCES`
4. Trigger an upstream-backed route such as `curl -H "Authorization: Bearer <token>" "http://127.0.0.1:8000/fx/koreaexim/rates?search_date=20260406&currency=USD"` when bearer auth is enabled
5. Inspect `var/log/api-requests.log` and `var/log/upstream-http.log`

## Next ops step
Set this up under a long-running process manager (LaunchAgent, systemd, pm2, or equivalent).

## DART notes

- Set `DART_API_KEY` in `apps/api/.env` before calling DART-backed endpoints.
- `DART_CORP_CODE_CACHE_TTL_SECONDS` controls how long the in-memory corp-code mapping is reused before refreshing from Open DART.
- `POST /disclosures/dart/sync/events` polls `list.json` over a rolling recent window and upserts by `rcept_no` into the disclosure-event tables.
- The DART machine-DB path now stores only periodic financial-report disclosures; non-financial DART events are intentionally skipped during persistence.
- `GET /disclosures/dart/sync/inspect` returns the latest sync run, recent runs, aggregate counts, and recent stored events.
- `POST /disclosures/dart/fundamentals/capture` consumes financial-report disclosure events, infers report-period identity from the filing name, fetches `fnlttSinglAcntAll`, and persists raw snapshots plus normalized rows/highlights.
- `GET /disclosures/dart/fundamentals/capture/inspect` returns the latest capture run, recent capture runs, capture summary counts, and recent financial events with capture state.
- `POST /disclosures/dart/fundamentals/backfill/current-universe` batches over the current active symbol universe, clamps the start boundary to 2015-01-01 or later, syncs only financial report events for that slice, and then captures only that slice's pending/failed fundamentals.
- DART fundamentals capture statuses are operationally meaningful: `pending` and `failed` are retryable; DART `[013]` financial-statement no-data responses become terminal `no_data`; filing/request inference problems become terminal `mapping_failed`.
- `POST /disclosures/dart/fundamentals/daily/run-if-due` is the steady-state incremental refresh path: for each selected active symbol it checks the latest resolved fundamentals receipt date, syncs only newer DART financial disclosures for that symbol, captures exact pending/failed event IDs for stale or missing symbols, and skips an already completed market date unless `force=true`. Limited daily runs prefer retryable events with fewer attempts and older receipt dates first.
- The daily fundamentals refresh path now also computes and upserts `daily_piotroski_filter_results` for the same market date. It stores one row for every evaluated symbol, including both passed and not-passed results, and returns `piotroski_filter` plus `piotroski_filter_error` in the refresh response.
- `GET /disclosures/dart/fundamentals/daily/inspect` reads persisted `dart_fundamentals_refresh_runs` summaries, including counts, sync run IDs, capture run IDs, and bounded symbol/event samples for operator review.
- Daily refresh and backfill results include `operator_buckets` so lookup/mapping errors such as `Unknown stock_code`, retryable capture failures, terminal no-data events, and remaining retryable backlog can be reviewed separately.
- For shell-driven continuation batches, prefer `python3.11 -m app.dart.backfill_current_universe ...` or keep the full `curl` URL quoted. Unquoted `?` / `&` query strings can be eaten by the shell before the API request is sent.
- For unattended overnight runs, use `python3.11 -m app.dart.backfill_current_universe_runner ...`. It owns batch iteration, writes JSONL progress records to a log file, and exits non-zero on failure after logging the failing offset.
- Open DART filing search (`list.json`) and `fnlttSinglAcntAll` practical support starts in 2015, so `daily_price_bars` data before 2015 does not imply DART fundamentals availability before 2015.
- `GET /disclosures/dart/filings/document` streams the original filing package returned by Open DART `document.xml`; the response is binary and intended for agent-side parsing, not JSON inspection.
- `GET /disclosures/dart/filings/xbrl` streams the XBRL package returned by Open DART `fnlttXbrl.xml`; pass `report_code` when the filing package contains multiple regular-report windows.
- `DART_SYNC_RECENT_DAYS` sets the default rolling window size for the event sync, and `DART_SYNC_PAGE_COUNT` controls DART `list.json` page size.
- `DART_FUNDAMENTALS_MIN_BUSINESS_YEAR` defaults the fundamentals backfill floor to `2015`.
- `DART_FUNDAMENTALS_DEFAULT_REPORT_CODE` defaults the fundamentals slice to `11011` (annual report).
- `DART_FUNDAMENTALS_DEFAULT_FS_DIV` defaults the fundamentals slice to `CFS` (consolidated statements).
- `DART_FUNDAMENTALS_EVENT_TIMEOUT_SECONDS` bounds one fundamentals-capture event before it is marked failed and the batch moves on.
- `DART_FUNDAMENTALS_STALE_RUN_MINUTES` marks abandoned `running` capture runs as failed before a new capture batch starts.

## Korea Exim FX notes

- Set `KOREAEXIM_API_KEY` in `apps/api/.env` before calling the Korea Exim FX route.
- `GET /fx/koreaexim/rates` currently targets the Korea Exim `AP01` daily exchange-rate slice only.
- Optional query parameters: `search_date=YYYYMMDD`, `currency=USD`, and `fallback_days=0..10`.
- `KOREAEXIM_FALLBACK_DAYS` controls how many prior days the service searches when the requested date is a weekend or holiday.

## Market snapshot notes

- `GET /market/snapshot` is a production-facing dashboard dependency and now resolves KIS reads against the real domain by default, even when the broader process still uses `KIS_USE_MOCK=true` for other flows.
- Keep `MARKET_SNAPSHOT_KIS_USE_REAL=true` unless you intentionally want deploy preflight and smoke-check to fail the market snapshot path.
- Required deploy-time secrets for the route are `KIS_APP_KEY`, `KIS_APP_SECRET`, and `KOREAEXIM_API_KEY`.
- Use `GET /broker/kis/health` to inspect `market_snapshot_runtime` when deploy preflight or smoke-check reports a market snapshot readiness failure.

## ECOS notes

- Set `BOK_ECOS_API_KEY` in `apps/api/.env` before calling ECOS-backed endpoints.
- `BOK_API_KEY` and `BOK_BASE_URL` are still accepted as legacy fallback names, but `BOK_ECOS_API_KEY` and `BOK_ECOS_BASE_URL` are the canonical settings.
