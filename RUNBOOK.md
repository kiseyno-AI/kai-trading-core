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
- starts FastAPI via uvicorn on `127.0.0.1:8000` by default
- if `UPSTREAM_BEARER_TOKEN` is set, all incoming API requests require `Authorization: Bearer <token>`
- local orchestration should prefer `python3.11 -m app.internal.ops ...` over raw localhost `curl` so bearer auth does not break internal jobs
- if `DB_AUTO_UPGRADE=true`, startup also runs Alembic migrations to `head`
- if `API_REQUEST_LOGGING_ENABLED=true` and/or `UPSTREAM_HTTP_LOGGING_ENABLED=true`, the service writes rotating JSONL audit logs under `apps/api/var/log` by default

## Health checks
- `GET /health`
- `GET /macro/ecos/series?stat_code=722Y001&cycle=M&start_period=202401&end_period=202403&item_code_1=0101000`
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
- `POST /eod/master/refresh-if-due`
- `POST /eod/pykrx/universe/refresh`
- `POST /eod/pykrx/backfill`
- `POST /eod/pykrx/daily/run-if-due`
- `POST /eod/technical-snapshots/compute`
- `GET /eod/technical-snapshots`
- `POST /eod/action-summaries/compute`
- `GET /eod/action-summaries`
- `POST /eod/signal-features/collect`
- `GET /eod/signal-features`

## Current runtime expectations
- Core server should stay up 24/7.
- Pre-market master refresh uses the KIS master zip files.
- A background runner checks market state every 60 seconds while the API process is alive.
- During `PREOPEN`, it refreshes the daily master if needed.
- During `CLOSED`, after `EOD_AUTO_PYKRX_AFTER_CLOSE_MINUTES`, it schedules a thread-based pykrx daily EOD run if that trade date is available and not already completed.
- Database persistence stores symbol master data, tracked symbols, quote cache/history, market status snapshots, EOD bars, EOD run records, daily technical/action snapshots, KIS daily signal features, and stored day-based trending ranks.

## Database bootstrap

Before first startup on a fresh machine, from `apps/api`:

1. `pip install -r requirements.txt`
2. Create the Postgres database and user referenced by `.env`
3. `alembic upgrade head`
4. `python -c "from app.main import app"`

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
- Preferred internal daily auto path: `python3.11 -m app.internal.ops eod-pykrx-daily-run-if-due --market-date 2026-04-03 --force`
- Manual technical snapshot rerun after EOD: `python3.11 -m app.eod.technical_snapshots --market-date 2026-04-07`
- Manual action summary rerun after EOD: `python3.11 -m app.eod.action_summaries --market-date 2026-04-07`
- Manual stored trending rank rebuild after EOD: `python3.11 -m app.internal.ops eod-trending-ranks-compute --market-date 2026-04-07`
- API-triggered technical snapshot rerun: `curl -X POST "http://127.0.0.1:8000/eod/technical-snapshots/compute?market_date=2026-04-07"`
- API-triggered action summary rerun: `curl -X POST "http://127.0.0.1:8000/eod/action-summaries/compute?market_date=2026-04-07"`
- KIS ranking signal-feature collection: `python3.11 -m app.internal.ops kis-daily-signal-features-collect --market-date 2026-04-14`
- API-triggered KIS ranking signal-feature collection: `curl -X POST "http://127.0.0.1:8000/eod/signal-features/collect?market_date=2026-04-14"`

Internal ops commands from `apps/api`:

- `python3.11 -m app.internal.ops market-status`
- `python3.11 -m app.internal.ops quotes-tracked`
- `python3.11 -m app.internal.ops scanner-tracked-symbols`
- `python3.11 -m app.internal.ops eod-status`
- `python3.11 -m app.internal.ops eod-master-refresh-if-due --force`
- `python3.11 -m app.internal.ops eod-pykrx-daily-run-if-due --market-date 2026-04-08 --force`
- `python3.11 -m app.internal.ops eod-trending-ranks-compute --market-date 2026-04-08`
- `python3.11 -m app.internal.ops dart-fundamentals-daily-run-if-due --market-date 2026-04-14 --symbol-limit 250 --capture-limit 500`
- `python3.11 -m app.internal.ops kis-daily-signal-features-collect --market-date 2026-04-14`
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
- The automatic pykrx daily ingestion path now also persists `daily_technical_snapshots`, `daily_action_summaries`, and day-based `daily_trending_ranks` after the day bar write completes.
- KIS ranking signal-feature collection is a separate ops path because it calls KIS ranking endpoints (`volume-power`, `overtime-volume`, and `overtime-fluctuation`) rather than pykrx bars.
- Duplicate bar writes are harmless because `daily_price_bars` upserts on `(symbol_id, trade_date)`, and the scheduler checks `eod_collection_runs` plus a Postgres advisory lock before starting.
- When pykrx market-wide universe endpoints are empty, the command falls back to the existing KIS master universe so delisted names are still excluded.
- Historical bars use pykrx's default adjusted OHLCV series because long-range `adjusted=False` calls returned empty data during local verification.
- `--limit` runs do not deactivate missing symbols in the DB, so they are safe for small smoke tests.
- The backfill writes into `symbols`, `daily_price_bars`, `eod_collection_runs`, `daily_technical_snapshots`, `daily_action_summaries`, and day-based `daily_trending_ranks`.

Local overrides:

- Set `EOD_AUTO_PYKRX_ENABLED=false` in `apps/api/.env` to disable the recurring scheduler.
- Tune `EOD_AUTO_PYKRX_AFTER_CLOSE_MINUTES`, `EOD_AUTO_PYKRX_POLL_SECONDS`, `EOD_AUTO_PYKRX_MARKETS`, `EOD_AUTO_PYKRX_REFRESH_UNIVERSE`, `EOD_AUTO_PYKRX_STALE_RUN_MINUTES`, and `EOD_PYKRX_REQUEST_TIMEOUT_SECONDS` in `apps/api/.env`, then restart the API.
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
3. Trigger an upstream-backed route such as `curl -H "Authorization: Bearer <token>" "http://127.0.0.1:8000/fx/koreaexim/rates?search_date=20260406&currency=USD"` when bearer auth is enabled
4. Inspect `var/log/api-requests.log` and `var/log/upstream-http.log`

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

## ECOS notes

- Set `BOK_ECOS_API_KEY` in `apps/api/.env` before calling ECOS-backed endpoints.
- `BOK_API_KEY` and `BOK_BASE_URL` are still accepted as legacy fallback names, but `BOK_ECOS_API_KEY` and `BOK_ECOS_BASE_URL` are the canonical settings.
