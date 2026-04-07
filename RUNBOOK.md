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
- if `DB_AUTO_UPGRADE=true`, startup also runs Alembic migrations to `head`
- if `API_REQUEST_LOGGING_ENABLED=true` and/or `UPSTREAM_HTTP_LOGGING_ENABLED=true`, the service writes rotating JSONL audit logs under `apps/api/var/log` by default

## Health checks
- `GET /health`
- `GET /macro/ecos/series?stat_code=722Y001&cycle=M&start_period=202401&end_period=202403&item_code_1=0101000`
- `GET /disclosures/dart/company-snapshot?stock_code=005930`
- `POST /disclosures/dart/sync/events?days=7`
- `GET /disclosures/dart/sync/inspect?financial_only=true`
- `GET /disclosures/dart/fundamentals?stock_code=005930&business_year=2025&report_code=11011&fs_div=CFS`
- `POST /disclosures/dart/fundamentals/capture?limit=20`
- `GET /disclosures/dart/fundamentals/capture/inspect?capture_status=failed`
- `GET /fx/koreaexim/rates?search_date=20260406&currency=USD`
- `GET /eod/status`
- `POST /eod/master/refresh-if-due`
- `POST /eod/pykrx/universe/refresh`
- `POST /eod/pykrx/backfill`
- `POST /eod/pykrx/daily/run-if-due`

## Current runtime expectations
- Core server should stay up 24/7.
- Pre-market master refresh uses the KIS master zip files.
- A background runner checks market state every 60 seconds while the API process is alive.
- During `PREOPEN`, it refreshes the daily master if needed.
- During `CLOSED`, after `EOD_AUTO_PYKRX_AFTER_CLOSE_MINUTES`, it schedules a thread-based pykrx daily EOD run if that trade date is available and not already completed.
- Database persistence stores symbol master data, tracked symbols, quote cache/history, market status snapshots, EOD bars, and EOD run records.

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
- DART fundamentals capture smoke test: `curl -X POST "http://127.0.0.1:8000/disclosures/dart/fundamentals/capture?limit=5"`
- DART fundamentals capture inspect smoke test: `curl "http://127.0.0.1:8000/disclosures/dart/fundamentals/capture/inspect?capture_status=failed"`
- ECOS series smoke test: `curl "http://127.0.0.1:8000/macro/ecos/series?stat_code=722Y001&cycle=M&start_period=202401&end_period=202403&item_code_1=0101000"`
- API-triggered daily auto path: `curl -X POST "http://127.0.0.1:8000/eod/pykrx/daily/run-if-due?market_date=2026-04-03&force=true"`

Notes:

- The pykrx path refreshes the active listed universe first, then backfills only those active symbols.
- Automatic daily ingestion uses the same pykrx universe/backfill persistence path, but only for the resolved market date and only once per day unless forced or retrying a partial/failed run.
- Duplicate bar writes are harmless because `daily_price_bars` upserts on `(symbol_id, trade_date)`, and the scheduler checks `eod_collection_runs` plus a Postgres advisory lock before starting.
- When pykrx market-wide universe endpoints are empty, the command falls back to the existing KIS master universe so delisted names are still excluded.
- Historical bars use pykrx's default adjusted OHLCV series because long-range `adjusted=False` calls returned empty data during local verification.
- `--limit` runs do not deactivate missing symbols in the DB, so they are safe for small smoke tests.
- The backfill writes into `symbols`, `daily_price_bars`, and `eod_collection_runs` without adding new schema.

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
2. Call `curl -i http://127.0.0.1:8000/health`
3. Trigger an upstream-backed route such as `curl "http://127.0.0.1:8000/fx/koreaexim/rates?search_date=20260406&currency=USD"`
4. Inspect `var/log/api-requests.log` and `var/log/upstream-http.log`

## Next ops step
Set this up under a long-running process manager (LaunchAgent, systemd, pm2, or equivalent).

## DART notes

- Set `DART_API_KEY` in `apps/api/.env` before calling DART-backed endpoints.
- `DART_CORP_CODE_CACHE_TTL_SECONDS` controls how long the in-memory corp-code mapping is reused before refreshing from Open DART.
- `POST /disclosures/dart/sync/events` polls `list.json` over a rolling recent window and upserts by `rcept_no` into the disclosure-event tables.
- `GET /disclosures/dart/sync/inspect` returns the latest sync run, recent runs, aggregate counts, and recent stored events.
- `POST /disclosures/dart/fundamentals/capture` consumes financial-report disclosure events, infers report-period identity from the filing name, fetches `fnlttSinglAcntAll`, and persists raw snapshots plus normalized rows/highlights.
- `GET /disclosures/dart/fundamentals/capture/inspect` returns the latest capture run, recent capture runs, capture summary counts, and recent financial events with capture state.
- `DART_SYNC_RECENT_DAYS` sets the default rolling window size for the event sync, and `DART_SYNC_PAGE_COUNT` controls DART `list.json` page size.
- `DART_FUNDAMENTALS_DEFAULT_REPORT_CODE` defaults the fundamentals slice to `11011` (annual report).
- `DART_FUNDAMENTALS_DEFAULT_FS_DIV` defaults the fundamentals slice to `CFS` (consolidated statements).

## Korea Exim FX notes

- Set `KOREAEXIM_API_KEY` in `apps/api/.env` before calling the Korea Exim FX route.
- `GET /fx/koreaexim/rates` currently targets the Korea Exim `AP01` daily exchange-rate slice only.
- Optional query parameters: `search_date=YYYYMMDD`, `currency=USD`, and `fallback_days=0..10`.
- `KOREAEXIM_FALLBACK_DAYS` controls how many prior days the service searches when the requested date is a weekend or holiday.

## ECOS notes

- Set `BOK_ECOS_API_KEY` in `apps/api/.env` before calling ECOS-backed endpoints.
- `BOK_API_KEY` and `BOK_BASE_URL` are still accepted as legacy fallback names, but `BOK_ECOS_API_KEY` and `BOK_ECOS_BASE_URL` are the canonical settings.
