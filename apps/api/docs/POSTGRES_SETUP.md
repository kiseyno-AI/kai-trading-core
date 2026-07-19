# PostgreSQL Setup

## Current local defaults

- DB: `kai_trading`
- User: `kai_trading_app`
- Port: `5432`
- Host: `localhost`

## Environment variables

Use these keys in local `.env`:

- `DATABASE_URL`
- `POSTGRES_HOST`
- `POSTGRES_PORT`
- `POSTGRES_DB`
- `POSTGRES_USER`
- `POSTGRES_PASSWORD`

## Current app behavior

- FastAPI startup can auto-apply Alembic migrations when `DB_AUTO_UPGRADE=true`
- `apps/api/start-api.sh` now runs migration preflight before uvicorn when `DB_AUTO_UPGRADE=true`, applies `alembic upgrade head` itself when the database is safely behind head, then verifies `--require-head`
- `/health` reports database connectivity
- Schema is managed in `apps/api/db_migrations/versions/`

## Fresh-machine bootstrap

From `apps/api`:

1. Install dependencies:
   `pip install -r requirements.txt`
2. Ensure PostgreSQL is running and the database/user from `.env` already exist.
3. Apply the schema:
   `alembic upgrade head`
4. Verify the app imports cleanly:
   `python -c "from app.main import app"`

If you keep `DB_AUTO_UPGRADE=true` and launch uvicorn directly, normal FastAPI startup will still try to bring the schema to `head`. The `./start-api.sh` wrapper now owns that migration step itself and clears `DB_AUTO_UPGRADE` for the uvicorn child process after the schema is verified at head, so startup does not immediately run a duplicate migration pass.

For deploy-time hygiene from `apps/api`:

1. `python3.11 -m app.internal.deploy_hygiene preflight --format text`
2. `python3.11 -m app.internal.deploy_hygiene schema-drift-preflight --format text`
3. `alembic upgrade head`
4. `python3.11 -m app.internal.deploy_hygiene preflight --require-head --format text`
5. Restart the API
6. `python3.11 -m app.internal.deploy_hygiene smoke-check --base-url http://127.0.0.1:8000 --format text`

If the preflight flags likely duplicate-table drift, verify the live table shape against the pending migration and use `alembic stamp <matching_revision>` only after schema parity is confirmed. If parity is not clean, prefer a corrective migration over destructive table drops.
If runtime writes fail, reproduce the real write path first, inspect the schema-drift report second, and debug provider payloads or credentials only after the expected constraints are present.

## First-pass schema

- `symbols` - instrument master for KRX/KIS symbols
- `tracked_symbols` - persisted dashboard watchlist / scanner inputs
- `quote_snapshots` - append-only quote history
- `current_quote_cache` - last known quote per symbol for restart-safe fallback
- `market_status_snapshots` - persisted market clock snapshots
- `daily_price_bars` - end-of-day OHLCV per symbol and trade date
- `eod_collection_runs` - run-level EOD collection status and counts

## Next recommended step

Add follow-up migrations as the product contract around symbol taxonomy, retention, and non-KRX instruments becomes explicit.
