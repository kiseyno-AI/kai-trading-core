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

If you keep `DB_AUTO_UPGRADE=true`, normal FastAPI startup will also try to bring the schema to `head`.

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
