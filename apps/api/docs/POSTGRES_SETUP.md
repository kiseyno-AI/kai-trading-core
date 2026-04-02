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

- FastAPI startup initializes database tables via SQLAlchemy `create_all`
- `/health` reports database connectivity
- Initial table: `tracked_symbols`

## Next recommended step

Move from `create_all` to Alembic migrations once the schema stabilizes.
