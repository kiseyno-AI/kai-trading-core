# trading-core

Core trading platform repository.

## Scope
- execution engine
- data pipeline
- shared domain models
- docs
- research

## API bootstrap
- `apps/api` contains the initial FastAPI-based broker integration skeleton.
- KIS credentials are loaded from environment variables, not hardcoded.
- `apps/api/start-api.sh` starts the API with `.env` loaded.
- Set `UPSTREAM_BEARER_TOKEN` to a non-empty value to require `Authorization: Bearer <token>` on incoming API requests; leave it unset/empty for local development.

## Runtime direction
- The core API is intended to run 24/7.
- Pre-market master refresh prepares the daily symbol universe.
- A background runner inside the API process handles pre-market refresh and after-close EOD checks.
- After market close, EOD capture can use that universe for OHLCV collection.
- Database persistence will be connected after the DB layer is ready.
