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

## Runtime direction
- The core API is intended to run 24/7.
- Pre-market master refresh prepares the daily symbol universe.
- After market close, EOD capture can use that universe for OHLCV collection.
- Database persistence will be connected after the DB layer is ready.
