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

## Health checks
- `GET /health`
- `GET /eod/status`
- `POST /eod/master/refresh-if-due`

## Current runtime expectations
- Core server should stay up 24/7.
- Pre-market master refresh uses the KIS master zip files.
- A background runner checks market state every 60 seconds while the API process is alive.
- During `PREOPEN`, it refreshes the daily master if needed.
- During `CLOSED`, it attempts EOD capture if needed.
- Database persistence stores symbol master data, tracked symbols, quote cache/history, market status snapshots, EOD bars, and EOD run records.

## Database bootstrap

Before first startup on a fresh machine, from `apps/api`:

1. `pip install -r requirements.txt`
2. Create the Postgres database and user referenced by `.env`
3. `alembic upgrade head`
4. `python -c "from app.main import app"`

## Next ops step
Set this up under a long-running process manager (LaunchAgent, systemd, pm2, or equivalent).
