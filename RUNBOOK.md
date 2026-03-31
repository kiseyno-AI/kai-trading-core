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
- Database persistence is not wired yet.

## Next ops step
Set this up under a long-running process manager (LaunchAgent, systemd, pm2, or equivalent).
