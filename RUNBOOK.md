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

## Health checks
- `GET /health`
- `GET /eod/status`
- `POST /eod/master/refresh-if-due`

## Current runtime expectations
- Core server should stay up 24/7.
- Pre-market master refresh uses the KIS master zip files.
- EOD capture uses the daily universe built from the refreshed master.
- Database persistence is not wired yet.

## Next ops step
Set this up under a long-running process manager (LaunchAgent, systemd, pm2, or equivalent).
