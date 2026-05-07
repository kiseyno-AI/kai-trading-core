# KIS API Onboarding

## What is implemented
- FastAPI skeleton under `apps/api`
- KIS config separated into environment variables
- KIS healthcheck route
- KIS access-token issue route
- KIS quote route
- KIS intraday snapshot collect/read routes for execution-aware context persistence

## Files
- `apps/api/app/core/settings.py`
- `apps/api/app/brokers/kis/client.py`
- `apps/api/app/api/routes_kis.py`
- `apps/api/app/market/kis_intraday_snapshots.py`
- `apps/api/app/market_data_store.py`
- `apps/api/app/internal/ops.py`
- `apps/api/app/main.py`
- `apps/api/.env.example`

## What you need to do
1. Copy `.env.example` to `.env`
2. Fill in:
   - `KIS_APP_KEY`
   - `KIS_APP_SECRET`
3. Keep `KIS_USE_MOCK=true` for initial testing
4. Optional intraday snapshot knobs:
   - `KIS_INTRADAY_SNAPSHOT_ENABLED=true`
   - `KIS_INTRADAY_SNAPSHOT_MARKET_CODE=J`
   - `KIS_INTRADAY_SNAPSHOT_MAX_RANKING_PAGES=2`
   - `KIS_INTRADAY_SNAPSHOT_NEWS_LIMIT=5`
5. Install dependencies:
   - `pip install -r apps/api/requirements.txt`
6. Run server:
   - `uvicorn app.main:app --app-dir apps/api --reload`

## Test routes
- `GET /health`
- `GET /broker/kis/health`
- `POST /broker/kis/token`
- `GET /broker/kis/quote?symbol=005930&market_code=J`
- `POST /broker/kis/intraday-snapshots/collect?symbols=005930`
- `GET /broker/kis/intraday-snapshots?symbols=005930`

## Operator command
- From `apps/api`, prefer `python3.11 -m app.internal.ops kis-intraday-snapshots-collect --symbols 005930 --include-news true` for shell-driven capture.

## Notes
- Do not commit real secrets
- Start with mock trading base URL first
- `GET /market/snapshot` is the exception to the general mock-first guidance: it uses real-domain KIS market data by default because the selected index and investor-flow APIs do not exist on the mock domain. Keep `MARKET_SNAPSHOT_KIS_USE_REAL=true` and set `KOREAEXIM_API_KEY` alongside the KIS credentials when you need that route.
- The intraday snapshot path is separate from PyKRX/DART EOD jobs. When KIS credentials are missing or `KIS_INTRADAY_SNAPSHOT_ENABLED=false`, the collect route returns `status=disabled` instead of failing or trying partial live work.
- Add account, quote, balance, and order adapters after token flow is confirmed
