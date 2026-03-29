# KIS API Onboarding

## What is implemented
- FastAPI skeleton under `apps/api`
- KIS config separated into environment variables
- KIS healthcheck route
- KIS access-token issue route

## Files
- `apps/api/app/core/settings.py`
- `apps/api/app/brokers/kis/client.py`
- `apps/api/app/api/routes_kis.py`
- `apps/api/app/main.py`
- `apps/api/.env.example`

## What you need to do
1. Copy `.env.example` to `.env`
2. Fill in:
   - `KIS_APP_KEY`
   - `KIS_APP_SECRET`
3. Keep `KIS_USE_MOCK=true` for initial testing
4. Install dependencies:
   - `pip install -r apps/api/requirements.txt`
5. Run server:
   - `uvicorn app.main:app --app-dir apps/api --reload`

## Test routes
- `GET /health`
- `GET /broker/kis/health`
- `POST /broker/kis/token`

## Notes
- Do not commit real secrets
- Start with mock trading base URL first
- Add account, quote, balance, and order adapters after token flow is confirmed
