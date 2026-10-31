# KIS API Onboarding

## What is implemented
- FastAPI skeleton under `apps/api`
- KIS config separated into environment variables
- KIS healthcheck route
- KIS access-token issue route
- KIS quote route
- KIS intraday snapshot collect/read routes for execution-aware context persistence
- Core-owned KIS realtime monitored price websocket with process-local cache and Core fan-out routes
- Core-owned KRX trading-calendar reads through KIS `chk-holiday` (`CTCA0903R`)

## Files
- `apps/api/app/core/settings.py`
- `apps/api/app/brokers/kis/client.py`
- `apps/api/app/api/routes_kis.py`
- `apps/api/app/market/session.py`
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
   - `UPSTREAM_BEARER_TOKEN` to enable authenticated token issuance
3. Keep `KIS_USE_MOCK=true` for initial testing
4. Optional intraday snapshot knobs:
   - `KIS_INTRADAY_SNAPSHOT_ENABLED=true`
   - `KIS_INTRADAY_SNAPSHOT_MARKET_CODE=J`
   - `KIS_INTRADAY_SNAPSHOT_MAX_RANKING_PAGES=2`
   - `KIS_INTRADAY_SNAPSHOT_NEWS_LIMIT=5`
5. Optional realtime monitored price knobs:
   - `KIS_REALTIME_PRICE_ENABLED=true`
   - `KIS_REALTIME_PRICE_TR_ID=H0STCNT0`
   - `KIS_REALTIME_PRICE_MAX_SUBSCRIPTIONS=40`
   - `KIS_REALTIME_PRICE_STALE_AFTER_SECONDS=30`
   - `SIGNAL_DECIDER_WAIT_RECHECK_MONITORING_ENABLED=false`
   - `SIGNAL_DECIDER_WAIT_RECHECK_MAX_SYMBOLS=20`
6. Install dependencies:
   - `pip install -r apps/api/requirements.txt`
7. Run server:
   - `uvicorn app.main:app --app-dir apps/api --reload`

## Test routes
- `GET /health`
- `GET /broker/kis/health`
- `POST /broker/kis/token` with `Authorization: Bearer <UPSTREAM_BEARER_TOKEN>`
- `GET /broker/kis/quote?symbol=005930&market_code=J`
- `GET /quotes/realtime/monitored`
- `POST /broker/kis/intraday-snapshots/collect?symbols=005930`
- `GET /broker/kis/intraday-snapshots?symbols=005930`

## Operator command
- From `apps/api`, prefer `python3.11 -m app.internal.ops kis-intraday-snapshots-collect --symbols 005930 --include-news true` for shell-driven capture.

## Notes
- Do not commit real secrets
- The token route returns presence, cache, expiry, type, and safe defer metadata only. It never returns the raw KIS access token and is disabled when `UPSTREAM_BEARER_TOKEN` is unset.
- Start with mock trading base URL first
- `GET /market/snapshot` is the exception to the general mock-first guidance: it uses real-domain KIS market data by default because the selected index and investor-flow APIs do not exist on the mock domain. Keep `MARKET_SNAPSHOT_KIS_USE_REAL=true` and set `KOREAEXIM_API_KEY` alongside the KIS credentials when you need that route.
- KRX open-day checks use KIS `chk-holiday` `opnd_yn` as the primary truth with a small in-process TTL cache. If KIS calendar data is unavailable, read-only market/status views can fall back to `exchange_calendars` with explicit `calendar_source` metadata; those non-authoritative results use a separate 60-second per-date cache to prevent retry storms and strict live paths continue to fail closed. Live order submission/dispatch paths fail closed unless an operator uses the existing `force=true` override.
- The intraday snapshot path is separate from PyKRX/DART EOD jobs. When KIS credentials are missing or `KIS_INTRADAY_SNAPSHOT_ENABLED=false`, the collect route returns `status=disabled` instead of failing or trying partial live work.
- Core opens one KIS websocket, subscribes to current monitored symbols with bounded/deduplicated updates, keeps latest ticks in process memory, and serves web clients through Core routes. When WAIT recheck monitoring is explicitly enabled, a fresh regular-session best ask may persist one reassessment event per `(target_trade_date, symbol, agent_name)` for an eligible prior-session WAIT. It calls the exact originating lane: `signal-decider` may freshly promote to trigger-gated `buy`, while `short-term-decider` may freshly promote to `watch_for_trigger`. Neither event grants direct order authority. Durable event, immutable context, quote observations, and actual source snapshot must match exactly before dispatch/readback; swapped lineage, stale/pre-open/limited ticks, or source drift fail closed. Do not persist the raw tick stream or let browsers open KIS sockets directly.
- `KIS_REALTIME_PRICE_ENABLED=true` means the feature is configured, not that a socket is always active. Core requests a realtime approval key and opens the price websocket only during a KIS-calendar-verified open KRX trading session. Closed, weekend, holiday, fallback-calendar, and unknown calendar states stay disconnected and report `market_session_active=false`; inactive sessions also retire the replaceable WAIT partition while preserving base holding/Exec monitoring membership for the next open. `market_session_active` is session eligibility, while `websocket_connected=true` plus a `connected`, `subscribed`, or `limited` status proves an actual socket.
- Monitored-symbol source refresh is time-bounded. A stalled dashboard/read-model refresh preserves the last observed symbol set, records a retryable health error, and respects the configured refresh interval instead of preventing websocket startup or retrying in a tight loop.
- KIS's official open-trading-api `ccnl_krx` sample uses `H0STCNT0` for domestic realtime transaction prices in both real and mock environments. Keep `KIS_REALTIME_PRICE_TR_ID=H0STCNT0` unless KIS publishes a replacement.
- Add account, quote, balance, and order adapters after token flow is confirmed
