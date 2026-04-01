---
name: fastapi-trading-api
description: FastAPI app structure, routers, middleware, lifespan, and settings for kai-trading-core. Use when editing app/main.py, app/api/routes_*.py, app/core/settings.py, or adding HTTP endpoints.
---

# FastAPI trading API (apps/api)

## Entry

- **`app/main.py`:** `FastAPI` app, `lifespan` starts/stops `background_runner`, CORS, bearer middleware (`UPSTREAM_BEARER_TOKEN`), router includes.
- **Routers:** `app/api/routes_*.py` — each should use `APIRouter` with a clear `prefix` and `tags`.

## Patterns

- **Settings:** `app/core/settings.py` — `pydantic_settings.BaseSettings`, env from `.env`. Add new config fields here with defaults and env names; document in `apps/api/.env.example`.
- **Responses:** Prefer explicit response models or stable `dict` shapes when consumers (e.g. trading-web) depend on them.
- **Errors:** Use `HTTPException` with safe `detail` messages; do not leak stack traces or secrets to clients in production paths.
- **Background work:** EOD and market-driven logic ties to `app/eod/` and lifespan; avoid blocking the event loop in async routes (use async httpx, thread pool only when justified).

## Consumer alignment

- Paths like `/market/status`, `/quotes/tracked`, `/scanner/...` should stay compatible with the frontend BFF contract; update `docs/API_BUILD_ORDER.md` (and external contract docs) when routes or payloads change.
