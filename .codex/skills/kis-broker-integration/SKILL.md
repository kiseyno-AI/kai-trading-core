---
name: kis-broker-integration
description: KIS Open API integration, mock vs production, and token/quote flows. Use when editing app/brokers/kis/, KIS-related routes in app/api/routes_kis.py, or KIS_* settings.
---

# KIS broker integration

## Configuration (`app/core/settings.py`)

- **`kis_app_key` / `kis_app_secret`:** Required for real calls; never commit.
- **`kis_use_mock`:** When true, use virtual/mock OpenAPI host (`kis_mock_base_url`); when false, `kis_base_url` (production).
- **`kis_token_refresh_margin_seconds`:** Token refresh timing buffer.

## Code layout

- **`app/brokers/kis/`:** Client, provider, schemas — keep HTTP and parsing separate from route handlers where possible.
- **`app/api/routes_kis.py`:** HTTP surface for token, health, quotes (see `docs/API_BUILD_ORDER.md`).

## Practices

- Prefer explicit timeouts and error mapping from httpx failures to API responses.
- Do not add order submission or account mutation endpoints without explicit approval and risk review.
- After changing auth or base URLs, verify against KIS documentation and `docs/KIS_API_ONBOARDING.md` when applicable.
