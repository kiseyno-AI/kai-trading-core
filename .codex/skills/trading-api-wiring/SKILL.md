---
name: trading-api-wiring
description: Wires the Next.js trading dashboard to trading-core or mock responses. Use when editing src/lib/trading-api.ts, src/lib/trading-upstream.ts, src/app/api/trading routes, src/types/trading-api.ts, environment variables for the trading API, or docs/API_CONTRACT.md.
---

# Trading API wiring

## Before changing behavior

1. Read `docs/API_CONTRACT.md` and `README.md` (Environment).
2. Trace callers: `tradingApi` in `src/lib/trading-api.ts` and hooks in `src/lib/use-*.ts`.

## Checklist

- Keep bearer tokens and upstream URLs in server-only env vars (`TRADING_API_UPSTREAM_*`). Never use `NEXT_PUBLIC_*` for secrets.
- New browser-facing paths should go through `src/app/api/trading/**` when the client must not talk to the upstream origin directly.
- Update `src/types/trading-api.ts` and any affected `src/features/**` when DTOs change.
- If the contract doc and code disagree, fix both in the same task.

## Mock path

When `NEXT_PUBLIC_TRADING_API_BASE_URL` is unset, mock data must still render the dashboard for local development.
