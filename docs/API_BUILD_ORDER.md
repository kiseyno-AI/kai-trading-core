# API build order

## Current milestone order
1. KIS token + health + quote routes
2. Market status route
3. Scanner tracked-symbol routes
4. Quotes tracked + single-symbol routes
5. Symbol search route
6. EOD status + master refresh + capture routes
7. Macro BOK ECOS route
8. FX Korea Exim route
9. DART company snapshot route
10. DART disclosure sync + inspect routes
11. DART stored fundamentals history retrieval route
12. EOD daily technical snapshot compute/inspect routes
13. EOD daily action summary compute/inspect routes
14. Agent symbol/day aggregate context read route
15. DART daily incremental fundamentals refresh route
16. Favorite symbol read/delete routes
17. Stored trending stocks read route
18. KIS realtime trending route
19. KIS daily ranking signal-feature collect/inspect routes
20. KIS daily market-risk snapshot collect/inspect routes
21. KIS daily market-risk filter compute/read routes
22. Daily Piotroski filter result read route
23. Daily low-risk factor compute/read routes
24. Agent feedback intake/read routes for OpenClaw summaries
25. Role-specific post-EOD agent feedback dispatch route
26. Daily price momentum factor compute/inspect routes
27. Daily value factor compute/read routes
28. Daily beta factor compute/read routes
29. Daily advanced value factor compute/read routes
30. Daily composite factor compute/read routes
31. Signal-decider evaluation, diagnostics, and policy-candidate routes
32. Internal trading order control, inquiry, and reconciliation routes

## Current routes
- `POST /broker/kis/token`
- `GET /broker/kis/health`
- `GET /broker/kis/quote?symbol=005930&market_code=J`
- `POST /trading/orders/buy`
- `POST /trading/orders/sell`
- `POST /trading/orders/reconcile`
- `POST /trading/orders/{client_order_id}/cancel`
- `POST /trading/orders/{client_order_id}/amend`
- `POST /trading/orders/{client_order_id}/reconcile`
- `GET /trading/orders/{client_order_id}?account_reference=KIS:12345678-01&refresh=false`
- `GET /trading/orders?account_reference=KIS:12345678-01&status=submitted&limit=100`
- `GET /trading/orders/inspect?account_reference=KIS:12345678-01&limit=50`
- `GET /trading/orders/lifecycle?account_reference=KIS:12345678-01&limit=25`
- `GET /trading/orders/mismatches?account_reference=KIS:12345678-01&limit=50`
- `GET /trading/orders/stale-pending?account_reference=KIS:12345678-01&limit=50`
- `GET /trading/orders/realtime-health`
- `GET /trading/orders/{client_order_id}/timeline?account_reference=KIS:12345678-01&refresh=false`
- `GET /trading/accounts/{account_reference}/summary`
- `GET /trading/accounts/{account_reference}/positions?symbol=005930&include_zero=false`
- `GET /trading/accounts/{account_reference}/buying-power?symbol=005930&price=57000&order_type=market`
- `GET /market/status`
- `GET /quotes/tracked`
- `GET /quotes/005930`
- `GET /scanner/tracked-symbols`
- `GET /scanner/realtime-trending?limit=20&market_code=J&sources=volume-rank,fluctuation,hts-top-view`
- `POST /scanner/tracked-symbols`
- `GET /scanner/trending-stocks?period=5d&direction=both&limit=20&markets=KOSPI`
- `GET /symbols/search?q=삼성&limit=20`
- `GET /symbols/favorites`
- `DELETE /symbols/favorites/005930`
- `GET /symbols/005930/price-bars?start_date=2021-04-09&end_date=2026-04-09`
- `GET /macro/bok/series?series_id=722Y001/0101000&start_period=20240101&end_period=20240131&cycle=D`
- `GET /fx/koreaexim/rates?search_date=20260408&data=AP01`
- `GET /disclosures/dart/company-snapshot?stock_code=005930`
- `POST /disclosures/dart/sync/events?days=5`
- `GET /disclosures/dart/sync/inspect?financial_only=true`
- `GET /disclosures/dart/fundamentals?stock_code=005930&business_year=2024&report_code=11011&fs_div=CFS`
- `POST /disclosures/dart/fundamentals/capture?limit=20&fs_div=CFS`
- `GET /disclosures/dart/fundamentals/history?symbols=005930&symbols=000660&start_date=2024-01-01&end_date=2025-12-31&fs_div=CFS`
- `GET /disclosures/dart/fundamentals/capture/inspect?capture_status=captured`
- `POST /disclosures/dart/fundamentals/daily/run-if-due?market_date=2026-04-14&symbol_limit=250&capture_limit=500&fs_div=CFS`
- `GET /disclosures/dart/fundamentals/daily/inspect?market_date=2026-04-14&fs_div=CFS`
- `POST /disclosures/dart/fundamentals/backfill/current-universe?symbol_limit=25&symbol_offset=0&capture_limit=250&fs_div=CFS`
- `GET /disclosures/dart/filings/document?rcept_no=20250312000123`
- `GET /disclosures/dart/filings/xbrl?rcept_no=20250312000123&report_code=11011`
- `GET /eod/status`
- `GET /eod/freshness`
- `GET /eod/pipeline/status?market_date=YYYY-MM-DD&fs_div=CFS`
- `GET /eod/pipeline/stages`
- `POST /eod/pipeline/run`
- `POST /eod/master/refresh-if-due?force=false`
- `POST /eod/capture-if-due`
- `POST /eod/capture?market_date=YYYY-MM-DD`
- `POST /eod/pykrx/universe/refresh?markets=KOSPI,KOSDAQ`
- `POST /eod/pykrx/backfill?start_date=20150101&end_date=20260408&markets=KOSPI,KOSDAQ`
- `POST /eod/pykrx/daily/run-if-due?market_date=YYYY-MM-DD`
- `POST /eod/technical-snapshots/compute?market_date=YYYY-MM-DD&symbols=005930,000660`
- `GET /eod/technical-snapshots?market_date=YYYY-MM-DD&symbols=005930,000660`
- `POST /eod/action-summaries/compute?market_date=YYYY-MM-DD&symbols=005930,000660`
- `GET /eod/action-summaries?market_date=YYYY-MM-DD&symbols=005930,000660`
- `POST /eod/signal-features/collect?market_date=YYYY-MM-DD&sources=volume-power,overtime-volume,overtime-fluctuation`
- `GET /eod/signal-features?market_date=YYYY-MM-DD&symbols=005930,000660`
- `POST /eod/market-risk-snapshots/collect?market_date=YYYY-MM-DD&symbols=005930,000660&market_code=J`
- `GET /eod/market-risk-snapshots?market_date=YYYY-MM-DD&symbol=005930`
- `POST /eod/market-risk-filters/compute?market_date=YYYY-MM-DD&symbols=005930,000660&market_code=J`
- `GET /eod/market-risk-filters?market_date=YYYY-MM-DD&symbol=005930`
- `POST /eod/momentum-factors/compute?market_date=YYYY-MM-DD&symbols=005930,000660`
- `GET /eod/momentum-factors?market_date=YYYY-MM-DD&symbols=005930,000660`
- `POST /eod/value-factors/compute?market_date=YYYY-MM-DD&symbols=005930,000660&source=value_factor_v1`
- `GET /eod/value-factors?market_date=YYYY-MM-DD&symbol=005930&limit=100`
- `GET /eod/piotroski-filter-results`
- `POST /eod/low-risk-factors/compute?market_date=YYYY-MM-DD&symbols=005930,000660`
- `GET /eod/low-risk-factors?market_date=YYYY-MM-DD&symbol=005930`
- `POST /eod/beta-factors/compute?market_date=YYYY-MM-DD&symbols=005930,000660&source=beta_v1`
- `GET /eod/beta-factors?market_date=YYYY-MM-DD&symbol=005930`
- `POST /eod/advanced-value-factors/compute?market_date=YYYY-MM-DD&symbols=005930,000660&source=advanced_value_v1`
- `GET /eod/advanced-value-factors?market_date=YYYY-MM-DD&symbol=005930`
- `POST /eod/composite-factors/compute?market_date=YYYY-MM-DD&symbols=005930,000660&source=composite_v1`
- `GET /eod/composite-factors?market_date=YYYY-MM-DD&symbol=005930`
- `GET /eod/screening-results?family=composite&market_date=YYYY-MM-DD&limit=100&offset=0`
- `GET /agent-context/symbol-day?market_date=YYYY-MM-DD&symbols=005930&symbols=000660&fs_div=CFS&disclosures_limit=5`
- `POST /agent-context/feedback`
- `POST /agent-context/feedback/dispatch`
- `POST /agent-context/feedback/dispatch/symbols`
- `POST /agent-context/feedback/retry-cycles`
- `POST /agent-context/feedback/session-cleanup/run`
- `GET /agent-context/feedback/session-cleanup?market_date=YYYY-MM-DD&agent_name=eod-summarizer`
- `GET /agent-context/feedback?market_date=YYYY-MM-DD&symbols=005930&agent_name=openclaw&limit=50`
- `GET /agent-context/feedback/technical-summaries?market_date=YYYY-MM-DD&symbol=005930&limit=50`
- `GET /agent-context/feedback/eod-summaries?market_date=YYYY-MM-DD&symbol=005930`
- `GET /agent-context/feedback/latest?market_date=YYYY-MM-DD&symbols=005930&symbols=000660`
- `GET /agent-context/feedback/payloads?market_date=YYYY-MM-DD&symbols=005930`
- `GET /agent-context/feedback/requests?market_date=YYYY-MM-DD&symbols=005930`
- `GET /agent-context/feedback/status?market_date=YYYY-MM-DD&symbols=005930&symbols=000660`
- `GET /agent-context/feedback/progress?market_date=YYYY-MM-DD&run_id=symbol-run-...`
- `GET /agent-context/signal-decider/decisions?market_date=YYYY-MM-DD&symbol=005930&limit=50`
- `GET /agent-context/signal-decider/decisions?market_date_from=YYYY-MM-DD&market_date_to=YYYY-MM-DD&action=wait&policy_version=signal_decider_policy_v1&verdict=missed_opportunity_after_wait&has_missed_opportunity=true&horizons=5`
- `GET /agent-context/signal-decider/decisions/{snapshot_id}?evaluation_version=signal_decider_eval_v1&horizons=1&horizons=5`
- `GET /agent-context/signal-decider/diagnostics?market_date=YYYY-MM-DD&symbol=005930&evaluation_version=signal_decider_eval_v1`
- `GET /agent-context/signal-decider/diagnostics/summary?market_date_from=YYYY-MM-DD&market_date_to=YYYY-MM-DD&symbols=005930&symbols=000660&horizons=5&evaluation_version=signal_decider_eval_v1&contributor_limit=20`
- `POST /agent-context/signal-decider/evaluations/compute`
- `GET /agent-context/signal-decider/evaluations?market_date=YYYY-MM-DD&symbol=005930&horizons=1&horizons=5`
- `GET /agent-context/signal-decider/decision-policies?status=active&limit=20`
- `GET /agent-context/signal-decider/decision-policies/signal_decider_policy_v1`
- `POST /agent-context/signal-decider/decision-policies/signal_decider_policy_v2/activate`
- `POST /agent-context/signal-decider/decision-policies/candidates/generate`
- `GET /agent-context/signal-decider/decision-policies/candidates?status=generated&evaluation_version=signal_decider_eval_v1`
- `POST /agent-context/signal-decider/policy-candidates/generate`
- `GET /agent-context/signal-decider/policy-candidates?status=generated&evaluation_version=signal_decider_eval_v1`

## Trading order adapter notes
- `POST /trading/orders/buy` and `POST /trading/orders/sell` are the internal order-control submit routes for the first KIS domestic cash adapter.
- `POST /trading/orders/{client_order_id}/cancel` and `POST /trading/orders/{client_order_id}/amend` are account-scoped control routes. They require `account_reference` in `KIS:<account_number>-<product_code>` format so the service can reconcile and control the correct broker account without exposing credentials.
- `POST /trading/orders/reconcile` runs an operator-triggered recent-order sweep over persisted KIS orders, and `POST /trading/orders/{client_order_id}/reconcile` forces the same broker-truth repair path for one order.
- `GET /trading/orders/{client_order_id}` and `GET /trading/orders` expose the persisted request snapshot, current broker-order snapshot, event timeline, and the persisted `last_reconciled_at` / `last_reconcile_status` state. `refresh=true` on the single-order read triggers KIS inquiry-based reconciliation before the response is returned.
- `GET /trading/orders/inspect` is the internal read surface for reconciliation operations. It returns the in-process scheduler snapshot plus the latest mismatch, stale-pending, and missing-fill-detail order buckets.
- `GET /trading/orders/lifecycle` is the web-facing monitor view for recent KIS orders. It groups today, open, partially filled, filled, canceled or rejected, and replaced orders into explicit buckets, and it also surfaces mismatch/stale counts plus realtime stream readiness so clients do not have to infer lifecycle groupings from raw statuses.
- `GET /trading/orders/mismatches` and `GET /trading/orders/stale-pending` are the direct operator review lists for reconcile mismatches and stale active submissions. They reuse the reconciliation inspect semantics but return web-friendly monitoring items with explicit attention flags and derived status labels.
- `GET /trading/orders/{client_order_id}/timeline` is the normalized timeline read for one order. It keeps the raw request/order payloads alongside a labeled event history so a web detail page can render lifecycle activity without reverse-mapping event codes client-side.
- `GET /trading/orders/realtime-health` is the trading-order-specific realtime websocket health view. It narrows the broader `GET /broker/kis/health` payload down to the order-notice stream and adds an explicit `healthy` summary plus operator attention flags.
- Submit idempotency is keyed by `broker_name + account_reference + client_order_id`. A repeated submit with the same payload returns the stored order snapshot instead of sending a second broker order. A repeated submit with a different payload returns an idempotency conflict.
- Durable persistence spans `order_requests`, `broker_orders`, and `broker_order_events`. KIS request and response payloads are preserved in the request snapshot, current broker snapshot, and event history for later debugging and reconciliation.
- Broker-truth repair is append-only: each reconcile pass writes a deduplicated reconciliation event when the observed broker truth changes, preserves the local-before vs local-after evidence in `last_reconcile_payload`, and never rewrites prior execution history.
- Automatic reconciliation now covers startup catch-up, periodic intraday sweeps, and realtime websocket reconnect recovery when the corresponding `KIS_ORDER_RECONCILE_*` settings are enabled. Manual shell access uses `python3.11 -m app.internal.ops trading-orders-reconcile ...` from `apps/api`.
- The stable KIS result-code and inquiry-state mapping contract for this adapter is documented in [docs/KIS_ORDER_RESULT_CODE_MAPPING.md](./KIS_ORDER_RESULT_CODE_MAPPING.md).

## Trading account inquiry notes
- `GET /trading/accounts/{account_reference}/summary` is the normalized KIS-backed cash and valuation snapshot for one KIS account reference. It combines `inquire-balance` and `inquire-account-balance` so UI cards can read cash and valuation fields without mapping KIS names client-side.
- `GET /trading/accounts/{account_reference}/positions` is the normalized holdings view. It defaults to active positions only, supports optional `symbol` filtering for pre-trade “already holding this?” checks, and exposes quantity, sellable quantity, pricing, market value, PnL, and derived weight per row.
- `GET /trading/accounts/{account_reference}/buying-power` is the pre-trade orderability read. Clients should call it before order submission with the intended `symbol`, `price`, and `order_type` so Core asks KIS directly for buyable quantity and amount instead of estimating from cached cash.
- The trading account inquiry routes reuse the same `KIS:<account_number>-<product_code>` account reference model already required by the order-control routes.

## KIS realtime order notice notes
- The API process now owns a background KIS websocket consumer for domestic order and execution notices when `KIS_REALTIME_ORDER_NOTICE_ENABLED=true` and `KIS_HTS_ID` is configured.
- The consumer subscribes to `H0STCNI0` in real accounts and `H0STCNI9` in mock accounts using the user HTS ID as the `tr_key`, following the official KIS realtime order-notice mechanism.
- Realtime notices append durable rows into `broker_order_events` with `source=ws_notice`, preserve the raw decrypted field payload, and patch the current `broker_orders` snapshot when the notice advances the known order state.
- Duplicate notices are deduplicated through a stable websocket event fingerprint. Older notices can still be persisted for audit, but they do not regress a newer `broker_orders` snapshot.
- `GET /broker/kis/health` now includes a `realtime_order_notices` block with connection status, last message timing, reconnect attempts, subscription or auth lifecycle state, last fatal vs recoverable error, and ingest counters.

## EOD freshness read-layer notes
- Canonical semantics live in `docs/EOD_STATUS_SEMANTICS_CONTRACT.md`.
- `GET /eod/freshness` is the compact web-facing read API for EOD ingestion freshness.
- It reports the current Seoul trade date, today's stage-specific status for price, technical analysis, action summary, and fundamentals, plus the latest stored dates for price bars, technical snapshots, and action summaries.
- `today.trade_date` is the current Seoul calendar date from `server_time[:10]`; weekends, holidays, and other break-days do not backshift `today` to a previous trading day.
- Today's price, technical-analysis, and action-summary stage totals are based on the current active symbol universe. Their succeeded counts come from same-day persisted rows for active symbols; once a stage has same-day rows, failed counts represent active symbols still missing that persisted data.
- `today.fundamentals` is explicitly an activity view. Its `basis` is `same_day_snapshot_updates`, so `symbols_succeeded` counts distinct stock codes with DART fundamentals snapshots updated during the current Seoul day and `symbols_failed` counts distinct financial-report stock codes whose capture attempt failed that same day.
- `today.fundamentals.current_run` is the latest persisted DART fundamentals refresh run for the current `market_date` when one exists. It is sourced from `dart_fundamentals_refresh_runs` and keeps refresh progress visible even when same-day snapshot updates are still zero. A running current-day refresh run overrides `today.fundamentals.status` to `running`, but a completed same-day no-op refresh still leaves the stage `missing` if no same-day snapshot updates exist.
- The `latest` block preserves `price_trade_date` plus the legacy `symbols_*` fields and now also mirrors its web-facing summary at top level through `latest.trade_date`, `latest.updated_at`, `latest.succeeded`, `latest.failed`, and `latest.total`.
- `latest.summary.basis` explicitly names the summary basis. `latest_eod_run_symbol_universe` means counts come from the selected `eod_collection_runs` row for `latest.trade_date`; `stored_price_bars_for_latest_trade_date` is the fallback when stored price bars exist but no matching EOD run row is available, in which case `total == succeeded` and `failed == 0`.
- `latest.updated_at` comes from the latest run's `run_metadata.last_progress_at` when available, otherwise from its run timestamps; the stored-price-bar fallback uses the max `daily_price_bars.captured_at` for `latest.trade_date`.
- `latest.eod_status` falls back to all stored price bars for `latest.price_trade_date`, including symbols that are no longer in the current active universe.
- `latest.fundamentals` is the latest-available fundamentals coverage view. It is sourced from the latest completed or partial `dart_fundamentals_refresh_runs` row across market dates, so weekends, pre-close periods, and same-day no-op refreshes do not collapse fundamentals availability back to zero.
- In `latest.fundamentals`, `symbols_total` is the selected refresh-run universe (`run_metadata.symbols_selected` when present, otherwise the stored row total), `symbols_checked` is the number processed by that run, `symbols_succeeded` is `symbols_fresh + symbols_refreshed`, and `symbols_refreshed` is preserved separately as the count of symbols whose fundamentals were newly updated in that run.
- The `today` block is scoped only to the current Seoul trade date; previous-day historical availability is surfaced through `latest.*`, not copied into `today.*`.

## EOD pipeline status notes
- Canonical semantics live in `docs/EOD_STATUS_SEMANTICS_CONTRACT.md`.
- `GET /eod/pipeline/status` is the operator-facing live status API for the post-close EOD pipeline.
- It stays separate from `GET /eod/freshness`: freshness answers whether the stored data is available, while pipeline status answers which stage is running now and what work is currently in progress.
- Its default `market_date` is also the current Seoul calendar date from `server_time[:10]`; weekends, holidays, and other break-days do not auto-resolve to the previous trading date unless the caller passes `market_date` explicitly.
- The pykrx stage breakdown reuses the existing `eod_collection_runs` row plus downstream rows linked by `eod_run_id`, and records lightweight per-stage progress in `eod_collection_runs.run_metadata`.
- The current pykrx pipeline stages are `price_collection`, `technical_snapshots`, `action_summaries`, and `trending_ranks`. The same payload also reports the latest DART `fundamentals_daily_refresh` run for the requested `market_date` and `fs_div`.
- While a pykrx run is `running`, the inferred current stage is authoritative for live display: that stage remains `running`, later pykrx stages stay `pending` until they actually start, and stale `stage_progress.status=running` entries must be downgraded once the owning run is no longer running.
- Stage payloads include normalized status, timestamps, processed or total or failed counts where available, current step or activity text, and linked source run IDs so operators can correlate the live run with existing inspect routes.

## EOD pipeline stage orchestration notes
- `GET /eod/pipeline/stages` is the operator-facing stage catalog for the explicit daily pipeline orchestrator.
- The catalog exposes the canonical stage order, the default daily run sequence, per-stage dependency metadata, supported inputs, inspect routes, and output-table hints.
- `POST /eod/pipeline/run` executes an explicit stage subset in canonical order while reusing the underlying stage operations rather than duplicating orchestration logic.
- Requested stages are reordered to the canonical sequence before execution. The response preserves both `requested_stages` and `resolved_stages` so operators can audit what was asked for versus what ran.
- The route does not silently inject missing dependencies. Callers can inspect `depends_on` from the catalog and choose whether they want a narrow rerun or a wider prerequisite-inclusive slice.

## EOD technical snapshot notes
- `daily_technical_snapshots` is the DB-backed post-EOD signal layer keyed by `symbol_id + market_date`.
- The current deterministic signal set is intentionally small: MA alignment and trend state, 5d and 20d returns, 20d volume ratio, 20d breakout or breakdown flags, and ATR-based volatility context.
- The pykrx daily EOD path computes and persists these snapshots after daily bar persistence completes, so downstream agents can consume stored data rather than recomputing chart state ad hoc.
- For manual reruns after an EOD capture, use `python3.11 -m app.eod.technical_snapshots --market-date YYYY-MM-DD` from `apps/api`.

## Quote fallback notes
- `GET /quotes/{symbol}` and `GET /quotes/tracked` now expose explicit quote provenance through `source`, `fallback`, `available`, `fallback_reason`, `unavailable_reason`, `cached_at`, and `trade_date`.
- Freshness policy is deliberate:
  - live KIS quote first
  - then current-market-date `daily_price_bars`
  - then same-day `quote_cache` or `current_quote_cache`
  - otherwise `available: false`
- Multi-day stale quote cache must not be surfaced as if it were a current quote.

## Realtime trending notes
- `GET /scanner/realtime-trending` is the user-facing realtime trending read API backed by KIS ranking sources only.
- Current source scope is `volume-rank`, `fluctuation`, and `hts-top-view`. The `sources` query parameter accepts a comma-separated subset of those source IDs.
- The response keeps each source bucket and also returns a merged `items` list keyed by symbol, with per-symbol `sources` metadata so clients can show which ranking buckets included the symbol.
- The route uses request-time KIS reads with a short in-memory TTL controlled by `KIS_REALTIME_TRENDING_CACHE_TTL_SECONDS`; it does not persist daily signal snapshots.

## Symbol price history notes
- `GET /symbols/{symbol}/price-bars` is the read API for the web symbol-detail candlestick chart.
- It reads directly from `daily_price_bars` and supports bounded range windows with `start_date` and `end_date`, plus a capped `limit` for recent-history or max-history retrieval.
- When `limit` is used, the API reads the newest rows first for index-friendly retrieval and returns them sorted ascending for chart consumers.

## Favorite symbol notes
- `GET /symbols/favorites` returns the persisted favorite symbol list in display order, including symbol metadata from `symbols`.
- `DELETE /symbols/favorites/{symbol}` removes one favorite symbol and returns `removed: false` when the symbol was not currently favorited, so web clients can treat repeat deletes as harmless.

## EOD action summary notes
- `daily_action_summaries` is the DB-backed rules and scoring layer keyed by `symbol_id + market_date`, reading only stored `daily_technical_snapshots`.
- The current summary contract is deterministic and machine-friendly: `action_label`, `bias_direction`, `signal_score`, `confidence_tier`, `risk_tier`, `reasons`, and trigger, invalidation, support, and resistance levels.
- The pykrx daily EOD path computes and persists these action summaries immediately after technical snapshots.
- For manual reruns after an EOD capture, use `python3.11 -m app.eod.action_summaries --market-date YYYY-MM-DD` from `apps/api`.

## Agent context read-layer notes
- `GET /agent-context/symbol-day` is the current machine-consumption read layer for symbol/day analysis. It does not rank names or emit final trading decisions.
- The route aggregates persisted symbol metadata, same-day technical snapshot, same-day deterministic action summary, same-day KIS ranking signal features, latest stored DART fundamentals summary, and recent disclosure events up to the requested `market_date`.
- Symbol-day context now also includes `price_context` plus up to seven recent persisted `price_context_history` entries read from `daily_price_contexts`, which the pykrx EOD pipeline derives from stored `daily_price_bars`: OHLC, intraday range, candle body and wick interpretation, close location inside the day's range, and gap-versus-previous-close fields when a prior stored close exists.
- Symbol-day context now uses exact-date technical snapshots for downstream agent dispatch and does not silently fall back to older technical snapshot rows.
- The response is intentionally block-oriented and compact so downstream agents can prioritize and interpret instead of re-querying multiple endpoints or parsing prose-heavy summaries.

## Agent feedback notes
- `POST /agent-context/feedback` is the JSON intake path for downstream agents such as OpenClaw to send post-EOD symbol summaries and structured feedback back to Core.
- `POST /agent-context/feedback/dispatch` is the orchestration trigger for role-based OpenClaw dispatch from a completed or partially completed EOD run.
- `POST /agent-context/feedback/dispatch/symbols` dispatches the same summarizer roles against symbol-day context for explicit symbol lists, but Core now dispatches only the subset whose persisted `daily_market_risk_filter_results.passed_filter` and `daily_piotroski_filter_results.passed_filter` rows are both `true` for the requested `market_date`. Requested symbols outside that active pre-filter intersection are skipped in the response. The route still accepts `reuse_existing` to skip reruns when the relevant source data has not changed.
- When `POST /agent-context/feedback/dispatch/symbols` runs with `wait_for_completion=false`, Core now creates the durable queued `agent_context_feedback_runs` row before returning and reuses that same `request_run_id` when the background dispatch starts, so operators can follow one stable run identity from queue to completion.
- Symbol dispatch now uses direct parallel fan-out over `/v1/responses`: Core chunks symbols, calls the same summarizer agent ids multiple times in parallel, and merges/stores one normalized result per symbol and role.
- Operational tuning for symbol dispatch is controlled by `OPENCLAW_AGENT_SYMBOL_CHUNK_SIZE` and `OPENCLAW_AGENT_PARALLEL_SUBTASKS`.
- `GET /agent-context/feedback/latest` returns the latest reusable feedback bundle by symbol and role, including whether Core would re-dispatch now or reuse the latest stored result.
- `GET /agent-context/feedback/payloads` reads stored agent feedback payloads by symbol and date.
- `GET /agent-context/feedback/requests` reads the stored request/context payloads that were sent to the agents by symbol and date.
- `GET /agent-context/feedback/status` reports whether the default four-stage feedback workflow (`eod-summarizer`, `technical-summarizer`, `fundamental-summarizer`, `signal-decider`) is pending, in progress, partial, or completed for requested symbols, plus completed counts and pending agents.
- `POST /agent-context/feedback/dispatch/symbols` now creates and checkpoints a durable `agent_context_feedback_runs` row for every normal symbol-dispatch run. The row records `running`, `partial`, `completed`, or `failed` lifecycle states plus durable per-run summary snapshots so same-day overlaps and interrupted runs stay auditable.
- Symbol dispatch persists buffered success/failure work at role boundaries and again during finalization. That preserves already-finished earlier roles for reuse on partial reruns without requiring immediate per-symbol writes after every single agent response.
- When batch-response recovery cannot parse `/v1/responses` output directly, Core now distinguishes transcript-gap failures from malformed JSON. Persisted failure rows and operator logs keep `session_key`, `session_id` or `response_id`, `request_marker`, and a specific recovery failure mode such as `session_history_missing_or_empty` or `session_history_assistant_non_json_after_marker` instead of flattening everything into one generic invalid-JSON bucket.
- `GET /agent-context/feedback/progress` reports aggregate progress for the expected four-stage feedback workflow, including `total_symbols`, `processed_symbols`, `completed_symbols`, `in_progress_symbols`, `pending_symbols`, `terminal_feedback_count`, `progress_ratio`, `progress_percent`, `boundary_progress_ratio`, `boundary_progress_percent`, `failed_items`, `completed_feedback_count`, `expected_feedback_count`, `first_feedback_at`, `elapsed_seconds`, `completed_feedback_per_minute`, `completed_symbols_per_minute`, `estimated_remaining_seconds`, `estimated_completion_at`, and `eta_confidence`. The top-level `progress_ratio` and `progress_percent` now reflect whole-cycle feedback completion across roles, while `boundary_progress_ratio` and `boundary_progress_percent` expose symbol-boundary coverage separately. It also returns top-level `result.market_date` plus `result.roles[role]` with persisted `requested_count`, `completed_count`, and `error_count` scoped to the requested market date and optional symbol filter. `run_id` is now an optional exact-run selector. When a matching durable run row exists, retry-cycle aggregate counts stay grounded in persisted feedback and failure rows while the summary still surfaces live lifecycle metadata such as `current_role`, `completed_roles`, and `cycle_status`. For dispatch and retry runs, the scoped symbol universe comes from the durable run payload, which now reflects the active pre-filter intersection rather than Piotroski alone. When `symbols` is omitted and no durable run scope is available, the symbol universe is derived from distinct `daily_price_bars` symbols for the requested market date.
- `POST /agent-context/feedback/retry-cycles` runs bounded retry passes over incomplete symbol feedback for the persisted active-pre-filter intersection on that `market_date`. Callers can now optionally provide an explicit `symbols` subset to bound the daily production flow to one symbol or a small hand-picked list without changing the normal summarizer -> decider sequence, persistence path, or progress/latest inspection surfaces. The response still returns cycle/batch results plus a fresh `progress` snapshot with the same timing, rate, and ETA fields as `GET /agent-context/feedback/progress`.
- `POST /agent-context/feedback/session-cleanup/run` retries only previously failed OpenClaw session deletions after feedback rows are already stored.
- `GET /agent-context/feedback/session-cleanup` exposes only the retained failed-deletion records so operators can confirm what still needs retry.
- The default dispatch phase now runs `eod-summarizer`, `technical-summarizer`, `fundamental-summarizer`, and then `signal-decider` as a final synthesis step.
- Latest reusable feedback lookup is bounded to feedback rows with `market_date <= requested market_date`, so prior-day reusable feedback can carry forward when the source inputs are unchanged.
- Fundamental freshness checks are bounded to DART filings and captured fundamentals eligible up to the requested `market_date`, so historical reads do not get marked stale by future filings.
- Recommended daily operations pattern is retry cycles over the active-pre-filter intersection: one intersection-scoped pass, then one or more bounded follow-up cycles over only the remaining incomplete eligible symbols, so transient gateway or agent failures do not require rerunning the full date universe.
- Repo helper entrypoint for that pattern: from `apps/api`, run `python3.11 -m scripts.run_feedback_retry_cycles`.
- One-symbol daily smoke variant of the same path: from `apps/api`, run `python3.11 -m scripts.run_feedback_retry_cycles --market-date YYYY-MM-DD --symbols 005930` or `python3.11 -m app.internal.ops agent-feedback-retry-cycles --market-date YYYY-MM-DD --symbols 005930`.
- Repo helper entrypoint for session deletion retries: from `apps/api`, run `python3.11 scripts/cleanup_stale_feedback_sessions.py --limit 100`. Successful immediate deletions are not persisted; only failed deletions remain queued.
- End-to-end one-command smoke path: from `apps/api`, run `python3.11 -m app.internal.ops agent-feedback-symbol-cleanup-smoke --market-date YYYY-MM-DD --symbols 005930,000660,...`.
- When `DART_API_KEY` is configured, the default daily EOD steady-state loop includes DART daily incremental fundamentals refresh immediately after successful daily pykrx capture. That refresh rebuilds persisted Piotroski filter rows for the same `market_date`, and when `OPENCLAW_FEEDBACK_TRIGGER_AFTER_FUNDAMENTALS_REFRESH=true` it can then automatically trigger the feedback retry-cycle workflow only after the Piotroski follow-up completes without persistence failures. The resulting feedback dispatch and retry scope is the intersection of the persisted active pre-filters for that same `market_date`.
- Each role now receives a different JSON payload built for its job instead of one shared generic blob: the EOD summary payload includes market breadth, the latest persisted KIS ranking participation context plus up to seven recent `participation_context_history` entries from `daily_market_participation_contexts`; the technical summary payload carries technical/state windows; and the fundamental summary payload carries disclosure plus fundamentals windows.
- The `signal-decider` request payload preserves the full normalized outputs from the three specialist summarizers under one symbol/day request, along with orchestration and traceability metadata, so the decider can synthesize evidence, disagreement, and data-quality warnings without flattening upstream structure away.
- Signal-decider batch recovery now also treats local OpenClaw trajectory artifacts under `OPENCLAW_STATE_DIR` as a secondary recovery source. If the session transcript `.jsonl` is missing or empty but the matching `*.trajectory.jsonl` still contains valid assistant JSON for that `response_id`, Core recovers and persists the normalized response instead of final-failing the symbol.
- Dispatch requires `OPENCLAW_BASE_URL` and `OPENCLAW_GATEWAY_TOKEN` configuration, retries retryable failures, and persists successful responses into `agent_context_feedback` as either market-level rows with symbol `__MARKET__` or symbol-level rows for symbol dispatch.
- Stored normalized feedback now includes a shared compatibility contract: `role`, `summary`, `confidence`, `risk_flags`, `details`, and `improvement_notes`. The `signal-decider` rows add a richer next-trading-day decision-support object with `next_day_open_action`, price-basis or target or stop fields, confidence or conviction scores, thesis or evidence or risk fields, disagreement notes, and traceability.
- Every persisted `signal-decider` response now also writes an append-only row into `signal_decider_decision_snapshots`. Each snapshot freezes `snapshot_id`, `decision_run_id`, policy version, structured traceability, the frozen request payload, the frozen normalized response payload, and the compact input-feature snapshot so later readback does not depend on the mutable latest `agent_context_feedback` row.
- The stored request/context payload for `signal-decider` now captures what the decider saw, including symbol-day context, full upstream summarizer outputs, request model metadata, orchestration metadata, the current policy version, and a compact feature snapshot for later evaluation. The snapshot table stores that full request payload separately from the latest feedback row so reruns do not overwrite prior evidence.
- `GET /agent-context/signal-decider/decisions` is the compact persisted decision search read over `signal_decider_decision_snapshots`. It now supports one-day or bounded-range reads plus stable `action`, `policy_version`, `verdict`, `has_missed_opportunity`, and horizon or evaluation-version filters, and every row now carries a dashboard-friendly `explanation` block plus `evaluation_summary`.
- `GET /agent-context/signal-decider/decisions/{snapshot_id}` is the immutable decision-detail read. It returns the same stable explanation fields, the frozen request/response payloads, upstream summary references, realized outcomes, and versioned evaluations in one place so operators can inspect why one decision was made without reading raw prompt text manually.
- `GET /agent-context/signal-decider/diagnostics` is the deeper traceability read for one date or symbol slice. It adds the frozen request payload, frozen normalized response payload, upstream summarizer bundle, realized outcomes, and versioned evaluation rows for later audit and missed-opportunity analysis.
- `GET /agent-context/signal-decider/diagnostics/summary` is the aggregate diagnostics read for dashboard and policy-review workflows. Over a bounded date window it reports verdict counts, buy precision, return and stop-hit metrics by horizon or policy version, plus factor and gate contributor buckets derived from immutable snapshot traceability and stored evaluation rows.
- `POST /agent-context/signal-decider/evaluations/compute` computes or refreshes raw realized outcomes and versioned grading rows from stored `daily_price_bars`. The current raw outcome horizons are 1, 3, 5, and 10 trading days measured from the next available open after the decision date.
- Raw realized outcomes now live in `signal_decider_realized_outcomes`, separate from the decision snapshot and separate from grading. Each row stores the observed entry or exit dates, close return, max upside, max drawdown, target or stop touches, and the compact price path used for evaluation, and each row now keys to immutable `decision_snapshot_id` rather than the mutable feedback row id.
- Raw outcome readback now also surfaces `best_exit_trade_date`, `worst_drawdown_trade_date`, `first_target_hit_trade_date`, and `first_stop_loss_hit_trade_date` so later verdict tuning can distinguish early drawdowns from later recoveries without recomputing price facts.
- Versioned grading now lives in `signal_decider_evaluations`, keyed by immutable `decision_snapshot_id`, horizon, and `evaluation_version`. The current deterministic labels are `good_buy`, `bad_buy`, `missed_opportunity_after_wait`, `correct_wait`, `missed_opportunity_after_reject`, `correct_reject`, plus `pending_horizon` when the full horizon is not available yet.
- The default `signal_decider_eval_v1` rule set now explicitly treats severe pre-target drawdowns as `bad_buy` even when the symbol later recovers, and the stored evaluation rationale/diagnostics persist the rule-set version plus the triggered drawdown guard.
- `GET /agent-context/signal-decider/evaluations` reads the persisted outcome-plus-grade bundle back per stored decision for the requested `market_date`, optional symbol, and optional horizon subset.
- The daily EOD stage catalog now includes `signal_decider_evaluations`, which refreshes the recent realized-outcome/evaluation window after `signal_decider` dispatch so the same pipeline can keep older decisions moving from `pending_horizon` to graded outcomes automatically.
- `POST /agent-context/signal-decider/policy-candidates/generate` and the alias `POST /agent-context/signal-decider/decision-policies/candidates/generate` automatically refresh the requested evaluation horizon slice, scan the accumulated immutable snapshot evidence, store manual-only candidate rows in `signal_decider_policy_candidates`, and upsert durable candidate policy-version rows in `signal_decider_policy_versions`.
- `signal_decider_policy_versions` is the durable registry for signal-decider decision-policy history. Each row stores `policy_version`, `status`, `parent_policy_version`, `change_summary`, `rule_bundle`, freeform metadata, and created or activated audit fields so candidate rulesets stay queryable even before activation.
- `GET /agent-context/signal-decider/policy-candidates` and the alias `GET /agent-context/signal-decider/decision-policies/candidates` read the generated candidates and now surface the linked policy-version registry status for each candidate version.
- `GET /agent-context/signal-decider/decision-policies` lists the durable policy-version registry, and `GET /agent-context/signal-decider/decision-policies/{policy_version}` returns one version's rule bundle, candidate linkage, and activation metadata.
- `POST /agent-context/signal-decider/decision-policies/{policy_version}/activate` is the explicit human approval step. Candidate generation remains automatic, but activation is intentionally manual-only: this repo does not auto-promote or auto-activate candidate rulesets.
- Signal-decider dispatch payloads now carry the active registry-backed policy context, including `current_version`, `change_summary`, and `rule_bundle`, so later decider runs can see which manually approved policy is active.
- Feedback is append-only and stored separately from deterministic `daily_action_summaries`; it does not create orders, positions, or live-trading behavior.
- `GET /agent-context/feedback` reads stored feedback for a requested `market_date`, optionally filtered by symbol and `agent_name`, so operators or later jobs can inspect the summaries Core received.
- `GET /agent-context/feedback/technical-summaries` is the bounded persisted read for the `technical-summarizer` role. `market_date` is required, `symbol` is optional, and `limit` bounds the returned stored rows for that date.
- The technical-summaries response reuses the shared feedback read contract from `agent_context_feedback`, scoped to `technical-summarizer`, so callers can consume stored role-level output plus technical detail slices such as `price_action`, `trend_context`, `support_resistance`, `anomalies_and_gaps`, `data_gaps`, and `improvement_notes`.
- `GET /agent-context/feedback/eod-summaries` is the consumer-facing persisted read for the `eod-summarizer` role. `market_date` is required, `symbol` is optional, and the response returns only the stored eod-summarizer result rows for that date.
- The dedicated eod-summaries payload exposes the persisted consumer-useful fields from `agent_context_feedback`: role-level summary/confidence/risk flags plus the stored detail slices such as `breadth_context`, `leaders_laggards`, `price_action`, `anomalies_and_gaps`, `data_gaps`, `conservative_take`, and `improvement_notes`.

## Daily signal feature notes
- `daily_signal_features` is the DB-backed KIS ranking feature layer keyed by `symbol_id + market_date`.
- The current scope intentionally covers only `volume-power`, `overtime-volume`, and `overtime-fluctuation`.
- `POST /eod/signal-features/collect` calls those KIS ranking sources, normalizes symbol-level ranks and metrics, and upserts one compact feature row per symbol/date.
- The same collect path now also rebuilds `daily_market_participation_contexts` for that market date from the stored `daily_signal_features` rows, so market-summary payloads no longer derive participation context on demand.
- Repo-owned bootstrap/backfill for the new derived-context tables is the internal ops command `python3.11 -m app.internal.ops agent-derived-contexts-backfill-recent`, which fills the recent seven trading days ending yesterday from existing `daily_price_bars` and stored `daily_signal_features` without changing the steady-state daily EOD scope.
- For shell-driven operations, prefer `python3.11 -m app.internal.ops kis-daily-signal-features-collect --market-date YYYY-MM-DD` from `apps/api`.

## Market risk snapshot and filter notes
- `daily_market_risk_snapshots` is the persisted KIS `inquire-price-2` status snapshot layer keyed by `symbol_id + market_date`.
- `daily_market_risk_filter_results` is the downstream policy-evaluation layer for the same symbol/date scope. It stores `passed_filter`, `watchlist_flagged`, `risk_state`, and explicit reason arrays without forcing snapshot callers through the filter schema.
- `POST /eod/market-risk-snapshots/collect` uses the stored `daily_price_bars` universe for the requested date unless `symbols` is provided explicitly, fetches KIS `inquire-price-2` once per symbol, normalizes the raw status flags, and persists one reusable snapshot row per symbol/date.
- `GET /eod/market-risk-snapshots` is the thin snapshot-first read API for that table. `market_date` is optional: when omitted, or when the requested date has no stored rows for the current scope, the read path resolves the latest stored snapshot date on or before the requested date and reports that resolution explicitly.
- `POST /eod/market-risk-filters/compute` now layers on top of snapshot collection first and focused evaluator outputs second. The response keeps the familiar filter summary plus a bounded `snapshot_collection` block so callers can see the collection stage separately from the evaluator stage.
- `GET /eod/market-risk-filters` is the thin persisted read API for the evaluator outputs. It returns policy state plus explicit blocking/watchlist reasons, not the full normalized KIS snapshot field set.
- The automatic pykrx daily EOD pipeline now also collects and stores `daily_market_risk_snapshots`, then computes and stores `daily_market_risk_filter_results` for the symbols whose daily bars were persisted successfully. Per-symbol KIS failures are recorded as partial errors and do not abort the overall daily pipeline.
- For shell-driven operations, use `python3.11 -m app.internal.ops eod-market-risk-snapshots-collect --market-date YYYY-MM-DD` for snapshot collection and `python3.11 -m app.internal.ops eod-market-risk-filters-compute --market-date YYYY-MM-DD` for the downstream filter run.

## EOD screening result notes
- `GET /eod/screening-results` is the compact dashboard-facing screening read across persisted factor and filter result families.
- The existing family-specific inspect routes are still the canonical detail reads, but they hydrate full rows and return `factor_payload` or `filter_payload` JSON blocks. For interactive screening lists, the avoidable bottleneck was overfetch plus Python-side row shaping rather than missing canonical storage.
- The screening route keeps the existing result tables, resolves the latest available `market_date` when omitted, computes summary counts in SQL, projects only list-needed columns, and pages collection reads with a default `limit=100`.
- Supported `family` values are `market_risk_filter`, `piotroski_filter`, `momentum`, `value`, `low_risk`, `beta`, `advanced_value`, and `composite`.
- Use the family-specific routes such as `GET /eod/composite-factors` or `GET /eod/market-risk-filters` when the client needs the full stored payload block or single-symbol inspection semantics.

## Daily momentum factor notes
- `daily_momentum_factor_results` is the DB-backed price-factor layer keyed by `symbol_id + market_date`.
- `POST /eod/momentum-factors/compute` reads only stored `daily_price_bars`; it does not fetch a new upstream price series during the request.
- V1 uses exact stored trading-bar offsets for 12-1 momentum: `close[t-21] / close[t-252] - 1`. Symbols with fewer than 253 stored bars still get persisted rows with moving-average and overheat fields where available, but they do not receive a momentum rank or percentile.
- The route stores `ma_60`, `ma_120`, `is_price_above_ma120`, `is_ma60_above_ma120`, `return_5d`, `return_5d_rank`, `return_5d_percentile`, `return_5d_realized_volatility_20d`, `return_5d_vol_adjusted`, `is_return_5d_extreme`, `is_volatility_adjusted_overheated`, `is_recent_overheated`, and `overheat_rule_version`. The same values remain available inside `factor_payload` with rule metadata and audit references.
- Cross-sectional `momentum_rank` and `momentum_percentile` are computed only across symbols in the selected compute universe that have non-null 12-1 momentum. Higher momentum receives a lower rank number and a higher percentile.
- The default 5-day overheat exclusion is `momentum_overheat_v2`: a symbol is marked overheated only when its 5-day return is cross-sectionally extreme, its volatility-adjusted 5-day acceleration exceeds the configured shock threshold, and the raw 5-day return clears a minimum absolute move. The persisted payload records the percentile threshold, the volatility-adjusted formula, the minimum-universe guardrail, and the threshold values so later tuning remains auditable.
- The current scope is intentionally manual or operator-triggered rather than part of the steady-state pykrx pipeline stages. For shell-driven operations, use `python3.11 -m app.internal.ops eod-momentum-factors-compute --market-date YYYY-MM-DD` from `apps/api`.

## Daily value factor notes
- `daily_value_factor_results` is the DB-backed value-factor layer keyed by `symbol_id + market_date`.
- `POST /eod/value-factors/compute` rebuilds persisted rows from stored `daily_price_bars`; it does not call live quote, broker, or execution paths during the request.
- `GET /eod/value-factors` is the persisted read API for that table. When `symbol` is provided, the route returns a single `item`; without `symbol`, it returns ordered `items` for the requested date, bounded by optional `limit`.
- V1 computes cross-sectional percentiles for `per`, `pbr`, and `dividend_yield`, then stores `value_score` as the equal-weight mean of those three percentiles. Lower `per` and `pbr` rank better; higher `dividend_yield` ranks better.
- Eligibility is explicit per metric: `per` and `pbr` must be strictly positive, `dividend_yield` must be non-negative, and symbols missing the requested market-date bar or any required metric remain persisted with a non-scored `value_status` rather than being dropped.
- Symbol-subset reruns still rank against the full stored market-date universe before persisting the requested subset, so `value_rank` and percentile fields remain comparable across targeted reruns.
- If the `symbols` query resolves to an explicit empty subset after trimming, the compute route returns a no-op payload with zero persisted rows instead of recomputing the full universe.
- For shell-driven operations, use `python3.11 -m app.internal.ops eod-value-factors-compute --market-date YYYY-MM-DD` from `apps/api`.

## Piotroski filter read-layer notes
- `GET /eod/piotroski-filter-results` is the persisted read API for `daily_piotroski_filter_results`.
- `market_date` is optional. When omitted, the route resolves the latest stored Piotroski result date for the current request scope. When `symbol` is provided, latest-date resolution is scoped to that symbol; without `symbol`, it is scoped to the full stored result set.
- When a specific `market_date` is requested, the read path preserves the exact date when rows exist for that scope. If no rows exist for the requested scope and date, it falls back to the latest stored date on or before the requested date and reports that resolution explicitly.
- The read path intentionally returns both passed and not-passed rows because the DART daily fundamentals refresh stores one persisted result per evaluated symbol, not just the winners.
- The persisted Piotroski table is one half of agent-feedback eligibility. Symbol-level agent feedback on a `market_date` now requires both `daily_piotroski_filter_results.passed_filter=true` and `daily_market_risk_filter_results.passed_filter=true`.
- Results are ordered with passed names first, then lower PBR percentile ranks first, so callers can consume the returned list directly without applying a second client-side sort.
- The response keeps `market_date` as the resolved result-set date for compatibility and now also includes `requested_market_date`, `resolved_market_date`, `latest_mode`, `is_latest_fallback`, and `fallback_policy='latest_on_or_before_requested_market_date'` so callers can distinguish exact reads, latest-mode reads, and requested-date fallback behavior.
- The `summary` block reports the number of returned entries, passed vs not-passed counts, and any requested symbol that did not have a stored row for the resolved date.

## Low-risk factor notes
- `daily_low_risk_factor_results` is the persisted low-volatility and downside-risk layer keyed by `symbol_id + market_date`.
- `POST /eod/low-risk-factors/compute` rebuilds the persisted rows from stored `daily_price_bars` only; it does not pull live quotes or execution data.
- `GET /eod/low-risk-factors` is the persisted read API for that table. When `symbol` is provided, the route returns a single `item`; without `symbol`, it returns ordered `items` for the requested date.
- v1 realized volatility uses simple close-to-close daily returns from stored `close_price`, with 60-return and 120-return trailing windows annualized by `sqrt(252)`.
- v1 downside risk uses trailing 120 stored closes for `max_drawdown_120d`, expressed as the worst `close / running_peak - 1.0` value inside that window.
- v1 liquidity support uses `avg_trading_value_20d` from the trailing 20 stored trade-date bars, then applies a cross-sectional liquidity-percentile floor so thinly traded names are not classified as safe by low volatility alone.
- Cross-sectional percentiles are stored with `best_symbol=1.0` and `worst_symbol=0.0`; v1 uses deterministic symbol ordering to break ties.
- The combined v1 low-risk score is explicit and persisted: `0.30 * volatility_60d_percentile + 0.30 * volatility_120d_percentile + 0.25 * max_drawdown_120d_percentile + 0.15 * liquidity_percentile`.
- `is_low_risk_eligible` requires the liquidity floor to pass and the combined score to meet the stored eligible threshold. Rows with incomplete history remain persisted with partial raw metrics, `low_risk_score=null`, and `low_risk_classification=insufficient_history`.

## Beta factor notes
- `daily_beta_factor_results` is the persisted benchmark-relative beta layer keyed by `symbol_id + market_date`.
- `POST /eod/beta-factors/compute` rebuilds persisted beta rows from stored `daily_price_bars` only; it does not fetch a live benchmark or quote series during the request.
- `GET /eod/beta-factors` is the persisted read API for that table. When `symbol` is provided, the route returns a single `item`; without `symbol`, it returns ordered `items` for the requested date.
- v1 benchmark policy is explicit and deterministic: KOSPI equities use `KOSPI_EQW`, KOSDAQ equities use `KOSDAQ_EQW`, and all other markets fall back to `KRX_EQW`.
- Those benchmark IDs are stored equal-weight market benchmarks built from the same-market stored `daily_price_bars` universe on each trade date, using simple close-to-close returns.
- v1 beta estimation uses the slope form `cov(symbol_returns, benchmark_returns) / var(benchmark_returns)` over trailing 60-return and 120-return windows, exposed as `beta_60d` and `beta_120d`.
- Lower beta ranks better. Cross-sectional percentiles are stored with `best_symbol=1.0` and `worst_symbol=0.0`, and the combined `low_beta_score` is `0.50 * beta_60d_percentile + 0.50 * beta_120d_percentile`.
- Rows with incomplete overlap history remain persisted with any available raw beta field, `low_beta_score=null`, and `low_beta_classification=insufficient_history`.
- For shell-driven operations, use `python3.11 -m app.internal.ops eod-beta-factors-compute --market-date YYYY-MM-DD` from `apps/api`.

## Advanced value factor notes
- `daily_advanced_value_factor_results` is the DB-backed advanced-value layer keyed by `symbol_id + market_date`, and it intentionally sits alongside `daily_value_factor_results` rather than replacing value v1.
- `POST /eod/advanced-value-factors/compute` rebuilds persisted rows from stored `daily_price_bars` plus stored DART fundamentals snapshots only; it does not call live quote, broker, or execution paths during the request.
- `GET /eod/advanced-value-factors` is the persisted read API for that table. When `symbol` is provided, the route returns a single `item`; without `symbol`, it returns ordered `items` for the requested date.
- `advanced_value_v1` uses the latest stored annual DART `CFS` snapshot whose receipt date is on or before the requested `market_date` (`report_code=11011`, `period_type=annual`).
- `fcf_yield` is defined as `free_cash_flow / market_cap_proxy`, where `free_cash_flow = operating_cash_flow - capex` and the first-pass capex proxy is the sum of matched PPE and intangible-asset acquisition cash outflows.
- `ev_to_ebitda` is defined as `enterprise_value / ebitda`, where `enterprise_value = market_cap_proxy + total_liabilities - cash_and_cash_equivalents` and `ebitda = operating_income + depreciation_and_amortization`.
- `market_cap_proxy` is explicit because stored KRX daily bars do not persist shares outstanding: `market_cap_proxy = pbr * total_equity` using same-day stored `daily_price_bars.pbr` and the selected annual DART `total_equity`.
- Higher `fcf_yield` ranks better and lower `ev_to_ebitda` ranks better. Cross-sectional percentiles are stored with `best_symbol=1.0` and `worst_symbol=0.0`.
- The combined `advanced_value_score` is the equal-weight mean of the two component percentiles and requires both component metrics to be valid for scoring. Rows with only one valid metric remain persisted with `advanced_value_score=null` and `advanced_value_classification=insufficient_components`.
- Negative free cash flow remains a valid but unattractive `fcf_yield` input. Non-positive EBITDA and non-positive enterprise value are treated as explicit invalid EV/EBITDA cases rather than ranked values.
- Auditability is explicit in `factor_payload`: the stored row keeps the selected annual snapshot metadata, matched DART input rows/highlights, component status flags, methodology assumptions, and the raw numerator/denominator pieces used to derive the final metrics.
- For shell-driven operations, use `python3.11 -m app.internal.ops eod-advanced-value-factors-compute --market-date YYYY-MM-DD` from `apps/api`.

## Composite factor notes
- `daily_composite_factor_results` is the persisted integrated ranking layer keyed by `symbol_id + market_date`, built on top of the stored momentum, value v1, advanced value, low-risk, and beta slices.
- `POST /eod/composite-factors/compute` reads only stored factor-result tables for the requested `market_date`; it does not backfill price bars, fundamentals, or factor slices during the request.
- `GET /eod/composite-factors` is the persisted read API for that table. When `symbol` is provided, the route returns a single `item`; without `symbol`, it returns ordered `items` for the requested date.
- The normalized score inputs are explicit and all point in the same direction: `momentum_percentile`, `value_score`, `advanced_value_score`, `low_risk_score`, and `low_beta_score` all mean higher is better.
- The first-pass base weights are explicit and versioned: momentum `0.25`, value v1 `0.15`, advanced value `0.20`, low-risk `0.20`, and beta `0.20`.
- Missing-input behavior is explicit: the composite layer renormalizes weights across available inputs only when the row has at least three valid factors, at least one valuation-family input (`value_v1` or `advanced_value`), and at least `0.60` of total base-weight coverage. Otherwise it persists `composite_score=null` with `composite_classification=insufficient_inputs`.
- Hard exclusions are explicit and auditable: `momentum_recent_overheat` excludes symbols whose stored momentum row is flagged `is_recent_overheated=true`, and `low_risk_liquidity_floor` excludes symbols whose stored low-risk row fails `passes_liquidity_floor`.
- Hard exclusions do not erase the integrated score. Rows can still store `composite_score`, `composite_rank`, and `composite_percentile` while `is_composite_eligible=false` and `composite_classification=hard_excluded` so operators can inspect both score quality and exclusion cause.
- `factor_payload` keeps the input basis, base and applied weights, per-factor contributions, gate metadata, and the summary rank fields so downstream consumers can explain why a symbol landed where it did.
- For shell-driven operations, use `python3.11 -m app.internal.ops eod-composite-factors-compute --market-date YYYY-MM-DD` from `apps/api`.

## Trending stocks read-layer notes
- `GET /scanner/trending-stocks` is the compact web-facing read API for stored trending-stock candidates.
- For day-based periods, it reads `daily_trending_ranks`; it does not recompute 1d, 5d, or 20d candidates during the request and does not call live order or execution APIs.
- The pykrx daily EOD path rebuilds stored `1d`, `5d`, and `20d` trending ranks after daily bars, technical snapshots, and action summaries are persisted. Manual rebuilds can use `python3.11 -m app.internal.ops eod-trending-ranks-compute --market-date YYYY-MM-DD` from `apps/api`.
- `period=1d` ranks by stored daily close versus the previous stored close, `period=5d` and `period=20d` rank by stored technical-snapshot returns, and `period=action_score` remains a direct read over stored action-summary signal score.
- `direction=up`, `direction=down`, and `direction=both` let clients request gainers, losers, or the largest absolute movers for a stored market date. When `market_date` is omitted, the route uses the latest stored date available for the requested period.

## DART pipeline notes
- Practical lower bound for `fnlttSinglAcntAll` and `list.json` in this pipeline is 2015 onward. Open DART documents both the filing-search date range and the financial-statement `bsns_year` support as 2015 and later, so the DB `daily_price_bars` minimum of `2007-06-05` does not expand the usable fundamentals backfill window.
- Ongoing machine-DB sync now persists only periodic financial-report events. Non-financial disclosures are still fetched from Open DART during polling, but the financial-report path is the one the capture and history workflow depends on.
- The current-universe backfill operator is intentionally batch-oriented. Use the current active symbol universe only, slice with `symbol_limit` and `symbol_offset`, sync only the relevant financial report events for that slice, then capture only those symbols' pending or failed fundamentals.
- Fundamentals capture treats `pending` and `failed` disclosure events as retryable. DART financial-statement `[013]` responses are persisted as terminal `no_data`, and filing/request inference problems are persisted as terminal `mapping_failed`, so normal daily retries do not re-hammer those events.
- For operator-driven continuation batches, prefer `python3.11 -m app.dart.backfill_current_universe ...` or a quoted `curl` URL. Unquoted shell query strings can turn `?` and `&` into zsh parsing issues before the request reaches the API.
- For unattended full-universe continuation, use `python3.11 -m app.dart.backfill_current_universe_runner ...`; it is the repo-owned long-run path with per-batch log-file progress and failure offsets.
- Daily incremental fundamentals refresh is the EOD-style steady-state operator path. It reads each selected active symbol's latest resolved DART fundamentals receipt date, syncs only that symbol's newer DART financial-report disclosure window, captures only stale or missing symbols' exact pending/failed event IDs, records a high-level `dart_fundamentals_refresh_runs` row for inspection, and then rebuilds persisted `daily_piotroski_filter_results` for that market date so both passed and not-passed symbols are stored.
- `GET /disclosures/dart/fundamentals/daily/inspect` now exposes the run-scoped cycle-day updated-company activity directly through `updated_entries_summary`, `updated_entries_preview`, and `updated_entries`. That surface is intentionally separate from latest-coverage semantics: `symbols_refreshed` and `updated_entries_summary.updated_count` answer “which symbols were updated during this refresh cycle,” while latest available fundamentals coverage remains a separate read concern.
- `updated_entries` returns one primary entry per updated symbol for the selected refresh run. The current deterministic category contract is: `initial_fundamentals_capture` when the symbol had no stored fundamentals before the run, and `financial_statement_refresh` when the symbol already had stored fundamentals and the cycle captured a newer filing.
- `updated_entries_preview` is the first three items from the full updated list, and `updated_entries_summary` includes `visible_preview_limit`, `visible_preview_count`, `has_more`, and `remaining_count` so the dashboard can render `first 3 + More` without inferring preview state from unrelated counters.
- Daily refresh and backfill responses include `operator_buckets` for lookup/mapping errors, retryable capture failures, terminal no-data events, terminal mapping failures, and remaining retryable events. `GET /disclosures/dart/fundamentals/capture/inspect?capture_status=no_data` can be used to inspect terminal no-data events directly.

## Next routes
- DART corp-code refresh and inspect operator if lookup freshness becomes an operational issue
- Curated DART ownership or dividend read slices built on top of the current disclosure backbone
- KIS balance inquiry
- KIS account positions

- Automatic daily EOD capture now chains into DART fundamentals daily refresh by default in environments where `DART_API_KEY` is configured, so price -> technical -> action -> fundamentals -> piotroski filter -> agent feedback can run as one daily cycle. The agent-feedback follow-up remains gated by `OPENCLAW_FEEDBACK_TRIGGER_AFTER_FUNDAMENTALS_REFRESH=true`.
