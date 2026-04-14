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

## Current routes
- `POST /broker/kis/token`
- `GET /broker/kis/health`
- `GET /broker/kis/quote?symbol=005930&market_code=J`
- `GET /market/status`
- `GET /quotes/tracked`
- `GET /quotes/005930`
- `GET /scanner/tracked-symbols`
- `POST /scanner/tracked-symbols`
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
- `GET /agent-context/symbol-day?market_date=YYYY-MM-DD&symbols=005930&symbols=000660&fs_div=CFS&disclosures_limit=5`

## EOD freshness read-layer notes
- `GET /eod/freshness` is the compact web-facing read API for EOD ingestion freshness.
- It reports the current Seoul trade date, today's EOD receipt status, stored run counts for that date when available, and the latest stored dates for price bars, technical snapshots, and action summaries.
- The `latest` block also reports EOD run status and symbol counts for `latest.price_trade_date`, so clients can show the latest stored run result before today's run exists.
- The `today` block is scoped only to the current Seoul trade date; previous-day historical availability is surfaced through `latest.*`, not copied into `today.*`.

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
- The route aggregates persisted symbol metadata, same-day technical snapshot, same-day deterministic action summary, latest stored DART fundamentals summary, and recent disclosure events up to the requested `market_date`.
- The response is intentionally block-oriented and compact so downstream agents can prioritize and interpret instead of re-querying multiple endpoints or parsing prose-heavy summaries.

## DART pipeline notes
- Practical lower bound for `fnlttSinglAcntAll` and `list.json` in this pipeline is 2015 onward. Open DART documents both the filing-search date range and the financial-statement `bsns_year` support as 2015 and later, so the DB `daily_price_bars` minimum of `2007-06-05` does not expand the usable fundamentals backfill window.
- Ongoing machine-DB sync now persists only periodic financial-report events. Non-financial disclosures are still fetched from Open DART during polling, but the financial-report path is the one the capture and history workflow depends on.
- The current-universe backfill operator is intentionally batch-oriented. Use the current active symbol universe only, slice with `symbol_limit` and `symbol_offset`, sync only the relevant financial report events for that slice, then capture only those symbols' pending or failed fundamentals.
- Fundamentals capture treats `pending` and `failed` disclosure events as retryable. DART financial-statement `[013]` responses are persisted as terminal `no_data`, and filing/request inference problems are persisted as terminal `mapping_failed`, so normal daily retries do not re-hammer those events.
- For operator-driven continuation batches, prefer `python3.11 -m app.dart.backfill_current_universe ...` or a quoted `curl` URL. Unquoted shell query strings can turn `?` and `&` into zsh parsing issues before the request reaches the API.
- For unattended full-universe continuation, use `python3.11 -m app.dart.backfill_current_universe_runner ...`; it is the repo-owned long-run path with per-batch log-file progress and failure offsets.
- Daily incremental fundamentals refresh is the EOD-style steady-state operator path. It reads each selected active symbol's latest resolved DART fundamentals receipt date, syncs only that symbol's newer DART financial-report disclosure window, captures only stale or missing symbols' exact pending/failed event IDs, and records a high-level `dart_fundamentals_refresh_runs` row for inspection. Candidate capture events are ordered by fewest attempts and oldest receipt date first so limited runs drain retryable backlog predictably.
- Daily refresh and backfill responses include `operator_buckets` for lookup/mapping errors, retryable capture failures, terminal no-data events, terminal mapping failures, and remaining retryable events. `GET /disclosures/dart/fundamentals/capture/inspect?capture_status=no_data` can be used to inspect terminal no-data events directly.

## Next routes
- DART corp-code refresh and inspect operator if lookup freshness becomes an operational issue
- Curated DART ownership or dividend read slices built on top of the current disclosure backbone
- KIS balance inquiry
- KIS account positions
