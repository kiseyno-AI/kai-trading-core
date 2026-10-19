# RUNBOOK

## Goal
Run `kai-trading-core` as a 24/7 local service.

## API service
Working directory:
- `apps/api`

Start command:
- `./start-api.sh`

What it does:
- loads `apps/api/.env` before interpreting launcher controls, while preserving explicit process-environment overrides
- chooses `PYTHON_BIN` when explicitly set, otherwise the active virtual environment, `apps/api/.venv/bin/python`, or `python3.11` on `PATH`, in that order
- installs Python requirements when `INSTALL_REQUIREMENTS_ON_START=true`, or in the default `auto` mode only when required imports are missing
- when `DB_AUTO_UPGRADE=true`, runs deploy preflight before uvicorn, applies `alembic upgrade head` itself when the database is safely behind head, then verifies `--require-head` before starting the server
- starts FastAPI via uvicorn on `127.0.0.1:8000` by default
- if `UPSTREAM_BEARER_TOKEN` is set, all incoming API requests require `Authorization: Bearer <token>`
- local orchestration should prefer `python3.11 -m app.internal.ops ...` over raw localhost `curl` so bearer auth does not break internal jobs
- when `./start-api.sh` handles the migration path, it exports `DB_AUTO_UPGRADE=false` for the uvicorn process so FastAPI lifespan startup does not immediately run a duplicate migration pass
- if `API_REQUEST_LOGGING_ENABLED=true` and/or `UPSTREAM_HTTP_LOGGING_ENABLED=true`, the service writes rotating JSONL audit logs under `apps/api/var/log` by default

## Health checks
- `GET /health`
- `GET /broker/kis/health`
- `GET /quotes/realtime/monitored`
- `GET /market-data/crypto/markets`
- `GET /market-data/crypto/tickers?markets=KRW-BTC,KRW-ETH`
- `GET /market-data/crypto/candles?market=KRW-BTC&interval=day&count=30`
- `GET /market-data/crypto/candles?market=KRW-BTC&interval=minute&unit=15&count=30`
- `GET /market-data/crypto/strategy-context?markets=KRW-BTC,KRW-ETH&count=30`
- `POST /broker/kis/intraday-snapshots/collect?symbols=005930`
- `GET /broker/kis/intraday-snapshots?symbols=005930&limit=5`
- `GET /portfolio/summary`
- `GET /trading/orders/exec/runtime?refresh=true`
- `GET /trading/orders/exec/signal-decider?target_trade_date=2026-04-30&refresh=true`
- `GET /trading/orders/intraday-auto-sell?account_reference=KIS:12345678-01&symbol=005930&refresh=true`
- `POST /trading/orders/intraday-auto-sell/run-if-due`
- `GET /macro/ecos/series?stat_code=722Y001&cycle=M&start_period=202401&end_period=202403&item_code_1=0101000`
- `GET /macro/context`
- `POST /macro/context/refresh?force=true`
- `GET /disclosures/dart/company-snapshot?stock_code=005930`
- `POST /disclosures/dart/sync/events?days=7`
- `GET /disclosures/dart/sync/inspect?financial_only=true`
- `GET /disclosures/dart/fundamentals?stock_code=005930&business_year=2025&report_code=11011&fs_div=CFS`
- `GET /disclosures/dart/fundamentals/history?symbols=005930&symbols=000660&start_date=2024-01-01&end_date=2024-12-31`
- `POST /disclosures/dart/fundamentals/capture?limit=20`
- `GET /disclosures/dart/fundamentals/capture/inspect?capture_status=failed`
- `POST /disclosures/dart/fundamentals/backfill/current-universe?symbol_limit=25&capture_limit=250`
- `POST /disclosures/dart/fundamentals/daily/run-if-due?market_date=2026-04-14&symbol_limit=250&capture_limit=500`
- `GET /disclosures/dart/fundamentals/daily/inspect?market_date=2026-04-14`
- `GET /disclosures/dart/filings/document?rcept_no=20260407000001`
- `GET /disclosures/dart/filings/xbrl?rcept_no=20260407000001&report_code=11011`
- `GET /fx/koreaexim/rates?search_date=20260406&currency=USD`
- `GET /scanner/trending-stocks?period=5d&direction=both&limit=20`
- `GET /eod/status`
- `GET /agent-context/feedback/decider-coverage?market_date=2026-05-08`
- `POST /agent-context/signal-decider/weekly-learning/publish`
- `GET /agent-context/signal-decider/weekly-learning/latest?as_of_date=2026-10-13`
- `POST /agent-context/feedback/rebind/prepare`
- `GET /eod/pipeline/status`
- `GET /eod/pipeline/stages`
- `POST /eod/master/refresh-if-due`
- `POST /eod/pykrx/universe/refresh`
- `POST /eod/pykrx/backfill`
- `POST /eod/pykrx/daily/run-if-due`
- `POST /eod/pykrx/enrichment/collect`
- `GET /eod/pykrx/enrichment`
- `POST /eod/pipeline/run`
- `POST /eod/technical-snapshots/compute`
- `GET /eod/technical-snapshots`
- `POST /eod/action-summaries/compute`
- `GET /eod/action-summaries`
- `POST /eod/signal-features/collect`
- `GET /eod/signal-features`
- `POST /eod/market-risk-snapshots/collect`
- `GET /eod/market-risk-snapshots`
- `POST /eod/market-risk-filters/compute`
- `GET /eod/market-risk-filters`
- `POST /eod/low-risk-factors/compute`
- `GET /eod/low-risk-factors`
- `POST /eod/beta-factors/compute`
- `GET /eod/beta-factors`
- `POST /eod/advanced-value-factors/compute`
- `GET /eod/advanced-value-factors`
- `POST /eod/unified-value-factors/compute`
- `GET /eod/unified-value-factors`
- `POST /eod/momentum-factors/compute`
- `GET /eod/momentum-factors`
- `POST /eod/composite-factors/compute`
- `GET /eod/composite-factors`

## Current runtime expectations
- Core server should stay up 24/7.
- The API startup path tries to prime the persisted `/market/snapshot` cache once. The background runner keeps flow and FX on the normal configured cadence (`MARKET_SNAPSHOT_REFRESH_INTERVAL_SECONDS`, default 3600s). On a KRX trading day from 09:00 through 14:30 KST it refreshes only the KIS index leg every `MARKET_SNAPSHOT_OPEN_DRIVE_REFRESH_INTERVAL_SECONDS` (default 300s). Process-local single-flight coalescing is backed by a durable database bucket claim so only one process starts a bucket refresh. Transaction advisory locking makes concurrent first insert safe; attempted time, each KOSPI/KOSDAQ provider observation, receipt time, and quality form a monotonic persistence fence, so a slow failure or one regressed market leg cannot overwrite newer successful evidence merely because the other leg advanced the top-level timestamp. An initial timeout stays fail-closed while the shielded single-flight task finishes; a later success clears the runner cooldown immediately, while failure/cancellation retains it. That refresh completes before open-session plan refresh or dispatch. The configured interval cannot exceed `MARKET_SNAPSHOT_OPEN_DRIVE_MAX_AGE_SECONDS` (default 360s).
- Open/trend evidence uses the KIS intraday index-bar business date and session time as its observation timestamp; the time-series request includes prior-session rows so PREOPEN can retain the latest provider proof, while cache collection/attempt timestamps never relabel or order a prior-session row. A newly built snapshot without provider `as_of` is not persisted or served: Core preserves a valid last-good snapshot as stale, or returns controlled unavailability until the next immediately-due refresh repairs an empty/legacy null cache. The same KIS index response's `prdy_tr_pbmn` is the preferred prior-session turnover baseline. PyKRX is queried only when that field is unavailable, and only a `completed`, zero-failure EOD run with complete as-of-universe metadata, exact requested-versus-persisted symbol membership, and matching SHA-256 membership digests qualifies. A missing or timed-out fallback leaves the fresh index leg visible but disables value-expansion confirmation.
- Every live index refresh performs current-before, one-minute-series, and current-after KIS reads per market. The timestamped bar must fit the monotonic cumulative-turnover bracket, and the newer current response becomes the displayed row. Budget two current-index requests plus one time-series request per market per refresh when checking KIS quotas and rate limits.
- A delegated v4 create is preceded by a durable, compact `pending_remote_ack` record. Restart recovery and all later readback/revocation calls must match its normalized original Exec URL, runtime mode, dispatch source, and account reference. If those settings change, Core sends nothing to the replacement endpoint and leaves the authority in manual review; restore the original destination or reconcile it explicitly. Bearer-token rotation at the same destination is supported, and tokens or embedded URL credentials must never be stored in dispatch evidence. Pending remote ACK state remains managed after dispatch or next-open execution is disabled: Core performs same-destination readback and revocation before routing can fall back to Core-local buys.
- Before any OPEN v4 buy dispatch, Core derives a route-wide `current_live_market_buy_authority_v1` observation from fresh authoritative KOSPI and KOSDAQ evidence, binds it to the exact KIS/KRX/KRW runtime/account scope, and commits the authority state plus outbox row in one transaction. It sends that observation to Exec at `POST /runtime/core-buy-authority/observations` even when no candidate survives. Missing or stale required market evidence is an explicit block; transport failure, a stale/conflicting observation ID, or a mismatched ACK leaves the outbox pending and buying blocked.
- A short-term `monitor_candidate` / `intraday_watch_buy` plan may still be delivered to Exec while that scoped buy-authority fence is active. This delivery grants no order authority: Exec must keep submission blocked until it observes a fresh `allow`, while continuing realtime price and trigger monitoring for the candidate.
- Open-session buy authority is evaluated from the exact KOSPI/KOSDAQ provider leg used by the candidate. A failed or stale unrelated snapshot leg such as USD/KRW does not invalidate a fresh, provider-sourced, same-session market breadth proof; the market-specific freshness, provenance, universe-coverage, and bucket-consistency checks still fail closed independently.
- Treat the Exec ACK as complete only when it matches the submitted observation and reports both `fence_active=false` and `projection_pending=false`. Exec persists the scoped fence before projecting it onto eligible Core-v4 KRX plan items, so an interrupted projection remains closed and retryable. Replayed healthy observations do not advance the healthy streak. Recovery releases only the scoped market-authority fence or drawdown latch; it never resumes the whole runtime, clears a kill switch, bypasses hourly risk-off, or revives an old order. The next fresh candidate must pass every current gate again.
- The accepted contract proof stores the exact ACK mapping from each Core item key to its Exec plan ID and plan-item ID. Exec v4 `item_acks` are immutable acceptance proof; operators and code must use `item_states`/Core `current_state` for live status. Plan-wide `contract_quarantined` overrides the historical accepted ACK, terminal no-execution outcomes make the Core row non-active, and unknown statuses remain unknown. Readback status is merged only on an exact three-field match. When remote orders or executions provide stable IDs, every supplied item/order ID plus symbol and side must resolve to the same ACK-bound item; symbol fallback is allowed only when no stable ID exists. Plan-scoped position reads require both external and internal plan IDs, KIS/KRX route proof, and one non-empty opaque account scope. Conflicts are discarded with a contract error and any later revoke continues to use the original ACK IDs. Compact readback total counts are not item counts: incomplete order/execution sources are excluded from P/L unless a complete alternate source exists.
- Exec US-account evidence is intentionally fail-closed. Each non-zero holding needs a finite, non-negative broker market value or a deterministic average-price/quantity/unrealized-P&L derivation, and each open buy needs a known positive-price notional. Legacy cumulative fills without explicit `fill_time_precision` are treated as `observed_upper_bound`, make FIFO chronology indeterminate, and block US new buys until a later exact-scope verified-flat boundary. Duplicate fill IDs deduplicate only when quantity, base/quote notional, price, settlement currency, transaction cost, coverage, source, and currency-conflict evidence agree; an economic conflict excludes the entire group from realized P/L and blocks US new buys for review.
- The shared KIS OAuth issuance lease is a hard monotonic deadline for Exec async and sync lock waiters, including the sync coroutine bridge. Lock or cache-operation timeout is retryable (`503`, `TRANSIENT`, `RETRY_PENDING`): confirm contention or lease expiry and retry normally. Do not bypass the shared cache, issue an uncoordinated token, or replay the failed broker request.
- A transient critical-evidence revocation can produce a new dispatch generation only after the exact revocation ACK, unchanged authority/destination/material fingerprint, and a fresh authoritative allow observation. Explicit revocations, route or material changes, and manual-review rows never auto-resume.
- Live Exec dispatch requires exactly one explicit valid `SIGNAL_DECIDER_NEXT_OPEN_ACCOUNT_TARGETS` KIS account and exact handoff-row binding to it. Missing, invalid, multiple, mismatched, or synthetic `KIS:00000000-00` routes fail before HTTP; the synthetic route is limited to mock/paper dispatch and monitor-only planning.
- All five KIS breadth buckets (upper-limit, advance, unchanged, decline, lower-limit) are retained. Strong breadth requires a completed same-date KIS-master universe proof and a bucket-total match that resolves the provider's limit-bucket semantics; count-only or partial responses fail closed. `aggressive_open` and `trend_day` remain observable shadow regimes, but their uncalibrated 2.0x/1.25x sizing and confidence uplifts are shadow-only: live `quantity_multiplier` is capped at 1.0 and live entry-priority boost is disabled. The value-expansion audit records the actual elapsed fraction and conservative denominator (`max(actual, 0.25)`); no full-day linear extrapolation or last-known fallback is used.
- `GET /market/snapshot` serves the persisted snapshot read model instead of making live upstream calls per dashboard request.
- Core owns one KIS realtime domestic price websocket for currently monitored execution-runtime symbols when `KIS_REALTIME_PRICE_ENABLED=true`; dashboard reads use process-local memory through `/quotes/realtime/monitored`, `/quotes/realtime/monitored/stream`, and execution-runtime display price fields instead of browser-side KIS sockets or request-time KIS quote fan-out. When monitored symbols exceed websocket capacity, execution-runtime dashboard rows can display local quote-cache/current-quote/daily-close fallbacks with explicit `price_source`, `price_status`, `display_price_available`, and realtime subscription metadata. Subscription-source refresh is time-bounded and preserves the last observed symbols on timeout, so a slow read-model build cannot hold the websocket forever in `starting`.
- Pre-market master refresh uses the KIS master zip files.
- A background runner checks market state every 60 seconds while the API process is alive.
- `GET /eod/status` exposes the runner heartbeat, last tick result, staleness threshold, in-flight steps, and timeout cooldowns. Treat `background_runner.status=stale`, `staleness.is_stale=true`, or `task_alive=false` while enabled as an orchestration-health failure. The `job` block is labeled legacy/manual capture; the current automatic daily pipeline is the separately labeled `pykrx_auto` block.
- During `PREOPEN`, it refreshes the daily master if needed.
- During `PREOPEN`, it also refreshes the persisted signal-decider next-open execution plan for the current trade date when account targets are configured.
- When `SIGNAL_DECIDER_EXEC_INTEGRATION_MODE=dispatch`, the same `PREOPEN` loop translates eligible next-open rows into normalized exec plan items and dispatches them to `kai-trading-exec` once the clock reaches 08:00 KST, using stable plan/item keys for idempotent redispatch.
- The pre-open refresh re-evaluates every non-terminal row against the latest actionable KRX-scoped snapshot, corrected daily bars, PyKRX enrichment, active-policy state, account targets, and deterministic `next_open_preopen_policy_v1` fingerprint inputs. It never inherits stale buy authority from an earlier scheduler tick. The fingerprint binds the integration mode, cost-evidence contract version, server non-spread cost, confidence/expected-return/reward-risk thresholds, and exact account label/reference/budget; confidence and expected-return settings must be finite and within their validated safety bounds. Terminal dispatched/submitted history remains immutable. Late feedback adds or updates rows on the next scheduler tick. During the same target-date `OPEN` session, a late short-term `watch_for_trigger` snapshot may be handed off only as `intraday_watch_buy`: it remains monitor-only until Exec observes the declared live trigger, and it must still pass current quote freshness, chase, cost, portfolio, kill-switch, and live market-risk gates. Post-open signal-decider next-open buys and unconditional short-term buy actions remain expired. During `OPEN`, dispatch mode repeats the incremental refresh and idempotent handoff before readback so a pre-open outage or restart cannot silently omit monitoring candidates.
- If Exec already accepted part of an account/agent group, Core sends only newly pending rows under a deterministic `-delta-<key>` plan id. This avoids the create-only Exec API's duplicate-plan conflict while keeping retries idempotent.
- Signal-decider, short-term-decider, and exec-agent handoffs now carry `decision_input_quality_context`, a deterministic buy-quality envelope that separates data quality, market/setup edge, execution feasibility, and final judgment. Operators should treat stale/fallback quote evidence and missing raw source turnover as hard blocks or major discounts in that envelope; missing news/disclosure context is secondary and neutral by default. The envelope is explicitly free/current-data-only (`paid_api_dependency=false`): missing provider data should be encoded with provenance and conservative gates, not solved by assuming paid APIs.
- Core bounds verbose advisory handoff context before v4 dispatch (`decider_handoff_context` at 12 KiB and entry `risk_context` at 32 KiB), records a digest of the complete source, and carries the sizing-critical flow/short-selling/sector/market-breadth context as a separate bounded object. Delegated gate structures are never admitted to the lossy nested-compaction allowlist; if their exact shape cannot fit, dispatch fails closed instead of silently weakening an execution fence.
- Executable spread evidence expires after `KIS_REALTIME_PRICE_STALE_AFTER_SECONDS` (default `30`). It requires the exact provider table set for one source context (`current_quote_cache` + `quote_snapshots`, or `current_kis_intraday_snapshot_cache` + `kis_intraday_snapshots`), a timezone-aware same-market-date timestamp, and a positive coherent price/bid/ask/spread bundle. Extra tables, mixed quote/intraday components, zero or crossed books, future timestamps, and stale observations are non-authoritative.
- Short-term next-open/watch candidates carry a signal-decider alignment gate audit. The live default remains `SIGNAL_DECIDER_WAIT_CONFLICT_GATE_MODE=report_only`. Explicit `hard_block` enforces zero; `downsize_probe` passes the configured probe as a hard `max_quantity` ceiling while Exec retains final sizing authority and may reduce or reject it. Valid overrides are preserved, and missing/stale signal-decider context is never silently approved. Core persists the final enforcement decision plus requested/final quantity through Exec handoff and risk context. Promotion thresholds and rollback are documented in [WAIT_CONFLICT_GATE_ENFORCEMENT.md](./docs/WAIT_CONFLICT_GATE_ENFORCEMENT.md).
- A plan row blocked as `decision_input_generation_changed` still has a latest candidate, but its durable source/policy/eligibility/prior-decision/upstream evidence no longer matches; regenerate or repair that authority before dispatch. `candidate_removed` means the latest candidate itself is absent. Do not bypass either fail-closed state merely to populate Exec monitoring.
- Agent-feedback eligibility treats only an eligible prior-source-date execution row as an active watch. A blocked row or a row produced from the market date currently being validated is downstream output, so it cannot force-include itself or invalidate its own durable decision-input fingerprint.
- Agent-context completeness follows the role-specific decision cohort. A short-term candidate requires its selected short-term scope and a signal-decider candidate requires its selected signal-decider scope; it does not fabricate a missing opposite-role requirement when the completed parent run intentionally used different role symbol scopes.
- Next-open authority reconstruction scans immutable snapshot routing/lineage columns plus bounded scalar JSON projections for tactical action and explicit market claims, then hydrates the full JSON payload only for the selected snapshot IDs in the same transaction and verifies an exact authority token. The bounded projections let the post-open loader distinguish trigger-gated watches without loading every historical snapshot payload. Feedback-run request/summary JSON is each fetched once per row and narrowed in process, avoiding repeated PostgreSQL detoast/parse work while preserving the same authority fields. This keeps the final Exec fence fail-closed without reading every historical snapshot TOAST payload.
- During `OPEN`, the `SIGNAL_DECIDER_EXEC_INTEGRATION_MODE=disabled` compatibility path can inspect Core-local next-open plans, but signal-decider buys remain ineligible because Core has no execution-time quote/cost recheck. A source-day quote and the non-spread allowance do not authorize submission; use Exec dispatch for live trigger-time full-cost validation.
- During `OPEN`, if `SIGNAL_DECIDER_EXEC_INTEGRATION_MODE=disabled`, core also evaluates linked next-open positions against persisted signal-decider target/stop thresholds and submits intraday auto-sell market orders when the live KIS quote crosses a configured exit rule.
- During `OPEN`, if `SIGNAL_DECIDER_EXEC_INTEGRATION_MODE=dispatch`, core does not place those live signal-decider orders itself. Instead it refreshes the core-owned exec read model from the exec runtime APIs so operator and web views can read dispatch/runtime/order/execution state without double-submitting live actions from core.
- Execution runtime dashboard reads (`/execution/runtime/daily-board`, `/execution/runtime/monitored-symbols`, and `/execution/runtime/symbols/{symbol}`) are cache-first/local-read-model paths. `refresh=true` no longer waits for exec/KIS readback in the HTTP request; it schedules one bounded, debounced background refresh and returns the current read model with `freshness` and `refresh` metadata for stale, partial, refreshing, timeout, or unavailable states. The background readback timeout defaults to 30 seconds because one plan can require several serialized Exec reads; the browser request remains nonblocking. Daily-board and symbol-detail rows include compact `today_activity` fields so Today bought/sold views can use selected-date buy/sell fills instead of inferring side from lifecycle intent.
- KIS intraday execution-context snapshots are a separate credential-aware capture lane. They do not run inside the PyKRX/DART EOD jobs, and the manual/API collect path safely returns `status=disabled` when KIS credentials are unavailable or the feature toggle is off.
- Macro context refresh is a slow-moving contextual input lane. It uses no-key World Bank indicators by default, reports optional FRED as disabled unless `FRED_API_KEY` is configured, and must be treated as background context only, not a direct buy/sell signal. Inside the global cooldown, it still retries configured series that are missing or individually due and reports the exact subset under `retry_scope`; recent series are not fetched again.
- External context provider registry is currently a foundational internal service (`app.external_context`) rather than an HTTP route. Providers declare supported context keys, TTL, minimum refresh interval, source status, source URL/upstream endpoint, and last success/error metadata. The in-process store can serve fresh cached payloads before TTL expiry, force refreshes bypass TTL while still honoring provider refresh guards, and provider errors preserve the last good payload as stale metadata.
- During a trading-day `CLOSED` state, after `EOD_AUTO_PYKRX_AFTER_CLOSE_MINUTES`, it schedules a thread-based pykrx daily EOD run if that trade date is available and not already completed. During `WEEKEND` or `HOLIDAY`, the same scheduler targets `latest_valid_trading_date` without applying a fictitious close buffer, so bounded recovery for the prior session is not starved until the next close. That scheduled PyKRX path includes `pykrx_enrichment` after price collection so sector classification, investor-flow-symbol, short-selling, and valuation rows exist before the next exec/decider cycle.
- After the daily decision-support deciders complete, the same EOD chain immediately refreshes the persisted next-open execution plan for the next KRX open date. This closes the overnight gap where buy snapshots existed but `signal_decider_execution_plan_items` remained empty until the PREOPEN scheduler. The refresh is idempotent and only materializes plan rows; normal PREOPEN/OPEN dispatch timing and execution safety gates remain unchanged. It fails closed when a candidate's role/symbol scope is incomplete, unknown, or unavailable. Inspect `summary.agent_context_completeness`: `status` is readiness across the visible candidates, `cohort_status` is the full retry-cycle cohort, and `candidate_set_definitive=false` means failures elsewhere may still have omitted candidates. Every row stores its own candidate-specific authority fingerprint, so one incomplete symbol does not block an independently complete symbol. Versioned retry parents accept only child runs explicitly linked by `parent_run_id`; a late child from an older parent cannot replace the current cohort. A versioned reuse must name the exact role/symbol under `reused_symbols_by_role` and bind `feedback_run_id`, `feedback_received_at`, available `feedback_id`, plus immutable decision identity (`decision_snapshot_id`, or its content fingerprint, and optional observed time) under `reused_feedback_evidence_by_role`. That evidence must resolve to the immutable snapshot actually selected by the immediately preceding applicable parent, so a mutable feedback receipt, late callback from an older parent, or older sibling retry cannot regain BUY authority. Non-versioned legacy rows may use snapshot creation time only when their feedback receipt is null. A requested result with neither a new snapshot nor valid exact evidence is a tombstone. An abandoned or stale-running newer parent or child is a fail-closed barrier instead of reviving an older BUY cohort; equal start times are ordered by the durable row id. Recover by completing a newer retry cycle rather than editing lineage rows. Refresh and pre-dispatch checks revoke or block stale BUY authority when this evidence changes. Both decision-support roles are invalidated when their upstream summarizer feedback changes; short-term decisions do not bypass this freshness rule.
- A decision snapshot is actionable for next-open-at-open authority only when its canonical observation time (`feedback_received_at`, with `created_at` allowed only for legacy rows that have no receipt time) is strictly before the target KRX session open. A snapshot observed at or after that open remains a tombstone for signal-decider BUYs, short-term `scalp_buy_candidate`, replay, evaluation, calibration, prior-positive eligibility, and policy-candidate reads; none of those paths may revive it or fall back to an older snapshot. The sole live-planning exception is a same-day KRX `OPEN` short-term `watch_for_trigger`: Core may materialize it as `monitor_candidate` / `awaiting_trigger` / `intraday_watch_buy`, never as retrospective next-open market-order authority. A same-symbol late signal-decider `wait` or `reject` may be retained only as non-BUY alignment context for that watch. Fresh live quotes, trigger/chase ranges, account and portfolio risk, kill switch, and live market risk-off remain mandatory at execution time. Once the target session is no longer `OPEN`, the late-watch exception closes and the tombstone contract applies again.
- The automatic after-close path now also refreshes recent signal-decider evaluation coverage over the rolling lookback once stored EOD bars are available, and it refreshes the persisted review calendar from the same recompute.
- API startup also runs a bounded recent signal-decider evaluation catch-up from the latest stored price-bar trade date so deploys or migrations can backfill mature horizons without waiting for the next close.
- Database persistence stores symbol master data, tracked symbols, quote cache/history, market status snapshots, the current market snapshot cache, durable Korea Exim FX snapshots for request-time USD/KRW fallback, macro context observations, EOD bars, EOD run records, persisted daily price contexts, daily PyKRX enrichment rows, daily technical/action snapshots, KIS daily signal features, KIS intraday snapshot history/current-cache rows, KIS daily market-risk snapshots, persisted daily market-risk filter results, persisted daily momentum factor results, persisted daily low-risk factor results, persisted daily beta factor results, persisted daily advanced value factor results, persisted daily unified value factor results, persisted daily composite factor results, persisted daily market participation context, and stored day-based trending ranks.

## Database bootstrap

Before first startup on a fresh machine, from `apps/api`:

1. `pip install -r requirements.txt`
2. Create the Postgres database and user referenced by `.env`
3. `alembic upgrade head`
4. `python -c "from app.main import app"`

## Database pool operations

`GET /health` reports configured capacity and current checked-in, checked-out,
and overflow counts. Health checks are single-flight, so overlapping probes
fail fast instead of stacking connection attempts during an outage.
Connections are pre-pinged, recycled after
`DB_POOL_RECYCLE_SECONDS`, and use LIFO reuse by default so idle server-side
connections can expire cleanly. Size `DB_POOL_SIZE + DB_MAX_OVERFLOW` against
the total number of API/worker processes, not one process in isolation.

JSON-heavy signal-decider realized-outcome, evaluation, and calendar-review
upserts are committed in chunks of 10 rows. Keep these transactions small so
locks, statement memory, and checked-out connections are released between
chunks while the daily pipeline is active.

Check logs for long checkout warnings before increasing capacity. A long-held
transaction or upstream call inside a database session will otherwise consume
any larger pool too. `DB_POOL_WATCHDOG_CANCEL_ENABLED` remains off by default;
when explicitly enabled, the watchdog only cancels known read-only analytics
queries older than the configured threshold and never cancels write queries.

## Deployment migration hygiene

Revision `20260911_0081` is a coordinated Core/Exec authority migration. Use
this order:

1. Pause delegated dispatch, then drain every Core database writer and
   background process.
2. Create a compressed PostgreSQL backup with `pg_dump -Fc`; verify it is
   non-empty and readable with `pg_restore --list`.
3. Upgrade Exec first through `202609110003`, start it paused, and verify its
   schema/runtime readiness.
4. Upgrade Core through `20260911_0081` with exactly one migration owner.
5. Start Core while dispatch remains paused, reconcile every pending authority
   outbox row, then verify a mock or paper authority observation and matching
   Exec ACK.
6. Resume only after the existing runtime, kill-switch, hourly risk-off,
   account-drawdown, and authority fences have been reviewed independently.

Once either `0081` table contains authority or outbox evidence, do not run the
migration's destructive downgrade. Restore the verified backup or fix forward.

Use these commands from `apps/api` when you want deploy-time schema checks to fail early and explain drift more clearly:

1. Preflight before upgrade or restart:
   `python3.11 -m app.internal.deploy_hygiene preflight --format text`
2. Runtime schema drift preflight when write-heavy workflows fail or before declaring them healthy:
   `python3.11 -m app.internal.deploy_hygiene schema-drift-preflight --format text`
   or through the internal ops surface:
   `python3.11 -m app.internal.ops schema-drift-preflight`
3. Explicit head gate for a deploy:
   `alembic upgrade head`
   `python3.11 -m app.internal.deploy_hygiene preflight --require-head --format text`
4. Restart the API.
5. Post-restart smoke:
   `python3.11 -m app.internal.deploy_hygiene smoke-check --base-url http://127.0.0.1:8000 --format text`

Notes:

- This release expects one Core Alembic head, `20260911_0081`; that revision adds the transaction-bound Core buy-authority state/outbox. Pair it with Exec head `202609110003` before enabling OPEN dispatch.
- The preflight is intentionally non-destructive. It does not mutate schema state.
- The migration preflight includes the runtime schema drift result. It blocks on schema drift when the database is expected to be at Alembic head, including the `--require-head` deploy gate.
- The preflight now also reports whether `/market/snapshot` is runtime-ready before restart, including the resolved KIS environment and any missing required secrets.
- The preflight now also reports whether `/portfolio/summary` is runtime-ready before restart, including whether `PORTFOLIO_ACCOUNT_REFERENCES` is present and whether production is still pointed at the mock KIS environment.
- The duplicate-table warning is heuristic by design: it flags live tables that overlap with still-pending unguarded `create_table(...)` revisions, which is the common `alembic_version` lag pattern behind repeated `DuplicateTable` deploy failures.
- The smoke command now exercises `/portfolio/summary` in addition to `/health`, `/eod/freshness`, and `/market/snapshot`, and it fails when portfolio sync is still `not_configured` after restart.
- The smoke command automatically sends `Authorization: Bearer <token>` when `UPSTREAM_BEARER_TOKEN` is set in the current shell environment.

Before starting Core, verify both versioned OpenClaw instruction contracts from
the repository root:

```bash
python3.11 scripts/sync_openclaw_runtime_contract.py --check --agent signal-decider
python3.11 scripts/sync_openclaw_runtime_contract.py --check --agent short-term-decider
```

Any status other than `match` blocks deployment. The check is read-only and
reports hashes without printing contract content. Use `--install` only after
reviewing the versioned template, then rerun `--check`; installation changes
only that workspace's `AGENTS.md` and never enables dispatch or alters trading
state.

## Runtime write-path schema drift diagnostics

When an EOD, DART, signal-decider, or execution write path fails, debug in this order:

1. Reproduce the real write path that failed, preferably through `python3.11 -m app.internal.ops ...`, so the same SQL conflict target and payload shape are exercised.
2. Inspect runtime schema constraints with `python3.11 -m app.internal.ops schema-drift-preflight`; if it blocks, compare the reported table, primary key, unique constraint/index, and foreign-key details against the expected migration.
3. Only after the write path and schema objects are verified should you debug providers, credentials, upstream payload content, or rate limits.

The schema drift preflight is diagnostic only. It never creates tables, adds constraints, stamps Alembic state, or mutates production schema.

## Duplicate-table drift recovery

If the preflight flags likely drift, or `alembic upgrade head` fails with `DuplicateTable`:

1. Stop automatic restart or migration retries for that deploy.
2. Inspect the flagged live table, indexes, unique constraints, and foreign keys against the pending migration file.
3. Only if the live schema materially matches the intended migration boundary, run `alembic stamp <matching_revision>`.
4. Use `alembic stamp head` only when every still-pending revision is already reflected in the live schema.
5. Rerun `python3.11 -m app.internal.deploy_hygiene preflight --require-head --format text`.
6. Restart the API and rerun `python3.11 -m app.internal.deploy_hygiene smoke-check --base-url http://127.0.0.1:8000 --format text`.

If schema parity is not clean, write a corrective migration instead of dropping tables in place.

## External-news point-in-time integrity

- Apply migrations through `20260906_0079` with `python3.11 -m alembic upgrade head` before collecting or reading external news. Revision `0078` adds article-to-symbol associations; `0079` adds append-only classifier versions.
- Article rows are immutable; repeated provider IDs or URLs may add an idempotent row in `external_news_item_symbols` but must not rewrite title, payload, `published_at`, or `collected_at`.
- Historical symbol reads require the article observation, its `associated_at`, and the selected classifier version's `classified_at` to be earlier than the exclusive knowledge cutoff.
- `external_news_enrich` inserts the current classifier version for legacy rows into `external_news_item_classifications`; it never rewrites an article. Replays are idempotent per article/version. Change the classifier version before changing classification semantics—existing versions must remain immutable.
- Embedded `payload.classification` values have no trustworthy availability timestamp and are not exposed as decision evidence. After deployment, run the normal `external_news_enrich` stage once to create timestamped sidecars for recent legacy rows.

## pykrx EOD backfill

From `apps/api` on this machine:

1. `python3.11 -m pip install -r requirements.txt`
2. `alembic upgrade head`
3. `python3.11 -m app.eod.pykrx_backfill refresh-universe`
4. `python3.11 -m app.eod.pykrx_backfill backfill --start-date 20260408 --end-date 20260408`

Useful limited-scope runs:

- One-symbol verification: `python3.11 -m app.eod.pykrx_backfill backfill --start-date 20260408 --end-date 20260408 --symbol 005930 --limit 1`
- API-triggered universe refresh: `curl -X POST http://127.0.0.1:8000/eod/pykrx/universe/refresh`
- API-triggered backfill: `curl -X POST "http://127.0.0.1:8000/eod/pykrx/backfill?start_date=20260408&end_date=20260408&stages=price_collection,technical_snapshots,action_summaries"`
- DART company snapshot smoke test: `curl "http://127.0.0.1:8000/disclosures/dart/company-snapshot?stock_code=005930"`
- DART fundamentals smoke test: `curl "http://127.0.0.1:8000/disclosures/dart/fundamentals?stock_code=005930&business_year=2025&report_code=11011&fs_div=CFS"`
- DART stored fundamentals history smoke test: `curl "http://127.0.0.1:8000/disclosures/dart/fundamentals/history?symbols=005930&symbols=000660&start_date=2024-01-01&end_date=2024-12-31"`
- DART fundamentals capture smoke test: `curl -X POST "http://127.0.0.1:8000/disclosures/dart/fundamentals/capture?limit=5"`
- DART fundamentals capture inspect smoke test: `curl "http://127.0.0.1:8000/disclosures/dart/fundamentals/capture/inspect?capture_status=failed"`
- DART current-universe backfill smoke test: `curl -X POST "http://127.0.0.1:8000/disclosures/dart/fundamentals/backfill/current-universe?symbol_limit=5&capture_limit=25"`
- DART daily incremental fundamentals smoke test: `curl -X POST "http://127.0.0.1:8000/disclosures/dart/fundamentals/daily/run-if-due?market_date=2026-04-14&symbol_limit=25&capture_limit=100"`
- DART daily incremental fundamentals inspect: `curl "http://127.0.0.1:8000/disclosures/dart/fundamentals/daily/inspect?market_date=2026-04-14"`
- DART current-universe backfill continuation CLI: `python3.11 -m app.dart.backfill_current_universe --symbol-limit 10 --symbol-offset 5 --capture-limit 200 --fs-div CFS`
- DART current-universe overnight runner: `python3.11 -m app.dart.backfill_current_universe_runner --start-offset 25 --batch-size 10 --capture-limit 200 --fs-div CFS --log-file var/log/dart-backfill-overnight.log`
- DART filing original document smoke test: `curl -OJ "http://127.0.0.1:8000/disclosures/dart/filings/document?rcept_no=20260407000001"`
- DART filing XBRL smoke test: `curl -OJ "http://127.0.0.1:8000/disclosures/dart/filings/xbrl?rcept_no=20260407000001&report_code=11011"`
- ECOS series smoke test: `curl "http://127.0.0.1:8000/macro/ecos/series?stat_code=722Y001&cycle=M&start_period=202401&end_period=202403&item_code_1=0101000"`
- API-triggered daily auto path: `curl -X POST "http://127.0.0.1:8000/eod/pykrx/daily/run-if-due?market_date=2026-04-03&force=true"`
- API-triggered PyKRX enrichment collect: `curl -X POST "http://127.0.0.1:8000/eod/pykrx/enrichment/collect?market_date=2026-04-21&markets=KOSPI,KOSDAQ&categories=sector_classification,investor_flow_symbol,short_selling,valuation_snapshot"`
- API-triggered PyKRX enrichment inspect: `curl "http://127.0.0.1:8000/eod/pykrx/enrichment?market_date=2026-04-21&markets=KOSPI&symbols=005930"`
- Stage catalog inspect path: `curl "http://127.0.0.1:8000/eod/pipeline/stages"`
- API-triggered explicit stage run: `curl -X POST "http://127.0.0.1:8000/eod/pipeline/run" -H "Content-Type: application/json" -d '{"market_date":"2026-04-03","stages":["price_collection","technical_snapshots","agent_feedback_summaries"],"symbols":["005930"]}'`
- API-triggered explicit stage run with signal-decider evaluation refresh: `curl -X POST "http://127.0.0.1:8000/eod/pipeline/run" -H "Content-Type: application/json" -d '{"market_date":"2026-04-30","stages":["price_collection","technical_snapshots","agent_feedback_summaries","signal_decider","signal_decider_evaluations"],"symbols":["005930"]}'`
- Preferred internal daily auto path: `python3.11 -m app.internal.ops eod-pykrx-daily-run-if-due --market-date 2026-04-03 --force`
- Internal PyKRX enrichment collect path: `python3.11 -m app.internal.ops eod-pykrx-enrichment-collect --market-date 2026-04-21 --markets KOSPI,KOSDAQ --categories sector_classification,investor_flow_symbol,short_selling,valuation_snapshot`
- Internal PyKRX enrichment inspect path: `python3.11 -m app.internal.ops eod-pykrx-enrichment-inspect --market-date 2026-04-21 --symbols 005930`
- Internal stage catalog path: `python3.11 -m app.internal.ops eod-pipeline-stages`
- Internal explicit stage run path: `python3.11 -m app.internal.ops eod-pipeline-run --market-date 2026-04-03 --stages price_collection,technical_snapshots --symbols 005930`
- Manual signal-decider evaluation refresh over the recent window: `python3.11 -m app.internal.ops agent-signal-decider-evaluations-compute --market-date 2026-04-30 --symbols 005930 --horizons 1,3,5,10`
- Manual signal-decider evaluation coverage repair over an explicit range: `python3.11 -m app.internal.ops agent-signal-decider-evaluations-repair --market-date-from 2026-04-01 --market-date-to 2026-04-30 --horizons 1,3,5,10 --primary-horizons 5`
- Manual rolling short-term WAIT-conflict probe replay: `python3.11 -m app.internal.ops agent-short-term-wait-conflict-replay --completed-weeks 4 --gate-mode downsize_probe --probe-quantity 3 --output-format markdown`
- Reproducible issue-547 rolling window: `python3.11 -m app.internal.ops agent-short-term-wait-conflict-replay --completed-weeks 4 --as-of-date-kst 2026-09-06 --gate-mode downsize_probe --probe-quantity 3 --output-format markdown`
- Manual technical snapshot rerun after EOD: `python3.11 -m app.eod.technical_snapshots --market-date 2026-04-07`
- Manual action summary rerun after EOD: `python3.11 -m app.eod.action_summaries --market-date 2026-04-07`
- Manual low-risk factor rerun from stored bars: `python3.11 -m app.internal.ops eod-low-risk-factors-compute --market-date 2026-04-23`
- Manual beta factor rerun from stored bars: `python3.11 -m app.internal.ops eod-beta-factors-compute --market-date 2026-04-23`
- Manual stored trending rank rebuild after EOD: `python3.11 -m app.internal.ops eod-trending-ranks-compute --market-date 2026-04-07`
- API-triggered technical snapshot rerun: `curl -X POST "http://127.0.0.1:8000/eod/technical-snapshots/compute?market_date=2026-04-07"`
- API-triggered action summary rerun: `curl -X POST "http://127.0.0.1:8000/eod/action-summaries/compute?market_date=2026-04-07"`
- API-triggered low-risk factor rerun: `curl -X POST "http://127.0.0.1:8000/eod/low-risk-factors/compute?market_date=2026-04-23"`
- Persisted low-risk factor inspect: `curl "http://127.0.0.1:8000/eod/low-risk-factors?market_date=2026-04-23&symbol=005930"`
- API-triggered beta factor rerun: `curl -X POST "http://127.0.0.1:8000/eod/beta-factors/compute?market_date=2026-04-23"`
- Persisted beta factor inspect: `curl "http://127.0.0.1:8000/eod/beta-factors?market_date=2026-04-23&symbol=005930"`
- Manual advanced value factor rerun: `python3.11 -m app.internal.ops eod-advanced-value-factors-compute --market-date 2026-04-24`
- API-triggered advanced value factor rerun: `curl -X POST "http://127.0.0.1:8000/eod/advanced-value-factors/compute?market_date=2026-04-24"`
- Persisted advanced value factor inspect: `curl "http://127.0.0.1:8000/eod/advanced-value-factors?market_date=2026-04-24&symbol=005930"`
- Manual unified value factor rerun: `python3.11 -m app.internal.ops eod-unified-value-factors-compute --market-date 2026-04-24`
- API-triggered unified value factor rerun: `curl -X POST "http://127.0.0.1:8000/eod/unified-value-factors/compute?market_date=2026-04-24"`
- Persisted unified value factor inspect: `curl "http://127.0.0.1:8000/eod/unified-value-factors?market_date=2026-04-24&symbol=005930"`
- Manual momentum factor rerun: `python3.11 -m app.internal.ops eod-momentum-factors-compute --market-date 2026-04-07`
- API-triggered momentum factor rerun: `curl -X POST "http://127.0.0.1:8000/eod/momentum-factors/compute?market_date=2026-04-07"`
- Manual composite factor rerun: `python3.11 -m app.internal.ops eod-composite-factors-compute --market-date 2026-04-24`
- API-triggered composite factor rerun: `curl -X POST "http://127.0.0.1:8000/eod/composite-factors/compute?market_date=2026-04-24"`
- Persisted composite factor inspect: `curl "http://127.0.0.1:8000/eod/composite-factors?market_date=2026-04-24&symbol=005930"`
- KIS ranking signal-feature collection: `python3.11 -m app.internal.ops kis-daily-signal-features-collect --market-date 2026-04-14`
- KIS intraday execution-context snapshot collection: `python3.11 -m app.internal.ops kis-intraday-snapshots-collect --symbols 005930 --include-news true`
- KIS market-risk snapshot collection: `python3.11 -m app.internal.ops eod-market-risk-snapshots-collect --market-date 2026-04-14`
- API-triggered KIS intraday execution-context snapshot collection: `curl -X POST "http://127.0.0.1:8000/broker/kis/intraday-snapshots/collect?symbols=005930&max_news_items=5"`
- API-triggered KIS intraday execution-context snapshot inspect: `curl "http://127.0.0.1:8000/broker/kis/intraday-snapshots?symbols=005930&limit=5"`
- API-triggered KIS market-risk snapshot collection: `curl -X POST "http://127.0.0.1:8000/eod/market-risk-snapshots/collect?market_date=2026-04-14"`
- API-triggered KIS market-risk filter compute: `curl -X POST "http://127.0.0.1:8000/eod/market-risk-filters/compute?market_date=2026-04-14"`
- Recent derived-context backfill: `python3.11 -m app.internal.ops agent-derived-contexts-backfill-recent`
- API-triggered KIS ranking signal-feature collection: `curl -X POST "http://127.0.0.1:8000/eod/signal-features/collect?market_date=2026-04-14"`

Weekly-policy rebind safety check (Saturday only):

- Call `POST /agent-context/feedback/rebind/prepare` with `{"as_of_date":"YYYY-MM-DD","market":"KRX"}` after the Friday session is complete. Add the normal bearer header when `UPSTREAM_BEARER_TOKEN` is configured.
- Continue only for `status="ready"`, and submit the returned `retry_request` byte-for-field to `POST /agent-context/feedback/retry-cycles`. Do not reconstruct the summary cutoff, symbols, role scopes, artifact identity, authority proof, or idempotency key.
- `status="noop"` means the exact fully completed two-decider rebind already exists. `status="blocked"` is a safe stop; inspect `reason` and `guards` and do not override it with `force`.
- The prepare and retry paths never call Exec or an order endpoint. Weekly-bound retries are rejected during every current KRX `OPEN` state and are checked again inside the feedback/snapshot write transaction. A partial/failed run leaves the prior next-open plan blocked rather than exposing a partial replacement cohort.
- PREOPEN next-open refresh uses the same coordinator as an idempotent backstop for both Monday and midweek target sessions. If the bound artifact is missing or changes, the source/target dates are not adjacent KRX sessions, or the session opens during work, Core persists no new feedback/snapshot transaction and keeps the plan fail-closed.

Internal ops commands from `apps/api`:

- `python3.11 -m app.internal.ops market-status`
- `python3.11 -m app.internal.ops quotes-tracked`
- `python3.11 -m app.internal.ops scanner-tracked-symbols`
- `python3.11 -m app.internal.ops eod-status`
- `python3.11 -m app.internal.ops eod-pipeline-stages`
- `python3.11 -m app.internal.ops eod-pipeline-run --market-date 2026-04-08 --stages price_collection,technical_snapshots`
- `python3.11 -m app.internal.ops eod-master-refresh-if-due --force`
- `python3.11 -m app.internal.ops eod-pykrx-daily-run-if-due --market-date 2026-04-08 --force`
- `python3.11 -m app.internal.ops eod-pykrx-enrichment-collect --market-date 2026-04-21 --categories sector_classification,investor_flow_symbol,short_selling,valuation_snapshot`
- `python3.11 -m app.internal.ops eod-pykrx-enrichment-inspect --market-date 2026-04-21 --symbols 005930`
- `python3.11 -m app.internal.ops eod-trending-ranks-compute --market-date 2026-04-08`
- `python3.11 -m app.internal.ops eod-momentum-factors-compute --market-date 2026-04-08`
- `python3.11 -m app.internal.ops eod-piotroski-filter-collect --market-date 2026-04-08`
- `python3.11 -m app.internal.ops eod-market-risk-snapshots-collect --market-date 2026-04-14`
- `python3.11 -m app.internal.ops eod-market-risk-filters-compute --market-date 2026-04-14`
- `python3.11 -m app.internal.ops eod-low-risk-factors-compute --market-date 2026-04-23`
- `python3.11 -m app.internal.ops eod-beta-factors-compute --market-date 2026-04-23`
- `python3.11 -m app.internal.ops eod-advanced-value-factors-compute --market-date 2026-04-24`
- `python3.11 -m app.internal.ops eod-unified-value-factors-compute --market-date 2026-04-24`
- `python3.11 -m app.internal.ops eod-composite-factors-compute --market-date 2026-04-24`
- `python3.11 -m app.internal.ops trading-intraday-auto-sell-run-if-due --account-reference KIS:12345678-01 --symbol 005930 --force`
- `python3.11 -m app.internal.ops trading-next-open-execution-run-if-due --target-trade-date 2026-04-30 --dry-run true --force`
- `python3.11 -m app.internal.ops trading-signal-decider-slim-exec-payloads --dry-run true --target-trade-date 2026-05-22`
- `python3.11 -m app.internal.ops dart-fundamentals-daily-run-if-due --market-date 2026-04-14 --symbol-limit 250 --capture-limit 500`
- `.venv/bin/python -m app.internal.ops dart-audit-signals-backfill --market-date 2026-09-15 --limit 25 --after-event-id 0 --dry-run`
- `python3.11 -m app.internal.ops agent-feedback-retry-cycles --market-date 2026-04-14 --symbols 005930`
- `python3.11 -m app.internal.ops agent-feedback-data-gap-summary --market-date-from 2026-05-01 --market-date-to 2026-05-06 --persist-missing-categories true`
- `python3.11 -m app.internal.ops agent-feedback-data-gap-quality-report --market-date-from 2026-05-01 --market-date-to 2026-05-06 --agent-name technical-summarizer`
- `python3.11 -m app.internal.ops agent-signal-decider-evaluations-compute --market-date 2026-04-30 --horizons 1,3,5,10`
- `python3.11 -m app.internal.ops agent-signal-decider-evaluations-repair --market-date-from 2026-04-01 --market-date-to 2026-04-30 --horizons 1,3,5,10 --primary-horizons 5`
- `python3.11 -m app.internal.ops agent-signal-decider-execution-outcomes-backfill --market-date-from 2026-06-29 --market-date-to 2026-07-03`
- `python3.11 -m app.internal.ops agent-short-term-wait-conflict-replay --completed-weeks 4 --gate-mode downsize_probe --probe-quantity 3 --output-format markdown`
- `python3.11 -m app.internal.ops agent-signal-decider-calendar-refresh --market-date 2026-04-30 --lookback-days 30 --horizons 1,3,5,10 --primary-horizons 1,3,5,10`
- Offline decider rule replay before activation: `python3.11 -m app.internal.ops agent-decider-replay --market-date-from 2026-06-15 --market-date-to 2026-06-19 --agent-name short-term-decider --rule-config-json '{"rules":[{"name":"stale_watch_trigger_drift","predicates":[{"type":"trigger_price_drift","max_below_trigger_pct":0.03}]}]}' --fee-bps 10 --slippage-bps 5 --output-format markdown`
  - The replay command accepts `--rule-config-json` or `--rule-config-path`, optional `--symbols`, `--policy-versions`, `--horizon-days`, `--fee-bps`, and `--slippage-bps`.
  - Supported first-pass predicates include `trigger_price_drift`, `confidence`, `risk_flag`, `liquidity_bucket`, `factor_score_bucket`, `source_availability`, and generic `field` path checks.
  - Candidate predicates are evaluated only against persisted snapshot-time request/response/traceability/input-feature payloads. Realized outcomes are used only for baseline vs candidate P/L comparison.
  - Generated signal-decider gate candidates use only the canonical `signal_decider_rule_predicate_v1` root with exactly one non-empty `all` or `any` branch. They remain manual-only/inactive/ineligible until all explicit activation fields are approved. Live gate matching reconstructs factor, liquidity, price, and action-summary buckets from the persisted server request; model prose and response-only features cannot satisfy those predicates.
  - Policy activation uses the exact inputs in the candidate policy's stored `rule_bundle.replay_contract` and requires the complete JSON replay artifact in `replay_evidence`. Core recomputes only the candidate's base-policy snapshot cohort server-side; a manually edited result or different base policy/rule/symbol/window/horizon/cost is rejected.
  - Only `tighten_buy_gate` currently has an activation-safe replay. `relax_wait_gate` and `relax_reject_gate` remain fail-closed because the buy-only replay cannot yet price trades that the relaxed policy would add.
  - Regenerate legacy candidates whose rule bundle lacks `replay_contract`, `base_policy_version`, or the base-policy-only `policy_versions` cohort; activation intentionally rejects them.
- If reprocessing appends replacement `signal_decider_decision_snapshots` for an existing non-null `feedback_id`, rerun `agent-signal-decider-evaluations-repair` for the affected date range and then `agent-signal-decider-calendar-refresh`. Replay selects the latest such snapshot; null-feedback legacy snapshots remain independent.
- `python3.11 -m app.internal.ops kis-daily-signal-features-collect --market-date 2026-04-14`
- `python3.11 -m app.internal.ops kis-intraday-snapshots-collect --symbols 005930 --include-news true`
- `python3.11 -m app.internal.ops agent-derived-contexts-backfill-recent`
- `python3.11 -m app.internal.index_audit`

Performance audit note:

- Use `python3.11 -m app.internal.index_audit` when large-table reads suddenly slow down after storage moves, cache resets, or dataset growth.
- It reports the heaviest sequentially-read tables from `pg_stat_user_tables`, shows existing indexes, and prints simple index suggestions for common date-driven access patterns.
- Current known hot-path examples include `daily_price_bars`, `daily_technical_snapshots`, `daily_action_summaries`, and `dart_fundamentals_rows`.

Notes:

- Freshness summaries remain anchored to the representative same-date universe after a narrow retry. If the newest candidate run has fewer symbols than the stored same-date price-bar population, `/eod/freshness` selects a more complete same-date run by total or success count; a 1-symbol repair must not replace a 2,679-symbol daily summary.
- In `/eod/pipeline/status`, a child stage left `running` under a terminal official parent is normalized to `interrupted`, carries `details.orphaned_running_stage=true`, and is excluded from `running_stages`. PyKRX diagnostic category coverage and `decider_*` gate fields remain authoritative over stale official details, so a completed ready gate cannot reappear as waiting.
- These commands call repo services directly instead of looping back through `127.0.0.1:8000`, so they are safe when `UPSTREAM_BEARER_TOKEN` is enabled.
- If HTTP must remain for an internal localhost caller, use `app.internal.http_client.InternalAPIClient`, which automatically sends the configured bearer token.
- Historical PyKRX backfills support exactly one requested market date per call: `start_date` and `end_date` must match. Run an explicit date-by-date loop outside the service for a longer interval, and retain each response so failed dates can be retried independently.
- Historical runs always resolve membership for that requested date, even when `refresh_universe` is false. They do not use the current active table or KIS master as a fallback, and they abort before bar collection when any queried market is unavailable. This avoids current-universe leakage while date-by-date membership reconstruction remains unavailable.
- Historical runs accept only `price_collection`, `technical_snapshots`, and `action_summaries`. Current-only ranking, KIS signal/market-risk, and downstream decider/follow-up stages are rejected before provider I/O. Metadata uses `membership_basis=end_date_snapshot`; a global `limit` may intentionally leave later markets as `not_queried_due_to_limit`, which is not a source failure.
- An automatic or operator-triggered daily recovery for a prior KRX session is promoted to point-in-time recovery even though it still uses `daily_auto` orchestration. It requires an exact-date PyKRX membership proof whose outer date/source/basis and inner market digests all match the recovered date. Price and PyKRX enrichment writes may create historical identity references but must not reactivate, rename, or reclassify the current symbol master. Current-only master/universe, KIS ranking, DART refresh, news/macro, and intraday stages become auditable terminal safe-skips. KIS market-risk snapshot/filter stages have one bounded exception: during a trading day's `PREOPEN`, the immediately previous KRX session may be recollected and persisted before the open; older dates and every prior-date request from `OPEN` onward remain safe-skipped. Stored-price/technical/action/trending inputs, Piotroski, and factor stages may run only with date-bounded inputs; Piotroski fundamentals, audit, and dilution evidence uses disclosure receipt dates on or before the recovered market date and records the selected provenance.
- If a prior-date retry cannot prove exact-date membership, it records a durable bounded failed attempt instead of returning an untracked no-op. That makes the five-attempt budget converge and prevents one unverifiable date from starving every newer session forever.
- Daily/current PyKRX runs may refresh the active listed universe and use the current KIS master fallback. Historical bar persistence can register previously unknown symbols as inactive for foreign-key integrity, but it does not reactivate or overwrite the current symbol state.
- Automatic daily ingestion uses the same PyKRX universe/backfill persistence path, but only for the resolved market date and only once per day unless forced or retrying a partial/failed run. Each scheduler poll inspects the latest five KRX sessions and selects the oldest unresolved automatic lineage first; later manual rows do not hide that lineage. The scheduler carries both `trade_date` and the selected `eod_run_id` into the advisory lock. If that exact automatic row is missing or superseded by a newer automatic row, recovery returns `recovery_target_invalid_or_superseded` and does not fall through to a manual, historical, generic-latest, or fresh collection run. Price retries are capped at five automatic runs per trade date and retry only the symbols that still lack an exact-date bar. A capped row left `running` is still selected so stale-run reconciliation can finish; once the price budget is genuinely exhausted, the selector advances to a newer recoverable date instead of selecting a date the runtime can no longer progress.
- Price collection completion does not hide a failed downstream stage. On the next automatic check, Core reads the exact linked EOD and official daily-pipeline lineage and resumes from the first incomplete fundamentals/factor/news/feedback/decider/plan/evaluation stage without re-running price collection. If price failures and downstream gaps coexist, the durable `downstream_retry_symbols` scope is inherited across retries, validated against the exact-date membership proof, and resumed first without symbols that lack a same-day price bar. For scope diagnosis, let `M` be the admitted exact-date membership and `P` the persisted exact-date price symbols: `M - P` remains retry debt, `P - M` is recorded as unexpected and excluded from downstream work, and only the verified admitted scope in `M ∩ P` may proceed. An orphan's durable downstream scope must also be contained in the exact-date membership and global exact-date bar set before any decider or plan side effect runs. The link must agree across `eod_collection_run_id`, `reused_eod_collection_run_id`, and stage source-run IDs; ambiguous or conflicting links fail closed. Startup reconciliation preserves failed, unattempted, and downstream scopes, and an interrupted/orphan parent converges only after every persisted resolved stage has a terminal satisfied outcome. Follow-up-only attempts use a five-minute minimum interval and a five-attempt cap per exact EOD lineage; inspect the returned `reason` and `followup_recovery` fields when a run is waiting or exhausted.
- A signal-decider-only recovery must carry the exact completed summary child run, persisted `feedback_scope_mode`, financial-statement division, knowledge cutoff, and the symbol intersection actually completed by every summarizer from the parent pipeline or linked EOD stage progress. It never falls back to a generic latest summary when that proof is missing or conflicting. Decider child dispatch and its pre/post-dispatch generation revalidation resolve reused upstream summaries from that completed parent's immutable feedback ID/run/receipt evidence; a later failure tombstone from an abandoned sibling cannot hide the proven summary, while mismatched or incomplete evidence remains blocked. The normal five-attempt cap remains unchanged; only an exact zero-work `summary_run_missing` block produced by the retired BROAD lookup against a proven FAST summary may use one additional contract-repair attempt, reported as `followup_recovery.cutoff_contract_repair=true`. An interrupted zero-work repair atomically revalidates and locks the same summary source, then reclaims the same sixth pipeline row once; it does not create a seventh run or rebuild historical membership. Fundamental reuse fingerprints use the same canonical DART snapshot selection as dispatch, so a repaired upstream summary can make its dependent decider retry eligible without bypassing dependency readiness.
- Daily-pipeline admission permits only one active run per `market_date`, even when a new request names a non-overlapping suffix. A stale timestamp alone does not authorize a second worker; only startup reconciliation or a new automatic worker holding the same provider advisory fence may interrupt a stale, exact automatic lineage. Long factor calls maintain short isolated run/stage heartbeats and persist each factor transition as it occurs. Feedback or signal-decider suffix recovery is rejected before a run row is created unless omitted same-date structural prerequisites are ready; prerequisites scheduled earlier in the same recovery are rechecked immediately before feedback. `bypass_prerequisite_gate` does not override these requirements. If persisted timestamps show factors, signal features, or optional context completed after feedback began, recovery restarts the affected suffix and forces feedback recomputation instead of reusing the stale artifact.
- An exact automatic feedback rearm inherits both the original parent feedback run ID and its point-in-time knowledge cutoff. Reuse is accepted only from successful children linked to that exact parent; newer interrupted or abandoned retry lineages cannot shadow the proven parent or expand a one-symbol retry back to the full feedback universe. Missing or mismatched source identity fails closed before dispatch.
- OpenClaw feedback preflight fails closed when the gateway cannot be reached. During dispatch, configured retries are still exhausted for the affected batch, but a connection-level gateway outage or provider HTTP 429/rate-limit failure then opens a circuit for that run: already in-flight batches may finish, while queued batches and later roles are not submitted. Item-scoped model or response failures do not open this circuit, and the next independent run probes the gateway again.
- Treat computed `ready_count`/`unready_count` together with post-write `persisted_ready_count`/`persisted_unready_count` as the factor-stage completion contract. For a non-empty requested or matched scope, zero persisted-ready rows with full persisted-unready coverage is blocking even when more than 99% of rows were written; do not manually mark that stage complete from row counts. An explicit `next_open_execution_plan` request resolves `signal_decider` first unless it is a persisted-boundary resume. Factor history reads use an indexed per-symbol `LATERAL ... LIMIT` lookback, and factor, Piotroski, and market-risk result persistence uses bounded batches with single-row fallback, so operators should investigate provider latency or missing inputs before increasing database connection pools.
- The automatic pykrx daily ingestion path now also persists `daily_price_contexts`, `daily_pykrx_enrichment_rows`, `daily_technical_snapshots`, `daily_action_summaries`, and day-based `daily_trending_ranks` after the day bar write completes.
- Symbol-level stages only receive symbols whose price collection succeeded for that run. A symbol without a fresh bar remains in `price_retry_symbols`; price contexts, technical snapshots, and action summaries report it under `causally_skipped_symbols` instead of manufacturing stale inputs or duplicating the same upstream failure as downstream errors.
- PyKRX enrichment reports provider errors separately from final data gaps. A category is `recovered` when fallback or an earlier same-date write leaves full expected coverage. Only `blocking_failed_categories` keep the stage partial and enter retry scope; optional gaps and required-category gaps whose own gate is ready remain under `tolerated_failed_categories`. Inspect those fields together with `provider_error_categories`, `recovered_categories`, and raw `unresolved_categories` when diagnosing a run.
- Exec/decider readiness requires `sector_classification`, `investor_flow_symbol`, and `short_selling` at 99% expected coverage, plus direct same-day `valuation_snapshot` rows at 95% of the fresh PyKRX KOSPI/KOSDAQ stock universe. A valuation row must have positive market cap and at least one valuation metric. Inspect `warning_categories`, `complete_present_rows`, `missing_component_rows`, and `completeness_status` as well as the gate result; missing short-balance components can be a visible warning even when decision-usable short-volume coverage passes.
- DART capture retries a consolidated-statement `[013]` response as `OFS`, rejects all non-`000` or empty-success payloads before replacing snapshots, and isolates disclosure-sync failures per corporation. A real annual filing writes its audit opinion before its financial snapshot and is marked captured only after both succeed; missing or unparseable audit evidence leaves the event retryable. Piotroski reads captured-event snapshots and only an audit signal with the same `rcept_no`, preventing an older clean opinion from masking a newer unknown filing. Three consecutive systemic transport/provider failures trip a circuit breaker and report the remaining corporations as unattempted instead of serially waiting through a global outage; corporation-specific lookup failures remain isolated and do not trip it. A partial/failed sync must remain partial/failed in daily refresh and manual backfill; an incomplete manual batch keeps its current resume offset. Normal backfill applies `capture_limit` and reports remaining retryable events as partial work. Explicit `force=true` recaptures the full selected symbol/date scope without truncating it by `capture_limit`, because a limited forced query has no durable event cursor and could otherwise repeat the same newest events while falsely reporting completion; bound forced work with `symbol_limit` instead. With initial backfill disabled, no-fundamentals symbols are sampled by a deterministic rotating probe bounded by `DART_FUNDAMENTALS_DAILY_MISSING_SYMBOL_PROBE_LIMIT` instead of scanning the whole universe every day.
- Legacy captured annual fundamentals that predate the audit-opinion gate can be repaired without rerunning fundamentals or any agent stage. Run each KRX date independently and dry-run first, then repeat with explicit `--apply`. The command uses that date's `daily_price_bars` universe and the same point-in-time annual selection as Piotroski: receipt date on or before the market date, captured successful `11011` CFS/OFS snapshots, the latest correction per symbol/year with CFS preferred for the same filing, and the latest available business year. An existing audit row only suppresses its exact selected `rcept_no`. For forward paging, continue from `next_forward_after_event_id` while `forward_scan_has_more=true`; after the forward scan, rerun from cursor zero until `next_after_event_id=0` and `retry_event_ids` is empty. `unattempted_event_ids` are separate circuit-breaker work. Apply-side `partial` and `completed_with_gaps` results deliberately exit with code 2. Pages are capped at 100 events and each filing is bounded by `DART_FUNDAMENTALS_EVENT_TIMEOUT_SECONDS`.
- Deterministic repair tools use only `.venv/bin/python` and default to dry-run: `app.eod.missing_bar_repair` for one exact KRX date, `app.eod.symbol_history_repair` for at most ten explicit symbols and 450 calendar days, `app.eod.turnover_repair` for paired volume/turnover correction, and `app.eod.exact_factor_repair` for the nine stored-data factor stages. Back up and verify PostgreSQL before `--apply`. Stop Core and drain all other factor writers until exact-factor repair finishes; direct compute endpoints do not share its repair lock. Exact-factor apply must pin the reviewed dry-run's `exact_provider_membership_count` and `membership_sha256`; use `--expected-provider-membership-count`, `--expected-membership-sha256`, and only then optional `--prune-stale`. These paths never regenerate agent feedback or run current KIS, news, macro, decider, or execution stages. Repair rows deliberately use `eod_run_id=NULL`; validate by market date rather than rewriting the historical failed-run record.
- The same automatic daily path now also collects `daily_market_risk_snapshots` from KIS `inquire-price-2`, then computes `daily_market_risk_filter_results` from those stored snapshots for the successfully stored symbol universe. Per-symbol KIS failures are reported as partial errors and do not abort the rest of the daily run.
- KIS ranking signal-feature collection is a separate ops path because it calls KIS ranking endpoints (`volume-power`, `overtime-volume`, and `overtime-fluctuation`) rather than pykrx bars.
- The KIS ranking signal-feature collection path also rebuilds `daily_market_participation_contexts` from the stored `daily_signal_features` rows for that market date.
- `python3.11 -m app.internal.ops agent-derived-contexts-backfill-recent` is the repo-owned bootstrap path for the new derived-context tables. It backfills the recent seven trading days ending yesterday from existing stored `daily_price_bars` and `daily_signal_features`, and it skips participation dates that do not yet have stored signal-feature rows.
- Duplicate bar writes are harmless because `daily_price_bars` upserts on `(symbol_id, trade_date)`, and the scheduler checks `eod_collection_runs` plus a Postgres advisory lock before starting.
- When PyKRX market-wide universe endpoints are empty, only current/daily runs may fall back to the existing KIS master universe. Historical backfills fail closed instead.
- Historical bars use pykrx's default adjusted OHLCV series because long-range `adjusted=False` calls returned empty data during local verification.
- `--limit` runs do not deactivate missing symbols in the DB, so they are safe for small smoke tests.
- The historical PyKRX backfill writes inactive symbol identity rows when needed for foreign-key integrity, plus `daily_price_bars`, `eod_collection_runs`, `daily_price_contexts`, `daily_technical_snapshots`, and `daily_action_summaries`; it does not replace current active-universe membership or write current-only trending ranks.

Local overrides:

- Set `EOD_AUTO_PYKRX_ENABLED=false` in `apps/api/.env` to disable the recurring scheduler.
- Automatic collection defaults to `EOD_AUTO_PYKRX_MARKETS=KOSPI,KOSDAQ`. Add `KONEX` only after verifying that the provider returns a complete KONEX universe for the target session; an incomplete requested market fails closed instead of silently shrinking the universe.
- An incomplete-universe preflight failure enters exponential backoff for the same market date and exact market configuration (30, 60, 120, then at most 240 minutes). This prevents one-minute retry storms while still allowing same-day recovery when the provider feed returns. A successful run, the next market date, or a changed `EOD_AUTO_PYKRX_MARKETS` value clears the backoff.
- Tune `EOD_AUTO_PYKRX_AFTER_CLOSE_MINUTES`, `EOD_AUTO_PYKRX_POLL_SECONDS`, `EOD_AUTO_PYKRX_MARKETS`, `EOD_AUTO_PYKRX_REFRESH_UNIVERSE`, `EOD_AUTO_PYKRX_STALE_RUN_MINUTES`, and `EOD_PYKRX_REQUEST_TIMEOUT_SECONDS` in `apps/api/.env`, then restart the API.
- Tune `DART_FUNDAMENTALS_DAILY_MISSING_SYMBOL_PROBE_LIMIT` to bound the rotating daily check of active symbols that still have no stored fundamentals (`0` disables that probe; default `100`). This does not enable the separate full initial-backfill path.
- Use `SIGNAL_DECIDER_NEXT_OPEN_ENABLED`, `SIGNAL_DECIDER_NEXT_OPEN_DRY_RUN`, `SIGNAL_DECIDER_NEXT_OPEN_ACCOUNT_TARGETS`, `SIGNAL_DECIDER_NEXT_OPEN_MIN_CONFIDENCE_SCORE_10`, `SIGNAL_DECIDER_NEXT_OPEN_MIN_EXPECTED_RETURN_PCT`, `SIGNAL_DECIDER_NEXT_OPEN_MIN_REWARD_RISK_RATIO`, `SIGNAL_DECIDER_WAIT_CONFLICT_GATE_MODE`, `SIGNAL_DECIDER_WAIT_CONFLICT_PROBE_QUANTITY`, and the `SIGNAL_DECIDER_NEXT_OPEN_REQUIRE_*` / `...BLOCK_ON_*` guardrails in `apps/api/.env`, then restart the API. Keep the WAIT-conflict gate at `report_only` until the documented rolling replay promotion threshold and manual approval are satisfied; follow the documented rollback procedure for any trial.
- Use `SIGNAL_DECIDER_EXEC_INTEGRATION_MODE=disabled|observe|dispatch`, `SIGNAL_DECIDER_EXEC_BASE_URL`, optional `SIGNAL_DECIDER_EXEC_BEARER_TOKEN`, `SIGNAL_DECIDER_EXEC_RUNTIME_MODE`, and `SIGNAL_DECIDER_EXEC_REQUEST_TIMEOUT_SECONDS` in `apps/api/.env` to control the core-to-exec integration lane, then restart the API.
- In `dispatch` mode, eligible `signal-decider` next-open buys and actionable `short-term-decider` watcher candidates with `confidence_score_10 >= 6.0` are handed to Exec for account-aware sizing at/after 08:00 KST. Core sends budget/reference/target/stop/confidence/risk context and never grants executable fixed sizing; even a `downsize_probe` is only a `max_quantity` ceiling. Accepted rows remain delegated unless Exec returns a positive agent-approved quantity.
- The KRX next-open loader requires an explicit domestic market claim (`KRX`, `KOSPI`, `KOSDAQ`, or `KONEX`) in the persisted symbol context. Missing, US, unknown, or contradictory market claims are skipped rather than defaulted to KRX.
- Use `GET /eod/status` to inspect the scheduler state and last result.

## Audit logging

- Default log directory: `apps/api/var/log`
- Incoming API log file: `api-requests.log`
- Outbound integration log file: `upstream-http.log`
- Rotation uses `RotatingFileHandler` with `API_LOG_ROTATION_MAX_BYTES` and `API_LOG_ROTATION_BACKUP_COUNT`
- Logged fields are metadata only: request ID, method, path/target URL, redacted query params, status, timing, content type/length, client IP, user agent, and error type/message when a call fails
- Request and response bodies are omitted by default; secrets such as bearer tokens, API keys, app secrets, cookies, and token-like query params are redacted

Local validation:

1. From `apps/api`, start the API with `python -m uvicorn app.main:app --reload`
2. Call `curl -i -H "Authorization: Bearer <token>" http://127.0.0.1:8000/health` when `UPSTREAM_BEARER_TOKEN` is set
3. Verify portfolio wiring with `curl -H "Authorization: Bearer <token>" http://127.0.0.1:8000/portfolio/summary` after setting `PORTFOLIO_ACCOUNT_REFERENCES`
4. Trigger an upstream-backed route such as `curl -H "Authorization: Bearer <token>" "http://127.0.0.1:8000/fx/koreaexim/rates?search_date=20260406&currency=USD"` when bearer auth is enabled
5. Inspect `var/log/api-requests.log` and `var/log/upstream-http.log`

## Next ops step
Set this up under a long-running process manager (LaunchAgent, systemd, pm2, or equivalent).

## DART notes

- Set `DART_API_KEY` in `apps/api/.env` before calling DART-backed endpoints.
- `DART_CORP_CODE_CACHE_TTL_SECONDS` controls how long the in-memory corp-code mapping is reused before refreshing from Open DART.
- `POST /disclosures/dart/sync/events` polls `list.json` over a rolling recent window and upserts by `rcept_no` into the disclosure-event tables.
- The DART machine-DB path now stores only periodic financial-report disclosures; non-financial DART events are intentionally skipped during persistence.
- `GET /disclosures/dart/sync/inspect` returns the latest sync run, recent runs, aggregate counts, and recent stored events.
- `POST /disclosures/dart/fundamentals/capture` consumes financial-report disclosure events, infers report-period identity from the filing name, fetches `fnlttSinglAcntAll`, and persists raw snapshots plus normalized rows/highlights.
- `GET /disclosures/dart/fundamentals/capture/inspect` returns the latest capture run, recent capture runs, capture summary counts, and recent financial events with capture state.
- `POST /disclosures/dart/fundamentals/backfill/current-universe` batches over the current active symbol universe, clamps the start boundary to 2015-01-01 or later, syncs only financial report events for that slice, and then captures only that slice's pending/failed fundamentals.
- DART fundamentals capture statuses are operationally meaningful: `pending` and `failed` are retryable; DART `[013]` financial-statement no-data responses become terminal `no_data`; filing/request inference problems become terminal `mapping_failed`.
- `POST /disclosures/dart/fundamentals/daily/run-if-due` is the steady-state incremental refresh path: for each selected active symbol it checks the latest resolved fundamentals receipt date, syncs only newer DART financial disclosures for that symbol, captures exact pending/failed event IDs for stale or missing symbols, and skips an already completed market date unless `force=true`. Limited daily runs prefer retryable events with fewer attempts and older receipt dates first.
- The daily fundamentals refresh path now also computes and upserts `daily_piotroski_filter_results` for the same market date. It stores one row for every evaluated symbol, including both passed and not-passed results, and returns `piotroski_filter` plus `piotroski_filter_error` in the refresh response.
- `GET /disclosures/dart/fundamentals/daily/inspect` reads persisted `dart_fundamentals_refresh_runs` summaries, including counts, sync run IDs, capture run IDs, and bounded symbol/event samples for operator review.
- Daily refresh and backfill results include `operator_buckets` so lookup/mapping errors such as `Unknown stock_code`, retryable capture failures, terminal no-data events, and remaining retryable backlog can be reviewed separately.
- For shell-driven continuation batches, prefer `python3.11 -m app.dart.backfill_current_universe ...` or keep the full `curl` URL quoted. Unquoted `?` / `&` query strings can be eaten by the shell before the API request is sent.
- For unattended overnight runs, use `python3.11 -m app.dart.backfill_current_universe_runner ...`. It owns batch iteration, writes JSONL progress records to a log file, and exits non-zero on failure after logging the failing offset.
- Open DART filing search (`list.json`) and `fnlttSinglAcntAll` practical support starts in 2015, so `daily_price_bars` data before 2015 does not imply DART fundamentals availability before 2015.
- `GET /disclosures/dart/filings/document` streams the original filing package returned by Open DART `document.xml`; the response is binary and intended for agent-side parsing, not JSON inspection.
- `GET /disclosures/dart/filings/xbrl` streams the XBRL package returned by Open DART `fnlttXbrl.xml`; pass `report_code` when the filing package contains multiple regular-report windows.
- `DART_SYNC_RECENT_DAYS` sets the default rolling window size for the event sync, and `DART_SYNC_PAGE_COUNT` controls DART `list.json` page size.
- `DART_FUNDAMENTALS_MIN_BUSINESS_YEAR` defaults the fundamentals backfill floor to `2015`.
- `DART_FUNDAMENTALS_DEFAULT_REPORT_CODE` defaults the fundamentals slice to `11011` (annual report).
- `DART_FUNDAMENTALS_DEFAULT_FS_DIV` defaults the fundamentals slice to `CFS` (consolidated statements).
- `DART_FUNDAMENTALS_EVENT_TIMEOUT_SECONDS` bounds one fundamentals-capture event before it is marked failed and the batch moves on.
- `DART_FUNDAMENTALS_STALE_RUN_MINUTES` marks abandoned `running` capture runs as failed before a new capture batch starts.

## Korea Exim FX notes

- Set `KOREAEXIM_API_KEY` in `apps/api/.env` before calling the Korea Exim FX route.
- `GET /fx/koreaexim/rates` currently targets the Korea Exim `AP01` daily exchange-rate slice only.
- Optional query parameters: `search_date=YYYYMMDD`, `currency=USD`, and `fallback_days=0..10`.
- `KOREAEXIM_FALLBACK_DAYS` controls how many prior days the service searches when the requested date is a weekend or holiday.

## Market snapshot notes

- `GET /market/snapshot` is a production-facing dashboard dependency and now resolves KIS reads against the real domain by default, even when the broader process still uses `KIS_USE_MOCK=true` for other flows.
- During an open session, a single transient master/snapshot/plan-refresh exception stops that cycle from creating new BUY authority but does not revoke already delegated BUY watches. Revocation is staged after three consecutive failures of the same critical step; one successful run resets that step's failure streak. This preserves fail-closed dispatch while preventing a one-off KIS transport failure from collapsing the live watch set.
- Keep `MARKET_SNAPSHOT_KIS_USE_REAL=true` unless you intentionally want deploy preflight and smoke-check to fail the market snapshot path.
- Required deploy-time secrets for the route are `KIS_APP_KEY`, `KIS_APP_SECRET`, and `KOREAEXIM_API_KEY`.
- Use `GET /broker/kis/health` to inspect `market_snapshot_runtime` when deploy preflight or smoke-check reports a market snapshot readiness failure.

## ECOS notes

- Set `BOK_ECOS_API_KEY` in `apps/api/.env` before calling ECOS-backed endpoints.
- `BOK_API_KEY` and `BOK_BASE_URL` are still accepted as legacy fallback names, but `BOK_ECOS_API_KEY` and `BOK_ECOS_BASE_URL` are the canonical settings.
