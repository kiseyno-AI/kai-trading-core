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
   - `KIS_REALTIME_ORDER_NOTICE_ENABLED=false`
   - `KIS_REALTIME_PRICE_TR_ID=H0STCNT0`
   - `KIS_REALTIME_PRICE_MAX_SUBSCRIPTIONS=40`
   - `KIS_REALTIME_PRICE_STALE_AFTER_SECONDS=30`
   - KIS permits one session and 41 combined realtime registrations, including trade feeds, order books, and execution notices. Core's current price and order-notice consumers open separate sockets: enable only one for the same allowance. With the price consumer enabled and the separate notice consumer disabled, `KIS_REALTIME_PRICE_MAX_SUBSCRIPTIONS=41` is usable if no other registration consumes the allowance. The default 40 is a local setting, not KIS's published maximum. Do not count 41 registrations as 41 symbols when receiving both trades and books. See the [official quota and deployment evidence](research/kis-websocket-subscription-limits-2026-11-12.md).
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

## Overseas index daily data: first integration slice

`KISClient.fetch_overseas_index_daily_prices(index_code=..., start_date=..., end_date=...)`
reads `/uapi/overseas-price/v1/quotations/inquire-daily-chartprice` with
`FHKST03030100`, market code `N` and daily period `D`. Dates must be valid
`YYYYMMDD` strings in ascending order; invalid inputs fail before a provider call.
It reuses the existing client authentication, pacing, timeout and error mapping.
It adds no retry loop, HTTP route, scheduler, persistence, signal or order behavior.

```python
payload = await client.fetch_overseas_index_daily_prices(
    index_code='SPX', start_date='20261120', end_date='20261124',
)
```

The official index master and one read-only request per index on 2026-11-24
confirmed `SPX` (S&P500), `COMP` (Nasdaq Composite) and `SOX` (PHLX Semiconductor).
Use master codes rather than assuming the dotted code in older examples is the
required spelling. [Official master helper](https://github.com/koreainvestment/open-trading-api/blob/main/stocks_info/overseas_index_code.py),
[official API example](https://github.com/koreainvestment/open-trading-api/blob/main/examples_llm/overseas_stock/inquire_daily_chartprice/inquire_daily_chartprice.py).

The method returns a complete single-page provider payload unchanged, including
`output1` and `output2`; an empty result stays empty. If KIS reports more pages
(`tr_cont=M/F`), it raises `KISClientError` instead of returning partial history;
request a shorter date range. It makes no automatic continuation requests.
A response may include the **current, unfinished US session** alongside the last
completed one. The three verification
responses contained both November 24 and November 23 rows. This proves retrieval,
not before-KRX publication timing, completed-session selection or full history.
Do not treat the first row or a successful response as a ready trading signal.
Calendar selection and exact-row validation are provided below; immutable
storage remains a separate follow-up slice.

## Completed US session calendar: second integration slice

`app.market.us_session.latest_completed_us_session(as_of=...)` is a pure calendar
lookup. It requires an explicit timezone-aware `datetime` and returns a frozen
`CompletedUSSession` with `session_date`, `close_at`, `previous_session_date` and
`previous_close_at`. Dates are XNYS session labels; both close timestamps are UTC.

The candidate is based on the New York civil date, then checked against its
scheduled exchange close. A close is eligible when `close_at <= as_of`, including
the exact closing instant without rounding away microseconds. Before that instant
the preceding session is selected. XNYS supplies holidays, early closes and DST;
there is no fixed UTC closing hour or weekday fallback. Naive/invalid inputs and
dates outside the installed calendar's supported range raise `ValueError`.

```python
from datetime import datetime
from app.market.us_session import latest_completed_us_session

session = latest_completed_us_session(
    as_of=datetime.fromisoformat('2026-11-24T07:00:00+09:00'),
)
# session_date: 2026-11-23; close_at: 2026-11-23T21:00:00+00:00
# previous_session_date: 2026-11-20
payload = await client.fetch_overseas_index_daily_prices(
    index_code='SPX',
    start_date=session.previous_session_date.strftime('%Y%m%d'),
    end_date=session.session_date.strftime('%Y%m%d'),
)
```

Verified calendar cases include Thanksgiving 2026-11-26 (closed), Black Friday
2026-11-27 (13:00 New York / 18:00 UTC close), and the March/November DST changes.
The implementation uses the existing `exchange_calendars` dependency (verified
with 4.13.2). [Versioned calendar API](https://github.com/gerrymanoim/exchange_calendars/blob/4.13.2/exchange_calendars/exchange_calendar.py),
[NYSE hours and holidays](https://www.nyse.com/trade/hours-calendars),
[NIST DST dates](https://www.nist.gov/pml/time-and-frequency-division/popular-links/daylight-saving-time-dst).

**Scheduled closure is not proof that KIS has published a final bar.** These dates
are request bounds only. The validator below checks row identity, dates and prices;
publication readiness and collection provenance require separate evidence.
This function performs no provider or database I/O and adds no scheduler, route,
pipeline gate, signal or order behavior.

## Exact US index prices: third integration slice

`app.market.us_index_prices.validate_kis_us_index_prices(payload, index_code=...,
session=...)` validates a successful response from the first-slice KIS client.
Pass the `CompletedUSSession` produced by `latest_completed_us_session` with the
collector's explicit timestamp. The returned frozen `USIndexSessionPrices`
contains the normalized `index_code`, that `session`, `price` and `previous_price`.

- The supported master codes are `SPX`, `COMP` and `SOX`. A nonempty response must
  identify the requested index in `output1.stck_shrn_iscd` and have `rt_cd="0"`.
- Prices come only from the two exact `output2.stck_bsop_date` matches, using each
  row's `ovrs_nmix_prpr`. Row order is irrelevant. Other dated rows, including an
  unfinished current session, do not supply prices. Header current/prior prices
  and percentages are not used for historical session values.
- Prices must pass the existing finite-number admission check and be positive.
  They are returned as `Decimal` values parsed from the original representation,
  preserving decimal-string precision without rounding. Proper thousands
  separators are accepted; malformed grouping, booleans, blanks, NaN, infinity,
  zero and negative values are rejected.
- A successful empty `output2` list/object or absent required session raises
  `USIndexPricesUnavailable` (a `LookupError`) listing the missing dates. No older
  date, synthetic zero or sibling index substitutes for a missing value. Both list and single-object row shapes
  are understood; one row alone cannot satisfy two required dates.
- Malformed response containers, non-success result codes, identity mismatches,
  invalid business-date strings, invalid required prices and duplicate required
  dates raise `ValueError`. Every row must have a valid `YYYYMMDD` business date;
  prices are checked only for the two required dates. Duplicate required dates
  are rejected even when their prices agree.

```python
from app.market.us_index_prices import validate_kis_us_index_prices

prices = validate_kis_us_index_prices(payload, index_code='SPX', session=session)
# prices.price / prices.previous_price: exact provider Decimal values
# prices.session: the same calendar window used for the request
```

The field mapping is documented in the [official KIS example](https://github.com/koreainvestment/open-trading-api/blob/main/examples_llm/overseas_stock/inquire_daily_chartprice/chk_inquire_daily_chartprice.py).
That example does not establish a publication timestamp or final-bar guarantee;
`mod_yn` is a modification flag, not proof of finality. Validation certifies the
selected values' structure and consistency only. The caller must still establish
when the response was collected and whether it was available before its cutoff.
The validator performs no I/O, does not mutate the payload and adds no retries,
storage, HTTP route, scheduler, daily-pipeline gate, trading signal or order.

## Durable US index inputs: fourth integration slice

`app.market.us_index_snapshot_store` provides `save_us_index_snapshot` and
`load_us_index_snapshot`. Apply migration `20261124_0091` before using this store.
It adds only `us_index_input_snapshots`; existing market, daily and order tables
are unchanged. The store uses Core's existing PostgreSQL connection/session scope.

Saving requires the original decoded KIS response, requested index, explicit
aware `as_of` and `collected_at`, and `source_environment="real"` or `"mock"`.
`as_of` fixes the requested calendar window; `collected_at` records the caller's
response-receipt time and cannot precede `as_of`. Both are normalized to UTC.
The store freezes the JSON response before validating the two exact prices, then
records its canonical JSON/hash, exact decimal strings, frozen session dates and
closes, and installed calendar version. Reads restore these stored values without
fetching quotes or recalculating historical calendar rules.

```python
from datetime import UTC, datetime
from app.market.us_index_snapshot_store import load_us_index_snapshot, save_us_index_snapshot

as_of = datetime.now(UTC)
session = latest_completed_us_session(as_of=as_of)
payload = await client.fetch_overseas_index_daily_prices(
    index_code='SPX',
    start_date=session.previous_session_date.strftime('%Y%m%d'),
    end_date=session.session_date.strftime('%Y%m%d'),
)
collected_at = datetime.now(UTC)
saved = save_us_index_snapshot(
    payload, index_code='SPX', as_of=as_of, collected_at=collected_at,
    source_environment='mock' if client.use_mock else 'real',
)
restored = load_us_index_snapshot(saved.snapshot_id)
```

The SHA-256 snapshot ID includes the normalized timestamps, environment, source
response, validated values and calendar/schema versions. Repeating the same
capture under the same versions returns the original record, including under
concurrent writes; a later capture, changed response or new calendar version
creates another record. Source hashes describe canonical decoded JSON, not the
original HTTP bytes. Text storage preserves decimal strings and hash input across
database round trips. PostgreSQL guards reject UPDATE, DELETE and TRUNCATE on the
new table. Downgrading this migration removes its snapshot history.

`created_at` is a database-generated transaction/insertion timestamp outside the
input hash, so retries do not rewrite it. It is not a commit or publication time.
The returned frozen record includes prices, all capture timestamps, environment,
source JSON/hash and calendar version. Unknown IDs return `None`; hash or indexed
metadata mismatches raise `ValueError`. Missing/invalid source data is rejected
before opening a database session, and storage failures are not converted into
in-memory success.

Receipt time/environment are caller-supplied provenance, not a KIS final-bar
attestation. The default Core session scope commits before returning; callers
injecting their own transaction scope own its commit. Future cutoff selection
must still freeze successfully committed snapshot IDs on time.
This slice adds no provider calls, automatic collector, route, scheduler,
daily-pipeline gate, scoring or order behavior. Existing optional-process failure
isolation must be preserved when collection is wired in a later slice.

## Manual US index collector: fifth integration slice

`app.market.us_index_collector.collect_us_index_inputs(as_of=..., index_codes=...)`
connects the client, completed-session selector, validator and durable store.
`as_of` must be timezone-aware and fixes one session window for the entire batch,
even if collection crosses the next US close. The supported selection defaults
to `SPX,COMP,SOX`; a subset supports retrying only failed indices. Codes are
normalized and deduplicated before any requests. Invalid selections/timestamps
are rejected before provider or database access.

The default is the existing shared `kis_real_client`: real-domain read-only
quotes even when orders use mock mode. Each selected index is requested once,
sequentially, with the two calendar dates as bounds. Existing KIS token caching,
pacing and transport limits apply; the collector adds no retry or continuation
loop. It records UTC receipt time immediately after each response and derives
`real`/`mock` from the actual client's `use_mock`. Valid captures are committed
individually through the existing store, off the event-loop thread.

Run from `apps/api`, with configured KIS credentials, Core database access and
migration `20261124_0091` already applied:

```sh
python -m app.internal.us_index_collect
# Retry only COMP for the same frozen session selection:
python -m app.internal.us_index_collect --indices COMP --as-of 2026-11-24T07:00:00+09:00
```

The CLI prints JSON containing `as_of`, both US session dates,
`source_environment`, `counts` (`saved`, `unavailable`, `failed`) and an `indices`
list. Saved entries include `index_code`, `status="saved"`, `snapshot_id` and
`collected_at`. A successful empty response or absent required date is
`status="unavailable", stage="validate", reason="missing_session_prices"`,
with no fabricated snapshot. Technical errors are `status="failed"` with
`stage="fetch"|"validate"|"store"` and `error_type`; exception messages, raw
responses and credentials are not emitted. A storage lookup error remains a
storage failure, not provider unavailability. Other indices still run, and
previously committed captures remain readable by ID. Cancellation propagates.
Only `USIndexPricesUnavailable` is normal missing data; unexpected `KeyError`,
`IndexError` or other lookup failures during validation remain technical failures.

Batch `status="completed"` means no technical failures, even if prices are
unavailable; `data_complete` is true only when every selected index was saved.
Any technical failure yields `partial` (other indices handled) or `failed` (all
failed). CLI exit codes are 0 for completed, 1 for partial/failed and 2 for bad
arguments. Rerunning a subset makes a new capture with a new receipt time; it
does not rewrite earlier evidence or rerun successful indices automatically.

This command neither applies migrations nor schedules itself. It adds no HTTP
route, daily-pipeline dependency, cutoff selection, signal, score or order.
Receipt time and structurally valid bars still do not prove provider finality.

## Independent preopen scheduling: sixth integration slice

The opt-in standalone `app.internal.us_index_preopen` command adds durable
07:00/07:05/07:10 KST attempts, failed/unavailable-index-only recovery and a
07:12 cutoff. It reuses the shared real quote client and existing snapshot table;
no daily-runner or trading caller is added. The manual command above retains its
default behavior. Programmatic collectors may pass an aware `deadline` after
`as_of` to reject new requests or responses at/after that boundary.
See [schedule, recovery, budgets and launchd installation](US_INDEX_PREOPEN_COLLECTION.md).

## Opt-in full US-index history: ninth integration slice

`include_history=True` / `--include-history` on the existing collection, preopen
and feature paths adds 22 exact completed US-session closes. It widens the same
single request, shares preopen claims and cutoff, stores schema-2 snapshots and
uses a separate history seal. All two-close defaults remain unchanged. Missing
required dates remain unavailable; the official samples do not guarantee one-page
completeness. See [commands, official sources and rollback](US_INDEX_HISTORY.md).

## Notes
- Do not commit real secrets
- The token route returns presence, cache, expiry, type, and safe defer metadata only. It never returns the raw KIS access token and is disabled when `UPSTREAM_BEARER_TOKEN` is unset.
- Start with mock trading base URL first
- `GET /market/snapshot` is the exception to the general mock-first guidance: it uses real-domain KIS market data by default because the selected index and investor-flow APIs do not exist on the mock domain. Keep `MARKET_SNAPSHOT_KIS_USE_REAL=true` and set `KOREAEXIM_API_KEY` alongside the KIS credentials when you need that route.
- KRX open-day checks use KIS `chk-holiday` `opnd_yn` as the primary truth with a small in-process TTL cache. If KIS calendar data is unavailable, read-only market/status views can fall back to `exchange_calendars` with explicit `calendar_source` metadata; those non-authoritative results use a separate 60-second per-date cache to prevent retry storms and strict live paths continue to fail closed. Live order submission/dispatch paths fail closed unless an operator uses the existing `force=true` override.
- The intraday snapshot path is separate from PyKRX/DART EOD jobs. When KIS credentials are missing or `KIS_INTRADAY_SNAPSHOT_ENABLED=false`, the collect route returns `status=disabled` instead of failing or trying partial live work.
- Core opens one KIS websocket, subscribes to current monitored symbols with bounded/deduplicated updates, keeps latest ticks in process memory, and serves web clients through Core routes. WAIT rows are not re-evaluated from ticks. Once Core and Exec have accepted a complete trigger-gated plan, Exec observes the subscribed price and applies the trigger/chase and safety gates without calling a decider again. Do not persist the raw tick stream or let browsers open KIS sockets directly.
- `KIS_REALTIME_PRICE_ENABLED=true` means the feature is configured, not that a socket is always active. Core requests a realtime approval key and opens the price websocket only during a KIS-calendar-verified open KRX trading session. Closed, weekend, holiday, fallback-calendar, and unknown calendar states stay disconnected and report `market_session_active=false`; base holding, active-plan, and Exec monitoring membership remains available for the next open. `market_session_active` is session eligibility, while `websocket_connected=true` plus a `connected`, `subscribed`, or `limited` status proves an actual socket.
- Monitored-symbol source refresh is time-bounded. A stalled dashboard/read-model refresh preserves the last observed symbol set, records a retryable health error, and respects the configured refresh interval instead of preventing websocket startup or retrying in a tight loop.
- KIS's official open-trading-api `ccnl_krx` sample uses `H0STCNT0` for domestic realtime transaction prices in both real and mock environments. Keep `KIS_REALTIME_PRICE_TR_ID=H0STCNT0` unless KIS publishes a replacement.
- Add account, quote, balance, and order adapters after token flow is confirmed
