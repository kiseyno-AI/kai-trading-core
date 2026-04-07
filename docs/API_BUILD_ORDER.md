# API Build Order

## Implemented order
1. KIS auth token issuance
2. KIS domestic quote route
3. BOK/ECOS macro series route
4. Korea Exim FX daily exchange-rate snapshot route
5. DART company snapshot route
6. DART single-company fundamentals route
7. DART fundamentals incremental capture from synced disclosure events
8. DART current-active-universe fundamentals backfill operator

## Current routes
- `POST /broker/kis/token`
- `GET /broker/kis/health`
- `GET /broker/kis/quote?symbol=005930`
- `GET /macro/ecos/series?stat_code=...&cycle=...&start_period=...&end_period=...&item_code_1=...`
- `GET /macro/ecos/catalog`
- `GET /macro/bok/series?series_id=...&cycle=...&start_period=...&end_period=...&item_code_1=...`
- `GET /fx/koreaexim/rates?search_date=YYYYMMDD&currency=USD&fallback_days=2`
- `GET /disclosures/dart/company-snapshot?stock_code=005930`
- `POST /disclosures/dart/sync/events?days=7`
- `GET /disclosures/dart/sync/inspect?runs_limit=10&events_limit=25&financial_only=false`
- `GET /disclosures/dart/fundamentals?stock_code=005930&fiscal_year=2025&period=annual`
- `GET /disclosures/dart/fundamentals?stock_code=005930&business_year=2025&report_code=11011&fs_div=CFS`
- `POST /disclosures/dart/fundamentals/capture?limit=20&force=false`
- `GET /disclosures/dart/fundamentals/capture/inspect?runs_limit=10&events_limit=25&capture_status=failed`
- `POST /disclosures/dart/fundamentals/backfill/current-universe?symbol_limit=25&symbol_offset=0&capture_limit=250`
- `GET /eod/status`
- `POST /eod/master/refresh-if-due`
- `POST /eod/capture-if-due`
- `POST /eod/capture?market_date=YYYY-MM-DD`
- `POST /eod/pykrx/universe/refresh?markets=KOSPI,KOSDAQ,KONEX`
- `POST /eod/pykrx/backfill?start_date=19560101&end_date=YYYYMMDD`
- `POST /eod/pykrx/daily/run-if-due?market_date=YYYY-MM-DD`

## DART pipeline notes
- Practical lower bound for `fnlttSinglAcntAll` and `list.json` in this pipeline is 2015 onward. Open DART documents both the filing-search date range and the financial-statement `bsns_year` support as 2015 and later, so the DB `daily_price_bars` minimum of `2007-06-05` does not expand the usable fundamentals backfill window.
- Ongoing machine-DB sync now persists only periodic financial-report events (`사업보고서`, `반기보고서`, `분기보고서`). Non-financial disclosures are still fetched from Open DART during polling, but they are not inserted into `dart_disclosure_events`.
- The current-universe backfill operator is intentionally batch-oriented. Use the current active symbol universe only, slice with `symbol_limit` and `symbol_offset`, sync only financial report events for that slice, then capture only those symbols' pending/failed fundamentals.

## Next routes
- DART filing document/original attachment access
- KIS balance inquiry
- KIS account positions
