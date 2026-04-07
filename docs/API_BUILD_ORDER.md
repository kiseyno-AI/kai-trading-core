# API Build Order

## Implemented order
1. KIS auth token issuance
2. KIS domestic quote route
3. BOK/ECOS macro series route
4. Korea Exim FX daily exchange-rate snapshot route
5. DART company snapshot route
6. DART single-company fundamentals route

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
- `GET /eod/status`
- `POST /eod/master/refresh-if-due`
- `POST /eod/capture-if-due`
- `POST /eod/capture?market_date=YYYY-MM-DD`
- `POST /eod/pykrx/universe/refresh?markets=KOSPI,KOSDAQ,KONEX`
- `POST /eod/pykrx/backfill?start_date=19560101&end_date=YYYYMMDD`
- `POST /eod/pykrx/daily/run-if-due?market_date=YYYY-MM-DD`

## Next routes
- DART single-company financial statements or key financial indicators persisted from synced financial-report events
- DART filing document/original attachment access
- KIS balance inquiry
- KIS account positions
