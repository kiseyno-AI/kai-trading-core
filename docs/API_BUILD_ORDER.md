# API Build Order

## Implemented order
1. KIS auth token issuance
2. KIS domestic quote route
3. BOK/ECOS macro series route
4. Korea Exim FX daily exchange-rate snapshot route
5. DART company snapshot route
6. DART single-company fundamentals route
7. DART fundamentals incremental capture from synced disclosure events
8. DART filing original-document download route
9. DART filing XBRL download route

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
- `GET /disclosures/dart/filings/document?rcept_no=20260407000001`
- `GET /disclosures/dart/filings/xbrl?rcept_no=20260407000001&report_code=11011`
- `GET /eod/status`
- `POST /eod/master/refresh-if-due`
- `POST /eod/capture-if-due`
- `POST /eod/capture?market_date=YYYY-MM-DD`
- `POST /eod/pykrx/universe/refresh?markets=KOSPI,KOSDAQ,KONEX`
- `POST /eod/pykrx/backfill?start_date=19560101&end_date=YYYYMMDD`
- `POST /eod/pykrx/daily/run-if-due?market_date=YYYY-MM-DD`

## DART prioritization

### High value now
- Filing original-document download via `document.xml`: lets agents pull the source filing package for parsing, excerpt extraction, and reconciliation against synced disclosure events.
- Filing XBRL download via `fnlttXbrl.xml`: pairs naturally with the existing fundamentals flow when a deeper statement parse is needed beyond `fnltt_singl_acnt_all`.
- Corp-code mapping robustness: keep corp-code refresh trustworthy because every DART workflow depends on correct corp lookup; current TTL cache is sufficient for now, but refresh/inspect controls are the next infrastructure slice if lookup drift appears.

### Useful later
- `fnltt_multi_acnt`: useful for screener-style multi-name ranking when we have a clearer batch/snapshot contract and persistence target.
- `alot_matter`, `majorstock`, `elestock`, `stock_totqy_sttus`, `tesstk_acqs_dsps_sttus`, `mrhl_sttus`: materially relevant for dividend policy, ownership change, float, and buyback workflows, but better added as curated analytics slices rather than raw endpoint sprawl.
- `emp_sttus`, `exctv_sttus`, `outcmpny_drctr_nd_change_sttus`: useful for labor intensity, executive churn, and governance context once we define stable normalized payloads.

### Low priority for now
- Audit-fee, debt-security, capital-use, and other narrow regular-report tables: these are available in Open DART, but they add payload breadth faster than agent value for the current trading/analysis backend.
- Raw taxonomy endpoints without a consuming workflow: useful only when we commit to richer XBRL normalization.

## Next routes
- DART corp-code refresh/inspect operator if lookup freshness becomes an operational issue
- Curated DART ownership/dividend slices built on top of `majorstock` or `alot_matter`
- KIS balance inquiry
- KIS account positions
