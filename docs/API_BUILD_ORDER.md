# API Build Order

## Implemented order
1. KIS auth token issuance
2. KIS domestic quote route
3. BOK macro series route
4. Korea Exim FX rates route

## Current routes
- `POST /broker/kis/token`
- `GET /broker/kis/health`
- `GET /broker/kis/quote?symbol=005930`
- `GET /macro/bok/series?series_id=...&start_period=...&end_period=...`
- `GET /fx/koreaexim/rates?search_date=YYYYMMDD`

## Next routes
- KIS balance inquiry
- KIS account positions
- KIS order request skeleton
- News provider abstraction
