# POST /eod/pykrx/universe/refresh

## Summary
Refreshes the pykrx-backed symbol universe for the selected markets.

## Example request
```http
POST /eod/pykrx/universe/refresh?markets=KOSPI,KOSDAQ
```

## Example response
```json
{
  "status": "ok",
  "markets": ["KOSPI", "KOSDAQ"],
  "symbols_updated": 4331
}
```
