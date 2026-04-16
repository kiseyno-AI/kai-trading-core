# POST /disclosures/dart/fundamentals/backfill/current-universe

## Summary
Batch backfills fundamentals for the current active symbol universe.

## Example request
```http
POST /disclosures/dart/fundamentals/backfill/current-universe?symbol_limit=25&symbol_offset=0&capture_limit=250&fs_div=CFS
```

## Example response
```json
{
  "status": "ok",
  "symbol_limit": 25,
  "symbol_offset": 0,
  "capture_limit": 250,
  "captured": 21
}
```
