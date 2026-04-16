# GET /scanner/tracked-symbols and POST /scanner/tracked-symbols

## Summary
Reads or replaces the tracked symbol list used by scanner/quote surfaces.

## Example read request
```http
GET /scanner/tracked-symbols
```

## Example read response
```json
{
  "tracked_symbols": ["005930", "000660"],
  "count": 2,
  "last_updated_at": "2026-04-16T07:12:00Z"
}
```

## Example replace request
```http
POST /scanner/tracked-symbols
Content-Type: application/json

{
  "symbols": ["005930", "000660", "012450"]
}
```

## Example replace response
```json
{
  "tracked_symbols": ["005930", "000660", "012450"],
  "count": 3,
  "last_updated_at": "2026-04-16T07:13:00Z"
}
```
