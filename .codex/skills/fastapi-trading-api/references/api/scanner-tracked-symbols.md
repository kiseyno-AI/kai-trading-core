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
  "symbols": ["005930", "000660"],
  "count": 2
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
  "symbols": ["005930", "000660", "012450"],
  "count": 3
}
```
