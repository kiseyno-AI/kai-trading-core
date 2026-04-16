# GET /scanner/realtime-trending

## Summary
Reads request-time KIS realtime trending buckets and merged symbol items.

## Example request
```http
GET /scanner/realtime-trending?limit=20&market_code=J&sources=volume-rank,fluctuation,hts-top-view
```

## Example response
```json
{
  "sources": {},
  "items": []
}
```
