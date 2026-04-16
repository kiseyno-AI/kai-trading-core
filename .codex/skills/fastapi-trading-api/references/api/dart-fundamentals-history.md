# GET /disclosures/dart/fundamentals/history

## Summary
Reads stored DART fundamentals history for one or more symbols.

## Example request
```http
GET /disclosures/dart/fundamentals/history?symbols=005930&symbols=000660&start_date=2024-01-01&end_date=2025-12-31&fs_div=CFS
```

## Example response
```json
{
  "items": [{"symbol": "005930", "snapshots": []}]
}
```
