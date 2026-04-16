# POST /disclosures/dart/fundamentals/capture

## Summary
Captures fundamentals from already-synced disclosure events.

## Example request
```http
POST /disclosures/dart/fundamentals/capture?limit=20&fs_div=CFS
```

## Example response
```json
{
  "status": "ok",
  "capture_limit": 20,
  "captured": 18,
  "failed": 2
}
```
