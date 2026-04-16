# GET /health

## Summary
Simple liveness and dependency check for the core API.

## Example request
```http
GET /health
```

## Example response
```json
{
  "status": "ok",
  "app": "kai-trading-core-api",
  "env": "development",
  "database": {
    "connected": true,
    "error": null
  }
}
```
