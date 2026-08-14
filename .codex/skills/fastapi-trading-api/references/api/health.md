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
    "error": null,
    "pool": {
      "configured_pool_size": 10,
      "configured_max_overflow": 10,
      "configured_capacity": 20,
      "checked_in": 1,
      "checked_out": 0,
      "overflow": -9
    }
  }
}
```

The database check fails fast when every configured pool slot is checked out.
The pool snapshot contains no credentials and is intended for outage diagnosis.
