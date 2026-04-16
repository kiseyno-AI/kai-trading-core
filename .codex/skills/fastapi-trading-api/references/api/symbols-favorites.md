# GET /symbols/favorites and DELETE /symbols/favorites/{symbol}

## Summary
Reads or removes favorite symbols persisted in the backend.

## Example read request
```http
GET /symbols/favorites
```

## Example read response
```json
{
  "items": [{"symbol": "005930", "name": "삼성전자"}]
}
```

## Example delete request
```http
DELETE /symbols/favorites/005930
```

## Example delete response
```json
{
  "symbol": "005930",
  "removed": true
}
```
