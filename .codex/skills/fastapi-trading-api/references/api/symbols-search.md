# GET /symbols/search

## Summary
Searches the stored symbol master by code or Korean company name.

## Example request
```http
GET /symbols/search?q=삼성&limit=5
```

## Example response
```json
{
  "items": [
    {
      "symbol": "005930",
      "name": "삼성전자",
      "market": "KOSPI",
      "is_active": true
    }
  ],
  "count": 1
}
```
