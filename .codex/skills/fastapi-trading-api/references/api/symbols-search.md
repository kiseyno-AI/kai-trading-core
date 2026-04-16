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
  "query": "삼성",
  "count": 1,
  "items": [
    {
      "symbol": "005930",
      "standard_code": "KR7005930003",
      "name_ko": "삼성전자",
      "market": "KOSPI"
    }
  ]
}
```
