# GET /scanner/trending-stocks

## Summary
Reads stored trending-stock candidates for a given period and direction.

## Example request
```http
GET /scanner/trending-stocks?period=5d&direction=both&limit=20&markets=KOSPI
```

## Example response
```json
{
  "period": "5d",
  "direction": "both",
  "items": []
}
```
