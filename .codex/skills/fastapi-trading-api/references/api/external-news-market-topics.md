# GET /external/news/market/topics

## Summary
The market news topic catalog (46 NAVER queries: Korean market, US, global, and Korean business lines) with the business-line keys each topic speaks to, plus the latest observation time.

## Example request
```http
GET /external/news/market/topics
Authorization: Bearer <token>
```

## Example response
```json
{
  "topics": [
    {"key": "kr_kospi", "region": "KR", "query": "코스피", "business_lines": []},
    {"key": "line_hbm", "region": "KR", "query": "HBM 메모리", "business_lines": ["semiconductor_memory", "semiconductor_equipment_materials"]}
  ],
  "latest_first_seen_at": "2026-11-08T08:20:11+09:00"
}
```
