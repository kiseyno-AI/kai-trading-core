# GET /external/news/market

## Summary
Stored market and business-line news articles (NAVER News Search topics from `app/market_news/catalog.py`). Never calls NAVER. Newest first by publication time. `as_of` keeps only articles Core had first observed before that instant (point-in-time reads); `start`/`end` filter by publication time.

## Example request
```http
GET /external/news/market?topics=line_hbm,line_shipbuilding&as_of=2026-11-08T09:00:00%2B09:00&limit=2
Authorization: Bearer <token>
```

## Example response
```json
{
  "count": 2,
  "items": [
    {"topic_key": "line_hbm", "region": "KR", "title": "이재용의 ‘뉴삼성’ 방정식은 AI", "description": "…", "url": "https://…", "published_at": "2026-11-08T08:00:00+09:00", "first_seen_at": "2026-11-08T08:20:11+09:00"},
    {"topic_key": "line_shipbuilding", "region": "KR", "title": "LNG 운반선 7척 중 5척 중국이 낚아채…", "description": "…", "url": "https://…", "published_at": "2026-11-08T07:54:00+09:00", "first_seen_at": "2026-11-08T08:20:11+09:00"}
  ]
}
```
