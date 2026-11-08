# POST /external/news/market/collect

## Summary
Operator collection from NAVER News Search (the background runner already does this every `MARKET_NEWS_COLLECT_INTERVAL_MINUTES`, default 60). `topics` is a comma list of topic keys (all when omitted). `pages` > 1 backfills older articles, up to 10 pages (NAVER stops at the 1,000th result). Articles are stored once per topic and URL; the first observation time is never overwritten. One failing topic does not stop the others.

## Example request
```http
POST /external/news/market/collect?topics=line_hbm,line_shipbuilding&pages=1
Authorization: Bearer <token>
```

## Example response
```json
{
  "status": "completed",
  "observed_at": "2026-11-07T23:20:11+00:00",
  "requests": 2,
  "fetched": 100,
  "inserted": 37,
  "failed_topics": 0,
  "topics": {"line_hbm": {"fetched": 50, "inserted": 20}, "line_shipbuilding": {"fetched": 50, "inserted": 17}}
}
```
