# /external/news

## Summary
Inspect or collect optional external news context for persisted symbols. Stored news classifications are prompt-safe: material items come first, source evidence is explicit, and headline-only or paywalled items are not treated as full-text summaries.

## Example request
```http
GET /external/news?symbols=005930&limit=10
```

```http
POST /external/news/collect?symbols=005930&display=5
```

## Example response
```json
{
  "status": "ok",
  "provider": {
    "provider": "naver_news_search",
    "enabled": true,
    "status": "available",
    "reason": null
  },
  "summary": {
    "count": 1,
    "requested_symbols": ["005930"]
  },
  "items": [
    {
      "symbol": "005930",
      "provider": "naver_news_search",
      "title": "삼성전자 공급 계약",
      "description": "실적 개선 기대",
      "url": "https://news.example/1",
      "published_at": "2026-05-08T00:00:00+00:00",
      "event_type": "contract",
      "event_tags": ["contract", "earnings"],
      "materiality": "material",
      "materiality_reason": "Detected company-specific material event type \"contract\" from headline/snippet keywords.",
      "source_evidence": {
        "url": "https://news.example/1",
        "source_url": "https://search.example/1",
        "publication": "news.example",
        "published_at": "2026-05-08T00:00:00+00:00"
      },
      "access_status": "headline_only",
      "content_basis": "headline_or_snippet_only",
      "price_forecast_prohibited": true
    }
  ]
}
```

## Notes
- `access_status` is one of `full_text`, `headline_only`, `paywalled`, or `fetch_failed`.
- `materiality` is one of `material`, `noise`, or `unknown`.
- Decider context exposes material news first and keeps background/noise items in a bounded `noise_filter` section.
