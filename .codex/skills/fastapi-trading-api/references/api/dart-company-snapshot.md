# GET /disclosures/dart/company-snapshot

## Summary
Looks up a company by corp code, stock code, or corp name and returns recent filing context.

## Example request
```http
GET /disclosures/dart/company-snapshot?stock_code=005930
```

## Example response
```json
{
  "company": {"corp_code": "00126380", "stock_code": "005930", "corp_name": "삼성전자"},
  "recent_filings": {"count": 5, "items": []}
}
```
