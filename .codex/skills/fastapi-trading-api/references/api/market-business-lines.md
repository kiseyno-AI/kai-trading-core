# GET /market/business-lines

## Summary
The business-line taxonomy (`app/market/business_lines.py`, 42 lines with Korean labels) with how many symbols each holds, plus classification sources. `symbols` (comma list) adds each symbol's current profile: business line, source (`override` / `ksic` / `krx_sector` / `default`), DART KSIC industry code, KRX sector, effective date. Reads stored `symbol_industry_profiles` only.

## Example request
```http
GET /market/business-lines?symbols=005930,329180
Authorization: Bearer <token>
```

## Example response
```json
{
  "business_lines": [
    {"key": "semiconductor_memory", "label": "반도체 칩", "symbols": 38},
    {"key": "shipbuilding", "label": "조선", "symbols": 21}
  ],
  "classified_symbols": 2769,
  "sources": {"ksic": 2690, "override": 57, "krx_sector": 18, "default": 4},
  "profiles": [
    {"symbol": "005930", "business_line": "semiconductor_memory", "label": "반도체 칩", "line_source": "override", "ksic_code": "264", "krx_sector": "전기·전자", "corp_name": "삼성전자", "effective_from": "2026-11-08"},
    {"symbol": "329180", "business_line": "shipbuilding", "label": "조선", "line_source": "override", "ksic_code": "31113", "krx_sector": "운송장비·부품", "corp_name": "HD현대중공업", "effective_from": "2026-11-08"}
  ]
}
```
