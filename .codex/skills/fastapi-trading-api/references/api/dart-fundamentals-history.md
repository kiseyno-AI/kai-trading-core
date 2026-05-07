# GET /disclosures/dart/fundamentals/history

## Summary
Reads stored DART fundamentals history for one or more symbols over a requested period-end window. Each entry includes the original filing/period metadata, stored highlight rows, raw statement rows, and a normalized `fundamental_context` block rebuilt from persisted DART highlights.

## Example request
```http
GET /disclosures/dart/fundamentals/history?symbols=005930&symbols=000660&start_date=2024-01-01&end_date=2025-12-31&fs_div=CFS
```

## Example response
```json
{
  "request": {
    "symbols": ["005930", "000660"],
    "start_date": "2024-01-01",
    "end_date": "2025-12-31",
    "fs_div": "CFS",
    "matched_on": "period_end_date"
  },
  "summary": {
    "requested_symbol_count": 2,
    "matched_symbol_count": 1,
    "missing_symbols": ["000660"],
    "entries": 1
  },
  "companies": [
    {
      "symbol": "005930",
      "corp_code": "00126380",
      "corp_name": "삼성전자",
      "entries": [
        {
          "filing": {
            "rcept_no": "20250312000123",
            "report_name": "사업보고서 (2024.12)",
            "receipt_date": "2025-03-12",
            "is_correction": false
          },
          "period": {
            "business_year": 2024,
            "report_code": "11011",
            "period_end_date": "2024-12-31"
          },
          "fundamental_context": {
            "normalized_facts": {
              "balance_sheet": {
                "total_assets": {
                  "status": "available",
                  "current_value": 400
                }
              }
            },
            "analysis": {
              "leverage_liquidity": {
                "metrics": {
                  "debt_to_equity": "0.6667"
                }
              }
            },
            "source_provenance": {
              "provider": "dart",
              "dataset": "stored_dart_fundamentals",
              "rcept_no": "20250312000123",
              "captured_at": "2025-03-12T01:02:03+00:00"
            }
          },
          "statements": {
            "count": 1,
            "items": [
              {
                "row_key": "row-1",
                "account_name": "매출액",
                "current_amount": 100
              }
            ]
          }
        }
      ]
    }
  ]
}
```
