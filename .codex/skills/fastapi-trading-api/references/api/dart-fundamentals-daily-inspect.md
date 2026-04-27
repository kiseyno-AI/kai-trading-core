# GET /disclosures/dart/fundamentals/daily/inspect

## Summary
Reads persisted daily DART fundamentals refresh runs for the requested `market_date` and `fs_div`.

The inspect payload keeps run-level counters such as `symbols_checked`, `symbols_refreshed`, and `symbols_failed`, and now also exposes a dedicated cycle-day updated-company activity surface through:
- `updated_entries_summary`
- `updated_entries_preview`
- `updated_entries`

That activity list is intentionally separate from latest-available coverage semantics. One primary updated entry is returned per symbol for the selected run:
- `initial_fundamentals_capture`: the symbol had no stored fundamentals before this run
- `financial_statement_refresh`: the symbol already had stored fundamentals and this run captured a newer filing

`updated_entries_preview` is the first three items from the full updated list, and `updated_entries_summary` carries preview metadata for `first 3 + More` UI flows.

## Example request
`GET /disclosures/dart/fundamentals/daily/inspect?market_date=2026-04-27&fs_div=CFS`

## Example response
```json
{
  "latest_run": {
    "id": 41,
    "market_date": "2026-04-27",
    "status": "completed",
    "fs_div": "CFS",
    "symbols_checked": 2812,
    "symbols_refreshed": 4,
    "symbols_failed": 0,
    "events_detected": 6,
    "events_captured": 4,
    "updated_entries_summary": {
      "updated_count": 4,
      "visible_preview_limit": 3,
      "visible_preview_count": 3,
      "has_more": true,
      "remaining_count": 1,
      "full_list_available": true
    },
    "updated_entries_preview": [
      {
        "symbol": "005930",
        "stock_code": "005930",
        "company_name": "삼성전자",
        "update_category": "financial_statement_refresh",
        "category_label": "Financial statement refresh"
      },
      {
        "symbol": "123456",
        "stock_code": "123456",
        "company_name": "Missing Corp",
        "update_category": "initial_fundamentals_capture",
        "category_label": "Initial fundamentals capture"
      },
      {
        "symbol": "000660",
        "stock_code": "000660",
        "company_name": "SK하이닉스",
        "update_category": "financial_statement_refresh",
        "category_label": "Financial statement refresh"
      }
    ],
    "updated_entries": [
      {
        "symbol": "005930",
        "stock_code": "005930",
        "company_name": "삼성전자",
        "update_category": "financial_statement_refresh",
        "category_label": "Financial statement refresh"
      },
      {
        "symbol": "123456",
        "stock_code": "123456",
        "company_name": "Missing Corp",
        "update_category": "initial_fundamentals_capture",
        "category_label": "Initial fundamentals capture"
      },
      {
        "symbol": "000660",
        "stock_code": "000660",
        "company_name": "SK하이닉스",
        "update_category": "financial_statement_refresh",
        "category_label": "Financial statement refresh"
      },
      {
        "symbol": "051910",
        "stock_code": "051910",
        "company_name": "LG화학",
        "update_category": "financial_statement_refresh",
        "category_label": "Financial statement refresh"
      }
    ]
  },
  "runs": []
}
```
