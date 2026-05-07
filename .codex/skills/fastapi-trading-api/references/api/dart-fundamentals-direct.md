# GET /disclosures/dart/fundamentals

## Summary
Fetches direct single-company DART financial statements for a requested business year and report window. The response now includes `fundamental_context`, which normalizes common balance-sheet, income-statement, and cash-flow facts and derives agent-ready profitability, leverage/liquidity, and cash-flow quality metrics with source provenance.

## Example request
```http
GET /disclosures/dart/fundamentals?stock_code=005930&business_year=2024&report_code=11011&fs_div=CFS
```

## Example response
```json
{
  "lookup": {
    "stock_code": "005930",
    "corp_code": "00126380"
  },
  "request": {
    "business_year": 2024,
    "report_code": "11011",
    "report_label": "annual_report",
    "statement_type": "consolidated",
    "fs_div": "CFS",
    "dataset": "fnlttSinglAcntAll"
  },
  "highlights": {
    "revenue": {
      "account_name": "매출액",
      "current_cumulative_amount": 100000
    },
    "operating_income": {
      "account_name": "영업이익",
      "current_amount": 20000
    },
    "ratios": {
      "operating_margin": "0.2000",
      "current_ratio": "1.7778"
    }
  },
  "fundamental_context": {
    "normalized_facts": {
      "balance_sheet": {
        "current_assets": {
          "status": "available",
          "current_value": 320000
        },
        "current_liabilities": {
          "status": "available",
          "current_value": 180000
        }
      },
      "cash_flow_statement": {
        "operating_cash_flow": {
          "status": "available",
          "current_value": 25000
        },
        "investing_cash_flow": {
          "status": "missing"
        }
      }
    },
    "analysis": {
      "profitability": {
        "status": "partial",
        "metrics": {
          "operating_margin": "0.2000",
          "return_on_assets": "0.0300"
        }
      },
      "leverage_liquidity": {
        "status": "available",
        "metrics": {
          "debt_to_equity": "1.5000",
          "current_ratio": "1.7778",
          "cash_ratio": "0.5000"
        }
      },
      "cash_flow_quality": {
        "status": "partial",
        "metrics": {
          "operating_cash_flow_to_net_income": "1.6667"
        }
      }
    },
    "source_provenance": {
      "provider": "dart",
      "dataset": "fnlttSinglAcntAll",
      "rcept_no": "20250312000123",
      "report_code": "11011",
      "fs_div": "CFS",
      "currency": "KRW",
      "source_status": "000"
    }
  }
}
```
