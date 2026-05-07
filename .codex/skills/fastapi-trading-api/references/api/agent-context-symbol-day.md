# GET /agent-context/symbol-day

## Summary
Returns compact symbol/day analysis context for downstream agents. The payload combines quote, price, factor, and PyKRX enrichment blocks with stored DART fundamentals and recent disclosure events. The DART section now includes `fundamentals.latest_snapshot.fundamental_context` for canonical BS/IS/CF facts plus profitability, leverage/liquidity, and cash-flow quality metrics, and `fundamentals.disclosure_context` for recent corrections and ownership or large-holder disclosure signals.

## Example request
```http
GET /agent-context/symbol-day?market_date=2026-04-15&symbols=005930&fs_div=CFS&disclosures_limit=5
```

## Example response
```json
{
  "request": {
    "market_date": "2026-04-15",
    "symbols": ["005930"],
    "fs_div": "CFS",
    "disclosures_limit": 5
  },
  "summary": {
    "requested_symbol_count": 1,
    "matched_symbol_count": 1,
    "fundamentals_count": 1,
    "quote_context_count": 1
  },
  "items": [
    {
      "symbol": "005930",
      "company": {
        "name_ko": "삼성전자",
        "dart": {
          "corp_code": "00126380"
        }
      },
      "fundamentals": {
        "latest_snapshot": {
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
                "current_assets": {
                  "status": "available",
                  "current_value": 240
                },
                "current_liabilities": {
                  "status": "available",
                  "current_value": 120
                }
              },
              "cash_flow_statement": {
                "operating_cash_flow": {
                  "status": "available",
                  "current_value": 24
                }
              }
            },
            "analysis": {
              "leverage_liquidity": {
                "status": "available",
                "metrics": {
                  "current_ratio": "2.0000",
                  "cash_ratio": "0.7500"
                }
              },
              "cash_flow_quality": {
                "status": "partial",
                "metrics": {
                  "operating_cash_flow_to_net_income": "1.6000"
                }
              }
            },
            "source_provenance": {
              "provider": "dart",
              "dataset": "stored_dart_fundamentals",
              "rcept_no": "20250312000123",
              "fs_div": "CFS",
              "currency": "KRW",
              "freshness_provenance": {
                "freshness_status": "available_but_old"
              }
            }
          }
        },
        "disclosure_context": {
          "summary": {
            "correction_count": 1,
            "ownership_related_count": 1,
            "large_holder_related_count": 1
          },
          "recent_ownership_large_holder_changes": [
            {
              "rcept_no": "20260406000055",
              "report_nm": "주식등의 대량보유상황보고서",
              "is_large_holder_related": true
            }
          ]
        },
        "freshness_provenance": {
          "freshness_status": "available_but_old",
          "provenance_status": "provider_sourced"
        }
      },
      "recent_disclosures": {
        "count": 2,
        "items": [
          {
            "rcept_no": "20260407000001",
            "report_nm": "[기재정정] 사업보고서 (2025.12)",
            "is_correction": true,
            "disclosure_tags": ["financial_report", "annual_report", "correction"]
          },
          {
            "rcept_no": "20260406000055",
            "report_nm": "주식등의 대량보유상황보고서",
            "is_large_holder_related": true
          }
        ]
      },
      "context_provenance": {
        "fundamentals": {
          "freshness_status": "available_but_old",
          "provenance_status": "provider_sourced"
        },
        "recent_disclosures": {
          "freshness_status": "available_but_old",
          "provenance_status": "provider_sourced"
        }
      }
    }
  ]
}
```

## Notes
- `fundamentals.latest_snapshot.fundamental_context` is the canonical normalized DART block for downstream agents.
- Missing DART facts are explicit with `status="missing"` instead of being silently omitted.
- `fundamentals.disclosure_context` summarizes recent corrections and ownership or large-holder changes from the existing DART disclosure event stream; it does not require a separate ownership-only endpoint.
