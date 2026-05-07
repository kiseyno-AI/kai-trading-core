# GET /agent-context/symbol-day

## Summary
Returns compact symbol/day analysis context for downstream agents. The payload combines the latest `quote_context` from `current_quote_cache` with persisted `price_context` from `daily_price_contexts`, structured factor-score summaries, and same-day `signal_features`. When `signal_features` are present, the row now explicitly marks whether the symbol carries direct KIS ranking evidence, owned-data fallback context, or both.

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
    "quote_context_count": 1,
    "price_context_count": 1,
    "signal_feature_count": 1,
    "factor_row_counts": {
      "unified_value": 1,
      "quality": 1,
      "momentum": 1,
      "advanced_value": 1,
      "low_risk": 1,
      "beta": 1,
      "composite": 1
    },
    "context_completeness": {
      "signal_features": {
        "present_rows": 1,
        "missing_rows": 0,
        "coverage_status": "complete"
      }
    }
  },
  "items": [
    {
      "symbol": "005930",
      "market_date": "2026-04-15",
      "company": {"name_ko": "삼성전자", "market": "KOSPI"},
      "quote_context": {
        "available": true,
        "source": "kis",
        "trade_date": "2026-04-15",
        "cached_at": "2026-04-15T09:57:00+09:00"
      },
      "price_context": {
        "trade_date": "2026-04-15",
        "close_price": 72100,
        "liquidity": {
          "turnover_krw": 9300000000,
          "turnover_source": "provider",
          "turnover_is_estimated": false
        },
        "price_bar_quality": {
          "open_price_zero": false,
          "turnover_missing_or_zero": false,
          "gap_unreliable_due_to_open": false,
          "issue_codes": []
        }
      },
      "signal_features": {
        "symbol": "005930",
        "market_date": "2026-04-15",
        "source": "kis_ranking",
        "signal_count": 2,
        "volume_power_rank": 3,
        "overtime_volume_rank": 7,
        "row_kind": "direct_plus_fallback",
        "provenance": {
          "row_kind": "direct_plus_fallback",
          "direct_signal_source_count": 2,
          "fallback_groups_present": [
            "price_action",
            "liquidity",
            "returns",
            "volatility",
            "factor_scores",
            "quote_snapshot"
          ]
        },
        "freshness": {
          "market_date": "2026-04-15",
          "price_bars": {
            "trade_date": "2026-04-15",
            "trade_date_relation_to_market_date": "same_market_date"
          },
          "quote_snapshot": {
            "trade_date": "2026-04-15",
            "trade_date_relation_to_market_date": "same_market_date"
          }
        },
        "fallback_features": {
          "price_action": {
            "close_return_1d_pct": 2.43553,
            "candle_direction": "bullish",
            "close_location_label": "near_high"
          },
          "factor_scores": {
            "quality": {
              "score": 0.741,
              "status": "partial"
            }
          }
        }
      },
      "scores": {
        "quality": {
          "status": "partial",
          "score": 0.741
        },
        "momentum": {
          "status": "available",
          "score": 0.914
        }
      },
      "context_completeness": {
        "factor_rows": {
          "quality": {
            "row_status": "present",
            "null_submetric_fields": ["accruals_ratio"]
          }
        },
        "signal_features": {
          "row_status": "present",
          "row_kind": "direct_plus_fallback",
          "direct_signal_source_count": 2,
          "fallback_groups_present": [
            "price_action",
            "liquidity",
            "returns",
            "volatility",
            "factor_scores",
            "quote_snapshot"
          ]
        },
        "sector_peer_mapping": {"status": "unavailable"}
      }
    }
  ]
}
```
